import { randomUUID } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { performance } from 'node:perf_hooks'

import { requestContext, type RequestContext } from '../context/request-context.js'
import {
  isAbortError,
  NdjsonLimitError,
  NdjsonSyntaxError,
  OperationAbortedError,
  OverloadedError,
  PoolClosedError,
  ReliabilityError,
} from '../errors.js'
import { ShutdownCoordinator, type ShutdownResult } from '../lifecycle/shutdown-coordinator.js'
import { RuntimeProbe } from '../observability/runtime-probe.js'
import { BoundedWorkerPool } from '../workers/bounded-worker-pool.js'
import type { EventTaskInput, EventTaskOutput } from '../workers/event-task.js'
import type { AppConfig } from './config.js'
import { IngestionService } from './ingestion-service.js'
import { ServiceMetrics } from './metrics.js'
import { sendJson, sendProblem } from './problem.js'

export type StructuredLogger = (record: Readonly<Record<string, unknown>>) => void

export interface ReliabilityServerOptions {
  readonly config: AppConfig
  readonly logger?: StructuredLogger
}

export interface ListeningAddress {
  readonly host: string
  readonly port: number
  readonly origin: string
}

const requestIdPattern = /^[A-Za-z0-9._-]{1,128}$/u

export class ReliabilityServer {
  readonly #config: AppConfig
  readonly #logger: StructuredLogger
  readonly #server: Server
  readonly #pool: BoundedWorkerPool<EventTaskInput, EventTaskOutput>
  readonly #probe = new RuntimeProbe()
  readonly #metrics = new ServiceMetrics()
  readonly #coordinator: ShutdownCoordinator
  readonly #ingestion: IngestionService
  #activeIngestions = 0
  #serverClosePromise: Promise<void> | undefined

  constructor(options: ReliabilityServerOptions) {
    this.#config = options.config
    this.#logger = options.logger ?? (() => undefined)
    this.#pool = new BoundedWorkerPool<EventTaskInput, EventTaskOutput>({
      workerUrl: new URL('../workers/event-worker.js', import.meta.url),
      size: this.#config.workerCount,
      maxQueue: this.#config.workerQueueSize,
      name: 'event-fingerprint',
    })
    this.#ingestion = new IngestionService({
      pool: this.#pool,
      concurrency: this.#config.perRequestConcurrency,
      fingerprintRounds: this.#config.fingerprintRounds,
      limits: {
        maxBodyBytes: this.#config.maxBodyBytes,
        maxLineBytes: this.#config.maxLineBytes,
        maxRecords: this.#config.maxRecords,
      },
    })
    this.#coordinator = new ShutdownCoordinator({
      gracePeriodMs: this.#config.shutdownGracePeriodMs,
    })
    this.#server = createServer((request, response) => {
      void this.#handle(request, response)
    })
    this.#server.keepAliveTimeout = 5_000
    this.#server.headersTimeout = 10_000
    this.#server.requestTimeout = 30_000

    this.#coordinator.register({
      name: 'http-server',
      stop: () => this.#stopAccepting(),
      drain: () => this.#serverClosePromise,
      force: () => this.#server.closeAllConnections(),
    })
    this.#coordinator.register({
      name: 'worker-pool',
      close: () => this.#pool.close({ gracePeriodMs: this.#config.shutdownGracePeriodMs }),
      force: () => this.#pool.close({ gracePeriodMs: 0 }),
    })
    this.#coordinator.register({
      name: 'runtime-probe',
      close: () => this.#probe.stop(),
      force: () => this.#probe.stop(),
    })
  }

  get isReady(): boolean {
    return this.#coordinator.isReady
  }

  get metrics(): ServiceMetrics {
    return this.#metrics
  }

  get workerStats(): ReturnType<BoundedWorkerPool<EventTaskInput, EventTaskOutput>['stats']> {
    return this.#pool.stats()
  }

  async start(): Promise<ListeningAddress> {
    if (this.#server.listening) throw new Error('The server is already listening')
    if (!this.#coordinator.isReady) throw new Error('A stopped server cannot be started again')
    this.#probe.start()
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => {
        this.#server.off('listening', onListening)
        reject(error)
      }
      const onListening = (): void => {
        this.#server.off('error', onError)
        resolve()
      }
      this.#server.once('error', onError)
      this.#server.once('listening', onListening)
      this.#server.listen({ host: this.#config.host, port: this.#config.port })
    })

    const address = this.#server.address()
    if (!address || typeof address === 'string') throw new Error('The server has no TCP address')
    const displayHost = address.address.includes(':') ? `[${address.address}]` : address.address
    return {
      host: address.address,
      port: address.port,
      origin: `http://${displayHost}:${address.port}`,
    }
  }

  shutdown(reason: string): Promise<ShutdownResult> {
    return this.#coordinator.shutdown(reason)
  }

  forceShutdown(reason: string): Promise<ShutdownResult> {
    return this.#coordinator.force(reason)
  }

  async #handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const startedAt = performance.now()
    const requestId = resolveRequestId(request.headers['x-request-id'])
    const context: RequestContext = Object.freeze({ requestId, startedAt })
    response.setHeader('x-request-id', requestId)
    this.#metrics.requestStarted()

    try {
      await requestContext.run(context, () => this.#route(request, response, context))
    } catch (error) {
      this.#logger({
        level: 'error',
        event: 'request.unhandled_error',
        requestId,
        error: error instanceof Error ? error.message : String(error),
      })
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/internal-error',
        title: 'Internal server error',
        status: 500,
        detail: 'The request could not be completed',
        requestId,
      })
    } finally {
      const statusCode = request.aborted || response.destroyed ? 499 : response.statusCode
      this.#metrics.requestFinished(statusCode, performance.now() - startedAt)
    }
  }

  async #route(
    request: IncomingMessage,
    response: ServerResponse,
    context: RequestContext,
  ): Promise<void> {
    const url = new URL(request.url ?? '/', 'http://localhost')
    if (request.method === 'GET' && url.pathname === '/health/live') {
      sendJson(response, 200, { status: 'live' })
      return
    }
    if (request.method === 'GET' && url.pathname === '/health/ready') {
      const status = this.#coordinator.isReady ? 200 : 503
      sendJson(response, status, { status: this.#coordinator.isReady ? 'ready' : 'draining' })
      return
    }
    if (request.method === 'GET' && url.pathname === '/metrics') {
      const body = this.#metrics.renderPrometheus(
        this.#pool.stats(),
        this.#probe.snapshot(),
        this.#coordinator.isReady,
      )
      response.writeHead(200, {
        'content-type': 'text/plain; version=0.0.4; charset=utf-8',
        'content-length': String(Buffer.byteLength(body)),
      })
      response.end(body)
      return
    }
    if (request.method === 'POST' && url.pathname === '/v1/events') {
      await this.#handleIngestion(request, response, context)
      return
    }

    sendProblem(response, {
      type: 'https://node-reliability-lab.dev/problems/not-found',
      title: 'Route not found',
      status: 404,
      detail: 'The requested route does not exist',
      requestId: context.requestId,
    })
  }

  async #handleIngestion(
    request: IncomingMessage,
    response: ServerResponse,
    context: RequestContext,
  ): Promise<void> {
    if (!this.#coordinator.isReady) {
      this.#sendUnavailable(response, context.requestId, 'The service is shutting down')
      request.resume()
      return
    }
    if (contentType(request) !== 'application/x-ndjson') {
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/unsupported-media-type',
        title: 'Unsupported media type',
        status: 415,
        detail: 'Content-Type must be application/x-ndjson',
        requestId: context.requestId,
      })
      request.resume()
      return
    }
    const contentLength = parseContentLength(request)
    if (contentLength !== undefined && contentLength > this.#config.maxBodyBytes) {
      sendProblem(
        response,
        {
          type: 'https://node-reliability-lab.dev/problems/payload-too-large',
          title: 'Payload too large',
          status: 413,
          detail: `The request body exceeds ${this.#config.maxBodyBytes} bytes`,
          requestId: context.requestId,
        },
        { connection: 'close' },
      )
      request.resume()
      return
    }
    if (this.#activeIngestions >= this.#config.maxConcurrentRequests) {
      this.#sendUnavailable(response, context.requestId, 'The ingestion admission limit is full')
      request.resume()
      return
    }

    this.#activeIngestions += 1
    const clientAbort = new AbortController()
    const abortForClient = (): void => {
      if (!clientAbort.signal.aborted) {
        clientAbort.abort(new OperationAbortedError('The client disconnected'))
      }
    }
    const abortForClosedResponse = (): void => {
      if (!response.writableEnded) abortForClient()
    }
    request.once('aborted', abortForClient)
    response.once('close', abortForClosedResponse)
    const signal = AbortSignal.any([clientAbort.signal, this.#coordinator.signal])

    try {
      const result = await this.#ingestion.ingest(drainableBody(request), context, signal)
      this.#metrics.eventsProcessed(result.processed, result.normalizedBytes)
      sendJson(response, 200, result, this.#coordinator.isReady ? {} : { connection: 'close' })
    } catch (error) {
      this.#handleIngestionError(error, request, response, context.requestId, signal)
    } finally {
      this.#activeIngestions -= 1
      request.off('aborted', abortForClient)
      response.off('close', abortForClosedResponse)
      if (!this.#coordinator.isReady) this.#server.closeIdleConnections()
    }
  }

  #handleIngestionError(
    error: unknown,
    request: IncomingMessage,
    response: ServerResponse,
    requestId: string,
    signal: AbortSignal,
  ): void {
    if (signal.aborted || isAbortError(error)) {
      this.#metrics.requestAborted()
      if (!request.aborted && !response.destroyed) {
        this.#sendUnavailable(response, requestId, 'The request was aborted')
      }
      return
    }

    if (error instanceof NdjsonLimitError) {
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/payload-too-large',
        title: 'Payload too large',
        status: 413,
        detail: error.message,
        requestId,
        ...(error.lineNumber === undefined ? {} : { lineNumber: error.lineNumber }),
      })
    } else if (error instanceof NdjsonSyntaxError) {
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/invalid-ndjson',
        title: 'Invalid NDJSON',
        status: 400,
        detail: error.message,
        requestId,
        lineNumber: error.lineNumber,
      })
    } else if (
      error instanceof ReliabilityError &&
      (error.code === 'invalid_event' || error.code === 'empty_ingestion')
    ) {
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/invalid-event',
        title: 'Invalid event',
        status: 400,
        detail: error.message,
        requestId,
      })
    } else if (error instanceof OverloadedError || error instanceof PoolClosedError) {
      this.#sendUnavailable(response, requestId, error.message)
    } else {
      this.#logger({
        level: 'error',
        event: 'ingestion.failed',
        requestId,
        error: error instanceof Error ? error.message : String(error),
      })
      sendProblem(response, {
        type: 'https://node-reliability-lab.dev/problems/internal-error',
        title: 'Internal server error',
        status: 500,
        detail: 'The event stream could not be processed',
        requestId,
      })
    }

    if (!request.complete && !request.destroyed) request.resume()
  }

  #sendUnavailable(response: ServerResponse, requestId: string, detail: string): void {
    sendProblem(
      response,
      {
        type: 'https://node-reliability-lab.dev/problems/service-unavailable',
        title: 'Service unavailable',
        status: 503,
        detail,
        requestId,
      },
      { 'retry-after': '1' },
    )
  }

  #stopAccepting(): void {
    if (!this.#server.listening || this.#serverClosePromise) return
    this.#serverClosePromise = new Promise<void>((resolve, reject) => {
      this.#server.close((error) => {
        if (error) reject(error)
        else resolve()
      })
      this.#server.closeIdleConnections()
    })
  }
}

function resolveRequestId(header: string | string[] | undefined): string {
  const candidate = Array.isArray(header) ? header[0] : header
  return candidate && requestIdPattern.test(candidate) ? candidate : randomUUID()
}

function contentType(request: IncomingMessage): string {
  return (request.headers['content-type'] ?? '').split(';', 1)[0]?.trim().toLowerCase() ?? ''
}

function parseContentLength(request: IncomingMessage): number | undefined {
  const raw = request.headers['content-length']
  if (raw === undefined) return undefined
  const parsed = Number(raw)
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined
}

function drainableBody(request: IncomingMessage): AsyncIterable<Uint8Array> {
  const iterator = request[Symbol.asyncIterator]()
  return {
    [Symbol.asyncIterator]() {
      return {
        next: async (): Promise<IteratorResult<Uint8Array>> => {
          const result = await iterator.next()
          return result.done
            ? { done: true, value: undefined }
            : { done: false, value: Buffer.from(result.value as Uint8Array) }
        },
        return: (): Promise<IteratorReturnResult<undefined>> => {
          request.resume()
          return Promise.resolve({ done: true, value: undefined })
        },
      }
    },
  }
}
