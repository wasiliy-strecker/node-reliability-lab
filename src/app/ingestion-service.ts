import { createHash } from 'node:crypto'
import { performance } from 'node:perf_hooks'

import type { RequestContext } from '../context/request-context.js'
import { ReliabilityError, RemoteWorkerError } from '../errors.js'
import { decodeNdjson } from '../streams/ndjson.js'
import { mapConcurrent } from '../streams/map-concurrent.js'
import type { BoundedWorkerPool } from '../workers/bounded-worker-pool.js'
import type { EventTaskInput, EventTaskOutput } from '../workers/event-task.js'

export interface IngestionLimits {
  readonly maxBodyBytes: number
  readonly maxLineBytes: number
  readonly maxRecords: number
}

export interface IngestionServiceOptions {
  readonly pool: BoundedWorkerPool<EventTaskInput, EventTaskOutput>
  readonly concurrency: number
  readonly fingerprintRounds: number
  readonly limits: IngestionLimits
}

export interface IngestionResult {
  readonly requestId: string
  readonly processed: number
  readonly normalizedBytes: number
  readonly fingerprint: string
  readonly durationMs: number
}

export class IngestionService {
  readonly #pool: BoundedWorkerPool<EventTaskInput, EventTaskOutput>
  readonly #concurrency: number
  readonly #fingerprintRounds: number
  readonly #limits: IngestionLimits

  constructor(options: IngestionServiceOptions) {
    this.#pool = options.pool
    this.#concurrency = options.concurrency
    this.#fingerprintRounds = options.fingerprintRounds
    this.#limits = options.limits
  }

  async ingest(
    source: AsyncIterable<Uint8Array | string>,
    context: RequestContext,
    signal: AbortSignal,
  ): Promise<IngestionResult> {
    const startedAt = performance.now()
    const aggregate = createHash('sha256')
    const records = decodeNdjson(source, { ...this.#limits, signal })
    const processed = mapConcurrent(
      records,
      async (record, index, taskSignal) => {
        try {
          return await this.#pool.run(
            {
              index,
              value: record.value,
              fingerprintRounds: this.#fingerprintRounds,
            },
            {
              signal: taskSignal,
              context: { ...context, taskId: `${context.requestId}:${index}` },
            },
          )
        } catch (error) {
          if (error instanceof RemoteWorkerError && error.code === 'invalid_event') {
            throw new ReliabilityError(
              `Event at line ${record.lineNumber} is invalid: ${error.message}`,
              'invalid_event',
              { cause: error },
            )
          }
          throw error
        }
      },
      { concurrency: this.#concurrency, signal },
    )

    let count = 0
    let normalizedBytes = 0
    for await (const event of processed) {
      count += 1
      normalizedBytes += event.normalizedBytes
      aggregate.update(event.fingerprint)
    }

    if (count === 0) {
      throw new ReliabilityError('At least one event is required', 'empty_ingestion')
    }

    return {
      requestId: context.requestId,
      processed: count,
      normalizedBytes,
      fingerprint: aggregate.digest('hex'),
      durationMs: performance.now() - startedAt,
    }
  }
}
