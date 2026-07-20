import assert from 'node:assert/strict'
import { request as httpRequest, type ClientRequest } from 'node:http'
import { describe, it, type TestContext } from 'node:test'

import { loadConfig, type AppConfig } from '../src/app/config.js'
import { ReliabilityServer } from '../src/app/server.js'

describe('ReliabilityServer', () => {
  it('serves health, ingests events, preserves request IDs, and exposes metrics', async (testContext) => {
    const { application, origin } = await startServer(testContext)
    const live = await fetch(`${origin}/health/live`)
    assert.equal(live.status, 200)
    assert.deepEqual(await live.json(), { status: 'live' })

    const ingestion = await fetch(`${origin}/v1/events`, {
      method: 'POST',
      headers: {
        'content-type': 'application/x-ndjson',
        'x-request-id': 'integration-request',
      },
      body: `${JSON.stringify(event(1))}\n${JSON.stringify(event(2))}\n`,
    })
    const result = (await ingestion.json()) as Record<string, unknown>
    assert.equal(ingestion.status, 200)
    assert.equal(ingestion.headers.get('x-request-id'), 'integration-request')
    assert.equal(result.requestId, 'integration-request')
    assert.equal(result.processed, 2)
    assert.match(String(result.fingerprint), /^[a-f0-9]{64}$/u)

    const metrics = await fetch(`${origin}/metrics`)
    const body = await metrics.text()
    assert.equal(metrics.status, 200)
    assert.match(body, /node_reliability_events_processed_total 2/u)
    assert.match(body, /node_reliability_worker_queued 0/u)
    assert.equal(application.workerStats.completed, 2)
  })

  it('returns precise client errors without exposing internals', async (testContext) => {
    const { origin } = await startServer(testContext)
    const unsupported = await fetch(`${origin}/v1/events`, { method: 'POST', body: '{}' })
    assert.equal(unsupported.status, 415)

    const malformed = await postNdjson(origin, `${JSON.stringify(event(1))}\nnot-json\n`)
    const malformedProblem = (await malformed.json()) as Record<string, unknown>
    assert.equal(malformed.status, 400)
    assert.equal(malformedProblem.lineNumber, 2)

    const invalidEvent = await postNdjson(origin, `${JSON.stringify({ id: 'missing-fields' })}\n`)
    const invalidProblem = (await invalidEvent.json()) as Record<string, unknown>
    assert.equal(invalidEvent.status, 400)
    assert.match(String(invalidProblem.detail), /Event at line 1 is invalid/u)

    const empty = await postNdjson(origin, '\n\n')
    assert.equal(empty.status, 400)

    const missing = await fetch(`${origin}/unknown`)
    assert.equal(missing.status, 404)
  })

  it('enforces declared and streamed payload limits', async (testContext) => {
    const { origin } = await startServer(testContext, { maxBodyBytes: 128, maxLineBytes: 80 })
    const declared = await postNdjson(origin, `${'x'.repeat(200)}\n`)
    assert.equal(declared.status, 413)
    assert.equal(declared.headers.get('connection'), 'close')

    const streamed = openStreamingRequest(origin)
    streamed.request.end(`${JSON.stringify(event(99))}\n`)
    assert.equal((await streamed.response).status, 413)
  })

  it('generates a safe request ID when the caller sends an invalid one', async (testContext) => {
    const { origin } = await startServer(testContext)
    const response = await fetch(`${origin}/health/live`, {
      headers: { 'x-request-id': 'contains spaces and is rejected' },
    })
    assert.match(response.headers.get('x-request-id') ?? '', /^[a-f0-9-]{36}$/u)
  })

  it('rejects a second ingestion while the admission slot is occupied', async (testContext) => {
    const { application, origin } = await startServer(testContext, {
      maxConcurrentRequests: 1,
      perRequestConcurrency: 1,
    })
    const first = openStreamingRequest(origin)
    const serialized = JSON.stringify(event(1))
    first.request.write(serialized.slice(0, -1))
    await waitFor(() => application.metrics.snapshot().requestsActive === 1)

    const rejected = await postNdjson(origin, `${JSON.stringify(event(2))}\n`)
    assert.equal(rejected.status, 503)
    assert.equal(rejected.headers.get('retry-after'), '1')

    first.request.end(`${serialized.slice(-1)}\n`)
    assert.equal((await first.response).status, 200)
  })

  it('lets an accepted request finish during graceful shutdown', async () => {
    const application = new ReliabilityServer({
      config: configuration({ shutdownGracePeriodMs: 1_000 }),
    })
    const address = await application.start()
    const first = openStreamingRequest(address.origin)
    const serialized = JSON.stringify(event(1))
    first.request.write(serialized.slice(0, -1))
    await waitFor(() => application.metrics.snapshot().requestsActive === 1)

    const shutdown = application.shutdown('integration-test')
    assert.equal(application.isReady, false)
    first.request.end(`${serialized.slice(-1)}\n`)

    assert.equal((await first.response).status, 200)
    assert.equal((await shutdown).forced, false)
  })

  it('propagates a client disconnect into the active request', async (testContext) => {
    const { application, origin } = await startServer(testContext)
    const first = openStreamingRequest(origin)
    first.request.write('{"id":"partial"')
    await waitFor(() => application.metrics.snapshot().requestsActive === 1)
    first.request.destroy()
    await first.response.catch(() => undefined)
    await waitFor(() => application.metrics.snapshot().requestAbortsTotal === 1)
    assert.equal(application.metrics.snapshot().requestsActive, 0)
  })
})

async function startServer(
  testContext: TestContext,
  overrides: Partial<AppConfig> = {},
): Promise<{ readonly application: ReliabilityServer; readonly origin: string }> {
  const application = new ReliabilityServer({ config: configuration(overrides) })
  const address = await application.start()
  testContext.after(() => application.shutdown('test-cleanup'))
  return { application, origin: address.origin }
}

function configuration(overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    ...loadConfig({}),
    host: '127.0.0.1',
    port: 0,
    workerCount: 1,
    workerQueueSize: 2,
    maxConcurrentRequests: 1,
    perRequestConcurrency: 1,
    fingerprintRounds: 4,
    shutdownGracePeriodMs: 500,
    ...overrides,
  }
}

function event(index: number): Record<string, unknown> {
  return {
    id: `event-${index}`,
    type: 'order.updated',
    occurredAt: '2026-07-20T06:00:00.000Z',
    payload: { index, source: 'integration-test' },
  }
}

function postNdjson(origin: string, body: string): Promise<Response> {
  return fetch(`${origin}/v1/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-ndjson' },
    body,
  })
}

function openStreamingRequest(origin: string): {
  readonly request: ClientRequest
  readonly response: Promise<{ readonly status: number; readonly body: string }>
} {
  const request = httpRequest(`${origin}/v1/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-ndjson' },
  })
  const response = new Promise<{ readonly status: number; readonly body: string }>(
    (resolve, reject) => {
      request.once('response', (incoming) => {
        const chunks: Buffer[] = []
        incoming.on('data', (chunk: Buffer) => chunks.push(chunk))
        incoming.once('end', () => {
          resolve({
            status: incoming.statusCode ?? 0,
            body: Buffer.concat(chunks).toString('utf8'),
          })
        })
        incoming.once('error', reject)
      })
      request.once('error', reject)
    },
  )
  return { request, response }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 2_000; attempt += 1) {
    if (predicate()) return
    await new Promise<void>((resolve) => setImmediate(resolve))
  }
  throw new Error('Condition was not reached')
}
