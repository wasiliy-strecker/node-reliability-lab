import { setImmediate as waitForImmediate } from 'node:timers/promises'

import { loadConfig, type AppConfig } from '../app/config.js'
import { ReliabilityServer } from '../app/server.js'
import { decodeNdjson } from '../streams/ndjson.js'
import { mapConcurrent } from '../streams/map-concurrent.js'
import { BoundedWorkerPool } from '../workers/bounded-worker-pool.js'
import {
  processEventTask,
  type EventTaskInput,
  type EventTaskOutput,
} from '../workers/event-task.js'

const smoke = process.argv.includes('--smoke')

await workerIsolationScenario(smoke)
await backpressureScenario(smoke)
await serviceLifecycleScenario(smoke)

async function workerIsolationScenario(isSmoke: boolean): Promise<void> {
  const rounds = isSmoke ? 64 : 8_000
  const tasks = isSmoke ? 2 : 8
  const input = (index: number): EventTaskInput => ({
    index,
    value: sampleEvent(index),
    fingerprintRounds: rounds,
  })
  const runtime = {
    isCancelled: () => false,
    throwIfCancelled: () => undefined,
  }

  let mainThreadHeartbeats = 0
  const mainHeartbeat = setInterval(() => {
    mainThreadHeartbeats += 1
  }, 1)
  for (let index = 0; index < tasks; index += 1) processEventTask(input(index), runtime)
  clearInterval(mainHeartbeat)

  const pool = new BoundedWorkerPool<EventTaskInput, EventTaskOutput>({
    workerUrl: new URL('../workers/event-worker.js', import.meta.url),
    size: 2,
    maxQueue: Math.max(0, tasks - 2),
    name: 'scenario-worker',
  })
  let workerHeartbeats = 0
  const workerHeartbeat = setInterval(() => {
    workerHeartbeats += 1
  }, 1)
  await Promise.all(Array.from({ length: tasks }, (_, index) => pool.run(input(index))))
  clearInterval(workerHeartbeat)
  await pool.close()

  report({
    scenario: 'worker-isolation',
    tasks,
    rounds,
    mainThreadHeartbeats,
    workerHeartbeats,
    invariant: 'all tasks completed and the worker pool remained bounded',
  })
}

async function backpressureScenario(isSmoke: boolean): Promise<void> {
  const count = isSmoke ? 8 : 100
  const concurrency = 3
  let produced = 0
  let completed = 0
  let active = 0
  let maxActive = 0
  let maxAhead = 0

  async function* chunks(): AsyncGenerator<Uint8Array> {
    const payload = Array.from({ length: count }, (_, index) =>
      JSON.stringify(sampleEvent(index)),
    ).join('\n')
    const bytes = Buffer.from(`${payload}\n`)
    const chunkSizes = [7, 19, 3, 41]
    let offset = 0
    let chunkIndex = 0
    while (offset < bytes.byteLength) {
      const size = chunkSizes[chunkIndex % chunkSizes.length] ?? 7
      yield bytes.subarray(offset, offset + size)
      offset += size
      chunkIndex += 1
    }
  }

  async function* observedSource(): AsyncGenerator<unknown> {
    for await (const record of decodeNdjson(chunks())) {
      produced += 1
      maxAhead = Math.max(maxAhead, produced - completed)
      yield record.value
    }
  }

  for await (const _value of mapConcurrent(
    observedSource(),
    async (value) => {
      active += 1
      maxActive = Math.max(maxActive, active)
      await waitForImmediate()
      active -= 1
      completed += 1
      return value
    },
    { concurrency },
  )) {
    // Consumption is intentionally empty. Counters prove the bounded behavior.
  }

  if (completed !== count || maxActive > concurrency || maxAhead > concurrency) {
    throw new Error('Backpressure scenario violated its concurrency bounds')
  }
  report({
    scenario: 'stream-backpressure',
    records: count,
    concurrency,
    maxActive,
    maxAhead,
    invariant: 'the source never advanced beyond the configured processing window',
  })
}

async function serviceLifecycleScenario(isSmoke: boolean): Promise<void> {
  const config: AppConfig = {
    ...loadConfig({}),
    port: 0,
    workerCount: 1,
    workerQueueSize: 2,
    maxConcurrentRequests: 1,
    perRequestConcurrency: 1,
    fingerprintRounds: isSmoke ? 8 : 256,
    shutdownGracePeriodMs: 2_000,
  }
  const application = new ReliabilityServer({ config })
  const address = await application.start()
  const response = await fetch(`${address.origin}/v1/events`, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-ndjson',
      'x-request-id': 'scenario-request',
    },
    body: `${JSON.stringify(sampleEvent(1))}\n${JSON.stringify(sampleEvent(2))}\n`,
  })
  const result = (await response.json()) as { readonly processed?: number }
  const shutdown = await application.shutdown('scenario-complete')
  if (response.status !== 200 || result.processed !== 2 || shutdown.forced) {
    throw new Error('Service lifecycle scenario did not complete gracefully')
  }
  report({
    scenario: 'service-lifecycle',
    status: response.status,
    processed: result.processed,
    forced: shutdown.forced,
    invariant: 'the real HTTP service processed data and released every owned resource',
  })
}

function sampleEvent(index: number): Record<string, unknown> {
  return {
    id: `evt-${index}`,
    type: index % 2 === 0 ? 'order.created' : 'order.updated',
    occurredAt: '2026-07-20T06:00:00.000Z',
    payload: { index, source: 'node-reliability-lab' },
  }
}

function report(value: Readonly<Record<string, unknown>>): void {
  process.stdout.write(`${JSON.stringify(value)}\n`)
}
