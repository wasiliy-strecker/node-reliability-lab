import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { loadConfig } from '../src/app/config.js'

describe('loadConfig', () => {
  it('parses explicit bounded runtime settings', () => {
    const config = loadConfig({
      HOST: '0.0.0.0',
      PORT: '0',
      WORKER_COUNT: '2',
      WORKER_QUEUE_SIZE: '4',
      MAX_CONCURRENT_REQUESTS: '2',
      PER_REQUEST_CONCURRENCY: '2',
      FINGERPRINT_ROUNDS: '64',
      MAX_BODY_BYTES: '4096',
      MAX_LINE_BYTES: '1024',
      MAX_RECORDS: '20',
      SHUTDOWN_GRACE_PERIOD_MS: '2500',
    })

    assert.deepEqual(config, {
      host: '0.0.0.0',
      port: 0,
      workerCount: 2,
      workerQueueSize: 4,
      maxConcurrentRequests: 2,
      perRequestConcurrency: 2,
      fingerprintRounds: 64,
      maxBodyBytes: 4096,
      maxLineBytes: 1024,
      maxRecords: 20,
      shutdownGracePeriodMs: 2500,
    })
  })

  it('rejects malformed values and contradictory admission capacity', () => {
    assert.throws(() => loadConfig({ PORT: 'not-a-number' }), /PORT must be an integer/u)
    assert.throws(
      () =>
        loadConfig({
          WORKER_COUNT: '1',
          WORKER_QUEUE_SIZE: '0',
          MAX_CONCURRENT_REQUESTS: '2',
          PER_REQUEST_CONCURRENCY: '1',
        }),
      /must fit the worker pool and queue/u,
    )
  })
})
