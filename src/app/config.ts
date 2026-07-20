import { availableParallelism } from 'node:os'

export interface AppConfig {
  readonly host: string
  readonly port: number
  readonly workerCount: number
  readonly workerQueueSize: number
  readonly maxConcurrentRequests: number
  readonly perRequestConcurrency: number
  readonly fingerprintRounds: number
  readonly maxBodyBytes: number
  readonly maxLineBytes: number
  readonly maxRecords: number
  readonly shutdownGracePeriodMs: number
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): AppConfig {
  const workerCount = integerFromEnvironment(
    environment.WORKER_COUNT,
    Math.max(1, Math.min(4, availableParallelism() - 1)),
    1,
    32,
    'WORKER_COUNT',
  )
  const workerQueueSize = integerFromEnvironment(
    environment.WORKER_QUEUE_SIZE,
    workerCount * 2,
    0,
    10_000,
    'WORKER_QUEUE_SIZE',
  )
  const maxConcurrentRequests = integerFromEnvironment(
    environment.MAX_CONCURRENT_REQUESTS,
    2,
    1,
    1_000,
    'MAX_CONCURRENT_REQUESTS',
  )
  const perRequestConcurrency = integerFromEnvironment(
    environment.PER_REQUEST_CONCURRENCY,
    Math.min(2, workerCount),
    1,
    32,
    'PER_REQUEST_CONCURRENCY',
  )

  if (maxConcurrentRequests * perRequestConcurrency > workerCount + workerQueueSize) {
    throw new Error(
      'MAX_CONCURRENT_REQUESTS multiplied by PER_REQUEST_CONCURRENCY must fit the worker pool and queue',
    )
  }

  return {
    host: environment.HOST?.trim() || '127.0.0.1',
    port: integerFromEnvironment(environment.PORT, 3000, 0, 65_535, 'PORT'),
    workerCount,
    workerQueueSize,
    maxConcurrentRequests,
    perRequestConcurrency,
    fingerprintRounds: integerFromEnvironment(
      environment.FINGERPRINT_ROUNDS,
      256,
      1,
      100_000,
      'FINGERPRINT_ROUNDS',
    ),
    maxBodyBytes: integerFromEnvironment(
      environment.MAX_BODY_BYTES,
      10 * 1024 * 1024,
      1,
      1024 * 1024 * 1024,
      'MAX_BODY_BYTES',
    ),
    maxLineBytes: integerFromEnvironment(
      environment.MAX_LINE_BYTES,
      64 * 1024,
      1,
      10 * 1024 * 1024,
      'MAX_LINE_BYTES',
    ),
    maxRecords: integerFromEnvironment(
      environment.MAX_RECORDS,
      10_000,
      1,
      10_000_000,
      'MAX_RECORDS',
    ),
    shutdownGracePeriodMs: integerFromEnvironment(
      environment.SHUTDOWN_GRACE_PERIOD_MS,
      10_000,
      0,
      300_000,
      'SHUTDOWN_GRACE_PERIOD_MS',
    ),
  }
}

function integerFromEnvironment(
  raw: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
  name: string,
): number {
  const value = raw === undefined || raw.trim() === '' ? fallback : Number(raw)
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`)
  }
  return value
}
