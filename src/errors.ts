export class ReliabilityError extends Error {
  constructor(
    message: string,
    readonly code: string,
    options?: ErrorOptions,
  ) {
    super(message, options)
    this.name = new.target.name
  }
}

export class OperationAbortedError extends ReliabilityError {
  constructor(message = 'The operation was aborted', options?: ErrorOptions) {
    super(message, 'operation_aborted', options)
  }
}

export class OverloadedError extends ReliabilityError {
  constructor(message = 'The bounded worker queue is full') {
    super(message, 'worker_pool_overloaded')
  }
}

export class PoolClosedError extends ReliabilityError {
  constructor(message = 'The worker pool is no longer accepting tasks') {
    super(message, 'worker_pool_closed')
  }
}

export class WorkerCrashedError extends ReliabilityError {
  constructor(workerId: number, options?: ErrorOptions) {
    super(`Worker ${workerId} exited before completing its task`, 'worker_crashed', options)
  }
}

export class RemoteWorkerError extends ReliabilityError {
  constructor(
    message: string,
    code: string,
    readonly remoteName: string,
    readonly remoteStack?: string,
  ) {
    super(message, code)
  }
}

export class ShutdownTimeoutError extends ReliabilityError {
  constructor(gracePeriodMs: number) {
    super(`Graceful shutdown exceeded ${gracePeriodMs} ms`, 'shutdown_timeout')
  }
}

export type NdjsonLimit = 'body_bytes' | 'line_bytes' | 'records'

export class NdjsonLimitError extends ReliabilityError {
  constructor(
    readonly limit: NdjsonLimit,
    readonly maximum: number,
    readonly actual: number,
    readonly lineNumber?: number,
  ) {
    const location = lineNumber === undefined ? '' : ` at line ${lineNumber}`
    super(`NDJSON ${limit} limit exceeded${location}`, 'ndjson_limit_exceeded')
  }
}

export class NdjsonSyntaxError extends ReliabilityError {
  constructor(
    readonly lineNumber: number,
    message: string,
    options?: ErrorOptions,
  ) {
    super(`Invalid NDJSON at line ${lineNumber}: ${message}`, 'invalid_ndjson', options)
  }
}

export function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason
  if (typeof signal.reason === 'string') return new OperationAbortedError(signal.reason)
  return new OperationAbortedError()
}

export function isAbortError(error: unknown): boolean {
  return (
    error instanceof OperationAbortedError ||
    (error instanceof DOMException && error.name === 'AbortError') ||
    (error instanceof Error && error.name === 'AbortError')
  )
}
