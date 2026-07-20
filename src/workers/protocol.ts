import type { RequestContext } from '../context/request-context.js'

export interface WorkerTaskMessage<TInput> {
  readonly kind: 'run'
  readonly taskId: string
  readonly input: TInput
  readonly context?: RequestContext
  readonly cancellationBuffer: SharedArrayBuffer
}

export interface WorkerSuccessMessage<TOutput> {
  readonly kind: 'result'
  readonly taskId: string
  readonly ok: true
  readonly output: TOutput
}

export interface SerializedWorkerError {
  readonly name: string
  readonly message: string
  readonly code: string
  readonly stack?: string
}

export interface WorkerFailureMessage {
  readonly kind: 'result'
  readonly taskId: string
  readonly ok: false
  readonly error: SerializedWorkerError
}

export type WorkerResultMessage<TOutput> = WorkerSuccessMessage<TOutput> | WorkerFailureMessage

export interface WorkerTaskRuntime {
  readonly context?: RequestContext
  isCancelled(): boolean
  throwIfCancelled(): void
}
