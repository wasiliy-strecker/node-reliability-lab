import { AsyncResource } from 'node:async_hooks'
import { randomUUID } from 'node:crypto'
import { performance } from 'node:perf_hooks'
import { Worker } from 'node:worker_threads'

import type { RequestContext } from '../context/request-context.js'
import {
  abortReason,
  OperationAbortedError,
  OverloadedError,
  PoolClosedError,
  RemoteWorkerError,
  ShutdownTimeoutError,
  WorkerCrashedError,
} from '../errors.js'
import { publishDiagnostic } from '../observability/diagnostics.js'
import type { WorkerResultMessage, WorkerTaskMessage } from './protocol.js'

export interface WorkerPoolOptions {
  readonly workerUrl: URL
  readonly size: number
  readonly maxQueue: number
  readonly name?: string
  readonly workerData?: unknown
}

export interface WorkerRunOptions {
  readonly signal?: AbortSignal
  readonly context?: RequestContext
}

export interface WorkerPoolStats {
  readonly state: 'accepting' | 'closing' | 'closed'
  readonly configuredWorkers: number
  readonly liveWorkers: number
  readonly active: number
  readonly queued: number
  readonly submitted: number
  readonly started: number
  readonly completed: number
  readonly failed: number
  readonly aborted: number
  readonly rejectedOverload: number
  readonly rejectedClosed: number
  readonly workerCrashes: number
}

interface Task<TInput, TOutput> {
  readonly id: string
  readonly input: TInput
  readonly context?: RequestContext
  readonly enqueuedAt: number
  readonly cancellation: Int32Array
  readonly resource: AsyncResource
  readonly resolve: (output: TOutput) => void
  readonly reject: (error: unknown) => void
  readonly signal?: AbortSignal
  abortListener?: () => void
  startedAt?: number
  settled: boolean
}

interface WorkerSlot<TInput, TOutput> {
  readonly id: number
  readonly worker: Worker
  task?: Task<TInput, TOutput>
  failure?: Error
}

interface MutableCounters {
  submitted: number
  started: number
  completed: number
  failed: number
  aborted: number
  rejectedOverload: number
  rejectedClosed: number
  workerCrashes: number
}

interface Deferred<T> {
  readonly promise: Promise<T>
  readonly resolve: (value: T) => void
}

export class BoundedWorkerPool<TInput, TOutput> {
  readonly #workerUrl: URL
  readonly #configuredWorkers: number
  readonly #maxQueue: number
  readonly #name: string
  readonly #workerData: unknown
  readonly #slots: Array<WorkerSlot<TInput, TOutput>> = []
  readonly #pending: Array<Task<TInput, TOutput>> = []
  readonly #counters: MutableCounters = {
    submitted: 0,
    started: 0,
    completed: 0,
    failed: 0,
    aborted: 0,
    rejectedOverload: 0,
    rejectedClosed: 0,
    workerCrashes: 0,
  }
  #nextWorkerId = 1
  #state: WorkerPoolStats['state'] = 'accepting'
  #closeDeferred: Deferred<void> | undefined
  #terminating = false

  constructor(options: WorkerPoolOptions) {
    assertNonNegativeInteger(options.maxQueue, 'maxQueue')
    assertPositiveInteger(options.size, 'size')
    this.#workerUrl = options.workerUrl
    this.#configuredWorkers = options.size
    this.#maxQueue = options.maxQueue
    this.#name = options.name ?? 'bounded-worker-pool'
    this.#workerData = options.workerData

    for (let index = 0; index < this.#configuredWorkers; index += 1) this.#spawnWorker()
  }

  run(input: TInput, options: WorkerRunOptions = {}): Promise<TOutput> {
    const requestId = options.context?.requestId
    if (this.#state !== 'accepting') {
      this.#counters.rejectedClosed += 1
      publishDiagnostic({
        type: 'worker.task.rejected',
        reason: 'closed',
        ...(requestId ? { requestId } : {}),
      })
      return Promise.reject(new PoolClosedError())
    }

    const occupied = this.#activeCount() + this.#pending.length
    if (occupied >= this.#configuredWorkers + this.#maxQueue) {
      this.#counters.rejectedOverload += 1
      publishDiagnostic({
        type: 'worker.task.rejected',
        reason: 'overloaded',
        ...(requestId ? { requestId } : {}),
      })
      return Promise.reject(new OverloadedError())
    }

    if (options.signal?.aborted) return Promise.reject(abortReason(options.signal))

    this.#counters.submitted += 1
    return new Promise<TOutput>((resolve, reject) => {
      const task: Task<TInput, TOutput> = {
        id: randomUUID(),
        input,
        ...(options.context ? { context: options.context } : {}),
        enqueuedAt: performance.now(),
        cancellation: new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)),
        resource: new AsyncResource(`${this.#name}.task`),
        resolve,
        reject,
        ...(options.signal ? { signal: options.signal } : {}),
        settled: false,
      }

      if (task.signal) {
        task.abortListener = () => this.#abortTask(task)
        task.signal.addEventListener('abort', task.abortListener, { once: true })
      }

      const idle = this.#slots.find((slot) => !slot.task)
      if (idle) this.#dispatch(idle, task)
      else this.#pending.push(task)
    })
  }

  stats(): WorkerPoolStats {
    return {
      state: this.#state,
      configuredWorkers: this.#configuredWorkers,
      liveWorkers: this.#slots.length,
      active: this.#activeCount(),
      queued: this.#pending.length,
      ...this.#counters,
    }
  }

  async close(options: { readonly gracePeriodMs?: number } = {}): Promise<void> {
    if (this.#state === 'closed') return
    const gracePeriodMs = options.gracePeriodMs ?? 5_000
    assertNonNegativeInteger(gracePeriodMs, 'gracePeriodMs')
    if (this.#closeDeferred) {
      if (gracePeriodMs === 0) this.#forceClose(new ShutdownTimeoutError(gracePeriodMs))
      return this.#closeDeferred.promise
    }

    this.#state = 'closing'
    this.#closeDeferred = deferred<void>()
    this.#drainQueue()
    this.#finishCloseWhenDrained()

    let timer: NodeJS.Timeout | undefined
    if (gracePeriodMs > 0) {
      timer = setTimeout(
        () => this.#forceClose(new ShutdownTimeoutError(gracePeriodMs)),
        gracePeriodMs,
      )
    } else {
      this.#forceClose(new ShutdownTimeoutError(gracePeriodMs))
    }

    try {
      await this.#closeDeferred.promise
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  #spawnWorker(): void {
    const worker = new Worker(this.#workerUrl, {
      name: `${this.#name}-${this.#nextWorkerId}`,
      ...(this.#workerData === undefined ? {} : { workerData: this.#workerData }),
    })
    const slot: WorkerSlot<TInput, TOutput> = {
      id: this.#nextWorkerId,
      worker,
    }
    this.#nextWorkerId += 1
    this.#slots.push(slot)

    worker.on('message', (message: WorkerResultMessage<TOutput>) =>
      this.#handleResult(slot, message),
    )
    worker.once('error', (error) => {
      slot.failure = error
    })
    worker.once('exit', (exitCode) => this.#handleExit(slot, exitCode))
  }

  #dispatch(slot: WorkerSlot<TInput, TOutput>, task: Task<TInput, TOutput>): void {
    if (task.signal?.aborted) {
      this.#abortTask(task)
      this.#drainQueue()
      return
    }

    slot.task = task
    task.startedAt = performance.now()
    this.#counters.started += 1
    publishDiagnostic({
      type: 'worker.task.started',
      taskId: task.id,
      workerId: slot.id,
      ...(task.context ? { requestId: task.context.requestId } : {}),
      queuedForMs: task.startedAt - task.enqueuedAt,
    })

    const message: WorkerTaskMessage<TInput> = {
      kind: 'run',
      taskId: task.id,
      input: task.input,
      ...(task.context ? { context: task.context } : {}),
      cancellationBuffer: task.cancellation.buffer as SharedArrayBuffer,
    }
    slot.worker.postMessage(message)
  }

  #handleResult(slot: WorkerSlot<TInput, TOutput>, message: WorkerResultMessage<TOutput>): void {
    const task = slot.task
    if (!task || task.id !== message.taskId) return
    delete slot.task
    this.#removeAbortListener(task)

    if (!task.settled) {
      const durationMs = performance.now() - (task.startedAt ?? task.enqueuedAt)
      if (message.ok) {
        this.#counters.completed += 1
        publishDiagnostic({
          type: 'worker.task.completed',
          taskId: task.id,
          workerId: slot.id,
          ...(task.context ? { requestId: task.context.requestId } : {}),
          durationMs,
        })
        this.#resolveTask(task, message.output)
      } else {
        this.#counters.failed += 1
        publishDiagnostic({
          type: 'worker.task.failed',
          taskId: task.id,
          workerId: slot.id,
          ...(task.context ? { requestId: task.context.requestId } : {}),
          code: message.error.code,
          durationMs,
        })
        this.#rejectTask(
          task,
          new RemoteWorkerError(
            message.error.message,
            message.error.code,
            message.error.name,
            message.error.stack,
          ),
        )
      }
    }

    task.resource.emitDestroy()
    this.#drainQueue()
    this.#finishCloseWhenDrained()
  }

  #handleExit(slot: WorkerSlot<TInput, TOutput>, exitCode: number): void {
    const index = this.#slots.indexOf(slot)
    if (index >= 0) this.#slots.splice(index, 1)
    if (this.#terminating) return

    this.#counters.workerCrashes += 1
    publishDiagnostic({ type: 'worker.crashed', workerId: slot.id, exitCode })
    const task = slot.task
    if (task && !task.settled) {
      this.#counters.failed += 1
      this.#removeAbortListener(task)
      this.#rejectTask(task, new WorkerCrashedError(slot.id, { cause: slot.failure }))
      task.resource.emitDestroy()
    }

    if (this.#state !== 'closed' && (this.#state === 'accepting' || this.#pending.length > 0)) {
      this.#spawnWorker()
      this.#drainQueue()
    }
    this.#finishCloseWhenDrained()
  }

  #abortTask(task: Task<TInput, TOutput>): void {
    if (task.settled) return
    Atomics.store(task.cancellation, 0, 1)
    const pendingIndex = this.#pending.indexOf(task)
    const wasPending = pendingIndex >= 0
    if (pendingIndex >= 0) {
      this.#pending.splice(pendingIndex, 1)
      this.#removeAbortListener(task)
    }

    this.#counters.failed += 1
    this.#counters.aborted += 1
    const workerId = this.#slots.find((slot) => slot.task === task)?.id
    if (workerId !== undefined) {
      publishDiagnostic({
        type: 'worker.task.failed',
        taskId: task.id,
        workerId,
        ...(task.context ? { requestId: task.context.requestId } : {}),
        code: 'operation_aborted',
        durationMs: performance.now() - (task.startedAt ?? task.enqueuedAt),
      })
    }
    this.#rejectTask(task, task.signal ? abortReason(task.signal) : new OperationAbortedError())
    if (wasPending) task.resource.emitDestroy()
    this.#drainQueue()
    this.#finishCloseWhenDrained()
  }

  #resolveTask(task: Task<TInput, TOutput>, output: TOutput): void {
    if (task.settled) return
    task.settled = true
    task.resource.runInAsyncScope(task.resolve, undefined, output)
  }

  #rejectTask(task: Task<TInput, TOutput>, error: unknown): void {
    if (task.settled) return
    task.settled = true
    task.resource.runInAsyncScope(task.reject, undefined, error)
  }

  #removeAbortListener(task: Task<TInput, TOutput>): void {
    if (task.signal && task.abortListener) {
      task.signal.removeEventListener('abort', task.abortListener)
      delete task.abortListener
    }
  }

  #drainQueue(): void {
    for (const slot of this.#slots) {
      if (slot.task) continue
      const task = this.#pending.shift()
      if (!task) break
      this.#dispatch(slot, task)
    }
  }

  #activeCount(): number {
    return this.#slots.filter((slot) => slot.task !== undefined).length
  }

  #finishCloseWhenDrained(): void {
    if (this.#state !== 'closing' || this.#activeCount() > 0 || this.#pending.length > 0) return
    this.#terminateWorkers()
  }

  #forceClose(error: Error): void {
    if (this.#state === 'closed') return
    for (const task of this.#pending.splice(0)) {
      this.#removeAbortListener(task)
      this.#counters.failed += 1
      this.#rejectTask(task, error)
      task.resource.emitDestroy()
    }
    for (const slot of this.#slots) {
      const task = slot.task
      if (!task) continue
      Atomics.store(task.cancellation, 0, 1)
      this.#removeAbortListener(task)
      if (!task.settled) {
        this.#counters.failed += 1
        this.#counters.aborted += 1
        this.#rejectTask(task, error)
        task.resource.emitDestroy()
      }
      delete slot.task
    }
    this.#terminateWorkers()
  }

  #terminateWorkers(): void {
    if (this.#terminating) return
    this.#terminating = true
    const workers = this.#slots.splice(0).map((slot) => slot.worker)
    void Promise.allSettled(workers.map((worker) => worker.terminate())).then(() => {
      this.#state = 'closed'
      this.#closeDeferred?.resolve(undefined)
    })
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) throw new RangeError(`${name} must be positive`)
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0) throw new RangeError(`${name} must not be negative`)
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}
