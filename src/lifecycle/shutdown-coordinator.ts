import { performance } from 'node:perf_hooks'

import { ShutdownTimeoutError } from '../errors.js'
import { publishDiagnostic } from '../observability/diagnostics.js'

export interface ShutdownResource {
  readonly name: string
  stop?(): Promise<void> | void
  drain?(): Promise<void> | void
  close?(): Promise<void> | void
  force?(reason: Error): Promise<void> | void
}

export interface ShutdownResult {
  readonly reason: string
  readonly forced: boolean
  readonly durationMs: number
}

export interface ShutdownCoordinatorOptions {
  readonly gracePeriodMs?: number
}

interface Deferred<T> {
  readonly promise: Promise<T>
  readonly resolve: (value: T) => void
}

export class ShutdownCoordinator {
  readonly #gracePeriodMs: number
  readonly #resources: ShutdownResource[] = []
  readonly #abortController = new AbortController()
  readonly #forceRequested = deferred<Error>()
  #ready = true
  #shutdownPromise: Promise<ShutdownResult> | undefined

  constructor(options: ShutdownCoordinatorOptions = {}) {
    this.#gracePeriodMs = options.gracePeriodMs ?? 10_000
    if (!Number.isInteger(this.#gracePeriodMs) || this.#gracePeriodMs < 0) {
      throw new RangeError('gracePeriodMs must not be negative')
    }
  }

  get signal(): AbortSignal {
    return this.#abortController.signal
  }

  get isReady(): boolean {
    return this.#ready
  }

  register(resource: ShutdownResource): void {
    if (!this.#ready || this.#shutdownPromise) {
      throw new Error('Resources cannot be registered after shutdown starts')
    }
    if (this.#resources.some((candidate) => candidate.name === resource.name)) {
      throw new Error(`Shutdown resource ${resource.name} is already registered`)
    }
    this.#resources.push(resource)
  }

  shutdown(reason: string): Promise<ShutdownResult> {
    this.#shutdownPromise ??= this.#run(reason)
    return this.#shutdownPromise
  }

  force(reason: string): Promise<ShutdownResult> {
    const shutdown = this.shutdown(reason)
    this.#forceRequested.resolve(new Error(`Forced shutdown requested: ${reason}`))
    return shutdown
  }

  async #run(reason: string): Promise<ShutdownResult> {
    const startedAt = performance.now()
    this.#ready = false
    publishDiagnostic({
      type: 'shutdown.started',
      reason,
      gracePeriodMs: this.#gracePeriodMs,
    })

    const timeout = deferred<Error>()
    const timer = setTimeout(
      () => timeout.resolve(new ShutdownTimeoutError(this.#gracePeriodMs)),
      this.#gracePeriodMs,
    )

    const graceful = this.#runGracefulPhases().then(
      () => ({ type: 'graceful' as const }),
      (error: unknown) => ({ type: 'failed' as const, error: toError(error) }),
    )
    const forced = Promise.race([this.#forceRequested.promise, timeout.promise]).then((error) => ({
      type: 'forced' as const,
      error,
    }))
    const outcome = await Promise.race([graceful, forced])
    clearTimeout(timer)

    let wasForced = false
    if (outcome.type !== 'graceful') {
      wasForced = true
      this.#abortController.abort(outcome.error)
      await Promise.allSettled(
        this.#resources.map((resource) => Promise.resolve(resource.force?.(outcome.error))),
      )
    }

    const result = {
      reason,
      forced: wasForced,
      durationMs: performance.now() - startedAt,
    }
    publishDiagnostic({ type: 'shutdown.completed', ...result })
    return result
  }

  async #runGracefulPhases(): Promise<void> {
    await runPhase(this.#resources, 'stop')
    await runPhase(this.#resources, 'drain')
    await runPhase(this.#resources, 'close')
  }
}

async function runPhase(
  resources: ShutdownResource[],
  phase: 'stop' | 'drain' | 'close',
): Promise<void> {
  const results = await Promise.allSettled(
    resources.map((resource) => Promise.resolve(resource[phase]?.())),
  )
  const failures = results.flatMap((result, index) =>
    result.status === 'rejected'
      ? [
          new Error(`${resources[index]?.name ?? 'unknown'} failed during ${phase}`, {
            cause: result.reason,
          }),
        ]
      : [],
  )
  if (failures.length > 0) throw new AggregateError(failures, `Shutdown ${phase} phase failed`)
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
