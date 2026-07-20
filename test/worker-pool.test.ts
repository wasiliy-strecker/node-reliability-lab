import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import type { RequestContext } from '../src/context/request-context.js'
import {
  OperationAbortedError,
  OverloadedError,
  PoolClosedError,
  RemoteWorkerError,
  ShutdownTimeoutError,
  WorkerCrashedError,
} from '../src/errors.js'
import { subscribeDiagnostics, type DiagnosticEvent } from '../src/observability/diagnostics.js'
import { BoundedWorkerPool } from '../src/workers/bounded-worker-pool.js'
import type { ControlWorkerInput, ControlWorkerOutput } from './fixtures/control-types.js'

describe('BoundedWorkerPool', () => {
  it('bounds active work and its queue, then reports overload', async (testContext) => {
    const pool = createPool({ size: 1, maxQueue: 1 })
    testContext.after(() => pool.close({ gracePeriodMs: 0 }))
    const gate = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
    const started = waitForDiagnostic(
      (event) => event.type === 'worker.task.started' && event.requestId === 'first',
    )
    const first = pool.run({ kind: 'hold', gate, value: 'first' }, { context: context('first') })
    await started.promise
    started.unsubscribe()
    const second = pool.run({ kind: 'echo', value: 'second' })

    await assert.rejects(pool.run({ kind: 'echo', value: 'third' }), OverloadedError)
    assert.deepEqual(
      { active: pool.stats().active, queued: pool.stats().queued },
      { active: 1, queued: 1 },
    )

    releaseGate(gate)
    assert.equal((await first).value, 'first')
    assert.equal((await second).value, 'second')
    assert.equal(pool.stats().rejectedOverload, 1)
  })

  it('propagates request context across the worker boundary', async (testContext) => {
    const pool = createPool()
    testContext.after(() => pool.close())
    const output = await pool.run({ kind: 'context' }, { context: context('request-42') })
    assert.deepEqual(output, { value: 'context', requestId: 'request-42' })
  })

  it('preserves a typed remote worker failure', async (testContext) => {
    const pool = createPool()
    testContext.after(() => pool.close())
    await assert.rejects(
      pool.run({ kind: 'fail' }),
      (error: unknown) =>
        error instanceof RemoteWorkerError &&
        error.code === 'controlled_failure' &&
        error.remoteName === 'ReliabilityError',
    )
  })

  it('cancels queued work without disturbing the active task', async (testContext) => {
    const pool = createPool({ size: 1, maxQueue: 1 })
    testContext.after(() => pool.close({ gracePeriodMs: 0 }))
    const gate = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
    const firstStarted = waitForDiagnostic((event) => event.type === 'worker.task.started')
    const first = pool.run({ kind: 'hold', gate, value: 'active' })
    await firstStarted.promise
    firstStarted.unsubscribe()

    const controller = new AbortController()
    const queued = pool.run({ kind: 'echo', value: 'queued' }, { signal: controller.signal })
    controller.abort(new OperationAbortedError('queued abort'))
    await assert.rejects(queued, /queued abort/u)
    assert.equal(pool.stats().queued, 0)

    releaseGate(gate)
    assert.equal((await first).value, 'active')
  })

  it('cooperatively cancels CPU work already running in a worker', async (testContext) => {
    const pool = createPool()
    testContext.after(() => pool.close({ gracePeriodMs: 0 }))
    const controller = new AbortController()
    const started = waitForDiagnostic((event) => event.type === 'worker.task.started')
    const running = pool.run(
      { kind: 'spin', iterations: 1_000_000_000 },
      { signal: controller.signal },
    )
    await started.promise
    started.unsubscribe()
    controller.abort(new OperationAbortedError('active abort'))

    await assert.rejects(running, /active abort/u)
    await waitFor(() => pool.stats().active === 0)
    assert.equal(pool.stats().aborted, 1)
  })

  it('rejects a crashed task and replaces the worker without retrying it', async (testContext) => {
    const pool = createPool()
    testContext.after(() => pool.close())
    await assert.rejects(pool.run({ kind: 'crash' }), WorkerCrashedError)
    assert.equal(pool.stats().workerCrashes, 1)
    assert.equal((await pool.run({ kind: 'echo', value: 'replacement' })).value, 'replacement')
  })

  it('drains admitted work before closing and rejects later submissions', async () => {
    const pool = createPool()
    const gate = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
    const started = waitForDiagnostic((event) => event.type === 'worker.task.started')
    const running = pool.run({ kind: 'hold', gate, value: 'drained' })
    await started.promise
    started.unsubscribe()
    const closing = pool.close({ gracePeriodMs: 1_000 })
    await assert.rejects(pool.run({ kind: 'echo', value: 'late' }), PoolClosedError)
    releaseGate(gate)

    assert.equal((await running).value, 'drained')
    await closing
    assert.equal(pool.stats().state, 'closed')
    assert.equal(pool.stats().liveWorkers, 0)
  })

  it('terminates uncooperative work when the close deadline is zero', async () => {
    const pool = createPool()
    const gate = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
    const started = waitForDiagnostic((event) => event.type === 'worker.task.started')
    const running = pool.run({ kind: 'hold', gate, value: 'never' })
    await started.promise
    started.unsubscribe()

    const closing = pool.close({ gracePeriodMs: 0 })
    await assert.rejects(running, ShutdownTimeoutError)
    await closing
    assert.equal(pool.stats().state, 'closed')
  })
})

function createPool(
  overrides: Partial<{ readonly size: number; readonly maxQueue: number }> = {},
): BoundedWorkerPool<ControlWorkerInput, ControlWorkerOutput> {
  return new BoundedWorkerPool({
    workerUrl: new URL('./fixtures/control-worker.js', import.meta.url),
    size: overrides.size ?? 1,
    maxQueue: overrides.maxQueue ?? 2,
    name: 'control-test',
  })
}

function context(requestId: string): RequestContext {
  return { requestId, startedAt: performance.now() }
}

function releaseGate(buffer: SharedArrayBuffer): void {
  const gate = new Int32Array(buffer)
  Atomics.store(gate, 0, 1)
  Atomics.notify(gate, 0)
}

function waitForDiagnostic(predicate: (event: DiagnosticEvent) => boolean): {
  readonly promise: Promise<DiagnosticEvent>
  readonly unsubscribe: () => void
} {
  let unsubscribe = (): void => undefined
  const promise = new Promise<DiagnosticEvent>((resolve) => {
    unsubscribe = subscribeDiagnostics((event) => {
      if (predicate(event)) resolve(event)
    })
  })
  return { promise, unsubscribe: () => unsubscribe() }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise<void>((resolve) => setTimeout(resolve, 1))
  }
  throw new Error('Condition was not reached')
}
