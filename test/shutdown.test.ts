import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { ShutdownCoordinator } from '../src/lifecycle/shutdown-coordinator.js'

describe('ShutdownCoordinator', () => {
  it('runs stop, drain, and close as distinct phases', async () => {
    const events: string[] = []
    const coordinator = new ShutdownCoordinator({ gracePeriodMs: 1_000 })
    coordinator.register({
      name: 'first',
      stop: () => {
        events.push('first.stop')
      },
      drain: () => {
        events.push('first.drain')
      },
      close: () => {
        events.push('first.close')
      },
    })
    coordinator.register({
      name: 'second',
      stop: () => {
        events.push('second.stop')
      },
      drain: () => {
        events.push('second.drain')
      },
      close: () => {
        events.push('second.close')
      },
    })

    const result = await coordinator.shutdown('test')
    assert.equal(coordinator.isReady, false)
    assert.equal(result.forced, false)
    assert.ok(events.indexOf('first.stop') < events.indexOf('first.drain'))
    assert.ok(events.indexOf('second.drain') < events.indexOf('second.close'))
    assert.deepEqual(await coordinator.shutdown('ignored'), result)
  })

  it('aborts and forces resources after the grace period', async () => {
    const never = new Promise<void>(() => undefined)
    let forcedWith: Error | undefined
    const coordinator = new ShutdownCoordinator({ gracePeriodMs: 10 })
    coordinator.register({
      name: 'blocked',
      drain: () => never,
      force: (reason) => {
        forcedWith = reason
      },
    })

    const result = await coordinator.shutdown('deadline-test')
    assert.equal(result.forced, true)
    assert.equal(coordinator.signal.aborted, true)
    assert.match(forcedWith?.message ?? '', /Graceful shutdown exceeded 10 ms/u)
  })

  it('supports an explicit forced second signal', async () => {
    const never = new Promise<void>(() => undefined)
    let forced = false
    const coordinator = new ShutdownCoordinator({ gracePeriodMs: 60_000 })
    coordinator.register({
      name: 'blocked',
      drain: () => never,
      force: () => {
        forced = true
      },
    })

    const first = coordinator.shutdown('SIGTERM')
    const second = coordinator.force('SIGTERM:second-signal')
    assert.strictEqual(first, second)
    assert.equal((await second).forced, true)
    assert.equal(forced, true)
  })

  it('rejects duplicate and late resource registration', async () => {
    const coordinator = new ShutdownCoordinator()
    coordinator.register({ name: 'one' })
    assert.throws(() => coordinator.register({ name: 'one' }), /already registered/u)
    await coordinator.shutdown('done')
    assert.throws(() => coordinator.register({ name: 'late' }), /after shutdown starts/u)
  })
})
