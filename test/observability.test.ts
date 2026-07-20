import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { RuntimeProbe } from '../src/observability/runtime-probe.js'

describe('RuntimeProbe', () => {
  it('returns finite event-loop observations and supports reset', async () => {
    const probe = new RuntimeProbe({ resolutionMs: 1 })
    probe.start()
    await new Promise<void>((resolve) => setImmediate(resolve))
    const first = probe.snapshot()
    assert.equal(Object.values(first).every(Number.isFinite), true)
    probe.reset()
    await new Promise<void>((resolve) => setImmediate(resolve))
    const second = probe.snapshot()
    assert.equal(Object.values(second).every(Number.isFinite), true)
    probe.stop()
    assert.throws(() => probe.snapshot(), /must be started/u)
  })

  it('validates resolution and treats repeated start and stop as idempotent', () => {
    assert.throws(() => new RuntimeProbe({ resolutionMs: 0 }), /must be a positive integer/u)
    const probe = new RuntimeProbe()
    probe.start()
    probe.start()
    probe.stop()
    probe.stop()
  })
})
