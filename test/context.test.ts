import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { RequestContextStore } from '../src/context/request-context.js'

describe('RequestContextStore', () => {
  it('isolates overlapping asynchronous operations', async () => {
    const store = new RequestContextStore<{ readonly requestId: string }>()
    const firstMayContinue = deferred<void>()
    const secondHasStarted = deferred<void>()

    const first = store.run({ requestId: 'first' }, async () => {
      await secondHasStarted.promise
      assert.equal(store.require().requestId, 'first')
      firstMayContinue.resolve(undefined)
      await Promise.resolve()
      assert.equal(store.require().requestId, 'first')
    })
    const second = store.run({ requestId: 'second' }, async () => {
      secondHasStarted.resolve(undefined)
      await firstMayContinue.promise
      assert.equal(store.require().requestId, 'second')
    })

    await Promise.all([first, second])
    assert.equal(store.current(), undefined)
  })

  it('binds a callback to the captured context', () => {
    const store = new RequestContextStore<{ readonly requestId: string }>()
    const bound = store.run({ requestId: 'captured' }, () =>
      store.bind((suffix: string) => `${store.require().requestId}:${suffix}`),
    )

    assert.equal(store.current(), undefined)
    assert.equal(bound('done'), 'captured:done')
  })

  it('fails explicitly when context is required outside a scope', () => {
    const store = new RequestContextStore<object>()
    assert.throws(() => store.require(), /No request context is active/u)
  })
})

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}
