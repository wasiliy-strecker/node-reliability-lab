import { abortReason } from '../errors.js'

export interface MapConcurrentOptions {
  readonly concurrency: number
  readonly signal?: AbortSignal
}

type SettledResult<TResult> =
  { readonly ok: true; readonly value: TResult } | { readonly ok: false; readonly error: unknown }

export async function* mapConcurrent<TInput, TOutput>(
  source: AsyncIterable<TInput>,
  mapper: (input: TInput, index: number, signal: AbortSignal) => Promise<TOutput> | TOutput,
  options: MapConcurrentOptions,
): AsyncGenerator<TOutput> {
  if (!Number.isInteger(options.concurrency) || options.concurrency <= 0) {
    throw new RangeError('concurrency must be a positive integer')
  }

  const controller = new AbortController()
  const combinedSignal = options.signal
    ? AbortSignal.any([options.signal, controller.signal])
    : controller.signal
  const iterator = source[Symbol.asyncIterator]()
  const running = new Map<number, Promise<SettledResult<TOutput>>>()
  let sourceEnded = false
  let inputIndex = 0
  let outputIndex = 0

  const fill = async (): Promise<void> => {
    while (!sourceEnded && running.size < options.concurrency) {
      if (combinedSignal.aborted) throw abortReason(combinedSignal)
      const next = await iterator.next()
      if (next.done) {
        sourceEnded = true
        return
      }

      const currentIndex = inputIndex
      inputIndex += 1
      const execution = Promise.resolve()
        .then(() => {
          if (combinedSignal.aborted) throw abortReason(combinedSignal)
          return mapper(next.value, currentIndex, combinedSignal)
        })
        .then<SettledResult<TOutput>>((value) => ({ ok: true, value }))
        .catch<SettledResult<TOutput>>((error: unknown) => ({ ok: false, error }))
      running.set(currentIndex, execution)
    }
  }

  try {
    while (!sourceEnded || running.size > 0) {
      await fill()
      const execution = running.get(outputIndex)
      if (!execution) break

      const result = await execution
      running.delete(outputIndex)
      outputIndex += 1
      if (!result.ok) throw result.error
      yield result.value
    }
  } finally {
    controller.abort(new Error('Concurrent map stopped'))
    await iterator.return?.()
    await Promise.all(running.values())
  }
}
