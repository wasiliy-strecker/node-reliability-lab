import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { NdjsonLimitError, NdjsonSyntaxError, OperationAbortedError } from '../src/errors.js'
import { decodeNdjson } from '../src/streams/ndjson.js'
import { mapConcurrent } from '../src/streams/map-concurrent.js'

describe('decodeNdjson', () => {
  it('decodes records across arbitrary chunks, CRLF, blanks, and a final line', async () => {
    const bytes = Buffer.from('{"id":1}\r\n\n{"id":2}\n{"id":3}')
    const records = await collect(
      decodeNdjson<{ readonly id: number }>(chunks(bytes, [2, 1, 7, 3, 5])),
    )

    assert.deepEqual(
      records.map((record) => ({ line: record.lineNumber, id: record.value.id })),
      [
        { line: 1, id: 1 },
        { line: 3, id: 2 },
        { line: 4, id: 3 },
      ],
    )
  })

  it('reports the physical line containing invalid JSON', async () => {
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from('\n{"ok":true}\nnot-json\n'), [4]))),
      (error: unknown) => error instanceof NdjsonSyntaxError && error.lineNumber === 3,
    )
  })

  it('rejects invalid UTF-8', async () => {
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from([0xff, 0x0a]), [2]))),
      (error: unknown) => error instanceof NdjsonSyntaxError && error.lineNumber === 1,
    )
  })

  it('enforces body, line, and record limits independently', async () => {
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from('{"value":1}\n'), [20]), { maxBodyBytes: 5 })),
      (error: unknown) => error instanceof NdjsonLimitError && error.limit === 'body_bytes',
    )
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from('{"value":1}\n'), [20]), { maxLineBytes: 5 })),
      (error: unknown) => error instanceof NdjsonLimitError && error.limit === 'line_bytes',
    )
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from('{}\n{}\n'), [20]), { maxRecords: 1 })),
      (error: unknown) => error instanceof NdjsonLimitError && error.limit === 'records',
    )
  })

  it('stops before reading when its signal is aborted', async () => {
    const controller = new AbortController()
    controller.abort(new OperationAbortedError('test abort'))
    await assert.rejects(
      collect(decodeNdjson(chunks(Buffer.from('{}\n'), [3]), { signal: controller.signal })),
      /test abort/u,
    )
  })
})

describe('mapConcurrent', () => {
  it('preserves order and bounds both execution and source advancement', async () => {
    let produced = 0
    let completed = 0
    let active = 0
    let maxActive = 0
    let maxAhead = 0

    async function* source(): AsyncGenerator<number> {
      for (let value = 0; value < 12; value += 1) {
        produced += 1
        maxAhead = Math.max(maxAhead, produced - completed)
        yield value
      }
    }

    const output = await collect(
      mapConcurrent(
        source(),
        async (value) => {
          active += 1
          maxActive = Math.max(maxActive, active)
          await new Promise<void>((resolve) => setImmediate(resolve))
          active -= 1
          completed += 1
          return value * 2
        },
        { concurrency: 3 },
      ),
    )

    assert.deepEqual(
      output,
      Array.from({ length: 12 }, (_, index) => index * 2),
    )
    assert.equal(maxActive, 3)
    assert.equal(maxAhead, 3)
  })

  it('closes its source and aborts sibling work after a mapper failure', async () => {
    let sourceClosed = false
    let siblingObservedAbort = false
    async function* source(): AsyncGenerator<number> {
      try {
        yield 0
        yield 1
        yield 2
      } finally {
        sourceClosed = true
      }
    }

    await assert.rejects(
      collect(
        mapConcurrent(
          source(),
          async (value, _index, signal) => {
            if (value === 1) throw new Error('controlled mapper failure')
            await new Promise<void>((resolve) => setImmediate(resolve))
            siblingObservedAbort ||= signal.aborted
            return value
          },
          { concurrency: 3 },
        ),
      ),
      /controlled mapper failure/u,
    )
    assert.equal(sourceClosed, true)
    assert.equal(siblingObservedAbort, true)
  })

  it('validates its concurrency setting', async () => {
    await assert.rejects(
      collect(mapConcurrent(emptyAsyncIterable(), async () => 1, { concurrency: 0 })),
      /concurrency must be a positive integer/u,
    )
  })
})

async function* chunks(bytes: Buffer, sizes: number[]): AsyncGenerator<Uint8Array> {
  let offset = 0
  let index = 0
  while (offset < bytes.byteLength) {
    const size = sizes[index % sizes.length] ?? 1
    yield bytes.subarray(offset, offset + size)
    offset += size
    index += 1
  }
}

async function collect<T>(source: AsyncIterable<T>): Promise<T[]> {
  const output: T[] = []
  for await (const value of source) output.push(value)
  return output
}

async function* emptyAsyncIterable(): AsyncGenerator<never> {
  yield* []
}
