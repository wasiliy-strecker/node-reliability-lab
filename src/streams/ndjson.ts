import { abortReason, NdjsonLimitError, NdjsonSyntaxError } from '../errors.js'
import { TextDecoder } from 'node:util'

export interface NdjsonRecord<T = unknown> {
  readonly lineNumber: number
  readonly byteLength: number
  readonly value: T
}

export interface NdjsonLimits {
  readonly maxBodyBytes?: number
  readonly maxLineBytes?: number
  readonly maxRecords?: number
  readonly signal?: AbortSignal
}

const defaults = {
  maxBodyBytes: 10 * 1024 * 1024,
  maxLineBytes: 64 * 1024,
  maxRecords: 10_000,
} as const

export async function* decodeNdjson<T = unknown>(
  source: AsyncIterable<Uint8Array | string>,
  limits: NdjsonLimits = {},
): AsyncGenerator<NdjsonRecord<T>> {
  const maxBodyBytes = positiveInteger(limits.maxBodyBytes ?? defaults.maxBodyBytes, 'maxBodyBytes')
  const maxLineBytes = positiveInteger(limits.maxLineBytes ?? defaults.maxLineBytes, 'maxLineBytes')
  const maxRecords = positiveInteger(limits.maxRecords ?? defaults.maxRecords, 'maxRecords')
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let buffered = Buffer.alloc(0)
  let totalBytes = 0
  let recordCount = 0
  let lineNumber = 1

  for await (const rawChunk of source) {
    throwIfAborted(limits.signal)
    const chunk = typeof rawChunk === 'string' ? Buffer.from(rawChunk) : Buffer.from(rawChunk)
    totalBytes += chunk.byteLength
    if (totalBytes > maxBodyBytes) {
      throw new NdjsonLimitError('body_bytes', maxBodyBytes, totalBytes)
    }

    buffered = buffered.byteLength === 0 ? chunk : Buffer.concat([buffered, chunk])
    let newlineIndex = buffered.indexOf(0x0a)

    while (newlineIndex >= 0) {
      const rawLine = buffered.subarray(0, newlineIndex)
      buffered = buffered.subarray(newlineIndex + 1)
      const line = withoutCarriageReturn(rawLine)

      if (line.byteLength > maxLineBytes) {
        throw new NdjsonLimitError('line_bytes', maxLineBytes, line.byteLength, lineNumber)
      }

      if (line.byteLength > 0) {
        recordCount += 1
        if (recordCount > maxRecords) {
          throw new NdjsonLimitError('records', maxRecords, recordCount, lineNumber)
        }
        yield parseLine<T>(line, lineNumber, decoder)
      }

      lineNumber += 1
      throwIfAborted(limits.signal)
      newlineIndex = buffered.indexOf(0x0a)
    }

    const bufferedLineBytes = withoutCarriageReturn(buffered).byteLength
    if (bufferedLineBytes > maxLineBytes) {
      throw new NdjsonLimitError('line_bytes', maxLineBytes, bufferedLineBytes, lineNumber)
    }
  }

  throwIfAborted(limits.signal)
  const finalLine = withoutCarriageReturn(buffered)
  if (finalLine.byteLength === 0) return

  recordCount += 1
  if (recordCount > maxRecords) {
    throw new NdjsonLimitError('records', maxRecords, recordCount, lineNumber)
  }
  yield parseLine<T>(finalLine, lineNumber, decoder)
}

function parseLine<T>(line: Buffer, lineNumber: number, decoder: TextDecoder): NdjsonRecord<T> {
  let text: string
  try {
    text = decoder.decode(line)
  } catch (error) {
    throw new NdjsonSyntaxError(lineNumber, 'the line is not valid UTF-8', { cause: error })
  }

  try {
    return {
      lineNumber,
      byteLength: line.byteLength,
      value: JSON.parse(text) as T,
    }
  } catch (error) {
    throw new NdjsonSyntaxError(lineNumber, 'the line is not valid JSON', { cause: error })
  }
}

function withoutCarriageReturn(line: Buffer): Buffer {
  return line.at(-1) === 0x0d ? line.subarray(0, -1) : line
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) throw new RangeError(`${name} must be positive`)
  return value
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw abortReason(signal)
}
