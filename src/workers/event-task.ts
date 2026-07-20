import { createHash } from 'node:crypto'

import { ReliabilityError } from '../errors.js'
import type { WorkerTaskRuntime } from './protocol.js'

export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }

export interface EventTaskInput {
  readonly index: number
  readonly value: unknown
  readonly fingerprintRounds: number
}

export interface EventTaskOutput {
  readonly index: number
  readonly eventId: string
  readonly fingerprint: string
  readonly normalizedBytes: number
}

interface IngestEvent {
  readonly id: string
  readonly type: string
  readonly occurredAt: string
  readonly payload: JsonValue
}

export function processEventTask(
  input: EventTaskInput,
  runtime: WorkerTaskRuntime,
): EventTaskOutput {
  if (!Number.isInteger(input.fingerprintRounds) || input.fingerprintRounds <= 0) {
    throw new ReliabilityError('fingerprintRounds must be positive', 'invalid_worker_input')
  }
  if (input.fingerprintRounds > 100_000) {
    throw new ReliabilityError('fingerprintRounds is too large', 'invalid_worker_input')
  }

  const event = validateEvent(input.value)
  const normalized = canonicalize({
    id: event.id,
    type: event.type,
    occurredAt: event.occurredAt,
    payload: event.payload,
  })
  let digest = Buffer.from(normalized)
  for (let round = 0; round < input.fingerprintRounds; round += 1) {
    if (round % 32 === 0) runtime.throwIfCancelled()
    digest = createHash('sha256').update(digest).digest()
  }
  runtime.throwIfCancelled()

  return {
    index: input.index,
    eventId: event.id,
    fingerprint: digest.toString('hex'),
    normalizedBytes: Buffer.byteLength(normalized),
  }
}

function validateEvent(value: unknown): IngestEvent {
  if (!isRecord(value)) throw invalidEvent('an event must be a JSON object')
  const id = requiredString(value, 'id', 128)
  const type = requiredString(value, 'type', 128)
  const occurredAt = requiredString(value, 'occurredAt', 64)
  if (Number.isNaN(Date.parse(occurredAt)))
    throw invalidEvent('occurredAt must be an ISO timestamp')
  if (!('payload' in value)) throw invalidEvent('payload is required')
  assertJsonValue(value.payload, 0, { nodes: 0 })
  return { id, type, occurredAt, payload: value.payload as JsonValue }
}

function requiredString(value: Record<string, unknown>, key: string, maximum: number): string {
  const candidate = value[key]
  if (typeof candidate !== 'string' || candidate.length === 0 || candidate.length > maximum) {
    throw invalidEvent(`${key} must be a non-empty string with at most ${maximum} characters`)
  }
  return candidate
}

function assertJsonValue(value: unknown, depth: number, counter: { nodes: number }): void {
  if (depth > 32) throw invalidEvent('payload nesting exceeds 32 levels')
  counter.nodes += 1
  if (counter.nodes > 20_000) throw invalidEvent('payload contains too many values')
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw invalidEvent('payload numbers must be finite')
    return
  }
  if (Array.isArray(value)) {
    for (const entry of value) assertJsonValue(entry, depth + 1, counter)
    return
  }
  if (isRecord(value)) {
    for (const entry of Object.values(value)) assertJsonValue(entry, depth + 1, counter)
    return
  }
  throw invalidEvent('payload must contain JSON-compatible values')
}

function canonicalize(value: JsonValue): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map((entry) => canonicalize(entry)).join(',')}]`
  const properties = Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalize(value[key] as JsonValue)}`)
  return `{${properties.join(',')}}`
}

function invalidEvent(message: string): ReliabilityError {
  return new ReliabilityError(message, 'invalid_event')
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
