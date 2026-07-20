import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { it } from 'node:test'

it('drains the real process after SIGTERM', { timeout: 10_000 }, async (testContext) => {
  const child = spawn(process.execPath, ['--enable-source-maps', 'dist/src/app/main.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      HOST: '127.0.0.1',
      PORT: '0',
      WORKER_COUNT: '1',
      WORKER_QUEUE_SIZE: '1',
      MAX_CONCURRENT_REQUESTS: '1',
      PER_REQUEST_CONCURRENCY: '1',
      FINGERPRINT_ROUNDS: '4',
      SHUTDOWN_GRACE_PERIOD_MS: '1000',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  testContext.after(() => {
    if (child.exitCode === null) child.kill('SIGKILL')
  })
  const errors: Buffer[] = []
  child.stderr.on('data', (chunk: Buffer) => errors.push(chunk))
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]()
  const started = await nextRecord(lines, (record) => record.event === 'server.started')
  assert.equal(typeof started.port, 'number')

  const exited = new Promise<{
    readonly code: number | null
    readonly signal: NodeJS.Signals | null
  }>((resolve) => {
    child.once('exit', (code, signal) => resolve({ code, signal }))
  })
  assert.equal(child.kill('SIGTERM'), true)
  const stopped = await nextRecord(lines, (record) => record.event === 'server.stopped')
  const exit = await exited

  assert.equal(stopped.forced, false)
  assert.deepEqual(exit, { code: 0, signal: null })
  assert.equal(Buffer.concat(errors).toString('utf8'), '')
})

async function nextRecord(
  lines: AsyncIterator<string>,
  predicate: (record: Record<string, unknown>) => boolean,
): Promise<Record<string, unknown>> {
  while (true) {
    const next = await lines.next()
    if (next.done) throw new Error('Process output ended before the expected record')
    const record = JSON.parse(next.value) as Record<string, unknown>
    if (predicate(record)) return record
  }
}
