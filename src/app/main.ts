import { loadConfig } from './config.js'
import { ReliabilityServer } from './server.js'
import { subscribeDiagnostics } from '../observability/diagnostics.js'

const serializeLog = (record: Readonly<Record<string, unknown>>): string =>
  `${JSON.stringify({ timestamp: new Date().toISOString(), ...record })}\n`

const writeLog = (record: Readonly<Record<string, unknown>>): void => {
  process.stdout.write(serializeLog(record))
}

const writeLogAndFlush = (record: Readonly<Record<string, unknown>>): Promise<void> =>
  new Promise((resolve, reject) => {
    process.stdout.write(serializeLog(record), (error) => {
      if (error) reject(error)
      else resolve()
    })
  })

const unsubscribe = subscribeDiagnostics((event) => writeLog({ level: 'info', ...event }))
let application: ReliabilityServer | undefined

try {
  const runningApplication = new ReliabilityServer({ config: loadConfig(), logger: writeLog })
  application = runningApplication
  const address = await runningApplication.start()

  let signalCount = 0
  const handleSignal = (signal: NodeJS.Signals): void => {
    signalCount += 1
    const shutdown =
      signalCount === 1
        ? runningApplication.shutdown(signal)
        : runningApplication.forceShutdown(`${signal}:second-signal`)
    void shutdown
      .then(async (result) => {
        await writeLogAndFlush({ level: 'info', event: 'server.stopped', ...result })
        unsubscribe()
        process.exitCode = result.forced ? 1 : 0
      })
      .catch((error: unknown) => {
        unsubscribe()
        process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`)
        process.exitCode = 1
      })
  }

  process.on('SIGINT', handleSignal)
  process.on('SIGTERM', handleSignal)
  writeLog({ level: 'info', event: 'server.started', ...address })
} catch (error) {
  writeLog({
    level: 'error',
    event: 'server.start_failed',
    error: error instanceof Error ? error.message : String(error),
  })
  await application?.forceShutdown('startup-failed')
  unsubscribe()
  process.exitCode = 1
}
