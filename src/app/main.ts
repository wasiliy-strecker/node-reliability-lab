import { loadConfig } from './config.js'
import { ReliabilityServer } from './server.js'
import { subscribeDiagnostics } from '../observability/diagnostics.js'

const writeLog = (record: Readonly<Record<string, unknown>>): void => {
  process.stdout.write(`${JSON.stringify({ timestamp: new Date().toISOString(), ...record })}\n`)
}

const unsubscribe = subscribeDiagnostics((event) => writeLog({ level: 'info', ...event }))
let application: ReliabilityServer | undefined

try {
  const runningApplication = new ReliabilityServer({ config: loadConfig(), logger: writeLog })
  application = runningApplication
  const address = await runningApplication.start()
  writeLog({ level: 'info', event: 'server.started', ...address })

  let signalCount = 0
  const handleSignal = (signal: NodeJS.Signals): void => {
    signalCount += 1
    const shutdown =
      signalCount === 1
        ? runningApplication.shutdown(signal)
        : runningApplication.forceShutdown(`${signal}:second-signal`)
    void shutdown.then((result) => {
      writeLog({ level: 'info', event: 'server.stopped', ...result })
      unsubscribe()
      process.exitCode = result.forced ? 1 : 0
    })
  }

  process.on('SIGINT', handleSignal)
  process.on('SIGTERM', handleSignal)
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
