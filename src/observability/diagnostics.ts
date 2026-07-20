import { channel, type Channel } from 'node:diagnostics_channel'

export type DiagnosticEvent =
  | {
      readonly type: 'worker.task.started'
      readonly taskId: string
      readonly workerId: number
      readonly requestId?: string
      readonly queuedForMs: number
    }
  | {
      readonly type: 'worker.task.completed'
      readonly taskId: string
      readonly workerId: number
      readonly requestId?: string
      readonly durationMs: number
    }
  | {
      readonly type: 'worker.task.failed'
      readonly taskId: string
      readonly workerId: number
      readonly requestId?: string
      readonly code: string
      readonly durationMs: number
    }
  | {
      readonly type: 'worker.task.rejected'
      readonly reason: 'closed' | 'overloaded'
      readonly requestId?: string
    }
  | {
      readonly type: 'worker.crashed'
      readonly workerId: number
      readonly exitCode: number
    }
  | {
      readonly type: 'shutdown.started'
      readonly reason: string
      readonly gracePeriodMs: number
    }
  | {
      readonly type: 'shutdown.completed'
      readonly reason: string
      readonly forced: boolean
      readonly durationMs: number
    }

type DiagnosticType = DiagnosticEvent['type']

const channels: Record<DiagnosticType, Channel> = {
  'worker.task.started': channel('node-reliability.worker.task.started'),
  'worker.task.completed': channel('node-reliability.worker.task.completed'),
  'worker.task.failed': channel('node-reliability.worker.task.failed'),
  'worker.task.rejected': channel('node-reliability.worker.task.rejected'),
  'worker.crashed': channel('node-reliability.worker.crashed'),
  'shutdown.started': channel('node-reliability.shutdown.started'),
  'shutdown.completed': channel('node-reliability.shutdown.completed'),
}

export function publishDiagnostic(event: DiagnosticEvent): void {
  channels[event.type].publish(event)
}

export function subscribeDiagnostics(listener: (event: DiagnosticEvent) => void): () => void {
  const subscriptions = Object.values(channels).map((diagnosticChannel) => {
    const handler = (message: unknown): void => listener(message as DiagnosticEvent)
    diagnosticChannel.subscribe(handler)
    return { diagnosticChannel, handler }
  })

  return () => {
    for (const { diagnosticChannel, handler } of subscriptions) {
      diagnosticChannel.unsubscribe(handler)
    }
  }
}
