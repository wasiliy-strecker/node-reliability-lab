import { processEventTask, type EventTaskInput, type EventTaskOutput } from './event-task.js'
import { startWorker } from './worker-runtime.js'

startWorker<EventTaskInput, EventTaskOutput>(processEventTask)
