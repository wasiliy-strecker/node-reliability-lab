import { parentPort } from 'node:worker_threads'

import { requestContext } from '../context/request-context.js'
import { OperationAbortedError, ReliabilityError } from '../errors.js'
import type {
  SerializedWorkerError,
  WorkerResultMessage,
  WorkerTaskMessage,
  WorkerTaskRuntime,
} from './protocol.js'

export type WorkerHandler<TInput, TOutput> = (
  input: TInput,
  runtime: WorkerTaskRuntime,
) => Promise<TOutput> | TOutput

export function startWorker<TInput, TOutput>(handler: WorkerHandler<TInput, TOutput>): void {
  const port = parentPort
  if (!port) throw new Error('A worker runtime needs a parent port')

  port.on('message', (message: WorkerTaskMessage<TInput>) => {
    void executeAndPost(message, handler, port).catch((error: unknown) => {
      process.nextTick(() => {
        throw error
      })
    })
  })
}

async function executeAndPost<TInput, TOutput>(
  message: WorkerTaskMessage<TInput>,
  handler: WorkerHandler<TInput, TOutput>,
  port: NonNullable<typeof parentPort>,
): Promise<void> {
  const result = await execute(message, handler)
  try {
    port.postMessage(result)
  } catch (error) {
    port.postMessage({
      kind: 'result',
      taskId: message.taskId,
      ok: false,
      error: serializeError(
        new ReliabilityError(
          'The worker result could not be cloned',
          'worker_output_not_cloneable',
          {
            cause: error,
          },
        ),
      ),
    } satisfies WorkerResultMessage<TOutput>)
  }
}

async function execute<TInput, TOutput>(
  message: WorkerTaskMessage<TInput>,
  handler: WorkerHandler<TInput, TOutput>,
): Promise<WorkerResultMessage<TOutput>> {
  const cancellation = new Int32Array(message.cancellationBuffer)
  const runtime: WorkerTaskRuntime = {
    ...(message.context ? { context: message.context } : {}),
    isCancelled: () => Atomics.load(cancellation, 0) === 1,
    throwIfCancelled: () => {
      if (Atomics.load(cancellation, 0) === 1) throw new OperationAbortedError()
    },
  }

  try {
    const executeHandler = (): Promise<TOutput> => Promise.resolve(handler(message.input, runtime))
    const output = message.context
      ? await requestContext.run(message.context, executeHandler)
      : await executeHandler()
    return { kind: 'result', taskId: message.taskId, ok: true, output }
  } catch (error) {
    return {
      kind: 'result',
      taskId: message.taskId,
      ok: false,
      error: serializeError(error),
    }
  }
}

function serializeError(error: unknown): SerializedWorkerError {
  if (error instanceof ReliabilityError) {
    return {
      name: error.name,
      message: error.message,
      code: error.code,
      ...(error.stack ? { stack: error.stack } : {}),
    }
  }
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      code: 'worker_task_failed',
      ...(error.stack ? { stack: error.stack } : {}),
    }
  }
  return {
    name: 'Error',
    message: String(error),
    code: 'worker_task_failed',
  }
}
