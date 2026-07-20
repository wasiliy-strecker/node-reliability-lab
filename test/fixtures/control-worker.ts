import { requestContext } from '../../src/context/request-context.js'
import { ReliabilityError } from '../../src/errors.js'
import type { WorkerTaskRuntime } from '../../src/workers/protocol.js'
import { startWorker } from '../../src/workers/worker-runtime.js'
import type { ControlWorkerInput, ControlWorkerOutput } from './control-types.js'

startWorker<ControlWorkerInput, ControlWorkerOutput>(handle)

function handle(input: ControlWorkerInput, runtime: WorkerTaskRuntime): ControlWorkerOutput {
  switch (input.kind) {
    case 'echo':
      return { value: input.value }
    case 'context': {
      const currentContext = requestContext.current()
      return {
        value: 'context',
        ...(currentContext ? { requestId: currentContext.requestId } : {}),
      }
    }
    case 'fail':
      throw new ReliabilityError('Controlled worker failure', 'controlled_failure')
    case 'crash':
      return process.exit(23)
    case 'hold': {
      const gate = new Int32Array(input.gate)
      while (Atomics.load(gate, 0) === 0) {
        runtime.throwIfCancelled()
        Atomics.wait(gate, 0, 0, 25)
      }
      return { value: input.value }
    }
    case 'spin': {
      let value = 0
      for (let index = 0; index < input.iterations; index += 1) {
        if (index % 1_024 === 0) runtime.throwIfCancelled()
        value = (value + Math.imul(index, 31)) >>> 0
      }
      runtime.throwIfCancelled()
      return { value: String(value) }
    }
  }
}
