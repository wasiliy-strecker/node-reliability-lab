import { AsyncLocalStorage } from 'node:async_hooks'

export interface RequestContext {
  readonly requestId: string
  readonly startedAt: number
  readonly taskId?: string
}

export class RequestContextStore<TContext extends object> {
  readonly #storage = new AsyncLocalStorage<TContext>()

  run<TResult>(context: TContext, operation: () => TResult): TResult {
    return this.#storage.run(context, operation)
  }

  current(): TContext | undefined {
    return this.#storage.getStore()
  }

  require(): TContext {
    const context = this.current()
    if (!context) throw new Error('No request context is active')
    return context
  }

  bind<TArguments extends unknown[], TResult>(
    callback: (...arguments_: TArguments) => TResult,
  ): (...arguments_: TArguments) => TResult {
    const runInCapturedContext = AsyncLocalStorage.snapshot()
    return (...arguments_: TArguments): TResult => runInCapturedContext(callback, ...arguments_)
  }
}

export const requestContext = new RequestContextStore<RequestContext>()
