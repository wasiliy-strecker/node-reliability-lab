import {
  monitorEventLoopDelay,
  performance,
  type EventLoopUtilization,
  type IntervalHistogram,
} from 'node:perf_hooks'

export interface RuntimeSnapshot {
  readonly eventLoopUtilization: number
  readonly delayMeanMs: number
  readonly delayMaxMs: number
  readonly delayP50Ms: number
  readonly delayP99Ms: number
}

export interface RuntimeProbeOptions {
  readonly resolutionMs?: number
}

const nanosecondsPerMillisecond = 1_000_000

export class RuntimeProbe {
  readonly #resolutionMs: number
  #histogram: IntervalHistogram | undefined
  #baseline: EventLoopUtilization | undefined

  constructor(options: RuntimeProbeOptions = {}) {
    this.#resolutionMs = options.resolutionMs ?? 10
    if (!Number.isInteger(this.#resolutionMs) || this.#resolutionMs <= 0) {
      throw new RangeError('resolutionMs must be a positive integer')
    }
  }

  start(): void {
    if (this.#histogram) return
    this.#histogram = monitorEventLoopDelay({ resolution: this.#resolutionMs })
    this.#baseline = performance.eventLoopUtilization()
    this.#histogram.enable()
  }

  snapshot(): RuntimeSnapshot {
    if (!this.#histogram || !this.#baseline) {
      throw new Error('RuntimeProbe must be started before taking a snapshot')
    }

    const utilization = performance.eventLoopUtilization(this.#baseline)
    return {
      eventLoopUtilization: finite(utilization.utilization),
      delayMeanMs: toMilliseconds(this.#histogram.mean),
      delayMaxMs: toMilliseconds(this.#histogram.max),
      delayP50Ms: toMilliseconds(this.#histogram.percentile(50)),
      delayP99Ms: toMilliseconds(this.#histogram.percentile(99)),
    }
  }

  reset(): void {
    if (!this.#histogram) throw new Error('RuntimeProbe must be started before resetting it')
    this.#histogram.reset()
    this.#baseline = performance.eventLoopUtilization()
  }

  stop(): void {
    this.#histogram?.disable()
    this.#histogram = undefined
    this.#baseline = undefined
  }
}

function toMilliseconds(nanoseconds: number): number {
  return finite(nanoseconds / nanosecondsPerMillisecond)
}

function finite(value: number): number {
  return Number.isFinite(value) ? value : 0
}
