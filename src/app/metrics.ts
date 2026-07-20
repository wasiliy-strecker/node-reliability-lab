import type { RuntimeSnapshot } from '../observability/runtime-probe.js'
import type { WorkerPoolStats } from '../workers/bounded-worker-pool.js'

export interface ServiceMetricsSnapshot {
  readonly requestsTotal: number
  readonly requestsActive: number
  readonly requestFailuresTotal: number
  readonly requestAbortsTotal: number
  readonly requestDurationSecondsTotal: number
  readonly eventsProcessedTotal: number
  readonly normalizedBytesTotal: number
}

export class ServiceMetrics {
  #requestsTotal = 0
  #requestsActive = 0
  #requestFailuresTotal = 0
  #requestAbortsTotal = 0
  #requestDurationSecondsTotal = 0
  #eventsProcessedTotal = 0
  #normalizedBytesTotal = 0

  requestStarted(): void {
    this.#requestsTotal += 1
    this.#requestsActive += 1
  }

  requestFinished(statusCode: number, durationMs: number): void {
    this.#requestsActive = Math.max(0, this.#requestsActive - 1)
    if (statusCode >= 400) this.#requestFailuresTotal += 1
    this.#requestDurationSecondsTotal += durationMs / 1000
  }

  requestAborted(): void {
    this.#requestAbortsTotal += 1
  }

  eventsProcessed(count: number, normalizedBytes: number): void {
    this.#eventsProcessedTotal += count
    this.#normalizedBytesTotal += normalizedBytes
  }

  snapshot(): ServiceMetricsSnapshot {
    return {
      requestsTotal: this.#requestsTotal,
      requestsActive: this.#requestsActive,
      requestFailuresTotal: this.#requestFailuresTotal,
      requestAbortsTotal: this.#requestAbortsTotal,
      requestDurationSecondsTotal: this.#requestDurationSecondsTotal,
      eventsProcessedTotal: this.#eventsProcessedTotal,
      normalizedBytesTotal: this.#normalizedBytesTotal,
    }
  }

  renderPrometheus(pool: WorkerPoolStats, runtime: RuntimeSnapshot, ready: boolean): string {
    const service = this.snapshot()
    const values: ReadonlyArray<readonly [string, string, number]> = [
      ['node_reliability_ready', 'Whether the service accepts ingestion requests', ready ? 1 : 0],
      ['node_reliability_requests_total', 'HTTP requests observed', service.requestsTotal],
      [
        'node_reliability_requests_active',
        'HTTP requests currently active',
        service.requestsActive,
      ],
      [
        'node_reliability_request_failures_total',
        'HTTP requests completed with an error status',
        service.requestFailuresTotal,
      ],
      [
        'node_reliability_request_aborts_total',
        'Requests aborted by a client or shutdown',
        service.requestAbortsTotal,
      ],
      [
        'node_reliability_request_duration_seconds_total',
        'Accumulated HTTP request duration',
        service.requestDurationSecondsTotal,
      ],
      [
        'node_reliability_events_processed_total',
        'NDJSON events processed successfully',
        service.eventsProcessedTotal,
      ],
      [
        'node_reliability_normalized_bytes_total',
        'Canonical event bytes processed successfully',
        service.normalizedBytesTotal,
      ],
      ['node_reliability_worker_active', 'Worker tasks currently active', pool.active],
      ['node_reliability_worker_queued', 'Worker tasks currently queued', pool.queued],
      [
        'node_reliability_worker_rejected_total',
        'Worker tasks rejected at capacity',
        pool.rejectedOverload,
      ],
      ['node_reliability_worker_crashes_total', 'Unexpected worker exits', pool.workerCrashes],
      [
        'node_reliability_event_loop_utilization',
        'Event loop utilization since the runtime probe started',
        runtime.eventLoopUtilization,
      ],
      [
        'node_reliability_event_loop_delay_p99_seconds',
        'Observed p99 event loop delay',
        runtime.delayP99Ms / 1000,
      ],
    ]

    return `${values
      .flatMap(([name, help, value]) => [
        `# HELP ${name} ${help}`,
        `# TYPE ${name} gauge`,
        `${name} ${finite(value)}`,
      ])
      .join('\n')}\n`
  }
}

function finite(value: number): number {
  return Number.isFinite(value) ? value : 0
}
