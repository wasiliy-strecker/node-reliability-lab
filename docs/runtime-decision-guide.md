# Runtime decision guide

## Keep work on the event loop when

- it spends most of its lifetime waiting for sockets, timers, or other asynchronous APIs
- each synchronous continuation is short and bounded
- the dependency already uses libuv or native asynchronous work correctly

Adding a worker to asynchronous I/O introduces serialization and lifecycle cost without creating
more network capacity.

## Use a worker pool when

- profiling shows synchronous CPU work delaying unrelated requests
- inputs and outputs are structured-clone friendly
- concurrency can be bounded from operational capacity
- the task can cooperate with cancellation or tolerate worker termination

Measure the event loop before and after. Worker threads are an isolation tool, not an automatic
speedup.

## Use streaming when

- input size is caller-controlled or routinely large
- records can be handled incrementally
- downstream work has a meaningful capacity limit
- partial failure can be expressed with a record or line location

Buffering may still be simpler for small, trusted, strictly bounded payloads. Do not introduce a
stream pipeline only for style.

## Reject instead of queue when

- callers can retry with backoff
- waiting longer would violate the request deadline
- the process has no durable storage for queued work
- accepted work already consumes all declared capacity

A `503` is more honest than accepting work into an unbounded promise list.

## Introduce a broker when

- work must survive process restarts
- producers and consumers require independent scaling
- retries need durable scheduling
- delivery state must be observable outside one process

The in-memory worker queue in this repository intentionally provides none of those guarantees.

## Preserve order only when it is meaningful

Ordered results simplify deterministic aggregation and audit output. They also create head-of-line
blocking when an early task is slow. Choose completion order for independent jobs where latency is
more important than source order.

`mapConcurrent` preserves order deliberately because the demo creates one deterministic aggregate
fingerprint from an input stream.

## Choose a shutdown deadline from the outer platform

The service grace period must be shorter than the container orchestrator or process manager's
termination window. Reserve time after the application deadline for forced socket closure, worker
termination, and log flushing.
