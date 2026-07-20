# Failure-mode catalog

## Buffering the entire request

**Temptation:** concatenate every chunk, split after `end`, then parse.

**Failure:** memory use grows with caller-controlled input and no downstream signal can slow the
socket.

**Response:** bound total bytes and partial-line bytes, emit an async record stream, and pull only
when the processing window has capacity.

## Treating a concurrency number as backpressure

**Temptation:** create one promise per input and use a semaphore inside each promise.

**Failure:** execution is bounded but the promise queue still grows without a limit.

**Response:** do not pull more source items than the concurrency window can currently represent.

## Running CPU work in an async function

**Temptation:** mark a hashing or parsing function `async` and assume it yields.

**Failure:** synchronous work still monopolizes the event loop and delays unrelated sockets,
timers, and abort handlers.

**Response:** use a fixed worker pool for measurable CPU work. Keep ordinary asynchronous I/O on
the event loop.

## Spawning one worker per task

**Temptation:** create a worker whenever a request arrives.

**Failure:** startup cost and unconstrained isolates move overload from the event loop to memory and
scheduler pressure.

**Response:** own a fixed pool and a finite queue. Reject work when both are occupied.

## Assuming cancellation is forceful

**Temptation:** reject the caller's promise and report the task as stopped.

**Failure:** CPU work may continue and consume capacity after the caller has gone away.

**Response:** distinguish caller settlement from worker completion and use a shared cooperative
flag. Terminate the isolate only during forced shutdown.

## Retrying after a worker crash

**Temptation:** submit the same task automatically to a replacement worker.

**Failure:** the crashed task may already have produced an external side effect.

**Response:** fail the active task, replace the worker, and let a higher layer with idempotency
knowledge decide whether to retry.

## Expecting AsyncLocalStorage to cross isolates

**Temptation:** read the main thread's context inside a worker.

**Failure:** workers have independent JavaScript heaps and async context stores.

**Response:** serialize the minimal correlation context into the worker protocol and install a new
worker-local scope.

## Closing the worker pool before requests drain

**Temptation:** close every resource in parallel after `SIGTERM`.

**Failure:** accepted request streams may still need to submit their remaining bounded work.

**Response:** stop acceptance, drain HTTP, then close workers. Force all resources only after the
shared deadline.

## Waiting forever for keep-alive connections

**Temptation:** call `server.close()` and assume every connection becomes idle immediately.

**Failure:** a request accepted before shutdown may finish on a keep-alive connection after the
initial idle-connection sweep.

**Response:** send `Connection: close` for responses completed while draining and sweep idle
connections again.

## Using benchmark timings as tests

**Temptation:** fail CI when a worker scenario takes a few milliseconds longer.

**Failure:** shared runners, operating systems, and CPU scaling make wall-clock thresholds noisy.

**Response:** tests assert capacity, ordering, cleanup, and completion. Scenario timings remain
observational output.
