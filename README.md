# Thread-pool

A small, header-light thread pool for C++17. You hand it a callable and its
arguments, it runs the work on a fixed set of worker threads, and you get back a
`std::future` for the result. Exceptions thrown inside a task surface through
that future, and the destructor drains everything that was submitted before it
joins.

The whole thing is a couple hundred lines. It is meant to be read in one sitting
and dropped into a project without a build-system fight.

## Why this exists

Most real workloads spawn far more units of work than there are cores. Creating
a `std::thread` per unit is expensive and unbounded. A pool keeps a fixed number
of threads alive and feeds them from a queue, which is the standard way to cap
concurrency while keeping the threads warm.

## Usage

```cpp
#include "thread_pool.h"

ThreadPool pool(4);

// Fire and forget.
pool.enqueue([] { do_some_work(); });

// Get a result back.
auto answer = pool.enqueue([] { return 42; });
int value = answer.get();              // blocks until ready

// Arguments are forwarded to the callable.
auto sum = pool.enqueue([](int a, int b) { return a + b; }, 10, 20);
assert(sum.get() == 30);

// Exceptions travel through the future.
auto bad = pool.enqueue([]() -> int { throw std::runtime_error("nope"); });
try {
    bad.get();
} catch (const std::runtime_error& e) {
    // handle it here
}
```

The pool does not need an explicit shutdown call. When it goes out of scope the
destructor closes the queue, lets the workers finish the backlog, and joins
them.

## Cancellation

Some work becomes pointless before a worker gets to it - a request times out, a
user navigates away. `enqueue_cancellable` returns a `TaskId` you can act on:

```cpp
ThreadPool pool(4);

TaskId id = pool.enqueue_cancellable([] { expensive_thing(); });

if (pool.cancel(id)) {
    // Cancelled in time: a worker never ran it.
}

switch (pool.get_status(id)) {
    case TaskStatus::Queued:    /* still waiting */      break;
    case TaskStatus::Running:   /* too late to cancel */ break;
    case TaskStatus::Cancelled: /* skipped */            break;
    case TaskStatus::Completed: /* finished */           break;
    case TaskStatus::Failed:    /* threw */              break;
    case TaskStatus::Unknown:   /* no such id */          break;
}
```

`cancel` returns `true` only while the task is still queued. The implementation
avoids scanning the queue: each cancellable task carries an atomic status, and
`cancel` and the worker both try to move it off `Queued` with a single
compare-and-swap. Exactly one wins, so a task is never half-cancelled and never
runs twice. A cancelled task stays in the queue and is skipped when a worker
reaches it, which keeps `cancel` O(1).

One trade-off worth naming: the status table keeps an entry per cancellable task
for the life of the pool, so `get_status` works after a task finishes. A pool
that submits an unbounded stream of cancellable tasks would want that table
pruned or a handle-based API instead.

## Design

Two pieces:

`BlockingQueue<T>` is a mutex plus condition-variable queue. `pop()` blocks while
the queue is empty and open. `close()` flips a flag and wakes every waiter; after
that, `pop()` keeps returning queued items until the queue is empty and only then
reports closure by returning `std::nullopt`. Returning an empty optional instead
of throwing means the worker loop has no exceptions on its shutdown path.

`ThreadPool` owns the queue and the worker threads. `enqueue` is where the
template work happens:

- `std::invoke_result_t<F, Args...>` gives the return type, which becomes the
  future's type.
- `std::bind` pins the arguments to the callable so the queued task takes no
  parameters.
- The task is wrapped in a `std::packaged_task`, which captures either the
  return value or a thrown exception into the future. `packaged_task` is
  move-only, so it lives in a `shared_ptr` that the queued `std::function` can
  copy.

Single queue, no work stealing. Tasks are handed to whichever worker wakes
first, so ordering across threads is not guaranteed. With one worker, tasks run
in submission order.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build        # or: ./build/thread_pool_tests
```

Header-only if you prefer: `include/blocking_queue.h` has no dependencies, and
`ThreadPool` needs `src/thread_pool.cpp` on the link line.

## Tests

`tests/test_thread_pool.cpp` covers the queue and the pool directly: blocking
and non-blocking pops, close-then-drain ordering, argument forwarding, exception
propagation, move-only results, single-thread ordering, destruction draining
pending work, a 100k-task stress run, and concurrent submission from several
threads. The cancellation path adds tests for cancelling a queued task, failing
to cancel a running one, and reading status through to Completed and Failed.

The suite also runs clean under ThreadSanitizer:

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread
cmake --build build-tsan
./build-tsan/thread_pool_tests
```

One detail the exception test has to respect: a plain `future::get()` drops the
caller's reference to the shared state, after which a worker can tear that state
down (including the stored exception object) while the caller is still reading
it. The test holds the state alive with `shared_future` until it is done, which
is the safe pattern when you need to inspect a propagated exception.

## Benchmarks

There are two benchmark binaries. Both submit empty tasks so the timing reflects
pool overhead rather than the work itself, and numbers depend heavily on the
machine, so run them on yours.

`benchmark` sweeps thread counts up to `hardware_concurrency` and reports
throughput, speedup over a single thread, and steady-state dispatch latency:

```bash
./build/benchmark               # defaults to 1,000,000 tasks
./build/benchmark 2000000
```

```
threads   tasks/sec          speedup
1         851375             1.00x
2         ...                ...x
4         ...                ...x

dispatch latency, steady state (us): p50=9.65  p95=11.03  p99=29.08  max=86.24
```

`benchmark_comprehensive` adds a comparison against `std::async(launch::async)`
on the same workload and a measurement of the cancellation path, then writes a
`benchmark_results.csv` you can drop into a report:

```bash
./build/benchmark_comprehensive 1000000
```

```
scenario                   threads  throughput/s     p50_us  p95_us  p99_us
threadpool                 1        1428796          ...     ...     ...
std_async                  0        59750
cancellation_success_pct   1        100
cancel_ops_per_sec         1        14343064
```

The pool reuses its threads, while `std::async` tends to start a new one per
call, which is why the pool is roughly an order of magnitude faster per task on
the same workload. The cancellation rows are measured with the workers held
busy so the queue cannot drain mid-run: every still-queued task cancels (100%),
and `cancel` itself is an O(1) atomic operation, hence the high ops/sec.

Throughput scales with cores until the single queue mutex becomes the
bottleneck. Sharding the queue or moving to a lock-free design would push that
ceiling higher and is the obvious next step if a workload needs it.

## License

MIT. See [LICENSE](LICENSE).
