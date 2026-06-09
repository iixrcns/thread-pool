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
threads.

The suite also runs clean under ThreadSanitizer:

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread
cmake --build build-tsan
./build-tsan/thread_pool_tests
```

## Benchmarks

`bench/benchmark.cpp` submits empty tasks (so the timing reflects pool overhead,
not the work itself), sweeps thread counts up to `hardware_concurrency`, and
reports throughput, speedup over a single thread, and steady-state dispatch
latency.

```bash
./build/benchmark               # defaults to 1,000,000 tasks
./build/benchmark 2000000       # or pass your own count
```

Numbers depend heavily on the machine, so run it on yours. Output looks like:

```
hardware_concurrency = 12
tasks per run        = 2000000

threads   tasks/sec          speedup
1         679209             1.00x
2         910313             1.34x
4         812082             1.20x
8         503392             0.74x
12        542120             0.80x

dispatch latency, steady state (us): p50=1.54  p95=4.20  p99=8.21  max=82.19

```

Throughput scales with cores until the queue mutex becomes the bottleneck. A
single global lock is the limiting factor at high core counts; sharding the
queue or moving to a lock-free design would push that ceiling higher and is the
obvious next step if the workload needs it.

## License

MIT. See [LICENSE](LICENSE).
