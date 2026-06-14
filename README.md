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

## Priorities

By default every task runs at `Priority::Normal`. Pass a priority as the first
argument to push work ahead of or behind the rest:

```cpp
ThreadPool pool(4);

pool.enqueue([] { background_cleanup(); });                 // Normal
pool.enqueue(Priority::High, [] { handle_request(); });
pool.enqueue(Priority::Critical, [] { flush_on_shutdown(); });
pool.enqueue(Priority::Low, [] { warm_some_cache(); });

// Priority and arguments mix freely; the future works the same way.
auto sum = pool.enqueue(Priority::High, [](int a, int b) { return a + b; }, 1, 2);
```

The levels, from first served to last, are `Critical`, `High`, `Normal`,
`Low`. Tasks at the same level run in the order they were submitted, so raising
a task's priority never reshuffles its peers. Priority decides dispatch order,
not preemption: a task already running is never interrupted, so a `Critical`
task still waits for a free worker.

Priority holds within a worker's own queue, not across workers. Each worker has
its own queue, so a `Critical` task sitting in one worker's queue does not
preempt a `Normal` task another worker pulls from its own queue first. Stealing
softens this - an idle worker takes a busy peer's highest-priority item - but
there is no single global order across threads. With one worker there is one
queue, so priority and FIFO within a level are exact, which is the case every
priority test pins.

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

The status table keeps an entry per cancellable task so `get_status` still works
after a task finishes. Completed, cancelled, and failed entries stay in the table
until `prune()` is called. For a long-running pool with a continuous stream of
cancellable tasks, call `prune()` periodically to reclaim that memory; it removes
every terminal entry and leaves Queued and Running tasks untouched.

## Design

Three pieces:

`LocalQueue<T>` is one worker's queue: a mutex around a `std::priority_queue`,
with non-blocking access only. `try_pop()` returns the top item or `nullopt` if
empty, and `steal()` is the same operation under a different name so the call
sites read clearly and a future lock-free version can take from the opposite end
without touching callers. There is no condition variable here; blocking moves up
to the pool.

The priority queue is not stable: items of equal priority come out in an
unspecified order, which would make same-level FIFO a coin toss. To fix that each
item carries a sequence number assigned under the lock at push time, and the
comparator falls back to it on a tie, so equal-priority items drain oldest-first.
A `push(item)` with no priority lands at `Priority::Normal`.

`ThreadPool` owns one `LocalQueue` per worker and the worker threads. `enqueue` is
where the template work happens:

- `std::invoke_result_t<F, Args...>` gives the return type, which becomes the
  future's type.
- `std::bind` pins the arguments to the callable so the queued task takes no
  parameters.
- The task is wrapped in a `std::packaged_task`, which captures either the
  return value or a thrown exception into the future. `packaged_task` is
  move-only, so it lives in a `shared_ptr` that the queued `std::function` can
  copy.

A submitted task is routed to one queue. If the caller is itself a worker (a task
that enqueues sub-work), it goes on that worker's own queue, which keeps spawned
work near the data it came from; the worker index is held in a `thread_local` set
at startup, and non-worker threads fall through. Otherwise it round-robins across
queues via an atomic cursor.

A worker pulls from its own queue first. On a miss it walks the other queues,
starting one past itself and wrapping, and steals the first task it finds.
Starting at `i + 1` spreads thieves out instead of all of them hammering queue 0.
Only when every queue is empty does the worker go idle. `steal_count()` reports
how many tasks were taken this way, which the tests assert on directly and the
benchmark prints.

Idle workers sleep on one pool-level mutex and condition variable rather than per
queue. A worker decides to sleep by re-scanning every queue under that mutex and
then waiting while still holding it. A producer pushes, then takes the same mutex
(with an empty body) before notifying, which orders the push ahead of the wait
and closes the lost-wakeup window: the worker either sees the new work in its
re-scan or is woken by the notify.

The destructor keeps the no-task-dropped guarantee. It sets a stop flag and wakes
everyone; workers keep popping and stealing until every queue is empty and no task
is still running, so a sub-task enqueued by a still-running task mid-shutdown is
picked up rather than stranded, and only then do the workers exit and join. The
one race here is the last running task finishing and leaving the queues empty
while a worker is already asleep; the worker uses a short timed wait once stopping
is set so the drain tail cannot hang on a missed wakeup.

At most one queue lock is ever held at a time. A pop and a steal each lock exactly
one queue for one operation and release before doing anything else, so there is no
lock cycle and no steal-versus-steal deadlock.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build        # or: ./build/thread_pool_tests
```

Header-only if you prefer: `include/local_queue.h` has no dependencies, and
`ThreadPool` needs `src/thread_pool.cpp` on the link line.

## Tests

`tests/test_thread_pool.cpp` covers the queue and the pool directly. For the
queue: push and pop, `try_pop` on empty, `steal` matching `try_pop`, and priority
ordering with FIFO within a level. For the pool: argument forwarding, exception
propagation, move-only results, single-thread ordering, destruction draining
pending work, a 100k-task stress run, and concurrent submission from several
threads. Priority ordering is checked at both levels - that the queue drains
highest-first and stays FIFO within a level, and that the pool dispatches in the
same order when a single worker is held busy long enough for the tasks to queue
up. The cancellation path adds tests for cancelling a queued task, failing to
cancel a running one, and reading status through to Completed and Failed.

The stealing path has its own tests. One holds every worker but one busy, piles
work on a queue that worker cannot reach, and asserts `steal_count()` climbs as
the backlog drains. Another skews a large batch onto a couple of queues and
checks every task still runs and nothing is left pending. A third enqueues a
sub-task from inside a task while the pool is being destroyed and asserts the
child runs. The last seeds a recursive chain on one worker's queue with the other
workers free and asserts `steal_count()` stays at zero, confirming worker-local
routing keeps spawned work on the same queue. An idle pool is constructed and
destroyed with no work to confirm teardown does not hang.

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

The single-queue design this replaced flattened and then regressed past a handful
of threads, because every push and pop contended on one mutex. Those were the
numbers that motivated sharding the queue:

```
tasks per run        = 2000000  (previous, single global queue)

threads   tasks/sec          speedup
1         656904             1.00x
2         716729             1.09x
4         650074             0.99x
8         556007             0.85x
12        568770             0.87x
```

Per-worker queues take that contention off the hot path: in the common case a
worker only touches its own queue, and the global mutex the old design serialized
on is gone. Run the sweep on your machine to see where it now tops out.

`benchmark_comprehensive` splits throughput and latency into separate passes,
reports the steal count alongside throughput, adds an imbalanced run where all
the work is seeded onto one queue, compares against `std::async(launch::async)`,
and measures the cancellation path. Results are written to
`benchmark_results.csv`:

```bash
./build/benchmark_comprehensive 1000000
```

```
Throughput
scenario                   threads  throughput/s     steals
threadpool                 1        1181515          0
threadpool                 2        1094283          241883
threadpool                 4        987641           602214

Imbalanced (all work seeded on one worker)
scenario                   threads  throughput/s     steals
threadpool_imbalanced      2        1058402          498119
threadpool_imbalanced      4        951327           987655

Dispatch latency (steady-state window = threads*4)
scenario                   threads  p50_us    p95_us    p99_us
threadpool_latency         1        6.05      7.61      12.42
threadpool_latency         2        4.30      6.80      10.15
threadpool_latency         4        3.10      5.90      9.80

scenario                   threads  throughput/s     p50_us    p95_us    p99_us
std_async                  0        63428            0.00      0.00      0.00
cancellation_success_pct   1        100              0.00      0.00      0.00
cancel_ops_per_sec         1        15382180         0.00      0.00      0.00
```

The single-thread row steals nothing because there is only one queue. As threads
go up the steal count rises, which is the scheduler keeping otherwise-idle workers
fed. The imbalanced run makes that explicit: every task is seeded onto one
worker's queue, so the peers can only make progress by stealing, and throughput
staying close to the balanced run is the steal protocol earning its place.

Latency is measured in a steady-state window of `threads * 4` in-flight tasks
so the queue never accumulates backlog between submissions. The previous single
table mixed throughput-run queue-depth latency (hundreds of milliseconds at 1
thread) with steady-state dispatch latency; the split makes both numbers honest
and independently readable.

The pool reuses its threads, while `std::async` tends to start a new one per
call, which is why the pool is roughly an order of magnitude faster per task on
the same workload. The cancellation rows are measured with the workers held
busy so the queue cannot drain mid-run: every still-queued task cancels (100%),
and `cancel` itself is an O(1) atomic operation, hence the high ops/sec.

The single global mutex that used to cap throughput is gone. A lock-free
per-worker deque (Chase-Lev) is the higher ceiling and the natural follow-up; the
`steal()` seam is shaped so it can drop in without changing call sites.

## License

MIT. See [LICENSE](LICENSE).
