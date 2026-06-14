#include "local_queue.h"
#include "thread_pool.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// Test-only access to the pool's private routing seam. Lets a test force a task
// onto a chosen worker's queue and read which worker the current task runs on,
// which is what makes the steal and routing tests deterministic instead of
// timing-dependent.
struct PoolTestAccess {
    static void push_to_queue(ThreadPool& pool, std::size_t index, Priority p,
                              std::function<void()> task) {
        pool.push_to_queue_for_test(index, p, std::move(task));
    }
    static std::size_t worker_index() {
        return ThreadPool::worker_index_for_test();
    }
};

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool cond, const char* expr, const char* file, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::printf("  FAIL %s:%d  %s\n", file, line, expr);
    }
}

#define CHECK(cond) check((cond), #cond, __FILE__, __LINE__)

// Polls until the predicate holds or a timeout passes. Keeps the cancellation
// and stealing tests deterministic without sleeping for a fixed, fragile time.
template <typename Pred>
bool wait_until(Pred pred, int timeout_ms = 4000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

void run(const char* name, void (*fn)()) {
    int before = g_failures;
    std::printf("%-38s", name);
    fn();
    std::printf("%s\n", g_failures == before ? "ok" : "FAILED");
}

// ---- LocalQueue ------------------------------------------------------------

void queue_push_pop() {
    LocalQueue<int> q;
    q.push(1);
    q.push(2);
    CHECK(q.size() == 2);
    CHECK(q.try_pop().value() == 1);
    CHECK(q.try_pop().value() == 2);
    CHECK(q.empty());
}

void queue_try_pop_on_empty() {
    LocalQueue<int> q;
    CHECK(!q.try_pop().has_value());
    q.push(7);
    CHECK(q.try_pop().value() == 7);
    CHECK(!q.try_pop().has_value());
}

void queue_steal_matches_try_pop() {
    LocalQueue<int> q;
    q.push(Priority::High, 1);
    q.push(Priority::Critical, 2);
    // steal() pulls the same item try_pop() would: highest priority first.
    CHECK(q.steal().value() == 2);
    CHECK(q.steal().value() == 1);
    CHECK(!q.steal().has_value());
}

void queue_orders_by_priority() {
    LocalQueue<int> q;
    q.push(Priority::Low, 1);
    q.push(Priority::Critical, 2);
    q.push(Priority::Normal, 3);
    q.push(Priority::Critical, 4);  // same level as 2, queued later
    q.push(Priority::High, 5);

    // Critical first (2 before 4 by submission order), then High, Normal, Low.
    CHECK(q.try_pop().value() == 2);
    CHECK(q.try_pop().value() == 4);
    CHECK(q.try_pop().value() == 5);
    CHECK(q.try_pop().value() == 3);
    CHECK(q.try_pop().value() == 1);
    CHECK(q.empty());
}

void queue_same_priority_is_fifo() {
    LocalQueue<int> q;
    for (int i = 0; i < 5; ++i) {
        q.push(Priority::High, i);
    }
    for (int i = 0; i < 5; ++i) {
        CHECK(q.try_pop().value() == i);
    }
}

// ---- ThreadPool ------------------------------------------------------------

void pool_rejects_zero_threads() {
    bool threw = false;
    try {
        ThreadPool pool(0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
}

void pool_runs_void_task() {
    std::atomic<int> counter{0};
    {
        ThreadPool pool(2);
        pool.enqueue([&] { ++counter; });
        pool.enqueue([&] { ++counter; });
    }  // destructor drains
    CHECK(counter.load() == 2);
}

void pool_returns_result() {
    ThreadPool pool(2);
    auto f = pool.enqueue([] { return 42; });
    CHECK(f.get() == 42);
}

void pool_forwards_arguments() {
    ThreadPool pool(2);
    auto f = pool.enqueue([](int x, int y) { return x + y; }, 10, 20);
    CHECK(f.get() == 30);
}

void pool_handles_many_results() {
    ThreadPool pool(4);
    std::vector<std::future<int>> futures;
    for (int i = 0; i < 64; ++i) {
        futures.push_back(pool.enqueue([i] { return i * i; }));
    }
    bool ok = true;
    for (int i = 0; i < 64; ++i) {
        ok = ok && futures[i].get() == i * i;
    }
    CHECK(ok);
}

void pool_propagates_exception() {
    ThreadPool pool(2);
    auto f = pool.enqueue([]() -> int {
        throw std::runtime_error("boom");
    });
    // Retain the shared state on this thread. A plain future::get() drops the
    // consumer's reference, leaving a worker to tear down the state (and the
    // exception object inside it) concurrently with this read; share() keeps it
    // alive until we are done.
    std::shared_future<int> sf = f.share();
    bool caught = false;
    try {
        sf.get();
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()) == "boom";
    }
    CHECK(caught);
}

void pool_serializes_on_single_thread() {
    ThreadPool pool(1);
    std::vector<int> order;
    std::mutex m;
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 5; ++i) {
        futures.push_back(pool.enqueue([i, &order, &m] {
            std::lock_guard<std::mutex> lock(m);
            order.push_back(i);
        }));
    }
    for (auto& f : futures) f.get();
    bool sorted = true;
    for (int i = 0; i < 5; ++i) sorted = sorted && order[i] == i;
    CHECK(sorted);
}

void pool_supports_move_only_result() {
    ThreadPool pool(2);
    auto f = pool.enqueue([] { return std::make_unique<int>(123); });
    auto ptr = f.get();
    CHECK(ptr && *ptr == 123);
}

void pool_drains_on_destruction() {
    std::atomic<int> done{0};
    {
        ThreadPool pool(2);
        for (int i = 0; i < 200; ++i) {
            pool.enqueue([&done] {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                ++done;
            });
        }
    }  // must wait for all 200
    CHECK(done.load() == 200);
}

void pool_stress_100k() {
    ThreadPool pool(4);
    std::atomic<long> sum{0};
    std::vector<std::future<void>> futures;
    futures.reserve(100000);
    for (int i = 0; i < 100000; ++i) {
        futures.push_back(pool.enqueue([&sum] { sum.fetch_add(1, std::memory_order_relaxed); }));
    }
    for (auto& f : futures) f.get();
    CHECK(sum.load() == 100000);
}

void pool_concurrent_enqueue() {
    ThreadPool pool(4);
    std::atomic<int> counter{0};
    std::vector<std::thread> producers;
    for (int t = 0; t < 4; ++t) {
        producers.emplace_back([&pool, &counter] {
            for (int i = 0; i < 250; ++i) {
                pool.enqueue([&counter] { ++counter; });
            }
        });
    }
    for (auto& p : producers) p.join();
    // Let the pool finish the backlog before reading the counter.
    while (pool.pending() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(counter.load() == 1000);
}

void pool_unknown_task_id() {
    ThreadPool pool(2);
    CHECK(pool.get_status(999999) == TaskStatus::Unknown);
    CHECK(!pool.cancel(999999));
}

void pool_cancel_queued_task() {
    ThreadPool pool(1);  // single worker so we can occupy it deliberately
    std::atomic<bool> blocker_started{false};
    std::atomic<bool> release{false};
    std::atomic<bool> ran{false};

    // Occupy the only worker until we say otherwise.
    pool.enqueue([&] {
        blocker_started = true;
        while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    CHECK(wait_until([&] { return blocker_started.load(); }));

    TaskId id = pool.enqueue_cancellable([&] { ran = true; });
    CHECK(pool.get_status(id) == TaskStatus::Queued);
    CHECK(pool.cancel(id));
    CHECK(pool.get_status(id) == TaskStatus::Cancelled);

    release = true;
    // Give the worker a moment to drain the (skipped) cancelled task.
    CHECK(wait_until([&] { return pool.pending() == 0; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(!ran.load());
}

void pool_cannot_cancel_running_task() {
    ThreadPool pool(2);
    std::atomic<bool> running{false};
    std::atomic<bool> release{false};

    TaskId id = pool.enqueue_cancellable([&] {
        running = true;
        while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    CHECK(wait_until([&] { return running.load(); }));

    CHECK(!pool.cancel(id));
    CHECK(pool.get_status(id) == TaskStatus::Running);

    release = true;
    CHECK(wait_until([&] { return pool.get_status(id) == TaskStatus::Completed; }));
}

void pool_status_completed_and_failed() {
    ThreadPool pool(2);
    TaskId ok = pool.enqueue_cancellable([] {});
    TaskId bad = pool.enqueue_cancellable([] { throw std::runtime_error("x"); });

    CHECK(wait_until([&] { return pool.get_status(ok) == TaskStatus::Completed; }));
    CHECK(wait_until([&] { return pool.get_status(bad) == TaskStatus::Failed; }));
}

void pool_prune_removes_terminal_entries() {
    ThreadPool pool(4);
    const std::size_t count = 100;
    std::vector<TaskId> ids;
    ids.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        ids.push_back(pool.enqueue_cancellable([] {}));
    }

    // Wait until every task has reached its terminal Completed status before
    // pruning, so the count is deterministic.
    CHECK(wait_until([&] {
        for (TaskId id : ids) {
            if (pool.get_status(id) != TaskStatus::Completed) return false;
        }
        return true;
    }));

    std::size_t removed = pool.prune();
    CHECK(removed == count);
    CHECK(pool.get_status(ids[0]) == TaskStatus::Unknown);
    CHECK(pool.get_status(ids[99]) == TaskStatus::Unknown);
}

void pool_prune_keeps_queued_entries() {
    ThreadPool pool(1);  // single worker so the second task stays queued
    std::atomic<bool> blocker_started{false};
    std::atomic<bool> release{false};

    // Occupy the only worker so the cancellable task below cannot start.
    pool.enqueue([&] {
        blocker_started = true;
        while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    CHECK(wait_until([&] { return blocker_started.load(); }));

    TaskId id = pool.enqueue_cancellable([] {});
    CHECK(pool.get_status(id) == TaskStatus::Queued);

    std::size_t removed = pool.prune();
    CHECK(removed == 0u);
    CHECK(pool.get_status(id) == TaskStatus::Queued);

    release = true;
}

// The priority tests run on a single worker that we deliberately block first,
// so every task below is sitting in that worker's one queue before any of them
// can run. With a single worker there is a single queue, so dispatch order is
// fully determined by priority instead of by timing.
struct Gate {
    ThreadPool& pool;
    std::atomic<bool> open{false};
    std::atomic<bool> running{false};

    explicit Gate(ThreadPool& p) : pool(p) {
        pool.enqueue([this] {
            running = true;
            while (!open) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
    void wait_until_worker_busy() {
        wait_until([this] { return running.load(); });
    }
    void release() { open = true; }
};

void pool_runs_higher_priority_first() {
    ThreadPool pool(1);
    Gate gate(pool);
    gate.wait_until_worker_busy();

    std::vector<int> order;
    std::mutex m;
    auto record = [&](int tag) {
        return [&, tag] {
            std::lock_guard<std::mutex> lock(m);
            order.push_back(tag);
        };
    };

    // Submitted lowest-first; expected to run highest-first.
    pool.enqueue(Priority::Low, record(0));
    pool.enqueue(Priority::Normal, record(1));
    pool.enqueue(Priority::High, record(2));
    pool.enqueue(Priority::Critical, record(3));

    gate.release();
    CHECK(wait_until([&] {
        std::lock_guard<std::mutex> lock(m);
        return order.size() == 4;
    }));
    std::lock_guard<std::mutex> lock(m);
    CHECK((order == std::vector<int>{3, 2, 1, 0}));
}

void pool_same_priority_runs_in_order() {
    ThreadPool pool(1);
    Gate gate(pool);
    gate.wait_until_worker_busy();

    std::vector<int> order;
    std::mutex m;
    for (int i = 0; i < 4; ++i) {
        pool.enqueue(Priority::High, [&, i] {
            std::lock_guard<std::mutex> lock(m);
            order.push_back(i);
        });
    }

    gate.release();
    CHECK(wait_until([&] {
        std::lock_guard<std::mutex> lock(m);
        return order.size() == 4;
    }));
    std::lock_guard<std::mutex> lock(m);
    CHECK((order == std::vector<int>{0, 1, 2, 3}));
}

void pool_mixed_priorities() {
    ThreadPool pool(1);
    Gate gate(pool);
    gate.wait_until_worker_busy();

    std::vector<std::string> order;
    std::mutex m;
    auto add = [&](Priority p, std::string name) {
        pool.enqueue(p, [&, name = std::move(name)] {
            std::lock_guard<std::mutex> lock(m);
            order.push_back(name);
        });
    };

    add(Priority::Low, "L1");
    add(Priority::High, "H1");
    add(Priority::Normal, "N1");
    add(Priority::Low, "L2");
    add(Priority::High, "H2");

    gate.release();
    CHECK(wait_until([&] {
        std::lock_guard<std::mutex> lock(m);
        return order.size() == 5;
    }));
    std::lock_guard<std::mutex> lock(m);
    const std::vector<std::string> expected{"H1", "H2", "N1", "L1", "L2"};
    CHECK((order == expected));
}

void pool_priority_returns_result() {
    ThreadPool pool(2);
    auto f = pool.enqueue(Priority::Critical, [](int a, int b) { return a * b; }, 6, 7);
    CHECK(f.get() == 42);
}

// ---- Work stealing ---------------------------------------------------------

// Blocks every worker, recording which worker index ran each blocker, and lets
// the test release workers one index at a time. Pinning a specific worker busy
// is what makes the steal and routing assertions exact rather than likely.
//
// The shared state lives behind shared_ptrs that the blocker lambdas copy, so a
// worker still running a blocker never touches freed memory even if the helper
// itself goes out of scope before the pool joins.
struct WorkerHold {
    std::size_t n;
    std::shared_ptr<std::atomic<int>> blocked = std::make_shared<std::atomic<int>>(0);
    std::shared_ptr<std::vector<std::atomic<bool>>> release;

    explicit WorkerHold(ThreadPool& pool) : n(pool.thread_count()) {
        release = std::make_shared<std::vector<std::atomic<bool>>>(n);
        for (auto& r : *release) r.store(false);

        // One blocker per worker. Each grabs a blocker and parks on its own
        // release flag; with n blockers and n workers, no worker can take a
        // second, so all of them end up held, each knowing its index.
        auto blk = blocked;
        auto rel = release;
        for (std::size_t i = 0; i < n; ++i) {
            pool.enqueue([blk, rel] {
                std::size_t idx = PoolTestAccess::worker_index();
                blk->fetch_add(1);
                while (!(*rel)[idx].load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });
        }
    }

    bool wait_until_all_held() {
        return wait_until([this] { return blocked->load() == static_cast<int>(n); });
    }
    void free(std::size_t index) { (*release)[index].store(true); }
    void free_all() {
        for (auto& r : *release) r.store(true);
    }
};

void pool_steal_happens() {
    const std::size_t N = 4;
    ThreadPool pool(N);

    WorkerHold hold(pool);
    CHECK(hold.wait_until_all_held());

    const std::uint64_t baseline = pool.steal_count();

    // Pile work that only worker 0's queue holds. Worker 0 stays blocked, so the
    // backlog can only drain through the other workers stealing it.
    const int P = 500;
    std::atomic<int> done{0};
    for (int i = 0; i < P; ++i) {
        PoolTestAccess::push_to_queue(pool, 0, Priority::Normal, [&] { ++done; });
    }

    for (std::size_t i = 1; i < N; ++i) hold.free(i);

    CHECK(wait_until([&] { return done.load() == P; }));
    // Worker 0 never touched its own queue, so every piled task was stolen.
    CHECK(pool.steal_count() - baseline == static_cast<std::uint64_t>(P));

    hold.free(0);  // let worker 0's blocker finish so the pool can join
}

void pool_no_task_lost_under_imbalance() {
    const std::size_t N = 4;
    ThreadPool pool(N);

    // Skew a large batch onto two of the four queues. The other two workers can
    // only make progress by stealing; nothing should be dropped.
    const int P = 5000;
    std::atomic<int> done{0};
    for (int i = 0; i < P; ++i) {
        const std::size_t q = (i % 2 == 0) ? 0 : 1;
        PoolTestAccess::push_to_queue(pool, q, Priority::Normal,
                                      [&] { done.fetch_add(1, std::memory_order_relaxed); });
    }

    CHECK(wait_until([&] { return done.load() == P; }, 8000));
    CHECK(pool.pending() == 0);
}

void pool_subtask_during_drain() {
    std::atomic<bool> child_ran{false};
    {
        ThreadPool pool(2);
        pool.enqueue([&] {
            // Still running when the pool starts shutting down; the child it
            // enqueues here must still be picked up by the drain.
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            pool.enqueue([&] { child_ran = true; });
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }  // destructor runs while the parent is mid-flight
    CHECK(child_ran.load());
}

void pool_idle_teardown() {
    // Construct, submit nothing, destroy. Reaching the check without the
    // destructor hanging is the assertion.
    { ThreadPool pool(4); }
    CHECK(true);
}

void pool_worker_local_routing() {
    const std::size_t N = 2;
    ThreadPool pool(N);

    WorkerHold hold(pool);
    CHECK(hold.wait_until_all_held());

    const std::uint64_t baseline = pool.steal_count();

    // A recursive chain: each task, while running on a worker, enqueues the
    // next. Worker-local routing keeps the chain on the queue of whichever
    // worker runs it. We seed the root on queue 0 and free only worker 0, so the
    // entire chain runs on worker 0 and nothing is ever stolen.
    const int CHAIN = 200;
    std::atomic<int> ran{0};
    std::function<void()> step = [&] {
        int n = ran.fetch_add(1) + 1;
        if (n < CHAIN) pool.enqueue(step);  // worker-local: stays on this worker
    };
    PoolTestAccess::push_to_queue(pool, 0, Priority::Normal, step);

    hold.free(0);  // free the queue-0 owner; the other worker stays blocked

    CHECK(wait_until([&] { return ran.load() == CHAIN; }));
    CHECK(pool.steal_count() - baseline == 0u);

    hold.free_all();
}

}  // namespace

int main() {
    run("queue_push_pop", queue_push_pop);
    run("queue_try_pop_on_empty", queue_try_pop_on_empty);
    run("queue_steal_matches_try_pop", queue_steal_matches_try_pop);
    run("queue_orders_by_priority", queue_orders_by_priority);
    run("queue_same_priority_is_fifo", queue_same_priority_is_fifo);

    run("pool_rejects_zero_threads", pool_rejects_zero_threads);
    run("pool_runs_void_task", pool_runs_void_task);
    run("pool_returns_result", pool_returns_result);
    run("pool_forwards_arguments", pool_forwards_arguments);
    run("pool_handles_many_results", pool_handles_many_results);
    run("pool_propagates_exception", pool_propagates_exception);
    run("pool_serializes_on_single_thread", pool_serializes_on_single_thread);
    run("pool_supports_move_only_result", pool_supports_move_only_result);
    run("pool_drains_on_destruction", pool_drains_on_destruction);
    run("pool_stress_100k", pool_stress_100k);
    run("pool_concurrent_enqueue", pool_concurrent_enqueue);

    run("pool_unknown_task_id", pool_unknown_task_id);
    run("pool_cancel_queued_task", pool_cancel_queued_task);
    run("pool_cannot_cancel_running_task", pool_cannot_cancel_running_task);
    run("pool_status_completed_and_failed", pool_status_completed_and_failed);
    run("pool_prune_removes_terminal_entries", pool_prune_removes_terminal_entries);
    run("pool_prune_keeps_queued_entries", pool_prune_keeps_queued_entries);

    run("pool_runs_higher_priority_first", pool_runs_higher_priority_first);
    run("pool_same_priority_runs_in_order", pool_same_priority_runs_in_order);
    run("pool_mixed_priorities", pool_mixed_priorities);
    run("pool_priority_returns_result", pool_priority_returns_result);

    run("pool_steal_happens", pool_steal_happens);
    run("pool_no_task_lost_under_imbalance", pool_no_task_lost_under_imbalance);
    run("pool_subtask_during_drain", pool_subtask_during_drain);
    run("pool_idle_teardown", pool_idle_teardown);
    run("pool_worker_local_routing", pool_worker_local_routing);

    std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
