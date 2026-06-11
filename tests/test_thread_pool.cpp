#include "blocking_queue.h"
#include "thread_pool.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
// tests deterministic without sleeping for a fixed, fragile duration.
template <typename Pred>
bool wait_until(Pred pred, int timeout_ms = 2000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

void run(const char* name, void (*fn)()) {
    int before = g_failures;
    std::printf("%-34s", name);
    fn();
    std::printf("%s\n", g_failures == before ? "ok" : "FAILED");
}

// ---- BlockingQueue ---------------------------------------------------------

void queue_push_pop() {
    BlockingQueue<int> q;
    q.push(1);
    q.push(2);
    CHECK(q.size() == 2);
    CHECK(q.pop().value() == 1);
    CHECK(q.pop().value() == 2);
    CHECK(q.empty());
}

void queue_try_pop_on_empty() {
    BlockingQueue<int> q;
    CHECK(!q.try_pop().has_value());
    q.push(7);
    CHECK(q.try_pop().value() == 7);
    CHECK(!q.try_pop().has_value());
}

void queue_close_drains_then_signals() {
    BlockingQueue<int> q;
    q.push(10);
    q.push(20);
    q.close();
    CHECK(q.is_closed());
    CHECK(q.pop().value() == 10);  // drained first
    CHECK(q.pop().value() == 20);
    CHECK(!q.pop().has_value());   // empty + closed -> nullopt
}

void queue_blocks_until_pushed() {
    BlockingQueue<int> q;
    std::atomic<bool> got{false};
    std::thread consumer([&] {
        auto v = q.pop();
        got = v.has_value() && v.value() == 99;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(!got.load());
    q.push(99);
    consumer.join();
    CHECK(got.load());
}

void queue_orders_by_priority() {
    BlockingQueue<int> q;
    q.push(Priority::Low, 1);
    q.push(Priority::Critical, 2);
    q.push(Priority::Normal, 3);
    q.push(Priority::Critical, 4);  // same level as 2, queued later
    q.push(Priority::High, 5);

    // Critical first (2 before 4 by submission order), then High, Normal, Low.
    CHECK(q.pop().value() == 2);
    CHECK(q.pop().value() == 4);
    CHECK(q.pop().value() == 5);
    CHECK(q.pop().value() == 3);
    CHECK(q.pop().value() == 1);
    CHECK(q.empty());
}

void queue_same_priority_is_fifo() {
    BlockingQueue<int> q;
    for (int i = 0; i < 5; ++i) {
        q.push(Priority::High, i);
    }
    for (int i = 0; i < 5; ++i) {
        CHECK(q.pop().value() == i);
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

// The priority tests run on a single worker that we deliberately block first,
// so every task below is sitting in the queue before any of them can run. That
// makes the dispatch order fully determined by priority instead of by timing.
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

}  // namespace

int main() {
    run("queue_push_pop", queue_push_pop);
    run("queue_try_pop_on_empty", queue_try_pop_on_empty);
    run("queue_close_drains_then_signals", queue_close_drains_then_signals);
    run("queue_blocks_until_pushed", queue_blocks_until_pushed);
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

    run("pool_runs_higher_priority_first", pool_runs_higher_priority_first);
    run("pool_same_priority_runs_in_order", pool_same_priority_runs_in_order);
    run("pool_mixed_priorities", pool_mixed_priorities);
    run("pool_priority_returns_result", pool_priority_returns_result);

    std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
