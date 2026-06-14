// Comprehensive benchmark suite.
//
//   - ThreadPool throughput across thread counts (with steal counts)
//   - ThreadPool dispatch latency (steady-state window)
//   - ThreadPool imbalanced workload: all work seeded onto one queue, peers steal
//   - ThreadPool vs std::async(launch::async) on the same workload
//   - cancellation success rate under load
//
// Results print to the console and are written to benchmark_results.csv.
// Usage: ./benchmark_comprehensive [task_count]
#include "thread_pool.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <future>
#include <random>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

struct Result {
    std::string scenario;
    unsigned threads;
    double throughput_per_sec;
    double p50_us;
    double p95_us;
    double p99_us;
    std::uint64_t steals;
};

static std::vector<Result> g_results;

static double seconds_since(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

static void percentiles(std::vector<double>& samples, double& p50, double& p95, double& p99) {
    std::sort(samples.begin(), samples.end());
    auto at = [&](double p) { return samples[static_cast<std::size_t>(p * (samples.size() - 1))]; };
    p50 = at(0.50);
    p95 = at(0.95);
    p99 = at(0.99);
}

// ThreadPool throughput: submit `count` empty tasks and time submission through
// completion. No per-task latency is collected here because the queue fills
// faster than it drains at low thread counts, which produces queue-depth
// latency rather than dispatch latency.
static void bench_pool_throughput(unsigned threads, std::size_t count) {
    ThreadPool pool(threads);
    std::vector<std::future<void>> futures;
    futures.reserve(count);

    auto start = Clock::now();
    for (std::size_t i = 0; i < count; ++i) {
        futures.push_back(pool.enqueue([] {}));
    }
    for (auto& f : futures) f.get();
    double elapsed = seconds_since(start);

    g_results.push_back({"threadpool", threads, count / elapsed, 0, 0, 0,
                         pool.steal_count()});
}

// ThreadPool dispatch latency: measured in a steady-state window of
// threads * 4 in-flight tasks so the queue never accumulates backlog between
// submissions. The timestamp is captured immediately before enqueue with no
// other work between the two calls.
static void bench_pool_latency(unsigned threads, std::size_t count = 50'000) {
    ThreadPool pool(threads);
    const std::size_t window = threads * 4;

    std::vector<double> samples;
    samples.reserve(count);

    std::deque<std::future<double>> inflight;

    auto drain_one = [&] {
        samples.push_back(inflight.front().get());
        inflight.pop_front();
    };

    for (std::size_t i = 0; i < count; ++i) {
        auto submitted = Clock::now();
        inflight.push_back(pool.enqueue([submitted] {
            return std::chrono::duration<double, std::micro>(
                Clock::now() - submitted).count();
        }));
        if (inflight.size() >= window) drain_one();
    }
    while (!inflight.empty()) drain_one();

    double p50, p95, p99;
    percentiles(samples, p50, p95, p99);
    g_results.push_back({"threadpool_latency", threads, 0, p50, p95, p99, 0});
}

// Imbalanced workload: all the work is seeded from inside a single worker, so
// worker-local routing piles every task onto that one worker's queue. The other
// workers start empty and can only make progress by stealing. Throughput staying
// close to the balanced run is the point - the steal count shows the peers are
// the ones draining the backlog, not the owner alone.
static void bench_imbalanced(unsigned threads, std::size_t count) {
    ThreadPool pool(threads);

    std::atomic<std::size_t> done{0};

    // The bootstrap task runs on some worker and enqueues the whole batch from
    // there, so every child lands on that worker's local queue.
    auto bootstrap = pool.enqueue([&] {
        for (std::size_t i = 0; i < count; ++i) {
            pool.enqueue([&] { done.fetch_add(1, std::memory_order_relaxed); });
        }
    });
    bootstrap.get();

    auto start = Clock::now();
    while (done.load(std::memory_order_relaxed) < count) {
        std::this_thread::yield();
    }
    double elapsed = seconds_since(start);

    g_results.push_back({"threadpool_imbalanced", threads, count / elapsed,
                         0, 0, 0, pool.steal_count()});
}

// std::async with the same workload. Each call may spin up a fresh thread, so
// this is the cost the pool is meant to avoid. The number of outstanding
// futures is bounded because keeping tens of thousands of live threads is
// neither a fair comparison nor something every machine can survive.
static void bench_std_async(std::size_t count) {
    const std::size_t n = std::min<std::size_t>(count, 20000);
    const std::size_t window = 64;
    std::vector<std::future<void>> inflight;
    inflight.reserve(window);

    auto start = Clock::now();
    std::size_t launched = 0;
    try {
        for (std::size_t i = 0; i < n; ++i) {
            inflight.push_back(std::async(std::launch::async, [] {}));
            ++launched;
            if (inflight.size() >= window) {
                inflight.front().get();
                inflight.erase(inflight.begin());
            }
        }
        for (auto& f : inflight) f.get();
    } catch (const std::system_error&) {
        // Hit the OS thread limit; report what we managed to launch.
        for (auto& f : inflight) {
            if (f.valid()) f.get();
        }
    }
    double elapsed = seconds_since(start);

    if (launched > 0 && elapsed > 0) {
        g_results.push_back({"std_async", 0, launched / elapsed, 0, 0, 0, 0});
    }
}

// Demonstrates cancellation under load and measures how fast cancel() runs.
// The workers are held with a blocker so the queue cannot drain while we
// cancel; that way the success rate reflects correctness (a still-queued task
// always cancels) rather than a race between draining and cancelling.
static void bench_cancellation(unsigned threads, std::size_t count) {
    ThreadPool pool(threads);
    std::atomic<bool> release{false};
    std::atomic<unsigned> blocked{0};

    // Occupy every worker so nothing in the queue gets a chance to start.
    for (unsigned t = 0; t < threads; ++t) {
        pool.enqueue([&] {
            ++blocked;
            while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
    while (blocked < threads) std::this_thread::sleep_for(std::chrono::milliseconds(1));

    std::vector<TaskId> ids;
    ids.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        ids.push_back(pool.enqueue_cancellable([] {}));
    }

    std::mt19937 rng(12345);
    std::bernoulli_distribution coin(0.5);
    std::size_t attempts = 0;
    std::size_t cancelled = 0;
    auto start = Clock::now();
    for (TaskId id : ids) {
        if (coin(rng)) {
            ++attempts;
            if (pool.cancel(id)) ++cancelled;
        }
    }
    double elapsed = seconds_since(start);
    release = true;

    double rate = attempts ? (100.0 * cancelled / attempts) : 0.0;
    double ops = attempts ? attempts / elapsed : 0.0;
    g_results.push_back({"cancellation_success_pct", threads, rate, 0, 0, 0, 0});
    g_results.push_back({"cancel_ops_per_sec", threads, ops, 0, 0, 0, 0});
}

static void write_csv(const std::string& path) {
    std::ofstream out(path);
    out << "scenario,threads,throughput_per_sec,p50_us,p95_us,p99_us,steals\n";
    for (const auto& r : g_results) {
        out << r.scenario << ',' << r.threads << ',' << r.throughput_per_sec << ','
            << r.p50_us << ',' << r.p95_us << ',' << r.p99_us << ',' << r.steals << '\n';
    }
}

int main(int argc, char** argv) {
    std::size_t count = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 1000000;
    unsigned hw = std::max(1u, std::thread::hardware_concurrency());

    std::printf("hardware_concurrency = %u, tasks = %zu\n\n", hw, count);

    bench_pool_throughput(1, count);
    bench_pool_latency(1);
    for (unsigned t = 2; t <= hw; t *= 2) {
        bench_pool_throughput(t, count);
        bench_pool_latency(t);
    }
    if (hw > 1 && (hw & (hw - 1)) != 0) {
        bench_pool_throughput(hw, count);
        bench_pool_latency(hw);
    }

    bench_std_async(count);
    bench_cancellation(hw, 10000);

    // Imbalanced runs: only meaningful with more than one worker, since the
    // whole point is peers stealing from the seeded worker.
    for (unsigned t = 2; t <= hw; t *= 2) {
        bench_imbalanced(t, count);
    }
    if (hw > 1 && (hw & (hw - 1)) != 0) {
        bench_imbalanced(hw, count);
    }

    // Throughput table.
    std::printf("Throughput\n");
    std::printf("%-26s %-8s %-16s %-12s\n", "scenario", "threads", "throughput/s", "steals");
    for (const auto& r : g_results) {
        if (r.scenario == "threadpool") {
            std::printf("%-26s %-8u %-16.0f %-12llu\n",
                        r.scenario.c_str(), r.threads, r.throughput_per_sec,
                        static_cast<unsigned long long>(r.steals));
        }
    }

    std::printf("\n");

    // Imbalanced workload: all work seeded onto one queue, peers steal to keep up.
    std::printf("Imbalanced (all work seeded on one worker)\n");
    std::printf("%-26s %-8s %-16s %-12s\n", "scenario", "threads", "throughput/s", "steals");
    for (const auto& r : g_results) {
        if (r.scenario == "threadpool_imbalanced") {
            std::printf("%-26s %-8u %-16.0f %-12llu\n",
                        r.scenario.c_str(), r.threads, r.throughput_per_sec,
                        static_cast<unsigned long long>(r.steals));
        }
    }

    std::printf("\n");

    // Dispatch latency table.
    std::printf("Dispatch latency (steady-state window = threads*4)\n");
    std::printf("%-26s %-8s %-9s %-9s %-9s\n",
                "scenario", "threads", "p50_us", "p95_us", "p99_us");
    for (const auto& r : g_results) {
        if (r.scenario == "threadpool_latency") {
            std::printf("%-26s %-8u %-9.2f %-9.2f %-9.2f\n",
                        r.scenario.c_str(), r.threads,
                        r.p50_us, r.p95_us, r.p99_us);
        }
    }

    std::printf("\n");

    // Remaining rows (std_async, cancellation).
    std::printf("%-26s %-8s %-16s %-9s %-9s %-9s\n",
                "scenario", "threads", "throughput/s", "p50_us", "p95_us", "p99_us");
    for (const auto& r : g_results) {
        if (r.scenario != "threadpool" && r.scenario != "threadpool_latency" &&
            r.scenario != "threadpool_imbalanced") {
            std::printf("%-26s %-8u %-16.0f %-9.2f %-9.2f %-9.2f\n",
                        r.scenario.c_str(), r.threads, r.throughput_per_sec,
                        r.p50_us, r.p95_us, r.p99_us);
        }
    }

    write_csv("benchmark_results.csv");
    std::printf("\nResults written to benchmark_results.csv\n");
    return 0;
}
