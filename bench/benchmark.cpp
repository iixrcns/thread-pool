// Measures task throughput, scaling across thread counts, and submit-to-finish
// latency percentiles. Run with no arguments for the default sweep, or pass a
// task count, e.g. ./benchmark 2000000
#include "thread_pool.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static double seconds_since(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

// Submit `count` trivial tasks and wait for all of them. Returns tasks/sec.
static double throughput(std::size_t threads, std::size_t count) {
    ThreadPool pool(threads);
    std::vector<std::future<void>> futures;
    futures.reserve(count);

    auto start = Clock::now();
    for (std::size_t i = 0; i < count; ++i) {
        futures.push_back(pool.enqueue([] {}));
    }
    for (auto& f : futures) {
        f.get();
    }
    double elapsed = seconds_since(start);
    return static_cast<double>(count) / elapsed;
}

// Measures submit-to-completion latency in steady state. The number of
// outstanding tasks is capped so the queue does not grow without bound, which
// would otherwise just measure queue depth instead of dispatch overhead.
static void latency(std::size_t threads, std::size_t count) {
    ThreadPool pool(threads);
    std::vector<double> us;
    us.reserve(count);

    const std::size_t window = threads * 4;
    std::vector<std::future<double>> inflight;
    inflight.reserve(window);

    auto drain_one = [&] {
        us.push_back(inflight.front().get());
        inflight.erase(inflight.begin());
    };

    for (std::size_t i = 0; i < count; ++i) {
        auto submitted = Clock::now();
        inflight.push_back(pool.enqueue([submitted] {
            return std::chrono::duration<double, std::micro>(Clock::now() - submitted).count();
        }));
        if (inflight.size() >= window) {
            drain_one();
        }
    }
    while (!inflight.empty()) {
        drain_one();
    }

    std::sort(us.begin(), us.end());
    auto pct = [&](double p) { return us[static_cast<std::size_t>(p * (count - 1))]; };
    std::printf("dispatch latency, steady state (us): p50=%.2f  p95=%.2f  p99=%.2f  max=%.2f\n",
                pct(0.50), pct(0.95), pct(0.99), us.back());
}

int main(int argc, char** argv) {
    std::size_t count = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 1000000;
    unsigned hw = std::max(1u, std::thread::hardware_concurrency());

    std::printf("hardware_concurrency = %u\n", hw);
    std::printf("tasks per run        = %zu\n\n", count);

    double base = throughput(1, count);
    std::printf("%-9s %-18s %s\n", "threads", "tasks/sec", "speedup");
    std::printf("%-9d %-18.0f %.2fx\n", 1, base, 1.0);

    for (unsigned t = 2; t <= hw; t *= 2) {
        double tp = throughput(t, count);
        std::printf("%-9u %-18.0f %.2fx\n", t, tp, tp / base);
    }
    if ((hw & (hw - 1)) != 0) {  // also test the exact core count if not a power of two
        double tp = throughput(hw, count);
        std::printf("%-9u %-18.0f %.2fx\n", hw, tp, tp / base);
    }

    std::printf("\n");
    latency(hw, std::min<std::size_t>(count, 200000));
    return 0;
}
