#ifndef THREAD_POOL_THREAD_POOL_H
#define THREAD_POOL_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "local_queue.h"

using TaskId = std::uint64_t;

enum class TaskStatus {
    Queued,     // waiting in a queue, still cancellable
    Running,    // a worker has started it
    Cancelled,  // cancelled before a worker picked it up
    Completed,  // finished normally
    Failed,     // threw an exception
    Unknown,    // no task with this id
};

const char* to_string(TaskStatus status);

// A fixed-size pool of worker threads, each with its own queue, that steal work
// from one another when their own queue runs dry.
//
// enqueue() accepts any callable plus its arguments and hands back a
// std::future for the result. Exceptions thrown inside a task propagate to the
// caller through that future. The destructor drains all pending work before
// joining, so no submitted task is silently dropped.
//
// A task can be given a Priority; within a single queue higher-priority work is
// dispatched first, and tasks of equal priority run in submission order. Across
// queues there is no global order: a Critical task waiting in one worker's queue
// does not preempt a Normal task another worker pulls from its own queue first.
// Stealing softens this but does not restore a global order. With a single
// worker there is a single queue, so priority and FIFO are exact. The plain
// enqueue() overload runs at Priority::Normal.
//
// enqueue_cancellable() is for work that might need to be called off. It
// returns a TaskId; cancel(id) drops the task if no worker has claimed it yet,
// and get_status(id) reports where it is in its lifecycle.
class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template <typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;

    // Same as enqueue() but dispatched at the given priority.
    template <typename F, typename... Args>
    auto enqueue(Priority priority, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;

    // Submit a task that can be cancelled before it starts running.
    TaskId enqueue_cancellable(std::function<void()> work);

    // Returns true only if the task was still queued; a running or finished
    // task cannot be cancelled.
    bool cancel(TaskId id);

    TaskStatus get_status(TaskId id) const;

    // Removes every entry from the cancellation table whose status is terminal
    // (Completed, Cancelled, or Failed). Entries for tasks that are still
    // Queued or Running are left in place, since their Control is still
    // referenced by the queued lambda.
    //
    // Thread-safe and safe to call from any thread while workers are active.
    // Returns the number of entries removed.
    std::size_t prune();

    std::size_t thread_count() const { return workers_.size(); }

    // Total tasks sitting across every worker queue right now.
    std::size_t pending() const;

    // Total number of successful steals since construction. Monotonic; handy in
    // tests and the benchmark to confirm stealing is actually happening.
    std::uint64_t steal_count() const {
        return steals_.load(std::memory_order_relaxed);
    }

private:
    using Task = std::function<void()>;

    // Shared between the queued task and the bookkeeping map so cancel() and
    // the worker can race on a single atomic instead of touching any queue.
    struct Control {
        std::atomic<TaskStatus> status{TaskStatus::Queued};
    };

    // Identifies the worker the current thread is, and which pool it belongs to.
    // A task that submits more work consults this to keep the spawned work on
    // its own queue. Threads that are not workers of this pool (and workers of a
    // different pool) see a null owner and fall through to round-robin.
    struct WorkerContext {
        ThreadPool* pool = nullptr;
        std::size_t index = 0;
    };
    static thread_local WorkerContext tls_ctx_;

    void worker_loop(std::size_t index);

    // Pop from queue `index` first; on a miss, walk the other queues starting at
    // index + 1 and wrapping, stealing the first task found. nullopt if every
    // queue is empty. Bumps the steal counter on a successful steal.
    std::optional<Task> pop_local_or_steal(std::size_t index);

    // True if any queue holds work. Slow-path only (a worker that already found
    // nothing locally and stole nothing), so the brief per-queue locks it takes
    // stay off the hot path.
    bool has_any_work() const;

    // Picks the target queue for a submission: the caller's own queue if the
    // caller is one of this pool's workers, otherwise round-robin.
    std::size_t choose_queue();

    // Push onto the chosen queue and wake one sleeping worker. The empty lock on
    // idle_mtx_ before the notify is deliberate: it orders the push ahead of any
    // worker's decision to sleep, closing the lost-wakeup window.
    void dispatch(Priority priority, Task task);

    // Test-only seam: force a task onto a specific worker's queue, bypassing
    // routing, so the steal and routing tests are deterministic. Reached through
    // a friend so the public API stays clean.
    friend struct PoolTestAccess;
    void push_to_queue_for_test(std::size_t index, Priority priority, Task task);
    static std::size_t worker_index_for_test() { return tls_ctx_.index; }

    std::vector<std::thread> workers_;
    std::vector<std::unique_ptr<LocalQueue<Task>>> queues_;

    std::atomic<std::size_t> rr_{0};       // round-robin cursor for non-workers
    std::atomic<std::uint64_t> steals_{0};

    // Idle coordinator. Workers with nothing to do sleep on idle_cv_; producers
    // and shutdown wake them. active_ counts tasks currently executing, which
    // the drain condition needs to know no work is still in flight.
    std::mutex idle_mtx_;
    std::condition_variable idle_cv_;
    std::atomic<bool> stopping_{false};
    std::atomic<std::size_t> active_{0};

    std::atomic<TaskId> next_id_{0};
    mutable std::mutex controls_mutex_;
    std::unordered_map<TaskId, std::shared_ptr<Control>> controls_;
};

template <typename F, typename... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
    return enqueue(Priority::Normal, std::forward<F>(f),
                   std::forward<Args>(args)...);
}

template <typename F, typename... Args>
auto ThreadPool::enqueue(Priority priority, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
    using Result = std::invoke_result_t<F, Args...>;

    // packaged_task is move-only, but std::function needs a copyable target,
    // so the task is owned through a shared_ptr that the queued lambda copies.
    auto bound = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    auto task = std::make_shared<std::packaged_task<Result()>>(std::move(bound));

    std::future<Result> result = task->get_future();
    dispatch(priority, [task] { (*task)(); });
    return result;
}

#endif  // THREAD_POOL_THREAD_POOL_H
