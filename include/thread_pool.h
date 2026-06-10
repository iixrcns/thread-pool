#ifndef THREAD_POOL_THREAD_POOL_H
#define THREAD_POOL_THREAD_POOL_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "blocking_queue.h"

using TaskId = std::uint64_t;

enum class TaskStatus {
    Queued,     // waiting in the queue, still cancellable
    Running,    // a worker has started it
    Cancelled,  // cancelled before a worker picked it up
    Completed,  // finished normally
    Failed,     // threw an exception
    Unknown,    // no task with this id
};

const char* to_string(TaskStatus status);

// A fixed-size pool of worker threads that pull tasks from a shared queue.
//
// enqueue() accepts any callable plus its arguments and hands back a
// std::future for the result. Exceptions thrown inside a task propagate to the
// caller through that future. The destructor drains all pending work before
// joining, so no submitted task is silently dropped.
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

    // Submit a task that can be cancelled before it starts running.
    TaskId enqueue_cancellable(std::function<void()> work);

    // Returns true only if the task was still queued; a running or finished
    // task cannot be cancelled.
    bool cancel(TaskId id);

    TaskStatus get_status(TaskId id) const;

    std::size_t thread_count() const { return workers_.size(); }
    std::size_t pending() const { return tasks_.size(); }

private:
    using Task = std::function<void()>;

    // Shared between the queued task and the bookkeeping map so cancel() and
    // the worker can race on a single atomic instead of touching the queue.
    struct Control {
        std::atomic<TaskStatus> status{TaskStatus::Queued};
    };

    void worker_loop();

    std::vector<std::thread> workers_;
    BlockingQueue<Task> tasks_;

    std::atomic<TaskId> next_id_{0};
    mutable std::mutex controls_mutex_;
    std::unordered_map<TaskId, std::shared_ptr<Control>> controls_;
};

template <typename F, typename... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
    using Result = std::invoke_result_t<F, Args...>;

    // packaged_task is move-only, but std::function needs a copyable target,
    // so the task is owned through a shared_ptr that the queued lambda copies.
    auto bound = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    auto task = std::make_shared<std::packaged_task<Result()>>(std::move(bound));

    std::future<Result> result = task->get_future();
    tasks_.push([task] { (*task)(); });
    return result;
}

#endif  // THREAD_POOL_THREAD_POOL_H
