#ifndef THREAD_POOL_THREAD_POOL_H
#define THREAD_POOL_THREAD_POOL_H

#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "blocking_queue.h"

// A fixed-size pool of worker threads that pull tasks from a shared queue.
//
// enqueue() accepts any callable plus its arguments and hands back a
// std::future for the result. Exceptions thrown inside a task propagate to the
// caller through that future. The destructor drains all pending work before
// joining, so no submitted task is silently dropped.
class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template <typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;

    std::size_t thread_count() const { return workers_.size(); }
    std::size_t pending() const { return tasks_.size(); }

private:
    using Task = std::function<void()>;

    void worker_loop();

    std::vector<std::thread> workers_;
    BlockingQueue<Task> tasks_;
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
