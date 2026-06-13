#include "thread_pool.h"

#include <stdexcept>

const char* to_string(TaskStatus status) {
    switch (status) {
        case TaskStatus::Queued:    return "Queued";
        case TaskStatus::Running:   return "Running";
        case TaskStatus::Cancelled: return "Cancelled";
        case TaskStatus::Completed: return "Completed";
        case TaskStatus::Failed:    return "Failed";
        case TaskStatus::Unknown:   return "Unknown";
    }
    return "Unknown";
}

ThreadPool::ThreadPool(std::size_t num_threads) {
    if (num_threads == 0) {
        throw std::invalid_argument("ThreadPool needs at least one thread");
    }

    workers_.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] { worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    // close() lets workers finish the queue and then return on the next empty
    // pop(). Joining afterwards guarantees every task has run.
    tasks_.close();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadPool::worker_loop() {
    while (true) {
        std::optional<Task> task = tasks_.pop();
        if (!task) {
            break;  // queue closed and drained
        }
        (*task)();
    }
}

TaskId ThreadPool::enqueue_cancellable(std::function<void()> work) {
    const TaskId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    auto control = std::make_shared<Control>();
    {
        std::lock_guard<std::mutex> lock(controls_mutex_);
        controls_.emplace(id, control);
    }

    tasks_.push([control, work = std::move(work)] {
        // Claim the task. If cancel() already moved it to Cancelled, this CAS
        // fails and the worker leaves it alone.
        TaskStatus expected = TaskStatus::Queued;
        if (!control->status.compare_exchange_strong(expected, TaskStatus::Running)) {
            return;
        }
        try {
            work();
            control->status.store(TaskStatus::Completed);
        } catch (...) {
            control->status.store(TaskStatus::Failed);
        }
    });
    return id;
}

bool ThreadPool::cancel(TaskId id) {
    std::shared_ptr<Control> control;
    {
        std::lock_guard<std::mutex> lock(controls_mutex_);
        auto it = controls_.find(id);
        if (it == controls_.end()) {
            return false;
        }
        control = it->second;
    }
    // Succeeds only while the task is still Queued; loses the CAS if a worker
    // has already moved it to Running.
    TaskStatus expected = TaskStatus::Queued;
    return control->status.compare_exchange_strong(expected, TaskStatus::Cancelled);
}

TaskStatus ThreadPool::get_status(TaskId id) const {
    std::lock_guard<std::mutex> lock(controls_mutex_);
    auto it = controls_.find(id);
    if (it == controls_.end()) {
        return TaskStatus::Unknown;
    }
    return it->second->status.load();
}

std::size_t ThreadPool::prune() {
    std::lock_guard<std::mutex> lock(controls_mutex_);
    std::size_t removed = 0;
    for (auto it = controls_.begin(); it != controls_.end(); ) {
        const TaskStatus s = it->second->status.load(std::memory_order_relaxed);
        if (s == TaskStatus::Completed ||
            s == TaskStatus::Cancelled ||
            s == TaskStatus::Failed) {
            it = controls_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}
