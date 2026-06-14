#include "thread_pool.h"

#include <chrono>
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

thread_local ThreadPool::WorkerContext ThreadPool::tls_ctx_{};

ThreadPool::ThreadPool(std::size_t num_threads) {
    if (num_threads == 0) {
        throw std::invalid_argument("ThreadPool needs at least one thread");
    }

    // Queues first so a worker can index into them the moment it starts.
    queues_.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i) {
        queues_.push_back(std::make_unique<LocalQueue<Task>>());
    }

    workers_.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this, i] { worker_loop(i); });
    }
}

ThreadPool::~ThreadPool() {
    // Signal shutdown and wake everyone. Taking idle_mtx_ before the notify
    // orders the stopping_ store ahead of any worker's decision to sleep, so a
    // worker about to park cannot miss it.
    stopping_.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(idle_mtx_);
    }
    idle_cv_.notify_all();

    // Workers keep popping and stealing until every queue is empty and no task
    // is still running, so a sub-task spawned mid-drain is still picked up.
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

std::optional<ThreadPool::Task> ThreadPool::pop_local_or_steal(std::size_t index) {
    if (std::optional<Task> task = queues_[index]->try_pop()) {
        return task;
    }
    const std::size_t n = queues_.size();
    for (std::size_t offset = 1; offset < n; ++offset) {
        const std::size_t victim = (index + offset) % n;
        if (std::optional<Task> task = queues_[victim]->steal()) {
            steals_.fetch_add(1, std::memory_order_relaxed);
            return task;
        }
    }
    return std::nullopt;
}

bool ThreadPool::has_any_work() const {
    for (const auto& q : queues_) {
        if (!q->empty()) {
            return true;
        }
    }
    return false;
}

void ThreadPool::worker_loop(std::size_t index) {
    tls_ctx_ = WorkerContext{this, index};

    while (true) {
        if (std::optional<Task> task = pop_local_or_steal(index)) {
            active_.fetch_add(1, std::memory_order_acq_rel);
            (*task)();  // status / exception handling lives inside the task
            // fetch_sub returns the previous count; a previous value of 1 means
            // this was the last running task. If we are shutting down, a worker
            // that has already parked needs waking so it can see the drain
            // condition is finally true.
            if (active_.fetch_sub(1, std::memory_order_acq_rel) == 1 &&
                stopping_.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lock(idle_mtx_);
                idle_cv_.notify_all();
            }
            continue;
        }

        std::unique_lock<std::mutex> lock(idle_mtx_);
        if (has_any_work()) {
            continue;  // something arrived while we were scanning
        }
        const bool stopping = stopping_.load(std::memory_order_acquire);
        if (stopping && active_.load(std::memory_order_acquire) == 0) {
            break;  // drained and shutting down
        }
        if (stopping) {
            // Drain tail: a task is still running and may yet spawn more work.
            // Poll briefly instead of waiting outright so a missed completion
            // notify can never wedge shutdown.
            idle_cv_.wait_for(lock, std::chrono::milliseconds(1));
        } else {
            idle_cv_.wait(lock);  // woken by a submission or by shutdown
        }
    }
}

std::size_t ThreadPool::choose_queue() {
    if (tls_ctx_.pool == this) {
        // A worker submitting more work keeps it on its own queue: better
        // locality, and the spawned work stays near the data it came from.
        return tls_ctx_.index;
    }
    return rr_.fetch_add(1, std::memory_order_relaxed) % queues_.size();
}

void ThreadPool::dispatch(Priority priority, Task task) {
    const std::size_t target = choose_queue();
    queues_[target]->push(priority, std::move(task));
    {
        std::lock_guard<std::mutex> lock(idle_mtx_);  // orders push before wait
    }
    idle_cv_.notify_one();
}

void ThreadPool::push_to_queue_for_test(std::size_t index, Priority priority,
                                        Task task) {
    queues_[index]->push(priority, std::move(task));
    {
        std::lock_guard<std::mutex> lock(idle_mtx_);
    }
    idle_cv_.notify_one();
}

TaskId ThreadPool::enqueue_cancellable(std::function<void()> work) {
    const TaskId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    auto control = std::make_shared<Control>();
    {
        std::lock_guard<std::mutex> lock(controls_mutex_);
        controls_.emplace(id, control);
    }

    dispatch(Priority::Normal, [control, work = std::move(work)] {
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

std::size_t ThreadPool::pending() const {
    std::size_t total = 0;
    for (const auto& q : queues_) {
        total += q->size();
    }
    return total;
}
