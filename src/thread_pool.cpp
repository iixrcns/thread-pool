#include "thread_pool.h"

#include <stdexcept>

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
