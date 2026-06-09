#ifndef THREAD_POOL_BLOCKING_QUEUE_H
#define THREAD_POOL_BLOCKING_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

// A multi-producer / multi-consumer FIFO queue.
//
// Consumers call pop() and block until an item is available. Once close() is
// called the queue stops accepting work: pending items are still drained, but
// pop() on an empty closed queue reports closure instead of blocking forever.
template <typename T>
class BlockingQueue {
public:
    BlockingQueue() = default;

    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;

    void push(T item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        // Wake a single waiter outside the lock so it does not immediately
        // contend on the mutex we still hold.
        not_empty_.notify_one();
    }

    // Blocks until an item is ready or the queue is closed and empty.
    // Returns std::nullopt only in the latter case.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });

        if (queue_.empty()) {
            return std::nullopt;  // closed and drained
        }

        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    // Non-blocking variant. Returns std::nullopt if nothing is queued.
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
    }

    bool is_closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    bool closed_ = false;
};

#endif  // THREAD_POOL_BLOCKING_QUEUE_H
