#ifndef THREAD_POOL_BLOCKING_QUEUE_H
#define THREAD_POOL_BLOCKING_QUEUE_H

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

// Relative urgency of a queued item. Higher values are served first; items of
// the same level keep their submission order (see the sequence tie-break below).
enum class Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
};

// A multi-producer / multi-consumer queue ordered by priority, falling back to
// FIFO within a single priority level.
//
// Consumers call pop() and block until an item is available. Once close() is
// called the queue stops accepting work: pending items are still drained, but
// pop() on an empty closed queue reports closure instead of blocking forever.
//
// push(item) without a priority is treated as Priority::Normal, so code that
// does not care about ordering sees a plain FIFO queue.
template <typename T>
class BlockingQueue {
public:
    BlockingQueue() = default;

    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;

    void push(T item) {
        push(Priority::Normal, std::move(item));
    }

    void push(Priority priority, T item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // seq is assigned under the lock so two items pushed at the same
            // priority keep the order their producers reached the queue in.
            queue_.push(Entry{priority, next_seq_++, std::move(item)});
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
        return take_top();
    }

    // Non-blocking variant. Returns std::nullopt if nothing is queued.
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        return take_top();
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
    struct Entry {
        Priority priority;
        std::uint64_t seq;
        T item;
    };

    // Orders the underlying heap. std::priority_queue keeps the "greatest"
    // entry on top, so "greater" has to mean "served sooner": higher priority
    // wins, and on a tie the smaller sequence number (the older item) wins.
    // The payload itself is never compared, which lets T be non-comparable.
    struct ServedLater {
        bool operator()(const Entry& a, const Entry& b) const {
            if (a.priority != b.priority) {
                return a.priority < b.priority;
            }
            return a.seq > b.seq;
        }
    };

    // Caller must hold the mutex and have checked the queue is non-empty.
    // top() hands back a const reference, so the move-out needs a const_cast;
    // it is safe because the entry is popped on the next line and never read
    // again. This is the standard way to extract from std::priority_queue.
    T take_top() {
        T item = std::move(const_cast<Entry&>(queue_.top()).item);
        queue_.pop();
        return item;
    }

    std::priority_queue<Entry, std::vector<Entry>, ServedLater> queue_;
    std::uint64_t next_seq_ = 0;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    bool closed_ = false;
};

#endif  // THREAD_POOL_BLOCKING_QUEUE_H
