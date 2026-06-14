#ifndef THREAD_POOL_LOCAL_QUEUE_H
#define THREAD_POOL_LOCAL_QUEUE_H

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

// A single worker's queue: a priority queue guarded by one mutex, with
// non-blocking access only. It keeps the same priority + sequence ordering the
// single shared queue used to have, but the blocking and close logic is gone -
// sleeping is the pool's job now, coordinated across all queues at once.
//
// The owner pops from it with try_pop(); other workers reach into it with
// steal(). For this locked design the two are the same operation; they are kept
// separate so the call sites read clearly and so a future lock-free queue can
// hand the owner and the thieves opposite ends without touching callers.
//
// push(item) without a priority is treated as Priority::Normal, so code that
// does not care about ordering sees a plain FIFO queue.
template <typename T>
class LocalQueue {
public:
    LocalQueue() = default;

    LocalQueue(const LocalQueue&) = delete;
    LocalQueue& operator=(const LocalQueue&) = delete;

    void push(T item) {
        push(Priority::Normal, std::move(item));
    }

    void push(Priority priority, T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        // seq is assigned under the lock so two items pushed at the same
        // priority keep the order their producers reached the queue in.
        queue_.push(Entry{priority, next_seq_++, std::move(item)});
    }

    // Returns std::nullopt if nothing is queued, otherwise the top item.
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        return take_top();
    }

    // Identical to try_pop() for the locked queue. A thief calls this instead so
    // the worker loop reads the way the algorithm is described, and so the
    // lock-free follow-up can take from the far end here without a call-site
    // change.
    std::optional<T> steal() {
        return try_pop();
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
};

#endif  // THREAD_POOL_LOCAL_QUEUE_H
