#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace pulsefeed {

// Exactly one producer and one consumer. N slots are usable. Construction and
// destruction require both threads to be stopped. No allocation or retry loops.
template <typename T, std::size_t N>
class SpscRing {
    static_assert(N > 0 && (N & (N - 1)) == 0, "capacity must be a power of two");
    static_assert(N < (std::uint64_t{1} << 63));
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    struct alignas(64) Cursor { std::atomic<std::uint64_t> value{0}; };
    Cursor head_;
    Cursor tail_;
    alignas(64) std::array<T, N> slots_{};

public:
    SpscRing() = default;
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    bool try_push(const T& value) noexcept {
        const auto head = head_.value.load(std::memory_order_relaxed);
        const auto tail = tail_.value.load(std::memory_order_acquire);
        if (head - tail == N) return false;
        slots_[head & (N - 1)] = value;
        head_.value.store(head + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& value) noexcept {
        const auto tail = tail_.value.load(std::memory_order_relaxed);
        const auto head = head_.value.load(std::memory_order_acquire);
        if (head == tail) return false;
        value = slots_[tail & (N - 1)];
        tail_.value.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Only the producer may use this for admission. A concurrent consumer can
    // only increase free space, so the returned value is conservative.
    std::size_t free_slots() const noexcept {
        const auto head = head_.value.load(std::memory_order_relaxed);
        const auto tail = tail_.value.load(std::memory_order_acquire);
        return N - static_cast<std::size_t>(head - tail);
    }

    // Telemetry only: this is not a synchronized snapshot of both cursors.
    std::size_t size() const noexcept {
        const auto tail = tail_.value.load(std::memory_order_acquire);
        const auto head = head_.value.load(std::memory_order_acquire);
        return static_cast<std::size_t>(std::min<std::uint64_t>(head - tail, N));
    }

    static constexpr std::size_t capacity() noexcept { return N; }
};
} // namespace pulsefeed
