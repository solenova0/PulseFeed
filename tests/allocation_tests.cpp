#include "OrderBook.hpp"
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <new>

static std::atomic<bool> tracking{false};
static std::atomic<std::size_t> allocations{0};
static std::atomic<std::size_t> c_allocations{0};
static void count() noexcept { if (tracking.load(std::memory_order_relaxed)) ++allocations; }
#if defined(PULSEFEED_WRAP_C_ALLOC)
// GNU ld wrapping also catches direct C allocation calls from the core object.
// Shared-library internals are outside this instrumentation boundary.
extern "C" {
void* __real_malloc(std::size_t);
void* __real_calloc(std::size_t, std::size_t);
void* __real_realloc(void*, std::size_t);
void* __real_aligned_alloc(std::size_t, std::size_t);
void* __wrap_malloc(std::size_t size) {
    if (tracking.load(std::memory_order_relaxed)) ++c_allocations;
    return __real_malloc(size);
}
void* __wrap_calloc(std::size_t n, std::size_t size) {
    if (tracking.load(std::memory_order_relaxed)) ++c_allocations;
    return __real_calloc(n, size);
}
void* __wrap_realloc(void* ptr, std::size_t size) {
    if (tracking.load(std::memory_order_relaxed)) ++c_allocations;
    return __real_realloc(ptr, size);
}
void* __wrap_aligned_alloc(std::size_t alignment, std::size_t size) {
    if (tracking.load(std::memory_order_relaxed)) ++c_allocations;
    return __real_aligned_alloc(alignment, size);
}
}
#endif
void* operator new(std::size_t size) {
    count();
    if (void* p = std::malloc(size == 0 ? 1 : size)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void* operator new(std::size_t size, std::align_val_t alignment) {
    count();
    auto a = static_cast<std::size_t>(alignment);
    if (void* p = std::aligned_alloc(a, ((size == 0 ? 1 : size) + a - 1) / a * a)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size, std::align_val_t alignment) { return ::operator new(size, alignment); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

int main() {
    using namespace pulsefeed;
    OrderBook book({256, 256});
    bool correct = true;
    tracking.store(true);
    for (std::uint64_t i = 1; i <= 20000; ++i) {
        correct &= book.submit({i * 3, Side::Sell, 100, 5}) == Result::Accepted;
        correct &= book.submit({i * 3 + 1, Side::Buy, 101, 3}) == Result::Accepted;
        Trade trade{};
        correct &= book.try_pop_trade(trade) && trade.quantity == 3;
        correct &= book.submit({i * 3, Side::Sell, 100, 1}) == Result::Duplicate;
        const auto snapshot = book.snapshot();
        correct &= snapshot.ask_count == 1 && snapshot.asks[0].quantity == 2;
        correct &= book.cancel(i * 3) == Result::Accepted;
        correct &= book.cancel(i * 3) == Result::NotFound;
    }
    tracking.store(false);
    if (!correct || allocations.load() != 0 || c_allocations.load() != 0 || !book.validate()) {
        std::cerr << "FAIL: hot-path C++ allocations=" << allocations.load()
                  << " C allocations=" << c_allocations.load() << '\n';
        return 1;
    }
    std::cout << "PASS: zero C++ and wrapped C allocations across 20000 insertion/match/cancel/snapshot cycles\n";
}
