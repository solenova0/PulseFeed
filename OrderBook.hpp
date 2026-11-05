#pragma once

#include "SpscRing.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace pulsefeed {

enum class Side : std::uint8_t { Buy, Sell };
struct Order {
    std::uint64_t id;
    Side side;
    std::uint32_t price;
    std::uint32_t quantity;
};
enum class Result { Accepted, Invalid, Duplicate, Capacity, FeedFull, NotFound };
const char* result_name(Result result) noexcept;
struct Trade {
    std::uint64_t maker_order_id;
    std::uint64_t taker_order_id;
    std::uint32_t price;
    std::uint32_t quantity;
    Side aggressor_side;
};
struct Level {
    std::uint32_t price{};
    std::uint64_t quantity{};
    std::uint32_t orders{};
};
struct Snapshot {
    std::array<Level, 10> bids{}, asks{};
    std::size_t bid_count{}, ask_count{};
};
struct BookConfig {
    std::uint32_t max_orders{65'536};
    std::uint32_t max_price{20'000};
};

// A single instrument, single writer, integer tick/lot, GTC limit order book.
// The construction-time allocations are the only allocations by this class.
// All methods except try_pop_trade must be called by the owning thread, or
// while it is quiescent. Exactly one consumer may call try_pop_trade.
class OrderBook {
    static constexpr std::uint32_t none = UINT32_MAX;
    struct alignas(64) Node {
        std::uint64_t id{};
        std::uint32_t price{}, quantity{}, previous{none}, next{none};
        Side side{Side::Buy};
        bool active{};
    };
    static_assert(sizeof(Node) == 64);
    struct PriceLevel {
        std::uint64_t quantity{};
        std::uint32_t head{none}, tail{none}, count{};
    };
    struct IdEntry {
        std::uint64_t id{};
        std::uint32_t index{none};
    };
    BookConfig config_;
    std::vector<Node> nodes_;
    std::vector<PriceLevel> bids_, asks_;
    std::vector<std::uint64_t> bid_bits_, ask_bits_;
    std::vector<IdEntry> ids_;
    std::uint32_t free_head_{none};
    std::size_t active_{};
    SpscRing<Trade, 8192> trades_;

    std::size_t hash(std::uint64_t id) const noexcept;
    std::uint32_t find(std::uint64_t id) const noexcept;
    void insert_id(std::uint64_t id, std::uint32_t index) noexcept;
    void erase_id(std::uint64_t id) noexcept;
    PriceLevel& level(Side side, std::uint32_t price) noexcept;
    const PriceLevel& level(Side side, std::uint32_t price) const noexcept;
    void mark(Side side, std::uint32_t price, bool occupied) noexcept;
    std::uint32_t first_ask(std::uint32_t from) const noexcept;
    std::uint32_t first_bid(std::uint32_t from) const noexcept;
    std::uint32_t best_opposite(Side side) const noexcept;
    bool crosses(const Order& order, std::uint32_t price) const noexcept;
    void remove(std::uint32_t index) noexcept;

public:
    explicit OrderBook(BookConfig config = {});
    OrderBook(const OrderBook&) = delete;
    OrderBook& operator=(const OrderBook&) = delete;
    Result submit(Order order) noexcept;
    Result cancel(std::uint64_t id) noexcept;
    bool try_pop_trade(Trade& trade) noexcept { return trades_.try_pop(trade); }
    Snapshot snapshot() const noexcept;
    std::size_t active_orders() const noexcept { return active_; }
    // Expensive diagnostic, intended for tests and offline verification.
    bool validate() const noexcept;
};
} // namespace pulsefeed
