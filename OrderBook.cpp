#include "OrderBook.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <exception>
#include <limits>
#include <stdexcept>

namespace pulsefeed {
namespace {
BookConfig checked(BookConfig config) {
    if (config.max_orders == 0 || config.max_orders >= UINT32_MAX ||
        config.max_price == 0 || config.max_price > 10'000'000)
        throw std::invalid_argument("invalid order/price capacity");
    return config;
}
}

const char* result_name(Result result) noexcept {
    switch (result) {
    case Result::Accepted: return "accepted";
    case Result::Invalid: return "invalid";
    case Result::Duplicate: return "duplicate";
    case Result::Capacity: return "capacity";
    case Result::FeedFull: return "feed_full";
    case Result::NotFound: return "not_found";
    }
    return "unknown";
}

OrderBook::OrderBook(BookConfig config)
    : config_(checked(config)), nodes_(config_.max_orders),
      bids_(static_cast<std::size_t>(config_.max_price) + 1), asks_(bids_.size()),
      bid_bits_(bids_.size() / 64 + 1), ask_bits_(bid_bits_.size()),
      ids_(std::bit_ceil(static_cast<std::size_t>(config_.max_orders) * 2)), free_head_(0) {
    for (std::uint32_t i = 0; i < config_.max_orders; ++i)
        nodes_[i].next = i + 1 == config_.max_orders ? none : i + 1;
}

std::size_t OrderBook::hash(std::uint64_t id) const noexcept {
    // SplitMix64 finalizer: predictable bounded storage, well-mixed integer IDs.
    id ^= id >> 30; id *= 0xbf58476d1ce4e5b9ULL;
    id ^= id >> 27; id *= 0x94d049bb133111ebULL;
    id ^= id >> 31;
    return static_cast<std::size_t>(id) & (ids_.size() - 1);
}
std::uint32_t OrderBook::find(std::uint64_t id) const noexcept {
    if (id == 0) return none;
    auto slot = hash(id);
    while (ids_[slot].id != 0) {
        if (ids_[slot].id == id) return ids_[slot].index;
        slot = (slot + 1) & (ids_.size() - 1);
    }
    return none;
}
void OrderBook::insert_id(std::uint64_t id, std::uint32_t index) noexcept {
    auto slot = hash(id);
    while (ids_[slot].id != 0) slot = (slot + 1) & (ids_.size() - 1);
    ids_[slot] = {id, index};
}
void OrderBook::erase_id(std::uint64_t id) noexcept {
    const auto mask = ids_.size() - 1;
    auto hole = hash(id);
    while (ids_[hole].id != id) hole = (hole + 1) & mask;
    // Backward-shift deletion avoids tombstone accumulation under churn.
    for (auto slot = (hole + 1) & mask; ids_[slot].id != 0; slot = (slot + 1) & mask) {
        const auto home = hash(ids_[slot].id);
        if (((slot - home) & mask) >= ((slot - hole) & mask)) {
            ids_[hole] = ids_[slot];
            hole = slot;
        }
    }
    ids_[hole] = {};
}
OrderBook::PriceLevel& OrderBook::level(Side side, std::uint32_t price) noexcept {
    return side == Side::Buy ? bids_[price] : asks_[price];
}
const OrderBook::PriceLevel& OrderBook::level(Side side, std::uint32_t price) const noexcept {
    return side == Side::Buy ? bids_[price] : asks_[price];
}
void OrderBook::mark(Side side, std::uint32_t price, bool occupied) noexcept {
    auto& bits = side == Side::Buy ? bid_bits_ : ask_bits_;
    const auto bit = std::uint64_t{1} << (price % 64);
    if (occupied) bits[price / 64] |= bit;
    else bits[price / 64] &= ~bit;
}
std::uint32_t OrderBook::first_ask(std::uint32_t from) const noexcept {
    if (from > config_.max_price) return 0;
    auto word = static_cast<std::size_t>(from / 64);
    auto bits = ask_bits_[word] & (~std::uint64_t{0} << (from % 64));
    for (;;) {
        if (bits != 0) return static_cast<std::uint32_t>(word * 64 + std::countr_zero(bits));
        if (++word == ask_bits_.size()) return 0;
        bits = ask_bits_[word];
    }
}
std::uint32_t OrderBook::first_bid(std::uint32_t from) const noexcept {
    auto word = static_cast<std::size_t>(from / 64);
    auto bits = bid_bits_[word] & (~std::uint64_t{0} >> (63 - from % 64));
    for (;;) {
        if (bits != 0) return static_cast<std::uint32_t>(word * 64 + 63 - std::countl_zero(bits));
        if (word == 0) return 0;
        bits = bid_bits_[--word];
    }
}
std::uint32_t OrderBook::best_opposite(Side side) const noexcept {
    return side == Side::Buy ? first_ask(1) : first_bid(config_.max_price);
}
bool OrderBook::crosses(const Order& order, std::uint32_t price) const noexcept {
    return price != 0 && (order.side == Side::Buy ? order.price >= price : order.price <= price);
}

void OrderBook::remove(std::uint32_t index) noexcept {
    auto& node = nodes_[index];
    auto& price = level(node.side, node.price);
    price.quantity -= node.quantity;
    --price.count;
    if (node.previous == none) price.head = node.next;
    else nodes_[node.previous].next = node.next;
    if (node.next == none) price.tail = node.previous;
    else nodes_[node.next].previous = node.previous;
    if (price.count == 0) mark(node.side, node.price, false);
    erase_id(node.id);
    node.active = false;
    node.next = free_head_;
    free_head_ = index;
    --active_;
}

Result OrderBook::submit(Order order) noexcept {
    if (order.id == 0 || order.price == 0 || order.price > config_.max_price ||
        order.quantity == 0 || (order.side != Side::Buy && order.side != Side::Sell))
        return Result::Invalid;
    if (find(order.id) != none) return Result::Duplicate;
    // Conservative admission: a full pool rejects even a fully crossing order.
    if (free_head_ == none) return Result::Capacity;

    // Reserve feed space logically before modifying any book state. There is
    // only one producer; the consumer can only create additional free space.
    const auto available = trades_.free_slots();
    std::size_t required = 0;
    auto remaining = order.quantity;
    const auto opposite = order.side == Side::Buy ? Side::Sell : Side::Buy;
    for (auto price = best_opposite(order.side); crosses(order, price) && remaining != 0;) {
        for (auto index = level(opposite, price).head; index != none && remaining != 0;
             index = nodes_[index].next) {
            if (++required > available) return Result::FeedFull;
            remaining -= std::min(remaining, nodes_[index].quantity);
        }
        if (remaining == 0) break;
        price = order.side == Side::Buy ? first_ask(price + 1) : first_bid(price - 1);
    }

    remaining = order.quantity;
    for (auto price = best_opposite(order.side); crosses(order, price) && remaining != 0;
         price = best_opposite(order.side)) {
        const auto index = level(opposite, price).head;
        auto& maker = nodes_[index];
        const auto quantity = std::min(remaining, maker.quantity);
        maker.quantity -= quantity;
        level(opposite, price).quantity -= quantity;
        remaining -= quantity;
        if (!trades_.try_push({maker.id, order.id, price, quantity, order.side}))
            std::terminate(); // Unreachable under the documented SPSC ownership.
        if (maker.quantity == 0) remove(index);
    }
    if (remaining != 0) {
        const auto index = free_head_;
        auto& node = nodes_[index];
        free_head_ = node.next;
        auto& price = level(order.side, order.price);
        node.id = order.id; node.side = order.side; node.price = order.price;
        node.quantity = remaining; node.previous = price.tail;
        node.next = none; node.active = true;
        if (price.tail == none) price.head = index;
        else nodes_[price.tail].next = index;
        price.tail = index;
        price.quantity += remaining;
        ++price.count;
        mark(order.side, order.price, true);
        insert_id(order.id, index);
        ++active_;
    }
    return Result::Accepted;
}

Result OrderBook::cancel(std::uint64_t id) noexcept {
    const auto index = find(id);
    if (index == none) return Result::NotFound;
    remove(index);
    return Result::Accepted;
}

Snapshot OrderBook::snapshot() const noexcept {
    Snapshot result;
    for (auto price = first_bid(config_.max_price); price != 0 && result.bid_count < 10;
         price = first_bid(price - 1)) {
        const auto& entry = bids_[price];
        result.bids[result.bid_count++] = {price, entry.quantity, entry.count};
    }
    for (auto price = first_ask(1); price != 0 && result.ask_count < 10;
         price = first_ask(price + 1)) {
        const auto& entry = asks_[price];
        result.asks[result.ask_count++] = {price, entry.quantity, entry.count};
    }
    return result;
}

bool OrderBook::validate() const noexcept {
    std::size_t counted = 0;
    for (auto side : {Side::Buy, Side::Sell}) {
        const auto& bitmap = side == Side::Buy ? bid_bits_ : ask_bits_;
        for (std::uint32_t p = 1; p <= config_.max_price; ++p) {
            const auto& entry = level(side, p);
            std::uint64_t quantity = 0;
            std::uint32_t count = 0, previous = none;
            for (auto i = entry.head; i != none; i = nodes_[i].next) {
                if (i >= nodes_.size() || ++count > active_) return false;
                const auto& node = nodes_[i];
                if (!node.active || node.side != side || node.price != p ||
                    node.quantity == 0 || node.previous != previous || find(node.id) != i) return false;
                quantity += node.quantity;
                previous = i;
            }
            if (count != entry.count || quantity != entry.quantity || previous != entry.tail ||
                ((bitmap[p / 64] >> (p % 64)) & 1) != (count != 0)) return false;
            counted += count;
        }
    }
    if (counted != active_) return false;
    std::size_t free_count = 0, mapped_count = 0;
    for (auto i = free_head_; i != none; i = nodes_[i].next) {
        if (i >= nodes_.size() || nodes_[i].active || ++free_count > nodes_.size()) return false;
    }
    for (const auto& entry : ids_) if (entry.id != 0) {
        ++mapped_count;
        if (entry.index >= nodes_.size() || !nodes_[entry.index].active ||
            nodes_[entry.index].id != entry.id) return false;
    }
    const auto bid = first_bid(config_.max_price), ask = first_ask(1);
    return counted + free_count == nodes_.size() && mapped_count == active_ &&
        (bid == 0 || ask == 0 || bid < ask);
}
} // namespace pulsefeed
