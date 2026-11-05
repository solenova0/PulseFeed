#include "OrderBook.hpp"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <map>
#include <random>
#include <thread>
#include <vector>

using namespace pulsefeed;
#define CHECK(condition) do { if (!(condition)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": " #condition " failed\n"; std::exit(1); \
} } while (false)

static bool same(const Trade& a, const Trade& b) {
    return a.maker_order_id == b.maker_order_id && a.taker_order_id == b.taker_order_id &&
        a.price == b.price && a.quantity == b.quantity && a.aggressor_side == b.aggressor_side;
}
static void priority() {
    OrderBook book({32, 256});
    CHECK(book.submit({1, Side::Sell, 101, 7}) == Result::Accepted);
    CHECK(book.submit({2, Side::Sell, 100, 3}) == Result::Accepted);
    CHECK(book.submit({3, Side::Sell, 100, 5}) == Result::Accepted);
    CHECK(book.submit({4, Side::Buy, 101, 10}) == Result::Accepted);
    Trade t{};
    CHECK(book.try_pop_trade(t) && same(t, {2, 4, 100, 3, Side::Buy}));
    CHECK(book.try_pop_trade(t) && same(t, {3, 4, 100, 5, Side::Buy}));
    CHECK(book.try_pop_trade(t) && same(t, {1, 4, 101, 2, Side::Buy}));
    CHECK(!book.try_pop_trade(t));
    auto depth = book.snapshot();
    CHECK(depth.bid_count == 0 && depth.ask_count == 1 && depth.asks[0].quantity == 5);
    CHECK(book.cancel(1) == Result::Accepted && book.cancel(1) == Result::NotFound);
    CHECK(book.validate());
    CHECK(book.submit({5, Side::Buy, 101, 7}) == Result::Accepted);
    CHECK(book.submit({6, Side::Buy, 102, 3}) == Result::Accepted);
    CHECK(book.submit({7, Side::Sell, 100, 6}) == Result::Accepted);
    CHECK(book.try_pop_trade(t) && same(t, {6, 7, 102, 3, Side::Sell}));
    CHECK(book.try_pop_trade(t) && same(t, {5, 7, 101, 3, Side::Sell}));
    CHECK(book.validate());
}

static void boundaries() {
    OrderBook book({32, 256});
    CHECK(book.submit({0, Side::Buy, 100, 1}) == Result::Invalid);
    CHECK(book.submit({1, Side::Buy, 0, 1}) == Result::Invalid);
    CHECK(book.submit({1, Side::Buy, 257, 1}) == Result::Invalid);
    CHECK(book.submit({1, Side::Buy, 1, 0}) == Result::Invalid);
    CHECK(book.submit({1, static_cast<Side>(99), 1, 1}) == Result::Invalid);
    std::uint64_t id = 1;
    for (const auto p : {1U, 63U, 64U, 65U, 127U, 128U, 255U, 256U}) {
        CHECK(book.submit({id++, Side::Buy, p, 1}) == Result::Accepted);
        CHECK(book.validate());
    }
    auto depth = book.snapshot();
    CHECK(depth.bid_count == 8 && depth.bids[0].price == 256 && depth.bids[7].price == 1);
    for (std::uint64_t i = 1; i < id; ++i) CHECK(book.cancel(i) == Result::Accepted);
    CHECK(book.submit({UINT64_MAX, Side::Sell, 256, UINT32_MAX}) == Result::Accepted);
    CHECK(book.submit({UINT64_MAX, Side::Buy, 1, 1}) == Result::Duplicate);
    CHECK(book.submit({100, Side::Sell, 256, UINT32_MAX}) == Result::Accepted);
    CHECK(book.snapshot().asks[0].quantity == std::uint64_t{UINT32_MAX} * 2);
    CHECK(book.cancel(UINT64_MAX) == Result::Accepted);
    CHECK(book.cancel(100) == Result::Accepted && book.validate());

    OrderBook full({2, 128});
    CHECK(full.submit({1, Side::Sell, 100, 1}) == Result::Accepted);
    CHECK(full.submit({2, Side::Sell, 101, 1}) == Result::Accepted);
    CHECK(full.submit({3, Side::Buy, 101, 2}) == Result::Capacity);
    CHECK(full.active_orders() == 2 && full.validate());
    CHECK(full.cancel(1) == Result::Accepted);
    CHECK(full.submit({3, Side::Buy, 101, 2}) == Result::Accepted);
    CHECK(full.snapshot().bids[0].quantity == 1 && full.validate());
    OrderBook tiny({1, 1});
    CHECK(tiny.submit({1, Side::Buy, 1, 1}) == Result::Accepted);
    CHECK(tiny.validate());
}

static void feed_admission() {
    OrderBook book({16, 128});
    for (std::uint64_t i = 0; i < 8192; ++i) {
        CHECK(book.submit({2 * i + 1, Side::Sell, 100, 1}) == Result::Accepted);
        CHECK(book.submit({2 * i + 2, Side::Buy, 100, 1}) == Result::Accepted);
    }
    CHECK(book.submit({20000, Side::Sell, 100, 1}) == Result::Accepted);
    CHECK(book.submit({20001, Side::Sell, 101, 1}) == Result::Accepted);
    CHECK(book.submit({20002, Side::Buy, 101, 2}) == Result::FeedFull);
    CHECK(book.active_orders() == 2 && book.validate());
    Trade t{};
    CHECK(book.try_pop_trade(t));
    CHECK(book.submit({20002, Side::Buy, 101, 2}) == Result::FeedFull);
    CHECK(book.snapshot().asks[0].quantity == 1 && book.snapshot().ask_count == 2);
    CHECK(book.try_pop_trade(t));
    CHECK(book.submit({20002, Side::Buy, 101, 2}) == Result::Accepted);
    CHECK(book.active_orders() == 0 && book.validate());
    for (std::size_t i = 0; i < 8190; ++i) CHECK(book.try_pop_trade(t));
    CHECK(book.try_pop_trade(t) && t.maker_order_id == 20000);
    CHECK(book.try_pop_trade(t) && t.maker_order_id == 20001);
    CHECK(!book.try_pop_trade(t));
}

// Deliberately simple independent reference: scan a chronological vector.
struct Reference {
    std::vector<Order> orders;
    std::vector<Trade> trades;
    Result submit(Order order) {
        trades.clear();
        if (!order.id || !order.price || order.price > 256 || !order.quantity) return Result::Invalid;
        for (const auto& active : orders) if (active.id == order.id) return Result::Duplicate;
        if (orders.size() == 128) return Result::Capacity;
        while (order.quantity != 0) {
            std::size_t best = orders.size();
            for (std::size_t i = 0; i < orders.size(); ++i) {
                const auto& candidate = orders[i];
                if (candidate.side == order.side || (order.side == Side::Buy ?
                    candidate.price > order.price : candidate.price < order.price)) continue;
                if (best == orders.size() || (order.side == Side::Buy ?
                    candidate.price < orders[best].price : candidate.price > orders[best].price)) best = i;
            }
            if (best == orders.size()) break;
            auto& maker = orders[best];
            const auto quantity = std::min(maker.quantity, order.quantity);
            trades.push_back({maker.id, order.id, maker.price, quantity, order.side});
            maker.quantity -= quantity;
            order.quantity -= quantity;
            if (maker.quantity == 0) orders.erase(orders.begin() + static_cast<std::ptrdiff_t>(best));
        }
        if (order.quantity != 0) orders.push_back(order);
        return Result::Accepted;
    }
    Result cancel(std::uint64_t id) {
        trades.clear();
        auto it = std::find_if(orders.begin(), orders.end(), [id](const Order& o) { return o.id == id; });
        if (it == orders.end()) return Result::NotFound;
        orders.erase(it);
        return Result::Accepted;
    }
    void compare(const OrderBook& book) const {
        CHECK(book.active_orders() == orders.size());
        std::map<std::uint32_t, Level> bids, asks;
        for (auto order : orders) {
            auto& l = (order.side == Side::Buy ? bids : asks)[order.price];
            l.price = order.price; l.quantity += order.quantity; ++l.orders;
        }
        const auto snapshot = book.snapshot();
        CHECK(snapshot.bid_count == std::min<std::size_t>(10, bids.size()));
        CHECK(snapshot.ask_count == std::min<std::size_t>(10, asks.size()));
        std::size_t i = 0;
        for (auto it = bids.rbegin(); it != bids.rend() && i < 10; ++it, ++i) {
            const auto& actual = snapshot.bids[i];
            CHECK(actual.price == it->first && actual.quantity == it->second.quantity && actual.orders == it->second.orders);
        }
        i = 0;
        for (const auto& [price, expected] : asks) {
            if (i == 10) break;
            const auto& actual = snapshot.asks[i++];
            CHECK(actual.price == price && actual.quantity == expected.quantity && actual.orders == expected.orders);
        }
    }
};

static void differential() {
    OrderBook book({128, 256});
    Reference reference;
    std::mt19937_64 random(0xfeed1234);
    for (std::uint64_t i = 0; i < 60000; ++i) {
        const auto id = random() % 500 + 1;
        if (random() % 4 == 0) CHECK(book.cancel(id) == reference.cancel(id));
        else {
            const Order order{id, random() % 2 ? Side::Buy : Side::Sell,
                static_cast<std::uint32_t>(random() % 256 + 1),
                static_cast<std::uint32_t>(random() % 50 + 1)};
            CHECK(book.submit(order) == reference.submit(order));
        }
        Trade actual{};
        for (const auto& expected : reference.trades) CHECK(book.try_pop_trade(actual) && same(actual, expected));
        CHECK(!book.try_pop_trade(actual));
        reference.compare(book);
        if (i % 64 == 0) CHECK(book.validate());
    }
    CHECK(book.validate());
}

static void ring_stress() {
    struct Payload { std::uint64_t sequence, complement; };
    SpscRing<Payload, 1024> ring;
    constexpr std::uint64_t count = 1'000'000;
    std::thread producer([&] {
        for (std::uint64_t i = 0; i < count; ++i)
            while (!ring.try_push({i, ~i})) std::this_thread::yield();
    });
    for (std::uint64_t i = 0; i < count; ++i) {
        Payload payload{};
        while (!ring.try_pop(payload)) std::this_thread::yield();
        CHECK(payload.sequence == i && payload.complement == ~i);
    }
    producer.join();
    CHECK(ring.size() == 0 && ring.free_slots() == 1024);
    for (std::uint64_t i = 0; i < 1024; ++i) CHECK(ring.try_push({i, ~i}));
    CHECK(!ring.try_push({0, 0}));
    for (std::uint64_t i = 0; i < 1024; ++i) {
        Payload p{}; CHECK(ring.try_pop(p) && p.sequence == i);
    }
}

int main() {
    priority(); boundaries(); feed_admission(); differential(); ring_stress();
    std::cout << "PASS: price/time priority, boundaries, admission, 60000 reference commands, 1000000 SPSC messages\n";
}
