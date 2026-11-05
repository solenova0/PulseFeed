#include "OrderBook.hpp"
#include "SpscRing.hpp"
#include "pulsefeed.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace {
using namespace std::chrono_literals;
namespace wire = pulsefeed::v1;
volatile std::sig_atomic_t interrupted = 0;
void handle_signal(int) { interrupted = 1; }

std::string env(const char* name, std::string fallback = {}) {
    const auto* value = std::getenv(name);
    return value == nullptr ? std::move(fallback) : std::string(value);
}
std::uint64_t env_number(const char* name, std::uint64_t fallback,
                         std::uint64_t minimum, std::uint64_t maximum) {
    const auto value = env(name);
    if (value.empty()) return fallback;
    std::uint64_t number{};
    const auto result = std::from_chars(value.data(), value.data() + value.size(), number);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size() ||
        number < minimum || number > maximum) {
        throw std::runtime_error(std::string("invalid ") + name);
    }
    return number;
}
bool env_bool(const char* name, bool fallback) {
    const auto value = env(name);
    if (value.empty()) return fallback;
    if (value == "true" || value == "1") return true;
    if (value == "false" || value == "0") return false;
    throw std::runtime_error(std::string("invalid boolean ") + name);
}
std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("cannot read TLS file: " + path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}
std::uint64_t unix_nanos() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}
std::string new_epoch() {
    std::random_device random;
    std::ostringstream output;
    output << std::hex << unix_nanos() << '-' << random() << random() << '-' << getpid();
    return output.str();
}

struct Metrics {
    std::atomic<std::uint64_t> commands{0}, rejected{0}, trades{0}, feed_dropped{0};
    std::atomic<std::uint64_t> ingress_full{0}, subscriber_dropped{0}, published{0};
    std::atomic<std::uint64_t> active_orders{0}, subscribers{0}, sequence{0};
};

// Only this fixed-size, trivially copyable object crosses the matching boundary.
struct alignas(64) Envelope {
    pulsefeed::Snapshot depth{};
    pulsefeed::Trade trade{};
    std::uint64_t sequence{};
    std::uint64_t unix_nanos{};
    bool has_trade{};
};
static_assert(std::is_trivially_copyable_v<Envelope>);

struct CommandOutcome {
    pulsefeed::Result result{pulsefeed::Result::Invalid};
    std::uint64_t sequence{};
    bool unavailable{};
};
struct Command {
    pulsefeed::Order order{};
    bool cancel{};
    std::promise<CommandOutcome> done;
};
struct Subscriber {
    std::mutex mutex;
    std::condition_variable changed;
    std::deque<wire::FeedEvent> pending;
    grpc::ServerContext* context{}; // Protected by mutex, nulled before handler returns.
    bool failed{};
};

class Engine final : public wire::MarketData::Service {
public:
    static constexpr std::size_t ingress_capacity = 4096;
    static constexpr std::size_t subscriber_capacity = 256;
    static constexpr std::size_t maximum_subscribers = 256;

    Engine(bool demo, std::chrono::microseconds interval)
        : epoch_(new_epoch()), token_(env("ENGINE_TOKEN")), demo_(demo), interval_(interval) {
        cached_.set_epoch(epoch_);
        cached_.set_snapshot(true);
        cached_.set_unix_nanos(unix_nanos());
    }
    ~Engine() override { stop(); }
    void start() {
        running_.store(true, std::memory_order_release);
        matcher_ = std::thread([this] { matching_loop(); });
        dispatcher_ = std::thread([this] { dispatch_loop(); });
        if (demo_) demo_thread_ = std::thread([this] { demo_loop(); });
    }
    void stop() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) return;
        ingress_changed_.notify_all();
        demo_changed_.notify_all();
        if (demo_thread_.joinable()) demo_thread_.join();
        if (matcher_.joinable()) matcher_.join();
        matcher_done_.store(true, std::memory_order_release);
        output_signal_.fetch_add(1, std::memory_order_release);
        output_signal_.notify_one();
        if (dispatcher_.joinable()) dispatcher_.join();
        std::lock_guard registry_lock(subscribers_mutex_);
        for (const auto& weak : subscribers_) {
            if (auto subscriber = weak.lock()) {
                std::lock_guard lock(subscriber->mutex);
                subscriber->failed = true;
                if (subscriber->context != nullptr) subscriber->context->TryCancel();
                subscriber->changed.notify_all();
            }
        }
    }
    const Metrics& metrics() const { return metrics_; }
    bool running() const { return running_.load(std::memory_order_acquire); }

    grpc::Status Submit(grpc::ServerContext* context, const wire::OrderRequest* request,
                        wire::CommandReply* reply) override {
        if (!authorized(context)) return {grpc::StatusCode::UNAUTHENTICATED, "invalid token"};
        if (request->side() != wire::BUY && request->side() != wire::SELL) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "side must be BUY or SELL"};
        }
        auto command = std::make_shared<Command>();
        command->order = {request->order_id(), request->side() == wire::BUY
            ? pulsefeed::Side::Buy : pulsefeed::Side::Sell, request->price(), request->quantity()};
        return execute(context, std::move(command), reply);
    }
    grpc::Status Cancel(grpc::ServerContext* context, const wire::CancelRequest* request,
                        wire::CommandReply* reply) override {
        if (!authorized(context)) return {grpc::StatusCode::UNAUTHENTICATED, "invalid token"};
        auto command = std::make_shared<Command>();
        command->cancel = true;
        command->order.id = request->order_id();
        return execute(context, std::move(command), reply);
    }
    grpc::Status Subscribe(grpc::ServerContext* context, const wire::SubscribeRequest*,
                           grpc::ServerWriter<wire::FeedEvent>* writer) override {
        if (!authorized(context)) return {grpc::StatusCode::UNAUTHENTICATED, "invalid token"};
        auto subscriber = std::make_shared<Subscriber>();
        subscriber->context = context;
        {
            std::lock_guard registry_lock(subscribers_mutex_);
            std::erase_if(subscribers_, [](const auto& item) { return item.expired(); });
            if (!running()) return {grpc::StatusCode::UNAVAILABLE, "engine is stopping"};
            if (subscribers_.size() >= maximum_subscribers) {
                return {grpc::StatusCode::RESOURCE_EXHAUSTED, "subscriber limit reached"};
            }
            auto snapshot = cached_;
            snapshot.clear_trade();
            snapshot.set_snapshot(true);
            subscriber->pending.push_back(std::move(snapshot));
            subscribers_.push_back(subscriber);
            metrics_.subscribers.fetch_add(1, std::memory_order_relaxed);
        }
        grpc::Status status;
        while (running() && !context->IsCancelled()) {
            wire::FeedEvent event;
            {
                std::unique_lock lock(subscriber->mutex);
                subscriber->changed.wait_for(lock, 100ms, [&] {
                    return !subscriber->pending.empty() || subscriber->failed || !running();
                });
                if (subscriber->failed) {
                    status = {grpc::StatusCode::RESOURCE_EXHAUSTED, "subscriber queue overflow"};
                    break;
                }
                if (subscriber->pending.empty()) continue;
                event = std::move(subscriber->pending.front());
                subscriber->pending.pop_front();
            }
            if (!writer->Write(event)) break;
        }
        {
            std::lock_guard lock(subscriber->mutex);
            subscriber->context = nullptr;
            if (subscriber->failed) {
                status = {grpc::StatusCode::RESOURCE_EXHAUSTED, "subscriber queue overflow or shutdown"};
            }
        }
        metrics_.subscribers.fetch_sub(1, std::memory_order_relaxed);
        return status;
    }

private:
    bool authorized(grpc::ServerContext* context) const {
        if (token_.empty()) return true;
        const auto found = context->client_metadata().find("authorization");
        if (found == context->client_metadata().end()) return false;
        return std::string(found->second.data(), found->second.length()) == "Bearer " + token_;
    }
    bool enqueue(const std::shared_ptr<Command>& command) {
        std::lock_guard lock(ingress_mutex_);
        if (!running()) return false;
        if (ingress_.size() >= ingress_capacity) {
            metrics_.ingress_full.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        ingress_.push_back(command);
        ingress_changed_.notify_one();
        return true;
    }
    grpc::Status execute(grpc::ServerContext* context, std::shared_ptr<Command> command,
                         wire::CommandReply* reply) {
        auto result = command->done.get_future();
        if (!enqueue(command)) return {running() ? grpc::StatusCode::RESOURCE_EXHAUSTED
                                                : grpc::StatusCode::UNAVAILABLE,
                                      "engine ingress unavailable"};
        while (result.wait_for(10ms) != std::future_status::ready) {
            // Cancellation is an unknown outcome: an already admitted command may execute.
            if (context->IsCancelled()) return {grpc::StatusCode::CANCELLED, "command outcome unknown"};
        }
        const auto outcome = result.get();
        if (outcome.unavailable) return {grpc::StatusCode::UNAVAILABLE, "engine is stopping"};
        reply->set_accepted(outcome.result == pulsefeed::Result::Accepted);
        reply->set_reason(pulsefeed::result_name(outcome.result));
        reply->set_sequence(outcome.sequence);
        return grpc::Status::OK;
    }
    void publish(const pulsefeed::Snapshot& snapshot, const pulsefeed::Trade* trade) {
        Envelope envelope;
        envelope.depth = snapshot;
        envelope.sequence = ++sequence_;
        envelope.unix_nanos = unix_nanos();
        envelope.has_trade = trade != nullptr;
        if (trade != nullptr) envelope.trade = *trade;
        metrics_.sequence.store(sequence_, std::memory_order_relaxed);
        if (!output_.try_push(envelope)) {
            metrics_.feed_dropped.fetch_add(1, std::memory_order_relaxed);
        } else {
            output_signal_.fetch_add(1, std::memory_order_release);
            output_signal_.notify_one();
        }
    }
    void matching_loop() {
        while (running()) {
            std::shared_ptr<Command> command;
            {
                std::unique_lock lock(ingress_mutex_);
                const bool ready = ingress_changed_.wait_for(lock, 1s, [&] {
                    return !ingress_.empty() || !running();
                });
                if (!running()) break;
                if (!ready) {
                    lock.unlock();
                    // Full snapshots also recover subscribers after an idle Redis reconnect.
                    publish(book_.snapshot(), nullptr);
                    continue;
                }
                command = std::move(ingress_.front());
                ingress_.pop_front();
            }
            const auto result = command->cancel ? book_.cancel(command->order.id)
                                                : book_.submit(command->order);
            metrics_.commands.fetch_add(1, std::memory_order_relaxed);
            if (result == pulsefeed::Result::Accepted) {
                const auto depth = book_.snapshot();
                pulsefeed::Trade trade{};
                bool traded = false;
                while (book_.try_pop_trade(trade)) {
                    traded = true;
                    metrics_.trades.fetch_add(1, std::memory_order_relaxed);
                    publish(depth, &trade);
                }
                if (!traded) publish(depth, nullptr);
                metrics_.active_orders.store(book_.active_orders(), std::memory_order_relaxed);
            } else {
                metrics_.rejected.fetch_add(1, std::memory_order_relaxed);
            }
            command->done.set_value({result, sequence_, false});
        }
        std::deque<std::shared_ptr<Command>> abandoned;
        {
            std::lock_guard lock(ingress_mutex_);
            abandoned.swap(ingress_);
        }
        for (auto& command : abandoned) {
            command->done.set_value({pulsefeed::Result::Invalid, sequence_, true});
        }
    }
    wire::FeedEvent serialize(const Envelope& envelope) const {
        wire::FeedEvent event;
        event.set_epoch(epoch_);
        event.set_sequence(envelope.sequence);
        event.set_unix_nanos(envelope.unix_nanos);
        event.set_snapshot(!envelope.has_trade);
        if (envelope.has_trade) {
            auto* trade = event.mutable_trade();
            trade->set_maker_order_id(envelope.trade.maker_order_id);
            trade->set_taker_order_id(envelope.trade.taker_order_id);
            trade->set_price(envelope.trade.price);
            trade->set_quantity(envelope.trade.quantity);
            trade->set_aggressor_side(envelope.trade.aggressor_side == pulsefeed::Side::Buy
                                     ? wire::BUY : wire::SELL);
        }
        for (std::size_t i = 0; i < envelope.depth.bid_count; ++i) {
            const auto& level = envelope.depth.bids[i];
            auto* target = event.add_bids();
            target->set_price(level.price);
            target->set_quantity(level.quantity);
            target->set_orders(level.orders);
        }
        for (std::size_t i = 0; i < envelope.depth.ask_count; ++i) {
            const auto& level = envelope.depth.asks[i];
            auto* target = event.add_asks();
            target->set_price(level.price);
            target->set_quantity(level.quantity);
            target->set_orders(level.orders);
        }
        return event;
    }
    void dispatch_loop() {
        Envelope envelope;
        for (;;) {
            const auto signal = output_signal_.load(std::memory_order_acquire);
            if (!output_.try_pop(envelope)) {
                if (matcher_done_.load(std::memory_order_acquire)) break;
                // Loading the generation before checking the ring prevents lost wakeups.
                output_signal_.wait(signal, std::memory_order_acquire);
                continue;
            }
            auto event = serialize(envelope);
            std::lock_guard registry_lock(subscribers_mutex_);
            cached_ = event;
            std::erase_if(subscribers_, [](const auto& item) { return item.expired(); });
            for (const auto& weak : subscribers_) {
                if (auto subscriber = weak.lock()) {
                    std::lock_guard lock(subscriber->mutex);
                    if (subscriber->failed) continue;
                    if (subscriber->pending.size() >= subscriber_capacity) {
                        subscriber->failed = true;
                        metrics_.subscriber_dropped.fetch_add(1, std::memory_order_relaxed);
                        if (subscriber->context != nullptr) subscriber->context->TryCancel();
                    } else {
                        subscriber->pending.push_back(event);
                    }
                    subscriber->changed.notify_one();
                }
            }
            metrics_.published.fetch_add(1, std::memory_order_relaxed);
        }
    }
    void demo_loop() {
        std::uint64_t id = 1'000'000'000'000ULL;
        while (running()) {
            // A complete cycle leaves no resting orders; IDs are never reused in this process.
            const auto bid = id++;
            const auto ask = id++;
            const pulsefeed::Order orders[] = {
                {bid, pulsefeed::Side::Buy, 9999, 20},
                {ask, pulsefeed::Side::Sell, 10001, 20},
                {id++, pulsefeed::Side::Buy, 10001, 7},
                {id++, pulsefeed::Side::Sell, 9999, 9},
            };
            for (const auto& order : orders) {
                if (!demo_command(order, false)) return;
                if (!demo_pause()) return;
            }
            if (!demo_command({bid, pulsefeed::Side::Buy, 0, 0}, true)) return;
            if (!demo_pause()) return;
            if (!demo_command({ask, pulsefeed::Side::Sell, 0, 0}, true)) return;
            if (!demo_pause()) return;
        }
    }
    bool demo_command(pulsefeed::Order order, bool cancel) {
        auto command = std::make_shared<Command>();
        command->order = order;
        command->cancel = cancel;
        auto outcome = command->done.get_future();
        if (!enqueue(command)) return running();
        outcome.wait();
        return running();
    }
    bool demo_pause() {
        std::unique_lock lock(demo_mutex_);
        // Short waits allow stop() to join even with a very large configured interval.
        const auto until = std::chrono::steady_clock::now() + interval_;
        while (running() && std::chrono::steady_clock::now() < until) {
            demo_changed_.wait_until(lock, std::min(until, std::chrono::steady_clock::now() + 50ms));
        }
        return running();
    }

    pulsefeed::OrderBook book_;
    pulsefeed::SpscRing<Envelope, 8192> output_;
    Metrics metrics_;
    const std::string epoch_;
    const std::string token_;
    const bool demo_;
    const std::chrono::microseconds interval_;
    std::atomic<bool> running_{false}, matcher_done_{false};
    alignas(64) std::atomic<std::uint64_t> output_signal_{0};
    std::uint64_t sequence_{};
    std::mutex ingress_mutex_, subscribers_mutex_, demo_mutex_;
    std::condition_variable ingress_changed_, demo_changed_;
    std::deque<std::shared_ptr<Command>> ingress_;
    std::vector<std::weak_ptr<Subscriber>> subscribers_;
    wire::FeedEvent cached_;
    std::thread matcher_, dispatcher_, demo_thread_;
};

std::string metrics_text(const Metrics& metrics) {
    std::ostringstream output;
    const auto counter = [&](const char* name, const auto& value, const char* help) {
        output << "# HELP " << name << ' ' << help << "\n# TYPE " << name << " counter\n"
               << name << ' ' << value.load(std::memory_order_relaxed) << '\n';
    };
    const auto gauge = [&](const char* name, const auto& value, const char* help) {
        output << "# HELP " << name << ' ' << help << "\n# TYPE " << name << " gauge\n"
               << name << ' ' << value.load(std::memory_order_relaxed) << '\n';
    };
    counter("engine_commands_total", metrics.commands, "Commands processed by the matching thread.");
    counter("engine_rejected_total", metrics.rejected, "Commands rejected by the order book.");
    counter("engine_trades_total", metrics.trades, "Matching executions generated.");
    counter("engine_feed_dropped_total", metrics.feed_dropped, "Outbound envelopes dropped at the SPSC boundary.");
    counter("engine_ingress_full_total", metrics.ingress_full, "Commands rejected because ingress was full.");
    counter("engine_subscriber_dropped_total", metrics.subscriber_dropped, "Slow subscribers disconnected.");
    counter("engine_published_total", metrics.published, "Envelopes dispatched after serialization.");
    gauge("engine_active_orders", metrics.active_orders, "Resting L3 orders in the book.");
    gauge("engine_subscribers", metrics.subscribers, "Active gRPC stream subscribers.");
    gauge("engine_sequence", metrics.sequence, "Most recent assigned feed sequence.");
    return output.str();
}

class MetricsServer {
public:
    MetricsServer(const Engine& engine, std::uint16_t port) : engine_(engine) {
        socket_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (socket_ < 0) throw std::runtime_error("metrics socket creation failed");
        int enabled = 1;
        setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(port);
        if (bind(socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            listen(socket_, 32) < 0) {
            close(socket_);
            throw std::runtime_error("metrics port bind failed");
        }
        worker_ = std::thread([this] { loop(); });
    }
    ~MetricsServer() {
        stopping_.store(true, std::memory_order_release);
        if (worker_.joinable()) worker_.join();
        close(socket_);
    }
private:
    void loop() {
        while (!stopping_.load(std::memory_order_acquire)) {
            pollfd descriptor{socket_, POLLIN, 0};
            if (poll(&descriptor, 1, 100) <= 0) continue;
            const int client = accept4(socket_, nullptr, nullptr, SOCK_CLOEXEC);
            if (client < 0) continue;
            timeval timeout{0, 200000};
            setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            char buffer[1024]{};
            std::string request;
            const auto deadline = std::chrono::steady_clock::now() + 200ms;
            while (request.size() < 4096 && request.find("\r\n\r\n") == std::string::npos &&
                   std::chrono::steady_clock::now() < deadline) {
                const auto count = recv(client, buffer, sizeof(buffer), 0);
                if (count <= 0) break;
                request.append(buffer, static_cast<std::size_t>(count));
            }
            const bool metrics = request.starts_with("GET /metrics ");
            const bool healthy = request.starts_with("GET /health ") && engine_.running();
            const std::string body = metrics ? metrics_text(engine_.metrics()) : healthy ? "ok\n" : "not found\n";
            const auto response = std::string("HTTP/1.1 ") + (metrics || healthy ? "200 OK" : "404 Not Found") +
                "\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: close\r\nContent-Length: " +
                std::to_string(body.size()) + "\r\n\r\n" + body;
            std::size_t sent = 0;
            while (sent < response.size()) {
                const auto amount = send(client, response.data() + sent, response.size() - sent, MSG_NOSIGNAL);
                if (amount <= 0) break;
                sent += static_cast<std::size_t>(amount);
            }
            close(client);
        }
    }
    const Engine& engine_;
    int socket_{-1};
    std::atomic<bool> stopping_{false};
    std::thread worker_;
};

int healthcheck() {
    const auto port = static_cast<std::uint16_t>(env_number("ENGINE_METRICS_PORT", 9091, 1, 65535));
    const int socket = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (socket < 0) return 1;
    timeval timeout{1, 0};
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    int result = 1;
    if (connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) {
        constexpr char request[] = "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        if (send(socket, request, sizeof(request) - 1, MSG_NOSIGNAL) > 0) {
            char response[64]{};
            const auto count = recv(socket, response, sizeof(response) - 1, 0);
            if (count > 0 && std::string(response, static_cast<std::size_t>(count)).starts_with("HTTP/1.1 200")) result = 0;
        }
    }
    close(socket);
    return result;
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--healthcheck") return healthcheck();
        if (argc != 1) throw std::runtime_error("usage: pulsefeed_engine [--healthcheck]");
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        std::signal(SIGPIPE, SIG_IGN);
        auto engine = std::make_unique<Engine>(env_bool("ENGINE_DEMO", true),
            std::chrono::microseconds(env_number("DEMO_INTERVAL_US", 10000, 1, 60'000'000)));
        const auto address = env("ENGINE_ADDR", "0.0.0.0:50051");
        grpc::ServerBuilder builder;
        std::shared_ptr<grpc::ServerCredentials> credentials;
        const auto cert = env("ENGINE_TLS_CERT_FILE");
        const auto key = env("ENGINE_TLS_KEY_FILE");
        const auto ca = env("ENGINE_TLS_CLIENT_CA_FILE");
        if (cert.empty() != key.empty() || (!ca.empty() && cert.empty())) {
            throw std::runtime_error("TLS requires ENGINE_TLS_CERT_FILE and ENGINE_TLS_KEY_FILE together");
        }
        if (!cert.empty()) {
            grpc::SslServerCredentialsOptions options;
            options.pem_key_cert_pairs.push_back({read_file(key), read_file(cert)});
            if (!ca.empty()) {
                options.pem_root_certs = read_file(ca);
                options.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
            }
            credentials = grpc::SslServerCredentials(options);
        } else {
            credentials = grpc::InsecureServerCredentials();
        }
        builder.AddListeningPort(address, credentials);
        builder.SetMaxReceiveMessageSize(64 * 1024);
        builder.RegisterService(engine.get());
        auto server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("gRPC server bind failed: " + address);
        engine->start();
        try {
            MetricsServer metrics(*engine, static_cast<std::uint16_t>(
                env_number("ENGINE_METRICS_PORT", 9091, 1, 65535)));
            std::cout << "PulseFeed engine listening on " << address << std::endl;
            while (!interrupted) std::this_thread::sleep_for(100ms);
            engine->stop();
            server->Shutdown(std::chrono::system_clock::now() + 3s);
            server->Wait();
        } catch (...) {
            engine->stop();
            server->Shutdown(std::chrono::system_clock::now() + 3s);
            server->Wait();
            throw;
        }
    } catch (const std::exception& exception) {
        std::cerr << "engine: " << exception.what() << '\n';
        return 1;
    }
    return 0;
}
