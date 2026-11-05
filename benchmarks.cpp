#include "OrderBook.hpp"
#include <benchmark/benchmark.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#if defined(__linux__)
#include <sched.h>
#endif
#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#include <x86intrin.h>
#endif

using namespace pulsefeed;
using Clock = std::chrono::steady_clock;
namespace {
constexpr std::size_t max_samples = 262144;
enum class Work { Empty, Insert, Cancel, Match, Sweep8, Duplicate };

void require(Result actual, Result expected = Result::Accepted) {
    if (actual != expected) throw std::runtime_error(result_name(actual));
}
void prepare(OrderBook& book, Work work) {
    if (work == Work::Cancel || work == Work::Duplicate) require(book.submit({1, Side::Buy, 90, 1}));
    if (work == Work::Match) require(book.submit({1, Side::Sell, 100, 1}));
    if (work == Work::Sweep8)
        for (std::uint32_t i = 0; i < 8; ++i) require(book.submit({i + 1, Side::Sell, 100 + i, 1}));
}
Result operation(OrderBook& book, Work work) {
    switch (work) {
    case Work::Empty: benchmark::ClobberMemory(); return Result::Accepted;
    case Work::Insert: return book.submit({1, Side::Buy, 90, 1});
    case Work::Cancel: return book.cancel(1);
    case Work::Match: return book.submit({2, Side::Buy, 100, 1});
    case Work::Sweep8: return book.submit({9, Side::Buy, 107, 8});
    case Work::Duplicate: return book.submit({1, Side::Buy, 90, 1});
    }
    return Result::Invalid;
}
void cleanup(OrderBook& book, Work work) {
    if (work == Work::Insert || work == Work::Duplicate) require(book.cancel(1));
    Trade trade{};
    while (book.try_pop_trade(trade)) benchmark::DoNotOptimize(trade);
}
double quantile(const std::vector<double>& sorted, double q) {
    return sorted[static_cast<std::size_t>(std::ceil(q * static_cast<double>(sorted.size()))) - 1];
}
void report(benchmark::State& state, std::vector<double>& samples, const std::string& unit) {
    if (samples.empty()) return;
    std::sort(samples.begin(), samples.end());
    for (const auto [label, q] : {std::pair{"p50", .50}, {"p95", .95}, {"p99", .99}}) {
        const auto value = quantile(samples, q);
        state.counters[std::string(label) + "_" + unit] = value;
        if (unit == "ns") state.counters[std::string(label) + "_us"] = value / 1000.0;
    }
    state.counters["samples"] = static_cast<double>(samples.size());
}
bool has_rdtscp() {
#if defined(__x86_64__) || defined(__i386__)
    unsigned eax{}, ebx{}, ecx{}, edx{};
    return __get_cpuid(0x80000001U, &eax, &ebx, &ecx, &edx) && (edx & (1U << 27));
#else
    return false;
#endif
}
std::uint64_t cycle_stamp(unsigned& aux) {
#if defined(__x86_64__) || defined(__i386__)
    _mm_lfence();
    const auto stamp = __rdtscp(&aux);
    _mm_lfence();
    return stamp;
#else
    aux = 0; return 0;
#endif
}

void latency(benchmark::State& state, Work work, bool cycles) {
    if (cycles && !has_rdtscp()) { state.SkipWithError("RDTSCP unavailable on this CPU"); return; }
    OrderBook book;
    // Two populations exercise an empty book and a near-capacity ID index.
    // Background orders rest outside the measured workloads' crossing range.
    const auto population = static_cast<std::uint64_t>(state.range(0));
    for (std::uint64_t i = 0; i < population; ++i) {
        const bool buy = i % 2 == 0;
        require(book.submit({1000 + i, buy ? Side::Buy : Side::Sell,
            static_cast<std::uint32_t>(buy ? 1 + i % 80 : 120 + i % 80), 1}));
    }
    std::vector<double> samples;
    samples.reserve(max_samples);
    for (int i = 0; i < 2048; ++i) {
        prepare(book, work); require(operation(book, work), work == Work::Duplicate ? Result::Duplicate : Result::Accepted);
        cleanup(book, work);
    }
    std::size_t cursor = 0, migrations = 0;
    try {
        for (auto _ : state) {
            (void)_;
            prepare(book, work);
            unsigned before_cpu{}, after_cpu{};
            const auto start = Clock::now();
            const auto begin_cycles = cycles ? cycle_stamp(before_cpu) : 0;
            auto result = operation(book, work);
            benchmark::DoNotOptimize(result);
            const auto end_cycles = cycles ? cycle_stamp(after_cpu) : 0;
            const auto end = Clock::now();
            const auto ns = std::chrono::duration<double, std::nano>(end - start).count();
            state.SetIterationTime(ns / 1e9);
            require(result, work == Work::Duplicate ? Result::Duplicate : Result::Accepted);
            if (!cycles || before_cpu == after_cpu) {
                const double sample = cycles ? static_cast<double>(end_cycles - begin_cycles) : ns;
                if (samples.size() != max_samples) samples.push_back(sample);
                else samples[cursor++ % max_samples] = sample;
            } else ++migrations;
            cleanup(book, work);
        }
    } catch (const std::exception& e) { state.SkipWithError(e.what()); }
    report(state, samples, cycles ? "cycles" : "ns");
    state.counters["migrations_discarded"] = static_cast<double>(migrations);
    state.SetLabel(cycles ? "TSC ticks including fencing; no frequency conversion" : "per-operation samples; timer overhead included");
}

void insert_cancel(benchmark::State& state) {
    OrderBook book;
    for (auto _ : state) {
        (void)_;
        benchmark::DoNotOptimize(book.submit({1, Side::Buy, 100, 1}));
        benchmark::DoNotOptimize(book.cancel(1));
    }
    state.SetItemsProcessed(state.iterations() * 2);
}
void roundtrip(benchmark::State& state) {
    OrderBook book;
    Trade trade{};
    for (auto _ : state) {
        (void)_;
        benchmark::DoNotOptimize(book.submit({1, Side::Sell, 100, 1}));
        benchmark::DoNotOptimize(book.submit({2, Side::Buy, 100, 1}));
        benchmark::DoNotOptimize(book.try_pop_trade(trade));
    }
    state.SetItemsProcessed(state.iterations() * 2);
}

void pin_thread() {
    const char* value = std::getenv("PULSEFEED_CPU");
    if (!value) return;
    std::size_t consumed{};
    const auto cpu = std::stoi(value, &consumed);
    if (value[consumed] != '\0' || cpu < 0) throw std::runtime_error("invalid PULSEFEED_CPU");
#if defined(__linux__)
    if (cpu >= CPU_SETSIZE) throw std::runtime_error("PULSEFEED_CPU exceeds CPU_SETSIZE");
    cpu_set_t mask;
    CPU_ZERO(&mask); CPU_SET(cpu, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
        throw std::runtime_error("CPU affinity failed; choose a CPU in the process allowed set");
#else
    throw std::runtime_error("PULSEFEED_CPU affinity is supported on Linux only");
#endif
}
}

int main(int argc, char** argv) {
    try { pin_thread(); }
    catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
    const std::pair<const char*, Work> workloads[] = {
        {"Empty", Work::Empty}, {"Insert", Work::Insert}, {"Cancel", Work::Cancel},
        {"Match", Work::Match}, {"Sweep8", Work::Sweep8}, {"Duplicate", Work::Duplicate}};
    for (const auto& [name, work] : workloads) {
        benchmark::RegisterBenchmark((std::string("Latency/") + name).c_str(), latency, work, false)
            ->Arg(0)->Arg(60000)->UseManualTime()->Iterations(50000)->Unit(benchmark::kNanosecond);
        benchmark::RegisterBenchmark((std::string("Cycles/") + name).c_str(), latency, work, true)
            ->Arg(0)->Arg(60000)->UseManualTime()->Iterations(50000)->Unit(benchmark::kNanosecond);
    }
    benchmark::RegisterBenchmark("Throughput/InsertCancel", insert_cancel);
    benchmark::RegisterBenchmark("Throughput/MatchAndDrain", roundtrip);
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}
