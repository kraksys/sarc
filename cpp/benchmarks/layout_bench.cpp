#include <array>

#include <benchmark/benchmark.h>

#include "sarc/storage/layout.hpp"

static void BM_LayoutWriteObjectHeader(benchmark::State& state) {
    sarc::storage::ObjectHeader h{};
    h.version = sarc::storage::kLayoutVersion;
    h.zone = sarc::core::ZoneId{42};
    h.size_bytes = 123456;
    h.flags = 0x1234;
    h.reserved = 0;
    for (size_t i = 0; i < h.content.b.size(); ++i) {
        h.content.b[i] = static_cast<sarc::core::u8>(i);
    }

    std::array<sarc::storage::u8, sarc::storage::kObjectHeaderBytes> buf{};
    for (auto _ : state) {
        sarc::storage::u32 n =
            sarc::storage::layout_write_object_header(h, {buf.data(), static_cast<sarc::storage::u32>(buf.size())});
        benchmark::DoNotOptimize(static_cast<sarc::storage::u32>(n));
    }
}
BENCHMARK(BM_LayoutWriteObjectHeader);

static void BM_LayoutReadObjectHeader(benchmark::State& state) {
    sarc::storage::ObjectHeader h{};
    h.version = sarc::storage::kLayoutVersion;
    h.zone = sarc::core::ZoneId{42};
    h.size_bytes = 123456;
    h.flags = 0x1234;
    h.reserved = 0;

    std::array<sarc::storage::u8, sarc::storage::kObjectHeaderBytes> buf{};
    (void)sarc::storage::layout_write_object_header(h, {buf.data(), static_cast<sarc::storage::u32>(buf.size())});

    for (auto _ : state) {
        sarc::storage::ObjectHeader out{};
        sarc::storage::LayoutParseResult r = sarc::storage::layout_read_object_header(
            {buf.data(), static_cast<sarc::storage::u32>(buf.size())}, &out);
        benchmark::DoNotOptimize(static_cast<sarc::storage::u8>(r));
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_LayoutReadObjectHeader);
