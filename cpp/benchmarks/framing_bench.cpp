#include <array>

#include <benchmark/benchmark.h>

#include "sarc/net/framing.hpp"

static void BM_FrameWriteHeader(benchmark::State& state){
    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Request;
    h.flags = 0x1234;
    h.payload_len = static_cast<sarc::net::u32>(state.range(0));
    h.request_id = 0xaabbccdd;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};

    for (auto _ : state){
        sarc::net::BufferMut out{buf.data(), static_cast<sarc::net::u32>(buf.size())};
        const sarc::net::u32 written = sarc::net::frame_write_header(h, out);
        benchmark::DoNotOptimize(written);
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_FrameWriteHeader)->Arg(0)->Arg(16)->Arg(256)->Arg(4096);

static void BM_FrameReadHeader(benchmark::State& state){
    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Response;
    h.flags = 0x1234;
    h.payload_len = static_cast<sarc::net::u32>(state.range(0));
    h.request_id = 0xaabbccdd;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};
    (void)sarc::net::frame_write_header(h, {buf.data(), static_cast<sarc::net::u32>(buf.size())});

    for (auto _ : state){
        sarc::net::FrameHeader out{};
        const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
        benchmark::DoNotOptimize(r);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_FrameReadHeader)->Arg(0)->Arg(16)->Arg(256)->Arg(4096);
