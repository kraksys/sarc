#include <array>
#include <vector>
#include <cstring>

#include <benchmark/benchmark.h>

#include "sarc/net/framing.hpp"

// Benchmark: Header serialization only (fixed 16 bytes)
static void BM_FrameHeaderSerialize(benchmark::State& state){
    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Request;
    h.flags = 0x1234;
    h.payload_len = 1024;
    h.request_id = 0xaabbccdd;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};

    for (auto _ : state){
        sarc::net::BufferMut out{buf.data(), static_cast<sarc::net::u32>(buf.size())};
        const sarc::net::u32 written = sarc::net::frame_write_header(h, out);
        benchmark::DoNotOptimize(written);
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_FrameHeaderSerialize);

// Benchmark: Header deserialization only (fixed 16 bytes)
static void BM_FrameHeaderDeserialize(benchmark::State& state){
    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Response;
    h.flags = 0x1234;
    h.payload_len = 1024;
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
BENCHMARK(BM_FrameHeaderDeserialize);

// Benchmark: Full frame write (header + payload copy)
static void BM_FrameWrite(benchmark::State& state){
    const sarc::net::u32 payload_size = static_cast<sarc::net::u32>(state.range(0));

    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Request;
    h.flags = 0x1234;
    h.payload_len = payload_size;
    h.request_id = 0xaabbccdd;

    std::vector<sarc::net::u8> payload(payload_size, 0xAB);
    std::vector<sarc::net::u8> frame_buf(sarc::net::kFrameHeaderBytes + payload_size);

    for (auto _ : state){
        // Write header
        const sarc::net::u32 hdr_written = sarc::net::frame_write_header(
            h, {frame_buf.data(), sarc::net::kFrameHeaderBytes});

        // Copy payload
        if (payload_size > 0) {
            std::memcpy(frame_buf.data() + hdr_written, payload.data(), payload_size);
        }

        benchmark::DoNotOptimize(frame_buf.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                           (sarc::net::kFrameHeaderBytes + payload_size));
}
BENCHMARK(BM_FrameWrite)->Arg(0)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096)->Arg(16384);

// Benchmark: Full frame read (header + payload copy)
static void BM_FrameRead(benchmark::State& state){
    const sarc::net::u32 payload_size = static_cast<sarc::net::u32>(state.range(0));

    sarc::net::FrameHeader h{};
    h.version = 1;
    h.type = sarc::net::FrameType::Response;
    h.flags = 0x1234;
    h.payload_len = payload_size;
    h.request_id = 0xaabbccdd;

    std::vector<sarc::net::u8> payload(payload_size, 0xCD);
    std::vector<sarc::net::u8> frame_buf(sarc::net::kFrameHeaderBytes + payload_size);

    // Prepare frame
    (void)sarc::net::frame_write_header(h, {frame_buf.data(), sarc::net::kFrameHeaderBytes});
    if (payload_size > 0) {
        std::memcpy(frame_buf.data() + sarc::net::kFrameHeaderBytes, payload.data(), payload_size);
    }

    std::vector<sarc::net::u8> out_payload(payload_size);

    for (auto _ : state){
        // Read header
        sarc::net::FrameHeader out_hdr{};
        const sarc::net::FrameParseResult r = sarc::net::frame_read_header(
            {frame_buf.data(), static_cast<sarc::net::u32>(frame_buf.size())}, &out_hdr);

        // Copy payload
        if (payload_size > 0) {
            std::memcpy(out_payload.data(), frame_buf.data() + sarc::net::kFrameHeaderBytes, payload_size);
        }

        benchmark::DoNotOptimize(r);
        benchmark::DoNotOptimize(out_hdr);
        benchmark::DoNotOptimize(out_payload.data());
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                           (sarc::net::kFrameHeaderBytes + payload_size));
}
BENCHMARK(BM_FrameRead)->Arg(0)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096)->Arg(16384);
