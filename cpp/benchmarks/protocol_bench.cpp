#include <array>

#include <benchmark/benchmark.h>

#include "sarc/net/protocol.hpp"

static void BM_ProtocolWriteRequestHeader(benchmark::State& state) {
    sarc::net::RequestHeader h{};
    h.version = 1;
    h.type = sarc::net::MsgType::ListFiles;
    h.flags = 0x1234;
    h.request_id = 0xaabbccdd;
    h.zone = sarc::core::ZoneId{42};

    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes> buf{};
    for (auto _ : state) {
        const sarc::net::u32 n = sarc::net::protocol_write_request_header(
            h, {buf.data(), static_cast<sarc::net::u32>(buf.size())});
        benchmark::DoNotOptimize(static_cast<sarc::net::u32>(n));
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_ProtocolWriteRequestHeader);

static void BM_ProtocolReadRequestHeader(benchmark::State& state) {
    sarc::net::RequestHeader h{};
    h.version = 1;
    h.type = sarc::net::MsgType::ListFiles;
    h.flags = 0x1234;
    h.request_id = 0xaabbccdd;
    h.zone = sarc::core::ZoneId{42};

    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes> buf{};
    (void)sarc::net::protocol_write_request_header(h, {buf.data(), static_cast<sarc::net::u32>(buf.size())});

    for (auto _ : state) {
        sarc::net::RequestHeader out{};
        const sarc::net::ProtocolParseResult r =
            sarc::net::protocol_read_request_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
        benchmark::DoNotOptimize(static_cast<int>(r));
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_ProtocolReadRequestHeader);

static void BM_ProtocolWritePutObjectFixed(benchmark::State& state) {
    sarc::net::PutObjectRequest req{};
    req.header.version = 1;
    req.header.type = sarc::net::MsgType::PutObject;
    req.header.request_id = 123;
    req.header.zone = sarc::core::ZoneId{42};

    req.cap.zone = sarc::core::ZoneId{42};
    req.cap.grantee = sarc::core::UserId{7};
    req.cap.rights = 0x01020304;
    req.cap.issued_at = 100;
    req.cap.expires_at = 200;
    req.cap.version = 1;
    for (size_t i = 0; i < 16; ++i) {
        req.cap.proof.b[i] = static_cast<sarc::core::u8>(i);
    }

    req.object.zone = sarc::core::ZoneId{42};
    for (size_t i = 0; i < 32; ++i) {
        req.object.content.b[i] = static_cast<sarc::core::u8>(0xa0u + static_cast<sarc::core::u8>(i));
    }
    req.size_bytes = 4096;

    std::array<sarc::net::u8, sarc::net::kPutObjectRequestFixedBytes> buf{};
    for (auto _ : state) {
        const sarc::net::u32 n = sarc::net::protocol_write_put_object_request_fixed(
            req, {buf.data(), static_cast<sarc::net::u32>(buf.size())});
        benchmark::DoNotOptimize(static_cast<sarc::net::u32>(n));
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_ProtocolWritePutObjectFixed);

static void BM_ProtocolReadPutObjectFixed(benchmark::State& state) {
    sarc::net::PutObjectRequest req{};
    req.header.version = 1;
    req.header.type = sarc::net::MsgType::PutObject;
    req.header.request_id = 123;
    req.header.zone = sarc::core::ZoneId{42};

    req.cap.zone = sarc::core::ZoneId{42};
    req.cap.grantee = sarc::core::UserId{7};
    req.cap.rights = 0x01020304;
    req.cap.issued_at = 100;
    req.cap.expires_at = 200;
    req.cap.version = 1;
    for (size_t i = 0; i < 16; ++i) {
        req.cap.proof.b[i] = static_cast<sarc::core::u8>(i);
    }

    req.object.zone = sarc::core::ZoneId{42};
    for (size_t i = 0; i < 32; ++i) {
        req.object.content.b[i] = static_cast<sarc::core::u8>(0xa0u + static_cast<sarc::core::u8>(i));
    }
    req.size_bytes = 4096;

    std::array<sarc::net::u8, sarc::net::kPutObjectRequestFixedBytes> buf{};
    (void)sarc::net::protocol_write_put_object_request_fixed(req, {buf.data(), static_cast<sarc::net::u32>(buf.size())});

    for (auto _ : state) {
        sarc::net::PutObjectRequest out{};
        const sarc::net::ProtocolParseResult r =
            sarc::net::protocol_read_put_object_request_fixed({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
        benchmark::DoNotOptimize(static_cast<int>(r));
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_ProtocolReadPutObjectFixed);
