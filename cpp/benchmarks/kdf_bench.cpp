#include <array>
#include <cstddef>

#include <benchmark/benchmark.h>

#include "sarc/security/kdf.hpp"

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}

static sarc::core::Hash256 make_hash256_seq(sarc::core::u8 start) {
    sarc::core::Hash256 h{};
    for (size_t i = 0; i < 32; ++i) {
        h.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return h;
}
} // namespace

static void BM_KdfExpand(benchmark::State& state) {
    const auto zrk = make_key256_seq(7);

    const size_t n = static_cast<size_t>(state.range(0));
    std::array<sarc::core::u8, 4096> info_bytes{};
    for (size_t i = 0; i < info_bytes.size(); ++i) {
        info_bytes[i] = static_cast<sarc::core::u8>(i & 0xffu);
    }

    const sarc::security::BufferView salt{nullptr, 0};
    const sarc::security::BufferView info{info_bytes.data(), static_cast<sarc::security::u32>(n)};

    for (auto _ : state) {
        sarc::security::Key256 out{};
        const sarc::core::Status s = sarc::security::hkdf_expand(zrk, salt, info, &out);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(out);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
}
BENCHMARK(BM_KdfExpand)->Arg(0)->Arg(16)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

static void BM_DeriveObjectKey(benchmark::State& state) {
    const auto zrk = make_key256_seq(7);

    sarc::security::KdfInfo info{};
    info.domain = sarc::security::KdfDomain::ObjectKey;
    info.version = 1;
    info.object.zone = sarc::core::ZoneId{42};
    info.object.content = make_hash256_seq(1);

    for (auto _ : state) {
        sarc::security::Key256 out{};
        const sarc::core::Status s = sarc::security::hkdf_derive_object_key(zrk, info, &out);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_DeriveObjectKey);

static void BM_DeriveObjectNonce(benchmark::State& state) {
    const auto zrk = make_key256_seq(7);

    sarc::security::KdfInfo info{};
    info.domain = sarc::security::KdfDomain::ObjectNonce;
    info.version = 1;
    info.object.zone = sarc::core::ZoneId{42};
    info.object.content = make_hash256_seq(1);

    for (auto _ : state) {
        sarc::security::Nonce12 out{};
        const sarc::core::Status s = sarc::security::derive_object_nonce(zrk, info, &out);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_DeriveObjectNonce);
