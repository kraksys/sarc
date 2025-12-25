#include <cstddef>

#include <benchmark/benchmark.h>

#include "sarc/security/keystore.hpp"

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}

static sarc::security::Nonce12 make_nonce12_seq(sarc::core::u8 start) {
    sarc::security::Nonce12 n{};
    for (size_t i = 0; i < 12; ++i) {
        n.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return n;
}
} // namespace

static void BM_KeystoreWrapZrk(benchmark::State& state) {
    sarc::security::NodeMasterKey nmk{};
    nmk.key = make_key256_seq(1);

    sarc::security::KeyWrapNonce nonce{};
    nonce.nonce = make_nonce12_seq(9);

    sarc::security::ZoneRootKey zrk{};
    zrk.zone = sarc::core::ZoneId{42};
    zrk.version = 1;
    zrk.key = make_key256_seq(77);

    for (auto _ : state) {
        sarc::security::WrappedKey wrapped{};
        const sarc::core::Status s = sarc::security::wrap_zone_root_key(nmk, nonce, zrk, &wrapped);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(wrapped);
    }
}
BENCHMARK(BM_KeystoreWrapZrk);

static void BM_KeystoreUnwrapZrk(benchmark::State& state) {
    sarc::security::NodeMasterKey nmk{};
    nmk.key = make_key256_seq(1);

    sarc::security::KeyWrapNonce nonce{};
    nonce.nonce = make_nonce12_seq(9);

    sarc::security::ZoneRootKey zrk{};
    zrk.zone = sarc::core::ZoneId{42};
    zrk.version = 1;
    zrk.key = make_key256_seq(77);

    sarc::security::WrappedKey wrapped{};
    (void)sarc::security::wrap_zone_root_key(nmk, nonce, zrk, &wrapped);

    for (auto _ : state) {
        sarc::security::ZoneRootKey out{};
        const sarc::core::Status s =
            sarc::security::unwrap_zone_root_key(nmk, nonce, zrk.zone, zrk.version, wrapped, &out);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(out.key);
    }
}
BENCHMARK(BM_KeystoreUnwrapZrk);

