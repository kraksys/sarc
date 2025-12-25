#include <cstddef>

#include <benchmark/benchmark.h>

#include "sarc/security/policy.hpp"

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}
} // namespace

static void BM_CapabilitySeal(benchmark::State& state) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::ZoneId{42};
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read) |
                 sarc::security::right_mask(sarc::security::Right::List);
    cap.issued_at = 100;
    cap.expires_at = 200;
    cap.version = 1;

    for (auto _ : state) {
        sarc::security::Tag16 tag{};
        const sarc::core::Status s = sarc::security::capability_seal(key, cap, &tag);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
        benchmark::DoNotOptimize(tag);
    }
}
BENCHMARK(BM_CapabilitySeal);

static void BM_CapabilityVerify(benchmark::State& state) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::ZoneId{42};
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read) |
                 sarc::security::right_mask(sarc::security::Right::List);
    cap.issued_at = 100;
    cap.expires_at = 200;
    cap.version = 1;
    (void)sarc::security::capability_seal(key, cap, &cap.proof);

    for (auto _ : state) {
        const sarc::core::Status s = sarc::security::capability_verify(key, cap);
        benchmark::DoNotOptimize(static_cast<int>(s.code));
        benchmark::DoNotOptimize(static_cast<int>(s.domain));
        benchmark::DoNotOptimize(static_cast<sarc::core::u32>(s.aux));
    }
}
BENCHMARK(BM_CapabilityVerify);
