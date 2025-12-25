#include <array>
#include <cstddef>

#include <benchmark/benchmark.h>

#include "sarc/security/crypto.hpp"

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

static void BM_AeadSeal(benchmark::State& state) {
    const auto key = make_key256_seq(1);
    const auto nonce = make_nonce12_seq(9);

    const size_t n = static_cast<size_t>(state.range(0));
    std::array<sarc::core::u8, 4096> pt{};
    for (size_t i = 0; i < pt.size(); ++i) {
        pt[i] = static_cast<sarc::core::u8>(i & 0xffu);
    }

    std::array<sarc::core::u8, 4096> ct{};
    const sarc::security::BufferView aad{nullptr, 0};

    for (auto _ : state) {
        sarc::security::Tag16 tag{};
        const sarc::core::Status s = sarc::security::aead_seal(
            sarc::security::AeadId::ChaCha20Poly1305,
            key,
            nonce,
            aad,
            {pt.data(), static_cast<sarc::security::u32>(n)},
            {ct.data(), static_cast<sarc::security::u32>(n)},
            &tag);
        benchmark::DoNotOptimize(s.code);
        benchmark::DoNotOptimize(s.domain);
        benchmark::DoNotOptimize(s.aux);
        benchmark::DoNotOptimize(tag);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
}
BENCHMARK(BM_AeadSeal)->Arg(0)->Arg(16)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

static void BM_AeadOpen(benchmark::State& state) {
    const auto key = make_key256_seq(1);
    const auto nonce = make_nonce12_seq(9);

    const size_t n = static_cast<size_t>(state.range(0));
    std::array<sarc::core::u8, 4096> pt{};
    for (size_t i = 0; i < pt.size(); ++i) {
        pt[i] = static_cast<sarc::core::u8>(i & 0xffu);
    }

    std::array<sarc::core::u8, 4096> ct{};
    sarc::security::Tag16 tag{};
    (void)sarc::security::aead_seal(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {nullptr, 0},
        {pt.data(), static_cast<sarc::security::u32>(n)},
        {ct.data(), static_cast<sarc::security::u32>(n)},
        &tag);

    std::array<sarc::core::u8, 4096> out{};

    for (auto _ : state) {
        const sarc::core::Status s = sarc::security::aead_open(
            sarc::security::AeadId::ChaCha20Poly1305,
            key,
            nonce,
            {nullptr, 0},
            {ct.data(), static_cast<sarc::security::u32>(n)},
            tag,
            {out.data(), static_cast<sarc::security::u32>(n)});
        benchmark::DoNotOptimize(s.code);
        benchmark::DoNotOptimize(s.domain);
        benchmark::DoNotOptimize(s.aux);
        benchmark::DoNotOptimize(out);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
}
BENCHMARK(BM_AeadOpen)->Arg(0)->Arg(16)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

