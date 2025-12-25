#include <array>
#include <cstddef>

#include <benchmark/benchmark.h>

#include "sarc/storage/hashing.hpp"

static void BM_HashCompute(benchmark::State& state){
    const size_t n = static_cast<size_t>(state.range(0));

    std::array<sarc::storage::u8, 4096> buf{};
    for (size_t i = 0; i < buf.size(); ++i){
        buf[i] = static_cast<sarc::storage::u8>(i & 0xffu);
    }

    for (auto _ : state){
        sarc::core::Hash256 out{};
        sarc::core::Status s = sarc::storage::hash_compute({buf.data(), static_cast<sarc::storage::u32>(n)}, &out);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(out);

    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
}

BENCHMARK(BM_HashCompute)->Arg(0)->Arg(3)->Arg(64)->Arg(1024)->Arg(4096);
