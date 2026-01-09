#include <benchmark/benchmark.h>
#include "sarc/core/types.hpp"
#include "sarc/core/models.hpp"
#include <vector>
#include <numeric>

using namespace sarc::core;

// Benchmark: Iterate over Trace array accessing only hot fields
static void BM_TraceArrayIteration(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<Trace> traces(n);

    // Initialize with test data
    for (size_t i = 0; i < n; ++i) {
        traces[i].type = FieldType::F64;
        traces[i].data.f64v = static_cast<double>(i);
        traces[i].confidence = 0.5f + (i % 100) * 0.005f;
        traces[i].source = SourceId{static_cast<u32>(i % 10)};
        traces[i].updated_at = static_cast<Timestamp>(i);
    }

    float sum = 0.0f;
    for (auto _ : state) {
        // Hot loop: access only hot fields (type, data, confidence)
        for (const auto& t : traces) {
            if (t.type == FieldType::F64) {
                sum += t.confidence * static_cast<float>(t.data.f64v);
            }
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * n * sizeof(Trace));
}
BENCHMARK(BM_TraceArrayIteration)->Arg(100)->Arg(1000)->Arg(10000)->Arg(100000);

// Benchmark: Gestalt array weighted scoring (hot fields only)
static void BM_GestaltArrayScoring(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<Gestalt> gestalts(n);

    for (size_t i = 0; i < n; ++i) {
        gestalts[i].type = StringId{static_cast<u32>(i % 10)};
        gestalts[i].value = StringId{static_cast<u32>(i)};
        gestalts[i].weight = 1.0f + (i % 5) * 0.1f;
        gestalts[i].confidence = 0.8f + (i % 20) * 0.01f;
        gestalts[i].source = SourceId{static_cast<u32>(i % 10)};
        gestalts[i].created_at = static_cast<Timestamp>(i);
    }

    float total_score = 0.0f;
    for (auto _ : state) {
        // Hot loop: compute weighted scores using only hot fields
        for (const auto& g : gestalts) {
            total_score += g.weight * g.confidence;
        }
        benchmark::DoNotOptimize(total_score);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * n * sizeof(Gestalt));
}
BENCHMARK(BM_GestaltArrayScoring)->Arg(100)->Arg(1000)->Arg(10000)->Arg(100000);

// Benchmark: ObjectMeta array size calculations (hot field access)
static void BM_ObjectMetaSizeCalculation(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<ObjectMeta> metas(n);

    for (size_t i = 0; i < n; ++i) {
        metas[i].size_bytes = 1024 + i;
        metas[i].refcount = 1 + (i % 10);
        metas[i].created_at = static_cast<Timestamp>(i);
        metas[i].updated_at = static_cast<Timestamp>(i + 1000);
    }

    u64 total_size = 0;
    u32 total_refs = 0;
    for (auto _ : state) {
        // Hot loop: access only hot fields (size_bytes, refcount)
        for (const auto& m : metas) {
            total_size += m.size_bytes;
            total_refs += m.refcount;
        }
        benchmark::DoNotOptimize(total_size);
        benchmark::DoNotOptimize(total_refs);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * n * sizeof(ObjectMeta));
}
BENCHMARK(BM_ObjectMetaSizeCalculation)->Arg(100)->Arg(1000)->Arg(10000)->Arg(100000);

// Benchmark: FileMeta array hot field access (id, zone, content, size)
static void BM_FileMetaHotFieldAccess(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<FileMeta> files(n);

    for (size_t i = 0; i < n; ++i) {
        files[i].id = FileId{static_cast<u64>(i)};
        files[i].zone = ZoneId{static_cast<u32>(i % 100)};
        // Initialize content hash
        for (size_t j = 0; j < files[i].content.b.size(); ++j) {
            files[i].content.b[j] = static_cast<u8>((i + j) % 256);
        }
        files[i].size_bytes = 1024 * (1 + i % 1000);
        files[i].name = StringId{static_cast<u32>(i)};
        files[i].mime = StringId{static_cast<u32>(i % 10)};
        files[i].created_at = static_cast<Timestamp>(i);
        files[i].updated_at = static_cast<Timestamp>(i + 1000);
    }

    u64 total_size = 0;
    u32 zone_count = 0;
    for (auto _ : state) {
        // Hot loop: access hot fields (id, zone, content, size_bytes)
        for (const auto& f : files) {
            if (f.zone.v < 50) {
                total_size += f.size_bytes;
                zone_count++;
            }
            benchmark::DoNotOptimize(f.content.b[0]); // Touch content
        }
        benchmark::DoNotOptimize(total_size);
        benchmark::DoNotOptimize(zone_count);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * n * sizeof(FileMeta));
}
BENCHMARK(BM_FileMetaHotFieldAccess)->Arg(100)->Arg(1000)->Arg(10000)->Arg(100000);

// Benchmark: Sequential cache line utilization test for Trace
static void BM_TraceCacheLineUtilization(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<Trace> traces(n);

    // Initialize
    for (size_t i = 0; i < n; ++i) {
        traces[i].type = FieldType::I64;
        traces[i].data.i64v = static_cast<i64>(i);
        traces[i].confidence = 1.0f;
    }

    i64 sum = 0;
    for (auto _ : state) {
        // Touch first 16 bytes only (one cache line segment)
        for (const auto& t : traces) {
            sum += t.data.i64v;
            benchmark::ClobberMemory(); // Prevent over-optimization
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    // Report effective bytes accessed (just hot data, not full struct)
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * n * 16);
}
BENCHMARK(BM_TraceCacheLineUtilization)->Arg(100)->Arg(1000)->Arg(10000);

// Benchmark: Random access pattern (worst case for cache)
static void BM_TraceRandomAccess(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    std::vector<Trace> traces(n);
    std::vector<size_t> indices(n);

    // Initialize with sequential data
    for (size_t i = 0; i < n; ++i) {
        traces[i].type = FieldType::F64;
        traces[i].data.f64v = static_cast<double>(i);
        traces[i].confidence = 0.5f;
        indices[i] = i;
    }

    // Create pseudo-random access pattern
    for (size_t i = 0; i < n; ++i) {
        size_t j = (i * 7919) % n; // Simple pseudo-random
        std::swap(indices[i], indices[j]);
    }

    float sum = 0.0f;
    for (auto _ : state) {
        for (size_t idx : indices) {
            const auto& t = traces[idx];
            if (t.type == FieldType::F64) {
                sum += t.confidence * static_cast<float>(t.data.f64v);
            }
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
}
BENCHMARK(BM_TraceRandomAccess)->Arg(1000)->Arg(10000);
