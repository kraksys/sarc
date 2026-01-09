#include <benchmark/benchmark.h>
#include "sarc/storage/object_store.hpp"
#include "sarc/core/zone.hpp"
#include <vector>
#include <thread>
#include <cstdlib>
#include <sys/stat.h>

using namespace sarc::storage;
using namespace sarc::core;

// Global object store state
static ObjectStoreConfig g_config;
static bool g_initialized = false;

// Initialize object store once
static void InitializeObjectStore() {
    if (g_initialized) return;

    g_config.data_root = "/tmp/sarc_bench_objects";
    g_config.db_path = "/tmp/sarc_bench.db";
    g_config.max_object_bytes = 0;
    g_config.compression = CompressionPolicy::None;
    g_config.verify_on_read = false;

    // Clean up
    system("rm -rf /tmp/sarc_bench_objects");
    system("rm -f /tmp/sarc_bench.db");

    // Create directory
    mkdir("/tmp/sarc_bench_objects", 0755);

    Status s = object_store_init(g_config);
    if (!is_ok(s)) {
        throw std::runtime_error("Failed to initialize object store");
    }

    g_initialized = true;
}

// Cleanup at end
static void ShutdownObjectStore() {
    if (g_initialized) {
        object_store_shutdown();
        g_initialized = false;
    }
}

// ============================================================================
// Single-Threaded Put Benchmarks
// ============================================================================

static void BM_ObjectPut(benchmark::State& state) {
    InitializeObjectStore();

    const u64 size = static_cast<u64>(state.range(0));
    std::vector<u8> data(size);

    // Fill with deterministic pattern
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<u8>((i * 7) & 0xFF);
    }

    u32 counter = 0;

    for (auto _ : state) {
        // Create unique data for each iteration
        data[0] = static_cast<u8>(counter++ & 0xFF);

        ObjectPutParams params;
        params.zone = ZoneId{1};
        params.data = data.data();
        params.size_bytes = size;
        params.filename = nullptr;
        params.mime_type = "application/octet-stream";

        ObjectPutResult result;
        Status s = object_put(params, &result);

        if (!is_ok(s)) {
            state.SkipWithError("Put failed");
            break;
        }

        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * size);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectPut)
    ->Arg(4096)           // 4KB
    ->Arg(64 * 1024)      // 64KB
    ->Arg(1024 * 1024)    // 1MB
    ->Arg(15 * 1024 * 1024);  // 15MB

// ============================================================================
// Single-Threaded Get Benchmarks
// ============================================================================

static void BM_ObjectGet(benchmark::State& state) {
    InitializeObjectStore();

    const u64 size = static_cast<u64>(state.range(0));
    std::vector<u8> data(size);

    // Fill with pattern
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<u8>((i * 13) & 0xFF);
    }

    // Put one object to get
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = size;
    put_params.filename = nullptr;
    put_params.mime_type = "application/octet-stream";

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    if (!is_ok(s)) {
        state.SkipWithError("Setup put failed");
        return;
    }

    std::vector<u8> buffer(size);

    for (auto _ : state) {
        ObjectGetResult get_result;
        Status s = object_get(put_result.key, buffer.data(), buffer.size(), &get_result);

        if (!is_ok(s)) {
            state.SkipWithError("Get failed");
            break;
        }

        benchmark::DoNotOptimize(get_result);
        benchmark::DoNotOptimize(buffer.data());
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * size);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectGet)
    ->Arg(4096)
    ->Arg(64 * 1024)
    ->Arg(1024 * 1024)
    ->Arg(15 * 1024 * 1024);

// ============================================================================
// Deduplication Benchmarks
// ============================================================================

static void BM_ObjectPutDeduplication(benchmark::State& state) {
    InitializeObjectStore();

    const u64 size = 4096;
    std::vector<u8> data(size, 0xAB);

    // Put once to create the object
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = size;
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult first_result;
    Status s = object_put(put_params, &first_result);
    if (!is_ok(s)) {
        state.SkipWithError("Setup put failed");
        return;
    }

    for (auto _ : state) {
        // Put same content again (should hit deduplication path)
        ObjectPutResult result;
        Status s = object_put(put_params, &result);

        if (!is_ok(s)) {
            state.SkipWithError("Put failed");
            break;
        }

        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectPutDeduplication);

// ============================================================================
// Exists Benchmark
// ============================================================================

static void BM_ObjectExists(benchmark::State& state) {
    InitializeObjectStore();

    std::vector<u8> data(1024, 0xCD);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    if (!is_ok(s)) {
        state.SkipWithError("Setup failed");
        return;
    }

    for (auto _ : state) {
        bool exists = false;
        Status s = object_exists(put_result.key, &exists);

        if (!is_ok(s)) {
            state.SkipWithError("Exists failed");
            break;
        }

        benchmark::DoNotOptimize(exists);
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectExists);

// ============================================================================
// Parallel Put Benchmarks
// ============================================================================

static void BM_ObjectParallelPut(benchmark::State& state) {
    InitializeObjectStore();

    const int num_threads = state.range(0);

    for (auto _ : state) {
        state.PauseTiming();

        std::vector<std::thread> threads;
        std::atomic<int> counter{0};

        state.ResumeTiming();

        // Spawn threads
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([t, &counter]() {
                std::vector<u8> data(4096);

                // Unique data per thread
                u32 seed = t * 1000 + counter.fetch_add(1);
                for (size_t i = 0; i < data.size(); ++i) {
                    data[i] = static_cast<u8>((seed + i) & 0xFF);
                }

                ObjectPutParams params;
                params.zone = ZoneId{static_cast<u32>(t + 1)};
                params.data = data.data();
                params.size_bytes = data.size();
                params.filename = nullptr;
                params.mime_type = nullptr;

                ObjectPutResult result;
                object_put(params, &result);
            });
        }

        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
    }

    state.SetItemsProcessed(state.iterations() * num_threads);
}
BENCHMARK(BM_ObjectParallelPut)
    ->Arg(1)
    ->Arg(2)
    ->Arg(4)
    ->Arg(8)
    ->Arg(16)
    ->Arg(32)
    ->UseRealTime();

// ============================================================================
// Parallel Get Benchmarks
// ============================================================================

static void BM_ObjectParallelGet(benchmark::State& state) {
    InitializeObjectStore();

    const int num_threads = state.range(0);

    // Setup: put one object to read
    std::vector<u8> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<u8>(i & 0xFF);
    }

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    if (!is_ok(s)) {
        state.SkipWithError("Setup failed");
        return;
    }

    for (auto _ : state) {
        state.PauseTiming();

        std::vector<std::thread> threads;

        state.ResumeTiming();

        // Spawn threads all reading same object
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&put_result]() {
                std::vector<u8> buffer(4096);
                ObjectGetResult get_result;
                object_get(put_result.key, buffer.data(), buffer.size(), &get_result);
            });
        }

        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
    }

    state.SetItemsProcessed(state.iterations() * num_threads);
}
BENCHMARK(BM_ObjectParallelGet)
    ->Arg(1)
    ->Arg(2)
    ->Arg(4)
    ->Arg(8)
    ->Arg(16)
    ->Arg(32)
    ->UseRealTime();

// ============================================================================
// Throughput Benchmarks (Batch Operations)
// ============================================================================

static void BM_ObjectThroughputPut(benchmark::State& state) {
    InitializeObjectStore();

    const size_t batch_size = static_cast<size_t>(state.range(0));

    for (auto _ : state) {
        for (size_t i = 0; i < batch_size; ++i) {
            std::vector<u8> data(4096);

            // Unique data
            for (size_t j = 0; j < data.size(); ++j) {
                data[j] = static_cast<u8>((i + j) & 0xFF);
            }

            ObjectPutParams params;
            params.zone = ZoneId{1};
            params.data = data.data();
            params.size_bytes = data.size();
            params.filename = nullptr;
            params.mime_type = nullptr;

            ObjectPutResult result;
            Status s = object_put(params, &result);

            if (!is_ok(s)) {
                state.SkipWithError("Put failed");
                return;
            }
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * batch_size);
}
BENCHMARK(BM_ObjectThroughputPut)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

static void BM_ObjectThroughputGet(benchmark::State& state) {
    InitializeObjectStore();

    const size_t batch_size = static_cast<size_t>(state.range(0));

    // Setup: create objects
    std::vector<ObjectKey> keys;
    for (size_t i = 0; i < batch_size; ++i) {
        std::vector<u8> data(4096);
        for (size_t j = 0; j < data.size(); ++j) {
            data[j] = static_cast<u8>((i * 7 + j) & 0xFF);
        }

        ObjectPutParams params;
        params.zone = ZoneId{1};
        params.data = data.data();
        params.size_bytes = data.size();
        params.filename = nullptr;
        params.mime_type = nullptr;

        ObjectPutResult result;
        Status s = object_put(params, &result);
        if (is_ok(s)) {
            keys.push_back(result.key);
        }
    }

    if (keys.size() != batch_size) {
        state.SkipWithError("Setup failed");
        return;
    }

    std::vector<u8> buffer(4096);

    for (auto _ : state) {
        for (size_t i = 0; i < batch_size; ++i) {
            ObjectGetResult result;
            Status s = object_get(keys[i], buffer.data(), buffer.size(), &result);

            if (!is_ok(s)) {
                state.SkipWithError("Get failed");
                return;
            }

            benchmark::DoNotOptimize(result);
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * batch_size);
}
BENCHMARK(BM_ObjectThroughputGet)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

// ============================================================================
// Mixed Workload Benchmark
// ============================================================================

static void BM_ObjectMixedWorkload(benchmark::State& state) {
    InitializeObjectStore();

    std::vector<ObjectKey> keys;
    u32 counter = 0;

    for (auto _ : state) {
        // 70% gets, 30% puts
        int op = (counter++) % 10;

        if (op < 7 && !keys.empty()) {
            // Get
            size_t idx = counter % keys.size();
            std::vector<u8> buffer(4096);
            ObjectGetResult result;
            object_get(keys[idx], buffer.data(), buffer.size(), &result);
        } else {
            // Put
            std::vector<u8> data(4096);
            for (size_t i = 0; i < data.size(); ++i) {
                data[i] = static_cast<u8>((counter + i) & 0xFF);
            }

            ObjectPutParams params;
            params.zone = ZoneId{1};
            params.data = data.data();
            params.size_bytes = data.size();
            params.filename = nullptr;
            params.mime_type = nullptr;

            ObjectPutResult result;
            Status s = object_put(params, &result);
            if (is_ok(s)) {
                keys.push_back(result.key);

                // Limit cache size
                if (keys.size() > 100) {
                    keys.erase(keys.begin());
                }
            }
        }
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectMixedWorkload);

// ============================================================================
// Comparison with Old Database Layer (if available)
// ============================================================================

// Note: This assumes db_object_put/get still exist (legacy API)
#ifdef SARC_HAVE_LEGACY_DB_API
#include "sarc/db/db.hpp"

static void BM_LegacyDbObjectPut(benchmark::State& state) {
    const u64 size = static_cast<u64>(state.range(0));
    std::vector<u8> data(size);

    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<u8>((i * 11) & 0xFF);
    }

    // Open database
    db::DbConfig cfg{};
    cfg.path = "/tmp/legacy_bench.db";
    db::DbHandle db;
    db::db_open(cfg, &db);

    u32 counter = 0;

    for (auto _ : state) {
        data[0] = static_cast<u8>(counter++ & 0xFF);

        ObjectKey key;
        key.zone = ZoneId{1};
        // Compute hash
        hash_compute(data.data(), size, &key.content);

        db::DbObjectPutParams params;
        params.key = key;
        params.data = data.data();
        params.size_bytes = size;

        Status s = db::db_object_put(db, params);

        if (!is_ok(s)) {
            state.SkipWithError("Legacy put failed");
            break;
        }
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * size);

    db::db_close(db);
    system("rm -f /tmp/legacy_bench.db");
}
BENCHMARK(BM_LegacyDbObjectPut)
    ->Arg(4096)
    ->Arg(64 * 1024)
    ->Arg(1024 * 1024);
#endif
