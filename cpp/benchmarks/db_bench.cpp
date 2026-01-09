#include <benchmark/benchmark.h>
#include "sarc/db/db.hpp"
#include "sarc/core/zone.hpp"
#include <vector>
#include <cstring>

using namespace sarc::db;
using namespace sarc::core;

namespace {

Zone make_bench_zone(u32 id) {
    Zone z;
    z.id = ZoneId{id};
    z.type = ZoneType::User;
    z.owner = UserId{1000};
    z.category = CategoryKey{0};
    z.name = StringId{1};
    z.created_at = 1234567890;
    z.updated_at = 1234567890;
    return z;
}

ObjectKey make_bench_object_key(u32 zone_id, u32 counter) {
    ObjectKey k;
    k.zone = ZoneId{zone_id};
    // Use counter to create unique keys
    std::memcpy(k.content.b.data(), &counter, sizeof(counter));
    for (size_t i = sizeof(counter); i < 32; ++i) {
        k.content.b[i] = static_cast<u8>(counter ^ i);
    }
    return k;
}

} // namespace

//=============================================================================
// Database Lifecycle Benchmarks
//=============================================================================

static void BM_DatabaseOpen(benchmark::State& state) {
    DbConfig cfg{};

    for (auto _ : state) {
        DbHandle handle;
        Status s = db_open(cfg, &handle);
        benchmark::DoNotOptimize(s);
        db_close(handle);
    }
}
BENCHMARK(BM_DatabaseOpen);

static void BM_DatabaseClose(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    for (auto _ : state) {
        state.PauseTiming();
        DbHandle temp_handle;
        db_open(cfg, &temp_handle);
        state.ResumeTiming();

        Status s = db_close(temp_handle);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
}
BENCHMARK(BM_DatabaseClose);

//=============================================================================
// Transaction Benchmarks
//=============================================================================

static void BM_TransactionBeginCommit(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    for (auto _ : state) {
        DbTxn txn;
        db_txn_begin(handle, &txn);
        Status s = db_txn_commit(txn);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
}
BENCHMARK(BM_TransactionBeginCommit);

static void BM_TransactionRollback(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    for (auto _ : state) {
        DbTxn txn;
        db_txn_begin(handle, &txn);
        Status s = db_txn_rollback(txn);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
}
BENCHMARK(BM_TransactionRollback);

//=============================================================================
// Zone Benchmarks
//=============================================================================

static void BM_ZoneCreate(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u32 zone_counter = 1;
    for (auto _ : state) {
        Zone zone = make_bench_zone(zone_counter++);
        Status s = db_zone_create(handle, zone);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ZoneCreate);

static void BM_ZoneGet(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    // Pre-populate zones
    const int num_zones = static_cast<int>(state.range(0));
    for (int i = 1; i <= num_zones; ++i) {
        Zone zone = make_bench_zone(i);
        db_zone_create(handle, zone);
    }

    int zone_id = 1;
    for (auto _ : state) {
        Zone zone;
        Status s = db_zone_get(handle, ZoneId{static_cast<u32>(zone_id)}, &zone);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(zone);

        zone_id = (zone_id % num_zones) + 1;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ZoneGet)->Arg(10)->Arg(100)->Arg(1000);

static void BM_ZoneUpdate(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    Zone zone = make_bench_zone(1);
    db_zone_create(handle, zone);

    u32 update_counter = 0;
    for (auto _ : state) {
        zone.owner = UserId{update_counter++};
        Status s = db_zone_update(handle, zone);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ZoneUpdate);

static void BM_ZoneDelete(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u32 zone_counter = 1;
    for (auto _ : state) {
        state.PauseTiming();
        Zone zone = make_bench_zone(zone_counter);
        db_zone_create(handle, zone);
        state.ResumeTiming();

        Status s = db_zone_delete(handle, ZoneId{zone_counter});
        benchmark::DoNotOptimize(s);
        zone_counter++;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ZoneDelete);

static void BM_ZoneExists(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    Zone zone = make_bench_zone(1);
    db_zone_create(handle, zone);

    for (auto _ : state) {
        bool exists;
        Status s = db_zone_exists(handle, ZoneId{1}, &exists);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(exists);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ZoneExists);

//=============================================================================
// Object Metadata Benchmarks (Hybrid Store)
//=============================================================================

static void BM_ObjectPutMetadata(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    const size_t data_size = static_cast<size_t>(state.range(0));
    u32 obj_counter = 1;

    for (auto _ : state) {
        ObjectKey key = make_bench_object_key(1, obj_counter);

        // Construct filesystem path
        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/1/%02x/%02x/%08x.dat",
                 (obj_counter >> 8) & 0xFF, obj_counter & 0xFF, obj_counter);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = data_size;
        params.fs_path = fs_path;

        Status s = db_object_put_metadata(handle, params);
        benchmark::DoNotOptimize(s);

        obj_counter++;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectPutMetadata)->Arg(0)->Arg(64)->Arg(1024)->Arg(4096)->Arg(65536)->Arg(1024*1024);

static void BM_ObjectGetMetadata(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    const size_t data_size = static_cast<size_t>(state.range(0));

    // Pre-populate objects
    const int num_objects = 100;
    for (int i = 1; i <= num_objects; ++i) {
        ObjectKey key = make_bench_object_key(1, i);

        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/1/%02x/%02x/%08x.dat",
                 (i >> 8) & 0xFF, i & 0xFF, i);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = data_size;
        params.fs_path = fs_path;
        db_object_put_metadata(handle, params);
    }

    int obj_id = 1;

    for (auto _ : state) {
        ObjectKey key = make_bench_object_key(1, obj_id);
        DbObjectMetadata metadata;
        Status s = db_object_get_metadata(handle, key, &metadata);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(metadata);

        obj_id = (obj_id % num_objects) + 1;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectGetMetadata)->Arg(64)->Arg(1024)->Arg(4096)->Arg(65536)->Arg(1024*1024);

static void BM_ObjectExists(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    ObjectKey key = make_bench_object_key(1, 1);

    DbObjectPutMetadataParams params;
    params.key = key;
    params.size_bytes = 1024;
    params.fs_path = "/data/objects/1/00/01/test.dat";
    db_object_put_metadata(handle, params);

    for (auto _ : state) {
        bool exists;
        Status s = db_object_exists(handle, key, &exists);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(exists);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectExists);

static void BM_ObjectDelete(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u32 obj_counter = 1;

    for (auto _ : state) {
        state.PauseTiming();
        ObjectKey key = make_bench_object_key(1, obj_counter);

        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/1/%02x/%02x/%08x.dat",
                 (obj_counter >> 8) & 0xFF, obj_counter & 0xFF, obj_counter);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = 512;
        params.fs_path = fs_path;
        db_object_put_metadata(handle, params);
        state.ResumeTiming();

        Status s = db_object_delete(handle, key);
        benchmark::DoNotOptimize(s);
        obj_counter++;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectDelete);

static void BM_ObjectIncrementRefcount(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    // Pre-create object
    ObjectKey key = make_bench_object_key(1, 1);
    DbObjectPutMetadataParams params;
    params.key = key;
    params.size_bytes = 2048;
    params.fs_path = "/data/objects/1/00/01/refcount_test.dat";
    db_object_put_metadata(handle, params);

    for (auto _ : state) {
        Status s = db_object_increment_refcount(handle, key);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectIncrementRefcount);

static void BM_ObjectDecrementRefcount(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    // Pre-create object with high refcount
    ObjectKey key = make_bench_object_key(1, 1);
    DbObjectPutMetadataParams params;
    params.key = key;
    params.size_bytes = 2048;
    params.fs_path = "/data/objects/1/00/01/refcount_test.dat";
    db_object_put_metadata(handle, params);

    // Bump refcount high
    for (int i = 0; i < 10000; ++i) {
        db_object_increment_refcount(handle, key);
    }

    for (auto _ : state) {
        u32 new_refcount;
        Status s = db_object_decrement_refcount(handle, key, &new_refcount);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(new_refcount);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectDecrementRefcount);

static void BM_ObjectGcQuery(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    const int num_orphaned = static_cast<int>(state.range(0));

    // Create orphaned objects (refcount=0)
    for (int i = 0; i < num_orphaned; ++i) {
        ObjectKey key = make_bench_object_key(1, i + 1000);

        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/1/%02x/%02x/orphan%d.dat",
                 (i >> 8) & 0xFF, i & 0xFF, i);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = 4096;
        params.fs_path = fs_path;
        db_object_put_metadata(handle, params);

        // Decrement to refcount=0
        u32 new_refcount;
        db_object_decrement_refcount(handle, key, &new_refcount);
    }

    DbObjectGcEntry entries[1000];

    for (auto _ : state) {
        u32 count = 1000;
        Status s = db_object_gc_query(handle, ZoneId{1}, entries, &count);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(count);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectGcQuery)->Arg(10)->Arg(100)->Arg(1000);

static void BM_ObjectListQuery(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    const int num_objects = 1000;

    // Create many objects
    for (int i = 0; i < num_objects; ++i) {
        ObjectKey key = make_bench_object_key(1, i + 5000);

        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/1/%02x/%02x/list%d.dat",
                 (i >> 8) & 0xFF, i & 0xFF, i);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = 1000 + (i * 10);  // Varying sizes
        params.fs_path = fs_path;
        db_object_put_metadata(handle, params);
    }

    DbObjectQueryFilter filter;
    filter.min_size_bytes = 5000;
    filter.max_size_bytes = 7000;
    filter.created_after = 0;
    filter.created_before = 0;
    filter.limit = 100;

    ObjectKey results[100];

    for (auto _ : state) {
        u32 count = 100;
        Status s = db_object_list(handle, ZoneId{1}, filter, results, &count);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(count);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ObjectListQuery);

//=============================================================================
// File Benchmarks
//=============================================================================

static void BM_FileCreate(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u64 file_counter = 1;
    for (auto _ : state) {
        FileMeta file;
        file.id = FileId{file_counter++};
        file.zone = ZoneId{1};
        std::memcpy(file.content.b.data(), &file_counter, sizeof(file_counter));
        file.name = StringId{10};
        file.mime = StringId{20};
        file.size_bytes = 1024;
        file.created_at = 111111;
        file.updated_at = 222222;

        Status s = db_file_create(handle, file);
        benchmark::DoNotOptimize(s);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FileCreate);

static void BM_FileGet(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    // Pre-populate files
    const int num_files = static_cast<int>(state.range(0));
    for (int i = 1; i <= num_files; ++i) {
        FileMeta file;
        file.id = FileId{static_cast<u64>(i)};
        file.zone = ZoneId{1};
        std::memcpy(file.content.b.data(), &i, sizeof(i));
        file.name = StringId{10};
        file.mime = StringId{20};
        file.size_bytes = 2048;
        file.created_at = 333333;
        file.updated_at = 444444;
        db_file_create(handle, file);
    }

    int file_id = 1;
    for (auto _ : state) {
        FileMeta file;
        Status s = db_file_get(handle, FileId{static_cast<u64>(file_id)}, &file);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(file);

        file_id = (file_id % num_files) + 1;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FileGet)->Arg(10)->Arg(100)->Arg(1000);

static void BM_FileDelete(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u64 file_counter = 1;
    for (auto _ : state) {
        state.PauseTiming();
        FileMeta file;
        file.id = FileId{file_counter};
        file.zone = ZoneId{1};
        std::memcpy(file.content.b.data(), &file_counter, sizeof(file_counter));
        file.name = StringId{10};
        file.mime = StringId{20};
        file.size_bytes = 512;
        file.created_at = 555555;
        file.updated_at = 666666;
        db_file_create(handle, file);
        state.ResumeTiming();

        Status s = db_file_delete(handle, FileId{file_counter});
        benchmark::DoNotOptimize(s);
        file_counter++;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FileDelete);

//=============================================================================
// String Interning Benchmarks
//=============================================================================

static void BM_StringIntern(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    int counter = 0;
    for (auto _ : state) {
        std::string str = "test_string_" + std::to_string(counter++);
        StringId id;
        Status s = db_string_intern(handle, str.c_str(), &id);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(id);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_StringIntern);

static void BM_StringInternDuplicate(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    const char* constant_str = "duplicate_string";

    for (auto _ : state) {
        StringId id;
        Status s = db_string_intern(handle, constant_str, &id);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(id);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_StringInternDuplicate);

static void BM_StringGet(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    StringId id;
    db_string_intern(handle, "benchmark_test_string", &id);

    char buffer[256];
    u32 len;

    for (auto _ : state) {
        Status s = db_string_get(handle, id, buffer, 256, &len);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(buffer);
        benchmark::DoNotOptimize(len);
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_StringGet);

//=============================================================================
// Integrated Benchmarks
//=============================================================================

static void BM_IntegratedWorkload(benchmark::State& state) {
    DbConfig cfg{};
    DbHandle handle;
    db_open(cfg, &handle);

    u32 zone_counter = 1;
    u32 obj_counter = 1;

    for (auto _ : state) {
        // Create zone
        Zone zone = make_bench_zone(zone_counter);
        db_zone_create(handle, zone);

        // Put object metadata
        ObjectKey key = make_bench_object_key(zone_counter, obj_counter);

        char fs_path[256];
        snprintf(fs_path, sizeof(fs_path), "/data/objects/%u/%02x/%02x/%08x.dat",
                 zone_counter, (obj_counter >> 8) & 0xFF, obj_counter & 0xFF, obj_counter);

        DbObjectPutMetadataParams params;
        params.key = key;
        params.size_bytes = 1024;
        params.fs_path = fs_path;
        db_object_put_metadata(handle, params);

        // Read back object metadata
        DbObjectMetadata metadata;
        db_object_get_metadata(handle, key, &metadata);

        zone_counter++;
        obj_counter++;
    }

    db_close(handle);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_IntegratedWorkload);
