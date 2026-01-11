// Erlang NIF implementation for SARC object store (READ operations)
// Provides low-latency access to object_get, object_exists, and object_query

#include <cstdlib>
#include <cstring>
#include <vector>
#include <sys/stat.h>
#include <string>

#include <erl_nif.h>

#include "sarc/bindings/erlang.hpp"
#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/storage/object_store.hpp"

namespace {

using namespace sarc;
using namespace sarc::core;
using namespace sarc::storage;
using namespace sarc::bindings::erlang;

// NIF resource type for object store handle (singleton)
static ErlNifResourceType* OBJECT_STORE_RESOURCE = nullptr;

// Global object store initialization flag
static bool g_store_initialized = false;

// ============================================================================
// Utility Functions: Erlang ↔ C++ Conversions
// ============================================================================

// Convert 32-byte Erlang binary to Hash256
Status erl_binary_to_hash256(ErlNifEnv* env, ERL_NIF_TERM term, Hash256* out) noexcept {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin)) {
        return make_status(StatusDomain::Bindings, StatusCode::Invalid);
    }

    if (bin.size != 32) {
        return make_status(StatusDomain::Bindings, StatusCode::Invalid);
    }

    std::memcpy(out->b.data(), bin.data, 32);
    return ok_status();
}

// Convert Hash256 to Erlang binary
ERL_NIF_TERM hash256_to_erl_binary(ErlNifEnv* env, const Hash256& hash) noexcept {
    ErlNifBinary bin;
    if (!enif_alloc_binary(32, &bin)) {
        return enif_make_badarg(env);
    }

    std::memcpy(bin.data, hash.b.data(), 32);
    return enif_make_binary(env, &bin);
}

// Convert ObjectGetResult to Erlang map
ERL_NIF_TERM object_result_to_erl_map(ErlNifEnv* env, const ObjectGetResult& result) noexcept {
    ERL_NIF_TERM map = enif_make_new_map(env);

    // Add size_bytes
    ERL_NIF_TERM size_key = enif_make_atom(env, "size");
    ERL_NIF_TERM size_val = enif_make_uint64(env, result.meta.size_bytes);
    enif_make_map_put(env, map, size_key, size_val, &map);

    // Add refcount
    ERL_NIF_TERM refcount_key = enif_make_atom(env, "refcount");
    ERL_NIF_TERM refcount_val = enif_make_uint(env, result.meta.refcount);
    enif_make_map_put(env, map, refcount_key, refcount_val, &map);

    // Add created_at
    ERL_NIF_TERM created_key = enif_make_atom(env, "created_at");
    ERL_NIF_TERM created_val = enif_make_int64(env, result.meta.created_at);
    enif_make_map_put(env, map, created_key, created_val, &map);

    // Add updated_at
    ERL_NIF_TERM updated_key = enif_make_atom(env, "updated_at");
    ERL_NIF_TERM updated_val = enif_make_int64(env, result.meta.updated_at);
    enif_make_map_put(env, map, updated_key, updated_val, &map);

    // Add filename (if present)
    if (result.filename[0] != '\0') {
        ERL_NIF_TERM filename_key = enif_make_atom(env, "filename");
        ErlNifBinary filename_bin;
        size_t len = std::strlen(result.filename);
        if (enif_alloc_binary(len, &filename_bin)) {
            std::memcpy(filename_bin.data, result.filename, len);
            enif_make_map_put(env, map, filename_key, enif_make_binary(env, &filename_bin), &map);
        }
    }

    // Add mime_type (if present)
    if (result.mime_type[0] != '\0') {
        ERL_NIF_TERM mime_key = enif_make_atom(env, "mime_type");
        ErlNifBinary mime_bin;
        size_t len = std::strlen(result.mime_type);
        if (enif_alloc_binary(len, &mime_bin)) {
            std::memcpy(mime_bin.data, result.mime_type, len);
            enif_make_map_put(env, map, mime_key, enif_make_binary(env, &mime_bin), &map);
        }
    }

    // Add fs_path
    if (result.fs_path[0] != '\0') {
        ERL_NIF_TERM path_key = enif_make_atom(env, "fs_path");
        ErlNifBinary path_bin;
        size_t len = std::strlen(result.fs_path);
        if (enif_alloc_binary(len, &path_bin)) {
            std::memcpy(path_bin.data, result.fs_path, len);
            enif_make_map_put(env, map, path_key, enif_make_binary(env, &path_bin), &map);
        }
    }

    return map;
}

// Convert Status to Erlang error tuple {error, Reason}
ERL_NIF_TERM status_to_erl_error(ErlNifEnv* env, Status s) noexcept {
    const char* reason = nullptr;

    switch (s.code) {
        case StatusCode::Ok:
            return enif_make_atom(env, "ok");
        case StatusCode::NotFound:
            reason = "not_found";
            break;
        case StatusCode::Invalid:
            reason = "invalid";
            break;
        case StatusCode::Io:
            reason = "io_error";
            break;
        case StatusCode::Corrupt:
            reason = "corrupted";
            break;
        case StatusCode::PermissionDenied:
            reason = "permission_denied";
            break;
        case StatusCode::Conflict:
            reason = "conflict";
            break;
        case StatusCode::Unavailable:
            reason = "unavailable";
            break;
        default:
            reason = "unknown_error";
            break;
    }

    return enif_make_tuple2(env,
        enif_make_atom(env, "error"),
        enif_make_atom(env, reason));
}

// ============================================================================
// NIF Function Implementations
// ============================================================================

// object_get_nif(ZoneId :: non_neg_integer(), Hash :: binary()) ->
//     {ok, Data :: binary(), Meta :: map()} | {error, Reason :: atom()}
static ERL_NIF_TERM sarc_object_get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    // Parse ZoneId
    unsigned int zone_id;
    if (!enif_get_uint(env, argv[0], &zone_id)) {
        return enif_make_badarg(env);
    }

    // Parse Hash256
    Hash256 hash;
    Status s = erl_binary_to_hash256(env, argv[1], &hash);
    if (!is_ok(s)) {
        return enif_make_badarg(env);
    }

    // Construct ObjectKey
    ObjectKey key{ZoneId{zone_id}, hash};

    // First, get metadata to know the size
    ObjectMeta meta;
    bool exists = false;
    s = object_exists(key, &exists);
    if (!is_ok(s)) {
        return status_to_erl_error(env, s);
    }

    if (!exists) {
        return status_to_erl_error(env, make_status(StatusDomain::Storage, StatusCode::NotFound));
    }

    // Get object metadata to determine size
    // For now, allocate a large buffer and realloc if needed
    // TODO: Add object_get_metadata() to object_store API
    const u64 initial_buffer_size = 1024 * 1024;  // 1MB initial buffer
    std::vector<u8> buffer(initial_buffer_size);

    ObjectGetResult result;
    s = object_get(key, buffer.data(), buffer.size(), &result);

    if (!is_ok(s)) {
        // If buffer too small, reallocate and retry
        if (s.code == StatusCode::Invalid && s.aux != 0) {
            u64 required_size = s.aux;
            buffer.resize(required_size);
            s = object_get(key, buffer.data(), buffer.size(), &result);
            if (!is_ok(s)) {
                return status_to_erl_error(env, s);
            }
        } else {
            return status_to_erl_error(env, s);
        }
    }

    // Allocate Erlang binary for data
    ErlNifBinary data_bin;
    if (!enif_alloc_binary(result.bytes_read, &data_bin)) {
        return enif_make_tuple2(env,
            enif_make_atom(env, "error"),
            enif_make_atom(env, "enomem"));
    }

    std::memcpy(data_bin.data, buffer.data(), result.bytes_read);

    // Convert metadata to Erlang map
    ERL_NIF_TERM meta_map = object_result_to_erl_map(env, result);

    // Return {ok, Data, Meta}
    return enif_make_tuple3(env,
        enif_make_atom(env, "ok"),
        enif_make_binary(env, &data_bin),
        meta_map);
}

// object_get_metadata_nif(ZoneId :: non_neg_integer(), Hash :: binary()) ->
//     {ok, Meta :: map()} | {error, Reason :: atom()}
static ERL_NIF_TERM sarc_object_get_metadata_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    unsigned int zone_id;
    if (!enif_get_uint(env, argv[0], &zone_id)) {
        return enif_make_badarg(env);
    }

    Hash256 hash;
    Status s = erl_binary_to_hash256(env, argv[1], &hash);
    if (!is_ok(s)) {
        return enif_make_badarg(env);
    }

    ObjectKey key{ZoneId{zone_id}, hash};
    ObjectGetResult result;
    s = object_get_metadata(key, &result);

    if (!is_ok(s)) {
        return status_to_erl_error(env, s);
    }

    ERL_NIF_TERM meta_map = object_result_to_erl_map(env, result);

    return enif_make_tuple2(env,
        enif_make_atom(env, "ok"),
        meta_map);
}

// object_exists_nif(ZoneId :: non_neg_integer(), Hash :: binary()) ->
//     {ok, boolean()} | {error, Reason :: atom()}
static ERL_NIF_TERM sarc_object_exists_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    // Parse ZoneId
    unsigned int zone_id;
    if (!enif_get_uint(env, argv[0], &zone_id)) {
        return enif_make_badarg(env);
    }

    // Parse Hash256
    Hash256 hash;
    Status s = erl_binary_to_hash256(env, argv[1], &hash);
    if (!is_ok(s)) {
        return enif_make_badarg(env);
    }

    // Construct ObjectKey
    ObjectKey key{ZoneId{zone_id}, hash};

    // Check existence
    bool exists = false;
    s = object_exists(key, &exists);
    if (!is_ok(s)) {
        return status_to_erl_error(env, s);
    }

    // Return {ok, true} or {ok, false}
    return enif_make_tuple2(env,
        enif_make_atom(env, "ok"),
        enif_make_atom(env, exists ? "true" : "false"));
}

// object_query_nif(ZoneId :: non_neg_integer(), Filter :: map(), Limit :: pos_integer()) ->
//     {ok, Results :: [{ZoneId :: non_neg_integer(), Hash :: binary()}]} | {error, Reason :: atom()}
static ERL_NIF_TERM sarc_object_query_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 3) {
        return enif_make_badarg(env);
    }

    // Parse ZoneId
    unsigned int zone_id;
    if (!enif_get_uint(env, argv[0], &zone_id)) {
        return enif_make_badarg(env);
    }

    // Parse Filter map (argv[1])
    // For now, we'll support basic filters
    ObjectQueryFilter filter{};

    if (enif_is_map(env, argv[1])) {
        ERL_NIF_TERM map = argv[1];

        // Try to extract filename_pattern
        ERL_NIF_TERM filename_key = enif_make_atom(env, "filename_pattern");
        ERL_NIF_TERM filename_val;
        if (enif_get_map_value(env, map, filename_key, &filename_val)) {
            ErlNifBinary filename_bin;
            if (enif_inspect_binary(env, filename_val, &filename_bin)) {
                // Note: This is a temporary pointer - filter struct needs to handle this
                // For now, we'll skip string filters and implement them later
                // TODO: Proper string handling with null termination
            }
        }

        // Try to extract min_size
        ERL_NIF_TERM min_size_key = enif_make_atom(env, "min_size");
        ERL_NIF_TERM min_size_val;
        if (enif_get_map_value(env, map, min_size_key, &min_size_val)) {
            unsigned long min_size;
            if (enif_get_uint64(env, min_size_val, &min_size)) {
                filter.min_size_bytes = min_size;
            }
        }

        // Try to extract max_size
        ERL_NIF_TERM max_size_key = enif_make_atom(env, "max_size");
        ERL_NIF_TERM max_size_val;
        if (enif_get_map_value(env, map, max_size_key, &max_size_val)) {
            unsigned long max_size;
            if (enif_get_uint64(env, max_size_val, &max_size)) {
                filter.max_size_bytes = max_size;
            }
        }
    }

    // Parse Limit
    unsigned int limit;
    if (!enif_get_uint(env, argv[2], &limit)) {
        return enif_make_badarg(env);
    }

    // Allocate results buffer
    std::vector<ObjectQueryResult> results(limit);
    u32 count = limit;

    // Execute query
    Status s = object_query(ZoneId{zone_id}, filter, results.data(), &count);
    if (!is_ok(s)) {
        return status_to_erl_error(env, s);
    }

    // Build Erlang list of maps [#{zone=>, hash=>, filename=>...}, ...]
    ERL_NIF_TERM result_list = enif_make_list(env, 0);  // Empty list

    // Build list in reverse order (more efficient)
    for (u32 i = count; i > 0; --i) {
        const ObjectQueryResult& res = results[i - 1];

        ERL_NIF_TERM map = enif_make_new_map(env);

        // Add zone
        ERL_NIF_TERM zone_key = enif_make_atom(env, "zone");
        ERL_NIF_TERM zone_val = enif_make_uint(env, res.key.zone.v);
        enif_make_map_put(env, map, zone_key, zone_val, &map);

        // Add hash
        ERL_NIF_TERM hash_key = enif_make_atom(env, "hash");
        ERL_NIF_TERM hash_val = hash256_to_erl_binary(env, res.key.content);
        enif_make_map_put(env, map, hash_key, hash_val, &map);

        // Add size
        ERL_NIF_TERM size_key = enif_make_atom(env, "size");
        ERL_NIF_TERM size_val = enif_make_uint64(env, res.meta.size_bytes);
        enif_make_map_put(env, map, size_key, size_val, &map);

        // Add created_at
        ERL_NIF_TERM created_key = enif_make_atom(env, "created_at");
        ERL_NIF_TERM created_val = enif_make_int64(env, res.meta.created_at);
        enif_make_map_put(env, map, created_key, created_val, &map);

        // Add filename (if present)
        if (res.filename[0] != '\0') {
            ERL_NIF_TERM filename_key = enif_make_atom(env, "filename");
            ErlNifBinary filename_bin;
            size_t len = std::strlen(res.filename);
            if (enif_alloc_binary(len, &filename_bin)) {
                std::memcpy(filename_bin.data, res.filename, len);
                enif_make_map_put(env, map, filename_key, enif_make_binary(env, &filename_bin), &map);
            }
        }

        // Add mime_type (if present)
        if (res.mime_type[0] != '\0') {
            ERL_NIF_TERM mime_key = enif_make_atom(env, "mime_type");
            ErlNifBinary mime_bin;
            size_t len = std::strlen(res.mime_type);
            if (enif_alloc_binary(len, &mime_bin)) {
                std::memcpy(mime_bin.data, res.mime_type, len);
                enif_make_map_put(env, map, mime_key, enif_make_binary(env, &mime_bin), &map);
            }
        }

        result_list = enif_make_list_cell(env, map, result_list);
    }

    // Return {ok, Results}
    return enif_make_tuple2(env,
        enif_make_atom(env, "ok"),
        result_list);
}

// object_count_nif(ZoneId :: non_neg_integer()) ->
//     {ok, Count :: non_neg_integer()} | {error, Reason :: atom()}
static ERL_NIF_TERM sarc_object_count_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }

    unsigned int zone_id;
    if (!enif_get_uint(env, argv[0], &zone_id)) {
        return enif_make_badarg(env);
    }

    u64 count = 0;
    Status s = object_count(ZoneId{zone_id}, &count);
    if (!is_ok(s)) {
        return status_to_erl_error(env, s);
    }

    return enif_make_tuple2(env,
        enif_make_atom(env, "ok"),
        enif_make_uint64(env, count));
}

// object_db_info_nif() ->
//     {ok, #{db_path := binary(), inode := non_neg_integer(), size := non_neg_integer()}} | {error, Reason}
static ERL_NIF_TERM sarc_object_db_info_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 0) {
        return enif_make_badarg(env);
    }

    const char* db_path = nullptr;
    Status s = object_db_filename(&db_path);
    if (!is_ok(s) || !db_path) {
        return status_to_erl_error(env, s);
    }

    struct stat st {};
    int rc = stat(db_path, &st);
    if (rc != 0) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "stat_failed"));
    }

    ERL_NIF_TERM map = enif_make_new_map(env);

    ErlNifBinary path_bin;
    size_t len = std::strlen(db_path);
    if (enif_alloc_binary(len, &path_bin)) {
        std::memcpy(path_bin.data, db_path, len);
        ERL_NIF_TERM path_key = enif_make_atom(env, "db_path");
        enif_make_map_put(env, map, path_key, enif_make_binary(env, &path_bin), &map);
    }

    ERL_NIF_TERM inode_key = enif_make_atom(env, "inode");
    ERL_NIF_TERM inode_val = enif_make_uint64(env, static_cast<u64>(st.st_ino));
    enif_make_map_put(env, map, inode_key, inode_val, &map);

    ERL_NIF_TERM size_key = enif_make_atom(env, "size");
    ERL_NIF_TERM size_val = enif_make_uint64(env, static_cast<u64>(st.st_size));
    enif_make_map_put(env, map, size_key, size_val, &map);

    return enif_make_tuple2(env, enif_make_atom(env, "ok"), map);
}

// ============================================================================
// NIF Module Initialization
// ============================================================================

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    // Initialize object store on NIF load
    ObjectStoreConfig cfg{};

    // Read configuration from load_info map (preferred), fallback to env
    const char* data_root = nullptr;
    const char* db_path = nullptr;
    std::string data_root_buf;
    std::string db_path_buf;

    if (enif_is_map(env, load_info)) {
        ERL_NIF_TERM data_key = enif_make_atom(env, "data_root");
        ERL_NIF_TERM db_key = enif_make_atom(env, "db_path");
        ERL_NIF_TERM data_val;
        ERL_NIF_TERM db_val;

        if (enif_get_map_value(env, load_info, data_key, &data_val)) {
            ErlNifBinary bin;
            if (enif_inspect_binary(env, data_val, &bin)) {
                data_root_buf.assign(reinterpret_cast<const char*>(bin.data), bin.size);
                data_root = data_root_buf.c_str();
            }
        }

        if (enif_get_map_value(env, load_info, db_key, &db_val)) {
            ErlNifBinary bin;
            if (enif_inspect_binary(env, db_val, &bin)) {
                db_path_buf.assign(reinterpret_cast<const char*>(bin.data), bin.size);
                db_path = db_path_buf.c_str();
            }
        }
    }

    if (!data_root) {
        data_root = std::getenv("SARC_DATA_ROOT");
    }
    if (!db_path) {
        db_path = std::getenv("SARC_DB_PATH");
    }

    cfg.data_root = data_root ? data_root : "/tmp/sarc_data";
    cfg.db_path = db_path ? db_path : "/tmp/sarc.db";
    cfg.max_object_bytes = 0;          // Unlimited
    cfg.compression = CompressionPolicy::None;
    cfg.verify_on_read = false;

    Status s = object_store_init(cfg);
    if (!is_ok(s)) {
        return 1;  // Initialization failed
    }

    g_store_initialized = true;

    // Create resource type (for future use)
    OBJECT_STORE_RESOURCE = enif_open_resource_type(
        env,
        nullptr,
        "object_store_handle",
        nullptr,
        static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
        nullptr);

    if (OBJECT_STORE_RESOURCE == nullptr) {
        (void)object_store_shutdown();
        return 1;
    }

    return 0;  // Success
}

static void unload(ErlNifEnv* env, void* priv_data) {
    if (g_store_initialized) {
        (void)object_store_shutdown();
        g_store_initialized = false;
    }
}

// NIF function exports
static ErlNifFunc nif_funcs[] = {
    {"object_get_nif", 2, sarc_object_get_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"object_get_metadata_nif", 2, sarc_object_get_metadata_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"object_exists_nif", 2, sarc_object_exists_nif, 0},
    {"object_query_nif", 3, sarc_object_query_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"object_count_nif", 1, sarc_object_count_nif, 0},
    {"object_db_info_nif", 0, sarc_object_db_info_nif, 0}
};

// NIF module initialization macro
ERL_NIF_INIT(sarc_nif, nif_funcs, load, nullptr, nullptr, unload)

}  // namespace
