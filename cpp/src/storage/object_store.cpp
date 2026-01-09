#include "sarc/storage/object_store.hpp"
#include "sarc/storage/hashing.hpp"
#include "sarc/db/db.hpp"
#include "sarc/core/zone.hpp"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <mutex>
#include <vector>

namespace sarc::storage {

using namespace sarc::core;

// ========================================================================
// Internal State
// ========================================================================

struct ObjectStoreState {
    ObjectStoreConfig config;
    db::DbHandle db;
    std::mutex mutex;  // Protects initialization/shutdown
    bool initialized;
};

static ObjectStoreState g_state = {
    .config = {},
    .db = {0},
    .mutex = {},
    .initialized = false
};

// ========================================================================
// Internal Helpers
// ========================================================================

// Convert hash to hex string
static void hash_to_hex(const Hash256& hash, char* out, size_t out_size) {
    static const char hex[] = "0123456789abcdef";
    size_t pos = 0;
    for (size_t i = 0; i < hash.b.size() && pos + 2 < out_size; ++i) {
        out[pos++] = hex[(hash.b[i] >> 4) & 0xF];
        out[pos++] = hex[hash.b[i] & 0xF];
    }
    out[pos] = '\0';
}

// Check if we should compress based on MIME type
static bool should_compress(CompressionPolicy policy, const char* mime_type, u64 size_bytes) {
    if (policy == CompressionPolicy::None) return false;
    if (policy == CompressionPolicy::AlwaysLZ4 || policy == CompressionPolicy::AlwaysZstd) return true;

    // Auto policy
    if (size_bytes < 4096) return false;  // Too small

    if (!mime_type) return true;  // Default: compress

    // Never compress already-compressed formats
    if (strncmp(mime_type, "image/jpeg", 10) == 0) return false;
    if (strncmp(mime_type, "image/png", 9) == 0) return false;
    if (strncmp(mime_type, "video/", 6) == 0) return false;
    if (strncmp(mime_type, "audio/", 6) == 0) return false;

    // Always compress text
    if (strncmp(mime_type, "text/", 5) == 0) return true;

    // Default: compress
    return true;
}

// Construct filesystem path for object
// Format: {data_root}/{zone}/{hash[0:2]}/{hash[2:4]}/{hash}.dat[.lz4]
static void construct_fs_path(const char* data_root, ZoneId zone, const Hash256& hash,
                               bool compressed, char* out, size_t out_size) {
    char hash_str[65];
    hash_to_hex(hash, hash_str, sizeof(hash_str));

    const char* ext = compressed ? ".dat.lz4" : ".dat";

    snprintf(out, out_size, "%s/%u/%c%c/%c%c/%s%s",
             data_root,
             zone.v,
             hash_str[0], hash_str[1],
             hash_str[2], hash_str[3],
             hash_str,
             ext);
}

// Create directory hierarchy recursively
static Status create_directories(const char* path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);

    // Find last '/' to get directory path
    char* last_slash = strrchr(tmp, '/');
    if (!last_slash) {
        return ok_status();
    }

    *last_slash = '\0';  // Truncate at last slash to get directory

    // Try to create directory
    if (mkdir(tmp, 0755) == 0) {
        return ok_status();  // Created successfully
    }

    if (errno == EEXIST) {
        return ok_status();  // Already exists
    }

    if (errno == ENOENT) {
        // Parent doesn't exist, recurse
        Status s = create_directories(tmp);
        if (!is_ok(s)) return s;

        // Now create this directory
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }
        return ok_status();
    }

    return make_status(StatusDomain::Storage, StatusCode::Io, errno);
}

// ========================================================================
// Public API Implementation
// ========================================================================

Status object_store_init(const ObjectStoreConfig& cfg) noexcept {
    std::lock_guard<std::mutex> lock(g_state.mutex);

    if (g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate config
    if (!cfg.data_root || !cfg.db_path) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Open database
    db::DbConfig db_cfg{};
    db_cfg.path = cfg.db_path;

    Status s = db::db_open(db_cfg, &g_state.db);
    if (!is_ok(s)) {
        return s;
    }

    // Store config
    g_state.config = cfg;
    g_state.initialized = true;

    return ok_status();
}

Status object_store_shutdown() noexcept {
    std::lock_guard<std::mutex> lock(g_state.mutex);

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Close database
    Status s = db::db_close(g_state.db);

    g_state.initialized = false;
    g_state.db = {0};

    return s;
}

Status object_put(const ObjectPutParams& params, ObjectPutResult* result) noexcept {
    if (!result) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate inputs
    if (!params.zone.is_valid() || zone_is_universal(params.zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!params.data && params.size_bytes > 0) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (g_state.config.max_object_bytes > 0 && params.size_bytes > g_state.config.max_object_bytes) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid, params.size_bytes);
    }

    // Compute content hash
    Hash256 content_hash;
    Status s = hash_compute(BufferView{params.data, static_cast<u32>(params.size_bytes)}, &content_hash);
    if (!is_ok(s)) {
        return s;
    }

    ObjectKey key;
    key.zone = params.zone;
    key.content = content_hash;

    // Check if object already exists (deduplication)
    bool exists = false;
    s = db::db_object_exists(g_state.db, key, &exists);
    if (!is_ok(s)) {
        return s;
    }

    if (exists) {
        // Object already exists, increment refcount
        s = db::db_object_increment_refcount(g_state.db, key);
        if (!is_ok(s)) {
            return s;
        }

        // Get metadata
        db::DbObjectMetadata db_meta;
        s = db::db_object_get_metadata(g_state.db, key, &db_meta);
        if (!is_ok(s)) {
            return s;
        }

        result->key = key;
        result->meta.size_bytes = db_meta.size_bytes;
        result->meta.refcount = db_meta.refcount;
        result->meta.created_at = db_meta.created_at;
        result->meta.updated_at = db_meta.updated_at;
        result->deduplicated = true;

        return ok_status();
    }

    // Determine if we should compress
    bool compress = should_compress(g_state.config.compression, params.mime_type, params.size_bytes);

    // TODO: Implement compression (for now, always uncompressed)
    compress = false;

    // Construct filesystem path
    char fs_path[1024];
    construct_fs_path(g_state.config.data_root, params.zone, content_hash, compress, fs_path, sizeof(fs_path));

    // Create directory hierarchy
    s = create_directories(fs_path);
    if (!is_ok(s)) {
        return s;
    }

    // Write blob to filesystem
    int fd = open(fs_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        if (errno == EEXIST) {
            // File already exists (race condition with another writer)
            // This is okay, just proceed to database insertion
        } else {
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }
    } else {
        // Write data
        ssize_t written = 0;
        while (written < static_cast<ssize_t>(params.size_bytes)) {
            ssize_t n = write(fd, params.data + written, params.size_bytes - written);
            if (n < 0) {
                if (errno == EINTR) continue;
                close(fd);
                unlink(fs_path);  // Cleanup partial write
                return make_status(StatusDomain::Storage, StatusCode::Io, errno);
            }
            written += n;
        }

        // Sync to disk
        if (fsync(fd) != 0) {
            close(fd);
            unlink(fs_path);
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }

        close(fd);
    }

    // Insert metadata into database
    db::DbObjectPutMetadataParams db_params;
    db_params.key = key;
    db_params.size_bytes = params.size_bytes;
    db_params.fs_path = fs_path;

    s = db::db_object_put_metadata(g_state.db, db_params);
    if (!is_ok(s)) {
        // Cleanup filesystem blob on database error
        unlink(fs_path);
        return s;
    }

    // Return result
    result->key = key;
    result->meta.size_bytes = params.size_bytes;
    result->meta.refcount = 1;
    result->meta.created_at = 0;  // TODO: Get from database
    result->meta.updated_at = 0;
    result->deduplicated = false;

    return ok_status();
}

Status object_get(const ObjectKey& key, u8* buffer, u64 buffer_size, ObjectGetResult* result) noexcept {
    if (!result || !buffer) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate key
    if (!key.zone.is_valid() || zone_is_universal(key.zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Get metadata from database
    db::DbObjectMetadata db_meta;
    Status s = db::db_object_get_metadata(g_state.db, key, &db_meta);
    if (!is_ok(s)) {
        return s;
    }

    // Validate buffer size
    if (buffer_size < db_meta.size_bytes) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid, db_meta.size_bytes);
    }

    // Open file from filesystem
    int fd = open(db_meta.fs_path, O_RDONLY);
    if (fd < 0) {
        return make_status(StatusDomain::Storage, StatusCode::NotFound);
    }

    // Read blob content
    u64 bytes_read = 0;
    while (bytes_read < db_meta.size_bytes) {
        ssize_t n = read(fd, buffer + bytes_read, db_meta.size_bytes - bytes_read);
        if (n < 0) {
            if (errno == EINTR) continue;
            close(fd);
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }
        if (n == 0) break;  // EOF
        bytes_read += n;
    }

    close(fd);

    if (bytes_read != db_meta.size_bytes) {
        return make_status(StatusDomain::Storage, StatusCode::Corrupt);
    }

    // Optional: verify hash
    if (g_state.config.verify_on_read) {
        Hash256 computed_hash;
        s = hash_compute(BufferView{buffer, static_cast<u32>(db_meta.size_bytes)}, &computed_hash);
        if (!is_ok(s)) {
            return s;
        }

        // Compare hash
        bool match = true;
        for (size_t i = 0; i < computed_hash.b.size(); ++i) {
            if (computed_hash.b[i] != key.content.b[i]) {
                match = false;
                break;
            }
        }

        if (!match) {
            return make_status(StatusDomain::Storage, StatusCode::Corrupt);
        }
    }

    // Return result
    result->meta.size_bytes = db_meta.size_bytes;
    result->meta.refcount = db_meta.refcount;
    result->meta.created_at = db_meta.created_at;
    result->meta.updated_at = db_meta.updated_at;
    result->bytes_read = bytes_read;

    return ok_status();
}

Status object_exists(const ObjectKey& key, bool* exists) noexcept {
    if (!exists) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate key
    if (!key.zone.is_valid() || zone_is_universal(key.zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    return db::db_object_exists(g_state.db, key, exists);
}

Status object_delete(const ObjectKey& key) noexcept {
    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate key
    if (!key.zone.is_valid() || zone_is_universal(key.zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Decrement refcount
    u32 new_refcount = 0;
    Status s = db::db_object_decrement_refcount(g_state.db, key, &new_refcount);
    if (!is_ok(s)) {
        return s;
    }

    // If refcount reaches 0, object is marked for garbage collection
    // We don't delete the filesystem blob immediately - use object_gc() for that

    return ok_status();
}

Status object_query(ZoneId zone, const ObjectQueryFilter& filter,
                    ObjectKey* results, u32* count) noexcept {
    if (!results || !count) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate zone
    if (!zone.is_valid() || zone_is_universal(zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Convert to db filter format
    db::DbObjectQueryFilter db_filter;
    db_filter.min_size_bytes = filter.min_size_bytes;
    db_filter.max_size_bytes = filter.max_size_bytes;
    db_filter.created_after = filter.created_after;
    db_filter.created_before = filter.created_before;
    db_filter.limit = filter.limit;

    // Query database
    return db::db_object_list(g_state.db, zone, db_filter, results, count);
}

Status object_gc(ZoneId zone, u64* objects_deleted) noexcept {
    if (!objects_deleted) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate zone
    if (!zone.is_valid() || zone_is_universal(zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    *objects_deleted = 0;

    // Process in batches to avoid memory issues and improve performance
    const u32 batch_size = 100;
    db::DbObjectGcEntry entries[batch_size];

    while (true) {
        u32 count = batch_size;

        // Query next batch of objects with refcount=0
        Status s = db::db_object_gc_query(g_state.db, zone, entries, &count);
        if (!is_ok(s)) {
            return s;
        }

        // No more objects to collect
        if (count == 0) {
            break;
        }

        // Delete filesystem blobs and database entries in this batch
        for (u32 i = 0; i < count; ++i) {
            // Delete filesystem blob
            if (entries[i].fs_path[0] != '\0') {
                if (unlink(entries[i].fs_path) != 0) {
                    // Log error but continue (file might not exist)
                    // In production, we'd log this
                }
            }

            // Delete database entry
            s = db::db_object_delete(g_state.db, entries[i].key);
            if (is_ok(s)) {
                (*objects_deleted)++;
            }
        }

        // If we got less than batch_size, we're done
        if (count < batch_size) {
            break;
        }
    }

    return ok_status();
}

Status object_verify(const ObjectKey& key, bool* valid) noexcept {
    if (!valid) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Validate key
    if (!key.zone.is_valid() || zone_is_universal(key.zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    *valid = false;

    // Get metadata from database
    db::DbObjectMetadata db_meta;
    Status s = db::db_object_get_metadata(g_state.db, key, &db_meta);
    if (!is_ok(s)) {
        return s;
    }

    // Check if file exists
    int fd = open(db_meta.fs_path, O_RDONLY);
    if (fd < 0) {
        *valid = false;
        return ok_status();  // File missing, not valid
    }

    // Read file into buffer
    std::vector<u8> buffer(db_meta.size_bytes);
    u64 bytes_read = 0;
    while (bytes_read < db_meta.size_bytes) {
        ssize_t n = read(fd, buffer.data() + bytes_read, db_meta.size_bytes - bytes_read);
        if (n < 0) {
            if (errno == EINTR) continue;
            close(fd);
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }
        if (n == 0) break;  // EOF
        bytes_read += n;
    }
    close(fd);

    // Check if we read the expected amount
    if (bytes_read != db_meta.size_bytes) {
        *valid = false;
        return ok_status();
    }

    // Compute hash of file content
    Hash256 computed_hash;
    s = hash_compute(BufferView{buffer.data(), static_cast<u32>(db_meta.size_bytes)}, &computed_hash);
    if (!is_ok(s)) {
        return s;
    }

    // Compare hash with key
    bool match = true;
    for (size_t i = 0; i < 32; ++i) {
        if (computed_hash.b[i] != key.content.b[i]) {
            match = false;
            break;
        }
    }

    *valid = match;

    // Update verification timestamp if valid
    if (match) {
        // TODO: Get current timestamp
        u64 now = 0;
        db::db_object_update_verified_at(g_state.db, key, now);
    }

    return ok_status();
}

} // namespace sarc::storage
