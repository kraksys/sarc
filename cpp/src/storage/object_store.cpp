#include "sarc/storage/object_store.hpp"
#include "sarc/storage/hashing.hpp"
#include "sarc/db/db.hpp"
#include "sarc/core/zone.hpp"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <ctime>
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

static void copy_cstr(char* dst, size_t dst_size, const char* src) noexcept {
    if (!dst || dst_size == 0) {
        return;
    }
    if (!src) {
        dst[0] = '\0';
        return;
    }
    std::strncpy(dst, src, dst_size - 1);
    dst[dst_size - 1] = '\0';
}

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
        copy_cstr(result->filename, sizeof(result->filename), db_meta.filename);
        copy_cstr(result->mime_type, sizeof(result->mime_type), db_meta.mime_type);

        return ok_status();
    }

    // Determine if we should compress
    // bool compress = should_compress(g_state.config.compression, params.mime_type, params.size_bytes);
    bool compress = false; // Compression not yet supported

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
    db::DbObjectPutMetadataParams db_params{};
    db_params.key = key;
    db_params.size_bytes = params.size_bytes;
    db_params.fs_path = fs_path;
    db_params.filename = params.filename;
    db_params.mime_type = params.mime_type;
    db_params.origin_path = params.origin_path;

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
    result->meta.created_at = static_cast<u64>(std::time(nullptr));
    result->meta.updated_at = result->meta.created_at;
    result->deduplicated = false;
    copy_cstr(result->filename, sizeof(result->filename), params.filename);
    copy_cstr(result->mime_type, sizeof(result->mime_type), params.mime_type);

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

    if (db_meta.refcount == 0) {
        return make_status(StatusDomain::Storage, StatusCode::NotFound);
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

    std::strncpy(result->filename, db_meta.filename, sizeof(result->filename) - 1);
    result->filename[sizeof(result->filename) - 1] = '\0';

    std::strncpy(result->mime_type, db_meta.mime_type, sizeof(result->mime_type) - 1);
    result->mime_type[sizeof(result->mime_type) - 1] = '\0';

    std::strncpy(result->fs_path, db_meta.fs_path, sizeof(result->fs_path) - 1);
    result->fs_path[sizeof(result->fs_path) - 1] = '\0';

    std::strncpy(result->origin_path, db_meta.origin_path, sizeof(result->origin_path) - 1);
    result->origin_path[sizeof(result->origin_path) - 1] = '\0';

    return ok_status();
}

Status object_get_metadata(const ObjectKey& key, ObjectGetResult* result) noexcept {
    if (!result) {
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

    if (db_meta.refcount == 0) {
        return make_status(StatusDomain::Storage, StatusCode::NotFound);
    }

    // Return result
    result->meta.size_bytes = db_meta.size_bytes;
    result->meta.refcount = db_meta.refcount;
    result->meta.created_at = db_meta.created_at;
    result->meta.updated_at = db_meta.updated_at;
    result->bytes_read = 0; // No data read

    std::strncpy(result->filename, db_meta.filename, sizeof(result->filename) - 1);
    result->filename[sizeof(result->filename) - 1] = '\0';

    std::strncpy(result->mime_type, db_meta.mime_type, sizeof(result->mime_type) - 1);
    result->mime_type[sizeof(result->mime_type) - 1] = '\0';

    std::strncpy(result->fs_path, db_meta.fs_path, sizeof(result->fs_path) - 1);
    result->fs_path[sizeof(result->fs_path) - 1] = '\0';

    std::strncpy(result->origin_path, db_meta.origin_path, sizeof(result->origin_path) - 1);
    result->origin_path[sizeof(result->origin_path) - 1] = '\0';

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
                    ObjectQueryResult* results, u32* count) noexcept {
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

    // Use DbObjectQueryResult which has the same layout as ObjectQueryResult
    // (We cast to DbObjectQueryResult* since they are binary compatible)
    static_assert(sizeof(db::DbObjectQueryResult) == sizeof(ObjectQueryResult));
    
    return db::db_object_list(g_state.db, zone, db_filter, 
                             reinterpret_cast<db::DbObjectQueryResult*>(results), count);
}

Status object_count(ZoneId zone, u64* out) noexcept {
    if (!out) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (!zone.is_valid() || zone_is_universal(zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    return db::db_object_count(g_state.db, zone, out);
}

Status object_db_filename(const char** out) noexcept {
    if (!out) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    return db::db_db_filename(g_state.db, out);
}

Status object_search(ZoneId zone,
                     const char* query,
                     ObjectSearchResult* results,
                     u32* count) noexcept {
    if (!results || !count || !query) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (!zone.is_valid() || zone_is_universal(zone)) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }
    if (*count == 0) {
        return ok_status();
    }

    std::vector<db::DbObjectSearchResult> tmp;
    tmp.resize(*count);
    u32 found = *count;
    Status s = db::db_object_search(g_state.db, zone, query, *count, tmp.data(), &found);
    if (!is_ok(s)) {
        return s;
    }

    for (u32 i = 0; i < found; ++i) {
        results[i].key = tmp[i].key;
        results[i].meta.size_bytes = tmp[i].size_bytes;
        results[i].meta.refcount = tmp[i].refcount;
        results[i].meta.created_at = tmp[i].created_at;
        results[i].meta.updated_at = tmp[i].updated_at;
        copy_cstr(results[i].filename, sizeof(results[i].filename), tmp[i].filename);
        copy_cstr(results[i].mime_type, sizeof(results[i].mime_type), tmp[i].mime_type);
        copy_cstr(results[i].origin_path, sizeof(results[i].origin_path), tmp[i].origin_path);
    }

    *count = found;
    return ok_status();
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
        u64 now = static_cast<u64>(std::time(nullptr));
        db::db_object_update_verified_at(g_state.db, key, now);
    }

    return ok_status();
}

// ========================================================================
// ObjectWriter Implementation
// ========================================================================

ObjectWriter::ObjectWriter() noexcept {
    temp_path_[0] = '\0';
}

ObjectWriter::~ObjectWriter() noexcept {
    abort();
}

Status ObjectWriter::open() noexcept {
    if (fd_ >= 0) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (!g_state.initialized) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Ensure tmp directory exists
    char tmp_dir[1024];
    snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", g_state.config.data_root);
    
    // Generate temp path
    snprintf(temp_path_, sizeof(temp_path_), "%s/upload_XXXXXX", tmp_dir);
    
    // Ensure directory exists (create_directories creates parent of path)
    Status s = create_directories(temp_path_);
    if (!is_ok(s)) {
        return s;
    }
    
    fd_ = mkstemp(temp_path_);
    if (fd_ < 0) {
        return make_status(StatusDomain::Storage, StatusCode::Io, errno);
    }

    hasher_ = new (std::nothrow) Hasher();
    if (!hasher_) {
        close(fd_);
        unlink(temp_path_);
        fd_ = -1;
        return make_status(StatusDomain::Storage, StatusCode::OutOfMemory);
    }

    total_size_ = 0;
    return ok_status();
}

Status ObjectWriter::write(const u8* data, u64 size) noexcept {
    if (fd_ < 0 || !hasher_) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    if (size == 0) return ok_status();

    // Write to file
    ssize_t written = 0;
    while (written < static_cast<ssize_t>(size)) {
        ssize_t n = ::write(fd_, data + written, size - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return make_status(StatusDomain::Storage, StatusCode::Io, errno);
        }
        written += n;
    }

    // Update hash
    static_cast<Hasher*>(hasher_)->update(BufferView{data, static_cast<u32>(size)});
    total_size_ += size;

    return ok_status();
}

Status ObjectWriter::commit(const ObjectPutParams& params, ObjectPutResult* result) noexcept {
    if (fd_ < 0 || !hasher_) {
        return make_status(StatusDomain::Storage, StatusCode::Invalid);
    }

    // Finalize hash
    Hash256 content_hash;
    static_cast<Hasher*>(hasher_)->finalize(&content_hash);

    // Sync file
    if (fsync(fd_) != 0) {
        return make_status(StatusDomain::Storage, StatusCode::Io, errno);
    }
    close(fd_);
    fd_ = -1; // Closed

    ObjectKey key;
    key.zone = params.zone;
    key.content = content_hash;

    // Check if object exists (Deduplication)
    bool exists = false;
    Status s = db::db_object_exists(g_state.db, key, &exists);
    if (!is_ok(s)) {
        unlink(temp_path_);
        return s;
    }

    if (exists) {
        // Increment refcount
        s = db::db_object_increment_refcount(g_state.db, key);
        if (!is_ok(s)) {
            unlink(temp_path_);
            return s;
        }

        // Get metadata
        db::DbObjectMetadata db_meta;
        s = db::db_object_get_metadata(g_state.db, key, &db_meta);
        if (!is_ok(s)) {
            unlink(temp_path_);
            return s;
        }

        // Delete temp file (duplicate)
        unlink(temp_path_);

        result->key = key;
        result->meta.size_bytes = db_meta.size_bytes;
        result->meta.refcount = db_meta.refcount;
        result->meta.created_at = db_meta.created_at;
        result->meta.updated_at = db_meta.updated_at;
        result->deduplicated = true;
        
        // Populate filename/mime if provided (for return only, DB not updated on dup)
        // Or should we update DB with new filename? 
        // Current logic: deduplication means "same content". 
        // If we want to store multiple filenames for same content, we need the "files" table.
        // For now, we just return what's in DB.
        copy_cstr(result->filename, sizeof(result->filename), db_meta.filename);
        copy_cstr(result->mime_type, sizeof(result->mime_type), db_meta.mime_type);

        return ok_status();
    }

    // Move temp file to final location
    char final_path[1024];
    construct_fs_path(g_state.config.data_root, params.zone, content_hash, false, final_path, sizeof(final_path));

    s = create_directories(final_path);
    if (!is_ok(s)) {
        unlink(temp_path_);
        return s;
    }

    if (rename(temp_path_, final_path) != 0) {
        unlink(temp_path_);
        return make_status(StatusDomain::Storage, StatusCode::Io, errno);
    }

    // Ensure permissions are correct (mkstemp uses 0600)
    chmod(final_path, 0644);

    // Insert into DB
    db::DbObjectPutMetadataParams db_params{};
    db_params.key = key;
    db_params.size_bytes = total_size_;
    db_params.fs_path = final_path;
    db_params.filename = params.filename;
    db_params.mime_type = params.mime_type;
    db_params.origin_path = params.origin_path;

    s = db::db_object_put_metadata(g_state.db, db_params);
    if (!is_ok(s)) {
        unlink(final_path); // Rollback file
        return s;
    }

    result->key = key;
    result->meta.size_bytes = total_size_;
    result->meta.refcount = 1;
    result->meta.created_at = 0; // DB sets this
    result->meta.updated_at = 0;
    result->deduplicated = false;
    
    // We can just copy input filename/mime to result
    copy_cstr(result->filename, sizeof(result->filename), params.filename);
    copy_cstr(result->mime_type, sizeof(result->mime_type), params.mime_type);

    return ok_status();
}

void ObjectWriter::abort() noexcept {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    if (temp_path_[0] != '\0') {
        unlink(temp_path_);
        temp_path_[0] = '\0';
    }
    if (hasher_) {
        delete static_cast<Hasher*>(hasher_);
        hasher_ = nullptr;
    }
}

} // namespace sarc::storage
