#pragma once

#include "sarc/core/types.hpp"
#include "sarc/core/models.hpp"
#include "sarc/core/errors.hpp"
#include "sarc/storage/buffer.hpp"

namespace sarc::storage {

using u8 = sarc::core::u8;
using u32 = sarc::core::u32;
using u64 = sarc::core::u64;

// Compression policy for automatic compression
enum class CompressionPolicy : u8 {
    None = 0,              // Never compress
    Auto = 1,              // Compress based on MIME type (recommended)
    AlwaysLZ4 = 2,         // Always use LZ4 compression
    AlwaysZstd = 3,        // Always use zstd level 1
};

// Object store configuration
struct ObjectStoreConfig {
    const char* data_root;          // Root directory for objects (e.g., "/data/objects")
    const char* db_path;            // SQLite database path (e.g., "/data/db/metadata.db")
    u32 max_object_bytes;           // Maximum object size (0 = unlimited)
    CompressionPolicy compression;  // Compression policy
    bool verify_on_read;            // Verify content hash on every read (slow)
};

// Parameters for putting an object
struct ObjectPutParams {
    sarc::core::ZoneId zone;        // Zone for multi-tenant isolation
    const u8* data;                 // Object data
    u64 size_bytes;                 // Size of data
    const char* filename;           // Optional: original filename (for metadata)
    const char* mime_type;          // Optional: MIME type (for compression/search)
    const char* origin_path;        // Optional: original filesystem path (for open-original in clients)
};

// Result from putting an object
struct ObjectPutResult {
    sarc::core::ObjectKey key;      // Content-addressable key (zone + hash)
    sarc::core::ObjectMeta meta;    // Metadata (size, refcount, timestamps)
    bool deduplicated;              // true if object already existed
    char filename[256];
    char mime_type[128];
};

// Result from getting an object
struct ObjectGetResult {
    sarc::core::ObjectMeta meta;    // Metadata
    u64 bytes_read;                 // Bytes actually read into buffer
    char filename[256];             // Original filename
    char mime_type[128];            // MIME type
    char fs_path[1024];             // Filesystem path
    char origin_path[1024];         // Original filesystem path (may be empty)
};

// Filter for querying objects
struct ObjectQueryFilter {
    const char* filename_pattern;   // SQL LIKE pattern (e.g., "%.jpg")
    const char* mime_type_pattern;  // SQL LIKE pattern (e.g., "image/%")
    u64 min_size_bytes;             // Minimum file size (0 = no minimum)
    u64 max_size_bytes;             // Maximum file size (0 = no maximum)
    u64 created_after;              // Unix timestamp (0 = no limit)
    u64 created_before;             // Unix timestamp (0 = no limit)
    u32 limit;                      // Maximum results to return
};

// ========================================================================
// Object Store Lifecycle
// ========================================================================

// Initialize the object store
// Must be called before any other object_* functions
[[nodiscard]] sarc::core::Status object_store_init(const ObjectStoreConfig& cfg) noexcept;

// Shutdown the object store
// Flushes any pending writes and closes database connection
[[nodiscard]] sarc::core::Status object_store_shutdown() noexcept;

// ========================================================================
// Object CRUD Operations
// ========================================================================

// Put an object into the store
// - Computes content hash (BLAKE3)
// - Deduplicates if object already exists (increments refcount)
// - Writes blob to filesystem at: {data_root}/{zone}/{hash[0:2]}/{hash[2:4]}/{hash}.dat[.lz4]
// - Inserts metadata into database
[[nodiscard]] sarc::core::Status object_put(const ObjectPutParams& params,
                                             ObjectPutResult* result) noexcept;

// Get an object from the store
// - Queries database for filesystem path
// - Reads blob from filesystem
// - Optionally verifies content hash (if config.verify_on_read)
// - Returns data in provided buffer
[[nodiscard]] sarc::core::Status object_get(const sarc::core::ObjectKey& key,
                                             u8* buffer,
                                             u64 buffer_size,
                                             ObjectGetResult* result) noexcept;

// Get only metadata for an object (including filesystem path)
[[nodiscard]] sarc::core::Status object_get_metadata(const sarc::core::ObjectKey& key,
                                                      ObjectGetResult* result) noexcept;

// Check if an object exists
// - Queries database only (no filesystem access)
[[nodiscard]] sarc::core::Status object_exists(const sarc::core::ObjectKey& key,
                                                bool* exists) noexcept;

// Delete an object
// - Decrements refcount
// - If refcount reaches 0, marks for garbage collection (doesn't delete immediately)
// - Use object_gc() to actually remove orphaned blobs
[[nodiscard]] sarc::core::Status object_delete(const sarc::core::ObjectKey& key) noexcept;

// Result item for object query
struct ObjectQueryResult {
    sarc::core::ObjectKey key;
    sarc::core::ObjectMeta meta;
    char filename[256];
    char mime_type[128];
};

// ========================================================================
// Query Operations
// ========================================================================

// Query objects in a zone by filter
// - Returns array of ObjectQueryResults matching the filter
// - count: input = max results, output = actual results
[[nodiscard]] sarc::core::Status object_query(sarc::core::ZoneId zone,
                                               const ObjectQueryFilter& filter,
                                               ObjectQueryResult* results,
                                               u32* count) noexcept;

// Count objects in a zone.
[[nodiscard]] sarc::core::Status object_count(sarc::core::ZoneId zone, u64* out) noexcept;

// Get active DB filename (for debugging).
[[nodiscard]] sarc::core::Status object_db_filename(const char** out) noexcept;

// ========================================================================
// Search Operations
// ========================================================================

struct ObjectSearchResult {
    sarc::core::ObjectKey key;
    sarc::core::ObjectMeta meta;
    char filename[256];
    char mime_type[128];
    char origin_path[1024];
};

// Search objects in a zone (FTS5 if available; may fall back to LIKE)
// - query: user query string
// - count: input=max results, output=actual
[[nodiscard]] sarc::core::Status object_search(sarc::core::ZoneId zone,
                                               const char* query,
                                               ObjectSearchResult* results,
                                               u32* count) noexcept;

// ========================================================================
// Maintenance Operations
// ========================================================================

// Garbage collect objects with refcount=0
// - Deletes filesystem blobs for objects with refcount=0
// - Removes database metadata
// - Returns count of objects deleted
[[nodiscard]] sarc::core::Status object_gc(sarc::core::ZoneId zone,
                                            u64* objects_deleted) noexcept;

// Verify object integrity
// - Reads blob from filesystem
// - Computes content hash
// - Verifies hash matches ObjectKey
// - Sets valid=false if corrupted
[[nodiscard]] sarc::core::Status object_verify(const sarc::core::ObjectKey& key,
                                                bool* valid) noexcept;

// ========================================================================
// Streaming Uploads
// ========================================================================

class ObjectWriter {
public:
    ObjectWriter() noexcept;
    ~ObjectWriter() noexcept;

    // Start a new upload stream
    // Creates a temporary file
    [[nodiscard]] sarc::core::Status open() noexcept;

    // Write a chunk of data
    [[nodiscard]] sarc::core::Status write(const u8* data, u64 size) noexcept;

    // Finish upload and commit to store
    [[nodiscard]] sarc::core::Status commit(const ObjectPutParams& params,
                                             ObjectPutResult* result) noexcept;

    // Abort upload (cleanup temp file)
    void abort() noexcept;

private:
    char temp_path_[1024];
    int fd_{-1};
    u64 total_size_{0};
    // We use a void* to avoid including hashing.hpp in this public header if possible, 
    // but we already included it in hashing.cpp. 
    // To keep header clean, we can use PIMPL or just include hashing.hpp.
    // Given xmake setup, including hashing.hpp is safe.
    // But for now, let's use opaque storage aligned to blake3_hasher size (approx 2KB) or a pointer.
    // Simpler: Just include "sarc/storage/hashing.hpp" at top.
    
    // Wait, I can't modify the top of the file easily with 'replace' context. 
    // I'll use a pointer to implementation or just forward declare.
    // Let's use a pointer to Hasher.
    void* hasher_{nullptr}; 
};

} // namespace sarc::storage
