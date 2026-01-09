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
};

// Result from putting an object
struct ObjectPutResult {
    sarc::core::ObjectKey key;      // Content-addressable key (zone + hash)
    sarc::core::ObjectMeta meta;    // Metadata (size, refcount, timestamps)
    bool deduplicated;              // true if object already existed
};

// Result from getting an object
struct ObjectGetResult {
    sarc::core::ObjectMeta meta;    // Metadata
    u64 bytes_read;                 // Bytes actually read into buffer
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

// Check if an object exists
// - Queries database only (no filesystem access)
[[nodiscard]] sarc::core::Status object_exists(const sarc::core::ObjectKey& key,
                                                bool* exists) noexcept;

// Delete an object
// - Decrements refcount
// - If refcount reaches 0, marks for garbage collection (doesn't delete immediately)
// - Use object_gc() to actually remove orphaned blobs
[[nodiscard]] sarc::core::Status object_delete(const sarc::core::ObjectKey& key) noexcept;

// ========================================================================
// Query Operations
// ========================================================================

// Query objects in a zone by filter
// - Returns array of ObjectKeys matching the filter
// - count: input = max results, output = actual results
[[nodiscard]] sarc::core::Status object_query(sarc::core::ZoneId zone,
                                               const ObjectQueryFilter& filter,
                                               sarc::core::ObjectKey* results,
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

} // namespace sarc::storage
