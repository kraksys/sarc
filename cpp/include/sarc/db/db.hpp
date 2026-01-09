#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/db/schema.hpp"

namespace sarc::db {
    using u32 = sarc::core::u32;

    struct DbConfig {
        const char* path{nullptr};  // Database file path (nullptr = in-memory)
        u32 max_connections{0};
        u32 max_cache_bytes{0};
    };

    struct DbHandle {
        u32 id{0};
    };

    struct DbTxn {
        u32 id{0};
    };

    [[nodiscard]] constexpr bool db_handle_valid(DbHandle db) noexcept {
        return db.id != 0;
    }
    [[nodiscard]] constexpr bool db_txn_valid(DbTxn txn) noexcept {
        return txn.id != 0;
    }


    sarc::core::Status db_open(const DbConfig& cfg, DbHandle* out) noexcept;
    sarc::core::Status db_close(DbHandle db) noexcept;

    sarc::core::Status db_txn_begin(DbHandle db, DbTxn* out) noexcept;
    sarc::core::Status db_txn_commit(DbTxn txn) noexcept;
    sarc::core::Status db_txn_rollback(DbTxn txn) noexcept;

    // =========================================================================
    // Zone Operations
    // =========================================================================

    sarc::core::Status db_zone_create(DbHandle db, const sarc::core::Zone& zone) noexcept;
    sarc::core::Status db_zone_get(DbHandle db, sarc::core::ZoneId id, sarc::core::Zone* out) noexcept;
    sarc::core::Status db_zone_update(DbHandle db, const sarc::core::Zone& zone) noexcept;
    sarc::core::Status db_zone_delete(DbHandle db, sarc::core::ZoneId id) noexcept;
    sarc::core::Status db_zone_exists(DbHandle db, sarc::core::ZoneId id, bool* out) noexcept;

    // =========================================================================
    // Object Operations
    // =========================================================================

    using u64 = sarc::core::u64;
    using u8 = sarc::core::u8;

    // Hybrid store parameters (fs_path instead of blob data)
    struct DbObjectPutMetadataParams {
        sarc::core::ObjectKey key;
        u64 size_bytes;
        const char* fs_path;
    };

    // Metadata-only result (no blob data)
    struct DbObjectMetadata {
        u64 size_bytes;
        u32 refcount;
        u64 created_at;
        u64 updated_at;
        u64 verified_at;
        char fs_path[1024];
    };

    // Store metadata with filesystem path
    sarc::core::Status db_object_put_metadata(DbHandle db, const DbObjectPutMetadataParams& params) noexcept;

    // Get metadata without blob data
    sarc::core::Status db_object_get_metadata(DbHandle db, const sarc::core::ObjectKey& key,
                                               DbObjectMetadata* metadata) noexcept;

    sarc::core::Status db_object_exists(DbHandle db, const sarc::core::ObjectKey& key, bool* out) noexcept;
    sarc::core::Status db_object_delete(DbHandle db, const sarc::core::ObjectKey& key) noexcept;

    // New: Refcount management
    sarc::core::Status db_object_increment_refcount(DbHandle db, const sarc::core::ObjectKey& key) noexcept;
    sarc::core::Status db_object_decrement_refcount(DbHandle db, const sarc::core::ObjectKey& key, u32* new_refcount) noexcept;

    // New: Update verification timestamp
    sarc::core::Status db_object_update_verified_at(DbHandle db, const sarc::core::ObjectKey& key, u64 verified_at) noexcept;

    // New: Query objects for garbage collection
    struct DbObjectGcEntry {
        sarc::core::ObjectKey key;
        char fs_path[1024];
    };
    sarc::core::Status db_object_gc_query(DbHandle db, sarc::core::ZoneId zone,
                                          DbObjectGcEntry* results, u32* count) noexcept;

    // New: Query objects by zone with optional filters
    struct DbObjectQueryFilter {
        u64 min_size_bytes;
        u64 max_size_bytes;
        u64 created_after;
        u64 created_before;
        u32 limit;
    };
    sarc::core::Status db_object_list(DbHandle db, sarc::core::ZoneId zone,
                                      const DbObjectQueryFilter& filter,
                                      sarc::core::ObjectKey* results, u32* count) noexcept;

    // =========================================================================
    // File Operations
    // =========================================================================

    sarc::core::Status db_file_create(DbHandle db, const sarc::core::FileMeta& file) noexcept;
    sarc::core::Status db_file_get(DbHandle db, sarc::core::FileId id, sarc::core::FileMeta* out) noexcept;
    sarc::core::Status db_file_delete(DbHandle db, sarc::core::FileId id) noexcept;

    // =========================================================================
    // String Interning
    // =========================================================================

    sarc::core::Status db_string_intern(DbHandle db, const char* value, sarc::core::StringId* out) noexcept;
    sarc::core::Status db_string_get(DbHandle db, sarc::core::StringId id, char* out_buffer,
                                     u32 buffer_size, u32* out_len) noexcept;

    static_assert(std::is_trivially_copyable_v<DbConfig>);
    static_assert(std::is_trivially_copyable_v<DbHandle>);
    static_assert(std::is_trivially_copyable_v<DbTxn>);
    static_assert(std::is_standard_layout_v<DbConfig>);
    static_assert(std::is_standard_layout_v<DbHandle>);
    static_assert(std::is_standard_layout_v<DbTxn>);

} // namespace sarc::db
