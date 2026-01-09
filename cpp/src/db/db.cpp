#include "sarc/db/db.hpp"
#include <sqlite3.h>
#include <cstring>
#include <string>
#include <mutex>

namespace sarc::db {

using namespace sarc::core;

// Global database state
namespace {
    struct DbState {
        sqlite3* db = nullptr;
        std::mutex mutex;
    };

    DbState g_db_state;

    // SQL schema embedded (from schema.sql)
    constexpr const char* kSchemaSQL = R"SQL(
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS zones (
            id INTEGER PRIMARY KEY,
            type INTEGER NOT NULL,
            owner_id INTEGER NOT NULL,
            category_key INTEGER NOT NULL,
            name_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_zones_owner ON zones(owner_id);
        CREATE INDEX IF NOT EXISTS idx_zones_type ON zones(type);

        CREATE TABLE IF NOT EXISTS objects (
            zone_id INTEGER NOT NULL,
            content_hash BLOB NOT NULL,
            size_bytes INTEGER NOT NULL,
            refcount INTEGER NOT NULL DEFAULT 1,
            fs_path TEXT NOT NULL,
            verified_at INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (zone_id, content_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_objects_zone ON objects(zone_id);
        CREATE INDEX IF NOT EXISTS idx_objects_hash ON objects(content_hash);
        CREATE INDEX IF NOT EXISTS idx_objects_path ON objects(fs_path);

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zone_id INTEGER NOT NULL,
            content_hash BLOB NOT NULL,
            name_id INTEGER NOT NULL,
            mime_id INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY (zone_id, content_hash) REFERENCES objects(zone_id, content_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_files_zone ON files(zone_id);
        CREATE INDEX IF NOT EXISTS idx_files_content ON files(content_hash);

        CREATE TABLE IF NOT EXISTS strings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_strings_value ON strings(value);

        CREATE TABLE IF NOT EXISTS grants (
            zone_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            rights INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (zone_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_grants_zone ON grants(zone_id);
        CREATE INDEX IF NOT EXISTS idx_grants_user ON grants(user_id);
    )SQL";
}

// ============================================================================
// Database Lifecycle
// ============================================================================

Status db_open(const DbConfig& cfg, DbHandle* out) noexcept {
    if (!out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    // Close existing connection if any
    if (g_db_state.db) {
        sqlite3_close(g_db_state.db);
        g_db_state.db = nullptr;
    }

    // Open database
    const char* path = cfg.path ? cfg.path : ":memory:";
    int rc = sqlite3_open(path, &g_db_state.db);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    // Enable WAL mode for better concurrency and performance
    char* err_msg = nullptr;
    rc = sqlite3_exec(g_db_state.db, "PRAGMA journal_mode=WAL", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        sqlite3_free(err_msg);
        // Continue even if WAL fails (might be in-memory DB)
    }

    // Performance optimizations
    sqlite3_exec(g_db_state.db, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);  // Faster than FULL, still safe with WAL
    sqlite3_exec(g_db_state.db, "PRAGMA cache_size=-64000", nullptr, nullptr, nullptr);   // 64MB cache
    sqlite3_exec(g_db_state.db, "PRAGMA temp_store=MEMORY", nullptr, nullptr, nullptr);   // In-memory temp tables
    sqlite3_exec(g_db_state.db, "PRAGMA mmap_size=268435456", nullptr, nullptr, nullptr); // 256MB mmap

    // Execute schema
    rc = sqlite3_exec(g_db_state.db, kSchemaSQL, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        sqlite3_free(err_msg);
        sqlite3_close(g_db_state.db);
        g_db_state.db = nullptr;
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    out->id = 1;
    return ok_status();
}

Status db_close(DbHandle db) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (g_db_state.db) {
        sqlite3_close(g_db_state.db);
        g_db_state.db = nullptr;
    }

    return ok_status();
}

// ============================================================================
// Transaction Management
// ============================================================================

Status db_txn_begin(DbHandle db, DbTxn* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    char* err_msg = nullptr;
    int rc = sqlite3_exec(g_db_state.db, "BEGIN TRANSACTION", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        sqlite3_free(err_msg);
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    out->id = 1;
    return ok_status();
}

Status db_txn_commit(DbTxn txn) noexcept {
    if (!db_txn_valid(txn)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    char* err_msg = nullptr;
    int rc = sqlite3_exec(g_db_state.db, "COMMIT", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        sqlite3_free(err_msg);
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    return ok_status();
}

Status db_txn_rollback(DbTxn txn) noexcept {
    if (!db_txn_valid(txn)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    char* err_msg = nullptr;
    int rc = sqlite3_exec(g_db_state.db, "ROLLBACK", nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        sqlite3_free(err_msg);
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    return ok_status();
}

// ============================================================================
// Zone Operations
// ============================================================================

Status db_zone_create(DbHandle db, const Zone& zone) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "INSERT INTO zones (id, type, owner_id, category_key, name_id, created_at, updated_at) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?)";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, zone.id.v);
    sqlite3_bind_int(stmt, 2, static_cast<int>(zone.type));
    sqlite3_bind_int(stmt, 3, zone.owner.v);
    sqlite3_bind_int64(stmt, 4, zone.category.v);
    sqlite3_bind_int(stmt, 5, zone.name.v);
    sqlite3_bind_int64(stmt, 6, zone.created_at);
    sqlite3_bind_int64(stmt, 7, zone.updated_at);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Conflict);
    }

    return ok_status();
}

Status db_zone_get(DbHandle db, ZoneId id, Zone* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT id, type, owner_id, category_key, name_id, created_at, updated_at "
                      "FROM zones WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        out->id = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};
        out->type = static_cast<ZoneType>(sqlite3_column_int(stmt, 1));
        out->owner = UserId{static_cast<u32>(sqlite3_column_int(stmt, 2))};
        out->category = CategoryKey{static_cast<u64>(sqlite3_column_int64(stmt, 3))};
        out->name = StringId{static_cast<u32>(sqlite3_column_int(stmt, 4))};
        out->created_at = sqlite3_column_int64(stmt, 5);
        out->updated_at = sqlite3_column_int64(stmt, 6);

        sqlite3_finalize(stmt);
        return ok_status();
    }

    sqlite3_finalize(stmt);
    return make_status(StatusDomain::Db, StatusCode::NotFound);
}

Status db_zone_update(DbHandle db, const Zone& zone) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "UPDATE zones SET type = ?, owner_id = ?, category_key = ?, "
                      "name_id = ?, updated_at = ? WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, static_cast<int>(zone.type));
    sqlite3_bind_int(stmt, 2, zone.owner.v);
    sqlite3_bind_int64(stmt, 3, zone.category.v);
    sqlite3_bind_int(stmt, 4, zone.name.v);
    sqlite3_bind_int64(stmt, 5, zone.updated_at);
    sqlite3_bind_int(stmt, 6, zone.id.v);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    if (sqlite3_changes(g_db_state.db) == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

Status db_zone_delete(DbHandle db, ZoneId id) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "DELETE FROM zones WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    if (sqlite3_changes(g_db_state.db) == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

Status db_zone_exists(DbHandle db, ZoneId id, bool* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT 1 FROM zones WHERE id = ? LIMIT 1";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    *out = (rc == SQLITE_ROW);

    sqlite3_finalize(stmt);
    return ok_status();
}

// ============================================================================
// Object Operations
// ============================================================================

Status db_object_exists(DbHandle db, const ObjectKey& key, bool* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT 1 FROM objects WHERE zone_id = ? AND content_hash = ? LIMIT 1";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    *out = (rc == SQLITE_ROW);

    sqlite3_finalize(stmt);
    return ok_status();
}

Status db_object_delete(DbHandle db, const ObjectKey& key) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "DELETE FROM objects WHERE zone_id = ? AND content_hash = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    if (sqlite3_changes(g_db_state.db) == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

// ============================================================================
// File Operations
// ============================================================================

Status db_file_create(DbHandle db, const FileMeta& file) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // Don't include id in INSERT - let AUTOINCREMENT handle it
    const char* sql = "INSERT INTO files (zone_id, content_hash, name_id, mime_id, size_bytes, created_at, updated_at) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?)";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, file.zone.v);
    sqlite3_bind_blob(stmt, 2, file.content.b.data(), 32, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, file.name.v);
    sqlite3_bind_int(stmt, 4, file.mime.v);
    sqlite3_bind_int64(stmt, 5, file.size_bytes);
    sqlite3_bind_int64(stmt, 6, file.created_at);
    sqlite3_bind_int64(stmt, 7, file.updated_at);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Conflict);
    }

    return ok_status();
}

Status db_file_get(DbHandle db, FileId id, FileMeta* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT id, zone_id, content_hash, name_id, mime_id, size_bytes, created_at, updated_at "
                      "FROM files WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int64(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        out->id = FileId{static_cast<u64>(sqlite3_column_int64(stmt, 0))};
        out->zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 1))};

        const void* hash_blob = sqlite3_column_blob(stmt, 2);
        if (hash_blob) {
            std::memcpy(out->content.b.data(), hash_blob, 32);
        }

        out->name = StringId{static_cast<u32>(sqlite3_column_int(stmt, 3))};
        out->mime = StringId{static_cast<u32>(sqlite3_column_int(stmt, 4))};
        out->size_bytes = sqlite3_column_int64(stmt, 5);
        out->created_at = sqlite3_column_int64(stmt, 6);
        out->updated_at = sqlite3_column_int64(stmt, 7);

        sqlite3_finalize(stmt);
        return ok_status();
    }

    sqlite3_finalize(stmt);
    return make_status(StatusDomain::Db, StatusCode::NotFound);
}

Status db_file_delete(DbHandle db, FileId id) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "DELETE FROM files WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int64(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    if (sqlite3_changes(g_db_state.db) == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

// ============================================================================
// String Interning
// ============================================================================

Status db_string_intern(DbHandle db, const char* value, StringId* out) noexcept {
    if (!db_handle_valid(db) || !value || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // First try to find existing string
    const char* select_sql = "SELECT id FROM strings WHERE value = ?";
    sqlite3_stmt* select_stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, select_sql, -1, &select_stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_text(select_stmt, 1, value, -1, SQLITE_STATIC);
    rc = sqlite3_step(select_stmt);

    if (rc == SQLITE_ROW) {
        // String already exists
        *out = StringId{static_cast<u32>(sqlite3_column_int(select_stmt, 0))};
        sqlite3_finalize(select_stmt);
        return ok_status();
    }
    sqlite3_finalize(select_stmt);

    // Insert new string
    const char* insert_sql = "INSERT INTO strings (value) VALUES (?)";
    sqlite3_stmt* insert_stmt = nullptr;
    rc = sqlite3_prepare_v2(g_db_state.db, insert_sql, -1, &insert_stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_text(insert_stmt, 1, value, -1, SQLITE_STATIC);
    rc = sqlite3_step(insert_stmt);
    sqlite3_finalize(insert_stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    // Get the auto-generated ID
    i64 last_id = sqlite3_last_insert_rowid(g_db_state.db);
    *out = StringId{static_cast<u32>(last_id)};

    return ok_status();
}

Status db_string_get(DbHandle db, StringId id, char* out_buffer, u32 buffer_size, u32* out_len) noexcept {
    if (!db_handle_valid(db) || !out_buffer || !out_len) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT value FROM strings WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, id.v);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        int text_len = sqlite3_column_bytes(stmt, 0);

        if (text && text_len >= 0) {
            if (static_cast<u32>(text_len + 1) > buffer_size) {
                sqlite3_finalize(stmt);
                *out_len = text_len;
                return make_status(StatusDomain::Db, StatusCode::Invalid, text_len + 1);
            }

            std::memcpy(out_buffer, text, text_len);
            out_buffer[text_len] = '\0';
            *out_len = text_len;

            sqlite3_finalize(stmt);
            return ok_status();
        }
    }

    sqlite3_finalize(stmt);
    return make_status(StatusDomain::Db, StatusCode::NotFound);
}

// ===========================================================================
// New: Hybrid Store Database Functions
// ===========================================================================

Status db_object_put_metadata(DbHandle db, const DbObjectPutMetadataParams& params) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    if (!params.fs_path) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "INSERT INTO objects (zone_id, content_hash, size_bytes, refcount, fs_path, verified_at, created_at, updated_at) "
                      "VALUES (?, ?, ?, 1, ?, ?, ?, ?) "
                      "ON CONFLICT(zone_id, content_hash) DO UPDATE SET refcount = refcount + 1";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    Timestamp now = 0; // TODO: get actual timestamp

    sqlite3_bind_int(stmt, 1, params.key.zone.v);
    sqlite3_bind_blob(stmt, 2, params.key.content.b.data(), 32, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, params.size_bytes);
    sqlite3_bind_text(stmt, 4, params.fs_path, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, now); // verified_at
    sqlite3_bind_int64(stmt, 6, now); // created_at
    sqlite3_bind_int64(stmt, 7, now); // updated_at

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    return ok_status();
}

Status db_object_get_metadata(DbHandle db, const ObjectKey& key, DbObjectMetadata* metadata) noexcept {
    if (!db_handle_valid(db) || !metadata) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT size_bytes, refcount, fs_path, verified_at, created_at, updated_at "
                      "FROM objects WHERE zone_id = ? AND content_hash = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        metadata->size_bytes = sqlite3_column_int64(stmt, 0);
        metadata->refcount = sqlite3_column_int(stmt, 1);

        const char* fs_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        if (fs_path) {
            std::strncpy(metadata->fs_path, fs_path, sizeof(metadata->fs_path) - 1);
            metadata->fs_path[sizeof(metadata->fs_path) - 1] = '\0';
        } else {
            metadata->fs_path[0] = '\0';
        }

        metadata->verified_at = sqlite3_column_int64(stmt, 3);
        metadata->created_at = sqlite3_column_int64(stmt, 4);
        metadata->updated_at = sqlite3_column_int64(stmt, 5);

        sqlite3_finalize(stmt);
        return ok_status();
    }

    sqlite3_finalize(stmt);
    return make_status(StatusDomain::Db, StatusCode::NotFound);
}

Status db_object_increment_refcount(DbHandle db, const ObjectKey& key) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "UPDATE objects SET refcount = refcount + 1 "
                      "WHERE zone_id = ? AND content_hash = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    int changes = sqlite3_changes(g_db_state.db);
    if (changes == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

Status db_object_decrement_refcount(DbHandle db, const ObjectKey& key, u32* new_refcount) noexcept {
    if (!db_handle_valid(db) || !new_refcount) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // First get current refcount
    const char* select_sql = "SELECT refcount FROM objects WHERE zone_id = ? AND content_hash = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, select_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);

    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    u32 current_refcount = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);

    if (current_refcount == 0) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // Decrement
    const char* update_sql = "UPDATE objects SET refcount = refcount - 1 "
                             "WHERE zone_id = ? AND content_hash = ?";

    rc = sqlite3_prepare_v2(g_db_state.db, update_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, key.zone.v);
    sqlite3_bind_blob(stmt, 2, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    *new_refcount = current_refcount - 1;
    return ok_status();
}

Status db_object_update_verified_at(DbHandle db, const ObjectKey& key, u64 verified_at) noexcept {
    if (!db_handle_valid(db)) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "UPDATE objects SET verified_at = ? "
                      "WHERE zone_id = ? AND content_hash = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int64(stmt, 1, verified_at);
    sqlite3_bind_int(stmt, 2, key.zone.v);
    sqlite3_bind_blob(stmt, 3, key.content.b.data(), 32, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    int changes = sqlite3_changes(g_db_state.db);
    if (changes == 0) {
        return make_status(StatusDomain::Db, StatusCode::NotFound);
    }

    return ok_status();
}

Status db_object_gc_query(DbHandle db, ZoneId zone, DbObjectGcEntry* results, u32* count) noexcept {
    if (!db_handle_valid(db) || !results || !count) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    if (*count == 0) {
        return ok_status();
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // Query for objects with refcount=0 in the given zone
    const char* sql = "SELECT zone_id, content_hash, fs_path FROM objects "
                      "WHERE zone_id = ? AND refcount = 0 LIMIT ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, zone.v);
    sqlite3_bind_int(stmt, 2, *count);

    u32 found = 0;
    while (found < *count) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_ROW) {
            // Extract zone_id
            results[found].key.zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};

            // Extract content_hash
            const void* hash_blob = sqlite3_column_blob(stmt, 1);
            if (hash_blob) {
                std::memcpy(results[found].key.content.b.data(), hash_blob, 32);
            }

            // Extract fs_path
            const char* fs_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
            if (fs_path) {
                std::strncpy(results[found].fs_path, fs_path, sizeof(results[found].fs_path) - 1);
                results[found].fs_path[sizeof(results[found].fs_path) - 1] = '\0';
            } else {
                results[found].fs_path[0] = '\0';
            }

            found++;
        } else if (rc == SQLITE_DONE) {
            break;  // No more results
        } else {
            sqlite3_finalize(stmt);
            return make_status(StatusDomain::Db, StatusCode::Unknown);
        }
    }

    sqlite3_finalize(stmt);

    *count = found;
    return ok_status();
}

Status db_object_list(DbHandle db, ZoneId zone, const DbObjectQueryFilter& filter,
                      ObjectKey* results, u32* count) noexcept {
    if (!db_handle_valid(db) || !results || !count) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    if (*count == 0) {
        return ok_status();
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);

    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    // Build SQL query with filters
    std::string sql = "SELECT zone_id, content_hash FROM objects WHERE zone_id = ?";

    bool has_min_size = filter.min_size_bytes > 0;
    bool has_max_size = filter.max_size_bytes > 0;
    bool has_created_after = filter.created_after > 0;
    bool has_created_before = filter.created_before > 0;

    if (has_min_size) {
        sql += " AND size_bytes >= ?";
    }
    if (has_max_size) {
        sql += " AND size_bytes <= ?";
    }
    if (has_created_after) {
        sql += " AND created_at >= ?";
    }
    if (has_created_before) {
        sql += " AND created_at <= ?";
    }

    u32 limit = filter.limit > 0 ? filter.limit : *count;
    sql += " LIMIT ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    // Bind parameters
    int param_idx = 1;
    sqlite3_bind_int(stmt, param_idx++, zone.v);

    if (has_min_size) {
        sqlite3_bind_int64(stmt, param_idx++, filter.min_size_bytes);
    }
    if (has_max_size) {
        sqlite3_bind_int64(stmt, param_idx++, filter.max_size_bytes);
    }
    if (has_created_after) {
        sqlite3_bind_int64(stmt, param_idx++, filter.created_after);
    }
    if (has_created_before) {
        sqlite3_bind_int64(stmt, param_idx++, filter.created_before);
    }
    sqlite3_bind_int(stmt, param_idx++, limit);

    // Fetch results
    u32 found = 0;
    while (found < *count) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_ROW) {
            results[found].zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};

            const void* hash_blob = sqlite3_column_blob(stmt, 1);
            if (hash_blob) {
                std::memcpy(results[found].content.b.data(), hash_blob, 32);
            }

            found++;
        } else if (rc == SQLITE_DONE) {
            break;
        } else {
            sqlite3_finalize(stmt);
            return make_status(StatusDomain::Db, StatusCode::Unknown);
        }
    }

    sqlite3_finalize(stmt);
    *count = found;

    return ok_status();
}

} // namespace sarc::db
