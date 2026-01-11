#include "sarc/db/db.hpp"
#include <sqlite3.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <mutex>
#include <ctime>
#include <algorithm>

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
            filename TEXT,
            mime_type TEXT,
            origin_path TEXT,
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

    [[nodiscard]] bool table_has_column(sqlite3* db, const char* table, const char* column) noexcept {
        if (!db || !table || !column) {
            return false;
        }
        std::string sql = "PRAGMA table_info(";
        sql += table;
        sql += ")";

        sqlite3_stmt* stmt = nullptr;
        const int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK || stmt == nullptr) {
            return false;
        }

        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            if (name && std::strcmp(name, column) == 0) {
                found = true;
                break;
            }
        }

        sqlite3_finalize(stmt);
        return found;
    }

    void ensure_objects_origin_path_column(sqlite3* db) noexcept {
        if (!db) {
            return;
        }
        if (table_has_column(db, "objects", "origin_path")) {
            return;
        }
        char* err_msg = nullptr;
        sqlite3_exec(db, "ALTER TABLE objects ADD COLUMN origin_path TEXT DEFAULT ''", nullptr, nullptr, &err_msg);
        if (err_msg) {
            sqlite3_free(err_msg);
        }
    }

    [[nodiscard]] bool exec_sql(sqlite3* db, const char* sql) noexcept {
        if (!db || !sql) return false;
        char* err_msg = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err_msg);
        if (err_msg) sqlite3_free(err_msg);
        return rc == SQLITE_OK;
    }

    void ensure_objects_fts(sqlite3* db) noexcept {
        if (!db) return;

        auto has_table = [&](const char* name) -> bool {
            sqlite3_stmt* stmt = nullptr;
            const int rc = sqlite3_prepare_v2(
                db,
                "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1",
                -1,
                &stmt,
                nullptr);
            if (rc != SQLITE_OK || !stmt) {
                if (stmt) sqlite3_finalize(stmt);
                return false;
            }
            sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
            const int step = sqlite3_step(stmt);
            sqlite3_finalize(stmt);
            return step == SQLITE_ROW;
        };

        auto drop_triggers = [&]() {
            (void)exec_sql(db, "DROP TRIGGER IF EXISTS objects_ai;");
            (void)exec_sql(db, "DROP TRIGGER IF EXISTS objects_ad;");
            (void)exec_sql(db, "DROP TRIGGER IF EXISTS objects_au;");
        };

        // Prefer trigram tokenizer (better substring matching) if available, otherwise unicode61.
        if (!exec_sql(db, "CREATE VIRTUAL TABLE IF NOT EXISTS objects_fts "
                          "USING fts5(zone_id UNINDEXED, filename, mime_type, origin_path, tokenize='trigram')")) {
            (void)exec_sql(db, "CREATE VIRTUAL TABLE IF NOT EXISTS objects_fts "
                               "USING fts5(zone_id UNINDEXED, filename, mime_type, origin_path, tokenize='unicode61')");
        }

        // If FTS5 is not available (or virtual table creation failed), ensure we don't
        // leave triggers around that break normal INSERTs into objects.
        if (!has_table("objects_fts")) {
            drop_triggers();
            return;
        }

        // Triggers keep FTS in sync with objects.
        (void)exec_sql(db,
            "CREATE TRIGGER IF NOT EXISTS objects_ai AFTER INSERT ON objects BEGIN "
            "  INSERT INTO objects_fts(rowid, zone_id, filename, mime_type, origin_path) "
            "  VALUES (new.rowid, new.zone_id, new.filename, new.mime_type, new.origin_path); "
            "END;");
        (void)exec_sql(db,
            "CREATE TRIGGER IF NOT EXISTS objects_ad AFTER DELETE ON objects BEGIN "
            "  DELETE FROM objects_fts WHERE rowid = old.rowid; "
            "END;");
        (void)exec_sql(db,
            "CREATE TRIGGER IF NOT EXISTS objects_au AFTER UPDATE ON objects BEGIN "
            "  DELETE FROM objects_fts WHERE rowid = old.rowid; "
            "  INSERT INTO objects_fts(rowid, zone_id, filename, mime_type, origin_path) "
            "  VALUES (new.rowid, new.zone_id, new.filename, new.mime_type, new.origin_path); "
            "END;");

        // Backfill for older DBs that predate FTS.
        (void)exec_sql(db,
            "INSERT INTO objects_fts(rowid, zone_id, filename, mime_type, origin_path) "
            "SELECT rowid, zone_id, COALESCE(filename,''), COALESCE(mime_type,''), COALESCE(origin_path,'') "
            "FROM objects "
            "WHERE rowid NOT IN (SELECT rowid FROM objects_fts);");
    }
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

    // Enable WAL mode for better concurrency and performance (configurable).
    char* err_msg = nullptr;
    const char* journal_mode = std::getenv("SARC_DB_JOURNAL_MODE");
    if (!journal_mode || journal_mode[0] == '\0') {
        journal_mode = "WAL";
    }
    std::string journal_sql = "PRAGMA journal_mode=";
    journal_sql += journal_mode;
    rc = sqlite3_exec(g_db_state.db, journal_sql.c_str(), nullptr, nullptr, &err_msg);
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

    // Lightweight migrations for older DBs created before columns existed.
    ensure_objects_origin_path_column(g_db_state.db);
    ensure_objects_fts(g_db_state.db);

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

    const char* sql = "INSERT INTO objects (zone_id, content_hash, size_bytes, refcount, fs_path, verified_at, created_at, updated_at, filename, mime_type, origin_path) "
                      "VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?) "
                      "ON CONFLICT(zone_id, content_hash) DO UPDATE SET refcount = refcount + 1";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    Timestamp now = static_cast<Timestamp>(std::time(nullptr));

    sqlite3_bind_int(stmt, 1, params.key.zone.v);
    sqlite3_bind_blob(stmt, 2, params.key.content.b.data(), 32, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, params.size_bytes);
    sqlite3_bind_text(stmt, 4, params.fs_path, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, now); // verified_at
    sqlite3_bind_int64(stmt, 6, now); // created_at
    sqlite3_bind_int64(stmt, 7, now); // updated_at
    sqlite3_bind_text(stmt, 8, params.filename ? params.filename : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 9, params.mime_type ? params.mime_type : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 10, params.origin_path ? params.origin_path : "", -1, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    // Ensure readers in other processes see the latest writes promptly.
    // This is cheap in WAL mode and a no-op otherwise.
    (void)sqlite3_wal_checkpoint_v2(g_db_state.db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);

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

    const char* sql = "SELECT size_bytes, refcount, fs_path, verified_at, created_at, updated_at, filename, mime_type, origin_path "
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

        const char* filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 6));
        if (filename) {
            std::strncpy(metadata->filename, filename, sizeof(metadata->filename) - 1);
            metadata->filename[sizeof(metadata->filename) - 1] = '\0';
        } else {
            metadata->filename[0] = '\0';
        }

        const char* mime_type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 7));
        if (mime_type) {
            std::strncpy(metadata->mime_type, mime_type, sizeof(metadata->mime_type) - 1);
            metadata->mime_type[sizeof(metadata->mime_type) - 1] = '\0';
        } else {
            metadata->mime_type[0] = '\0';
        }

        const char* origin_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 8));
        if (origin_path) {
            std::strncpy(metadata->origin_path, origin_path, sizeof(metadata->origin_path) - 1);
            metadata->origin_path[sizeof(metadata->origin_path) - 1] = '\0';
        } else {
            metadata->origin_path[0] = '\0';
        }

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
                      DbObjectQueryResult* results, u32* count) noexcept {
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
    std::string sql = "SELECT zone_id, content_hash, size_bytes, refcount, created_at, updated_at, filename, mime_type FROM objects WHERE zone_id = ?";

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

    sql += " ORDER BY updated_at DESC, created_at DESC";

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
            results[found].key.zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};

            const void* hash_blob = sqlite3_column_blob(stmt, 1);
            if (hash_blob) {
                std::memcpy(results[found].key.content.b.data(), hash_blob, 32);
            }

            results[found].size_bytes = sqlite3_column_int64(stmt, 2);
            results[found].refcount = sqlite3_column_int(stmt, 3);
            results[found].created_at = sqlite3_column_int64(stmt, 4);
            results[found].updated_at = sqlite3_column_int64(stmt, 5);

            const char* filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 6));
            if (filename) {
                std::strncpy(results[found].filename, filename, sizeof(results[found].filename) - 1);
                results[found].filename[sizeof(results[found].filename) - 1] = '\0';
            } else {
                results[found].filename[0] = '\0';
            }

            const char* mime_type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 7));
            if (mime_type) {
                std::strncpy(results[found].mime_type, mime_type, sizeof(results[found].mime_type) - 1);
                results[found].mime_type[sizeof(results[found].mime_type) - 1] = '\0';
            } else {
                results[found].mime_type[0] = '\0';
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

Status db_object_search(DbHandle db,
                        ZoneId zone,
                        const char* query,
                        u32 limit,
                        DbObjectSearchResult* results,
                        u32* count) noexcept {
    if (!db_handle_valid(db) || !results || !count || !query) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }
    if (*count == 0 || limit == 0) {
        *count = 0;
        return ok_status();
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);
    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const u32 max_out = std::min<u32>(*count, limit);

    auto copy_text = [](char* dst, size_t dst_size, const unsigned char* txt) {
        if (!dst || dst_size == 0) return;
        if (!txt) {
            dst[0] = '\0';
            return;
        }
        std::strncpy(dst, reinterpret_cast<const char*>(txt), dst_size - 1);
        dst[dst_size - 1] = '\0';
    };

    u32 found = 0;

    // Try FTS5 first.
    const char* fts_sql =
        "SELECT o.zone_id, o.content_hash, o.size_bytes, o.refcount, o.created_at, o.updated_at, "
        "       o.filename, o.mime_type, o.origin_path "
        "FROM objects o "
        "JOIN objects_fts f ON f.rowid = o.rowid "
        "WHERE o.zone_id = ? AND f MATCH ? "
        "ORDER BY bm25(objects_fts) "
        "LIMIT ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, fts_sql, -1, &stmt, nullptr);
    if (rc == SQLITE_OK && stmt) {
        sqlite3_bind_int(stmt, 1, zone.v);
        sqlite3_bind_text(stmt, 2, query, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 3, static_cast<int>(max_out));

        while (found < max_out) {
            rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                results[found].key.zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};

                const void* hash_blob = sqlite3_column_blob(stmt, 1);
                if (hash_blob) {
                    std::memcpy(results[found].key.content.b.data(), hash_blob, 32);
                } else {
                    std::memset(results[found].key.content.b.data(), 0, 32);
                }

                results[found].size_bytes = sqlite3_column_int64(stmt, 2);
                results[found].refcount = sqlite3_column_int(stmt, 3);
                results[found].created_at = sqlite3_column_int64(stmt, 4);
                results[found].updated_at = sqlite3_column_int64(stmt, 5);

                copy_text(results[found].filename, sizeof(results[found].filename), sqlite3_column_text(stmt, 6));
                copy_text(results[found].mime_type, sizeof(results[found].mime_type), sqlite3_column_text(stmt, 7));
                copy_text(results[found].origin_path, sizeof(results[found].origin_path), sqlite3_column_text(stmt, 8));

                ++found;
                continue;
            }
            if (rc == SQLITE_DONE) {
                break;
            }
            sqlite3_finalize(stmt);
            return make_status(StatusDomain::Db, StatusCode::Unknown);
        }

        sqlite3_finalize(stmt);
        *count = found;
        return ok_status();
    }
    if (stmt) {
        sqlite3_finalize(stmt);
        stmt = nullptr;
    }

    // Fallback: LIKE scan (still useful if SQLite wasn't built with FTS5).
    const char* like_sql =
        "SELECT zone_id, content_hash, size_bytes, refcount, created_at, updated_at, filename, mime_type, origin_path "
        "FROM objects "
        "WHERE zone_id = ? AND (filename LIKE ? OR origin_path LIKE ? OR mime_type LIKE ?) "
        "ORDER BY updated_at DESC, created_at DESC "
        "LIMIT ?";

    rc = sqlite3_prepare_v2(g_db_state.db, like_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK || !stmt) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    std::string like = "%";
    like += query;
    like += "%";

    sqlite3_bind_int(stmt, 1, zone.v);
    sqlite3_bind_text(stmt, 2, like.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, like.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, like.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 5, static_cast<int>(max_out));

    while (found < max_out) {
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            results[found].key.zone = ZoneId{static_cast<u32>(sqlite3_column_int(stmt, 0))};

            const void* hash_blob = sqlite3_column_blob(stmt, 1);
            if (hash_blob) {
                std::memcpy(results[found].key.content.b.data(), hash_blob, 32);
            } else {
                std::memset(results[found].key.content.b.data(), 0, 32);
            }

            results[found].size_bytes = sqlite3_column_int64(stmt, 2);
            results[found].refcount = sqlite3_column_int(stmt, 3);
            results[found].created_at = sqlite3_column_int64(stmt, 4);
            results[found].updated_at = sqlite3_column_int64(stmt, 5);

            copy_text(results[found].filename, sizeof(results[found].filename), sqlite3_column_text(stmt, 6));
            copy_text(results[found].mime_type, sizeof(results[found].mime_type), sqlite3_column_text(stmt, 7));
            copy_text(results[found].origin_path, sizeof(results[found].origin_path), sqlite3_column_text(stmt, 8));

            ++found;
            continue;
        }
        if (rc == SQLITE_DONE) {
            break;
        }
        sqlite3_finalize(stmt);
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_finalize(stmt);
    *count = found;
    return ok_status();
}

Status db_object_count(DbHandle db, sarc::core::ZoneId zone, u64* out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);
    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* sql = "SELECT COUNT(*) FROM objects WHERE zone_id = ?";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(g_db_state.db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK || !stmt) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }

    sqlite3_bind_int(stmt, 1, zone.v);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        *out = static_cast<u64>(sqlite3_column_int64(stmt, 0));
        sqlite3_finalize(stmt);
        return ok_status();
    }

    sqlite3_finalize(stmt);
    return make_status(StatusDomain::Db, StatusCode::Unknown);
}

Status db_db_filename(DbHandle db, const char** out) noexcept {
    if (!db_handle_valid(db) || !out) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    std::lock_guard<std::mutex> lock(g_db_state.mutex);
    if (!g_db_state.db) {
        return make_status(StatusDomain::Db, StatusCode::Invalid);
    }

    const char* path = sqlite3_db_filename(g_db_state.db, "main");
    if (!path) {
        return make_status(StatusDomain::Db, StatusCode::Unknown);
    }
    *out = path;
    return ok_status();
}

} // namespace sarc::db
