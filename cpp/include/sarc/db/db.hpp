#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/db/schema.hpp"

namespace sarc::db {
    using u32 = sarc::core::u32;

    struct DbConfig {
        u32 max_connections{0};
        u32 max_cache_bytes{0};
    };

    struct DbHandle {
        u32 id{0};
    };

    struct DbTxn {
        u32 id{0};
    };

    sarc::core::Status db_open(const DbConfig& cfg, DbHandle* out) noexcept;
    sarc::core::Status db_close(DbHandle db) noexcept;

    sarc::core::Status db_txn_begin(DbHandle db, DbTxn* out) noexcept;
    sarc::core::Status db_txn_commit(DbTxn txn) noexcept;
    sarc::core::Status db_txn_rollback(DbTxn txn) noexcept;

    static_assert(std::is_trivially_copyable_v<DbConfig>);
    static_assert(std::is_trivially_copyable_v<DbHandle>);
    static_assert(std::is_trivially_copyable_v<DbTxn>);
    static_assert(std::is_standard_layout_v<DbConfig>);
    static_assert(std::is_standard_layout_v<DbHandle>);
    static_assert(std::is_standard_layout_v<DbTxn>);

} // namespace sarc::db
