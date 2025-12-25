#pragma once

#include <type_traits>

#include "sarc/core/models.hpp"
#include "sarc/core/types.hpp"

namespace sarc::db {
    using u32 = sarc::core::u32;

    enum class TableId : u32 {
        Zones = 1,
        Objects = 2,
        Files = 3,
        Grants = 4,
        Peers = 5,
    };

    enum class IndexId : u32 {
        ZoneByOwner = 1,
        ObjectByHash = 2,
        FileByZone = 3,
        GrantByZone = 4,
    };

    struct ZoneRecord {
        sarc::core::Zone zone{};
    };

    struct ObjectRecord {
        sarc::core::ObjectKey key{};
        sarc::core::ObjectMeta meta{};
    };

    struct FileRecord {
        sarc::core::FileMeta meta{};
    };

    static_assert(std::is_trivially_copyable_v<ZoneRecord>);
    static_assert(std::is_trivially_copyable_v<ObjectRecord>);
    static_assert(std::is_trivially_copyable_v<FileRecord>);
    static_assert(std::is_standard_layout_v<ZoneRecord>);
    static_assert(std::is_standard_layout_v<ObjectRecord>);
    static_assert(std::is_standard_layout_v<FileRecord>);

} // namespace sarc::db
