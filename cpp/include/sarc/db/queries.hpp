#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/db/schema.hpp"

namespace sarc::db {
    using u32 = sarc::core::u32;

    struct QueryLimit {
        u32 offset{0};
        u32 limit{0};
    };

    struct ZoneList {
        const sarc::core::Zone* data{nullptr};
        u32 len{0};
    };

    struct FileList {
        const sarc::core::FileMeta* data{nullptr};
        u32 len{0};
    };

    sarc::core::Status query_zone_by_id(sarc::core::ZoneId id,
        sarc::core::Zone* out) noexcept;

    sarc::core::Status query_object_by_key(const sarc::core::ObjectKey& key,
        sarc::core::ObjectMeta* out) noexcept;

    sarc::core::Status query_file_by_id(sarc::core::FileId id,
        sarc::core::FileMeta* out) noexcept;

    sarc::core::Status list_zones_by_owner(sarc::core::UserId owner,
        QueryLimit limit,
        ZoneList* out) noexcept;

    sarc::core::Status list_files_by_zone(sarc::core::ZoneId zone,
        QueryLimit limit,
        FileList* out) noexcept;

    static_assert(std::is_trivially_copyable_v<QueryLimit>);
    static_assert(std::is_trivially_copyable_v<ZoneList>);
    static_assert(std::is_trivially_copyable_v<FileList>);
    static_assert(std::is_standard_layout_v<QueryLimit>);
    static_assert(std::is_standard_layout_v<ZoneList>);
    static_assert(std::is_standard_layout_v<FileList>);

} // namespace sarc::db
