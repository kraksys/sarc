#pragma once
#include <type_traits>
#include "sarc/core/types.hpp"

namespace sarc::core {
    struct Zone {
        ZoneId id{ZoneId::invalid()};
        ZoneType type{ZoneType::Universal};
        UserId owner{UserId::invalid()};
        CategoryKey category{};
        StringId name{StringId::invalid()};
        Timestamp created_at{0};
        Timestamp updated_at{0};
    };

    struct ObjectKey {
        ZoneId zone{ZoneId::invalid()};
        Hash256 content{};
        friend constexpr bool operator==(ObjectKey, ObjectKey) noexcept = default;
    };

    struct ObjectMeta {
        u64 size_bytes{0};
        u32 refcount{0};
        Timestamp created_at{0};
        Timestamp updated_at{0};
    };

    struct FileMeta {
        FileId id{FileId::invalid()};
        ZoneId zone{ZoneId::invalid()};
        Hash256 content{};
        StringId name{StringId::invalid()};
        StringId mime{StringId::invalid()};
        u64 size_bytes{0};
        Timestamp created_at{0};
        Timestamp updated_at{0};
    };

    static_assert(std::is_trivially_copyable_v<Zone>);
    static_assert(std::is_trivially_copyable_v<ObjectKey>);
    static_assert(std::is_trivially_copyable_v<ObjectMeta>);
    static_assert(std::is_trivially_copyable_v<FileMeta>);
    static_assert(std::is_standard_layout_v<Zone>);
    static_assert(std::is_standard_layout_v<ObjectKey>);
    static_assert(std::is_standard_layout_v<ObjectMeta>);
    static_assert(std::is_standard_layout_v<FileMeta>);
} // namespace sarc::core
