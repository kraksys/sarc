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
        u64 size_bytes{0};       // 8 bytes - HOT
        u32 refcount{0};         // 4 bytes - HOT
        u32 _pad;                // 4 bytes - explicit padding
        // --- 16 byte boundary ---
        Timestamp created_at{0}; // 8 bytes - COLD
        Timestamp updated_at{0}; // 8 bytes - COLD
    };

    struct FileMeta {
        FileId id{FileId::invalid()};        // 8 bytes - HOT
        Hash256 content{};                   // 32 bytes - HOT
        ZoneId zone{ZoneId::invalid()};      // 4 bytes - HOT
        u32 _pad1;                           // 4 bytes - explicit padding
        u64 size_bytes{0};                   // 8 bytes - HOT
        // --- 56 bytes of hot data ---
        StringId name{StringId::invalid()};  // 4 bytes - WARM
        StringId mime{StringId::invalid()};  // 4 bytes - WARM
        Timestamp created_at{0};             // 8 bytes - COLD
        Timestamp updated_at{0};             // 8 bytes - COLD
    };

    static_assert(sizeof(ObjectMeta) == 32);
    static_assert(sizeof(FileMeta) == 80);
    static_assert(std::is_trivially_copyable_v<Zone>);
    static_assert(std::is_trivially_copyable_v<ObjectKey>);
    static_assert(std::is_trivially_copyable_v<ObjectMeta>);
    static_assert(std::is_trivially_copyable_v<FileMeta>);
    static_assert(std::is_standard_layout_v<Zone>);
    static_assert(std::is_standard_layout_v<ObjectKey>);
    static_assert(std::is_standard_layout_v<ObjectMeta>);
    static_assert(std::is_standard_layout_v<FileMeta>);
} // namespace sarc::core
