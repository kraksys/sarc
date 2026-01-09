#pragma once

#include <array>
#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <compare>

namespace sarc::core{

    using u8 = std::uint8_t;
    using u16 = std::uint16_t;
    using u32 = std::uint32_t;
    using u64 = std::uint64_t;

    using i64 = std::int64_t;

    using Timestamp = i64;

    struct Hash256 {
        std::array<u8, 32> b{};
        friend constexpr bool operator==(Hash256, Hash256) noexcept = default;
        friend constexpr auto operator<=>(Hash256, Hash256) noexcept = default;
    };
    static_assert(sizeof(Hash256) == 32);


    template <typename Tag, typename Repr>
    struct Id {
        Repr v{};

        static constexpr Id invalid() noexcept { return Id{Repr(~Repr{0})}; }
        [[nodiscard]] constexpr bool is_valid() const noexcept { return v != invalid().v; }

        friend constexpr bool operator==(Id, Id) noexcept = default;
        friend constexpr bool operator<=>(Id, Id) noexcept = default;
    };

    struct UserIdTag {};
    using UserId = Id<UserIdTag, u32>;

    struct FileIdTag {};
    using FileId = Id<FileIdTag, u64>;

    struct ZoneIdTag {};
    using ZoneId = Id<ZoneIdTag, u32>;

    // Zone selection is optional by construction;
    // ZoneId{0} is the universal zone (full context, no separation)
    inline constexpr ZoneId kZoneUniversal{0};

    enum class ZoneType : u8 {
        Universal = 0,
        User = 1,
        Category = 2,
    };

    struct CategoryKey {
        u64 v{};
        friend constexpr bool operator==(CategoryKey, CategoryKey) noexcept = default;
        friend constexpr auto operator<=>(CategoryKey, CategoryKey) noexcept = default;
    };

    struct ZoneSpec {
        ZoneType type{ZoneType::Universal};
        UserId owner{UserId::invalid()};
        CategoryKey category{};
    };

    constexpr ZoneSpec zone_universal() noexcept {
        return ZoneSpec{ZoneType::Universal, UserId::invalid(), CategoryKey{0}};
    }

    constexpr ZoneSpec zone_user(UserId owner) noexcept {
        return ZoneSpec{ZoneType::User, owner, CategoryKey{0}};
    }

    constexpr ZoneSpec zone_category(UserId owner, CategoryKey key) noexcept {
        return ZoneSpec{ZoneType::Category, owner, key};
    }

    [[nodiscard]] constexpr bool zone_is_universal(ZoneId z) noexcept {
        return z.v == kZoneUniversal.v;
    }

    struct StringIdTag {};
    using StringId = Id<StringIdTag, u32>;

    struct SourceIdTag {};
    using SourceId = Id<SourceIdTag, u32>;

    struct F32Slice {
        u32 off{};
        u32 len{};
    };

    enum class FieldType : u8 {
        Text = 0,
        I64 = 1,
        F64 = 2,
        Bool = 3,
        F32Slice = 4,
    };

    union FieldData {
        StringId text;
        i64 i64v;
        double f64v;
        u8 boolv;
        F32Slice f32slice;
    };

    struct Trace {
        FieldData data{};                     // 8 bytes - HOT (first cache line)
        FieldType type{FieldType::Text};      // 1 byte - HOT
        u8 _pad[3];                           // 3 bytes - explicit padding
        float confidence{1.0f};               // 4 bytes - HOT
        // --- 16 byte boundary (hot data fits in single cache line) ---
        SourceId source{SourceId::invalid()}; // 4 bytes - COLD
        u8 _pad2[4];                          // 4 bytes - padding for alignment
        Timestamp updated_at{0};              // 8 bytes - COLD
    };

    struct Gestalt {
        StringId type{StringId::invalid()};       // 4 bytes - HOT
        StringId value{StringId::invalid()};      // 4 bytes - HOT
        float weight{1.0f};                       // 4 bytes - HOT
        float confidence{1.0f};                   // 4 bytes - HOT
        // --- 16 byte boundary (all hot data) ---
        SourceId source{SourceId::invalid()};     // 4 bytes - COLD
        u8 _pad[4];                               // 4 bytes - padding for alignment
        Timestamp created_at{0};                  // 8 bytes - COLD
    };

    static_assert(sizeof(Trace) == 32);
    static_assert(sizeof(Gestalt) == 32);
    static_assert(std::is_trivially_copyable_v<Trace>);
    static_assert(std::is_trivially_copyable_v<Gestalt>);
    static_assert(std::is_standard_layout_v<Trace>);
    static_assert(std::is_standard_layout_v<Gestalt>);

} // namespace sarc::core
