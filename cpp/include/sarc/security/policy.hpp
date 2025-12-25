#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/security/crypto.hpp"

namespace sarc::security {
    using u32 = sarc::core::u32;

    enum class Right : u32 {
        None = 0,
        Read = 1u << 0,
        Write = 1u << 1,
        List = 1u << 2,
        Share = 1u << 3,
        Admin = 1u << 4,
    };

    [[nodiscard]] constexpr u32 right_mask(Right r) noexcept {
        return static_cast<u32>(r);
    }

    [[nodiscard]] constexpr bool has_right(u32 mask, Right r) noexcept {
        return (mask & right_mask(r)) != 0;
    }

    struct Capability {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::UserId grantee{ sarc::core::UserId::invalid() };
        u32 rights{0};
        sarc::core::Timestamp issued_at{0};
        sarc::core::Timestamp expires_at{0};
        u32 version{1};
        Tag16 proof{}; // MAC over the fields above
    };

    [[nodiscard]] constexpr bool capability_valid_for_zone(const Capability& cap, sarc::core::ZoneId zone) noexcept {
        return cap.zone.v == zone.v && !sarc::core::zone_is_universal(zone);
    }

    sarc::core::Status capability_seal(const Key256& key,
        const Capability& cap,
        Tag16* out_tag) noexcept;

    sarc::core::Status capability_verify(const Key256& key,
        const Capability& cap) noexcept;

    static_assert(std::is_trivially_copyable_v<Capability>);
    static_assert(std::is_standard_layout_v<Capability>);

} // namespace sarc::security
