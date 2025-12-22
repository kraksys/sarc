#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/security/crypto.hpp"

namespace sarc::security {

    struct ZoneRootKey {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        Key256 key{};
        sarc::core::Timetamp created_at{0};
        sarc::core::Timestamp rotated_at{0};
        u32 version{1};
    };

    struct ZoneGrant {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::UserId grantee{ sarc::core::UserId::invalid() };
        Key256 wrapped_key{};
        sarc::core::Timestamp created_at{0};
        sarc::core::Timestamp revoked_at{0};
        u32 version{1};
    };

    struct NodeMasterKey {
        Key256 key{};
        sarc::core::Timestamp created_at{0};
        u32 version{1};
    };

    struct KeyWrapNonce {
        Nonce12 nonce{};
    };

    sarc::core::Status wrap_zone_root_key(const NodeMasterKey& nmk,
        const KeyWrapNonce& nonce,
        const ZoneRootKey& zrk,
        Key256* wrapped_out) noexcept;

    sarc::core::Status unwrap_zone_root_key(const NodeMasterKey& nmk,
        const KeyWrapNonce& nonce,
        const Key256& wrapped,
        ZoneRootKey* zrk_out) noexcept;

    static_assert(std::is_trivially_copyable_v<ZoneRootKey>);
    static_assert(std::is_trivially_copyable_v<ZoneGrant>);
    static_assert(std::is_trivially_copyable_v<NodeMasterKey>);
    static_assert(std::is_trivially_copyable_v<KeyWrapNonce>);

} // namespace sarc::security
