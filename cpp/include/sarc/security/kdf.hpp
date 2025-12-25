#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/security/crypto.hpp"

namespace sarc::security {
    using u8 = sarc::core::u8;
    using u32 = sarc::core::u32;

    enum class KdfDomain : u8 {
        ZoneRoot = 1,
        ObjectKey = 2,
        ObjectNonce = 3,
        Capability = 4,
    };

    struct KdfLabel {
        u8 b[16]{};
    };

    struct KdfInfo {
        KdfDomain domain{KdfDomain::ZoneRoot};
        u32 version{1};
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::ObjectKey object{};
    };

    // HKDF helpers (domain-separated, no allocations)
    sarc::core::Status hkdf_expand(const Key256& ikm,
        BufferView salt,
        BufferView info,
        Key256* out_key) noexcept;

    sarc::core::Status hkdf_derive_object_key(const Key256& zone_root_key,
        const KdfInfo& info,
        Key256* out_key) noexcept;

    sarc::core::Status derive_object_nonce(const Key256& zone_root_key,
        const KdfInfo& info,
        Nonce12* out_nonce) noexcept;

    static_assert(std::is_trivially_copyable_v<KdfInfo>);
    static_assert(std::is_standard_layout_v<KdfInfo>);
    static_assert(std::is_trivially_copyable_v<KdfLabel>);
    static_assert(std::is_standard_layout_v<KdfLabel>);

} // namespace sarc::security
