#include "sarc/security/kdf.hpp"

#include <cstddef>

#include <blake3.h>

namespace sarc::security {
    namespace {
        [[nodiscard]] bool buffer_ok(BufferView b) noexcept {
            return (b.len == 0) || (b.data != nullptr);
        }

        void hasher_update_u8(blake3_hasher* h, u8 v) noexcept {
            blake3_hasher_update(h, &v, 1);
        }

        void hasher_update_u32_be(blake3_hasher* h, u32 v) noexcept {
            const u8 b[4] = {
                static_cast<u8>((v >> 24) & 0xffu),
                static_cast<u8>((v >> 16) & 0xffu),
                static_cast<u8>((v >> 8) & 0xffu),
                static_cast<u8>((v >> 0) & 0xffu),
            };
            blake3_hasher_update(h, b, sizeof(b));
        }

        void hasher_update_zone_id(blake3_hasher* h, sarc::core::ZoneId zone) noexcept {
            hasher_update_u32_be(h, static_cast<u32>(zone.v));
        }

        void hasher_update_hash256(blake3_hasher* h, const sarc::core::Hash256& x) noexcept {
            blake3_hasher_update(h, x.b.data(), x.b.size());
        }

        void hasher_update_lenprefixed(blake3_hasher* h, BufferView b) noexcept {
            hasher_update_u32_be(h, b.len);
            if (b.len > 0) {
                blake3_hasher_update(h, b.data, static_cast<size_t>(b.len));
            }
        }
    } // namespace

    sarc::core::Status hkdf_expand(const Key256& ikm,
        BufferView salt,
        BufferView info,
        Key256* out_key) noexcept {
        if (out_key == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!buffer_ok(salt) || !buffer_ok(info)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        blake3_hasher h;
        blake3_hasher_init_keyed(&h, ikm.b);

        static constexpr char kLabel[] = "sarc.kdf.expand.v1";
        blake3_hasher_update(&h, kLabel, sizeof(kLabel) - 1);

        hasher_update_lenprefixed(&h, salt);
        hasher_update_lenprefixed(&h, info);

        blake3_hasher_finalize(&h, out_key->b, sizeof(out_key->b));
        return sarc::core::ok_status();
    }

    sarc::core::Status hkdf_derive_object_key(const Key256& zone_root_key,
        const KdfInfo& info,
        Key256* out_key) noexcept {
        if (out_key == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!info.object.zone.is_valid() || sarc::core::zone_is_universal(info.object.zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (info.zone.is_valid() && info.zone.v != info.object.zone.v) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        blake3_hasher h;
        blake3_hasher_init_keyed(&h, zone_root_key.b);

        static constexpr char kLabel[] = "sarc.kdf.object_key.v1";
        blake3_hasher_update(&h, kLabel, sizeof(kLabel) - 1);

        hasher_update_u8(&h, static_cast<u8>(info.domain));
        hasher_update_u32_be(&h, info.version);
        hasher_update_zone_id(&h, info.object.zone);
        hasher_update_hash256(&h, info.object.content);

        blake3_hasher_finalize(&h, out_key->b, sizeof(out_key->b));
        return sarc::core::ok_status();
    }

    sarc::core::Status derive_object_nonce(const Key256& zone_root_key,
        const KdfInfo& info,
        Nonce12* out_nonce) noexcept {
        if (out_nonce == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!info.object.zone.is_valid() || sarc::core::zone_is_universal(info.object.zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (info.zone.is_valid() && info.zone.v != info.object.zone.v) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        blake3_hasher h;
        blake3_hasher_init_keyed(&h, zone_root_key.b);

        static constexpr char kLabel[] = "sarc.kdf.object_nonce.v1";
        blake3_hasher_update(&h, kLabel, sizeof(kLabel) - 1);

        hasher_update_u8(&h, static_cast<u8>(info.domain));
        hasher_update_u32_be(&h, info.version);
        hasher_update_zone_id(&h, info.object.zone);
        hasher_update_hash256(&h, info.object.content);

        blake3_hasher_finalize(&h, out_nonce->b, sizeof(out_nonce->b));
        return sarc::core::ok_status();
    }
} // namespace sarc::security
