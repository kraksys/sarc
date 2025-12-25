#include "sarc/security/policy.hpp"

#include <cstddef>

#include <blake3.h>

namespace sarc::security {
    namespace {
        void put_u32_be(u8 out[4], u32 v) noexcept {
            out[0] = static_cast<u8>((v >> 24) & 0xffu);
            out[1] = static_cast<u8>((v >> 16) & 0xffu);
            out[2] = static_cast<u8>((v >> 8) & 0xffu);
            out[3] = static_cast<u8>((v >> 0) & 0xffu);
        }

        void put_u64_be(u8 out[8], sarc::core::u64 v) noexcept {
            out[0] = static_cast<u8>((v >> 56) & 0xffu);
            out[1] = static_cast<u8>((v >> 48) & 0xffu);
            out[2] = static_cast<u8>((v >> 40) & 0xffu);
            out[3] = static_cast<u8>((v >> 32) & 0xffu);
            out[4] = static_cast<u8>((v >> 24) & 0xffu);
            out[5] = static_cast<u8>((v >> 16) & 0xffu);
            out[6] = static_cast<u8>((v >> 8) & 0xffu);
            out[7] = static_cast<u8>((v >> 0) & 0xffu);
        }

        [[nodiscard]] bool tag16_equal_ct(const Tag16& a, const Tag16& b) noexcept {
            u8 acc = 0;
            for (size_t i = 0; i < sizeof(a.b); ++i) {
                acc = static_cast<u8>(acc | static_cast<u8>(a.b[i] ^ b.b[i]));
            }
            return acc == 0;
        }

        void capability_mac(const Key256& key, const Capability& cap, Tag16* out) noexcept {
            blake3_hasher h;
            blake3_hasher_init_keyed(&h, key.b);

            static constexpr char kLabel[] = "sarc.capability.mac.v1";
            blake3_hasher_update(&h, kLabel, sizeof(kLabel) - 1);

            u8 buf4[4];
            u8 buf8[8];

            put_u32_be(buf4, static_cast<u32>(cap.zone.v));
            blake3_hasher_update(&h, buf4, sizeof(buf4));

            put_u32_be(buf4, static_cast<u32>(cap.grantee.v));
            blake3_hasher_update(&h, buf4, sizeof(buf4));

            put_u32_be(buf4, cap.rights);
            blake3_hasher_update(&h, buf4, sizeof(buf4));

            put_u64_be(buf8, static_cast<sarc::core::u64>(static_cast<sarc::core::i64>(cap.issued_at)));
            blake3_hasher_update(&h, buf8, sizeof(buf8));

            put_u64_be(buf8, static_cast<sarc::core::u64>(static_cast<sarc::core::i64>(cap.expires_at)));
            blake3_hasher_update(&h, buf8, sizeof(buf8));

            put_u32_be(buf4, cap.version);
            blake3_hasher_update(&h, buf4, sizeof(buf4));

            blake3_hasher_finalize(&h, out->b, sizeof(out->b));
        }
    } // namespace

    sarc::core::Status capability_seal(const Key256& key, const Capability& cap, Tag16* out_tag) noexcept {
        if (out_tag == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!cap.zone.is_valid() || sarc::core::zone_is_universal(cap.zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!cap.grantee.is_valid()) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        Tag16 tag{};
        capability_mac(key, cap, &tag);
        *out_tag = tag;
        return sarc::core::ok_status();
    }

    sarc::core::Status capability_verify(const Key256& key, const Capability& cap) noexcept {
        if (!cap.zone.is_valid() || sarc::core::zone_is_universal(cap.zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!cap.grantee.is_valid()) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        Tag16 expected{};
        capability_mac(key, cap, &expected);
        if (!tag16_equal_ct(expected, cap.proof)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        return sarc::core::ok_status();
    }
} // namespace sarc::security
