#include "sarc/security/keystore.hpp"

#include <array>
#include <cstddef>
#include <cstring>

#include "sarc/security/crypto.hpp"

namespace sarc::security {
    namespace {
        void put_u32_be(u8 out[4], u32 v) noexcept {
            out[0] = static_cast<u8>((v >> 24) & 0xffu);
            out[1] = static_cast<u8>((v >> 16) & 0xffu);
            out[2] = static_cast<u8>((v >> 8) & 0xffu);
            out[3] = static_cast<u8>((v >> 0) & 0xffu);
        }

        BufferView make_zrk_aad(sarc::core::ZoneId zone, u32 version, std::array<u8, 32>* scratch) noexcept {
            scratch->fill(0);
            size_t off = 0;

            static constexpr char kLabel[] = "sarc.keystore.zrk.v1";
            static_assert(sizeof(kLabel) - 1 <= 24);

            std::memcpy(scratch->data() + off, kLabel, sizeof(kLabel) - 1);
            off += (sizeof(kLabel) - 1);

            u8 b4[4];
            put_u32_be(b4, static_cast<u32>(zone.v));
            std::memcpy(scratch->data() + off, b4, sizeof(b4));
            off += sizeof(b4);

            put_u32_be(b4, version);
            std::memcpy(scratch->data() + off, b4, sizeof(b4));
            off += sizeof(b4);

            return BufferView{scratch->data(), static_cast<u32>(off)};
        }
    } // namespace

    sarc::core::Status wrap_zone_root_key(const NodeMasterKey& nmk,
        const KeyWrapNonce& nonce,
        const ZoneRootKey& zrk,
        WrappedKey* wrapped_out) noexcept {
        if (wrapped_out == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!zrk.zone.is_valid() || sarc::core::zone_is_universal(zrk.zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        std::array<u8, 32> aad_scratch{};
        const BufferView aad = make_zrk_aad(zrk.zone, zrk.version, &aad_scratch);

        WrappedKey w{};
        const BufferView pt{zrk.key.b, 32};
        BufferMut ct{w.ct.b, 32};
        const sarc::core::Status s = aead_seal(AeadId::ChaCha20Poly1305, nmk.key, nonce.nonce, aad, pt, ct, &w.tag);
        if (!sarc::core::is_ok(s)) {
            return s;
        }

        *wrapped_out = w;
        return sarc::core::ok_status();
    }

    sarc::core::Status unwrap_zone_root_key(const NodeMasterKey& nmk,
        const KeyWrapNonce& nonce,
        sarc::core::ZoneId zone,
        u32 version,
        const WrappedKey& wrapped,
        ZoneRootKey* zrk_out) noexcept {
        if (zrk_out == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!zone.is_valid() || sarc::core::zone_is_universal(zone)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        std::array<u8, 32> aad_scratch{};
        const BufferView aad = make_zrk_aad(zone, version, &aad_scratch);

        ZoneRootKey zrk{};
        zrk.zone = zone;
        zrk.version = version;

        const BufferView ct{wrapped.ct.b, 32};
        BufferMut pt{zrk.key.b, 32};
        const sarc::core::Status s = aead_open(AeadId::ChaCha20Poly1305, nmk.key, nonce.nonce, aad, ct, wrapped.tag, pt);
        if (!sarc::core::is_ok(s)) {
            return s;
        }

        *zrk_out = zrk;
        return sarc::core::ok_status();
    }
} // namespace sarc::security
