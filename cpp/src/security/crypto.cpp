#include "sarc/security/crypto.hpp"

#include <cstddef>
#include <cstring>

#if defined(SARC_HAVE_LIBSODIUM)
#include <sodium.h>
#endif

#if defined(SARC_HAVE_OPENSSL)
#include <openssl/evp.h>
#endif

#include "sarc/security/kdf.hpp"

namespace sarc::security {
    namespace {
        [[nodiscard]] bool buffer_ok(BufferView b) noexcept {
            return (b.len == 0) || (b.data != nullptr);
        }

        [[nodiscard]] bool buffer_ok_mut(BufferMut b) noexcept {
            return (b.len == 0) || (b.data != nullptr);
        }

#if defined(SARC_HAVE_LIBSODIUM)
        sarc::core::Status ensure_sodium() noexcept {
            if (sodium_init() < 0) {
                return sarc::core::make_status(sarc::core::StatusDomain::External, sarc::core::StatusCode::Unavailable);
            }
            return sarc::core::ok_status();
        }
#endif
    } // namespace

    sarc::core::Status aead_seal(AeadId aead,
        const Key256& key,
        const Nonce12& nonce,
        BufferView aad,
        BufferView pt,
        BufferMut ct_out,
        Tag16* tag_out) noexcept {
        if (tag_out == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (!buffer_ok(aad) || !buffer_ok(pt) || !buffer_ok_mut(ct_out)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (ct_out.len < pt.len) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        if (aead != AeadId::ChaCha20Poly1305) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unsupported);
        }

#if defined(SARC_HAVE_LIBSODIUM)
        const sarc::core::Status init = ensure_sodium();
        if (!sarc::core::is_ok(init)) {
            return init;
        }

        unsigned long long mac_len = 0;

        const int rc = crypto_aead_chacha20poly1305_ietf_encrypt_detached(
            ct_out.data,
            tag_out->b,
            &mac_len,
            pt.data,
            static_cast<unsigned long long>(pt.len),
            aad.data,
            static_cast<unsigned long long>(aad.len),
            nullptr,
            nonce.b,
            key.b);

        if (rc != 0 || mac_len != sizeof(tag_out->b)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Crypto);
        }
        return sarc::core::ok_status();
#elif defined(SARC_HAVE_OPENSSL)
        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unavailable);
        }

        int ok = 1;
        ok &= EVP_EncryptInit_ex(ctx, EVP_chacha20_poly1305(), nullptr, nullptr, nullptr);
        ok &= EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, 12, nullptr);
        ok &= EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.b, nonce.b);

        int out_len = 0;
        if (aad.len > 0) {
            ok &= EVP_EncryptUpdate(ctx, nullptr, &out_len, aad.data, static_cast<int>(aad.len));
        }

        int ct_written = 0;
        if (pt.len > 0) {
            ok &= EVP_EncryptUpdate(ctx, ct_out.data, &out_len, pt.data, static_cast<int>(pt.len));
            ct_written += out_len;
        }

        ok &= EVP_EncryptFinal_ex(ctx, ct_out.data + ct_written, &out_len);
        ct_written += out_len;

        ok &= EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, 16, tag_out->b);
        EVP_CIPHER_CTX_free(ctx);

        if (!ok || static_cast<u32>(ct_written) != pt.len) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Crypto);
        }
        return sarc::core::ok_status();
#else
        (void)key;
        (void)nonce;
        (void)aad;
        (void)pt;
        (void)ct_out;
        std::memset(tag_out->b, 0, sizeof(tag_out->b));
        return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unavailable);
#endif
    }

    sarc::core::Status aead_open(AeadId aead,
        const Key256& key,
        const Nonce12& nonce,
        BufferView aad,
        BufferView ct,
        const Tag16& tag,
        BufferMut pt_out) noexcept {
        if (!buffer_ok(aad) || !buffer_ok(ct) || !buffer_ok_mut(pt_out)) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }
        if (pt_out.len < ct.len) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Invalid);
        }

        if (aead != AeadId::ChaCha20Poly1305) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unsupported);
        }

#if defined(SARC_HAVE_LIBSODIUM)
        const sarc::core::Status init = ensure_sodium();
        if (!sarc::core::is_ok(init)) {
            return init;
        }

        const int rc = crypto_aead_chacha20poly1305_ietf_decrypt_detached(
            pt_out.data,
            nullptr,
            ct.data,
            static_cast<unsigned long long>(ct.len),
            tag.b,
            aad.data,
            static_cast<unsigned long long>(aad.len),
            nonce.b,
            key.b);

        if (rc != 0) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Crypto);
        }
        return sarc::core::ok_status();
#elif defined(SARC_HAVE_OPENSSL)
        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unavailable);
        }

        int ok = 1;
        ok &= EVP_DecryptInit_ex(ctx, EVP_chacha20_poly1305(), nullptr, nullptr, nullptr);
        ok &= EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, 12, nullptr);
        ok &= EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.b, nonce.b);

        int out_len = 0;
        if (aad.len > 0) {
            ok &= EVP_DecryptUpdate(ctx, nullptr, &out_len, aad.data, static_cast<int>(aad.len));
        }

        int pt_written = 0;
        if (ct.len > 0) {
            ok &= EVP_DecryptUpdate(ctx, pt_out.data, &out_len, ct.data, static_cast<int>(ct.len));
            pt_written += out_len;
        }

        ok &= EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG, 16, const_cast<u8*>(tag.b));
        const int final_ok = EVP_DecryptFinal_ex(ctx, pt_out.data + pt_written, &out_len);
        EVP_CIPHER_CTX_free(ctx);

        if (!ok || final_ok <= 0) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Crypto);
        }
        pt_written += out_len;
        if (static_cast<u32>(pt_written) != ct.len) {
            return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Crypto);
        }
        return sarc::core::ok_status();
#else
        (void)key;
        (void)nonce;
        (void)aad;
        (void)ct;
        (void)tag;
        (void)pt_out;
        return sarc::core::make_status(sarc::core::StatusDomain::Security, sarc::core::StatusCode::Unavailable);
#endif
    }

    sarc::core::Status hkdf_derive_object_key(const Key256& zone_root_key,
        const AeadAad& context,
        Key256* out_key) noexcept {
        KdfInfo info{};
        info.domain = KdfDomain::ObjectKey;
        info.version = 1;
        info.zone = context.zone;
        info.object = context.object;
        return ::sarc::security::hkdf_derive_object_key(zone_root_key, info, out_key);
    }
} // namespace sarc::security
