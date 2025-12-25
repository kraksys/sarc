#include <array>
#include <cstddef>

#include <gtest/gtest.h>

#include "sarc/security/crypto.hpp"

#if !defined(SARC_HAVE_LIBSODIUM) && !defined(SARC_HAVE_OPENSSL)

TEST(SecurityCrypto, BackendMissing) {
    const sarc::security::Key256 key{};
    const sarc::security::Nonce12 nonce{};
    const std::array<sarc::core::u8, 1> pt_bytes = {0};
    std::array<sarc::core::u8, 1> ct{};
    sarc::security::Tag16 tag{};

    const sarc::core::Status s = sarc::security::aead_seal(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {nullptr, 0},
        {pt_bytes.data(), 1},
        {ct.data(), 1},
        &tag);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Unavailable);
}

#else

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}

static sarc::security::Nonce12 make_nonce12_seq(sarc::core::u8 start) {
    sarc::security::Nonce12 n{};
    for (size_t i = 0; i < 12; ++i) {
        n.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return n;
}
} // namespace

TEST(SecurityCrypto, ChaCha20Poly1305RoundTrip) {
    const auto key = make_key256_seq(1);
    const auto nonce = make_nonce12_seq(9);

    const std::array<sarc::core::u8, 3> aad_bytes = {9, 8, 7};
    const std::array<sarc::core::u8, 8> pt_bytes = {0, 1, 2, 3, 4, 5, 6, 7};

    std::array<sarc::core::u8, 8> ct{};
    sarc::security::Tag16 tag{};

    const sarc::core::Status s1 = sarc::security::aead_seal(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {aad_bytes.data(), static_cast<sarc::security::u32>(aad_bytes.size())},
        {pt_bytes.data(), static_cast<sarc::security::u32>(pt_bytes.size())},
        {ct.data(), static_cast<sarc::security::u32>(ct.size())},
        &tag);
    EXPECT_EQ(s1.code, sarc::core::StatusCode::Ok);

    EXPECT_NE(ct, pt_bytes);

    std::array<sarc::core::u8, 8> out{};
    const sarc::core::Status s2 = sarc::security::aead_open(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {aad_bytes.data(), static_cast<sarc::security::u32>(aad_bytes.size())},
        {ct.data(), static_cast<sarc::security::u32>(ct.size())},
        tag,
        {out.data(), static_cast<sarc::security::u32>(out.size())});
    EXPECT_EQ(s2.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(out, pt_bytes);
}

TEST(SecurityCrypto, OpenFailsWithWrongTag) {
    const auto key = make_key256_seq(1);
    const auto nonce = make_nonce12_seq(9);

    const std::array<sarc::core::u8, 3> aad_bytes = {9, 8, 7};
    const std::array<sarc::core::u8, 8> pt_bytes = {0, 1, 2, 3, 4, 5, 6, 7};

    std::array<sarc::core::u8, 8> ct{};
    sarc::security::Tag16 tag{};

    ASSERT_EQ(sarc::security::aead_seal(
                  sarc::security::AeadId::ChaCha20Poly1305,
                  key,
                  nonce,
                  {aad_bytes.data(), static_cast<sarc::security::u32>(aad_bytes.size())},
                  {pt_bytes.data(), static_cast<sarc::security::u32>(pt_bytes.size())},
                  {ct.data(), static_cast<sarc::security::u32>(ct.size())},
                  &tag)
                  .code,
              sarc::core::StatusCode::Ok);

    tag.b[0] ^= 0xffu;

    std::array<sarc::core::u8, 8> out{};
    const sarc::core::Status s = sarc::security::aead_open(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {aad_bytes.data(), static_cast<sarc::security::u32>(aad_bytes.size())},
        {ct.data(), static_cast<sarc::security::u32>(ct.size())},
        tag,
        {out.data(), static_cast<sarc::security::u32>(out.size())});
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Crypto);
}

TEST(SecurityCrypto, SealRejectsTooSmallOutput) {
    const auto key = make_key256_seq(1);
    const auto nonce = make_nonce12_seq(9);

    const std::array<sarc::core::u8, 8> pt_bytes = {0, 1, 2, 3, 4, 5, 6, 7};
    std::array<sarc::core::u8, 7> ct{};
    sarc::security::Tag16 tag{};

    const sarc::core::Status s = sarc::security::aead_seal(
        sarc::security::AeadId::ChaCha20Poly1305,
        key,
        nonce,
        {nullptr, 0},
        {pt_bytes.data(), static_cast<sarc::security::u32>(pt_bytes.size())},
        {ct.data(), static_cast<sarc::security::u32>(ct.size())},
        &tag);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

#endif
