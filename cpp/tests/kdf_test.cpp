#include <cstddef>
#include <cstring>

#include <gtest/gtest.h>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/security/kdf.hpp"

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}

static sarc::core::Hash256 make_hash256_seq(sarc::core::u8 start) {
    sarc::core::Hash256 h{};
    for (size_t i = 0; i < 32; ++i) {
        h.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return h;
}

static void expect_key_eq(const sarc::security::Key256& a, const sarc::security::Key256& b) {
    EXPECT_EQ(0, std::memcmp(a.b, b.b, 32));
}

static void expect_key_ne(const sarc::security::Key256& a, const sarc::security::Key256& b) {
    EXPECT_NE(0, std::memcmp(a.b, b.b, 32));
}

static void expect_nonce_eq(const sarc::security::Nonce12& a, const sarc::security::Nonce12& b) {
    EXPECT_EQ(0, std::memcmp(a.b, b.b, 12));
}

static void expect_nonce_ne(const sarc::security::Nonce12& a, const sarc::security::Nonce12& b) {
    EXPECT_NE(0, std::memcmp(a.b, b.b, 12));
}
} // namespace

TEST(SecurityKdf, ExpandRejectsNullOut) {
    const sarc::security::Key256 ikm = make_key256_seq(1);
    const sarc::core::Status s = sarc::security::hkdf_expand(
        ikm, sarc::security::BufferView{nullptr, 0}, sarc::security::BufferView{nullptr, 0}, nullptr);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityKdf, ExpandRejectsNullSaltWhenLenNonZero) {
    const sarc::security::Key256 ikm = make_key256_seq(1);
    sarc::security::Key256 out{};
    const sarc::core::Status s = sarc::security::hkdf_expand(
        ikm, sarc::security::BufferView{nullptr, 1}, sarc::security::BufferView{nullptr, 0}, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityKdf, ExpandRejectsNullInfoWhenLenNonZero) {
    const sarc::security::Key256 ikm = make_key256_seq(1);
    sarc::security::Key256 out{};
    const sarc::core::Status s = sarc::security::hkdf_expand(
        ikm, sarc::security::BufferView{nullptr, 0}, sarc::security::BufferView{nullptr, 7}, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityKdf, ExpandDeterministicAndSaltInfoMatter) {
    const sarc::security::Key256 ikm = make_key256_seq(1);

    const sarc::core::u8 salt1_bytes[] = {1, 2, 3, 4};
    const sarc::core::u8 salt2_bytes[] = {1, 2, 3, 5};
    const sarc::core::u8 info1_bytes[] = {9, 8, 7};
    const sarc::core::u8 info2_bytes[] = {9, 8, 6};

    sarc::security::Key256 a{};
    sarc::security::Key256 b{};
    sarc::security::Key256 c{};
    sarc::security::Key256 d{};

    EXPECT_EQ(sarc::security::hkdf_expand(
                  ikm,
                  sarc::security::BufferView{salt1_bytes, 4},
                  sarc::security::BufferView{info1_bytes, 3},
                  &a)
                  .code,
              sarc::core::StatusCode::Ok);

    EXPECT_EQ(sarc::security::hkdf_expand(
                  ikm,
                  sarc::security::BufferView{salt1_bytes, 4},
                  sarc::security::BufferView{info1_bytes, 3},
                  &b)
                  .code,
              sarc::core::StatusCode::Ok);

    EXPECT_EQ(sarc::security::hkdf_expand(
                  ikm,
                  sarc::security::BufferView{salt2_bytes, 4},
                  sarc::security::BufferView{info1_bytes, 3},
                  &c)
                  .code,
              sarc::core::StatusCode::Ok);

    EXPECT_EQ(sarc::security::hkdf_expand(
                  ikm,
                  sarc::security::BufferView{salt1_bytes, 4},
                  sarc::security::BufferView{info2_bytes, 3},
                  &d)
                  .code,
              sarc::core::StatusCode::Ok);

    expect_key_eq(a, b);
    expect_key_ne(a, c);
    expect_key_ne(a, d);
}

TEST(SecurityKdf, DeriveObjectKeyRejectsUniversalOrInvalidZone) {
    const sarc::security::Key256 zrk = make_key256_seq(7);

    sarc::security::KdfInfo info{};
    info.domain = sarc::security::KdfDomain::ObjectKey;
    info.version = 1;
    info.object.zone = sarc::core::ZoneId::invalid();
    info.object.content = make_hash256_seq(1);

    sarc::security::Key256 out{};
    {
        const sarc::core::Status s = sarc::security::hkdf_derive_object_key(zrk, info, &out);
        EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
        EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
    }

    info.object.zone = sarc::core::kZoneUniversal;
    {
        const sarc::core::Status s = sarc::security::hkdf_derive_object_key(zrk, info, &out);
        EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
        EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
    }
}

TEST(SecurityKdf, DeriveObjectKeyRejectsZoneMismatch) {
    const sarc::security::Key256 zrk = make_key256_seq(7);

    sarc::security::KdfInfo info{};
    info.domain = sarc::security::KdfDomain::ObjectKey;
    info.version = 1;
    info.zone = sarc::core::ZoneId{123};
    info.object.zone = sarc::core::ZoneId{456};
    info.object.content = make_hash256_seq(1);

    sarc::security::Key256 out{};
    const sarc::core::Status s = sarc::security::hkdf_derive_object_key(zrk, info, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityKdf, DeriveObjectKeyDeterministicAndDomainSeparated) {
    const sarc::security::Key256 zrk = make_key256_seq(7);

    sarc::security::KdfInfo a{};
    a.domain = sarc::security::KdfDomain::ObjectKey;
    a.version = 1;
    a.object.zone = sarc::core::ZoneId{42};
    a.object.content = make_hash256_seq(1);

    sarc::security::KdfInfo b = a;

    sarc::security::KdfInfo c = a;
    c.object.content = make_hash256_seq(2);

    sarc::security::KdfInfo d = a;
    d.domain = sarc::security::KdfDomain::Capability;

    sarc::security::Key256 k1{};
    sarc::security::Key256 k2{};
    sarc::security::Key256 k3{};
    sarc::security::Key256 k4{};

    EXPECT_EQ(sarc::security::hkdf_derive_object_key(zrk, a, &k1).code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::security::hkdf_derive_object_key(zrk, b, &k2).code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::security::hkdf_derive_object_key(zrk, c, &k3).code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::security::hkdf_derive_object_key(zrk, d, &k4).code, sarc::core::StatusCode::Ok);

    expect_key_eq(k1, k2);
    expect_key_ne(k1, k3);
    expect_key_ne(k1, k4);
}

TEST(SecurityKdf, DeriveObjectNonceDeterministicAndDifferentBetweenObjects) {
    const sarc::security::Key256 zrk = make_key256_seq(7);

    sarc::security::KdfInfo a{};
    a.domain = sarc::security::KdfDomain::ObjectNonce;
    a.version = 1;
    a.object.zone = sarc::core::ZoneId{42};
    a.object.content = make_hash256_seq(1);

    sarc::security::KdfInfo b = a;
    b.object.content = make_hash256_seq(2);

    sarc::security::Nonce12 n1{};
    sarc::security::Nonce12 n2{};
    sarc::security::Nonce12 n3{};

    EXPECT_EQ(sarc::security::derive_object_nonce(zrk, a, &n1).code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::security::derive_object_nonce(zrk, a, &n2).code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::security::derive_object_nonce(zrk, b, &n3).code, sarc::core::StatusCode::Ok);

    expect_nonce_eq(n1, n2);
    expect_nonce_ne(n1, n3);
}

TEST(SecurityKdf, DeriveObjectNonceRejectsNullOut) {
    const sarc::security::Key256 zrk = make_key256_seq(7);

    sarc::security::KdfInfo a{};
    a.domain = sarc::security::KdfDomain::ObjectNonce;
    a.version = 1;
    a.object.zone = sarc::core::ZoneId{42};
    a.object.content = make_hash256_seq(1);

    const sarc::core::Status s = sarc::security::derive_object_nonce(zrk, a, nullptr);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

