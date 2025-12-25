#include <cstddef>
#include <cstring>

#include <gtest/gtest.h>

#include "sarc/security/keystore.hpp"

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

TEST(SecurityKeystore, WrapUnwrapRoundTrip) {
    sarc::security::NodeMasterKey nmk{};
    nmk.key = make_key256_seq(1);

    sarc::security::KeyWrapNonce nonce{};
    nonce.nonce = make_nonce12_seq(9);

    sarc::security::ZoneRootKey zrk{};
    zrk.zone = sarc::core::ZoneId{42};
    zrk.version = 1;
    zrk.key = make_key256_seq(77);

    sarc::security::WrappedKey wrapped{};
    const sarc::core::Status s1 = sarc::security::wrap_zone_root_key(nmk, nonce, zrk, &wrapped);
    EXPECT_EQ(s1.code, sarc::core::StatusCode::Ok);

    sarc::security::ZoneRootKey out{};
    const sarc::core::Status s2 = sarc::security::unwrap_zone_root_key(
        nmk, nonce, zrk.zone, zrk.version, wrapped, &out);
    EXPECT_EQ(s2.code, sarc::core::StatusCode::Ok);

    EXPECT_EQ(out.zone.v, zrk.zone.v);
    EXPECT_EQ(out.version, zrk.version);
    EXPECT_EQ(0, std::memcmp(out.key.b, zrk.key.b, 32));
}

TEST(SecurityKeystore, UnwrapFailsIfTagTampered) {
    sarc::security::NodeMasterKey nmk{};
    nmk.key = make_key256_seq(1);

    sarc::security::KeyWrapNonce nonce{};
    nonce.nonce = make_nonce12_seq(9);

    sarc::security::ZoneRootKey zrk{};
    zrk.zone = sarc::core::ZoneId{42};
    zrk.version = 1;
    zrk.key = make_key256_seq(77);

    sarc::security::WrappedKey wrapped{};
    ASSERT_EQ(sarc::security::wrap_zone_root_key(nmk, nonce, zrk, &wrapped).code, sarc::core::StatusCode::Ok);

    wrapped.tag.b[0] ^= 0xffu;

    sarc::security::ZoneRootKey out{};
    const sarc::core::Status s = sarc::security::unwrap_zone_root_key(
        nmk, nonce, zrk.zone, zrk.version, wrapped, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_NE(s.code, sarc::core::StatusCode::Ok);
}

TEST(SecurityKeystore, UnwrapFailsIfZoneMismatch) {
    sarc::security::NodeMasterKey nmk{};
    nmk.key = make_key256_seq(1);

    sarc::security::KeyWrapNonce nonce{};
    nonce.nonce = make_nonce12_seq(9);

    sarc::security::ZoneRootKey zrk{};
    zrk.zone = sarc::core::ZoneId{42};
    zrk.version = 1;
    zrk.key = make_key256_seq(77);

    sarc::security::WrappedKey wrapped{};
    ASSERT_EQ(sarc::security::wrap_zone_root_key(nmk, nonce, zrk, &wrapped).code, sarc::core::StatusCode::Ok);

    sarc::security::ZoneRootKey out{};
    const sarc::core::Status s = sarc::security::unwrap_zone_root_key(
        nmk, nonce, sarc::core::ZoneId{43}, zrk.version, wrapped, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_NE(s.code, sarc::core::StatusCode::Ok);
}

