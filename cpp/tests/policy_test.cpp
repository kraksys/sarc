#include <cstddef>
#include <cstring>

#include <gtest/gtest.h>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/security/policy.hpp"

namespace {
static sarc::security::Key256 make_key256_seq(sarc::core::u8 start) {
    sarc::security::Key256 k{};
    for (size_t i = 0; i < 32; ++i) {
        k.b[i] = static_cast<sarc::core::u8>(start + static_cast<sarc::core::u8>(i));
    }
    return k;
}
} // namespace

TEST(SecurityPolicy, SealAndVerifyRoundTrip) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::ZoneId{42};
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read) |
                 sarc::security::right_mask(sarc::security::Right::List);
    cap.issued_at = 100;
    cap.expires_at = 200;
    cap.version = 1;

    sarc::security::Tag16 tag{};
    const sarc::core::Status s1 = sarc::security::capability_seal(key, cap, &tag);
    EXPECT_EQ(s1.code, sarc::core::StatusCode::Ok);

    cap.proof = tag;
    const sarc::core::Status s2 = sarc::security::capability_verify(key, cap);
    EXPECT_EQ(s2.code, sarc::core::StatusCode::Ok);
}

TEST(SecurityPolicy, VerifyFailsIfRightsTampered) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::ZoneId{42};
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read);
    cap.issued_at = 100;
    cap.expires_at = 200;
    cap.version = 1;

    sarc::security::Tag16 tag{};
    ASSERT_EQ(sarc::security::capability_seal(key, cap, &tag).code, sarc::core::StatusCode::Ok);
    cap.proof = tag;

    cap.rights |= sarc::security::right_mask(sarc::security::Right::Write);
    const sarc::core::Status s = sarc::security::capability_verify(key, cap);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityPolicy, SealRejectsUniversalZone) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::kZoneUniversal;
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read);

    sarc::security::Tag16 tag{};
    const sarc::core::Status s = sarc::security::capability_seal(key, cap, &tag);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityPolicy, VerifyRejectsUniversalZone) {
    const auto key = make_key256_seq(9);

    sarc::security::Capability cap{};
    cap.zone = sarc::core::kZoneUniversal;
    cap.grantee = sarc::core::UserId{7};
    cap.rights = sarc::security::right_mask(sarc::security::Right::Read);
    cap.version = 1;

    cap.proof = sarc::security::Tag16{}; // irrelevant; should fail fast on zone
    const sarc::core::Status s = sarc::security::capability_verify(key, cap);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Security);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(SecurityPolicy, CapabilityValidForZoneRequiresNonUniversal) {
    sarc::security::Capability cap{};
    cap.zone = sarc::core::ZoneId{3};

    EXPECT_TRUE(sarc::security::capability_valid_for_zone(cap, sarc::core::ZoneId{3}));
    EXPECT_FALSE(sarc::security::capability_valid_for_zone(cap, sarc::core::kZoneUniversal));
}

