#include <array>
#include <cstddef>

#include <gtest/gtest.h>

#include "sarc/storage/hashing.hpp"

static void expect_hash_eq(const sarc::core::Hash256& got, const std::array<unsigned char, 32>& exp) {
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_EQ(got.b[i], static_cast<sarc::core::u8>(exp[i])) << "byte " << i;
    }
}

TEST(StorageHashing, EmptyVector) {
    sarc::core::Hash256 out{};
    const sarc::core::Status s = sarc::storage::hash_compute({nullptr, 0}, &out);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Ok);

    // BLAKE3("") 32-byte output
    const std::array<unsigned char, 32> expected = {
        0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6,
        0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
        0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7,
        0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
    };

    expect_hash_eq(out, expected);
}

TEST(StorageHashing, DeterministicAndDifferent) {
    const unsigned char abc[] = {'a', 'b', 'c'};
    const unsigned char abd[] = {'a', 'b', 'd'};

    sarc::core::Hash256 h1{};
    sarc::core::Hash256 h2{};
    sarc::core::Hash256 h3{};

    EXPECT_EQ(sarc::storage::hash_compute({reinterpret_cast<const sarc::storage::u8*>(abc), 3}, &h1).code,
              sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::storage::hash_compute({reinterpret_cast<const sarc::storage::u8*>(abc), 3}, &h2).code,
              sarc::core::StatusCode::Ok);
    EXPECT_EQ(sarc::storage::hash_compute({reinterpret_cast<const sarc::storage::u8*>(abd), 3}, &h3).code,
              sarc::core::StatusCode::Ok);

    EXPECT_EQ(h1.b, h2.b);
    EXPECT_NE(h1.b, h3.b);
}


TEST(StorageHashing, InvalidWhenOutNull) {
    const sarc::core::Status s = sarc::storage::hash_compute({nullptr, 0}, nullptr);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Storage);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

TEST(StorageHashing, InvalidWhenDataNullButLenNonZero) {
    sarc::core::Hash256 out{};
    const sarc::core::Status s = sarc::storage::hash_compute({nullptr, 1}, &out);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Storage);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}
