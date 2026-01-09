#include <array>

#include <gtest/gtest.h>

#include "sarc/storage/layout.hpp"

TEST(StorageLayout, ObjectHeaderRoundTrip) {
    sarc::storage::ObjectHeader in{};
    in.version = sarc::storage::kLayoutVersion;
    in.zone = sarc::core::ZoneId{42};
    in.size_bytes = 0x0102030405060708ull;
    in.flags = 0xaabbccdd;
    in.reserved = 0;
    for (size_t i = 0; i < in.content.b.size(); ++i) {
        in.content.b[i] = static_cast<sarc::core::u8>(i);
    }

    std::array<sarc::storage::u8, sarc::storage::kObjectHeaderBytes> buf{};
    ASSERT_EQ(sarc::storage::layout_write_object_header(in, {buf.data(), static_cast<sarc::storage::u32>(buf.size())}),
              sarc::storage::kObjectHeaderBytes);

    sarc::storage::ObjectHeader out{};
    ASSERT_EQ(sarc::storage::layout_read_object_header({buf.data(), static_cast<sarc::storage::u32>(buf.size())}, &out),
              sarc::storage::LayoutParseResult::Ok);

    EXPECT_EQ(out.version, in.version);
    EXPECT_EQ(out.zone, in.zone);
    EXPECT_EQ(out.content.b, in.content.b);
    EXPECT_EQ(out.size_bytes, in.size_bytes);
    EXPECT_EQ(out.flags, in.flags);
    EXPECT_EQ(out.reserved, 0u);
}

TEST(StorageLayout, NeedMoreWhenShort) {
    std::array<sarc::storage::u8, sarc::storage::kObjectHeaderBytes - 1> buf{};
    sarc::storage::ObjectHeader out{};
    EXPECT_EQ(sarc::storage::layout_read_object_header({buf.data(), static_cast<sarc::storage::u32>(buf.size())}, &out),
              sarc::storage::LayoutParseResult::NeedMore);
}

TEST(StorageLayout, InvalidWhenReservedNonZero) {
    sarc::storage::ObjectHeader in{};
    in.version = sarc::storage::kLayoutVersion;
    in.zone = sarc::core::ZoneId{1};
    in.reserved = 0;

    std::array<sarc::storage::u8, sarc::storage::kObjectHeaderBytes> buf{};
    ASSERT_EQ(sarc::storage::layout_write_object_header(in, {buf.data(), static_cast<sarc::storage::u32>(buf.size())}),
              sarc::storage::kObjectHeaderBytes);

    // reserved is last 4 bytes
    buf[52] = 0;
    buf[53] = 0;
    buf[54] = 0;
    buf[55] = 1;

    sarc::storage::ObjectHeader out{};
    EXPECT_EQ(sarc::storage::layout_read_object_header({buf.data(), static_cast<sarc::storage::u32>(buf.size())}, &out),
              sarc::storage::LayoutParseResult::Invalid);
}

TEST(StorageLayout, IndexEntryRoundTrip) {
    sarc::storage::IndexEntry in{};
    in.zone = sarc::core::ZoneId{7};
    in.offset = 0x0102030405060708ull;
    in.size_bytes = 0x1112131415161718ull;
    for (size_t i = 0; i < in.content.b.size(); ++i) {
        in.content.b[i] = static_cast<sarc::core::u8>(0xffu - static_cast<unsigned>(i));
    }

    std::array<sarc::storage::u8, sarc::storage::kIndexEntryBytes> buf{};
    ASSERT_EQ(sarc::storage::layout_write_index_entry(in, {buf.data(), static_cast<sarc::storage::u32>(buf.size())}),
              sarc::storage::kIndexEntryBytes);

    sarc::storage::IndexEntry out{};
    ASSERT_EQ(sarc::storage::layout_read_index_entry({buf.data(), static_cast<sarc::storage::u32>(buf.size())}, &out),
              sarc::storage::LayoutParseResult::Ok);

    EXPECT_EQ(out.zone, in.zone);
    EXPECT_EQ(out.content.b, in.content.b);
    EXPECT_EQ(out.offset, in.offset);
    EXPECT_EQ(out.size_bytes, in.size_bytes);
}

