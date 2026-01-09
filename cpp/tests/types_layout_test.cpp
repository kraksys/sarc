#include <gtest/gtest.h>
#include "sarc/core/types.hpp"
#include "sarc/core/models.hpp"
#include "sarc/storage/layout.hpp"
#include <cstddef>

using namespace sarc::core;
using namespace sarc::storage;

TEST(TypesLayout, TraceSizeAndAlignment) {
    EXPECT_EQ(sizeof(Trace), 32);
    EXPECT_EQ(alignof(Trace), 8);
    EXPECT_TRUE(std::is_trivially_copyable_v<Trace>);
    EXPECT_TRUE(std::is_standard_layout_v<Trace>);

    // Verify hot data is in first 16 bytes
    EXPECT_EQ(offsetof(Trace, data), 0);
    EXPECT_EQ(offsetof(Trace, type), 8);
    EXPECT_EQ(offsetof(Trace, confidence), 12);

    // Verify cold data is after 16 byte boundary
    EXPECT_GE(offsetof(Trace, source), 16);
    EXPECT_GE(offsetof(Trace, updated_at), 16);
}

TEST(TypesLayout, GestaltSizeAndAlignment) {
    EXPECT_EQ(sizeof(Gestalt), 32);
    EXPECT_EQ(alignof(Gestalt), 8);
    EXPECT_TRUE(std::is_trivially_copyable_v<Gestalt>);
    EXPECT_TRUE(std::is_standard_layout_v<Gestalt>);

    // Verify hot data in first 16 bytes
    EXPECT_LT(offsetof(Gestalt, type), 16);
    EXPECT_LT(offsetof(Gestalt, value), 16);
    EXPECT_LT(offsetof(Gestalt, weight), 16);
    EXPECT_LT(offsetof(Gestalt, confidence), 16);

    // All hot fields should fit in first 16 bytes
    EXPECT_EQ(offsetof(Gestalt, type), 0);
    EXPECT_EQ(offsetof(Gestalt, value), 4);
    EXPECT_EQ(offsetof(Gestalt, weight), 8);
    EXPECT_EQ(offsetof(Gestalt, confidence), 12);
}

TEST(TypesLayout, ObjectMetaSizeAndAlignment) {
    EXPECT_EQ(sizeof(ObjectMeta), 32);
    EXPECT_EQ(alignof(ObjectMeta), 8);
    EXPECT_TRUE(std::is_trivially_copyable_v<ObjectMeta>);
    EXPECT_TRUE(std::is_standard_layout_v<ObjectMeta>);

    // Hot data first
    EXPECT_EQ(offsetof(ObjectMeta, size_bytes), 0);
    EXPECT_EQ(offsetof(ObjectMeta, refcount), 8);

    // Cold data after 16 byte boundary
    EXPECT_GE(offsetof(ObjectMeta, created_at), 16);
    EXPECT_GE(offsetof(ObjectMeta, updated_at), 16);
}

TEST(TypesLayout, FileMetaSizeAndAlignment) {
    EXPECT_EQ(sizeof(FileMeta), 80);
    EXPECT_EQ(alignof(FileMeta), 8);
    EXPECT_TRUE(std::is_trivially_copyable_v<FileMeta>);
    EXPECT_TRUE(std::is_standard_layout_v<FileMeta>);

    // Hot data grouped at start
    EXPECT_EQ(offsetof(FileMeta, id), 0);
    EXPECT_EQ(offsetof(FileMeta, content), 8);
    EXPECT_EQ(offsetof(FileMeta, zone), 40);
    EXPECT_LT(offsetof(FileMeta, size_bytes), 56);

    // Warm data (name, mime)
    EXPECT_GE(offsetof(FileMeta, name), 56);
    EXPECT_GE(offsetof(FileMeta, mime), 56);

    // Cold data (timestamps)
    EXPECT_GE(offsetof(FileMeta, created_at), 64);
    EXPECT_GE(offsetof(FileMeta, updated_at), 64);
}

TEST(TypesLayout, ObjectHeaderAlignment) {
    EXPECT_EQ(sizeof(ObjectHeader), 56);
    EXPECT_EQ(alignof(ObjectHeader), 8);
    EXPECT_TRUE(std::is_standard_layout_v<ObjectHeader>);
    EXPECT_TRUE(std::is_trivially_copyable_v<ObjectHeader>);
}

TEST(TypesLayout, IndexEntryAlignment) {
    EXPECT_EQ(sizeof(IndexEntry), 56);  // 52 bytes data + 4 bytes padding for alignas(8)
    EXPECT_EQ(alignof(IndexEntry), 8);
    EXPECT_TRUE(std::is_standard_layout_v<IndexEntry>);
    EXPECT_TRUE(std::is_trivially_copyable_v<IndexEntry>);
}

TEST(TypesLayout, CacheLineBoundaries) {
    // Trace: hot data should fit in 16 bytes (L1 cache line segment)
    constexpr size_t trace_hot_size =
        offsetof(Trace, source); // First cold field offset
    EXPECT_LE(trace_hot_size, 16);

    // Gestalt: hot data should fit in 16 bytes
    constexpr size_t gestalt_hot_size =
        offsetof(Gestalt, source); // First cold field offset
    EXPECT_LE(gestalt_hot_size, 16);

    // ObjectMeta: hot data should fit in 16 bytes
    constexpr size_t objectmeta_hot_size =
        offsetof(ObjectMeta, created_at); // First cold field offset
    EXPECT_LE(objectmeta_hot_size, 16);
}

TEST(TypesLayout, NoImplicitPadding) {
    // Verify explicit padding fields exist and are correctly sized
    Trace t{};
    Gestalt g{};
    ObjectMeta om{};
    FileMeta fm{};

    // These should compile - padding fields exist
    (void)t._pad;
    (void)t._pad2;
    (void)g._pad;
    (void)om._pad;
    (void)fm._pad1;

    // Size checks ensure padding is accounted for
    EXPECT_EQ(sizeof(t._pad), 3);
    EXPECT_EQ(sizeof(t._pad2), 4);
    EXPECT_EQ(sizeof(g._pad), 4);
    EXPECT_EQ(sizeof(om._pad), 4);
    EXPECT_EQ(sizeof(fm._pad1), 4);
}
