#pragma once

#include <type_traits>

#include "sarc/core/types.hpp"
#include "sarc/storage/buffer.hpp"

namespace sarc::storage {
    using u8 = sarc::core::u8;
    using u32 = sarc::core::u32;
    using u64 = sarc::core::u64;

    constexpr u32 kLayoutVersion = 1;

    // ObjectHeader wire/layout format (big-endian):
    // 0..3 version(u32)
    // 4..7 zone_id(u32)
    // 8..39 content_hash(32 bytes)
    // 40..47 size_bytes(u64)
    // 48..51 flags(u32)
    // 52..55 reserved(u32=0)
    inline constexpr u32 kObjectHeaderBytes = 56;

    struct alignas(8) ObjectHeader {
        u32 version{kLayoutVersion};
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::Hash256 content{};
        u64 size_bytes{0};
        u32 flags{0};
        u32 reserved{0};
    };

    // IndexEntry wire/layout format (big-endian):
    // 0..3 zone_id(u32)
    // 4..35 content_hash(32 bytes)
    // 36..43 offset(u64)
    // 44..51 size_bytes(u64)
    inline constexpr u32 kIndexEntryBytes = 52;

    struct alignas(8) IndexEntry {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::Hash256 content{};
        u64 offset{0};
        u64 size_bytes{0};
    };

    enum class LayoutParseResult : u8 {
        Ok = 0,
        NeedMore,
        Invalid,
    };

    // Big-endian on disk/wire. Returns bytes written (0 on failure).
    [[nodiscard]] u32 layout_write_object_header(const ObjectHeader& h, BufferMut out) noexcept;
    [[nodiscard]] LayoutParseResult layout_read_object_header(BufferView in, ObjectHeader* out) noexcept;

    [[nodiscard]] u32 layout_write_index_entry(const IndexEntry& e, BufferMut out) noexcept;
    [[nodiscard]] LayoutParseResult layout_read_index_entry(BufferView in, IndexEntry* out) noexcept;

    static_assert(sizeof(ObjectHeader) == 56);
    static_assert(sizeof(IndexEntry) == 56);  // 52 bytes data + 4 bytes padding for alignas(8)
    static_assert(alignof(ObjectHeader) == 8);
    static_assert(alignof(IndexEntry) == 8);
    static_assert(std::is_trivially_copyable_v<ObjectHeader>);
    static_assert(std::is_trivially_copyable_v<IndexEntry>);
    static_assert(std::is_standard_layout_v<ObjectHeader>);
    static_assert(std::is_standard_layout_v<IndexEntry>);

} // namespace sarc::storage
