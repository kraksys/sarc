#include "sarc/storage/layout.hpp"

#include <cstddef>
#include <cstring>

namespace sarc::storage {
    namespace {
        void put_u32_be(u8* out, u32 v) noexcept {
            out[0] = static_cast<u8>((v >> 24) & 0xffu);
            out[1] = static_cast<u8>((v >> 16) & 0xffu);
            out[2] = static_cast<u8>((v >> 8) & 0xffu);
            out[3] = static_cast<u8>((v >> 0) & 0xffu);
        }

        void put_u64_be(u8* out, u64 v) noexcept {
            out[0] = static_cast<u8>((v >> 56) & 0xffu);
            out[1] = static_cast<u8>((v >> 48) & 0xffu);
            out[2] = static_cast<u8>((v >> 40) & 0xffu);
            out[3] = static_cast<u8>((v >> 32) & 0xffu);
            out[4] = static_cast<u8>((v >> 24) & 0xffu);
            out[5] = static_cast<u8>((v >> 16) & 0xffu);
            out[6] = static_cast<u8>((v >> 8) & 0xffu);
            out[7] = static_cast<u8>((v >> 0) & 0xffu);
        }

        [[nodiscard]] u32 get_u32_be(const u8* in) noexcept {
            return (static_cast<u32>(in[0]) << 24) | (static_cast<u32>(in[1]) << 16) | (static_cast<u32>(in[2]) << 8) |
                   (static_cast<u32>(in[3]) << 0);
        }

        [[nodiscard]] u64 get_u64_be(const u8* in) noexcept {
            return (static_cast<u64>(in[0]) << 56) | (static_cast<u64>(in[1]) << 48) | (static_cast<u64>(in[2]) << 40) |
                   (static_cast<u64>(in[3]) << 32) | (static_cast<u64>(in[4]) << 24) | (static_cast<u64>(in[5]) << 16) |
                   (static_cast<u64>(in[6]) << 8) | (static_cast<u64>(in[7]) << 0);
        }
    } // namespace

    u32 layout_write_object_header(const ObjectHeader& h, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kObjectHeaderBytes) {
            return 0;
        }
        if (h.version != kLayoutVersion) {
            return 0;
        }
        if (!h.zone.is_valid() || sarc::core::zone_is_universal(h.zone)) {
            return 0;
        }
        if (h.reserved != 0) {
            return 0;
        }

        u8* p = out.data;
        put_u32_be(p + 0, h.version);
        put_u32_be(p + 4, static_cast<u32>(h.zone.v));
        std::memcpy(p + 8, h.content.b.data(), h.content.b.size());
        put_u64_be(p + 40, h.size_bytes);
        put_u32_be(p + 48, h.flags);
        put_u32_be(p + 52, h.reserved);
        return kObjectHeaderBytes;
    }

    LayoutParseResult layout_read_object_header(BufferView in, ObjectHeader* out) noexcept {
        if (out == nullptr) {
            return LayoutParseResult::Invalid;
        }
        if (in.len < kObjectHeaderBytes) {
            return LayoutParseResult::NeedMore;
        }
        if (in.data == nullptr) {
            return LayoutParseResult::Invalid;
        }

        const u8* p = in.data;
        const u32 version = get_u32_be(p + 0);
        const sarc::core::ZoneId zone{get_u32_be(p + 4)};
        sarc::core::Hash256 content{};
        std::memcpy(content.b.data(), p + 8, content.b.size());
        const u64 size_bytes = get_u64_be(p + 40);
        const u32 flags = get_u32_be(p + 48);
        const u32 reserved = get_u32_be(p + 52);

        if (version != kLayoutVersion) {
            return LayoutParseResult::Invalid;
        }
        if (!zone.is_valid() || sarc::core::zone_is_universal(zone)) {
            return LayoutParseResult::Invalid;
        }
        if (reserved != 0) {
            return LayoutParseResult::Invalid;
        }

        out->version = version;
        out->zone = zone;
        out->content = content;
        out->size_bytes = size_bytes;
        out->flags = flags;
        out->reserved = reserved;
        return LayoutParseResult::Ok;
    }

    u32 layout_write_index_entry(const IndexEntry& e, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kIndexEntryBytes) {
            return 0;
        }
        if (!e.zone.is_valid() || sarc::core::zone_is_universal(e.zone)) {
            return 0;
        }

        u8* p = out.data;
        put_u32_be(p + 0, static_cast<u32>(e.zone.v));
        std::memcpy(p + 4, e.content.b.data(), e.content.b.size());
        put_u64_be(p + 36, e.offset);
        put_u64_be(p + 44, e.size_bytes);
        return kIndexEntryBytes;
    }

    LayoutParseResult layout_read_index_entry(BufferView in, IndexEntry* out) noexcept {
        if (out == nullptr) {
            return LayoutParseResult::Invalid;
        }
        if (in.len < kIndexEntryBytes) {
            return LayoutParseResult::NeedMore;
        }
        if (in.data == nullptr) {
            return LayoutParseResult::Invalid;
        }

        const u8* p = in.data;
        const sarc::core::ZoneId zone{get_u32_be(p + 0)};
        sarc::core::Hash256 content{};
        std::memcpy(content.b.data(), p + 4, content.b.size());
        const u64 offset = get_u64_be(p + 36);
        const u64 size_bytes = get_u64_be(p + 44);

        if (!zone.is_valid() || sarc::core::zone_is_universal(zone)) {
            return LayoutParseResult::Invalid;
        }

        out->zone = zone;
        out->content = content;
        out->offset = offset;
        out->size_bytes = size_bytes;
        return LayoutParseResult::Ok;
    }
} // namespace sarc::storage
