#include "sarc/net/framing.hpp"

namespace sarc::net {
    static void put_u16_be(u8* p, u16 v) noexcept {
        p[0] = static_cast<u8>((v >> 8) & 0xffu);
        p[1] = static_cast<u8>((v >> 0) & 0xffu);
    }

    static void put_u32_be(u8* p, u32 v) noexcept {
        p[0] = static_cast<u8>((v >> 24) & 0xffu);
        p[1] = static_cast<u8>((v >> 16) & 0xffu);
        p[2] = static_cast<u8>((v >> 8) & 0xffu);
        p[3] = static_cast<u8>((v >> 0) & 0xffu);
    }

    static u16 get_u16_be(const u8* p) noexcept {
        return static_cast<u16>((static_cast<u16>(p[0]) << 8) | static_cast<u16>(p[1]));
    }

    static u32 get_u32_be(const u8* p) noexcept {
        return (static_cast<u32>(p[0]) << 24) |
               (static_cast<u32>(p[1]) << 16) |
               (static_cast<u32>(p[2]) << 8) |
               (static_cast<u32>(p[3]) << 0);
    }

    static bool frame_type_valid_u16(u16 v) noexcept {
        switch (static_cast<FrameType>(v)) {
        case FrameType::Request:
        case FrameType::Response:
        case FrameType::Event:
        case FrameType::Control:
            return true;
        default:
            return false;
        }
    }

    u32 frame_write_header(const FrameHeader& h, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kFrameHeaderBytes) {
            return 0;
        }

        put_u16_be(out.data + 0, h.version);
        put_u16_be(out.data + 2, static_cast<u16>(h.type));
        put_u16_be(out.data + 4, h.flags);
        put_u16_be(out.data + 6, 0); // reserved
        put_u32_be(out.data + 8, h.payload_len);
        put_u32_be(out.data + 12, h.request_id);

        return kFrameHeaderBytes;
    }

    FrameParseResult frame_read_header(BufferView in, FrameHeader* out) noexcept {
        if (out == nullptr) return FrameParseResult::Invalid;
        if (in.data == nullptr) return FrameParseResult::NeedMore;
        if (in.len < kFrameHeaderBytes) return FrameParseResult::NeedMore;

        const u16 version = get_u16_be(in.data + 0);
        const u16 type_u16 = get_u16_be(in.data + 2);
        const u16 flags = get_u16_be(in.data + 4);
        const u16 reserved = get_u16_be(in.data + 6);
        const u32 payload_len = get_u32_be(in.data + 8);
        const u32 request_id = get_u32_be(in.data + 12);

        if (version != 1) return FrameParseResult::Invalid;
        if (reserved != 0) return FrameParseResult::Invalid;
        if (!frame_type_valid_u16(type_u16)) return FrameParseResult::Invalid;

        FrameHeader h{};
        h.version = version;
        h.type = static_cast<FrameType>(type_u16);
        h.flags = flags;
        h.payload_len = payload_len;
        h.request_id = request_id;

        *out = h;
        return FrameParseResult::Ok;
    }
} // namespace sarc::net
