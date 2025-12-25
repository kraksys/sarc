#pragma once

#include <cstdint>
#include <type_traits>

#include "sarc/core/types.hpp"

namespace sarc::net {
    using u8 = sarc::core::u8;
    using u16 = sarc::core::u16;
    using u32 = sarc::core::u32;

    struct BufferView {
        const u8* data{nullptr};
        u32 len{0};
    };

    struct BufferMut {
        u8* data{nullptr};
        u32 len{0};
    };

    enum class FrameType : u16 {
        Request = 1,
        Response = 2,
        Event = 3,
        Control = 4,
    };

    struct FrameHeader {
        u16 version{1};
        FrameType type{FrameType::Request};
        u16 flags{0};
        u32 payload_len{0};
        u32 request_id{0};
    };

    // Layout (big-endian / network order):
    // 0..1 version(u16), 2..3 type(u16), 4..5 flags(u16), 6..7 reserved(u16=0),
    // 8..11 payload_len(u32), 12..15 request_id(u32).
    inline constexpr u32 kFrameHeaderBytes = 16;

    enum class FrameParseResult : u8 {
        Ok = 0,
        NeedMore,
        Invalid,
    };

    [[nodiscard]] constexpr bool frame_header_valid(const FrameHeader& h, u32 max_payload) noexcept {
        return h.version == 1 && h.payload_len <= max_payload;
    }

    // Big-endian on wire. Returns bytes written (0 on failure).
    [[nodiscard]] u32 frame_write_header(const FrameHeader& h, BufferMut out) noexcept;

    // Parses a header from the first bytes of 'in' (does not consume).
    [[nodiscard]] FrameParseResult frame_read_header(BufferView in, FrameHeader* out) noexcept;

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_standard_layout_v<BufferView>);
    static_assert(std::is_trivially_copyable_v<BufferMut>);
    static_assert(std::is_standard_layout_v<BufferMut>);
    static_assert(std::is_trivially_copyable_v<FrameHeader>);
    static_assert(std::is_standard_layout_v<FrameHeader>);

} // namespace sarc::net
