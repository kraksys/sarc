#include <array>

#include <gtest/gtest.h>

#include "sarc/net/framing.hpp"

TEST(NetFraming, HeaderRoundTrip) {
    sarc::net::FrameHeader in{};
    in.version = 1;
    in.type = sarc::net::FrameType::Response;
    in.flags = 0x1234;
    in.payload_len = 0x01020304;
    in.request_id = 0xaabbccdd;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};
    const sarc::net::u32 written = sarc::net::frame_write_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())});
    ASSERT_EQ(written, sarc::net::kFrameHeaderBytes);

    sarc::net::FrameHeader out{};
    const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
    ASSERT_EQ(r, sarc::net::FrameParseResult::Ok);

    EXPECT_EQ(out.version, in.version);
    EXPECT_EQ(out.type, in.type);
    EXPECT_EQ(out.flags, in.flags);
    EXPECT_EQ(out.payload_len, in.payload_len);
    EXPECT_EQ(out.request_id, in.request_id);
}

TEST(NetFraming, NeedMoreWhenShort){
    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes - 1> buf{};
    sarc::net::FrameHeader out{};
    const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
    EXPECT_EQ(r, sarc::net::FrameParseResult::NeedMore);
}

TEST(NetFraming, InvalidWhenReservedNonZero){
    sarc::net::FrameHeader in{};
    in.version = 1;
    in.type = sarc::net::FrameType::Request;
    in.flags = 0;
    in.payload_len = 0;
    in.request_id = 1;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::frame_write_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}), sarc::net::kFrameHeaderBytes);

    // reserved field is bytes 6-7
    buf[6] = 0x12;
    buf[7] = 0x34;

    sarc::net::FrameHeader out{};
    const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
    EXPECT_EQ(r, sarc::net::FrameParseResult::Invalid);
}

TEST(NetFraming, InvalidWhenUnknownType){
    sarc::net::FrameHeader in{};
    in.version = 1;
    in.type = sarc::net::FrameType::Request;
    in.flags = 0;
    in.payload_len = 0;
    in.request_id = 1;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::frame_write_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}), sarc::net::kFrameHeaderBytes);

    // type field is bytes 2-3; set to 0 => invalid
    buf[2] = 0;
    buf[3] = 0;

    sarc::net::FrameHeader out{};
    const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
    EXPECT_EQ(r, sarc::net::FrameParseResult::Invalid);
}

TEST(NetFraming, InvalidWhenVersionNotOne){
    sarc::net::FrameHeader in{};
    in.version = 1;
    in.type = sarc::net::FrameType::Request;

    std::array<sarc::net::u8, sarc::net::kFrameHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::frame_write_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}), sarc::net::kFrameHeaderBytes);

    // version field is bytes 0-1; set to 2
    buf[0] = 0;
    buf[1] = 2;

    sarc::net::FrameHeader out{};
    const sarc::net::FrameParseResult r = sarc::net::frame_read_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out);
    EXPECT_EQ(r, sarc::net::FrameParseResult::Invalid);
}
