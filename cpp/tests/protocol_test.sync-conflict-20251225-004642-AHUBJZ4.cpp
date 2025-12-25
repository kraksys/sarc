#include <array>

#include <gtest/gtest.h>

#include "sarc/net/protocol.hpp"

TEST(NetProtocol, RequestHeaderRoundTrip) {
    sarc::net::RequestHeader in{};
    in.version = 1;
    in.type = sarc::net::MsgType::ListFiles;
    in.flags = 0x1234;
    in.request_id = 0xaabbccdd;
    in.zone = sarc::core::ZoneId{42};

    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::protocol_write_request_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}),
              sarc::net::kRequestHeaderBytes);

    sarc::net::RequestHeader out{};
    ASSERT_EQ(sarc::net::protocol_read_request_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out),
              sarc::net::ProtocolParseResult::Ok);

    EXPECT_EQ(out.version, in.version);
    EXPECT_EQ(out.type, in.type);
    EXPECT_EQ(out.flags, in.flags);
    EXPECT_EQ(out.request_id, in.request_id);
    EXPECT_EQ(out.zone.v, in.zone.v);
}

TEST(NetProtocol, RequestHeaderNeedMoreWhenShort) {
    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes - 1> buf{};
    sarc::net::RequestHeader out{};
    EXPECT_EQ(sarc::net::protocol_read_request_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out),
              sarc::net::ProtocolParseResult::NeedMore);
}

TEST(NetProtocol, RequestHeaderInvalidWhenReservedNonZero) {
    sarc::net::RequestHeader in{};
    in.version = 1;
    in.type = sarc::net::MsgType::Ping;

    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::protocol_write_request_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}),
              sarc::net::kRequestHeaderBytes);

    buf[6] = 0x12;
    buf[7] = 0x34;

    sarc::net::RequestHeader out{};
    EXPECT_EQ(sarc::net::protocol_read_request_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out),
              sarc::net::ProtocolParseResult::Invalid);
}

TEST(NetProtocol, RequestHeaderInvalidWhenVersionNotOne) {
    sarc::net::RequestHeader in{};
    in.version = 1;
    in.type = sarc::net::MsgType::Ping;

    std::array<sarc::net::u8, sarc::net::kRequestHeaderBytes> buf{};
    ASSERT_EQ(sarc::net::protocol_write_request_header(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}),
              sarc::net::kRequestHeaderBytes);

    buf[0] = 0;
    buf[1] = 2;

    sarc::net::RequestHeader out{};
    EXPECT_EQ(sarc::net::protocol_read_request_header({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out),
              sarc::net::ProtocolParseResult::Invalid);
}

TEST(NetProtocol, PutObjectFixedRoundTrip) {
    sarc::net::PutObjectRequest in{};
    in.header.version = 1;
    in.header.type = sarc::net::MsgType::PutObject;
    in.header.flags = 0;
    in.header.request_id = 123;
    in.header.zone = sarc::core::ZoneId{42};

    in.cap.zone = sarc::core::ZoneId{42};
    in.cap.grantee = sarc::core::UserId{7};
    in.cap.rights = 0x01020304;
    in.cap.issued_at = 100;
    in.cap.expires_at = 200;
    in.cap.version = 1;
    for (size_t i = 0; i < 16; ++i) {
        in.cap.proof.b[i] = static_cast<sarc::core::u8>(i);
    }

    in.object.zone = sarc::core::ZoneId{42};
    for (size_t i = 0; i < 32; ++i) {
        in.object.content.b[i] = static_cast<sarc::core::u8>(0xa0u + static_cast<sarc::core::u8>(i));
    }
    in.size_bytes = 0x0102030405060708ull;

    std::array<sarc::net::u8, sarc::net::kPutObjectRequestFixedBytes> buf{};
    ASSERT_EQ(sarc::net::protocol_write_put_object_request_fixed(in, {buf.data(), static_cast<sarc::net::u32>(buf.size())}),
              sarc::net::kPutObjectRequestFixedBytes);

    sarc::net::PutObjectRequest out{};
    ASSERT_EQ(sarc::net::protocol_read_put_object_request_fixed({buf.data(), static_cast<sarc::net::u32>(buf.size())}, &out),
              sarc::net::ProtocolParseResult::Ok);

    EXPECT_EQ(out.header.type, in.header.type);
    EXPECT_EQ(out.header.request_id, in.header.request_id);
    EXPECT_EQ(out.header.zone.v, in.header.zone.v);

    EXPECT_EQ(out.cap.zone.v, in.cap.zone.v);
    EXPECT_EQ(out.cap.grantee.v, in.cap.grantee.v);
    EXPECT_EQ(out.cap.rights, in.cap.rights);
    EXPECT_EQ(out.cap.issued_at, in.cap.issued_at);
    EXPECT_EQ(out.cap.expires_at, in.cap.expires_at);
    EXPECT_EQ(out.cap.version, in.cap.version);
    EXPECT_EQ(0, std::memcmp(out.cap.proof.b, in.cap.proof.b, 16));

    EXPECT_EQ(out.object.zone.v, in.object.zone.v);
    EXPECT_EQ(out.object.content.b, in.object.content.b);
    EXPECT_EQ(out.size_bytes, in.size_bytes);
}

