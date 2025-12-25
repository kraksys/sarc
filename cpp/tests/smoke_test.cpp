#include <gtest/gtest.h>

#include "sarc/core/errors.hpp"
#include "sarc/net/framing.hpp"

TEST(Status, DefaultIsOk){
    sarc::core::Status s{};
    EXPECT_EQ(s.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(s.domain, sarc::core::StatusDomain::Core);
    EXPECT_EQ(s.aux, 0u);
}

TEST(NetFraming, FrameHeaderValid){
    sarc::net::FrameHeader h{};
    h.version = 1;
    h.payload_len = 16;
    EXPECT_TRUE(sarc::net::frame_header_valid(h, 1024));

    h.payload_len = 2048;
    EXPECT_FALSE(sarc::net::frame_header_valid(h, 1024));

    h.version = 2;
    h.payload_len = 16;
    EXPECT_FALSE(sarc::net::frame_header_valid(h, 1024));
}
