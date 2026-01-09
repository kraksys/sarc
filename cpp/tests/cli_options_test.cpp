#include <array>

#include <gtest/gtest.h>

#include "sarc/cli/options.hpp"

TEST(CliOptions, ParsesLongAndShortAndStopsAtCommand) {
    const std::array<sarc::cli::OptionSpec, 4> specs = {{
        {sarc::cli::OptionId::Zone, sarc::cli::OptionType::String, "zone", 'z'},
        {sarc::cli::OptionId::Output, sarc::cli::OptionType::String, "output", 'o'},
        {sarc::cli::OptionId::User, sarc::cli::OptionType::I64, "user", 'u'},
        {sarc::cli::OptionId::None, sarc::cli::OptionType::Flag, "verbose", 'v'},
    }};

    const char* argv[] = {"--verbose", "--zone", "z1", "-o", "out.bin", "put", "file.txt"};
    const sarc::cli::CliArgs args{argv, 7};

    sarc::cli::ParsedOption buf[8]{};
    sarc::cli::ParsedOptions out{buf, 0, 8};
    sarc::cli::u32 consumed = 0;
    const sarc::core::Status s = sarc::cli::parse_options(args, specs.data(), specs.size(), &out, &consumed);
    ASSERT_EQ(s.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(consumed, 5u);
    ASSERT_EQ(out.len, 3u);

    EXPECT_EQ(out.data[0].type, sarc::cli::OptionType::Flag);
    EXPECT_EQ(out.data[0].value.boolv, 1);

    EXPECT_EQ(out.data[1].id, sarc::cli::OptionId::Zone);
    EXPECT_STREQ(out.data[1].value.str, "z1");

    EXPECT_EQ(out.data[2].id, sarc::cli::OptionId::Output);
    EXPECT_STREQ(out.data[2].value.str, "out.bin");
}

TEST(CliOptions, SupportsEqualsAndAttachedValue) {
    const std::array<sarc::cli::OptionSpec, 2> specs = {{
        {sarc::cli::OptionId::Zone, sarc::cli::OptionType::String, "zone", 'z'},
        {sarc::cli::OptionId::User, sarc::cli::OptionType::I64, "user", 'u'},
    }};

    const char* argv[] = {"--zone=abc", "-u123"};
    const sarc::cli::CliArgs args{argv, 2};

    sarc::cli::ParsedOption buf[8]{};
    sarc::cli::ParsedOptions out{buf, 0, 8};
    sarc::cli::u32 consumed = 0;
    const sarc::core::Status s = sarc::cli::parse_options(args, specs.data(), specs.size(), &out, &consumed);
    ASSERT_EQ(s.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(consumed, 2u);
    ASSERT_EQ(out.len, 2u);
    EXPECT_STREQ(out.data[0].value.str, "abc");
    EXPECT_EQ(out.data[1].value.i64v, 123);
}

TEST(CliOptions, StopsAtDoubleDash) {
    const std::array<sarc::cli::OptionSpec, 2> specs = {{
        {sarc::cli::OptionId::Zone, sarc::cli::OptionType::String, "zone", 'z'},
        {sarc::cli::OptionId::None, sarc::cli::OptionType::Flag, "verbose", 'v'},
    }};

    const char* argv[] = {"--zone", "1", "--", "--verbose"};
    const sarc::cli::CliArgs args{argv, 4};

    sarc::cli::ParsedOption buf[8]{};
    sarc::cli::ParsedOptions out{buf, 0, 8};
    sarc::cli::u32 consumed = 0;
    const sarc::core::Status s = sarc::cli::parse_options(args, specs.data(), specs.size(), &out, &consumed);
    ASSERT_EQ(s.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(consumed, 3u);
    ASSERT_EQ(out.len, 1u);
    EXPECT_STREQ(out.data[0].value.str, "1");
}

TEST(CliOptions, InvalidOnUnknownOrMissingValue) {
    const std::array<sarc::cli::OptionSpec, 1> specs = {{
        {sarc::cli::OptionId::Zone, sarc::cli::OptionType::String, "zone", 'z'},
    }};

    {
        const char* argv[] = {"--nope"};
        sarc::cli::ParsedOption buf[2]{};
        sarc::cli::ParsedOptions out{buf, 0, 2};
        sarc::cli::u32 consumed = 0;
        const sarc::core::Status s =
            sarc::cli::parse_options({argv, 1}, specs.data(), specs.size(), &out, &consumed);
        EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
    }
    {
        const char* argv[] = {"--zone"};
        sarc::cli::ParsedOption buf[2]{};
        sarc::cli::ParsedOptions out{buf, 0, 2};
        sarc::cli::u32 consumed = 0;
        const sarc::core::Status s =
            sarc::cli::parse_options({argv, 1}, specs.data(), specs.size(), &out, &consumed);
        EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
    }
}

