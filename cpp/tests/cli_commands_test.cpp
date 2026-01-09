#include <array>

#include <gtest/gtest.h>

#include "sarc/cli/commands.hpp"

TEST(CliCommands, ParsesKnownCommandAndReturnsRemainingArgs) {
    const std::array<sarc::cli::CommandSpec, 3> specs = {{
        {sarc::cli::CommandId::Help, "help"},
        {sarc::cli::CommandId::Put, "put"},
        {sarc::cli::CommandId::Get, "get"},
    }};

    const char* argv[] = {"put", "file.txt", "--zone", "z1"};
    const sarc::cli::CliArgs args{argv, 4};

    sarc::cli::CommandInvocation out{};
    sarc::cli::u32 consumed = 0;
    const sarc::core::Status s = sarc::cli::parse_command(args, specs.data(), specs.size(), &out, &consumed);
    ASSERT_EQ(s.code, sarc::core::StatusCode::Ok);
    EXPECT_EQ(consumed, 1u);
    EXPECT_EQ(out.id, sarc::cli::CommandId::Put);
    ASSERT_EQ(out.args.argc, 3u);
    EXPECT_STREQ(out.args.argv[0], "file.txt");
}

TEST(CliCommands, InvalidOnUnknownCommand) {
    const std::array<sarc::cli::CommandSpec, 1> specs = {{{sarc::cli::CommandId::Help, "help"}}};
    const char* argv[] = {"nope"};
    sarc::cli::CommandInvocation out{};
    sarc::cli::u32 consumed = 0;
    const sarc::core::Status s = sarc::cli::parse_command({argv, 1}, specs.data(), specs.size(), &out, &consumed);
    EXPECT_EQ(s.code, sarc::core::StatusCode::Invalid);
}

