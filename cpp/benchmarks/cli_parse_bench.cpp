#include <array>

#include <benchmark/benchmark.h>

#include "sarc/cli/commands.hpp"
#include "sarc/cli/options.hpp"

static void BM_CliParseOptions(benchmark::State& state) {
    const std::array<sarc::cli::OptionSpec, 4> specs = {{
        {sarc::cli::OptionId::Zone, sarc::cli::OptionType::String, "zone", 'z'},
        {sarc::cli::OptionId::Output, sarc::cli::OptionType::String, "output", 'o'},
        {sarc::cli::OptionId::User, sarc::cli::OptionType::I64, "user", 'u'},
        {sarc::cli::OptionId::None, sarc::cli::OptionType::Flag, "verbose", 'v'},
    }};

    const char* argv[] = {"--verbose", "--zone", "z1", "--user=123", "-oout.bin", "--", "put"};
    const sarc::cli::CliArgs args{argv, 7};
    for (auto _ : state) {
        sarc::cli::ParsedOption buf[8]{};
        sarc::cli::ParsedOptions out{buf, 0, 8};
        sarc::cli::u32 consumed = 0;
        const sarc::core::Status s = sarc::cli::parse_options(args, specs.data(), specs.size(), &out, &consumed);
        benchmark::DoNotOptimize(static_cast<sarc::core::u16>(s.code));
        benchmark::DoNotOptimize(out.len);
        benchmark::DoNotOptimize(consumed);
    }
}
BENCHMARK(BM_CliParseOptions);

static void BM_CliParseCommand(benchmark::State& state) {
    const std::array<sarc::cli::CommandSpec, 4> specs = {{
        {sarc::cli::CommandId::Help, "help"},
        {sarc::cli::CommandId::Put, "put"},
        {sarc::cli::CommandId::Get, "get"},
        {sarc::cli::CommandId::List, "list"},
    }};

    const char* argv[] = {"put", "file.txt", "--zone", "z1"};
    const sarc::cli::CliArgs args{argv, 4};
    for (auto _ : state) {
        sarc::cli::CommandInvocation out{};
        sarc::cli::u32 consumed = 0;
        const sarc::core::Status s = sarc::cli::parse_command(args, specs.data(), specs.size(), &out, &consumed);
        benchmark::DoNotOptimize(static_cast<sarc::core::u16>(s.code));
        benchmark::DoNotOptimize(static_cast<sarc::cli::u32>(out.id));
        benchmark::DoNotOptimize(consumed);
    }
}
BENCHMARK(BM_CliParseCommand);

