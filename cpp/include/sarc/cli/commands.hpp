#pragma once

#include <type_traits>

#include "sarc/cli/options.hpp"
#include "sarc/core/errors.hpp"

namespace sarc::cli {
    using u32 = sarc::core::u32;

    enum class CommandId : u32 {
        None = 0,
        Help = 1,
        Put = 2,
        Get = 3,
        List = 4,
        CreateZone = 5,
        ShareZone = 6,
    };

    struct CommandSpec {
        CommandId id{CommandId::None};
        const char* name{nullptr};
    };

    struct CommandInvocation {
        CommandId id{CommandId::None};
        CliArgs args{};
    };

    sarc::core::Status parse_command(const CliArgs& args,
        const CommandSpec* specs,
        u32 spec_count,
        CommandInvocation* out,
        u32* consumed) noexcept;

    static_assert(std::is_trivially_copyable_v<CommandSpec>);
    static_assert(std::is_trivially_copyable_v<CommandInvocation>);
    static_assert(std::is_standard_layout_v<CommandSpec>);
    static_assert(std::is_standard_layout_v<CommandInvocation>);

} // namespace sarc::cli
