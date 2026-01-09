#include "sarc/cli/commands.hpp"

#include <cstring>

namespace sarc::cli {
    sarc::core::Status parse_command(const CliArgs& args,
        const CommandSpec* specs,
        u32 spec_count,
        CommandInvocation* out,
        u32* consumed) noexcept {
        if (out == nullptr || consumed == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }
        *consumed = 0;
        out->id = CommandId::None;
        out->args = CliArgs{};

        if (args.argc == 0) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }
        if (args.argv == nullptr || args.argv[0] == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }
        if (spec_count > 0 && specs == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }

        const char* cmd = args.argv[0];
        if (cmd[0] == '-') {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }

        const CommandSpec* match = nullptr;
        for (u32 i = 0; i < spec_count; ++i) {
            const CommandSpec& s = specs[i];
            if (s.name != nullptr && std::strcmp(s.name, cmd) == 0) {
                match = &s;
                break;
            }
        }
        if (match == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }

        out->id = match->id;
        out->args.argv = args.argv + 1;
        out->args.argc = args.argc - 1;
        *consumed = 1;
        return sarc::core::ok_status();
    }
} // namespace sarc::cli
