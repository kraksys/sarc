#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"

namespace sarc::cli {
    using u8 = sarc::core::u8;
    using u32 = sarc::core::u32;
    using i64 = sarc::core::i64;

    struct CliArgs {
        const char* const* argv{nullptr};
        u32 argc{0};
    };

    enum class OptionType : u8 {
        Flag = 0,
        String = 1,
        I64 = 2,
        F64 = 3,
    };

    enum class OptionId : u32 {
        None = 0,
        Zone = 1,
        File = 2,
        User = 3,
        Peer = 4,
        Output = 5,
    };

    struct OptionSpec {
        OptionId id{OptionId::None};
        OptionType type{OptionType::Flag};
        const char* long_name{nullptr};
        char short_name{'\0'};
    };

    union OptionValue {
        const char* str;
        i64 i64v;
        double f64v;
        u8 boolv;
    };

    struct ParsedOption {
        OptionId id{OptionId::None};
        OptionType type{OptionType::Flag};
        OptionValue value{};
    };

    struct ParsedOptions {
        ParsedOption* data{nullptr};
        u32 len{0};
        u32 cap{0};
    };

    sarc::core::Status parse_options(const CliArgs& args,
        const OptionSpec* specs,
        u32 spec_count,
        ParsedOptions* out,
        u32* consumed) noexcept;

    static_assert(std::is_trivially_copyable_v<CliArgs>);
    static_assert(std::is_trivially_copyable_v<OptionSpec>);
    static_assert(std::is_trivially_copyable_v<ParsedOption>);
    static_assert(std::is_trivially_copyable_v<ParsedOptions>);
    static_assert(std::is_standard_layout_v<CliArgs>);
    static_assert(std::is_standard_layout_v<OptionSpec>);
    static_assert(std::is_standard_layout_v<ParsedOption>);
    static_assert(std::is_standard_layout_v<ParsedOptions>);

} // namespace sarc::cli
