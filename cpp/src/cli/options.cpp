#include "sarc/cli/options.hpp"

#include <charconv>
#include <cerrno>
#include <cstdlib>
#include <cstring>

namespace sarc::cli {
    namespace {
        [[nodiscard]] const OptionSpec* find_long(const OptionSpec* specs, u32 spec_count, const char* name) noexcept {
            if (name == nullptr) {
                return nullptr;
            }
            for (u32 i = 0; i < spec_count; ++i) {
                const OptionSpec& s = specs[i];
                if (s.long_name != nullptr && std::strcmp(s.long_name, name) == 0) {
                    return &s;
                }
            }
            return nullptr;
        }

        [[nodiscard]] const OptionSpec* find_short(const OptionSpec* specs, u32 spec_count, char c) noexcept {
            if (c == '\0') {
                return nullptr;
            }
            for (u32 i = 0; i < spec_count; ++i) {
                const OptionSpec& s = specs[i];
                if (s.short_name == c) {
                    return &s;
                }
            }
            return nullptr;
        }

        [[nodiscard]] bool parse_i64(const char* s, i64* out) noexcept {
            if (out == nullptr || s == nullptr) {
                return false;
            }
            const char* end = s + std::strlen(s);
            i64 v{};
            auto r = std::from_chars(s, end, v, 10);
            if (r.ec != std::errc() || r.ptr != end) {
                return false;
            }
            *out = v;
            return true;
        }

        [[nodiscard]] bool parse_f64(const char* s, double* out) noexcept {
            if (out == nullptr || s == nullptr) {
                return false;
            }
            errno = 0;
            char* end = nullptr;
            const double v = std::strtod(s, &end);
            if (end == s || end == nullptr || *end != '\0' || errno != 0) {
                return false;
            }
            *out = v;
            return true;
        }

        [[nodiscard]] sarc::core::Status push_option(ParsedOptions* out, const ParsedOption& opt) noexcept {
            if (out == nullptr) {
                return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
            }
            if (out->cap == 0 || out->data == nullptr) {
                return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
            }
            if (out->len >= out->cap) {
                return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
            }
            out->data[out->len++] = opt;
            return sarc::core::ok_status();
        }
    } // namespace

    sarc::core::Status parse_options(const CliArgs& args,
        const OptionSpec* specs,
        u32 spec_count,
        ParsedOptions* out,
        u32* consumed) noexcept {
        if (out == nullptr || consumed == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }
        *consumed = 0;
        out->len = 0;

        if (args.argc > 0 && args.argv == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }
        if (spec_count > 0 && specs == nullptr) {
            return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
        }

        u32 i = 0;
        while (i < args.argc) {
            const char* tok = args.argv[i];
            if (tok == nullptr) {
                break;
            }
            if (tok[0] != '-' || tok[1] == '\0') {
                break;
            }
            if (std::strcmp(tok, "--") == 0) {
                ++i;
                break;
            }

            if (tok[0] == '-' && tok[1] == '-') {
                const char* name = tok + 2;
                if (*name == '\0') {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }

                const char* value = nullptr;
                char name_buf[128]{};
                const char* eq = std::strchr(name, '=');
                if (eq != nullptr) {
                    const size_t name_len = static_cast<size_t>(eq - name);
                    if (name_len == 0 || name_len >= sizeof(name_buf)) {
                        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                    }
                    std::memcpy(name_buf, name, name_len);
                    name_buf[name_len] = '\0';
                    name = name_buf;
                    value = eq + 1;
                }

                const OptionSpec* spec = find_long(specs, spec_count, name);
                if (spec == nullptr) {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }

                ParsedOption opt{};
                opt.id = spec->id;
                opt.type = spec->type;
                if (spec->type == OptionType::Flag) {
                    if (value != nullptr) {
                        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                    }
                    opt.value.boolv = 1;
                    const sarc::core::Status s = push_option(out, opt);
                    if (!sarc::core::is_ok(s)) {
                        return s;
                    }
                    ++i;
                    continue;
                }

                if (value == nullptr) {
                    if (i + 1 >= args.argc || args.argv[i + 1] == nullptr) {
                        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                    }
                    value = args.argv[i + 1];
                    i += 2;
                } else {
                    ++i;
                }

                if (spec->type == OptionType::String) {
                    opt.value.str = value;
                } else if (spec->type == OptionType::I64) {
                    i64 v{};
                    if (!parse_i64(value, &v)) {
                        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                    }
                    opt.value.i64v = v;
                } else if (spec->type == OptionType::F64) {
                    double v{};
                    if (!parse_f64(value, &v)) {
                        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                    }
                    opt.value.f64v = v;
                } else {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }

                const sarc::core::Status s = push_option(out, opt);
                if (!sarc::core::is_ok(s)) {
                    return s;
                }
                continue;
            }

            const char short_name = tok[1];
            const OptionSpec* spec = find_short(specs, spec_count, short_name);
            if (spec == nullptr) {
                return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
            }

            ParsedOption opt{};
            opt.id = spec->id;
            opt.type = spec->type;
            const char* value = nullptr;
            if (spec->type == OptionType::Flag) {
                if (tok[2] != '\0') {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }
                opt.value.boolv = 1;
                const sarc::core::Status s = push_option(out, opt);
                if (!sarc::core::is_ok(s)) {
                    return s;
                }
                ++i;
                continue;
            }

            if (tok[2] != '\0') {
                value = tok + 2;
                ++i;
            } else {
                if (i + 1 >= args.argc || args.argv[i + 1] == nullptr) {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }
                value = args.argv[i + 1];
                i += 2;
            }

            if (spec->type == OptionType::String) {
                opt.value.str = value;
            } else if (spec->type == OptionType::I64) {
                i64 v{};
                if (!parse_i64(value, &v)) {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }
                opt.value.i64v = v;
            } else if (spec->type == OptionType::F64) {
                double v{};
                if (!parse_f64(value, &v)) {
                    return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
                }
                opt.value.f64v = v;
            } else {
                return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
            }

            const sarc::core::Status s = push_option(out, opt);
            if (!sarc::core::is_ok(s)) {
                return s;
            }
        }

        *consumed = i;
        return sarc::core::ok_status();
    }
} // namespace sarc::cli
