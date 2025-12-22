#pragma once
#include <cstdint>
#include <type_traits>

namespace sarc::core {
    using u16 = std::uint16_t;
    using u32 = std::uint32_t;

    enum class StatusCode : u16 {
        Ok = 0,
        Unknown,
        Invalid,
        NotFound,
        PermissionDenied,
        Conflict,
        Busy,
        Corrupt,
        Io,
        Crypto,
        Network,
        Unsupported,
        Unavailable,
    };

    enum class StatusDomain : u16 {
        Core = 0,
        Storage,
        Db,
        Security,
        Net,
        Cli,
        Bindings,
        External,
    };

    struct Status {
        StatusCode code{StatusCode::Ok};
        StatusDomain domain{StatusDomain::Core};
        u32 aux{0};
    };

    [[nodiscard]] constexpr Status make_status(StatusDomain domain, StatusCode code, u32 aux = 0) noexcept {
        return Status{code, domain, aux};
    }

    [[nodiscard]] constexpr bool is_ok(Status s) noexcept {
        return s.code == StatusCode::Ok;
    }

    [[nodiscard]] constexpr Status ok_status() noexcept {
        return Status{};
    }

    static_assert(std::is_trivially_copyable_v<Status>);
    static_assert(std::is_standard_layout_v<Status>);
} // namespace sarc::core
