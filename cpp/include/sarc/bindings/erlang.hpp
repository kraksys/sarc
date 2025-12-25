#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/core/types.hpp"

namespace sarc::bindings::erlang {
    using u8 = sarc::core::u8;
    using u32 = sarc::core::u32;

    struct BufferView {
        const u8* data{nullptr};
        u32 len{0};
    };

    struct BufferMut {
        u8* data{nullptr};
        u32 len{0};
    };

    struct ErlPid {
        u32 node{0};
        u32 id{0};
        u32 serial{0};
        u32 creation{0};
    };

    struct ErlTerm {
        BufferView bytes{};
    };

    struct ErlMessage {
        ErlPid from{};
        ErlTerm term{};
    };

    sarc::core::Status decode_zone(const ErlTerm& term, sarc::core::Zone* out) noexcept;
    sarc::core::Status encode_zone(const sarc::core::Zone& zone, BufferMut out, u32* written) noexcept;

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_trivially_copyable_v<BufferMut>);
    static_assert(std::is_trivially_copyable_v<ErlPid>);
    static_assert(std::is_trivially_copyable_v<ErlTerm>);
    static_assert(std::is_trivially_copyable_v<ErlMessage>);
    static_assert(std::is_standard_layout_v<BufferView>);
    static_assert(std::is_standard_layout_v<BufferMut>);
    static_assert(std::is_standard_layout_v<ErlPid>);
    static_assert(std::is_standard_layout_v<ErlTerm>);
    static_assert(std::is_standard_layout_v<ErlMessage>);

} // namespace sarc::bindings::erlang
