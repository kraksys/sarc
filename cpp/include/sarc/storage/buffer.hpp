#pragma once

#include <type_traits>

#include "sarc/core/types.hpp"

namespace sarc::storage {
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

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_standard_layout_v<BufferView>);
    static_assert(std::is_trivially_copyable_v<BufferMut>);
    static_assert(std::is_standard_layout_v<BufferMut>);
} // namespace sarc::storage

