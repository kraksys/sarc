#pragma once

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/storage/buffer.hpp"

namespace sarc::storage {
    [[nodiscard]] constexpr bool hash_is_zero(const sarc::core::Hash256& h) noexcept {
        for (u8 b : h.b) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    sarc::core::Status hash_compute(BufferView data, sarc::core::Hash256* out) noexcept;

} // namespace sarc::storage
