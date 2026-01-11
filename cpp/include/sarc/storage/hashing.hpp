#pragma once

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/storage/buffer.hpp"
#include <blake3.h>

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

    class Hasher {
    public:
        Hasher() noexcept {
            blake3_hasher_init(&state_);
        }

        void update(BufferView data) noexcept {
            if (data.len > 0 && data.data != nullptr) {
                blake3_hasher_update(&state_, data.data, data.len);
            }
        }

        void finalize(sarc::core::Hash256* out) noexcept {
            if (out) {
                blake3_hasher_finalize(&state_, out->b.data(), out->b.size());
            }
        }

    private:
        blake3_hasher state_;
    };

} // namespace sarc::storage
