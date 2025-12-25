#include "sarc/storage/hashing.hpp"

#include <cstddef>

#include <blake3.h>

namespace sarc::storage {
    sarc::core::Status hash_compute(BufferView data, sarc::core::Hash256* out) noexcept {
        if (out == nullptr){
            return sarc::core::make_status(sarc::core::StatusDomain::Storage, sarc::core::StatusCode::Invalid);
        }
        if (data.len > 0 && data.data == nullptr){
            return sarc::core::make_status(sarc::core::StatusDomain::Storage, sarc::core::StatusCode::Invalid);
        }

        blake3_hasher hasher;
        blake3_hasher_init(&hasher);

        if (data.len > 0){
            blake3_hasher_update(&hasher, data.data, static_cast<size_t>(data.len));
        }

        blake3_hasher_finalize(&hasher, out->b.data(), out->b.size());
        return sarc::core::ok_status();
    }
} // namespace sarc::storage
