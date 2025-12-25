#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/storage/buffer.hpp"
#include "sarc/storage/layout.hpp"

namespace sarc::storage {
    struct PutResult {
        sarc::core::ObjectKey key{};
        sarc::core::ObjectMeta meta{};
    };

    struct GetResult {
        sarc::core::ObjectMeta meta{};
        u32 bytes_written{0};
    };

    struct StorageConfig {
        u32 max_object_bytes{0};
        u32 max_inline_bytes{0};
    };

    sarc::core::Status storage_put(const StorageConfig& cfg, const sarc::core::ObjectKey& key, BufferView data, PutResult* out) noexcept;

    sarc::core::Status storage_get(const StorageConfig& cfg,
        const sarc::core::ObjectKey& key,
        BufferMut out,
        GetResult* result) noexcept;


    sarc::core::Status storage_has(const sarc::core::ObjectKey& key, bool* out_exists) noexcept;

    static_assert(std::is_trivially_copyable_v<PutResult>);
    static_assert(std::is_trivially_copyable_v<GetResult>);

    static_assert(std::is_trivially_copyable_v<StorageConfig>);
    static_assert(std::is_standard_layout_v<PutResult>);
    static_assert(std::is_standard_layout_v<GetResult>);
    static_assert(std::is_standard_layout_v<StorageConfig>);

} // namespace sarc::storage
