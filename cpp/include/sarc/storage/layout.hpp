#pragma once

#include <type_traits>

#include "sarc/core/types.hpp"

namespace sarc::storage {
    using u32 = sarc::core::u32;
    using u64 = sarc::core::u64;

    constexpr u32 kLayoutVersion = 1;

    struct ObjectHeader {
        u32 version{kLayoutVersion};
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::Hash256 content{};
        u64 size_bytes{0};
        u32 flags{0};
        u32 reserved{0};
    };

    struct IndexEntry {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::Hash256 content{};
        u64 offset{0};
        u64 size_bytes{0};
    };

    static_assert(std::is_trivially_copyable_v<ObjectHeader>);
    static_assert(std::is_trivially_copyable_v<IndexEntry>);
    static_assert(std::is_standard_layout_v<ObjectHeader>);
    static_assert(std::is_standard_layout_v<IndexEntry>);

} // namespace sarc::storage
