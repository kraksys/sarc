#pragma once

#include <type_traits>

#include "sarc/core/types.hpp"

namespace sarc::core {

    struct ZoneRecord {
        ZoneId id{ZoneId::invalid()};
        ZoneType type{ZoneType::Universal};
        UserId owner{UserId::invalid()};
        CategoryKey category{};
        StringId name{StringId::invalid()};
        Timestamp created_at{0};
        Timestamp updated_at{0};
    };

    static_assert(std::is_trivially_copyable_v<ZoneRecord>);
    static_assert(std::is_standard_layout_v<ZoneRecord>);

} // namespace sarc::core
