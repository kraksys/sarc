#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"

namespace sarc::bindings::http {
    using u8 = sarc::core::u8;
    using u16 = sarc::core::u16;
    using u32 = sarc::core::u32;

    struct BufferView {
        const u8* data{nullptr};
        u32 len{0};
    };

    struct HttpHeader {
        BufferView name{};
        BufferView value{};
    };

    struct HttpRequest {
        BufferView method{};
        BufferView path{};
        BufferView body{};
        const HttpHeader* headers{nullptr};
        u32 header_count{0};
    };

    struct HttpResponse {
        u16 status{200};
        BufferView body{};
    };

    sarc::core::Status handle_http_request(const HttpRequest& req, HttpResponse* out) noexcept;

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_trivially_copyable_v<HttpHeader>);
    static_assert(std::is_trivially_copyable_v<HttpRequest>);
    static_assert(std::is_trivially_copyable_v<HttpResponse>);
    static_assert(std::is_standard_layout_v<BufferView>);
    static_assert(std::is_standard_layout_v<HttpHeader>);
    static_assert(std::is_standard_layout_v<HttpRequest>);
    static_assert(std::is_standard_layout_v<HttpResponse>);

} // namespace sarc::bindings::http
