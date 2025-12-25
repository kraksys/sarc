#pragma once

#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/core/types.hpp"
#include "sarc/net/framing.hpp"
#include "sarc/security/policy.hpp"

namespace sarc::net {
    using u8 = sarc::core::u8;
    using u16 = sarc::core::u16;
    using u32 = sarc::core::u32;

    enum class MsgType : u16 {
        Hello = 1,
        Ping = 2,
        PutObject = 3,
        GetObject = 4,
        ListFiles = 5,
        CreateZone = 6,
        ShareZone = 7,
    };

    struct RequestHeader {
        u16 version{1};
        MsgType type{MsgType::Hello};
        u16 flags{0};
        u32 request_id{0};
        sarc::core::ZoneId zone{sarc::core::ZoneId::invalid()};
    };

    struct ResponseHeader {
        u16 version{1};
        MsgType type{MsgType::Hello};
        u16 flags{0};
        u32 request_id{0};
        sarc::core::Status status{};
    };

    struct PutObjectRequest {
        RequestHeader header{};
        sarc::security::Capability cap{};
        sarc::core::ObjectKey object{};
        sarc::core::u64 size_bytes{0};
    };

    struct GetObjectRequest {
        RequestHeader header{};
        sarc::security::Capability cap{};
        sarc::core::ObjectKey object{};
    };

    struct ListFilesRequest {
        RequestHeader header{};
        sarc::security::Capability cap{};
    };

    struct CreateZoneRequest {
        RequestHeader header{};
        sarc::security::Capability cap{};
        sarc::core::ZoneSpec spec{};
        sarc::core::StringId name { sarc::core::StringId::invalid() };
    };

    struct ShareZoneRequest{
        RequestHeader header{};
        sarc::security::Capability cap{};
        sarc::core::UserId grantee{ sarc::core::UserId::invalid() };
        u32 rights{0};
    };

    inline constexpr u32 kRequestHeaderBytes = 16;
    inline constexpr u32 kResponseHeaderBytes = 20;
    inline constexpr u32 kCapabilityBytes = 48;
    inline constexpr u32 kObjectKeyBytes = 36;

    inline constexpr u32 kPutObjectRequestFixedBytes = kRequestHeaderBytes + kCapabilityBytes + kObjectKeyBytes + 8;
    inline constexpr u32 kGetObjectRequestFixedBytes = kRequestHeaderBytes + kCapabilityBytes + kObjectKeyBytes;
    inline constexpr u32 kListFilesRequestFixedBytes = kRequestHeaderBytes + kCapabilityBytes;
    inline constexpr u32 kCreateZoneRequestFixedBytes = kRequestHeaderBytes + kCapabilityBytes + 16 + 4;
    inline constexpr u32 kShareZoneRequestFixedBytes = kRequestHeaderBytes + kCapabilityBytes + 4 + 4;

    enum class ProtocolParseResult : u8 {
        Ok = 0,
        NeedMore,
        Invalid,
    };

    [[nodiscard]] u32 protocol_write_request_header(const RequestHeader& h, BufferMut out) noexcept;
    [[nodiscard]] ProtocolParseResult protocol_read_request_header(BufferView in, RequestHeader* out) noexcept;

    [[nodiscard]] u32 protocol_write_response_header(const ResponseHeader& h, BufferMut out) noexcept;
    [[nodiscard]] ProtocolParseResult protocol_read_response_header(BufferView in, ResponseHeader* out) noexcept;

    [[nodiscard]] u32 protocol_write_put_object_request_fixed(const PutObjectRequest& r, BufferMut out) noexcept;
    [[nodiscard]] ProtocolParseResult protocol_read_put_object_request_fixed(BufferView in, PutObjectRequest* out) noexcept;

    [[nodiscard]] u32 protocol_write_get_object_request_fixed(const GetObjectRequest& r, BufferMut out) noexcept;
    [[nodiscard]] ProtocolParseResult protocol_read_get_object_request_fixed(BufferView in, GetObjectRequest* out) noexcept;

    [[nodiscard]] u32 protocol_write_list_files_request_fixed(const ListFilesRequest& r, BufferMut out) noexcept;
    [[nodiscard]] ProtocolParseResult protocol_read_list_files_request_fixed(BufferView in, ListFilesRequest* out) noexcept;

    static_assert(std::is_trivially_copyable_v<RequestHeader>);
    static_assert(std::is_trivially_copyable_v<ResponseHeader>);
    static_assert(std::is_standard_layout_v<RequestHeader>);
    static_assert(std::is_standard_layout_v<ResponseHeader>);
} // namespace sarc::net
