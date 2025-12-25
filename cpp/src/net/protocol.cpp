#include "sarc/net/protocol.hpp"

#include <cstddef>
#include <cstring>

namespace sarc::net {
    namespace {
        void put_u16_be(u8* p, u16 v) noexcept {
            p[0] = static_cast<u8>((v >> 8) & 0xffu);
            p[1] = static_cast<u8>((v >> 0) & 0xffu);
        }

        void put_u32_be(u8* p, u32 v) noexcept {
            p[0] = static_cast<u8>((v >> 24) & 0xffu);
            p[1] = static_cast<u8>((v >> 16) & 0xffu);
            p[2] = static_cast<u8>((v >> 8) & 0xffu);
            p[3] = static_cast<u8>((v >> 0) & 0xffu);
        }

        void put_u64_be(u8* p, sarc::core::u64 v) noexcept {
            p[0] = static_cast<u8>((v >> 56) & 0xffu);
            p[1] = static_cast<u8>((v >> 48) & 0xffu);
            p[2] = static_cast<u8>((v >> 40) & 0xffu);
            p[3] = static_cast<u8>((v >> 32) & 0xffu);
            p[4] = static_cast<u8>((v >> 24) & 0xffu);
            p[5] = static_cast<u8>((v >> 16) & 0xffu);
            p[6] = static_cast<u8>((v >> 8) & 0xffu);
            p[7] = static_cast<u8>((v >> 0) & 0xffu);
        }

        u16 get_u16_be(const u8* p) noexcept {
            return static_cast<u16>((static_cast<u16>(p[0]) << 8) | static_cast<u16>(p[1]));
        }

        u32 get_u32_be(const u8* p) noexcept {
            return (static_cast<u32>(p[0]) << 24) |
                   (static_cast<u32>(p[1]) << 16) |
                   (static_cast<u32>(p[2]) << 8) |
                   (static_cast<u32>(p[3]) << 0);
        }

        sarc::core::u64 get_u64_be(const u8* p) noexcept {
            return (static_cast<sarc::core::u64>(p[0]) << 56) |
                   (static_cast<sarc::core::u64>(p[1]) << 48) |
                   (static_cast<sarc::core::u64>(p[2]) << 40) |
                   (static_cast<sarc::core::u64>(p[3]) << 32) |
                   (static_cast<sarc::core::u64>(p[4]) << 24) |
                   (static_cast<sarc::core::u64>(p[5]) << 16) |
                   (static_cast<sarc::core::u64>(p[6]) << 8) |
                   (static_cast<sarc::core::u64>(p[7]) << 0);
        }

        bool msg_type_valid_u16(u16 v) noexcept {
            switch (static_cast<MsgType>(v)) {
            case MsgType::Hello:
            case MsgType::Ping:
            case MsgType::PutObject:
            case MsgType::GetObject:
            case MsgType::ListFiles:
            case MsgType::CreateZone:
            case MsgType::ShareZone:
                return true;
            default:
                return false;
            }
        }

        bool buffer_has(BufferView in, u32 need) noexcept {
            return in.data != nullptr && in.len >= need;
        }

        u32 write_capability(const sarc::security::Capability& cap, u8* out, u32 out_len) noexcept {
            if (out == nullptr || out_len < kCapabilityBytes) {
                return 0;
            }

            put_u32_be(out + 0, static_cast<u32>(cap.zone.v));
            put_u32_be(out + 4, static_cast<u32>(cap.grantee.v));
            put_u32_be(out + 8, cap.rights);
            put_u64_be(out + 12, static_cast<sarc::core::u64>(static_cast<sarc::core::i64>(cap.issued_at)));
            put_u64_be(out + 20, static_cast<sarc::core::u64>(static_cast<sarc::core::i64>(cap.expires_at)));
            put_u32_be(out + 28, cap.version);
            std::memcpy(out + 32, cap.proof.b, 16);

            return kCapabilityBytes;
        }

        ProtocolParseResult read_capability(BufferView in, sarc::security::Capability* out) noexcept {
            if (out == nullptr) return ProtocolParseResult::Invalid;
            if (!buffer_has(in, kCapabilityBytes)) return ProtocolParseResult::NeedMore;

            sarc::security::Capability cap{};
            cap.zone = sarc::core::ZoneId{get_u32_be(in.data + 0)};
            cap.grantee = sarc::core::UserId{get_u32_be(in.data + 4)};
            cap.rights = get_u32_be(in.data + 8);
            cap.issued_at = static_cast<sarc::core::i64>(get_u64_be(in.data + 12));
            cap.expires_at = static_cast<sarc::core::i64>(get_u64_be(in.data + 20));
            cap.version = get_u32_be(in.data + 28);
            std::memcpy(cap.proof.b, in.data + 32, 16);

            *out = cap;
            return ProtocolParseResult::Ok;
        }

        u32 write_object_key(const sarc::core::ObjectKey& k, u8* out, u32 out_len) noexcept {
            if (out == nullptr || out_len < kObjectKeyBytes) {
                return 0;
            }
            put_u32_be(out + 0, static_cast<u32>(k.zone.v));
            std::memcpy(out + 4, k.content.b.data(), 32);
            return kObjectKeyBytes;
        }

        ProtocolParseResult read_object_key(BufferView in, sarc::core::ObjectKey* out) noexcept {
            if (out == nullptr) return ProtocolParseResult::Invalid;
            if (!buffer_has(in, kObjectKeyBytes)) return ProtocolParseResult::NeedMore;

            sarc::core::ObjectKey k{};
            k.zone = sarc::core::ZoneId{get_u32_be(in.data + 0)};
            std::memcpy(k.content.b.data(), in.data + 4, 32);
            *out = k;
            return ProtocolParseResult::Ok;
        }
    } // namespace

    u32 protocol_write_request_header(const RequestHeader& h, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kRequestHeaderBytes) {
            return 0;
        }
        put_u16_be(out.data + 0, h.version);
        put_u16_be(out.data + 2, static_cast<u16>(h.type));
        put_u16_be(out.data + 4, h.flags);
        put_u16_be(out.data + 6, 0); // reserved
        put_u32_be(out.data + 8, h.request_id);
        put_u32_be(out.data + 12, static_cast<u32>(h.zone.v));
        return kRequestHeaderBytes;
    }

    ProtocolParseResult protocol_read_request_header(BufferView in, RequestHeader* out) noexcept {
        if (out == nullptr) return ProtocolParseResult::Invalid;
        if (in.data == nullptr) return ProtocolParseResult::NeedMore;
        if (in.len < kRequestHeaderBytes) return ProtocolParseResult::NeedMore;

        const u16 version = get_u16_be(in.data + 0);
        const u16 type_u16 = get_u16_be(in.data + 2);
        const u16 flags = get_u16_be(in.data + 4);
        const u16 reserved = get_u16_be(in.data + 6);
        const u32 request_id = get_u32_be(in.data + 8);
        const u32 zone = get_u32_be(in.data + 12);

        if (version != 1) return ProtocolParseResult::Invalid;
        if (reserved != 0) return ProtocolParseResult::Invalid;
        if (!msg_type_valid_u16(type_u16)) return ProtocolParseResult::Invalid;

        RequestHeader h{};
        h.version = version;
        h.type = static_cast<MsgType>(type_u16);
        h.flags = flags;
        h.request_id = request_id;
        h.zone = sarc::core::ZoneId{zone};
        *out = h;
        return ProtocolParseResult::Ok;
    }

    u32 protocol_write_response_header(const ResponseHeader& h, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kResponseHeaderBytes) {
            return 0;
        }
        put_u16_be(out.data + 0, h.version);
        put_u16_be(out.data + 2, static_cast<u16>(h.type));
        put_u16_be(out.data + 4, h.flags);
        put_u16_be(out.data + 6, 0); // reserved
        put_u32_be(out.data + 8, h.request_id);
        put_u16_be(out.data + 12, static_cast<u16>(h.status.code));
        put_u16_be(out.data + 14, static_cast<u16>(h.status.domain));
        put_u32_be(out.data + 16, h.status.aux);
        return kResponseHeaderBytes;
    }

    ProtocolParseResult protocol_read_response_header(BufferView in, ResponseHeader* out) noexcept {
        if (out == nullptr) return ProtocolParseResult::Invalid;
        if (in.data == nullptr) return ProtocolParseResult::NeedMore;
        if (in.len < kResponseHeaderBytes) return ProtocolParseResult::NeedMore;

        const u16 version = get_u16_be(in.data + 0);
        const u16 type_u16 = get_u16_be(in.data + 2);
        const u16 flags = get_u16_be(in.data + 4);
        const u16 reserved = get_u16_be(in.data + 6);
        const u32 request_id = get_u32_be(in.data + 8);
        const u16 code_u16 = get_u16_be(in.data + 12);
        const u16 domain_u16 = get_u16_be(in.data + 14);
        const u32 aux = get_u32_be(in.data + 16);

        if (version != 1) return ProtocolParseResult::Invalid;
        if (reserved != 0) return ProtocolParseResult::Invalid;
        if (!msg_type_valid_u16(type_u16)) return ProtocolParseResult::Invalid;

        ResponseHeader h{};
        h.version = version;
        h.type = static_cast<MsgType>(type_u16);
        h.flags = flags;
        h.request_id = request_id;
        h.status.code = static_cast<sarc::core::StatusCode>(code_u16);
        h.status.domain = static_cast<sarc::core::StatusDomain>(domain_u16);
        h.status.aux = aux;
        *out = h;
        return ProtocolParseResult::Ok;
    }

    u32 protocol_write_put_object_request_fixed(const PutObjectRequest& r, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kPutObjectRequestFixedBytes) {
            return 0;
        }

        u32 off = 0;
        const u32 h = protocol_write_request_header(r.header, {out.data + off, out.len - off});
        if (h == 0) return 0;
        off += h;

        const u32 cap = write_capability(r.cap, out.data + off, out.len - off);
        if (cap == 0) return 0;
        off += cap;

        const u32 key = write_object_key(r.object, out.data + off, out.len - off);
        if (key == 0) return 0;
        off += key;

        put_u64_be(out.data + off, r.size_bytes);
        off += 8;

        return off;
    }

    ProtocolParseResult protocol_read_put_object_request_fixed(BufferView in, PutObjectRequest* out) noexcept {
        if (out == nullptr) return ProtocolParseResult::Invalid;
        if (in.data == nullptr) return ProtocolParseResult::NeedMore;
        if (in.len < kPutObjectRequestFixedBytes) return ProtocolParseResult::NeedMore;

        u32 off = 0;
        PutObjectRequest r{};

        {
            RequestHeader h{};
            const auto pr = protocol_read_request_header({in.data + off, in.len - off}, &h);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.header = h;
            off += kRequestHeaderBytes;
        }

        {
            sarc::security::Capability cap{};
            const auto pr = read_capability({in.data + off, in.len - off}, &cap);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.cap = cap;
            off += kCapabilityBytes;
        }

        {
            sarc::core::ObjectKey k{};
            const auto pr = read_object_key({in.data + off, in.len - off}, &k);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.object = k;
            off += kObjectKeyBytes;
        }

        r.size_bytes = get_u64_be(in.data + off);
        off += 8;

        *out = r;
        return ProtocolParseResult::Ok;
    }

    u32 protocol_write_get_object_request_fixed(const GetObjectRequest& r, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kGetObjectRequestFixedBytes) {
            return 0;
        }

        u32 off = 0;
        const u32 h = protocol_write_request_header(r.header, {out.data + off, out.len - off});
        if (h == 0) return 0;
        off += h;

        const u32 cap = write_capability(r.cap, out.data + off, out.len - off);
        if (cap == 0) return 0;
        off += cap;

        const u32 key = write_object_key(r.object, out.data + off, out.len - off);
        if (key == 0) return 0;
        off += key;

        return off;
    }

    ProtocolParseResult protocol_read_get_object_request_fixed(BufferView in, GetObjectRequest* out) noexcept {
        if (out == nullptr) return ProtocolParseResult::Invalid;
        if (in.data == nullptr) return ProtocolParseResult::NeedMore;
        if (in.len < kGetObjectRequestFixedBytes) return ProtocolParseResult::NeedMore;

        u32 off = 0;
        GetObjectRequest r{};

        {
            RequestHeader h{};
            const auto pr = protocol_read_request_header({in.data + off, in.len - off}, &h);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.header = h;
            off += kRequestHeaderBytes;
        }

        {
            sarc::security::Capability cap{};
            const auto pr = read_capability({in.data + off, in.len - off}, &cap);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.cap = cap;
            off += kCapabilityBytes;
        }

        {
            sarc::core::ObjectKey k{};
            const auto pr = read_object_key({in.data + off, in.len - off}, &k);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.object = k;
            off += kObjectKeyBytes;
        }

        *out = r;
        return ProtocolParseResult::Ok;
    }

    u32 protocol_write_list_files_request_fixed(const ListFilesRequest& r, BufferMut out) noexcept {
        if (out.data == nullptr || out.len < kListFilesRequestFixedBytes) {
            return 0;
        }

        u32 off = 0;
        const u32 h = protocol_write_request_header(r.header, {out.data + off, out.len - off});
        if (h == 0) return 0;
        off += h;

        const u32 cap = write_capability(r.cap, out.data + off, out.len - off);
        if (cap == 0) return 0;
        off += cap;

        return off;
    }

    ProtocolParseResult protocol_read_list_files_request_fixed(BufferView in, ListFilesRequest* out) noexcept {
        if (out == nullptr) return ProtocolParseResult::Invalid;
        if (in.data == nullptr) return ProtocolParseResult::NeedMore;
        if (in.len < kListFilesRequestFixedBytes) return ProtocolParseResult::NeedMore;

        u32 off = 0;
        ListFilesRequest r{};

        {
            RequestHeader h{};
            const auto pr = protocol_read_request_header({in.data + off, in.len - off}, &h);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.header = h;
            off += kRequestHeaderBytes;
        }

        {
            sarc::security::Capability cap{};
            const auto pr = read_capability({in.data + off, in.len - off}, &cap);
            if (pr != ProtocolParseResult::Ok) return pr;
            r.cap = cap;
            off += kCapabilityBytes;
        }

        *out = r;
        return ProtocolParseResult::Ok;
    }
} // namespace sarc::net
