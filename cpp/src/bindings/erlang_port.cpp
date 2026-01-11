// Erlang Port driver for SARC object store (WRITE operations)
// Provides isolated process for object_put, object_delete, and object_gc
// Communicates via stdin/stdout using Erlang external term format

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>
#include <unordered_map>

#include "sarc/bindings/erlang.hpp"
#include "sarc/core/errors.hpp"
#include "sarc/core/types.hpp"
#include "sarc/storage/object_store.hpp"

namespace {

using namespace sarc;
using namespace sarc::core;
using namespace sarc::storage;
using namespace sarc::bindings::erlang;

static size_t cstrnlen_(const char* s, size_t max_len) noexcept {
    if (s == nullptr) {
        return 0;
    }
    size_t n = 0;
    while (n < max_len && s[n] != '\0') {
        ++n;
    }
    return n;
}

// Operation codes
constexpr u8 OP_PUT_OBJECT = 1;
constexpr u8 OP_DELETE_OBJECT = 2;
constexpr u8 OP_GC = 3;
constexpr u8 OP_PUT_STREAM_START = 4;
constexpr u8 OP_PUT_STREAM_CHUNK = 5;
constexpr u8 OP_PUT_STREAM_FINISH = 6;
constexpr u8 OP_PUT_STREAM_ABORT = 7;
constexpr u8 OP_PUT_STREAM_CHUNK_ASYNC = 8;

// Response status codes
constexpr u8 STATUS_OK = 0;
constexpr u8 STATUS_ERROR = 1;

// Global writers map
static std::unordered_map<u32, ObjectWriter*> g_writers;
static u32 g_next_writer_id = 1;

// ============================================================================
// I/O Utilities
// ============================================================================

// Read exactly N bytes from file descriptor or return false
bool read_exact(int fd, u8* buf, u32 len) {
    u32 total_read = 0;
    while (total_read < len) {
        ssize_t n = read(fd, buf + total_read, len - total_read);
        if (n <= 0) {
            return false;  // EOF or error
        }
        total_read += n;
    }
    return true;
}

// Write exactly N bytes to file descriptor or return false
bool write_exact(int fd, const u8* buf, u32 len) {
    u32 total_written = 0;
    while (total_written < len) {
        ssize_t n = write(fd, buf + total_written, len - total_written);
        if (n <= 0) {
            return false;  // Error
        }
        total_written += n;
    }
    return true;
}

// Read 4-byte big-endian u32
u32 read_u32_be(int fd) {
    u8 buf[4];
    if (!read_exact(fd, buf, 4)) {
        return 0;
    }
    return (static_cast<u32>(buf[0]) << 24) |
           (static_cast<u32>(buf[1]) << 16) |
           (static_cast<u32>(buf[2]) << 8) |
           static_cast<u32>(buf[3]);
}

// Write 4-byte big-endian u32
bool write_u32_be(int fd, u32 val) {
    u8 buf[4];
    buf[0] = static_cast<u8>((val >> 24) & 0xFF);
    buf[1] = static_cast<u8>((val >> 16) & 0xFF);
    buf[2] = static_cast<u8>((val >> 8) & 0xFF);
    buf[3] = static_cast<u8>(val & 0xFF);
    return write_exact(fd, buf, 4);
}

// ============================================================================
// Erlang Term Encoding/Decoding (Simplified)
// ============================================================================

// Simple Erlang term decoder (minimal implementation for our needs)
// Full implementation would use ei library

struct TermDecoder {
    const u8* data;
    u32 len;
    u32 pos;

    TermDecoder(const u8* d, u32 l) : data(d), len(l), pos(0) {}

    bool has_bytes(u32 n) const {
        return (pos + n) <= len;
    }

    u8 read_u8() {
        if (!has_bytes(1)) return 0;
        return data[pos++];
    }

    u32 read_u32() {
        if (!has_bytes(4)) return 0;
        u32 val = (static_cast<u32>(data[pos]) << 24) |
                  (static_cast<u32>(data[pos + 1]) << 16) |
                  (static_cast<u32>(data[pos + 2]) << 8) |
                  static_cast<u32>(data[pos + 3]);
        pos += 4;
        return val;
    }

    bool read_u32_int(u32* out) {
        if (!out) return false;
        u8 tag = read_u8();
        switch (tag) {
            case 97:  // SMALL_INTEGER_EXT
                if (!has_bytes(1)) return false;
                *out = read_u8();
                return true;
            case 98:  // INTEGER_EXT
                if (!has_bytes(4)) return false;
                *out = read_u32();
                return true;
            default:
                return false;
        }
    }

    bool read_binary_view(const u8** out_data, u32* out_len) {
        if (!out_data || !out_len) return false;
        if (read_u8() != 109) return false;  // BINARY_EXT tag
        u32 size = read_u32();
        if (!has_bytes(size)) return false;
        *out_data = data + pos;
        *out_len = size;
        pos += size;
        return true;
    }

    bool read_binary(std::vector<u8>* out) {
        if (!out) return false;
        const u8* ptr = nullptr;
        u32 size = 0;
        if (!read_binary_view(&ptr, &size)) return false;
        out->assign(ptr, ptr + size);
        return true;
    }

    bool read_string(std::string* out) {
        u8 tag = read_u8();
        if (tag == 107) {  // STRING_EXT
            u32 len = (static_cast<u32>(data[pos]) << 8) | data[pos + 1];
            pos += 2;
            if (!has_bytes(len)) return false;
            out->assign(reinterpret_cast<const char*>(data + pos), len);
            pos += len;
            return true;
        } else if (tag == 109) {  // BINARY_EXT (also used for strings)
            u32 len = read_u32();
            if (!has_bytes(len)) return false;
            out->assign(reinterpret_cast<const char*>(data + pos), len);
            pos += len;
            return true;
        } else if (tag == 106) {  // NIL_EXT (empty list/undefined)
            *out = "";
            return true;
        }
        return false;
    }

    bool skip() {
        u8 tag = read_u8();
        switch (tag) {
            case 97:  // SMALL_INTEGER_EXT
                pos += 1;
                return true;
            case 98:  // INTEGER_EXT
                pos += 4;
                return true;
            case 100:  // ATOM_EXT
            {
                if (!has_bytes(2)) return false;
                u16 len = (static_cast<u16>(data[pos]) << 8) | data[pos + 1];
                pos += 2 + len;
                return true;
            }
            case 106:  // NIL_EXT
                return true;
            case 107:  // STRING_EXT
            {
                if (!has_bytes(2)) return false;
                u16 len = (static_cast<u16>(data[pos]) << 8) | data[pos + 1];
                pos += 2 + len;
                return true;
            }
            case 109:  // BINARY_EXT
            {
                u32 len = read_u32();
                if (!has_bytes(len)) return false;
                pos += len;
                return true;
            }
            case 114:  // NEW_REFERENCE_EXT
            {
                if (!has_bytes(2)) return false;
                u16 id_len = (static_cast<u16>(data[pos]) << 8) | data[pos + 1];
                pos += 2;  // Length field
                // Skip node (atom) recursively
                if (!skip()) return false;
                // Skip creation (1 byte) + IDs (id_len * 4 bytes)
                if (!has_bytes(1 + id_len * 4)) return false;
                pos += 1 + (id_len * 4);
                return true;
            }
            case 90:  // NEWER_REFERENCE_EXT
            {
                if (!has_bytes(2)) return false;
                u16 id_len = (static_cast<u16>(data[pos]) << 8) | data[pos + 1];
                pos += 2;  // Length field
                // Skip node (atom) recursively
                if (!skip()) return false;
                // Skip creation (4 bytes) + IDs (id_len * 4 bytes)
                if (!has_bytes(4 + id_len * 4)) return false;
                pos += 4 + (id_len * 4);
                return true;
            }
            case 115:  // SMALL_ATOM_EXT
            {
                if (!has_bytes(1)) return false;
                u8 len = data[pos];
                pos += 1 + len;
                return true;
            }
            case 118:  // ATOM_UTF8_EXT
            {
                if (!has_bytes(2)) return false;
                u16 len = (static_cast<u16>(data[pos]) << 8) | data[pos + 1];
                pos += 2 + len;
                return true;
            }
            case 119:  // SMALL_ATOM_UTF8_EXT
            {
                if (!has_bytes(1)) return false;
                u8 len = data[pos];
                pos += 1 + len;
                return true;
            }
            default:
                std::fprintf(stderr, "port: cannot skip unknown term type: %u at pos %u\n", tag, pos - 1);
                return false;
        }
    }
};

// Simple Erlang term encoder
struct TermEncoder {
    std::vector<u8> buf;

    void write_u8(u8 val) {
        buf.push_back(val);
    }

    void write_u32(u32 val) {
        buf.push_back(static_cast<u8>((val >> 24) & 0xFF));
        buf.push_back(static_cast<u8>((val >> 16) & 0xFF));
        buf.push_back(static_cast<u8>((val >> 8) & 0xFF));
        buf.push_back(static_cast<u8>(val & 0xFF));
    }

    void write_atom(const char* name) {
        u32 len = std::strlen(name);
        if (len <= 255) {
            write_u8(115);  // ATOM_UTF8_EXT
            write_u8(static_cast<u8>(len));
            for (u32 i = 0; i < len; ++i) {
                buf.push_back(name[i]);
            }
        }
    }

    void write_integer(u32 val) {
        write_u8(98);  // INTEGER_EXT
        write_u32(val);
    }

    void write_binary(const u8* data, u32 len) {
        write_u8(109);  // BINARY_EXT
        write_u32(len);
        for (u32 i = 0; i < len; ++i) {
            buf.push_back(data[i]);
        }
    }

    void write_tuple_header(u32 arity) {
        if (arity <= 255) {
            write_u8(104);  // SMALL_TUPLE_EXT
            write_u8(static_cast<u8>(arity));
        }
    }

    void write_map_header(u32 arity) {
        write_u8(116);  // MAP_EXT
        write_u32(arity);
    }
};

// ============================================================================
// Operation Handlers
// ============================================================================

// Handle PutObject operation
// Request payload (after opcode): {Ref, {Zone, Data, Filename, MimeType}}
// Response: {Ref, {ok, {Zone, Hash, Meta, Deduplicated}} | {error, Reason}}
void handle_put_object(const u8* payload, u32 payload_len) {
    TermDecoder dec(payload, payload_len);

    // Decode the entire payload as a term
    // Format: {Ref, {Zone, Data, Filename, MimeType}}

    // Skip version byte (131)
    if (dec.read_u8() != 131) {
        std::fprintf(stderr, "port: invalid term version\n");
        return;
    }

    // Read outer tuple {Ref, Payload}
    if (dec.read_u8() != 104) {  // SMALL_TUPLE_EXT
        std::fprintf(stderr, "port: expected outer tuple\n");
        return;
    }

    u8 outer_arity = dec.read_u8();
    if (outer_arity != 2) {
        std::fprintf(stderr, "port: wrong outer arity, expected 2, got %u\n", outer_arity);
        return;
    }

    // Save the Ref (we'll echo it back in response)
    u32 ref_start = dec.pos;
    if (!dec.skip()) {
        std::fprintf(stderr, "port: failed to skip ref\n");
        return;
    }
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    // Read inner tuple {Zone, Data, Filename, MimeType}
    if (dec.read_u8() != 104) {  // SMALL_TUPLE_EXT
        std::fprintf(stderr, "port: expected inner tuple\n");
        return;
    }

    u8 arity = dec.read_u8();
    if (arity != 4) {  // {Zone, Data, Filename, MimeType}
        std::fprintf(stderr, "port: wrong arity for put, expected 4, got %u\n", arity);
        return;
    }

    // Read Zone (u32) - can be SMALL_INTEGER_EXT or INTEGER_EXT
    u8 zone_tag = dec.read_u8();
    u32 zone_id;
    if (zone_tag == 97) {  // SMALL_INTEGER_EXT
        zone_id = dec.read_u8();
    } else if (zone_tag == 98) {  // INTEGER_EXT
        zone_id = dec.read_u32();
    } else {
        std::fprintf(stderr, "port: expected integer for zone, got tag %u\n", zone_tag);
        return;
    }

    // Read Data (binary)
    std::vector<u8> data;
    if (!dec.read_binary(&data)) {
        std::fprintf(stderr, "port: failed to read data binary\n");
        return;
    }

    // Read Filename (string or nil)
    std::string filename;
    if (!dec.read_string(&filename)) {
        filename = "";  // Optional
    }

    // Read MimeType (string or nil)
    std::string mime_type;
    if (!dec.read_string(&mime_type)) {
        mime_type = "application/octet-stream";  // Default
    }

    // Execute object_put
    ObjectPutParams params{};
    params.zone = ZoneId{zone_id};
    params.data = data.data();
    params.size_bytes = data.size();
    params.filename = filename.empty() ? nullptr : filename.c_str();
    params.mime_type = mime_type.empty() ? nullptr : mime_type.c_str();

    ObjectPutResult result{};
    Status s = object_put(params, &result);

    // Encode response: {Ref, Result}
    TermEncoder enc;
    enc.write_u8(131);  // Version
    enc.write_tuple_header(2);  // {Ref, Result}

    // Write Ref
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    if (is_ok(s)) {
        // {ok, {Zone, Hash}, Meta, Deduplicated}
        enc.write_tuple_header(4);
        enc.write_atom("ok");

        // Key: {Zone, Hash}
        enc.write_tuple_header(2);
        enc.write_integer(result.key.zone.v);
        enc.write_binary(result.key.content.b.data(), 32);

        // Meta map
        enc.write_map_header(6);
        enc.write_atom("size");
        enc.write_integer(static_cast<u32>(result.meta.size_bytes));
        enc.write_atom("refcount");
        enc.write_integer(result.meta.refcount);
        enc.write_atom("created_at");
        enc.write_integer(static_cast<u32>(result.meta.created_at));
        enc.write_atom("updated_at");
        enc.write_integer(static_cast<u32>(result.meta.updated_at));
        enc.write_atom("filename");
        enc.write_binary(reinterpret_cast<const u8*>(params.filename ? params.filename : ""), 
                         params.filename ? std::strlen(params.filename) : 0);
        enc.write_atom("mime_type");
        enc.write_binary(reinterpret_cast<const u8*>(params.mime_type ? params.mime_type : ""), 
                         params.mime_type ? std::strlen(params.mime_type) : 0);

        enc.write_atom(result.deduplicated ? "true" : "false");
    } else {
        // {error, Reason}
        enc.write_tuple_header(2);
        enc.write_atom("error");

        switch (s.code) {
            case StatusCode::NotFound:
                enc.write_atom("not_found");
                break;
            case StatusCode::Invalid:
                enc.write_atom("invalid");
                break;
            case StatusCode::Io:
                enc.write_atom("io_error");
                break;
            default:
                enc.write_atom("unknown_error");
                break;
        }
    }

    // Send response (with {packet, 4} format)
    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());

    std::fflush(stdout);
}

// Handle DeleteObject operation
// Request payload: {Ref, {Zone, Hash}}
// Response: {Ref, ok | {error, Reason}}
void handle_delete_object(const u8* payload, u32 payload_len) {
    TermDecoder dec(payload, payload_len);

    // Skip version byte
    if (dec.read_u8() != 131) return;

    // Read outer tuple {Ref, {Zone, Hash}}
    if (dec.read_u8() != 104) return;  // SMALL_TUPLE_EXT
    if (dec.read_u8() != 2) return;    // Arity 2

    // Save the Ref
    u32 ref_start = dec.pos;
    if (!dec.skip()) {
        std::fprintf(stderr, "port: failed to skip ref in delete\n");
        return;
    }
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    // Read inner tuple {Zone, Hash}
    if (dec.read_u8() != 104) return;  // SMALL_TUPLE_EXT
    if (dec.read_u8() != 2) return;    // {Zone, Hash}

    // Read Zone - can be SMALL_INTEGER_EXT or INTEGER_EXT
    u8 zone_tag = dec.read_u8();
    u32 zone_id;
    if (zone_tag == 97) {  // SMALL_INTEGER_EXT
        zone_id = dec.read_u8();
    } else if (zone_tag == 98) {  // INTEGER_EXT
        zone_id = dec.read_u32();
    } else {
        std::fprintf(stderr, "port: expected integer for zone in delete, got tag %u\n", zone_tag);
        return;
    }

    // Read Hash
    std::vector<u8> hash_vec;
    if (!dec.read_binary(&hash_vec) || hash_vec.size() != 32) {
        std::fprintf(stderr, "port: invalid hash size\n");
        return;
    }

    Hash256 hash;
    std::memcpy(hash.b.data(), hash_vec.data(), 32);

    // Execute object_delete
    ObjectKey key{ZoneId{zone_id}, hash};
    Status s = object_delete(key);

    // Encode response: {Ref, Result}
    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);  // {Ref, Result}

    // Write Ref
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    if (is_ok(s)) {
        enc.write_atom("ok");
    } else {
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom(s.code == StatusCode::NotFound ? "not_found" : "unknown_error");
    }

    // Send response
    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());

    std::fflush(stdout);
}

void handle_gc(const u8* payload, u32 payload_len) {
    // ... existing implementation ...
    TermDecoder dec(payload, payload_len);

    // Skip version byte
    if (dec.read_u8() != 131) return;

    // Read outer tuple {Ref, Zone}
    if (dec.read_u8() != 104) return;  // SMALL_TUPLE_EXT
    if (dec.read_u8() != 2) return;    // Arity 2

    // Save the Ref
    u32 ref_start = dec.pos;
    if (!dec.skip()) {
        std::fprintf(stderr, "port: failed to skip ref in gc\n");
        return;
    }
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    // Read Zone - can be SMALL_INTEGER_EXT or INTEGER_EXT
    u8 zone_tag = dec.read_u8();
    u32 zone_id;
    if (zone_tag == 97) {  // SMALL_INTEGER_EXT
        zone_id = dec.read_u8();
    } else if (zone_tag == 98) {  // INTEGER_EXT
        zone_id = dec.read_u32();
    } else {
        std::fprintf(stderr, "port: expected integer for zone in gc, got tag %u\n", zone_tag);
        return;
    }

    // Execute object_gc
    u64 deleted_count = 0;
    Status s = object_gc(ZoneId{zone_id}, &deleted_count);

    // Encode response: {Ref, Result}
    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);  // {Ref, Result}

    // Write Ref
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    if (is_ok(s)) {
        enc.write_tuple_header(2);
        enc.write_atom("ok");
        enc.write_integer(static_cast<u32>(deleted_count));
    } else {
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("unknown_error");
    }

    // Send response
    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());

    std::fflush(stdout);
}

// Request: {Ref, Zone}
// Response: {Ref, {ok, Handle}}
void handle_put_stream_start(const u8* payload, u32 payload_len) {
    std::fprintf(stderr, "port: handle_put_stream_start called (len=%u)\n", payload_len);
    TermDecoder dec(payload, payload_len);
    if (dec.read_u8() != 131) {
        std::fprintf(stderr, "port: invalid version\n");
        return;
    }
    if (dec.read_u8() != 104 || dec.read_u8() != 2) {
        std::fprintf(stderr, "port: expected tuple\n");
        return;
    }

    std::fprintf(stderr, "port: skipping ref\n");
    u32 ref_start = dec.pos;
    if (!dec.skip()) {
        std::fprintf(stderr, "port: failed to skip ref\n");
        return;
    }
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    std::fprintf(stderr, "port: skipping zone\n");
    // Zone (unused for open(), but protocol keeps it for future/policy)
    if (!dec.skip()) {
        std::fprintf(stderr, "port: failed to skip zone\n");
        return;
    }

    std::fprintf(stderr, "port: creating writer\n");
    ObjectWriter* writer = new ObjectWriter();
    std::fprintf(stderr, "port: opening writer\n");
    Status s = writer->open();
    
    if (!is_ok(s)) {
        std::fprintf(stderr, "port: writer open failed code=%d\n", (int)s.code);
    } else {
        std::fprintf(stderr, "port: writer opened successfully\n");
    }

    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    if (is_ok(s)) {
        u32 id = g_next_writer_id++;
        g_writers[id] = writer;
        enc.write_tuple_header(2);
        enc.write_atom("ok");
        enc.write_integer(id);
    } else {
        delete writer;
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("io_error");
    }

    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
    std::fflush(stdout);
}

// Request: {Ref, {Handle, Data}}
// Response: {Ref, ok | {error, Reason}}
void handle_put_stream_chunk(const u8* payload, u32 payload_len) {
    TermDecoder dec(payload, payload_len);
    if (dec.read_u8() != 131) return;
    if (dec.read_u8() != 104 || dec.read_u8() != 2) return;

    u32 ref_start = dec.pos;
    if (!dec.skip()) return;
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    // Inner tuple {Handle, Data}
    if (dec.read_u8() != 104 || dec.read_u8() != 2) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }

    u32 handle = 0;
    if (!dec.read_u32_int(&handle)) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }

    const u8* data_ptr = nullptr;
    u32 data_len = 0;
    if (!dec.read_binary_view(&data_ptr, &data_len)) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }

    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    auto it = g_writers.find(handle);
    if (it != g_writers.end()) {
        Status s = it->second->write(data_ptr, data_len);
        if (is_ok(s)) {
            enc.write_atom("ok");
        } else {
            enc.write_tuple_header(2);
            enc.write_atom("error");
            enc.write_atom("io_error");
        }
    } else {
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid_handle");
    }

    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
    std::fflush(stdout);
}

// Request: {Ref, {Handle, Zone, Filename, Mime}}
// Response: {Ref, {ok, Result} | {error, Reason}}
void handle_put_stream_finish(const u8* payload, u32 payload_len) {
    std::fprintf(stderr, "port: handle_put_stream_finish called\n");
    TermDecoder dec(payload, payload_len);
    if (dec.read_u8() != 131) return;
    if (dec.read_u8() != 104 || dec.read_u8() != 2) return;

    u32 ref_start = dec.pos;
    if (!dec.skip()) return;
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    // Inner tuple {Handle, Zone, Filename, Mime}
    if (dec.read_u8() != 104 || dec.read_u8() != 4) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }

    u32 handle = 0;
    if (!dec.read_u32_int(&handle)) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }
    u32 zone_id = 0;
    if (!dec.read_u32_int(&zone_id)) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }
    std::string filename, mime;
    dec.read_string(&filename);
    dec.read_string(&mime);

    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    auto it = g_writers.find(handle);
    if (it != g_writers.end()) {
        ObjectWriter* writer = it->second;
        ObjectPutParams params{};
        params.zone = ZoneId{zone_id};
        params.filename = filename.c_str();
        params.mime_type = mime.c_str();
        
        ObjectPutResult result{};
        Status s = writer->commit(params, &result);
        
        delete writer;
        g_writers.erase(it);

        if (is_ok(s)) {
            // Success response (same as put_object)
            enc.write_tuple_header(4);
            enc.write_atom("ok");
            enc.write_tuple_header(2);
            enc.write_integer(result.key.zone.v);
            enc.write_binary(result.key.content.b.data(), 32);
            
            enc.write_map_header(6);
            enc.write_atom("size"); enc.write_integer(static_cast<u32>(result.meta.size_bytes));
            enc.write_atom("refcount"); enc.write_integer(result.meta.refcount);
            enc.write_atom("created_at"); enc.write_integer(static_cast<u32>(result.meta.created_at));
            enc.write_atom("updated_at"); enc.write_integer(static_cast<u32>(result.meta.updated_at));
            enc.write_atom("filename"); enc.write_binary((const u8*)result.filename, cstrnlen_(result.filename, sizeof(result.filename)));
            enc.write_atom("mime_type"); enc.write_binary((const u8*)result.mime_type, cstrnlen_(result.mime_type, sizeof(result.mime_type)));
            
            enc.write_atom(result.deduplicated ? "true" : "false");
        } else {
            enc.write_tuple_header(2);
            enc.write_atom("error");
            enc.write_atom("io_error");
        }
    } else {
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid_handle");
    }

    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
    std::fflush(stdout);
}

// Request: {Ref, Handle}
// Response: {Ref, ok | {error, Reason}}
void handle_put_stream_abort(const u8* payload, u32 payload_len) {
    TermDecoder dec(payload, payload_len);
    if (dec.read_u8() != 131) return;
    if (dec.read_u8() != 104 || dec.read_u8() != 2) return;

    u32 ref_start = dec.pos;
    if (!dec.skip()) return;
    u32 ref_end = dec.pos;
    std::vector<u8> ref_bytes(dec.data + ref_start, dec.data + ref_end);

    u32 handle = 0;
    if (!dec.read_u32_int(&handle)) {
        TermEncoder enc;
        enc.write_u8(131);
        enc.write_tuple_header(2);
        enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid");
        write_u32_be(STDOUT_FILENO, enc.buf.size());
        write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
        std::fflush(stdout);
        return;
    }

    TermEncoder enc;
    enc.write_u8(131);
    enc.write_tuple_header(2);
    enc.buf.insert(enc.buf.end(), ref_bytes.begin(), ref_bytes.end());

    auto it = g_writers.find(handle);
    if (it != g_writers.end()) {
        delete it->second;
        g_writers.erase(it);
        enc.write_atom("ok");
    } else {
        enc.write_tuple_header(2);
        enc.write_atom("error");
        enc.write_atom("invalid_handle");
    }

    write_u32_be(STDOUT_FILENO, enc.buf.size());
    write_exact(STDOUT_FILENO, enc.buf.data(), enc.buf.size());
    std::fflush(stdout);
}

// Request: {Handle, Data}
// Response: NONE
void handle_put_stream_chunk_async(const u8* payload, u32 payload_len) {
    TermDecoder dec(payload, payload_len);
    if (dec.read_u8() != 131) return;
    
    // Tuple {Handle, Data}
    if (dec.read_u8() != 104 || dec.read_u8() != 2) return;

    u32 handle = 0;
    if (!dec.read_u32_int(&handle)) return;

    const u8* data_ptr = nullptr;
    u32 data_len = 0;
    if (!dec.read_binary_view(&data_ptr, &data_len)) return;

    auto it = g_writers.find(handle);
    if (it != g_writers.end()) {
        (void)it->second->write(data_ptr, data_len);
    }
}


}  // namespace

// ============================================================================
// Main Entry Point
// ============================================================================

int main() {
    // Initialize object store
    sarc::storage::ObjectStoreConfig cfg{};

    // Read configuration from environment variables with defaults
    const char* data_root = std::getenv("SARC_DATA_ROOT");
    const char* db_path = std::getenv("SARC_DB_PATH");

    cfg.data_root = data_root ? data_root : "/tmp/sarc_data";
    cfg.db_path = db_path ? db_path : "/tmp/sarc.db";
    cfg.max_object_bytes = 0;
    cfg.compression = sarc::storage::CompressionPolicy::None;
    cfg.verify_on_read = false;

    sarc::core::Status s = sarc::storage::object_store_init(cfg);
    if (!sarc::core::is_ok(s)) {
        std::fprintf(stderr, "port: failed to initialize object store code=%d\n", (int)s.code);
        return 1;
    }

    // Main message loop
    while (true) {
        // Read 4-byte message length (big-endian)
        u32 msg_len = read_u32_be(STDIN_FILENO);
        if (msg_len == 0) {
            break;  // EOF or error
        }

        // Read operation code
        u8 op_code;
        if (!read_exact(STDIN_FILENO, &op_code, 1)) {
            break;
        }

        // Read payload
        std::vector<u8> payload(msg_len - 1);
        if (!read_exact(STDIN_FILENO, payload.data(), payload.size())) {
            break;
        }

        // Dispatch to handler
        switch (op_code) {
            case OP_PUT_OBJECT:
                handle_put_object(payload.data(), payload.size());
                break;
            case OP_DELETE_OBJECT:
                handle_delete_object(payload.data(), payload.size());
                break;
            case OP_GC:
                handle_gc(payload.data(), payload.size());
                break;
            case OP_PUT_STREAM_START:
                handle_put_stream_start(payload.data(), payload.size());
                break;
            case OP_PUT_STREAM_CHUNK:
                handle_put_stream_chunk(payload.data(), payload.size());
                break;
            case OP_PUT_STREAM_FINISH:
                handle_put_stream_finish(payload.data(), payload.size());
                break;
            case OP_PUT_STREAM_ABORT:
                handle_put_stream_abort(payload.data(), payload.size());
                break;
            case OP_PUT_STREAM_CHUNK_ASYNC:
                handle_put_stream_chunk_async(payload.data(), payload.size());
                break;
            default:
                std::fprintf(stderr, "port: unknown operation code: %u\n", op_code);
                break;
        }
    }

    // Cleanup
    (void)sarc::storage::object_store_shutdown();
    return 0;
}
