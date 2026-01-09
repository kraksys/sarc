#include "sarc/bindings/http.hpp"
#include "sarc/storage/object_store.hpp"
#include "sarc/crypto/hash.hpp"
#include <cstring>
#include <cstdio>
#include <cstdlib>

namespace sarc::bindings::http {

using namespace sarc::core;
using namespace sarc::storage;

// Helper to convert BufferView to null-terminated string
static bool buffer_to_string(const BufferView& buf, char* out, u32 max_len) {
    if (buf.len >= max_len) return false;
    std::memcpy(out, buf.data, buf.len);
    out[buf.len] = '\0';
    return true;
}

// Helper to match HTTP method
static bool method_is(const BufferView& method, const char* expected) {
    size_t len = std::strlen(expected);
    return method.len == len && std::memcmp(method.data, expected, len) == 0;
}

// Helper to parse path components
// Expected formats:
// GET    /objects/{zone}/{hash}
// PUT    /objects/{zone}
// DELETE /objects/{zone}/{hash}
// GET    /objects/{zone}/query
// POST   /objects/{zone}/gc
struct ParsedPath {
    ZoneId zone;
    Hash256 hash;
    enum { OBJECT, QUERY, GC, UNKNOWN } action;
    bool has_hash;
};

static bool parse_path(const BufferView& path, ParsedPath* out) {
    char path_str[256];
    if (!buffer_to_string(path, path_str, sizeof(path_str))) {
        return false;
    }

    // Simple path parsing: /objects/{zone}/{hash_or_action}
    char* token = std::strtok(path_str, "/");
    if (!token || std::strcmp(token, "objects") != 0) {
        return false;
    }

    // Get zone
    token = std::strtok(nullptr, "/");
    if (!token) return false;
    out->zone = static_cast<ZoneId>(std::atoi(token));

    // Get hash or action
    token = std::strtok(nullptr, "/");
    if (!token) {
        // PUT /objects/{zone} - no hash needed
        out->action = ParsedPath::OBJECT;
        out->has_hash = false;
        return true;
    }

    // Check for special actions
    if (std::strcmp(token, "query") == 0) {
        out->action = ParsedPath::QUERY;
        out->has_hash = false;
        return true;
    }
    if (std::strcmp(token, "gc") == 0) {
        out->action = ParsedPath::GC;
        out->has_hash = false;
        return true;
    }

    // Otherwise, parse as hash (64 hex chars)
    if (std::strlen(token) != 64) {
        return false;
    }

    // Convert hex string to Hash256
    for (int i = 0; i < 32; i++) {
        char byte_str[3] = {token[i*2], token[i*2+1], '\0'};
        out->hash.bytes[i] = static_cast<u8>(std::strtol(byte_str, nullptr, 16));
    }

    out->action = ParsedPath::OBJECT;
    out->has_hash = true;
    return true;
}

// Handle GET /objects/{zone}/{hash}
static Status handle_get_object(const ParsedPath& path, const HttpRequest& req, HttpResponse* out) {
    if (!path.has_hash) {
        out->status = 400;
        return Status::Invalid;
    }

    ObjectKey key{path.zone, path.hash};

    // Check if object exists first
    bool exists = false;
    Status st = object_exists(key, &exists);
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }
    if (!exists) {
        out->status = 404;
        return Status::NotFound;
    }

    // Allocate buffer for response (this is a simplified implementation)
    // In production, you'd use a proper memory allocator
    static thread_local u8 response_buffer[64 * 1024 * 1024]; // 64MB max
    ObjectGetResult result;
    st = object_get(key, response_buffer, sizeof(response_buffer), &result);
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }

    out->status = 200;
    out->body.data = response_buffer;
    out->body.len = static_cast<u32>(result.bytes_read);
    return Status::Ok;
}

// Handle PUT /objects/{zone}
static Status handle_put_object(const ParsedPath& path, const HttpRequest& req, HttpResponse* out) {
    if (path.has_hash) {
        out->status = 400;
        return Status::Invalid;
    }

    ObjectPutParams params{};
    params.zone = path.zone;
    params.data = req.body.data;
    params.size_bytes = req.body.len;
    params.filename = "";
    params.mime_type = "application/octet-stream";

    // Check headers for Content-Type
    for (u32 i = 0; i < req.header_count; i++) {
        char name_str[64];
        if (!buffer_to_string(req.headers[i].name, name_str, sizeof(name_str))) {
            continue;
        }
        if (std::strcmp(name_str, "Content-Type") == 0) {
            static thread_local char mime_type_buf[128];
            if (buffer_to_string(req.headers[i].value, mime_type_buf, sizeof(mime_type_buf))) {
                params.mime_type = mime_type_buf;
            }
        }
    }

    ObjectPutResult result;
    Status st = object_put(params, &result);
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }

    // Format response with hash
    static thread_local char response_buf[256];
    int len = std::snprintf(response_buf, sizeof(response_buf),
                           "{\"zone\":%u,\"hash\":\"", result.key.zone);

    // Append hash in hex
    for (int i = 0; i < 32; i++) {
        len += std::snprintf(response_buf + len, sizeof(response_buf) - len,
                            "%02x", result.key.hash.bytes[i]);
    }

    len += std::snprintf(response_buf + len, sizeof(response_buf) - len,
                        "\",\"size\":%llu,\"deduplicated\":%s}",
                        result.meta.size_bytes,
                        result.deduplicated ? "true" : "false");

    out->status = 201;
    out->body.data = reinterpret_cast<const u8*>(response_buf);
    out->body.len = len;
    return Status::Ok;
}

// Handle DELETE /objects/{zone}/{hash}
static Status handle_delete_object(const ParsedPath& path, const HttpRequest& req, HttpResponse* out) {
    if (!path.has_hash) {
        out->status = 400;
        return Status::Invalid;
    }

    ObjectKey key{path.zone, path.hash};
    Status st = object_delete(key);
    if (st == Status::NotFound) {
        out->status = 404;
        return st;
    }
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }

    out->status = 204; // No Content
    return Status::Ok;
}

// Handle GET /objects/{zone}/query
static Status handle_query(const ParsedPath& path, const HttpRequest& req, HttpResponse* out) {
    // Simplified query - return all objects in zone
    ObjectQueryFilter filter{};
    filter.filename_pattern = nullptr;
    filter.mime_type_pattern = nullptr;
    filter.min_size_bytes = 0;
    filter.max_size_bytes = 0;
    filter.created_after = 0;
    filter.created_before = 0;
    filter.limit = 100;

    static thread_local ObjectKey results[100];
    u32 count = 100;
    Status st = object_query(path.zone, filter, results, &count);
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }

    // Format JSON response
    static thread_local char response_buf[64 * 1024];
    int len = std::snprintf(response_buf, sizeof(response_buf), "{\"results\":[");

    for (u32 i = 0; i < count; i++) {
        if (i > 0) {
            len += std::snprintf(response_buf + len, sizeof(response_buf) - len, ",");
        }
        len += std::snprintf(response_buf + len, sizeof(response_buf) - len, "\"");
        for (int j = 0; j < 32; j++) {
            len += std::snprintf(response_buf + len, sizeof(response_buf) - len,
                                "%02x", results[i].hash.bytes[j]);
        }
        len += std::snprintf(response_buf + len, sizeof(response_buf) - len, "\"");
    }

    len += std::snprintf(response_buf + len, sizeof(response_buf) - len,
                        "],\"count\":%u}", count);

    out->status = 200;
    out->body.data = reinterpret_cast<const u8*>(response_buf);
    out->body.len = len;
    return Status::Ok;
}

// Handle POST /objects/{zone}/gc
static Status handle_gc(const ParsedPath& path, const HttpRequest& req, HttpResponse* out) {
    u64 deleted = 0;
    Status st = object_gc(path.zone, &deleted);
    if (st != Status::Ok) {
        out->status = 500;
        return st;
    }

    static thread_local char response_buf[128];
    int len = std::snprintf(response_buf, sizeof(response_buf),
                           "{\"deleted\":%llu}", deleted);

    out->status = 200;
    out->body.data = reinterpret_cast<const u8*>(response_buf);
    out->body.len = len;
    return Status::Ok;
}

// Main HTTP request handler
Status handle_http_request(const HttpRequest& req, HttpResponse* out) noexcept {
    if (!out) {
        return Status::Invalid;
    }

    // Initialize response
    out->status = 500;
    out->body.data = nullptr;
    out->body.len = 0;

    // Parse path
    ParsedPath path;
    if (!parse_path(req.path, &path)) {
        out->status = 400;
        return Status::Invalid;
    }

    // Route based on method and action
    if (method_is(req.method, "GET")) {
        if (path.action == ParsedPath::OBJECT) {
            return handle_get_object(path, req, out);
        } else if (path.action == ParsedPath::QUERY) {
            return handle_query(path, req, out);
        }
    } else if (method_is(req.method, "PUT")) {
        if (path.action == ParsedPath::OBJECT && !path.has_hash) {
            return handle_put_object(path, req, out);
        }
    } else if (method_is(req.method, "DELETE")) {
        if (path.action == ParsedPath::OBJECT) {
            return handle_delete_object(path, req, out);
        }
    } else if (method_is(req.method, "POST")) {
        if (path.action == ParsedPath::GC) {
            return handle_gc(path, req, out);
        }
    }

    out->status = 405; // Method Not Allowed
    return Status::Invalid;
}

} // namespace sarc::bindings::http
