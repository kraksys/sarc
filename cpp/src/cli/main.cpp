#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <csignal>
#include <vector>
#include <string>
#include <set>
#include <map>
#include <regex>
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <cctype>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "sarc/cli/commands.hpp"
#include "sarc/cli/options.hpp"
#include "sarc/storage/object_store.hpp"
#include "sarc/storage/hashing.hpp"
#include "sarc/core/errors.hpp"
#include "sarc/core/zone.hpp"

// ========================================================================
// Global State
// ========================================================================

volatile sig_atomic_t g_running = 1;

// ========================================================================
// Configuration
// ========================================================================

struct CliConfig {
    std::string data_root;
    std::string db_path;
    std::string views_root;
    sarc::core::ZoneId zone{1};  // Default to zone 1 (zone 0 is reserved for cross-zone queries)
};

// ========================================================================
// Signal Handler
// ========================================================================

void sigint_handler(int sig) {
    (void)sig;
    g_running = 0;
}

// ========================================================================
// File I/O Utilities
// ========================================================================

struct FileReadResult {
    sarc::core::u8* data;
    sarc::core::u64 size;
};

sarc::core::Status read_file(const char* path, FileReadResult* result) {
    result->data = nullptr;
    result->size = 0;

    FILE* f = fopen(path, "rb");
    if (!f) {
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::NotFound);
    }

    // Get file size
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (size < 0) {
        fclose(f);
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::Io);
    }

    // Allocate buffer
    sarc::core::u8* buffer = static_cast<sarc::core::u8*>(
        malloc(static_cast<size_t>(size)));
    if (!buffer) {
        fclose(f);
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::Unavailable);
    }

    // Read file
    size_t read = fread(buffer, 1, static_cast<size_t>(size), f);
    fclose(f);

    if (read != static_cast<size_t>(size)) {
        free(buffer);
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::Io);
    }

    result->data = buffer;
    result->size = static_cast<sarc::core::u64>(size);
    return sarc::core::ok_status();
}

sarc::core::Status write_file(const char* path,
                               const sarc::core::u8* data,
                               sarc::core::u64 size) {
    FILE* f = fopen(path, "wb");
    if (!f) {
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::PermissionDenied);
    }

    size_t written = fwrite(data, 1, static_cast<size_t>(size), f);
    fclose(f);

    if (written != static_cast<size_t>(size)) {
        return sarc::core::make_status(
            sarc::core::StatusDomain::Cli,
            sarc::core::StatusCode::Io);
    }

    return sarc::core::ok_status();
}

const char* extract_filename(const char* path) {
    const char* last_slash = strrchr(path, '/');
    return last_slash ? last_slash + 1 : path;
}

const char* guess_mime_type(const char* filename) {
    const char* ext = strrchr(filename, '.');
    if (!ext) return "application/octet-stream";

    if (strcmp(ext, ".txt") == 0) return "text/plain";
    if (strcmp(ext, ".json") == 0) return "application/json";
    if (strcmp(ext, ".jpg") == 0 || strcmp(ext, ".jpeg") == 0)
        return "image/jpeg";
    if (strcmp(ext, ".png") == 0) return "image/png";
    if (strcmp(ext, ".pdf") == 0) return "application/pdf";
    if (strcmp(ext, ".html") == 0) return "text/html";
    if (strcmp(ext, ".md") == 0) return "text/markdown";

    return "application/octet-stream";
}

void hash_to_hex(const sarc::core::Hash256& hash, char* out, size_t out_size) {
    static const char hex[] = "0123456789abcdef";
    size_t pos = 0;
    for (size_t i = 0; i < hash.b.size() && pos + 2 < out_size; ++i) {
        out[pos++] = hex[(hash.b[i] >> 4) & 0xF];
        out[pos++] = hex[hash.b[i] & 0xF];
    }
    out[pos] = '\0';
}

static bool is_hex_char(char c) {
    return (c >= '0' && c <= '9') ||
           (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

static bool parse_hash256_hex(const char* hash_str, sarc::core::Hash256* out) {
    if (!hash_str || !out) return false;
    if (std::strlen(hash_str) != 64) return false;
    for (size_t i = 0; i < 64; ++i) {
        if (!is_hex_char(hash_str[i])) return false;
    }
    sarc::core::Hash256 h{};
    for (size_t i = 0; i < 32; ++i) {
        char hex[3] = {hash_str[i * 2], hash_str[i * 2 + 1], 0};
        char* end = nullptr;
        unsigned long val = std::strtoul(hex, &end, 16);
        if (!end || *end != '\0' || val > 255) return false;
        h.b[i] = static_cast<sarc::core::u8>(val);
    }
    *out = h;
    return true;
}

static bool parse_object_key(const CliConfig& cfg, const char* key_str, sarc::core::ObjectKey* out) {
    if (!key_str || !out) return false;

    const char* colon = std::strchr(key_str, ':');
    if (!colon) {
        // Allow bare hash, assume current zone.
        sarc::core::Hash256 h{};
        if (!parse_hash256_hex(key_str, &h)) return false;
        *out = sarc::core::ObjectKey{cfg.zone, h};
        return true;
    }

    // Parse zone
    sarc::core::u32 zone_val = 0;
    if (colon == key_str) return false;
    for (const char* p = key_str; p < colon; ++p) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        zone_val = zone_val * 10 + (*p - '0');
    }

    // Parse hash
    const char* hash_str = colon + 1;
    sarc::core::Hash256 h{};
    if (!parse_hash256_hex(hash_str, &h)) return false;

    *out = sarc::core::ObjectKey{sarc::core::ZoneId{zone_val}, h};
    return true;
}

static std::string object_key_string(const sarc::core::ObjectKey& key) {
    char hash_hex[65];
    hash_to_hex(key.content, hash_hex, sizeof(hash_hex));
    char buf[80];
    std::snprintf(buf, sizeof(buf), "%u:%s", key.zone.v, hash_hex);
    return std::string(buf);
}

static bool parse_u32_strict(const char* s, sarc::core::u32* out) {
    if (!s || !out) return false;
    if (*s == '\0') return false;
    sarc::core::u64 v = 0;
    for (const char* p = s; *p; ++p) {
        if (*p < '0' || *p > '9') return false;
        v = v * 10 + static_cast<sarc::core::u64>(*p - '0');
        if (v > 0xFFFFFFFFull) return false;
    }
    *out = static_cast<sarc::core::u32>(v);
    return true;
}

static sarc::core::Status query_objects(const sarc::core::ZoneId zone,
                                        sarc::core::u32 limit,
                                        std::vector<sarc::storage::ObjectQueryResult>* out) {
    if (!out) {
        return sarc::core::make_status(sarc::core::StatusDomain::Cli, sarc::core::StatusCode::Invalid);
    }
    out->clear();

    sarc::storage::ObjectQueryFilter filter{
        .filename_pattern = nullptr,
        .mime_type_pattern = nullptr,
        .min_size_bytes = 0,
        .max_size_bytes = 0,
        .created_after = 0,
        .created_before = 0,
        .limit = limit
    };

    out->resize(limit);
    sarc::core::u32 count = limit;
    sarc::core::Status s = sarc::storage::object_query(zone, filter, out->data(), &count);
    if (!sarc::core::is_ok(s)) {
        out->clear();
        return s;
    }
    out->resize(count);
    return sarc::core::ok_status();
}

#if defined(__APPLE__)
static std::string shell_escape_posix_single_quotes(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('\'');
    for (char c : s) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}
#endif

static bool open_with_default_app(const std::filesystem::path& path) {
    const std::string p = path.string();

#if defined(_WIN32)
    const std::string cmd = "cmd.exe /c start \"\" " + p;
    return std::system(cmd.c_str()) == 0;
#elif defined(__APPLE__)
    const std::string cmd = "nohup open " + shell_escape_posix_single_quotes(p) + " >/dev/null 2>&1 &";
    return std::system(cmd.c_str()) == 0;
#else
    const pid_t pid = fork();
    if (pid < 0) {
        return false;
    }
    if (pid > 0) {
        return true; // detached
    }

    (void)setsid();
    int fd = open("/dev/null", O_RDWR);
    if (fd >= 0) {
        (void)dup2(fd, STDIN_FILENO);
        (void)dup2(fd, STDOUT_FILENO);
        (void)dup2(fd, STDERR_FILENO);
        if (fd > 2) close(fd);
    }

    execlp("xdg-open", "xdg-open", p.c_str(), (char*)nullptr);
    _exit(127);
#endif
}

static std::string truncate_filename_preserving_ext(const std::string& name, size_t max_len) {
    namespace fs = std::filesystem;
    if (max_len == 0) return std::string();
    if (name.size() <= max_len) return name;

    const fs::path p{name};
    const std::string ext = p.has_extension() ? p.extension().string() : std::string();
    const std::string stem = p.has_stem() ? p.stem().string() : name;

    // Leave room for extension.
    if (ext.size() >= max_len) {
        return ext.substr(0, max_len);
    }
    const size_t stem_max = max_len - ext.size();
    std::string out = stem.substr(0, stem_max);
    out += ext;
    return out;
}

static void purge_views_folder(const std::filesystem::path& views_root) {
    namespace fs = std::filesystem;
    std::error_code ec;
    if (views_root.empty()) return;
    if (!fs::exists(views_root, ec) || !fs::is_directory(views_root, ec)) return;

    const auto cutoff = fs::file_time_type::clock::now() - std::chrono::hours(24);

    std::vector<fs::path> files;
    std::vector<fs::path> dirs;
    for (fs::recursive_directory_iterator it(views_root,
                                            fs::directory_options::skip_permission_denied,
                                            ec),
         end;
         it != end && !ec;
         it.increment(ec)) {
        const fs::directory_entry& de = *it;
        if (de.is_directory(ec)) {
            dirs.push_back(de.path());
            continue;
        }
        if (!de.is_regular_file(ec) && !de.is_symlink(ec)) continue;
        const auto t = de.last_write_time(ec);
        if (ec) {
            ec.clear();
            continue;
        }
        if (t < cutoff) {
            files.push_back(de.path());
        }
    }

    for (const auto& p : files) {
        (void)fs::remove(p, ec);
        ec.clear();
    }

    // Clean up empty directories (deepest-first).
    std::sort(dirs.begin(), dirs.end(),
              [](const fs::path& a, const fs::path& b) { return a.string().size() > b.string().size(); });
    for (const auto& d : dirs) {
        if (fs::is_empty(d, ec)) {
            (void)fs::remove(d, ec);
        }
        ec.clear();
    }
}

static bool contains_glob_chars(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

static std::regex glob_to_regex(const std::string& glob) {
    std::string re;
    re.reserve(glob.size() * 2);
    re += "^";

    for (size_t i = 0; i < glob.size(); ++i) {
        const char c = glob[i];
        if (c == '*') {
            const bool is_double = (i + 1 < glob.size() && glob[i + 1] == '*');
            if (is_double) {
                re += ".*";
                ++i;
            } else {
                re += "[^/]*";
            }
            continue;
        }
        if (c == '?') {
            re += "[^/]";
            continue;
        }
        if (c == '.' || c == '^' || c == '$' || c == '+' || c == '(' || c == ')' ||
            c == '[' || c == ']' || c == '{' || c == '}' || c == '|' || c == '\\') {
            re.push_back('\\');
        }
        re.push_back(c);
    }

    re += "$";
    return std::regex(re, std::regex::ECMAScript);
}

static void expand_put_inputs(const std::string& pattern, std::vector<std::filesystem::path>* out_paths) {
    namespace fs = std::filesystem;
    if (!out_paths) return;

    fs::path p = fs::path(pattern);
    std::error_code ec;

    if (!contains_glob_chars(pattern)) {
        if (fs::is_regular_file(p, ec)) {
            out_paths->push_back(p);
            return;
        }
        if (fs::is_directory(p, ec)) {
            fs::recursive_directory_iterator it(p, fs::directory_options::skip_permission_denied, ec);
            fs::recursive_directory_iterator end;
            for (; it != end; it.increment(ec)) {
                if (ec) {
                    ec.clear();
                    continue;
                }
                if (fs::is_regular_file(it->path(), ec)) {
                    out_paths->push_back(it->path());
                }
            }
            return;
        }
        return;
    }

    const std::string pat = fs::path(pattern).generic_string();
    const size_t first_wild = pat.find_first_of("*?");
    if (first_wild == std::string::npos) return;

    const size_t slash_before = pat.rfind('/', first_wild);
    fs::path root;
    std::string rel_pat;
    if (slash_before == std::string::npos) {
        root = fs::path(".");
        rel_pat = pat;
    } else {
        root = fs::path(pat.substr(0, slash_before));
        rel_pat = pat.substr(slash_before + 1);
        if (root.empty()) {
            root = fs::path(".");
        }
    }

    if (!fs::exists(root, ec) || !fs::is_directory(root, ec)) {
        return;
    }

    const std::regex re = glob_to_regex(rel_pat);

    fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    for (; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        const fs::path candidate = it->path();
        if (!fs::is_regular_file(candidate, ec)) {
            continue;
        }
        fs::path rel = fs::relative(candidate, root, ec);
        if (ec) {
            ec.clear();
            continue;
        }
        const std::string rel_s = rel.generic_string();
        if (std::regex_match(rel_s, re)) {
            out_paths->push_back(candidate);
        }
    }
}

static std::string to_lower_ascii(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

static void parse_extension_list(const std::string& spec, std::set<std::string>* out) {
    if (!out) return;
    out->clear();

    size_t start = 0;
    while (start < spec.size()) {
        size_t end = spec.find(',', start);
        if (end == std::string::npos) end = spec.size();
        std::string tok = spec.substr(start, end - start);

        while (!tok.empty() && std::isspace(static_cast<unsigned char>(tok.front()))) tok.erase(tok.begin());
        while (!tok.empty() && std::isspace(static_cast<unsigned char>(tok.back()))) tok.pop_back();

        if (!tok.empty()) {
            tok = to_lower_ascii(tok);
            if (tok[0] != '.') tok.insert(tok.begin(), '.');
            out->insert(tok);
        }

        start = end + 1;
    }
}

static bool extension_allowed(const std::filesystem::path& p, const std::set<std::string>& allowed_exts) {
    if (allowed_exts.empty()) return true;
    std::string ext = to_lower_ascii(p.extension().string());
    if (ext.empty()) return false;
    return allowed_exts.contains(ext);
}

// ========================================================================
// Line Parsing
// ========================================================================

void parse_line(const char* line, int* argc, char** argv, int max_args) {
    *argc = 0;

    // Skip leading whitespace
    while (*line && (*line == ' ' || *line == '\t' || *line == '\n')) {
        line++;
    }

    // Tokenize
    while (*line && *argc < max_args) {
        // Start of token
        const char* token_start = line;

        // Find end of token
        while (*line && *line != ' ' && *line != '\t' && *line != '\n') {
            line++;
        }

        // Copy token
        size_t token_len = line - token_start;
        if (token_len > 0) {
            char* token = static_cast<char*>(malloc(token_len + 1));
            memcpy(token, token_start, token_len);
            token[token_len] = '\0';
            argv[(*argc)++] = token;
        }

        // Skip whitespace
        while (*line && (*line == ' ' || *line == '\t' || *line == '\n')) {
            line++;
        }
    }
}

void free_argv(char** argv, int argc) {
    for (int i = 0; i < argc; ++i) {
        free(argv[i]);
    }
}

static char* dup_token(const char* s) {
    if (!s) return nullptr;
    const size_t n = std::strlen(s);
    char* out = static_cast<char*>(malloc(n + 1));
    if (!out) return nullptr;
    std::memcpy(out, s, n);
    out[n] = '\0';
    return out;
}

// ========================================================================
// Error Handling
// ========================================================================

void print_error(const char* msg) {
    fprintf(stderr, "error: %s\n", msg);
}

void print_status_error(const char* context, sarc::core::Status s) {
    fprintf(stderr, "error: %s failed (code=%u, domain=%u)\n",
            context,
            static_cast<unsigned>(s.code),
            static_cast<unsigned>(s.domain));
}

const char* status_code_name(sarc::core::StatusCode code) {
    using sarc::core::StatusCode;
    switch (code) {
        case StatusCode::Ok: return "Ok";
        case StatusCode::Unknown: return "Unknown";
        case StatusCode::Invalid: return "Invalid";
        case StatusCode::NotFound: return "NotFound";
        case StatusCode::PermissionDenied: return "PermissionDenied";
        case StatusCode::Conflict: return "Conflict";
        case StatusCode::Busy: return "Busy";
        case StatusCode::Corrupt: return "Corrupt";
        case StatusCode::Io: return "Io";
        case StatusCode::Crypto: return "Crypto";
        case StatusCode::Network: return "Network";
        case StatusCode::Unsupported: return "Unsupported";
        case StatusCode::Unavailable: return "Unavailable";
        case StatusCode::OutOfMemory: return "OutOfMemory";
    }
    return "Unknown";
}

const char* status_domain_name(sarc::core::StatusDomain domain) {
    using sarc::core::StatusDomain;
    switch (domain) {
        case StatusDomain::Core: return "Core";
        case StatusDomain::Storage: return "Storage";
        case StatusDomain::Db: return "Db";
        case StatusDomain::Security: return "Security";
        case StatusDomain::Net: return "Net";
        case StatusDomain::Cli: return "Cli";
        case StatusDomain::Bindings: return "Bindings";
        case StatusDomain::External: return "External";
    }
    return "Unknown";
}

void print_status_error_detailed(const char* context, sarc::core::Status s) {
    fprintf(stderr,
            "error: %s failed (code=%s/%u, domain=%s/%u, aux=%u)\n",
            context,
            status_code_name(s.code),
            static_cast<unsigned>(s.code),
            status_domain_name(s.domain),
            static_cast<unsigned>(s.domain),
            s.aux);
    if (s.code == sarc::core::StatusCode::Io && s.aux != 0) {
        fprintf(stderr, "error: %s: %s\n", context, std::strerror(static_cast<int>(s.aux)));
    }
}

// ========================================================================
// Command Handlers
// ========================================================================

void handle_help() {
    printf("Commands:\n");
    printf("  ls                List objects (RID - zone - filename)\n");
    printf("  lsa               List objects (RID - zone - filename + size + key)\n");
    printf("  put [opts] <paths..> Store files (supports '*' and '**' globs)\n");
    printf("                   Options: -e/--ext <.pdf,.txt> or -[.pdf,.txt] (filter by extension)\n");
    printf("  get <key|rid|name> Retrieve by zone:hash, RID, or filename\n");
    printf("                   Options: -o <path|dir>, --open, --open-dir, --open-origin, --open-origin-dir\n");
    printf("                   Note: --open/--open-dir materialize temporary files under ~/sarc/views (auto-purged after ~24h)\n");
    printf("                         Do not rely on ~/sarc/views for permanent work; use -o to export.\n");
    printf("  g                 Alias for get\n");
    printf("  oRID              get RID + --open-origin\n");
    printf("  eRID              get RID + --open-origin-dir\n");
    printf("  .RID              get RID + --open\n");
    printf("  ,RID              get RID + --open-dir\n");
    printf("  /                 Search (FTS5 if available)\n");
    printf("  zone [id]         Show or switch current zone\n");
    printf("  gc                Garbage collect deleted objects\n");
    printf("  verify <key>      Verify object integrity\n");
    printf("  help              Show this help\n");
    printf("  q, quit, exit     Exit REPL\n");
    printf("\n");
    printf("Key format: zone:hash (e.g., 1:a3b5c7d9...)\n");
}

void handle_put(const CliConfig& cfg, int argc, char** argv) {
    if (argc < 1) {
        print_error("put: missing path or glob");
        return;
    }

    namespace fs = std::filesystem;

    std::set<std::string> allowed_exts;

    std::vector<fs::path> inputs;
    inputs.reserve(static_cast<size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        const std::string tok = argv[i] ? std::string(argv[i]) : std::string();
        if (tok.empty()) continue;

        if (tok == "-e" || tok == "--ext") {
            if (i + 1 >= argc || !argv[i + 1]) {
                print_error("put: -e/--ext requires a comma-separated list");
                return;
            }
            std::set<std::string> exts;
            parse_extension_list(argv[i + 1], &exts);
            allowed_exts.insert(exts.begin(), exts.end());
            ++i;
            continue;
        }
        if (tok.rfind("--ext=", 0) == 0) {
            std::set<std::string> exts;
            parse_extension_list(tok.substr(6), &exts);
            allowed_exts.insert(exts.begin(), exts.end());
            continue;
        }
        if (tok.size() >= 3 && tok.rfind("-[", 0) == 0 && tok.back() == ']') {
            std::set<std::string> exts;
            parse_extension_list(tok.substr(2, tok.size() - 3), &exts);
            allowed_exts.insert(exts.begin(), exts.end());
            continue;
        }
        if (!tok.empty() && tok[0] == '-') {
            fprintf(stderr, "error: put: unknown option %s\n", tok.c_str());
            return;
        }

        std::vector<fs::path> expanded;
        expand_put_inputs(tok, &expanded);
        inputs.insert(inputs.end(), expanded.begin(), expanded.end());
    }

    // De-dup and normalize.
    std::set<std::string> seen;
    std::vector<fs::path> files;
    files.reserve(inputs.size());
    for (const auto& p : inputs) {
        std::error_code ec;
        if (!fs::is_regular_file(p, ec)) continue;
        if (!extension_allowed(p, allowed_exts)) continue;
        fs::path abs = fs::absolute(p, ec);
        if (ec) abs = p;
        abs = abs.lexically_normal();
        const std::string key = abs.string();
        if (seen.insert(key).second) {
            files.push_back(abs);
        }
    }

    if (files.empty()) {
        if (allowed_exts.empty()) {
            print_error("put: no matching files");
        } else {
            print_error("put: no matching files after extension filter");
        }
        return;
    }

    for (const auto& abs_path : files) {
        const std::string abs_s = abs_path.string();

        FileReadResult file;
        sarc::core::Status s = read_file(abs_s.c_str(), &file);
        if (!sarc::core::is_ok(s)) {
            fprintf(stderr, "error: put: failed to read %s\n", abs_s.c_str());
            continue;
        }

        sarc::storage::ObjectPutParams params{
            .zone = cfg.zone,
            .data = file.data,
            .size_bytes = file.size,
            .filename = extract_filename(abs_s.c_str()),
            .mime_type = guess_mime_type(abs_s.c_str()),
            .origin_path = abs_s.c_str()
        };

        sarc::storage::ObjectPutResult result;
        s = sarc::storage::object_put(params, &result);

        free(file.data);

        if (!sarc::core::is_ok(s)) {
            fprintf(stderr, "error: put failed for %s\n", abs_s.c_str());
            print_status_error_detailed("put", s);
            continue;
        }

        char hash_hex[65];
        hash_to_hex(result.key.content, hash_hex, sizeof(hash_hex));
        printf("%u:%s  %s\n", result.key.zone.v, hash_hex, abs_s.c_str());

        if (result.deduplicated) {
            fprintf(stderr, "info: deduplicated %s\n", abs_s.c_str());
        }
    }
}

void handle_get(const CliConfig& cfg, int argc, char** argv) {
    if (argc < 1) {
        print_error("get: missing selector (key|rid|name)");
        return;
    }

    const char* selector = argv[0];
    const char* output_override = nullptr;
    bool open_file = false;
    bool open_dir = false;
    bool open_origin_file = false;
    bool open_origin_dir = false;

    for (int i = 1; i < argc; ++i) {
        const char* a = argv[i];
        if (!a) continue;
        if (std::strcmp(a, "-o") == 0 || std::strcmp(a, "--output") == 0) {
            if (i + 1 >= argc) {
                print_error("get: -o/--output requires a value");
                return;
            }
            output_override = argv[++i];
            continue;
        }
        if (std::strncmp(a, "--output=", 9) == 0) {
            output_override = a + 9;
            continue;
        }
        if (std::strcmp(a, "--open") == 0) {
            open_file = true;
            continue;
        }
        if (std::strcmp(a, "--open-dir") == 0) {
            open_dir = true;
            continue;
        }
        if (std::strcmp(a, "--open-origin") == 0) {
            open_origin_file = true;
            continue;
        }
        if (std::strcmp(a, "--open-origin-dir") == 0) {
            open_origin_dir = true;
            continue;
        }
        fprintf(stderr, "error: get: unknown option %s\n", a);
        return;
    }

    sarc::core::ObjectKey key{};
    bool key_ok = parse_object_key(cfg, selector, &key);

    if (!key_ok) {
        sarc::core::u32 rid = 0;
        const bool rid_ok = parse_u32_strict(selector, &rid) && rid > 0;

        std::vector<sarc::storage::ObjectQueryResult> objs;
        sarc::core::Status s = query_objects(cfg.zone, 10000, &objs);
        if (!sarc::core::is_ok(s)) {
            print_status_error("get(query)", s);
            return;
        }

        if (rid_ok) {
            if (rid > objs.size()) {
                print_error("get: RID out of range");
                return;
            }
            key = objs[rid - 1].key;
            key_ok = true;
        } else {
            // Resolve by filename.
            std::vector<size_t> matches;
            matches.reserve(8);
            for (size_t i = 0; i < objs.size(); ++i) {
                if (std::strcmp(objs[i].filename, selector) == 0) {
                    matches.push_back(i);
                }
            }
            if (matches.empty()) {
                for (size_t i = 0; i < objs.size(); ++i) {
                    const std::string name = objs[i].filename;
                    if (!name.empty() && name.find(selector) != std::string::npos) {
                        matches.push_back(i);
                    }
                }
            }

            if (matches.empty()) {
                print_error("get: no object matches selector (try list)");
                return;
            }
            if (matches.size() > 1) {
                fprintf(stderr, "error: get: selector matches multiple objects; use RID or key:\n");
                for (size_t j = 0; j < matches.size() && j < 20; ++j) {
                    const size_t i = matches[j];
                    char hash_hex[65];
                    hash_to_hex(objs[i].key.content, hash_hex, sizeof(hash_hex));
                    fprintf(stderr, "  RID=%zu  %u:%s  %s\n", i + 1, objs[i].key.zone.v, hash_hex,
                            objs[i].filename[0] ? objs[i].filename : "<blob>");
                }
                if (matches.size() > 20) {
                    fprintf(stderr, "  ... (%zu more)\n", matches.size() - 20);
                }
                return;
            }

            key = objs[matches[0]].key;
            key_ok = true;
        }
    }

    if (!key_ok) {
        print_error("get: invalid selector");
        return;
    }

    // Fetch metadata first (for filename/origin/open operations).
    sarc::storage::ObjectGetResult meta{};
    sarc::core::Status s = sarc::storage::object_get_metadata(key, &meta);
    if (!sarc::core::is_ok(s)) {
        print_status_error("get(metadata)", s);
        return;
    }

    const std::string key_str = object_key_string(key);
    char hash_hex[65];
    hash_to_hex(key.content, hash_hex, sizeof(hash_hex));
    const std::string hash_str = std::string(hash_hex);

    // Determine output filename (preserve extension when possible).
    std::string suggested = meta.filename[0] ? std::string(meta.filename) : hash_str;

    namespace fs = std::filesystem;
    fs::path out_path = fs::path(suggested);

    // If user wants to open the file/dir and didn't provide an output path,
    // materialize under views to avoid littering the current working directory.
    if (!output_override && (open_file || open_dir)) {
        std::error_code ec;
        purge_views_folder(cfg.views_root);

        const fs::path zone_dir = fs::path(cfg.views_root) / std::to_string(key.zone.v);
        (void)fs::create_directories(zone_dir, ec);

        std::string view_name = hash_str;
        if (meta.filename[0]) {
            const std::string safe = fs::path(meta.filename).filename().string();
            const std::string truncated = truncate_filename_preserving_ext(safe, 200);
            view_name += "_";
            view_name += truncated;
        }

        out_path = zone_dir / fs::path(view_name).filename();
    }

    if (output_override && output_override[0] != '\0') {
        fs::path o = fs::path(output_override);
        std::error_code ec;
        const bool o_is_dir = fs::exists(o, ec) && fs::is_directory(o, ec);
        const bool ends_with_slash = (std::strlen(output_override) > 0 &&
                                      (output_override[std::strlen(output_override) - 1] == '/' ||
                                       output_override[std::strlen(output_override) - 1] == '\\'));
        if (o_is_dir || ends_with_slash) {
            out_path = o / fs::path(suggested).filename();
        } else {
            out_path = o;
        }
    } else {
        // Avoid clobber when defaulting to filename.
        std::error_code ec;
        if (meta.filename[0] && fs::exists(out_path, ec)) {
            out_path = fs::path(hash_str + "_" + fs::path(meta.filename).filename().string());
        }
    }

    // Allocate buffer.
    sarc::core::u64 buffer_size = meta.meta.size_bytes;
    const size_t alloc_size = static_cast<size_t>(buffer_size > 0 ? buffer_size : 1);
    sarc::core::u8* buffer = static_cast<sarc::core::u8*>(malloc(alloc_size));
    if (!buffer) {
        print_error("get: out of memory");
        return;
    }

    sarc::storage::ObjectGetResult result{};
    s = sarc::storage::object_get(key, buffer, buffer_size, &result);
    if (!sarc::core::is_ok(s)) {
        free(buffer);
        print_status_error("get", s);
        return;
    }

    const std::string out_s = out_path.string();
    s = write_file(out_s.c_str(), buffer, result.bytes_read);
    free(buffer);

    if (!sarc::core::is_ok(s)) {
        print_error("get: failed to write output file");
        return;
    }

    printf("%s  ->  %s  (%llu bytes)\n",
           key_str.c_str(),
           out_s.c_str(),
           static_cast<unsigned long long>(result.bytes_read));

    // Open operations (best effort).
    if (open_file) {
        open_with_default_app(out_path);
    }
    if (open_dir) {
        const fs::path dir = out_path.has_parent_path() ? out_path.parent_path() : fs::path(".");
        open_with_default_app(dir);
    }
    if (open_origin_file) {
        if (result.origin_path[0] == '\0') {
            fprintf(stderr, "error: get: no origin_path recorded for %s (re-put from a file path to capture origin)\n", key_str.c_str());
        } else {
            open_with_default_app(fs::path(result.origin_path));
        }
    }
    if (open_origin_dir) {
        if (result.origin_path[0] == '\0') {
            fprintf(stderr, "error: get: no origin_path recorded for %s (re-put from a file path to capture origin)\n", key_str.c_str());
        } else {
            const fs::path op = fs::path(result.origin_path);
            const fs::path dir = op.has_parent_path() ? op.parent_path() : op;
            open_with_default_app(dir);
        }
    }
}

void handle_list(const CliConfig& cfg, int argc, char** argv) {
    bool all = false;
    for (int i = 0; i < argc; ++i) {
        const char* a = argv[i];
        if (!a) continue;
        if (std::strcmp(a, "--all") == 0 || std::strcmp(a, "-a") == 0) {
            all = true;
            continue;
        }
        fprintf(stderr, "error: ls: unknown option %s\n", a);
        return;
    }

    // Query all objects in zone
    sarc::storage::ObjectQueryFilter filter{
        .filename_pattern = nullptr,
        .mime_type_pattern = nullptr,
        .min_size_bytes = 0,
        .max_size_bytes = 0,
        .created_after = 0,
        .created_before = 0,
        .limit = 1000
    };

    sarc::storage::ObjectQueryResult results[1000];
    sarc::core::u32 count = 1000;

    sarc::core::Status s = sarc::storage::object_query(cfg.zone, filter, results, &count);

    if (!sarc::core::is_ok(s)) {
        print_status_error("list", s);
        return;
    }

    if (count == 0) {
        printf("No objects found in zone %u\n", cfg.zone.v);
        return;
    }

    printf("Zone %u: %u objects\n", cfg.zone.v, count);
    if (all) {
        printf("%-6s  %-8s  %-20s  %-12s  %s\n", "RID", "Zone", "Filename", "Size", "Key");
    } else {
        printf("%-6s  %-8s  %s\n", "RID", "Zone", "Filename");
    }
    printf("%s\n",
           "--------------------------------------------------------------------------------");

    for (sarc::core::u32 i = 0; i < count; ++i) {
        char hash_hex[65];
        hash_to_hex(results[i].key.content, hash_hex, sizeof(hash_hex));
        const char* name = results[i].filename[0] ? results[i].filename : "<blob>";
        
        if (all) {
            printf("%-6u  %-8u  %-20s  %-12llu  %u:%s\n",
                   i + 1,
                   results[i].key.zone.v,
                   name,
                   static_cast<unsigned long long>(results[i].meta.size_bytes),
                   results[i].key.zone.v,
                   hash_hex);
        } else {
            printf("%-6u  %-8u  %s\n",
                   i + 1,
                   results[i].key.zone.v,
                   name);
        }
    }
}

void handle_zone(CliConfig& cfg, int argc, char** argv) {
    if (argc == 0) {
        printf("zone = %u\n", cfg.zone.v);
        return;
    }
    if (argc != 1 || !argv[0]) {
        print_error("zone: expected: zone <id>");
        return;
    }
    sarc::core::u32 zone_val = 0;
    if (!parse_u32_strict(argv[0], &zone_val)) {
        print_error("zone: invalid zone id");
        return;
    }
    cfg.zone = sarc::core::ZoneId{zone_val};
    printf("zone = %u\n", cfg.zone.v);
}

void handle_search(const CliConfig& cfg, const char* query) {
    if (!query) {
        print_error("search: missing query");
        return;
    }
    std::string q = query;
    while (!q.empty() && (q.back() == '\n' || q.back() == '\r')) q.pop_back();
    if (q.empty()) {
        print_error("search: empty query");
        return;
    }

    // Build RID map from current listing.
    std::vector<sarc::storage::ObjectQueryResult> objs;
    sarc::core::Status s = query_objects(cfg.zone, 10000, &objs);
    if (!sarc::core::is_ok(s)) {
        print_status_error("search(list)", s);
        return;
    }
    std::map<std::string, size_t> rid_by_key;
    rid_by_key.clear();
    for (size_t i = 0; i < objs.size(); ++i) {
        rid_by_key[object_key_string(objs[i].key)] = i + 1;
    }

    sarc::storage::ObjectSearchResult results[100];
    sarc::core::u32 count = 100;
    s = sarc::storage::object_search(cfg.zone, q.c_str(), results, &count);
    if (!sarc::core::is_ok(s)) {
        print_status_error("search", s);
        return;
    }

    if (count == 0) {
        printf("No matches in zone %u\n", cfg.zone.v);
        return;
    }

    printf("Zone %u: %u matches\n", cfg.zone.v, count);
    printf("%-6s  %-8s  %-20s  %s\n", "RID", "Zone", "Filename", "Key");
    printf("%s\n", "--------------------------------------------------------------------------------");

    for (sarc::core::u32 i = 0; i < count; ++i) {
        const std::string key = object_key_string(results[i].key);
        const auto it = rid_by_key.find(key);
        const size_t rid = (it == rid_by_key.end()) ? 0 : it->second;

        const char* name = results[i].filename[0] ? results[i].filename : "<blob>";
        char hash_hex[65];
        hash_to_hex(results[i].key.content, hash_hex, sizeof(hash_hex));
        char rid_buf[32];
        const char* rid_str = "-";
        if (rid) {
            std::snprintf(rid_buf, sizeof(rid_buf), "%zu", rid);
            rid_str = rid_buf;
        }
        printf("%-6s  %-8u  %-20s  %u:%s\n",
               rid_str,
               results[i].key.zone.v,
               name,
               results[i].key.zone.v,
               hash_hex);
    }
}

void handle_gc(const CliConfig& cfg, int argc, char** argv) {
    (void)argc;
    (void)argv;

    sarc::core::u64 deleted = 0;
    sarc::core::Status s = sarc::storage::object_gc(cfg.zone, &deleted);

    if (!sarc::core::is_ok(s)) {
        print_status_error("gc", s);
        return;
    }

    printf("Garbage collected %llu objects from zone %u\n",
           static_cast<unsigned long long>(deleted),
           cfg.zone.v);
}

void handle_verify(const CliConfig& cfg, int argc, char** argv) {
    (void)cfg;  // Zone is in the key itself

    if (argc < 1) {
        print_error("verify: missing object key");
        return;
    }

    // Parse key (reuse logic from handle_get)
    const char* key_str = argv[0];
    const char* colon = strchr(key_str, ':');
    if (!colon) {
        print_error("verify: invalid key format (expected zone:hash)");
        return;
    }

    // Parse zone
    sarc::core::u32 zone_val = 0;
    for (const char* p = key_str; p < colon; ++p) {
        if (*p < '0' || *p > '9') {
            print_error("verify: invalid zone in key");
            return;
        }
        zone_val = zone_val * 10 + (*p - '0');
    }

    // Parse hash
    const char* hash_str = colon + 1;
    sarc::core::Hash256 hash{};
    if (strlen(hash_str) != 64) {
        print_error("verify: hash must be 64 hex characters");
        return;
    }

    for (size_t i = 0; i < 32; ++i) {
        char hex[3] = {hash_str[i*2], hash_str[i*2+1], 0};
        char* end;
        unsigned long val = strtoul(hex, &end, 16);
        if (*end != '\0') {
            print_error("verify: invalid hex in hash");
            return;
        }
        hash.b[i] = static_cast<sarc::core::u8>(val);
    }

    sarc::core::ObjectKey key{sarc::core::ZoneId{zone_val}, hash};

    bool valid = false;
    sarc::core::Status s = sarc::storage::object_verify(key, &valid);

    if (!sarc::core::is_ok(s)) {
        print_status_error("verify", s);
        return;
    }

    if (valid) {
        printf("Object is valid\n");
    } else {
        printf("Object is CORRUPT\n");
    }
}

// ========================================================================
// Main
// ========================================================================

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    // Install signal handler
    signal(SIGINT, sigint_handler);

    // Configuration
    CliConfig cfg;

    // Default storage locations.
    const char* home = std::getenv("HOME");
    if (home && *home) {
        cfg.data_root = std::string(home) + "/sarc/objects";
        cfg.db_path = std::string(home) + "/sarc/sarc.db";
        cfg.views_root = std::string(home) + "/sarc/views";
    } else {
        cfg.data_root = "/tmp/sarc/objects";
        cfg.db_path = "/tmp/sarc/sarc.db";
        cfg.views_root = "/tmp/sarc/views";
    }

    std::error_code ec;
    std::filesystem::create_directories(cfg.data_root, ec);
    std::filesystem::create_directories(std::filesystem::path(cfg.db_path).parent_path(), ec);
    std::filesystem::create_directories(cfg.views_root, ec);
    purge_views_folder(cfg.views_root);

    // Initialize object store
    sarc::storage::ObjectStoreConfig store_cfg{
        .data_root = cfg.data_root.c_str(),
        .db_path = cfg.db_path.c_str(),
        .max_object_bytes = 0,
        .compression = sarc::storage::CompressionPolicy::Auto,
        .verify_on_read = false
    };

    sarc::core::Status s = sarc::storage::object_store_init(store_cfg);
    if (!sarc::core::is_ok(s)) {
        print_status_error("object store initialization", s);
        return EXIT_FAILURE;
    }

    // Command specs
    sarc::cli::CommandSpec g_commands[] = {
        {sarc::cli::CommandId::Help, "help"},
        {sarc::cli::CommandId::Put, "put"},
        {sarc::cli::CommandId::Get, "get"},
        {sarc::cli::CommandId::List, "list"},
        {sarc::cli::CommandId::Gc, "gc"},
        {sarc::cli::CommandId::Verify, "verify"},
        {sarc::cli::CommandId::Exit, "q"},
        {sarc::cli::CommandId::Exit, "quit"},
        {sarc::cli::CommandId::Exit, "exit"},
    };
    sarc::core::u32 command_count = sizeof(g_commands) / sizeof(g_commands[0]);

    // Welcome banner
    printf("SARC Object Store - Interactive Mode\n");
    printf("zone=%u\n", cfg.zone.v);
    printf("data_root=%s\n", cfg.data_root.c_str());
    printf("db_path=%s\n", cfg.db_path.c_str());
    printf("views_root=%s\n", cfg.views_root.c_str());
    printf("Type 'help' for commands, 'q' to quit\n\n");

    // REPL loop
    while (g_running) {
        printf("sarc> ");
        fflush(stdout);

        char line[1024];
        if (!fgets(line, sizeof(line), stdin)) {
            break;  // EOF (Ctrl-D)
        }

        // Parse line into tokens
        int cmd_argc = 0;
        char* cmd_argv[32];
        parse_line(line, &cmd_argc, cmd_argv, 32);

        if (cmd_argc == 0) {
            continue;  // Empty line
        }

        // Command aliases and helper shorthands.
        if (cmd_argv[0]) {
            auto replace_cmd0 = [&](const char* name) {
                free(cmd_argv[0]);
                cmd_argv[0] = dup_token(name);
            };
            auto insert_arg = [&](int pos, const char* value) {
                if (cmd_argc >= 32) return false;
                for (int j = cmd_argc; j > pos; --j) {
                    cmd_argv[j] = cmd_argv[j - 1];
                }
                cmd_argv[pos] = dup_token(value);
                ++cmd_argc;
                return true;
            };

            const char* cmd0 = cmd_argv[0];

            // Interactive search: "/" then prompt (or allow inline: "/ foo bar").
            if (std::strcmp(cmd0, "/") == 0) {
                std::string q;
                if (cmd_argc > 1) {
                    for (int i = 1; i < cmd_argc; ++i) {
                        if (!cmd_argv[i]) continue;
                        if (!q.empty()) q.push_back(' ');
                        q += cmd_argv[i];
                    }
                } else {
                    printf("query> ");
                    fflush(stdout);
                    char qline[1024];
                    if (!fgets(qline, sizeof(qline), stdin)) {
                        free_argv(cmd_argv, cmd_argc);
                        continue;
                    }
                    q = qline;
                }

                handle_search(cfg, q.c_str());
                free_argv(cmd_argv, cmd_argc);
                continue;
            }

            // zone / z: local (not part of parse_command).
            if (std::strcmp(cmd0, "zone") == 0 || std::strcmp(cmd0, "z") == 0) {
                handle_zone(cfg, cmd_argc - 1, cmd_argv + 1);
                free_argv(cmd_argv, cmd_argc);
                continue;
            }

            // Sister get shorthands:
            // oRID => --open-origin
            // eRID => --open-origin-dir
            // .RID => --open
            // ,RID => --open-dir
            if (cmd0[0] != '\0' && cmd0[1] != '\0') {
                const char pfx = cmd0[0];
                if (pfx == 'o' || pfx == 'e' || pfx == '.' || pfx == ',') {
                    const char* rid_str = cmd0 + 1;
                    bool all_digits = true;
                    for (const char* p = rid_str; *p; ++p) {
                        if (*p < '0' || *p > '9') {
                            all_digits = false;
                            break;
                        }
                    }
                    if (all_digits) {
                        const char* opt =
                            (pfx == 'o') ? "--open-origin" :
                            (pfx == 'e') ? "--open-origin-dir" :
                            (pfx == '.') ? "--open" :
                                           "--open-dir";

                        char* fake_argv[2];
                        fake_argv[0] = const_cast<char*>(rid_str);
                        fake_argv[1] = const_cast<char*>(opt);
                        handle_get(cfg, 2, fake_argv);

                        free_argv(cmd_argv, cmd_argc);
                        continue;
                    }
                }
            }

            // ls/lsa aliases for list.
            if (std::strcmp(cmd0, "ls") == 0) {
                replace_cmd0("list");
            } else if (std::strcmp(cmd0, "lsa") == 0) {
                replace_cmd0("list");
                (void)insert_arg(1, "--all");
            } else if (std::strcmp(cmd0, "g") == 0) {
                replace_cmd0("get");
            }
        }

        // Parse command
        sarc::cli::CommandInvocation cmd;
        sarc::core::u32 consumed = 0;
        sarc::cli::CliArgs args{cmd_argv, static_cast<sarc::core::u32>(cmd_argc)};
        s = sarc::cli::parse_command(args, g_commands, command_count, &cmd, &consumed);

        if (!sarc::core::is_ok(s)) {
            printf("error: unknown command\n");
            free_argv(cmd_argv, cmd_argc);
            continue;
        }

        // Get remaining args for handler
        int handler_argc = cmd.args.argc;
        const char* const* handler_argv_const = cmd.args.argv;
        char** handler_argv = const_cast<char**>(handler_argv_const);

        // Dispatch
        switch (cmd.id) {
            case sarc::cli::CommandId::Help:
                handle_help();
                break;
            case sarc::cli::CommandId::Put:
                handle_put(cfg, handler_argc, handler_argv);
                break;
            case sarc::cli::CommandId::Get:
                handle_get(cfg, handler_argc, handler_argv);
                break;
            case sarc::cli::CommandId::List:
                handle_list(cfg, handler_argc, handler_argv);
                break;
            case sarc::cli::CommandId::Gc:
                handle_gc(cfg, handler_argc, handler_argv);
                break;
            case sarc::cli::CommandId::Verify:
                handle_verify(cfg, handler_argc, handler_argv);
                break;
            case sarc::cli::CommandId::Exit:
                g_running = 0;
                break;
            default:
                printf("error: unknown command\n");
                break;
        }

        free_argv(cmd_argv, cmd_argc);
    }

    // Cleanup
    printf("Goodbye!\n");
    s = sarc::storage::object_store_shutdown();
    if (!sarc::core::is_ok(s)) {
        print_status_error("object store shutdown", s);
    }

    return EXIT_SUCCESS;
}
