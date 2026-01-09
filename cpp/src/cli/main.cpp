#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <csignal>
#include <vector>
#include <sys/stat.h>

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
    const char* data_root = "/tmp/sarc_data";
    const char* db_path = "/tmp/sarc.db";
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

// ========================================================================
// Command Handlers
// ========================================================================

void handle_help() {
    printf("Commands:\n");
    printf("  put <file>        Store a file and print its content key\n");
    printf("  get <key> [-o]    Retrieve a file by content key\n");
    printf("  list              List all stored objects\n");
    printf("  gc                Garbage collect deleted objects\n");
    printf("  verify <key>      Verify object integrity\n");
    printf("  help              Show this help\n");
    printf("  q, quit, exit     Exit REPL\n");
    printf("\n");
    printf("Key format: zone:hash (e.g., 0:a3b5c7d9...)\n");
}

void handle_put(const CliConfig& cfg, int argc, char** argv) {
    if (argc < 1) {
        print_error("put: missing filename");
        return;
    }

    const char* filepath = argv[0];

    // Read file
    FileReadResult file;
    sarc::core::Status s = read_file(filepath, &file);
    if (!sarc::core::is_ok(s)) {
        print_error("put: failed to read file");
        return;
    }

    // Put object
    sarc::storage::ObjectPutParams params{
        .zone = cfg.zone,
        .data = file.data,
        .size_bytes = file.size,
        .filename = extract_filename(filepath),
        .mime_type = guess_mime_type(filepath)
    };

    sarc::storage::ObjectPutResult result;
    s = sarc::storage::object_put(params, &result);

    free(file.data);

    if (!sarc::core::is_ok(s)) {
        print_status_error("put", s);
        return;
    }

    // Print key
    char hash_hex[65];
    hash_to_hex(result.key.content, hash_hex, sizeof(hash_hex));
    printf("%u:%s\n", result.key.zone.v, hash_hex);

    if (result.deduplicated) {
        fprintf(stderr, "info: file already exists (deduplicated)\n");
    }
}

void handle_get(const CliConfig& cfg, int argc, char** argv) {
    if (argc < 1) {
        print_error("get: missing object key (format: zone:hash)");
        return;
    }

    // Parse key format "zone:hash"
    const char* key_str = argv[0];
    const char* colon = strchr(key_str, ':');
    if (!colon) {
        print_error("get: invalid key format (expected zone:hash)");
        return;
    }

    // Parse zone
    sarc::core::u32 zone_val = 0;
    for (const char* p = key_str; p < colon; ++p) {
        if (*p < '0' || *p > '9') {
            print_error("get: invalid zone in key");
            return;
        }
        zone_val = zone_val * 10 + (*p - '0');
    }

    // Parse hash
    const char* hash_str = colon + 1;
    sarc::core::Hash256 hash{};
    if (strlen(hash_str) != 64) {
        print_error("get: hash must be 64 hex characters");
        return;
    }

    for (size_t i = 0; i < 32; ++i) {
        char hex[3] = {hash_str[i*2], hash_str[i*2+1], 0};
        char* end;
        unsigned long val = strtoul(hex, &end, 16);
        if (*end != '\0') {
            print_error("get: invalid hex in hash");
            return;
        }
        hash.b[i] = static_cast<sarc::core::u8>(val);
    }

    sarc::core::ObjectKey key{sarc::core::ZoneId{zone_val}, hash};

    // Determine output path (use -o if provided, otherwise hash)
    const char* output_path = hash_str;
    if (argc >= 3 && strcmp(argv[1], "-o") == 0) {
        output_path = argv[2];
    }

    // Check if object exists
    bool exists = false;
    sarc::core::Status s = sarc::storage::object_exists(key, &exists);
    if (!sarc::core::is_ok(s)) {
        print_status_error("get", s);
        return;
    }
    if (!exists) {
        print_error("get: object not found");
        return;
    }

    // Allocate buffer (start with 1MB)
    sarc::core::u64 buffer_size = 1024 * 1024;
    sarc::core::u8* buffer = static_cast<sarc::core::u8*>(malloc(buffer_size));
    if (!buffer) {
        print_error("get: out of memory");
        return;
    }

    // Get object
    sarc::storage::ObjectGetResult result;
    s = sarc::storage::object_get(key, buffer, buffer_size, &result);

    // If buffer too small, realloc and retry
    if (!sarc::core::is_ok(s) && result.meta.size_bytes > buffer_size) {
        free(buffer);
        buffer_size = result.meta.size_bytes;
        buffer = static_cast<sarc::core::u8*>(malloc(buffer_size));
        if (!buffer) {
            print_error("get: out of memory");
            return;
        }
        s = sarc::storage::object_get(key, buffer, buffer_size, &result);
    }

    if (!sarc::core::is_ok(s)) {
        free(buffer);
        print_status_error("get", s);
        return;
    }

    // Write to file
    s = write_file(output_path, buffer, result.bytes_read);
    free(buffer);

    if (!sarc::core::is_ok(s)) {
        print_error("get: failed to write output file");
        return;
    }

    printf("Retrieved %llu bytes to %s\n",
           static_cast<unsigned long long>(result.bytes_read),
           output_path);
}

void handle_list(const CliConfig& cfg, int argc, char** argv) {
    (void)argc;
    (void)argv;

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

    sarc::core::ObjectKey results[1000];
    sarc::core::u32 count = 1000;

    sarc::core::Status s = sarc::storage::object_query(
        cfg.zone, filter, results, &count);

    if (!sarc::core::is_ok(s)) {
        print_status_error("list", s);
        return;
    }

    if (count == 0) {
        printf("No objects found in zone %u\n", cfg.zone.v);
        return;
    }

    printf("Zone %u: %u objects\n", cfg.zone.v, count);
    printf("%-8s  %s\n", "Zone", "Content Hash");
    printf("--------  %s\n",
           "----------------------------------------------------------------");

    for (sarc::core::u32 i = 0; i < count; ++i) {
        char hash_hex[65];
        hash_to_hex(results[i].content, hash_hex, sizeof(hash_hex));
        printf("%-8u  %s\n", results[i].zone.v, hash_hex);
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

    // Create data directory if it doesn't exist
    mkdir(cfg.data_root, 0755);

    // Initialize object store
    sarc::storage::ObjectStoreConfig store_cfg{
        .data_root = cfg.data_root,
        .db_path = cfg.db_path,
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
