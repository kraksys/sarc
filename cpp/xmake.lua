set_languages("c++23")
set_warnings("all")

add_requires("gtest", { configs = { main = true } })
add_requires("benchmark")
add_requires("blake3")
add_requires("sqlite3")
add_requires("libsodium", { system = true, optional = true })
add_requires("openssl", { system = true, optional = true })

target("sarc")
set_kind("static")
set_symbols("debug")
set_optimize("none")
add_cxxflags("-fPIC")
add_includedirs("include", { public = true })
add_headerfiles("include/(sarc/**.hpp)")
add_packages("blake3")
if has_package("libsodium") then
    add_packages("libsodium")
    add_defines("SARC_HAVE_LIBSODIUM=1", { public = true })
end
if has_package("openssl") then
    add_packages("openssl")
    add_defines("SARC_HAVE_OPENSSL=1", { public = true })
end
if has_package("sqlite3") then
    add_packages("sqlite3", { public = true })
    add_defines("SARC_HAVE_SQLITE3=1", { public = true })
end
add_files(
    "src/core/**.cpp",
    "src/security/**.cpp",
    "src/net/**.cpp",
    "src/storage/**.cpp",
    "src/db/**.cpp",
    "src/cli/options.cpp",
    "src/cli/commands.cpp"
)

target("sarc_cli")
set_kind("binary")
add_deps("sarc")
add_includedirs("include")
add_packages("blake3")
if has_package("libsodium") then add_packages("libsodium") end
if has_package("openssl") then add_packages("openssl") end
add_files("src/cli/**.cpp")

target("sarc_tests")
set_kind("binary")
add_deps("sarc")
add_includedirs("include")
add_packages("gtest")
add_packages("blake3")
if has_package("libsodium") then add_packages("libsodium") end
if has_package("openssl") then add_packages("openssl") end
add_files("tests/**.cpp")
remove_files("tests/**.sync-conflict-*.cpp")

target("sarc_bench")
set_kind("binary")
add_deps("sarc")
add_includedirs("include")
add_packages("benchmark")
add_packages("blake3")
if has_package("libsodium") then add_packages("libsodium") end
if has_package("openssl") then add_packages("openssl") end
if has_package("sqlite3") then add_packages("sqlite3") end
add_files("benchmarks/**.cpp")
remove_files("benchmarks/**.sync-conflict-*.cpp")

-- ============================================================================
-- Erlang Integration Targets
-- ============================================================================

-- NIF shared library
-- Set ERL_INCLUDE environment variable or it will use default path
-- Example: export ERL_INCLUDE=/usr/lib/erlang/usr/include
target("sarc_nif")
    set_kind("shared")
    add_deps("sarc")
    add_includedirs("include")

    -- Add Erlang include path from environment or default location
    if os.getenv("ERL_INCLUDE") then
        add_includedirs(os.getenv("ERL_INCLUDE"))
    else
        -- Common Erlang installation paths
        add_includedirs("/usr/lib64/erlang/usr/include",
                       "/usr/lib/erlang/usr/include",
                       "/usr/local/lib/erlang/usr/include",
                       "/opt/homebrew/lib/erlang/usr/include")
    end

    add_files("src/bindings/erlang_nif.cpp")
    add_packages("blake3")
    if has_package("sqlite3") then add_packages("sqlite3") end
    if has_package("libsodium") then add_packages("libsodium") end
    if has_package("openssl") then add_packages("openssl") end

    -- Output as sarc_nif.so (not libsarc_nif.so)
    set_filename("sarc_nif.so")
    set_prefixname("")

    -- Install to Erlang priv directory after build
    after_build(function (target)
        local priv_dir = path.join(os.projectdir(), "erlang/sarc_gateway/priv")
        os.mkdir(priv_dir)
        os.cp(target:targetfile(), priv_dir)
        print("Installed NIF to: " .. priv_dir)
    end)
target_end()

-- Port executable (standalone, doesn't require Erlang headers)
target("sarc_port")
    set_kind("binary")
    add_deps("sarc")
    add_includedirs("include")
    add_files("src/bindings/erlang_port.cpp")
    add_packages("blake3")
    if has_package("sqlite3") then add_packages("sqlite3") end
    if has_package("libsodium") then add_packages("libsodium") end
    if has_package("openssl") then add_packages("openssl") end

    -- Install to Erlang priv directory after build
    after_build(function (target)
        local priv_dir = path.join(os.projectdir(), "erlang/sarc_gateway/priv")
        os.mkdir(priv_dir)
        os.cp(target:targetfile(), priv_dir)
        os.run("chmod +x " .. path.join(priv_dir, "sarc_port"))
        print("Installed port to: " .. priv_dir)
    end)
target_end()
