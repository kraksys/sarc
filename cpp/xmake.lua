set_languages("c++23")
set_warnings("all")

add_requires("gtest", { configs = { main = true } })
add_requires("benchmark")
add_requires("blake3")
add_requires("libsodium", { system = true, optional = true })
add_requires("openssl", { system = true, optional = true })

target("sarc")
set_kind("static")
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
add_files(
    "src/core/**.cpp",
    "src/security/**.cpp",
    "src/net/**.cpp",
    "src/storage/**.cpp",
    "src/db/**.cpp"
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
add_files("benchmarks/**.cpp")
