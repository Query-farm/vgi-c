cmake_minimum_required(VERSION 3.20)
project(vgi-c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
option(VGI_BUILD_TESTS "Build C tests and interoperability tools" ON)
option(VGI_BUILD_EXAMPLES "Build protocol inspection example" ON)
option(VGI_SANITIZE "Enable address and undefined-behavior sanitizers" OFF)
if(VGI_SANITIZE AND NOT MSVC)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()
# Module fallbacks for dependencies some platforms ship without CMake packages.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
option(VGI_USE_INSTALLED_RPC "Use an installed vgi-rpc-c package" OFF)
set(VGI_RPC_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../vgi-rpc-c" CACHE PATH "vgi-rpc-c source checkout")
if(VGI_USE_INSTALLED_RPC)
    find_package(vgi_rpc 0.1 CONFIG REQUIRED)
else()
    if(NOT EXISTS "${VGI_RPC_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR "Clone vgi-rpc-c beside vgi-c or set VGI_RPC_SOURCE_DIR")
    endif()
    set(VGI_RPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(VGI_RPC_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    add_subdirectory("${VGI_RPC_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/vgi-rpc")
endif()
# CMAKE_C_EXTENSIONS OFF compiles with -std=c11, which on glibc hides the POSIX
# and BSD declarations (clock_gettime, getline, fdopen, kill, ...) that macOS
# and the BSDs expose by default. Expose them on Linux too.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_compile_definitions(_DEFAULT_SOURCE)
endif()
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)
if(NOT TARGET SQLite3::SQLite3)
    add_library(SQLite3::SQLite3 ALIAS SQLite::SQLite3)
endif()
pkg_check_modules(JSONC REQUIRED IMPORTED_TARGET json-c)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)
add_library(vgi src/batch_named.c src/worker.c src/opaque.c src/arguments.c src/storage.c src/filter.c src/catalog.c src/memory_catalog.c src/statistics.c src/context.c src/copy.c src/cache.c src/partition.c src/split.c src/generated/protocol.c)
add_library(vgi::vgi ALIAS vgi)
target_sources(vgi PRIVATE src/attach_options.c src/credentials.c src/tickets.c)
target_include_directories(vgi PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_include_directories(vgi PRIVATE src)
target_link_libraries(vgi PUBLIC vgi_rpc::vgi_rpc)
target_link_libraries(vgi PRIVATE PkgConfig::JSONC PkgConfig::SODIUM Threads::Threads SQLite3::SQLite3)
if(UNIX)
    target_link_libraries(vgi PRIVATE m)
endif()
set_target_properties(vgi PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
if(MSVC)
    target_compile_options(vgi PRIVATE /W4)
else()
    target_compile_options(vgi PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(VGI_BUILD_EXAMPLES)
    add_subdirectory(example-worker)
    add_executable(vgi-protocol-info examples/protocol_info.c)
    target_link_libraries(vgi-protocol-info PRIVATE vgi::vgi)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
install(TARGETS vgi EXPORT vgi-targets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(DIRECTORY include/vgi DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES LICENSE DESTINATION "${CMAKE_INSTALL_DATADIR}/licenses/vgi-c")
install(EXPORT vgi-targets NAMESPACE vgi:: DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi")
configure_package_config_file(cmake/vgi-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/vgi-config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi")
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/vgi-config-version.cmake"
    VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMinorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/vgi-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/vgi-config-version.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi")

if(VGI_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
