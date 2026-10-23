# nanoarrow's IPC build calls find_package(zstd) and links zstd's CMake targets.
# Some distributions (Amazon Linux 2023, older Debian/Ubuntu) ship libzstd's
# pkg-config file but no CMake package, so fall back to pkg-config there.
find_package(zstd CONFIG QUIET)
if(NOT zstd_FOUND)
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(VGI_ZSTD QUIET IMPORTED_TARGET GLOBAL libzstd)
    endif()
    if(VGI_ZSTD_FOUND)
        if(NOT TARGET zstd::libzstd_shared)
            add_library(zstd::libzstd_shared INTERFACE IMPORTED GLOBAL)
            target_link_libraries(zstd::libzstd_shared INTERFACE PkgConfig::VGI_ZSTD)
        endif()
        set(zstd_VERSION "${VGI_ZSTD_VERSION}")
    endif()
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zstd REQUIRED_VARS VGI_ZSTD_FOUND VERSION_VAR zstd_VERSION)
endif()
