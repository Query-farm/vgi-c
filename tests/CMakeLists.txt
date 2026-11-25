add_executable(vgi-schema-test schema_test.c)
target_link_libraries(vgi-schema-test PRIVATE vgi::vgi)
add_test(NAME schema COMMAND vgi-schema-test)

add_executable(vgi-ipc-roundtrip ipc_roundtrip.c)
target_link_libraries(vgi-ipc-roundtrip PRIVATE vgi::vgi)

add_executable(vgi-metadata-test metadata_test.c)
target_link_libraries(vgi-metadata-test PRIVATE vgi::vgi PkgConfig::SODIUM)
add_test(NAME metadata COMMAND vgi-metadata-test)
add_executable(vgi-split-test split_test.c)
target_link_libraries(vgi-split-test PRIVATE vgi::vgi PkgConfig::SODIUM)
add_test(NAME split COMMAND vgi-split-test)
if(UNIX)
    # PROTOCOL_VERSIONS_V1 (advertisement, refusal, VGI_TEST_PROTOCOL_RANGE)
    # and the C SDK's refusal of direct S3/GCS storage, against the example worker.
    if(TARGET vgi-example-worker)
        add_executable(vgi-protocol-versions-test protocol_versions_test.c)
        target_link_libraries(vgi-protocol-versions-test PRIVATE vgi::vgi)
        add_test(NAME protocol_versions
            COMMAND vgi-protocol-versions-test $<TARGET_FILE:vgi-example-worker>)
    endif()
    add_executable(vgi-state-test state_test.c)
    target_link_libraries(vgi-state-test PRIVATE vgi::vgi)
    add_test(NAME state COMMAND vgi-state-test)
endif()

add_executable(vgi-credentials-test credentials_test.c)
target_link_libraries(vgi-credentials-test PRIVATE vgi::vgi PkgConfig::JSONC PkgConfig::SODIUM)
add_test(NAME credentials COMMAND vgi-credentials-test "${CMAKE_CURRENT_SOURCE_DIR}/data/attach_ticket_vectors.json" "${CMAKE_CURRENT_SOURCE_DIR}/data/grant_token_vectors.json")
add_executable(vgi-filter-test filter_test.c)
target_link_libraries(vgi-filter-test PRIVATE vgi::vgi)
add_test(NAME filter COMMAND vgi-filter-test)
add_executable(vgi-registration-test registration_test.c)
target_link_libraries(vgi-registration-test PRIVATE vgi::vgi)
add_test(NAME registration COMMAND vgi-registration-test)
add_executable(vgi-constraints-test constraints_test.c)
target_link_libraries(vgi-constraints-test PRIVATE vgi::vgi)
add_test(NAME constraints COMMAND vgi-constraints-test)
# Protocol 3.4 nested-binary conformance against the shared corpus.
add_executable(vgi-nested-binary-test nested_binary_test.c)
target_include_directories(vgi-nested-binary-test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(vgi-nested-binary-test PRIVATE vgi::vgi PkgConfig::JSONC PkgConfig::SODIUM)
add_test(NAME nested_binary COMMAND vgi-nested-binary-test
    "${PROJECT_SOURCE_DIR}/conformance/nested-binary")
# Secret scope matching (VgiScopeMatches, VgiSecretSelect) against DuckDB's own
# rule: 3025 shared vectors generated from the engine's C++ (duckdb#28684,
# authority boundary), vendored verbatim from vgi-python.
add_executable(vgi-secret-scope-test secret_scope_test.c)
target_link_libraries(vgi-secret-scope-test PRIVATE vgi::vgi PkgConfig::JSONC)
add_test(NAME secret_scope COMMAND vgi-secret-scope-test
    "${CMAKE_CURRENT_SOURCE_DIR}/data/scope_match_vectors.json")
