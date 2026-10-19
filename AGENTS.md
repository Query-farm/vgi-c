# Working on vgi-c

## Scope and layout

This repository implements VGI application workers in C11. It owns function
execution, catalogs, filtering, COPY, cache metadata, split plans, attach options,
and tickets. Generic RPC clients, transports, shared memory, worker pools,
telemetry, reflection, and identity belong in the sibling `vgi-rpc-c` library.

- `include/vgi/`: public SDK headers and generated protocol declarations.
- `src/`: VGI implementation and internal headers.
- `example-worker/`: the native example worker used by integration tests.
- `scripts/`: schema generation, reference fixtures, and integration runner.
- `tests/`: native tests and Python interoperability checks.
- `docs/`: repository boundaries, build options, and validation notes.

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The default RPC dependency is `../vgi-rpc-c`; override `VGI_RPC_SOURCE_DIR` for
another checkout. Installed-package consumers use `VGI_USE_INSTALLED_RPC=ON`
with `CMAKE_PREFIX_PATH`. Keep the implementation in C and use nanoarrow's C
interfaces. Python and the C++ worker are reference/generation tools, not runtime
dependencies. Format changed C files with `.clang-format`.

## Canonical schemas and fixtures

`vgi-python` supplies the VGI model. The bundled generator reads it directly;
ordinary builds consume checked-in C output and require no Python runtime.

```sh
./scripts/regenerate_protocol.sh
./scripts/regenerate_protocol.sh --check
../vgi-python/.venv/bin/python tests/check_interop.py --build-dir build
```

`VGI_PYTHON` selects the reference checkout. Change generators or canonical
definitions rather than manually editing generated declarations. Preserve field
order, nullability, dictionaries, schema metadata, and method signatures. The
example worker's Arrow fixtures describe reference metadata; execution remains
native C. Refresh fixtures deliberately using the capture scripts.

Follow the ownership contracts in public headers. Release schemas, arrays,
buffers, and execution state on completion, cancellation, and errors. Standard
output carries protocol data; send diagnostic text to standard error.

## Validation

Run native tests for implementation changes and the interoperability check for
schema/IPC changes. Worker behavior or function registration changes need the
full canonical DuckDB integration suite, not just the affected category:

```sh
./scripts/run_tests.sh --no-build
VGI_HTTP=1 ./scripts/run_tests.sh --no-build
VGI_RPC_SHM_SIZE_BYTES=67108864 VGI_RPC_SHM_MIN_BATCH_BYTES=0 \
  ./scripts/run_tests.sh --no-build
```

The runner uses `~/Development/vgi/build/release/test/unittest` by default and the strict
`no_error_skip.json` configuration. `VGI_EXT`, `VGI_C_BUILD`, and
`VGI_C_TEST_CACHE` override locations. Use the HTTP lane for hosting/state changes
and the forced-SHM lane for local transport, buffering, pooling, or retained-result
changes. Run against the rebuilt binary; `--no-build` deliberately skips building.
Report fixture-dependent skips separately from executed passes.

Use `-DVGI_SANITIZE=ON` for memory/ownership changes. Preserve test logs and
inspect them before repeating expensive suites. For documentation changes,
check links and `git diff --check`; for installation changes, stage the install
under a temporary prefix and verify its contents. Changes at the RPC boundary
also need the relevant checks in `vgi-rpc-c` and an installed-package consumer.

## License

The repository uses the Query Farm Source-Available License, Version 1.0. Keep
`LICENSE` identical to `../vgi-python/LICENSE` and the README consistent with it.
Dependencies, including nanoarrow and flatcc, retain their own licenses.
