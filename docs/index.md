# VGI for C

`vgi-c` implements VGI workers using nanoarrow and the separate
[vgi-rpc-c](https://github.com/Query-farm/vgi-rpc-c) library.

| Layer | Repository | Responsibilities |
| --- | --- | --- |
| RPC | vgi-rpc-c | IPC, clients, transports, reflection, identity, stream state, HTTP, external payloads |
| VGI application | vgi-c | Function execution, catalogs, filtering, COPY, cache metadata, split plans, attach tickets |
| Canonical schemas | vgi-python / vgi-rpc-python | VGI and RPC protocol definitions respectively |

The [README](../README.md) documents the SDK APIs and the implemented VGI feature
areas. Begin with [the example worker](../example-worker/main.c) and
[protocol inspection example](../examples/protocol_info.c).

## Build

Clone `vgi-rpc-c` beside `vgi-c`, then configure and build:

```sh
cmake -S . -B build -DBUILD_SHARED_LIBS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Set `VGI_RPC_SOURCE_DIR` for another checkout. To test the installed package
boundary, install `vgi-rpc-c` into a prefix and configure this project with
`-DVGI_USE_INSTALLED_RPC=ON -DCMAKE_PREFIX_PATH=/path/to/prefix`.
The exported VGI consumer target is `vgi::vgi`.

Public compatibility headers such as `vgi/rpc.h` forward to `vgi_rpc/rpc.h`.
Applications can include the RPC headers directly. Generic credentials and
Identity operations belong to the RPC package; VGI attach-ticket helpers remain
in `vgi/credentials.h`.

## Generation

`scripts/regenerate_protocol.sh` runs the bundled C schema generator in the
`vgi-python` environment. It imports live model definitions and exports them
through Arrow's C Data Interface, preserving dictionaries, nullability, and
metadata. `VGI_PYTHON` selects the reference checkout. Use `--check` to detect
drift. The standalone generator is included so regeneration does not require
unpublished changes to the Python repository.

## Validation

`scripts/run_tests.sh` runs the canonical extension tests from
`~/Development/vgi`, with strict `no_error_skip.json`. `VGI_HTTP=1` selects HTTP.
The example worker needs no Python or C++ process to execute methods.

The final validation used the 371-case canonical inventory: 328 subprocess cases
and 336 HTTP cases passed, with 43 and 35 fixture-dependent skips respectively.
The separate bearer lane passed 2 cases and 13 assertions for each transport.
Six native test groups also passed. The RPC package has independent shared
server, native-client, sanitizer, and HTTPS validation documented in its
[conformance guide](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/conformance.md).
