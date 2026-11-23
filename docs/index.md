# VGI for C

`vgi-c` implements VGI workers using nanoarrow and the separate
[vgi-rpc-c](https://github.com/Query-farm/vgi-rpc-c) library.

| Layer | Repository | Responsibilities |
| --- | --- | --- |
| RPC | vgi-rpc-c | IPC, clients, transports, shared memory, worker pools, telemetry, reflection, identity, stream state, HTTP, external payloads |
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

Local clients opt into [shared-memory transfer](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/shared-memory.md)
through `VgiClientOptions.shared_memory_size`. The [worker pool](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/pooling.md)
reuses bounded subprocess clients with exclusive leases. Both APIs are available
through the RPC headers. For [OpenTelemetry](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/telemetry.md),
create a telemetry instance and attach it with `VgiWorkerSetTelemetry()` before
serving. The example worker reads `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_SERVICE_NAME`, and the endpoint override `VGI_RPC_OTEL_ENDPOINT`.

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

To exercise shared-memory transfers throughout the subprocess suite, set
`VGI_RPC_SHM_SIZE_BYTES=67108864 VGI_RPC_SHM_MIN_BATCH_BYTES=0` when running the
script. The extension negotiates support before creating a segment. This lane
covers mixed inline and shared-memory calls on pooled workers, buffered functions,
COPY TO, and cached results retained across later requests.

`ci/run-integration.sh` stages the pinned suite for one transport (stdio,
launch or http), enforces an allowlist of fixture-dependent skip reasons and an
executed-case floor, and is what CI runs with the native runner built at the
pinned VGI commit. The vgi.v3 / 3.4.0 validation (Linux aarch64, suite and
native runner at `e99683f`) executed 354 subprocess, 355 launcher and 349 HTTP
cases with no failures, plus the 2-case bearer lane.

`ci/run-vendor.sh` runs the extension's blob-vendor lane (`make
test_http_vendor`) against the C worker: `test/sql/external_storage/*`,
including the protocol 3.4 `arguments_ref*.test` files, one file at a time
with a worker per file, then the HTTP suite with every value externalized
through vgi-rpc-python's fake vendor. It needs `uv` (the fake vendor runs with
`uvx`). `arguments_ref_state_ref.test` is an expected skip: vgi-c keeps streams
in process, so its init is never a stored `StateRef`. A Haybarn runner needs the
`VGI_ENGINE_*` variables from the vgi checkout's `make -s engine_test_env`
exported, as CI's runner download does.
Eight native test groups also passed. The RPC package has independent shared
server, native-client, sanitizer, and HTTPS validation documented in its
[conformance guide](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/conformance.md).

Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`, see the README) has two checks:
`tests/check_hosted.py`, a port of vgi-python's hosted-mode tests plus the C
runtime's JWKS-outage and authentication-before-body rules, and the extension
repository's SDK-neutral harness `test/hosted/run_hosted_conformance.py`, which
CI's hosted job runs against the example worker with stdio, `--unix` and
`--tcp` as refusal commands:

```sh
VGI_C_BUILD=build ../vgi-python/.venv/bin/python -m pytest tests/check_hosted.py
../vgi/test/hosted/run_hosted_conformance.py --worker-cwd . \
  --worker-cmd './build/example-worker/vgi-example-worker --http 0' \
  --refuse-cmd './build/example-worker/vgi-example-worker' \
  --refuse-cmd './build/example-worker/vgi-example-worker --tcp 127.0.0.1:{port}' \
  --refuse-cmd './build/example-worker/vgi-example-worker --unix /tmp/vgi-c-hosted-{port}.sock'
```

The vgi-python environment needs its `oauth` and `http` extras.
