<p align="center">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-shield.png" alt="Vector Gateway Interface logo" width="280">
</p>

<h1 align="center">vgi-c</h1>
<p align="center">C11 workers for the VGI protocol, built by <a href="https://query.farm">🚜 Query.Farm</a>.</p>

A C11 implementation of the VGI protocol and Arrow IPC payload layer, using
[nanoarrow](https://github.com/rustyconover/arrow-nanoarrow). The runtime compiles
as C and links nanoarrow and its C flatcc runtime. It does not link Arrow C++,
Arrow Compute, DuckDB, or a C++ RPC implementation.

The reusable transport, IPC, reflection, identity, and native client layer is
[vgi-rpc-c](https://github.com/Query-farm/vgi-rpc-c). This repository links that
library and owns VGI-specific worker behavior. See [the documentation](docs/index.md)
for the split and build options.

The SDK targets the `vgi-c++` VGI 2.2.0 behavior, with schemas generated from
`vgi-python`. It implements scalar and table functions, streaming exchanges,
buffered functions, grouped/window/streaming aggregates, immutable and mutable
catalogs, version negotiation and time travel, attach options, settings and
scoped secrets, COPY FROM/TO, cache metadata, partition pruning, dynamic filters,
and sealed split plans. SQLite stores state across worker processes.

Workers run over standard input/output, Unix sockets, TCP, or native HTTP.
HTTP supports bearer authentication, custom identity providers, sealed opaque
state, rotated grant keys, attach tickets, bounded streaming continuations,
request/response compression, cancellation, CORS, and JSON access logs.
Protocol reflection and additional hosted protocols use the same dispatcher.

The RPC library also supplies shared-memory transfers for subprocess and Unix
connections, bounded C client worker pools, and OpenTelemetry traces and metrics.
Shared-memory results own their buffers and remain valid after the connection
closes. See the [shared-memory](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/shared-memory.md),
[pooling](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/pooling.md), and
[telemetry](https://github.com/Query-farm/vgi-rpc-c/blob/main/docs/telemetry.md) guides.

The native dependencies are nanoarrow IPC, json-c, libsodium, SQLite3,
libevent, libcurl, zstd, LZ4, zlib, and platform threads. Python and the C++ worker are
used only for generation and compatibility testing. The example worker's
checked-in Arrow files contain declarative reference metadata; function
execution and catalog state are implemented in C. All 267 captured function
signatures have registered implementations; startup rejects an unsupported
fixture instead of silently advertising it.

## Build

```sh
git clone https://github.com/Query-farm/vgi-rpc-c.git ../vgi-rpc-c
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
./build/vgi-protocol-info
```

The default build uses a sibling `../vgi-rpc-c` checkout. Set
`VGI_RPC_SOURCE_DIR` for another path, or `VGI_USE_INSTALLED_RPC=ON` with
`CMAKE_PREFIX_PATH` to consume an installed RPC package.

The RPC dependency's CMake build fetches `rustyconover/arrow-nanoarrow` at
`4d6dbbb190d9e3fa2718086f8610b375877ed506` (the
`feat/ipc-writer-message-metadata-on-928` branch). This supplies dictionary
IPC support and `ArrowIpcWriterSetMessageMetadata`. The build applies the
encoder fix from your fork's `d83a538b1c0442d9fe4f5ecf697ce681893bed2e`
commit, which preserves embedded NUL bytes in metadata. Both revisions are
pinned because these changes currently live on different branches.

To work on nanoarrow alongside this library:

```sh
cmake -S . -B build-local \
  -DVGI_NANOARROW_SOURCE_DIR="$HOME/Development/arrow-nanoarrow-vgi"
```

The local checkout is used as supplied and must contain the required IPC APIs
and fixes. The build does not apply patches to it. `VGI_NANOARROW_REVISION`
overrides the fetched revision. Nanoarrow's own CMake configuration probes a
C++ compiler, but this project's runtime and dependencies compile only C sources.

## Protocol schemas

`vgi-python` remains the source of truth. The bundled `scripts/generate_protocol.py` generator
exports schemas through the Arrow C Data Interface, preserving nested fields,
dictionary types, nullability, flags, and metadata. Checked-in C output means
ordinary builds do not require Python.

```sh
./scripts/regenerate_protocol.sh
./scripts/regenerate_protocol.sh --check
```

`VGI_PYTHON` defaults to `~/Development/vgi-python`. The bundled generator reads
that checkout's model directly; checked-in generated output keeps ordinary builds
independent of Python.

```c
#include <vgi/generated/protocol.h>

struct ArrowSchema schema = {0};
struct ArrowError error = {{0}};
int status = VgiSchemaCatalogInfo(&schema, &error);
if (status == 0) {
    /* Inspect or build an ArrowArray against schema. */
    ArrowSchemaRelease(&schema);
}
```

`VgiSchemaInit(name, ...)` looks up schemas by name. `VGI_METHODS` and
`VgiMethodFind()` describe every method's parameters, response envelope,
payload schema when statically known, and stream header. This inventory
describes the protocol independently of the functions and catalogs an
application registers. Attach-ticket schemas are generated by the same command.
Reflection and Framework Identity schemas belong to `vgi-rpc-c` and are generated
from the `vgi-rpc` reference; only configured Identity methods are advertised.

## IPC and ownership

`<vgi/ipc.h>` provides `VgiIpcWriteBatch()` and `VgiIpcReadBatch()` for one-batch
IPC streams, including dictionary messages and per-batch custom metadata.
Input data is borrowed. Successful read outputs own their contents and remain
valid after the input bytes are freed. Outputs must be empty before a call;
failed operations do not retain partially constructed outputs.

Release schemas with `ArrowSchemaRelease()`, arrays with `ArrowArrayRelease()`,
and buffers with `ArrowBufferReset()`. Use nanoarrow's metadata builders for
the length-prefixed metadata representation. The reader requires exactly one
batch and an EOS marker and rejects extra streams or trailing bytes. `<vgi/stream.h>` provides incremental dictionary-aware IPC readers and writers.
`<vgi/rpc.h>` frames unary and streaming calls, structured errors, cancellation,
and in-band logs. The default dependency build reads LZ4 and Zstandard IPC bodies. HTTP separately
negotiates gzip and Zstandard content encoding.

## Validation

The interoperability check uses the reference checkout's Python environment:

```sh
"${VGI_PYTHON:-$HOME/Development/vgi-python}/.venv/bin/python" \
  tests/check_interop.py --build-dir build
```

It imports the compiled schemas through the C Data Interface and compares
them with the live Python model, checks every method signature, and round-trips
every generated schema plus populated, nullable, dictionary, and sliced batches
through C and PyArrow. Malformed, truncated, and multi-batch streams must fail.
Build with `-DBUILD_SHARED_LIBS=ON` for this check. `-DVGI_SANITIZE=ON` enables
AddressSanitizer and UndefinedBehaviorSanitizer for native tests.

The example worker is tested against the canonical extension harness:

```sh
./scripts/run_tests.sh                    # full subprocess suite
./scripts/run_tests.sh --no-build scalar  # one category
VGI_HTTP=1 ./scripts/run_tests.sh          # HTTP suite plus bearer lane
VGI_RPC_SHM_SIZE_BYTES=67108864 VGI_RPC_SHM_MIN_BATCH_BYTES=0 \
  ./scripts/run_tests.sh                  # force shared-memory transfers
```

The runner requires `~/Development/vgi/build/release/test/unittest` and always
supplies the canonical test directory and `no_error_skip.json` configuration.
`VGI_EXT`, `VGI_C_BUILD`, and `VGI_C_TEST_CACHE` override those locations.
Transport-specific tests require their corresponding fixtures. The runner reports
missing fixtures explicitly and uses the strict no-error-skip configuration.
The SHM lane exercises capability negotiation, segment reuse across unary calls,
COPY TO, buffered functions, and retained cached results. Inline catalog/bind
calls can share the same pooled worker connection.

Validated on macOS arm64 against the local reference checkouts:

| Check | Result |
| --- | --- |
| Full subprocess SQL suite | 328 passed, 43 fixture-dependent skips; 12,098 assertions |
| Full subprocess suite, forced shared memory | 328 passed, 43 fixture-dependent skips; 12,098 assertions |
| Full HTTP SQL suite | 336 passed, 35 fixture-dependent skips; 12,260 assertions |
| Protected bearer lane, each run | 2 passed; 13 assertions |
| Native C tests | 6 groups passed |
| Schema and IPC interoperability | 201 exact schemas, 72 method signatures, 203 round trips, 9 malformed streams |
| Installed vgi-rpc-c consumer | Separate package configured, built, and tested |
| Shared RPC conformance, separate vgi-rpc-c suite | 1,692 shared tests passed without skips |
| Native RPC client against Python | 1,397 passed without skips; server-only tests deselected |


The SQL skips require other fixture applications or services, such as the
writable Python worker, dynamic-code workers, companion targets, Docker, and
HTTP-specific version fixtures in the subprocess lane. They are not counted as
executed passes. This is a tested protocol/SDK parity baseline, not a claim to
implement every optional deployment feature of the separate `vgi-rpc-c++` package.
The Identity baseline is its two-method protocol; generated schemas also include
newer Python Identity methods, which are not advertised by this C implementation.

Run the shared ticket test against two C workers:

```sh
cd "$HOME/Development/vgi"
VGI_ATTACH_TICKET_SERVER_CMD="$HOME/Development/vgi-c/build/example-worker/vgi-example-worker --http 0" \
  ./test/run_http_attach_ticket_integration.sh \
  'test/sql/integration/attach_ticket/round_trip.test' \
  --test-dir "$HOME/Development/vgi"
```

For Identity conformance, use a `vgi-rpc` checkout matching the C++ baseline:

```sh
./build/example-worker/vgi-example-worker --http 0 \
  --conformance-identity --introspect-principals conformance-introspector
# Use the printed PORT in another terminal:
"$HOME/Development/vgi-rpc/.venv/bin/vgi-rpc-test-hosted" \
  --url http://127.0.0.1:PORT --expect vgi.v2,conformance.Secondary.v1 --identity
```

The conformance-header and test-bearer options are fixture-only and explicitly
opt in to trusted test identities.

## Worker callbacks

`<vgi/worker.h>` registers functions using Arrow C schemas and callbacks.
`VgiFunctionInit()` initializes mutable FunctionInfo metadata;
`VgiWorkerRegisterFunction()` takes ownership. Bind callbacks return an owned
schema. Process callbacks receive a borrowed input batch and return an owned
output batch and Arrow message metadata. Set `finished` to emit EOS.
`finalize` releases execution state on completion, cancellation, or failure.

`VgiFunctionCall` exposes decoded arguments, settings, secrets, the output
schema, execution ID, per-execution storage, and current filter state.
`VgiArgumentSchema()` accompanies typed argument views. Use `VgiRpcLog()` for
messages on the protocol channel; standard output is reserved for IPC.

`<vgi/catalog.h>` supports immutable catalog definitions and an application
callback for dynamic operations. `<vgi/cache.h>` renders cache advertisements;
`<vgi/partition.h>` serializes typed partition bounds. `<vgi/filter.h>` applies
predicates, tracks dynamic revisions, and exposes diagnostics and discrete
column values. `<vgi/storage.h>` supplies transactions, key/value state, and
atomic append/pop queues for worker pools.

## HTTP and delegated access

```sh
./build/example-worker/vgi-example-worker --http 0
VGI_BEARER_TOKENS='token=principal' ./build/example-worker/vgi-example-worker --http 8080
```

`VgiHttpOptionsInit()` supplies bounded defaults. Use `VgiWorkerServeHttp()` or
set options with `VgiWorkerSetHttpOptions()` before `VgiWorkerRun()`. The identity
callbacks in `<vgi/http.h>` supply the authenticated principal, domain, and
optional authentication time. `<vgi/identity.h>` configures credential-resolution
and grant-minting hooks; introspection requires an explicit principal allowlist.
Identity hooks run concurrently and must be thread-safe. Options and callback
state are borrowed and must outlive the server.

`--grant-key BASE64` (repeatable) or comma-separated `VGI_RPC_GRANT_KEYS` enables
sealed grants. Each key decodes to exactly 32 bytes; the first mints, and all
verify. `VGI_RPC_GRANT_AUDIENCE` and `VGI_RPC_GRANT_MAX_TTL_SECONDS` configure
scope and lifetime. Only a recently authenticated caller can mint a grant;
a grant cannot mint another grant. Configuring grants together with persistent
`VGI_SIGNING_KEY` enables `vgi.attach_tickets.v1`. Redeeming `vgi_attach_ticket`
restores the original catalog, options, and version specifications for its owner.
`<vgi/credentials.h>` also exposes the interoperable sealing primitives.

HTTP CLI configuration includes `VGI_HTTP_HOST`, `VGI_HTTP_NO_COMPRESSION`,
`VGI_CORS_ORIGIN`, `VGI_ACCESS_LOG` / `--access-log`, and
`VGI_OPTIONAL_BEARER_TOKENS`. Access logs contain routing and identity information,
not credentials or argument values. Attach-option secrets remain in private
server storage; attachment cookies and split plans are sealed on HTTP.

Enable OpenTelemetry in the example worker with an OTLP/HTTP collector:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_SERVICE_NAME=vgi-example-worker ./build/example-worker/vgi-example-worker --http 8080
```

`VGI_RPC_OTEL_ENDPOINT` overrides the collector endpoint. Embedders create a
`VgiTelemetry` and call `VgiWorkerSetTelemetry()` before serving. The telemetry
instance must outlive the worker and its serving threads. It propagates W3C trace
context, correlates in-band logs, and exports spans, request counters, and duration
histograms asynchronously. Collector failures do not change RPC results.

## Using the library

Use `add_subdirectory()` and link `vgi::vgi`, or install it:

```sh
cmake --install build --prefix /path/to/install
```

Consumers can then use `find_package(vgi CONFIG REQUIRED)` and link
`vgi::vgi`. Nanoarrow headers and its static IPC dependency are installed too.

Contributor and coding-agent guidance is in [AGENTS.md](AGENTS.md).

## License

Query Farm Source-Available License, Version 1.0; see [LICENSE](LICENSE).
Nanoarrow and its bundled flatcc runtime retain their upstream licenses.
