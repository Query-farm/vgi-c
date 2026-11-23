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

The SDK speaks VGI protocol `vgi.v3` / 3.4.0, with schemas generated from
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
execution and catalog state are implemented in C. All 270 captured function
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

Validated on Linux aarch64 for vgi.v3 / 3.4.0 with `ci/run-integration.sh`
and `ci/run-vendor.sh` (suite and native Haybarn runner both at Query-farm/vgi
`e99683f`, branch `protocol-3.4`, vgi-rpc-c `protocol-3.4`, with
`VGI_ENGINE_CORRELATED_NAMED_ARGS=1` and
`VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS=1` from its `make engine_test_env`):

| Check | Result |
| --- | --- |
| Subprocess SQL suite (422 staged) | 354 executed, all passed; 68 fixture-dependent skips |
| Launcher SQL suite (422 staged) | 355 executed, all passed; 67 fixture-dependent skips |
| HTTP SQL suite (419 staged) | 349 executed, all passed; 70 fixture-dependent skips |
| Protected bearer lane (HTTP) | 2 passed |
| Blob-vendor lane: `test/sql/external_storage/*` | 18 passed (all 6 applicable `arguments_ref*.test`); 2 expected skips (`arguments_ref_state_ref.test`, single replica; `fetch_loopback_refused.test`) |
| Blob-vendor lane: HTTP SQL suite with everything externalized | 349 executed, all passed; 70 fixture-dependent skips; bearer lane 2 passed |
| Native C tests | 9 groups passed, including the nested-binary corpus |
| Schema and IPC interoperability (vgi-python `protocol-3.4` 33099b4) | 264 exact schemas, 84 method signatures, 266 round trips, 9 malformed streams |
| RPC, continuation and cancellation checks (vgi-python `protocol-3.4` 33099b4) | passed |
| Orchard hosted-mode tests (`tests/check_hosted.py`) | 67 passed |

The SQL skips require other fixture applications or services, such as the
writable-table (`txfix`) worker, the extension's own visibility and
write-contract fixtures, secret-lookup fixtures, dynamic-code workers, companion
targets, Docker, and HTTP-specific version fixtures in the subprocess lane. They
are not counted as executed passes. This is a tested protocol/SDK parity
baseline, not a claim to implement every optional deployment feature of the
separate `vgi-rpc-c++` package.
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
  --url http://127.0.0.1:PORT --expect vgi.v3,conformance.Secondary.v1 --identity
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

A table-in-out function may answer one input with several replies, such as one
upstream API page per reply. `process` (or `resume`) sets `call->more_output`;
the reply then says `vgi.status: HAVE_MORE_OUTPUT`, and the client asks for the
next with a continuation tick that the worker answers through `resume`. Keep the
position to continue from in `call->state`, created by `init`; the stream keeps
it between turns on every transport, so keep it small. Registration requires
`init` with `resume` and refuses a catalog `max_workers` of 1. Ticks are strict:
a tick with nothing pending, a tick with rows, real input while output is
pending, and cache control on a tick's reply fail the stream. A blended function
may set FunctionInfo `input_rows_per_call` (a positive integer) to cap the input
rows of one call; the client splits each chunk accordingly.

`VgiFunctionCall` exposes decoded arguments, settings, secrets, the output
schema, execution ID, per-execution storage, and current filter state.
`VgiArgumentSchema()` accompanies typed argument views. Use `VgiRpcLog()` for
messages on the protocol channel; standard output is reserved for IPC.

`<vgi/catalog.h>` supports immutable catalog definitions and an application
callback for dynamic operations. `<vgi/constraints.h>` builds the canonical
`ConstraintInfo` records that `TableInfo.constraints` carries under `vgi.v3`
(`VgiConstraintRecordEncode`; there is no derivation from Arrow nullability).
Registering a declarative catalog checks every table the way the extension does
at ATTACH: ENFORCED + ALL_ROWS records owned by the table and its one-component
schema, unique ids and declared names, one PRIMARY KEY, resolvable column paths,
same-schema NO ACTION / RESTRICT foreign keys whose key exists, and CHECKs that
compare numeric columns and literals. A catalog whose tables carry records
offers `vgi.constraint.records.read.v1` and `vgi.expression.duckdb.scalar.v1`
and selects what the client's `ClientCapabilities.catalog` offer accepts; the
encode/decode/select helpers are public for custom attach handlers. Served
tables are nullable except for fields a NOT_NULL or PRIMARY KEY record names.
The memory catalog keeps metadata only, so it refuses constrained CREATE TABLE. `<vgi/cache.h>` renders cache advertisements;
`<vgi/partition.h>` serializes typed partition bounds. Protocol 3.1 distribution
metadata is set by field name in a plan callback: `ScanSplit.key_value`,
`range_index`, `column_bounds`, `location_ids`, and the plan's `key_grouping`,
`range_partitioning`, and `locations`. Split sealing preserves those fields;
`example-worker/splits.c` (`split_partitioned`) builds an identity
`KeyGrouping` and one-row key tuples. `<vgi/filter.h>` applies
predicates, tracks dynamic revisions, and exposes diagnostics and discrete
column values.

Protocol 3.2 filtered scan planning: a table whose `TableInfo.scan_planning` is
`VGI_SCAN_PLANNING_FILTERED` receives the query's predicates in
`catalog_table_scan_branches_get`. A catalog handler decodes them with
`VgiScanPlanningFiltersParse()` against the table's declared columns (NULL when
none were sent) and prunes candidate files or branches with
`VgiFiltersMayMatch()`, which keeps anything its `VgiColumnStatistics` cannot
rule out. Predicates are advisory: returning extra branches is correct, dropping
one that holds a matching row is not. Zero branches are legal for a filtered
table. `ScanBranch.row_count` and `column_statistics` (a `<vgi/statistics.h>`
batch) are set by field name; the client prunes branches whose statistics or
`branch_filter` contradict the query. Served tables default `scan_planning` to
`"bind"`, and a table with another value, or a filtered table that inlines
`scan_function`, is refused. `example-worker/scan_planning.c` implements the
shared suite's `planned_days*` and `planned_events` fixtures; the latter records
each planning call's days and `VgiFiltersDescribe()` lines (one stable
description per pushed predicate, as vgi-python's fixture writes them).

Protocol 3.4 moves vgi-rpc to upload-URL protocol v2 (`VGI-Upload-URL-Support:
2`: sized, SHA-256-checked client uploads), makes `vgi_rpc.location.sha256`
mandatory on request pointers, and adds large values (vgi
`docs/large-values-3.4.md`):

- **Argument references.** `BindRequest.arguments` is nullable and
  `arguments_location` (`struct<ref, size, sha256>`, the last field) names an
  argument object the client uploaded once through the worker's
  `__upload_url__` grant. Exactly one of the two is set; anything else fails
  with `invalid_bind_request`. The worker resolves the location at each
  request's entry (`read_context`) through vgi-rpc-c's object cache
  (`VgiRpcResolveArgumentsLocation`), so `call->arguments` always holds the
  arguments. A missing object answers `arguments_ref_missing` with "this
  prepared statement's large argument has expired; prepare it again"; a bad
  reference `invalid_arguments_ref`, other bytes `arguments_ref_integrity`.
  With a blob vendor the HTTP server sends `VGI-Arguments-Ref-Bytes`
  (`VGI_ARGUMENTS_REF_BYTES`, default 4194304), the capability signal;
  `VGI_TEST_DISABLE_ARGUMENTS_REF=1` turns it off.
- **The received bind call is kept.** `VgiFunctionCall.bind_call_bytes` holds
  the bind call exactly as the client sent it, captured before sealed attach
  and transaction values are opened; `bind_request` is the decoded working copy
  and is never re-encoded. Table-buffering persistence stores those bytes (the
  reference form), sealed with the storage key: they carry the bind's secrets.
- **Split fingerprints** hash an `arguments_location` as size and SHA-256,
  never its sealed `ref`.
- **Single-replica streams.** vgi-c keeps HTTP streams in process, so its
  `init` stays in memory; it has no cursor and no stored `StateRef`.

The request schemas are regenerated from vgi-python, and
`conformance/nested-binary/` is the shared corpus `tests/nested_binary_test.c`
checks (byte identity of `bind_call`, typed construction, null arguments,
re-encoding after opening sealed values, fingerprints, sealed persistence).

### Blob vendor and storage externalization

With `VGI_BLOB_VENDOR_URL` and `VGI_BLOB_VENDOR_TOKEN` set, the HTTP server
(through vgi-rpc-c's `VgiHttpConfigureFromEnv`) externalizes result batches of
at least `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` through the vendor, grants client
uploads from it (`VGI_RPC_MAX_UPLOAD_BYTES`, never above
`VGI_RPC_MAX_FETCH_BYTES`), resolves argument references from it, and sends
`VGI-Arguments-Ref-Bytes`. `VGI_STORAGE_EXTERNALIZE=1` additionally envelopes
every worker-storage value (`0x00 || raw`, or `0x01 || u64be size || sha256 ||
object_id`) and keeps values of at least `VGI_STORAGE_EXTERNALIZE_BYTES` as
vendor blobs (purpose `fs`), verified against the pointer on every read.

The example worker serves the extension's `blob_test` catalog
(`example-worker/blob_fixtures.c`, `blob_storage.c`; metadata captured from
vgi-python's fixture worker into `generated/blob_catalog.arrow`), so the
extension's vendor lane runs against it.

Protocol 3.3 function visibility: `FunctionInfo.visibility` is null (public),
`VGI_FUNCTION_VISIBILITY_PUBLIC` or `VGI_FUNCTION_VISIBILITY_TABLE_ONLY`;
registration refuses anything else. Every function a table names (its scan and
write functions) must be listed so the client knows its capabilities. Write
functions are `table_only`; a scan is `table_only` unless it is also meant to be
called from SQL. A function registered with `visibility = "table_only"` is added
to its schema's listing in each declarative catalog of the same name that does
not list it already, so register it before that catalog. Protocol 3.3 also
removes `catalog_table_scan_function_get` (catalogs answer
`catalog_table_scan_branches_get`, one branch for a single-source table), and
`catalog_table_create` answers `created`: the memory catalog returns false only
for `IF NOT EXISTS` on an existing table. `<vgi/storage.h>` supplies transactions, key/value state, and
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
`VGI_RPC_MAX_REQUEST_BYTES` (the request-body cap, default 64 MiB), the blob
vendor variables above,
`VGI_CORS_ORIGIN`, `VGI_ACCESS_LOG` / `--access-log`, and
`VGI_OPTIONAL_BEARER_TOKENS`. Access logs contain routing and identity information,
not credentials or argument values. Attach-option secrets remain in private
server storage; attachment cookies and split plans are sealed on HTTP.

### Orchard hosted mode

`VGI_ORCHARD_HOSTED=1` runs the worker as an Orchard *Field Worker*; the hosted
base image sets it. Callers present a *Pass*: a short-lived
[RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT access token issued by
the Orchard Front Desk for one canonical Desk Address. The worker verifies every
Pass locally and accepts no other credential. `VgiWorkerRun` and
`VgiWorkerServeHttp` both apply it (`<vgi/hosted.h>`); the design is the
extension repository's `docs/location-delegation.md`.

```sh
VGI_ORCHARD_HOSTED=1 \
VGI_JWT_ISSUER=https://auth.queryfarm.cloud \
VGI_JWT_AUDIENCE=https://desk.queryfarm.cloud/d/dep_123 \
VGI_JWT_JWKS_URI=https://auth.queryfarm.cloud/.well-known/jwks.json \
VGI_SIGNING_KEY="$(openssl rand -hex 32)" \
./build/example-worker/vgi-example-worker --http 8080
```

**Startup fails closed** (an error and a non-zero exit) unless
`VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and `VGI_JWT_JWKS_URI` each hold exactly
one non-empty value (no commas) and `VGI_SIGNING_KEY` is at least 32 bytes
(Orchard sets 64 hex characters; it must be the same on every machine of the
deployment). It also refuses `VGI_BEARER_TOKENS`, `VGI_OPTIONAL_BEARER_TOKENS`,
`VGI_RPC_GRANT_KEYS` or `--grant-key`, `VGI_OAUTH_RESOURCE`,
`VGI_PROXY_PROOF_MODE` other than `off`, identity hooks (`resolve_token`,
`mint_grant`), a custom `authenticate` callback, standard input/output,
`--unix` and `--tcp`, `VGI_JWT_ALGORITHMS` naming `none` or `HS*`, and a
`VGI_ORCHARD_HOSTED` value other than `1`/`0`/`true`/`false`/`yes`/`no`/`on`/`off`.

**Verification** (vgi-rpc-c's `<vgi_rpc/jwt.h>`, OpenSSL): `ES256` only
(`VGI_JWT_ALGORITHMS` can name ES384/512, RS*, PS*); `typ` `at+jwt`
(case-insensitive, `application/` optional); exact `iss` and exactly one
`aud`; `exp` required; `exp`, `nbf` and `iat` checked with 60 s of skew; `sub`
a non-empty string. The JWKS is cached for at most 10 minutes or a shorter
`Cache-Control: max-age`; once expired, the keys are discarded and a failed
re-fetch answers `503` with `Retry-After`, never stale keys. An unknown `kid`
re-fetches at most once per 60 s.

**Every request needs a Pass** except `HEAD /health` and CORS preflights,
including `GET /health`, other `OPTIONS` requests and unknown paths. Without a
valid one the answer is `401` with `WWW-Authenticate: Bearer
error="invalid_token"` (no `resource_metadata`), `VGI-Auth-Reason:
missing_credential|invalid_credential|expired_credential` and the capability
headers. Authentication runs before the body is read, size-checked or decoded;
an oversized body gets `413` only after a valid Pass. The request cap is
`VGI_RPC_MAX_REQUEST_BYTES`, 32 MiB by default in hosted mode (64 MiB otherwise). CORS is open (`*`), and
`Authorization` is never logged.

**Function code** reads the Pass from `VgiFunctionCall.rpc`: `principal` is
`sub`, `auth_domain` is `"orchard"` and `auth_claims_json` is the verified
claim set as signed, so claims keep their JSON types. In hosted mode the example
worker also registers `main.pass_claims(x BIGINT) -> VARCHAR`, the fixture of
the shared conformance harness (`test/hosted/` in the extension repository),
which `tests/check_hosted.py` and CI run against it.

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
