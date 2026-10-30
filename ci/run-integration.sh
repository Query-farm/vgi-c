#!/usr/bin/env bash
# Run the canonical Query-farm/vgi integration sqllogictest suite against the C
# example worker. CI uses the native runner built at the pinned VGI commit
# (RUNNER_LINKS_EXTENSIONS=1); a local run can use the standalone
# haybarn-unittest with the signed community extension instead.
#
# The single vgi-example-worker binary is routed into each catalog by the
# ci/wrappers/* scripts (which set VGI_WORKER_CATALOG_NAME).
#
# Required environment:
#   VGI_SRC          path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST path to the haybarn-unittest (or native unittest) binary
#   VGI_WORKER_BIN   path to the built vgi-example-worker
# Optional:
#   TRANSPORT        stdio | launch | http   (default stdio)
#   STAGE            scratch dir for the preprocessed test tree (default: mktemp)
#   RUNNER_LINKS_EXTENSIONS  1 for a matching native runner; keep require gates
#   MIN_EXECUTED     executed-case floor (see below)
set -uo pipefail # not -e: the suite exit code is managed explicitly

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_WORKER_BIN:?path to the built vgi-example-worker}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"
TRANSPORT="${TRANSPORT:-stdio}"
INSTALL_EXTENSIONS=1
if [ "${RUNNER_LINKS_EXTENSIONS:-0}" = "1" ]; then INSTALL_EXTENSIONS=0; fi
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }
# Without --test-config the runner turns any error whose text contains "HTTP"
# or "Unable to connect" into a SKIP that exits 0. The config comes from the
# same checkout as the suite.
TEST_CONFIG="$VGI_SRC/test/configs/no_error_skip.json"
[ -f "$TEST_CONFIG" ] || { echo "::error::VGI suite is missing $TEST_CONFIG"; exit 1; }

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD for the standalone runner; on
# the http lane it also injects `LOAD httpfs` before each ATTACH.
# Out of scope: writable/ and simple_writable/ (generic write-path fixture
# workers this SDK does not ship) and nested_type_combinations.test (segfaults
# the prebuilt runner; a property of that build, not the worker).
# ---------------------------------------------------------------------------
AWK_HTTP=0
HTTP_SKIP=()
RUNNER_SKIP=()
if [ "$INSTALL_EXTENSIONS" = "1" ]; then
  # The standalone runner's DuckDB/spatial build keeps a Filter above the pushed
  # spatial predicate in EXPLAIN, so the plan-shape assertion fails for every
  # worker (the C++ reference worker included); the native runner covers it.
  RUNNER_SKIP=(-not -name 'expression_filter.test')
fi
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  # One POST round trip per two rows; covered by the stdio lane.
  HTTP_SKIP=(-not -name 'projection_pushdown_repro.test')
fi
echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT) ..."
mkdir -p "$STAGE/test/sql/integration"
if ! ( cd "$INTEGRATION" || exit 1
  find . -name '*.test' \
       -not -path '*/writable/*' -not -path '*/simple_writable/*' \
       -not -name 'nested_type_combinations.test' \
       ${HTTP_SKIP[@]+"${HTTP_SKIP[@]}"} ${RUNNER_SKIP[@]+"${RUNNER_SKIP[@]}"} | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")" || exit 1
    awk -v http="$AWK_HTTP" -v install="$INSTALL_EXTENSIONS" -f "$HERE/preprocess-require.awk" "$f" \
        > "$STAGE/test/sql/integration/$f" || exit 1
  done ); then
  echo "::error::failed to stage the VGI test corpus" >&2
  exit 1
fi
# The database-worker package tests package this fixture by a path relative to
# the unittest working directory; stage it with its executable bit.
DATABASE_WORKER_FIXTURE="$VGI_SRC/test/support/database_worker_fixture.sh"
[ -f "$DATABASE_WORKER_FIXTURE" ] || { echo "::error::VGI suite is missing $DATABASE_WORKER_FIXTURE" >&2; exit 1; }
mkdir -p "$STAGE/test/support" || exit 1
install -m 0755 "$DATABASE_WORKER_FIXTURE" "$STAGE/test/support/database_worker_fixture.sh" || exit 1

# Background http workers, killed on exit.
BG_PIDS=()
cleanup() { for p in "${BG_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# boot_http_worker <executable> [env=val ...]: start an HTTP server on an
# ephemeral port and set BOOTED_PORT to the port it announces (PORT:<n>). Not
# wrapped in $(...), so the worker stays a direct child. Runs from $STAGE, the
# unittest's working directory, so relative COPY targets resolve identically.
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1"; shift
  local log pid port=""
  log="$(mktemp)"
  BOOTED_PORT=""
  ( cd "$STAGE" || exit 1; for kv in "$@"; do export "$kv"; done; exec "$exe" --http 0 ) >"$log" 2>&1 &
  pid=$!
  BG_PIDS+=("$pid")
  for _ in $(seq 1 80); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.25
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never announced a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

export VGI_WORKER_BIN
export VGI_DATABASE_PACKAGE_WORKER="$VGI_WORKER_BIN"
# Shared by the native-branch fixtures and the COPY-TO targets of the tests.
VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$STAGE/branches}"
export VGI_TEST_BRANCH_DIR
mkdir -p "$VGI_TEST_BRANCH_DIR"

WV="$HERE/wrappers/vgi-worker-versioned"
WVT="$HERE/wrappers/vgi-worker-versioned-tables"
WAO="$HERE/wrappers/vgi-worker-attach-options"
WBP="$HERE/wrappers/vgi-worker-bad-protocol"
# attach/versioned_tables_*_http and attach/versioning_http attach an http://
# worker regardless of the main transport.
boot_versioned_http() {
  boot_http_worker "$WVT" && export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
  boot_http_worker "$WV" && export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
}

case "$TRANSPORT" in
  stdio)
    # Subprocess transport: the only lane that spawns a worker per connection,
    # so the only one that may host the crash / pool-recovery tests.
    export VGI_TEST_WORKER="$VGI_WORKER_BIN"
    export VGI_TEST_DEDICATED_WORKER="$VGI_WORKER_BIN"
    export VGI_VERSIONED_WORKER="$WV"
    export VGI_VERSIONED_TABLES_WORKER="$WVT"
    export VGI_ATTACH_OPTIONS_WORKER="$WAO"
    export VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$WAO"
    export VGI_BAD_PROTOCOL_WORKER="$WBP"
    boot_versioned_http
    ;;
  launch)
    # AF_UNIX launcher transport (pooled workers).
    export VGI_TEST_WORKER="launch:${VGI_WORKER_BIN}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    export VGI_VERSIONED_WORKER="launch:${WV}"
    export VGI_VERSIONED_TABLES_WORKER="launch:${WVT}"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${WAO}"
    export VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:${WAO}"
    export VGI_BAD_PROTOCOL_WORKER="launch:${WBP}"
    boot_versioned_http
    ;;
  http)
    # Whole suite over HTTP. Optional bearer identities let
    # cache/identity_isolation.test attach as alice and bob; bearer_auth/* runs
    # separately against a protected server below.
    boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example" \
      "VGI_OPTIONAL_BEARER_TOKENS=vgi-test-alice=alice,vgi-test-bob=bob"
    export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    export VGI_HTTP_TRANSPORT=1
    boot_versioned_http
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected stdio|launch|http)"; exit 1 ;;
esac

cd "$STAGE" || { echo "::error::cannot enter integration stage: $STAGE" >&2; exit 1; }

if [ "$INSTALL_EXTENSIONS" = "1" ]; then
  echo "Warming the extension cache (vgi from community, dependencies from core) ..."
  mkdir -p "$STAGE/test" || exit 1
  # FORCE INSTALL picks up the currently published community build.
  cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;

statement ok
INSTALL spatial FROM core;
EOF
  "$HAYBARN_UNITTEST" --test-dir "$PWD" "test/_warm.test" >/dev/null 2>&1 ||
    echo "::warning::extension warm-up did not fully succeed; require gates remain authoritative"
  rm -f "$STAGE/test/_warm.test"
fi

# ---------------------------------------------------------------------------
# Skip contract and executed-case floor. A failed `require` / `require-env` is
# a SKIP, so "All tests passed" alone proves nothing ran. Every skip reason must
# be listed here, and the executed count must stay above MIN_EXECUTED.
# ---------------------------------------------------------------------------
EXPECTED_SKIP_REASONS=(
  'require-env VGI_BAD_ENUM_WORKER'              # malformed-ENUM fixture, not wired here
  'require-env VGI_RULES_WORKER'                 # vgi-rust multibatch-repro fixture
  'require-env VGI_SCHEMA_RECONCILE_DB'          # schema-reconcile sqlite fixture
  'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE' # dynamic-code registration, not implemented
  'require-env VGI_HTTP_DISABLE_ZSTD'            # gzip-fallback fixture server
  'require-env VGI_HTTP_NO_COMPRESSION'          # no-compression fixture server (Python-side)
  'require-env VGI_TEST_BEARER_TOKEN'            # bearer-auth server; run separately on http
  'require-env VGI_TEST_COMPANION_TARGET'        # companion-catalog fixture (Python-side)
  'require-env VGI_DOCKER_IMAGE'                 # containerised worker lane
  'require-env VGI_DOCKER_TCP_IMAGE'             # containerised worker over TCP
  'require-env VGI_GITHUB_NETWORK_TESTS'         # hits github.com; opt-in only
  'require-env VGI_DATABASE_BUN_WORKER'          # Bun package fixture belongs to its SDK lane
  'require-env VGI_DATABASE_PYTHON_WORKER'       # Python package fixture belongs to its SDK lane
  'require-env VGI_DATABASE_RUST_WORKER'         # Rust package fixture belongs to its SDK lane
  'require-env VGI_MALFORMED_BATCH_WORKER'       # hostile malformed-Arrow fixture (vgi test/support)
  'require-env VGI_ROWID_CONSTRAINT_WORKER'      # hostile rowid-constraint fixture (vgi test/support)
  'require-env VGI_ATTACH_OPTIONS_HTTP_WORKER'   # attach-options over http, not booted here
  'require-env VGI_TEST_ICEBERG'                 # multi-branch Iceberg fixture tree
)
if [ "${RUNNER_LINKS_EXTENSIONS:-0}" = "1" ]; then
  # The development runner omits spatial; no signed spatial build matches it.
  EXPECTED_SKIP_REASONS+=('require spatial')
fi
case "$TRANSPORT" in
  stdio)
    EXPECTED_SKIP_REASONS+=(
      'require-env VGI_REQUIRE_LAUNCHER_TRANSPORT'
      'require-env VGI_HTTP_TRANSPORT' # http-identity tests
    )
    MIN_EXECUTED="${MIN_EXECUTED:-250}" ;;
  launch)
    EXPECTED_SKIP_REASONS+=(
      'require-env VGI_TEST_DEDICATED_WORKER'
      'require-env VGI_HTTP_TRANSPORT'
    )
    MIN_EXECUTED="${MIN_EXECUTED:-245}" ;;
  http)
    EXPECTED_SKIP_REASONS+=(
      'require-env VGI_TEST_DEDICATED_WORKER'
      'require-env VGI_REQUIRE_LAUNCHER_TRANSPORT'
      'require-env VGI_VERSIONED_WORKER'
      'require-env VGI_VERSIONED_TABLES_WORKER'
      'require-env VGI_ATTACH_OPTIONS_WORKER'
      'require-env VGI_ATTACH_OPTIONS_REQUIRED_WORKER'
      'require-env VGI_BAD_PROTOCOL_WORKER'
    )
    MIN_EXECUTED="${MIN_EXECUTED:-240}" ;;
esac

TOTAL_EXECUTED=0
TOTAL_SKIPPED=0
# run_unittest <glob...>: run one invocation, streaming its report, then fail on
# what the exit code cannot express: a forked child's fatal-signal report, an
# empty stage, or an unlisted skip reason.
run_unittest() {
  local log unittest_rc total skipped reason
  log="$(mktemp)"
  "$HAYBARN_UNITTEST" --test-dir "$PWD" --test-config "$TEST_CONFIG" "$@" 2>&1 | tee "$log"
  unittest_rc="${PIPESTATUS[0]}"
  if grep -q 'due to a fatal error condition' "$log"; then
    echo "::error::a forked child ran the test harness's signal handler; the parent" \
         "exited $unittest_rc and would otherwise have passed."
    unittest_rc=1
  fi
  if grep -q 'No test cases matched\|No tests ran' "$log"; then
    echo "::error::the runner matched no test cases (transport=$TRANSPORT)"
    rm -f "$log"
    return 1
  fi
  total="$(sed -n 's/^\[[0-9]*\/\([0-9]*\)\].*/\1/p' "$log" | tail -1)"
  if [ -z "$total" ]; then
    echo "::error::could not parse a test-case total out of the report"
    rm -f "$log"
    return 1
  fi
  skipped="$(awk '
    /^Skipped tests for the following reasons:/ { b = 1; next }
    b && /^[[:space:]]*$/                       { b = 0; next }
    b && match($0, /: [0-9]+[[:space:]]*$/)     { n += substr($0, RSTART + 2) }
    END { print n + 0 }' "$log")"
  echo "  executed: $(( total - skipped )) / $total   skipped: $skipped   (transport=$TRANSPORT)"
  while IFS= read -r reason; do
    [ -n "$reason" ] || continue
    printf '%s\n' "${EXPECTED_SKIP_REASONS[@]}" | grep -qxF "$reason" && continue
    echo "::error::unexpected skip reason '$reason'. Either a gate regressed and a whole" \
         "file stopped running, or the skip is legitimate and belongs in EXPECTED_SKIP_REASONS."
    unittest_rc=1
  done < <(awk '
    /^Skipped tests for the following reasons:/ { b = 1; next }
    b && /^[[:space:]]*$/                       { b = 0; next }
    b && match($0, /: [0-9]+[[:space:]]*$/)     { print substr($0, 1, RSTART - 1) }' "$log")
  TOTAL_EXECUTED=$(( TOTAL_EXECUTED + total - skipped ))
  TOTAL_SKIPPED=$(( TOTAL_SKIPPED + skipped ))
  rm -f "$log"
  return "$unittest_rc"
}

echo "Running suite (transport=$TRANSPORT) ..."
rc=0
if [ "$TRANSPORT" = "http" ]; then
  run_unittest "test/sql/integration/*" "~test/sql/integration/bearer_auth/*" || rc=$?
  echo "Running bearer_auth/* against a bearer-protected http worker ..."
  boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example" \
    "VGI_BEARER_TOKENS=test-secret-token=test-principal"
  ( export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    export VGI_TEST_BEARER_TOKEN="test-secret-token"
    run_unittest "test/sql/integration/bearer_auth/*" ) || rc=$?
else
  run_unittest "test/sql/integration/*" || rc=$?
fi

if [ "$TOTAL_EXECUTED" -lt "$MIN_EXECUTED" ]; then
  echo "::error::only $TOTAL_EXECUTED test cases executed on the $TRANSPORT lane; the" \
       "floor is MIN_EXECUTED=$MIN_EXECUTED. Find what stopped running; do not lower it."
  rc=1
else
  echo "Executed $TOTAL_EXECUTED test cases (floor $MIN_EXECUTED), $TOTAL_SKIPPED skipped, on the $TRANSPORT lane."
fi
exit "$rc"
