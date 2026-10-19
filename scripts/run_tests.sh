#!/usr/bin/env bash
# Run the authoritative extension suite against the native C worker.
set -euo pipefail
VGI_C_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VGI_EXT="${VGI_EXT:-$HOME/Development/vgi}"
VGI_C_BUILD="${VGI_C_BUILD:-$VGI_C_ROOT/build}"
VGI_C_TEST_CACHE="${VGI_C_TEST_CACHE:-/tmp/vgi-c-test-cache}"
mkdir -p "$VGI_C_TEST_CACHE"
if [[ "${1:-}" == --no-build ]]; then shift; else cmake --build "$VGI_C_BUILD" --parallel 8; fi
VGI_C_BINARY="$VGI_C_BUILD/example-worker/vgi-example-worker"
VGI_C_UNITTEST="$VGI_EXT/build/release/test/unittest"
VGI_C_CONFIG="$VGI_EXT/test/configs/no_error_skip.json"
[[ -x "$VGI_C_BINARY" && -x "$VGI_C_UNITTEST" && -f "$VGI_C_CONFIG" ]] || { echo 'Build the C worker and VGI extension, including test/configs/no_error_skip.json.' >&2; exit 1; }
# Environment variables avoid interpolating paths into executable shell code.
export VGI_C_BINARY VGI_C_TEST_CACHE
cat > "$VGI_C_TEST_CACHE/worker.sh" <<'WRAPPER'
#!/usr/bin/env bash
exec "$VGI_C_BINARY" "$@" 2>>"$VGI_C_TEST_CACHE/worker.log"
WRAPPER
chmod +x "$VGI_C_TEST_CACHE/worker.sh"
: > "$VGI_C_TEST_CACHE/worker.log"
export VGI_TEST_WORKER="$VGI_C_TEST_CACHE/worker.sh"
export VGI_DATABASE_PACKAGE_WORKER="$VGI_TEST_WORKER"
for catalog in versioned versioned_tables attach_options; do
    cat > "$VGI_C_TEST_CACHE/$catalog.sh" <<WRAPPER
#!/usr/bin/env bash
export VGI_WORKER_CATALOG_NAME=$catalog
exec "\$VGI_C_BINARY" "\$@" 2>>"\$VGI_C_TEST_CACHE/worker.log"
WRAPPER
    chmod +x "$VGI_C_TEST_CACHE/$catalog.sh"
done
cat > "$VGI_C_TEST_CACHE/bad_protocol.sh" <<'WRAPPER'
#!/usr/bin/env bash
export VGI_PROTOCOL_VERSION_OVERRIDE=99.0.0
exec "$VGI_C_BINARY" "$@" 2>>"$VGI_C_TEST_CACHE/worker.log"
WRAPPER
chmod +x "$VGI_C_TEST_CACHE/bad_protocol.sh"
export VGI_VERSIONED_WORKER="$VGI_C_TEST_CACHE/versioned.sh"
export VGI_VERSIONED_TABLES_WORKER="$VGI_C_TEST_CACHE/versioned_tables.sh"
export VGI_ATTACH_OPTIONS_WORKER="$VGI_C_TEST_CACHE/attach_options.sh"
export VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$VGI_ATTACH_OPTIONS_WORKER"
export VGI_BAD_PROTOCOL_WORKER="$VGI_C_TEST_CACHE/bad_protocol.sh"
export VGI_TEST_DEDICATED_WORKER=1
export VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$VGI_C_TEST_CACHE/branches}"
mkdir -p "$VGI_TEST_BRANCH_DIR"
HTTP_PIDS=()
stop_http_workers() {
    for pid in ${HTTP_PIDS[@]+"${HTTP_PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
    for pid in ${HTTP_PIDS[@]+"${HTTP_PIDS[@]}"}; do wait "$pid" 2>/dev/null || true; done
}
trap stop_http_workers EXIT
start_http_worker() {
    local name=$1 catalog=$2
    shift 2
    local output="$VGI_C_TEST_CACHE/http-$name.out"
    : > "$output"
    (cd "$VGI_EXT"
     export VGI_WORKER_CATALOG_NAME="$catalog"
     for variable in "$@"; do export "$variable"; done
     exec "$VGI_C_BINARY" --http 0 >"$output" 2>>"$VGI_C_TEST_CACHE/worker.log") &
    HTTP_PIDS+=($!)
    local port=""
    for ((attempt=0; attempt<100; ++attempt)); do
        port=$(sed -n 's/^PORT:\([0-9]*\)$/\1/p' "$output" | head -1)
        [[ -n "$port" ]] && break
        sleep 0.1
    done
    [[ -n "$port" ]] || { echo "HTTP worker $name failed to start; see $VGI_C_TEST_CACHE/worker.log" >&2; return 1; }
    HTTP_URL="http://127.0.0.1:$port"
}
if [[ "${VGI_HTTP:-0}" == 1 ]]; then
    unset VGI_TEST_DEDICATED_WORKER
    start_http_worker example example VGI_OPTIONAL_BEARER_TOKENS=vgi-test-alice=alice,vgi-test-bob=bob
    export VGI_TEST_WORKER="$HTTP_URL" VGI_HTTP_TRANSPORT=1
    start_http_worker versioned versioned
    export VGI_VERSIONED_HTTP_WORKER="$HTTP_URL"
    start_http_worker versioned_tables versioned_tables
    export VGI_VERSIONED_TABLES_HTTP_WORKER="$HTTP_URL"
fi
filters=()
if [[ $# == 0 ]]; then filters=('test/sql/integration/*'); fi
for category in "$@"; do
    if [[ "$category" == test/* ]]; then filters+=("$category"); else filters+=("test/sql/integration/$category/*"); fi
done
filter="$(IFS=,; echo "${filters[*]}")"
cd "$VGI_EXT"
status=0
"$VGI_C_UNITTEST" --test-dir "$VGI_EXT" --test-config "$VGI_C_CONFIG" "$filter" 2>&1 | tee "$VGI_C_TEST_CACHE/run.log" || status=$?
if [[ $# == 0 ]]; then
    start_http_worker bearer example VGI_BEARER_TOKENS=test-secret-token=test-principal
    VGI_TEST_WORKER="$HTTP_URL" VGI_TEST_BEARER_TOKEN=test-secret-token \
        "$VGI_C_UNITTEST" --test-dir "$VGI_EXT" --test-config "$VGI_C_CONFIG" 'test/sql/integration/bearer_auth/*' 2>&1 | tee "$VGI_C_TEST_CACHE/bearer.log" || status=1
fi
exit "$status"
