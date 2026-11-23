#!/usr/bin/env bash
# The extension's blob-vendor lane (vgi `make test_http_vendor`) against the C
# example worker: everything externalized through vgi-rpc-python's fake blob
# vendor (results over 4 KiB are vendor pointers, requests over 32 KiB go up
# through __upload_url__, storage values over 4 KiB are vendor blobs, and large
# bind arguments travel as protocol 3.4 argument references).
#
#   1. test/sql/external_storage/* one file at a time (they compare the
#      vendor's lane-wide counters), a fresh C worker per file, started by the
#      suite's scripts/run_tests.py so each file's `# server-env:` applies;
#   2. the integration suite over HTTP with the vendor environment, through
#      ci/run-integration.sh (its skip contract and executed-case floor).
#
# Required environment:
#   VGI_SRC          a Query-farm/vgi checkout (its scripts/, test/)
#   HAYBARN_UNITTEST the native runner built at that commit
#   VGI_WORKER_BIN   the built vgi-example-worker
# Optional:
#   VGI_EXTERNAL_PROBE_WORKER  the extension's own download-client probe
#                    (fetch_*.test); without it those files skip, as they are
#                    not part of the worker contract
#   PART             external | integration (default: both)
#   STAGE            scratch dir for the integration stage
#
# Skips this lane expects (any other skip fails it):
#   require-env-not VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS
#       fetch_loopback_refused.test needs the loopback hook unset, which this
#       lane always sets (as the extension's own lane does);
#   require-env VGI_WORKER_INIT_STATE_REF
#       arguments_ref_state_ref.test: vgi-c keeps HTTP streams in process
#       (single replica, vgi docs/large-values-3.4.md §6), so its init is never
#       a stored StateRef (puts_state_ref).
set -uo pipefail
: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the native unittest runner}"
: "${VGI_WORKER_BIN:?path to the built vgi-example-worker}"
HERE="$(cd "$(dirname "$0")" && pwd)"
VGI_SRC="$(cd "$VGI_SRC" && pwd)"

# scripts/run_tests.py runs <checkout>/build/release/test/unittest.
mkdir -p "$VGI_SRC/build/release/test"
if [ "$(readlink -f "$HAYBARN_UNITTEST")" != "$(readlink -f "$VGI_SRC/build/release/test/unittest")" ]; then
  ln -sfn "$(readlink -f "$HAYBARN_UNITTEST")" "$VGI_SRC/build/release/test/unittest"
fi

cd "$VGI_SRC" || exit 1
# shellcheck source=/dev/null
source test/start_fake_blob_vendor.sh
trap stop_fake_blob_vendor EXIT
export VGI_WORKER_ARGUMENTS_REF=1 VGI_FAKE_VENDOR_DELETE_ROUTE=1 VGI_HTTP_TRANSPORT=1

rc=0
if [ "${PART:-all}" != integration ]; then
  SKIPS=(--allow-skip "require-env-not VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS"
         --allow-skip "require-env VGI_WORKER_INIT_STATE_REF")
  if [ -z "${VGI_EXTERNAL_PROBE_WORKER:-}" ]; then
    SKIPS+=(--allow-skip "require-env VGI_EXTERNAL_PROBE_WORKER")
  fi
  echo "Running test/sql/external_storage/* against $VGI_WORKER_BIN ..."
  python3 scripts/run_tests.py --build release --server-per-test "$VGI_WORKER_BIN --http 0" \
    "test/sql/external_storage/*" -j 1 "${SKIPS[@]}" || rc=$?
fi
if [ "${PART:-all}" != external ]; then
  echo "Running the integration suite over HTTP with the vendor environment ..."
  ( cd "$HERE/.." && TRANSPORT=http "$HERE/run-integration.sh" ) || rc=$?
fi
echo "fake blob vendor counters: $(python3 -c 'import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=10).read().decode().strip())' "$VGI_BLOB_VENDOR_URL/__stats__" 2>&1)"
exit "$rc"
