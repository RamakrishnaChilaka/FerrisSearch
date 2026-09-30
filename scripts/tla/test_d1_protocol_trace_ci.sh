#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/ferrissearch-d1-trace-ci.XXXXXX")

cleanup() {
    rm -rf -- "${RUN_DIR:?}"
}
trap cleanup EXIT

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"
export TLA_TRACE_HEAP="${TLA_TRACE_HEAP:-2g}"
export TLA_TRACE_TIMEOUT_SECONDS="${TLA_TRACE_TIMEOUT_SECONDS:-60}"

"$ROOT_DIR/scripts/tla/test_d1_protocol_trace.sh"

python3 "$ROOT_DIR/scripts/tla/sweep_d1_protocol_trace.py" \
    --mode correct \
    --seeds "${D1_TRACE_CI_SEEDS:-16,44,102,149,160}" \
    --output-dir "$RUN_DIR/randomized" \
    --jobs "${D1_TRACE_CI_JOBS:-2}" \
    --tla-timeout "$TLA_TRACE_TIMEOUT_SECONDS" \
    --tla-heap "$TLA_TRACE_HEAP"

echo "D1 protocol trace CI validation passed."
