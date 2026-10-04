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

cargo test \
    --manifest-path "$ROOT_DIR/Cargo.toml" \
    --features protocol-trace \
    --lib primary_replication_pause

if ! STALE_PRIMARY_EVIDENCE_OUTPUT="$RUN_DIR/live-stale-primary.jsonl" \
    cargo test \
        --manifest-path "$ROOT_DIR/Cargo.toml" \
        --features protocol-trace \
        --test stale_primary_failover \
        -- --nocapture; then
    if [[ -f "$RUN_DIR/live-stale-primary.jsonl" ]]; then
        cat "$RUN_DIR/live-stale-primary.jsonl" >&2
    fi
    exit 1
fi

"$ROOT_DIR/scripts/tla/test_d1_protocol_trace.sh"

python3 "$ROOT_DIR/scripts/tla/sweep_d1_protocol_trace.py" \
    --mode correct \
    --seeds "${D1_TRACE_CI_SEEDS:-16,44,102,149,160}" \
    --output-dir "$RUN_DIR/randomized" \
    --jobs "${D1_TRACE_CI_JOBS:-2}" \
    --tla-timeout "$TLA_TRACE_TIMEOUT_SECONDS" \
    --tla-heap "$TLA_TRACE_HEAP"

echo "D1 protocol trace and live stale-primary CI validation passed."
