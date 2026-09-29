#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/v3"
TIMEOUT_SECONDS="${TLA_TRACE_ROUND4_TIMEOUT_SECONDS:-600}"

run_validator() {
    env TLA_TRACE_TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
        "$VALIDATOR" "$1"
}

run_valid() {
    local label=$1
    local trace=$2
    local output
    output=$(mktemp "${TMPDIR:-/tmp}/ferrissearch-trace-round4-valid.XXXXXX")
    if ! run_validator "$FIXTURES/$trace" >"$output" 2>&1; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Expected accepted round-4 trace: $label ($trace)" >&2
        exit 1
    fi
    cat "$output"
    rm -f -- "$output"
    echo "round4 $label expected=accepted actual=accepted"
}

run_invalid() {
    local label=$1
    local trace=$2
    local step=$3
    local event=$4
    local output
    output=$(mktemp "${TMPDIR:-/tmp}/ferrissearch-trace-round4-invalid.XXXXXX")
    set +e
    run_validator "$FIXTURES/$trace" >"$output" 2>&1
    status=$?
    set -e
    if [[ $status -ne 1 ]] ||
        ! grep -Fq "Trace rejected at schema step $step (event $event)" "$output"; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Expected round-4 rejection: $label ($trace)" >&2
        exit 1
    fi
    cat "$output"
    rm -f -- "$output"
    echo "round4 $label expected=rejected actual=rejected step=$step event=$event"
}

run_valid m4c m4c-stale-term-delivery-received-only.jsonl
run_invalid m4 m4-stale-term-delivery-applied-after-promotion.jsonl 181 wal_appended
run_invalid m6b m6b-noop-fill-before-replay-finished.jsonl 194 promotion_noop_fill
run_valid v6c v6c-promoted-copy-restart-replay-reactivate-fill.jsonl
run_valid m7 m7-truncated-copy-restart-faithful-empty-replay.jsonl
run_valid m7-retained valid-truncated-copy-restart-retained-skip.jsonl
run_invalid m7-invalid invalid-truncated-copy-replay-applies-committed-entry.jsonl 16 replay_entry
run_invalid m8 m8-activation-without-noop-fill.jsonl 180 primary_activated
run_valid m9 m9-replica-restarts-before-primary-reads-its-acks.jsonl
run_valid m10 m10-promoted-primary-restart-replays-noop.jsonl
run_invalid m10c m10c-promoted-primary-restart-without-noop-entry.jsonl 244 replay_entry

echo "D1 trace-validator round-4 scenarios passed."
