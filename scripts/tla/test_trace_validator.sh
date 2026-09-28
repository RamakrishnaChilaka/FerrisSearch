#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/v2"

export PYTHONDONTWRITEBYTECODE=1

python3 -m unittest discover \
    -s "$ROOT_DIR/scripts/tla/tests" \
    -p 'test_*.py'

run_valid() {
    local label=$1
    local trace=$2
    local output
    output=$(mktemp "${TMPDIR:-/tmp}/ferrissearch-trace-valid.XXXXXX")
    if ! "$VALIDATOR" "$FIXTURES/$trace" >"$output" 2>&1; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Expected accepted trace: $label ($trace)" >&2
        exit 1
    fi
    cat "$output"
    rm -f -- "$output"
    echo "review $label expected=accepted actual=accepted"
}

run_invalid() {
    local label=$1
    local trace=$2
    local step=$3
    local event=$4
    local output
    output=$(mktemp "${TMPDIR:-/tmp}/ferrissearch-trace-invalid.XXXXXX")
    set +e
    "$VALIDATOR" "$FIXTURES/$trace" >"$output" 2>&1
    status=$?
    set -e
    if [[ $status -eq 0 ]]; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Expected rejected trace: $label ($trace)" >&2
        exit 1
    fi
    if ! grep -Fq "Trace rejected at schema step $step (event $event)" "$output"; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Trace $label did not fail at expected step $step ($event)" >&2
        exit 1
    fi
    cat "$output"
    rm -f -- "$output"
    echo "review $label expected=rejected actual=rejected step=$step event=$event"
}

# Baseline accepted traces for each exact composition.
run_valid baseline-order valid-concurrent-order.jsonl
run_valid baseline-replay valid-processed-checkpoint-replay.jsonl
run_valid baseline-authority valid-authority-activation.jsonl
run_valid baseline-stale-primary valid-stale-primary-local-append.jsonl
run_valid baseline-collision valid-term-collision.jsonl
run_valid baseline-recovery valid-recovery-snapshot-barrier.jsonl
run_valid baseline-recovery-planner valid-recovery-planner-sample.jsonl
run_valid baseline-replay-failure valid-replay-failed-unavailable.jsonl

# Original historical regressions.
run_invalid arrival-order invalid-arrival-order.jsonl 16 operation_processed
run_invalid seq-only-redelivery invalid-seq-only-redelivery.jsonl 6 operation_processed
run_invalid highest-commit invalid-highest-commit-replay.jsonl 16 commit_captured
run_invalid replay-wrong-at-replay invalid-replay-boundary-at-replay.jsonl 33 replay_entry

# Every Opus reviewer mutation.
run_invalid m1 m1-reorder-replica-applies.jsonl 11 operation_processed
run_invalid m2 m2-flip-stale-to-applied.jsonl 16 operation_processed
run_invalid m3 m3-drop-r2-fence.jsonl 5 operation_processed
run_invalid m4 m4-change-checkpoint.jsonl 11 operation_processed
run_invalid m5 m5-activate-without-fence.jsonl 5 primary_activated
run_invalid m6 m6-write-before-activation.jsonl 6 wal_appended
run_invalid m7 m7-restart-without-replay.jsonl 33 copy_state
run_invalid m8 m8-ack-nondurable-replica.jsonl 13 client_result
run_invalid m6b m6b-ack-before-activation.jsonl 6 wal_appended
run_invalid m8b m8b-ack-nondurable-replica.jsonl 13 client_result
run_valid m13 valid-replayed-delete-applied.jsonl
run_invalid m9 m9-recovery-misses-post-snapshot-op.jsonl 31 recovery_barrier
run_invalid m9b m9b-recovery-copies-post-snapshot-state.jsonl 31 copy_state
run_valid m14 valid-op-between-commit-capture-persist.jsonl
run_invalid m15 m15-drop-replica-wal.jsonl 10 operation_processed
run_invalid m18 invalid-replay-omits-entry.jsonl 35 replay_finished
run_invalid m19 invalid-truncate-above-commit.jsonl 30 wal_truncated

echo "D1 schema-v2 trace validator self-tests passed."
