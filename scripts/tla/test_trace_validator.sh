#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/v3"

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
run_valid baseline-core-three-nodes valid-core-three-nodes.jsonl
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

# Round-2 reviewer mutations and scale checks.
run_invalid n1 n1-recovery-profile-arrival-order-overwrite.jsonl 16 operation_processed
run_valid n1c n1c-recovery-profile-late-older-stale.jsonl
run_valid n2 n2-core-concurrent-primary-interleave.jsonl
run_invalid n3 n3-core-replay-flip-stale.jsonl 35 replay_entry
run_invalid n4 n4-core-copy-state-wrong.jsonl 20 copy_state
run_valid n5 n5-core-four-writes.jsonl
run_valid n6 n6-core-primary-crash.jsonl
run_invalid n7 n7-core-persist-without-capture.jsonl 16 commit_persisted
run_valid n8 n8-collision-at-seq-12.jsonl
run_invalid n9 n9-no-copy-behind-safety.jsonl 16 operation_processed
run_invalid n10 n10-persisted-checkpoint-mismatch.jsonl 16 operation_processed
TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-}" "$ROOT_DIR/scripts/tla/check.sh" d1-trace-actions
echo "review n11 expected=accepted actual=accepted"
run_valid n12 valid-replay-failed-unavailable.jsonl
run_valid n13 n13-primary-restart-replay.jsonl
run_valid n14 valid-concurrent-order.jsonl
run_valid n15 n15-core-with-routing-view.jsonl

run_valid a1 a1-per-write-persisted-buffer.jsonl
run_valid a1c a1c-results-adjacent.jsonl
run_valid b1 b1-bulk-batch-final-response-checkpoint.jsonl
run_valid b1c b1c-bulk-item-local-response-checkpoint.jsonl
run_valid b2 b2-bulk-faithful-order-batch-final.jsonl
run_valid b2c b2c-bulk-faithful-order-item-local.jsonl
run_valid b3 b3-bulk-receipts-first-batch-final.jsonl
run_valid b3c b3c-bulk-receipts-first-item-local.jsonl
run_invalid a2-overstated-response invalid-replica-response-overstates-persisted.jsonl 15 replica_result
run_invalid v1 v1-failover-after-replicated-write.jsonl 5 replica_received
run_invalid v2 v2-recovery-duplicate-catchup-redelivery.jsonl 33 operation_processed
run_invalid v4 v4-nonquiescent-without-copy-state.jsonl 19 trace_end
run_invalid v5 v5-collision-then-new-write.jsonl 8 client_write_routed
run_invalid v6 v6-pruned-tombstone-absent.jsonl 20 copy_state
run_invalid v9 v9-trace-sets-hidden-budget.jsonl 0 trace_start

echo "D1 schema-v3 trace validator self-tests passed."
