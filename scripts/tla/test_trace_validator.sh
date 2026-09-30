#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/v4"
TRACE_TEST_PREFIX="review"

export PYTHONDONTWRITEBYTECODE=1

python3 -m unittest discover \
    -s "$ROOT_DIR/scripts/tla/tests" \
    -p 'test_*.py'

source "$ROOT_DIR/scripts/tla/trace_test_runner.sh"
trace_test_init

run_valid() {
    trace_test_add_valid "$@"
}

run_invalid() {
    trace_test_add_invalid "$@"
}

run_inconclusive() {
    trace_test_add_inconclusive "$@"
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
run_valid truncated-retained-skip valid-truncated-copy-restart-retained-skip.jsonl
run_invalid truncated-reapply invalid-truncated-copy-replay-applies-committed-entry.jsonl 16 replay_entry

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
run_valid v1 v1-failover-after-replicated-write.jsonl
run_invalid v2 v2-recovery-duplicate-catchup-redelivery.jsonl 33 operation_processed
run_invalid v4 v4-nonquiescent-without-copy-state.jsonl 19 trace_end
run_invalid v5 v5-collision-then-new-write.jsonl 1 wal_appended
run_invalid v6 v6-pruned-tombstone-absent.jsonl 20 copy_state
run_invalid v9 v9-trace-sets-hidden-budget.jsonl 0 trace_start
run_valid combined-16-write valid-combined-two-term-16-writes.jsonl
run_valid representative-500 valid-representative-500-restart-failover.jsonl
run_invalid combined-arrival invalid-combined-arrival-order.jsonl 51 operation_processed
run_invalid combined-collision invalid-combined-collision-redelivery.jsonl 190 operation_processed
run_invalid combined-rollback invalid-combined-rollback-after-promotion.jsonl 219 copy_state
run_valid noop-applied valid-promotion-noop-applied.jsonl
run_invalid noop-not-applied invalid-promotion-noop-not-applied.jsonl 33 commit_captured
run_valid noop-collision-removed valid-promotion-noop-collision-removed.jsonl
run_invalid noop-collision-redelivery invalid-promotion-noop-collision-as-redelivery.jsonl 32 operation_processed
run_valid p7a valid-promotion-noop-replicated-p7a.jsonl
run_invalid p7b invalid-promotion-noop-untraced-p7b.jsonl 190 operation_processed
run_invalid \
    r6-b1-noop-seq \
    invalid-r6-noop-processed-wrong-seq-label.jsonl \
    186 \
    operation_processed
run_invalid \
    r6-b1-noop-term \
    invalid-r6-noop-processed-wrong-term-label.jsonl \
    186 \
    operation_processed
run_invalid \
    r6-b1-recovery-identity \
    invalid-r6-recovery-apply-wrong-identity.jsonl \
    16 \
    operation_processed
run_valid r6-activation-replay valid-r6-activation-replay-literal.jsonl
run_valid \
    r6-activation-replay-events \
    valid-r6-activation-replay-events-only.jsonl
run_valid \
    r6-activation-double-persist \
    valid-r6-activation-double-commit-persist.jsonl
run_valid \
    r6-restart-double-persist \
    valid-r6-restart-double-commit-persist.jsonl
run_valid \
    r6-restart-empty-replay \
    valid-r6-restart-no-truncate-empty-replay.jsonl
run_valid r6-physical-noop-fill valid-r6-promotion-physical-fill.jsonl
run_valid \
    r6-replica-crash-before-noop-send \
    valid-r6-noop-send-after-replica-crash.jsonl
run_valid \
    r6-primary-crash-before-noop-send \
    valid-r6-primary-crash-before-noop-send.jsonl
run_valid rust-faithful-scripted valid-rust-faithful-scripted.jsonl
run_invalid \
    r6-double-persist-unknown \
    invalid-r6-double-persist-unknown-commit.jsonl \
    181 \
    commit_persisted
run_invalid \
    r6-noop-send-omitted \
    invalid-r6-noop-send-omitted.jsonl \
    180 \
    promotion_noop_fill
run_invalid \
    r6-noop-send-before-activation \
    invalid-r6-noop-send-before-activation.jsonl \
    181 \
    promotion_noop_replication_started
run_invalid \
    r6-noop-send-non-insync \
    invalid-r6-noop-send-to-non-insync.jsonl \
    182 \
    promotion_noop_replication_started
run_invalid \
    r6-replay-omits-retained \
    invalid-r6-replay-omits-retained-entry.jsonl \
    221 \
    replay_entry
run_invalid \
    r6-replay-reapplies-committed \
    invalid-r6-replay-reapplies-committed-entry.jsonl \
    219 \
    replay_entry
run_invalid \
    r6-replay-finishes-early \
    invalid-r6-replay-finishes-early.jsonl \
    222 \
    replay_finished
run_invalid \
    r6-replay-reordered \
    invalid-r6-replay-reordered.jsonl \
    221 \
    replay_entry
run_invalid \
    r6-replay-receipt-relabeled \
    invalid-r6-replay-receipt-relabeled.jsonl \
    221 \
    replay_entry
run_invalid \
    rust-faithful-omitted-wal \
    invalid-rust-faithful-omitted-wal.jsonl \
    19 \
    operation_processed
run_valid \
    rust-faithful-replica-rejected \
    valid-rust-faithful-replica-rejected.jsonl
run_invalid \
    rust-faithful-missing-replica-rejected \
    invalid-rust-faithful-missing-replica-rejected.jsonl \
    120 \
    promotion_noop_result
run_valid \
    r6-quarantined-replica-rejected \
    valid-r6-quarantined-replica-rejected.jsonl
run_invalid \
    r6-quarantined-result-without-rejection \
    invalid-r6-quarantined-result-without-rejection.jsonl \
    197 \
    replica_result
run_invalid \
    r6-ordinary-apply-without-receive \
    invalid-r6-ordinary-apply-without-receive.jsonl \
    169 \
    operation_processed
run_invalid \
    r6-ordinary-receive-after-apply \
    invalid-r6-ordinary-receive-after-apply.jsonl \
    169 \
    operation_processed
run_invalid \
    r6-noop-apply-without-receive \
    invalid-r6-noop-apply-without-receive.jsonl \
    185 \
    operation_processed
run_invalid \
    r6-commit-term-state-contradicts-copy \
    invalid-r6-commit-term-state-contradicts-copy.jsonl \
    199 \
    commit_captured
run_invalid \
    r6-commit-term-state-invalid-shape \
    invalid-r6-commit-term-state-invalid-shape.jsonl \
    199 \
    commit_captured
run_valid \
    recovery-installs-term-state \
    valid-recovery-installs-term-state.jsonl
run_invalid \
    recovery-loses-term-state-range \
    invalid-recovery-loses-term-state-range.jsonl \
    261 \
    commit_captured
run_inconclusive \
    timeout \
    "trace validation exceeded 1s" \
    valid-combined-two-term-16-writes.jsonl \
    1 \
    "$TRACE_TEST_DEFAULT_HEAP"
run_inconclusive \
    out-of-memory \
    "trace validation exhausted memory" \
    valid-combined-two-term-16-writes.jsonl \
    60 \
    24m

TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-}" "$ROOT_DIR/scripts/tla/check.sh" d1-trace-actions
echo "review n11 expected=accepted actual=accepted"
trace_test_run_all "D1 schema-v4 trace validator self-tests passed."
