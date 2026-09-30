#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/v4"
TIMEOUT_SECONDS="${TLA_TRACE_ROUND4_TIMEOUT_SECONDS:-120}"
TRACE_TEST_PREFIX="round4"
TRACE_TEST_DEFAULT_TIMEOUT="$TIMEOUT_SECONDS"

source "$ROOT_DIR/scripts/tla/trace_test_runner.sh"
trace_test_init

run_valid() {
    trace_test_add_valid "$@"
}

run_invalid() {
    trace_test_add_invalid "$@"
}

run_valid m4c m4c-stale-term-delivery-received-only.jsonl
run_invalid m4 m4-stale-term-delivery-applied-after-promotion.jsonl 183 wal_appended
run_invalid m6b m6b-noop-fill-before-replay-finished.jsonl 194 promotion_noop_fill
run_valid v6c v6c-promoted-copy-restart-replay-reactivate-fill.jsonl
run_valid m7 m7-truncated-copy-restart-faithful-empty-replay.jsonl
run_valid m7-retained valid-truncated-copy-restart-retained-skip.jsonl
run_invalid m7-invalid invalid-truncated-copy-replay-applies-committed-entry.jsonl 16 replay_entry
run_invalid m8 m8-activation-without-noop-fill.jsonl 180 primary_activated
run_valid m9 m9-replica-restarts-before-primary-reads-its-acks.jsonl
run_valid m10 m10-promoted-primary-restart-replays-noop.jsonl
run_invalid m10c m10c-promoted-primary-restart-without-noop-entry.jsonl 246 replay_entry

trace_test_run_all "D1 trace-validator round-4 scenarios passed."
