#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
FIXTURES="$ROOT_DIR/specs/tla/trace/examples"

export PYTHONDONTWRITEBYTECODE=1

python3 -m unittest discover \
    -s "$ROOT_DIR/scripts/tla/tests" \
    -p 'test_*.py'

valid_traces=(
    valid-concurrent-order.jsonl
    valid-term-collision.jsonl
    valid-processed-checkpoint-replay.jsonl
)

for trace in "${valid_traces[@]}"; do
    "$VALIDATOR" "$FIXTURES/$trace"
done

invalid_traces=(
    "invalid-arrival-order.jsonl:19:operation_applied"
    "invalid-seq-only-redelivery.jsonl:25:operation_applied"
    "invalid-highest-commit-replay.jsonl:20:commit_persisted"
)

for expectation in "${invalid_traces[@]}"; do
    IFS=: read -r trace step event <<<"$expectation"
    output=$(mktemp "${TMPDIR:-/tmp}/ferrissearch-trace-test.XXXXXX")
    set +e
    "$VALIDATOR" "$FIXTURES/$trace" >"$output" 2>&1
    status=$?
    set -e
    if [[ $status -eq 0 ]]; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Expected trace rejection: $trace" >&2
        exit 1
    fi
    if ! grep -Fq "Trace rejected at schema step $step (event $event)" "$output"; then
        cat "$output" >&2
        rm -f -- "$output"
        echo "Trace rejection did not identify expected step $step: $trace" >&2
        exit 1
    fi
    cat "$output"
    rm -f -- "$output"
done

echo "D1 trace validator self-tests passed."
