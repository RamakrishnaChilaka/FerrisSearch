#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CHECKER="$ROOT_DIR/scripts/tla/check_d1_trace_invariants.py"
VALIDATOR="$ROOT_DIR/scripts/tla/validate_trace.sh"
SEED="${D1_TRACE_SEED:-13754061}"
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/ferrissearch-d1-rust-trace.XXXXXX")

cleanup() {
    rm -rf -- "${RUN_DIR:?}"
}
trap cleanup EXIT

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"
export RUST_TEST_THREADS=1
export TLA_TRACE_HEAP="${TLA_TRACE_HEAP:-2g}"
export TLA_TRACE_TIMEOUT_SECONDS="${TLA_TRACE_TIMEOUT_SECONDS:-60}"

capture() {
    local mutation=$1
    local output=$2
    D1_TRACE_SEED="$SEED" \
        D1_TRACE_MUTATION="$mutation" \
        D1_TRACE_OUTPUT="$output" \
        cargo test \
            --manifest-path "$ROOT_DIR/Cargo.toml" \
            --test d1_protocol_trace \
            --features protocol-trace \
            seeded_three_node_fault_trace \
            -- --exact --nocapture
}

expect_checker_rejected() {
    local trace=$1
    local expected_step=$2
    local output
    local status

    set +e
    output=$(python3 "$CHECKER" "$trace" 2>&1)
    status=$?
    set -e
    if [[ $status -ne 1 ]]; then
        printf '%s\n' "$output" >&2
        echo "invariant checker expected exit 1, got $status" >&2
        exit 1
    fi
    if ! grep -Fq "step $expected_step (event operation_processed)" <<<"$output"; then
        printf '%s\n' "$output" >&2
        echo "invariant checker rejected at an unexpected step" >&2
        exit 1
    fi
    printf '%s\n' "$output"
}

expect_tla_rejected() {
    local trace=$1
    local expected_step=$2
    local output
    local status

    set +e
    output=$("$VALIDATOR" "$trace" 2>&1)
    status=$?
    set -e
    if [[ $status -ne 1 ]]; then
        printf '%s\n' "$output" >&2
        echo "TLA validator expected exit 1, got $status" >&2
        exit 1
    fi
    if ! grep -Fq "step $expected_step (event operation_processed)" <<<"$output"; then
        printf '%s\n' "$output" >&2
        echo "TLA validator rejected at an unexpected step" >&2
        exit 1
    fi
    printf '%s\n' "$output"
}

correct="$RUN_DIR/correct.jsonl"
arrival="$RUN_DIR/arrival-order.jsonl"
seq_only="$RUN_DIR/seq-only-redelivery.jsonl"

capture none "$correct"
python3 "$CHECKER" "$correct"
TLA_TRACE_EXPECTED=accepted "$VALIDATOR" "$correct"

capture arrival-order "$arrival"
expect_checker_rejected "$arrival" 27
expect_tla_rejected "$arrival" 27

capture seq-only-redelivery "$seq_only"
expect_checker_rejected "$seq_only" 117
expect_tla_rejected "$seq_only" 117

echo "D1 Rust trace validation passed (seed=$SEED)."
