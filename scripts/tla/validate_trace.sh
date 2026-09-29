#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SPEC_DIR="$ROOT_DIR/specs/tla"
CONVERTER="$ROOT_DIR/scripts/tla/trace_to_tla.py"
TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
DEFAULT_JAR="${XDG_CACHE_HOME:-$HOME/.cache}/ferrissearch-tla/v${TLA_VERSION}/tla2tools.jar"
TIMEOUT_SECONDS="${TLA_TRACE_TIMEOUT_SECONDS:-120}"
HEAP_SIZE="${TLA_TRACE_HEAP:-4g}"
INCONCLUSIVE_EXIT=3

usage() {
    echo "Usage: $0 TRACE.jsonl" >&2
}

inconclusive() {
    echo "INCONCLUSIVE: $*" >&2
    exit "$INCONCLUSIVE_EXIT"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

TRACE_PATH=$1
if [[ ! -f "$TRACE_PATH" ]]; then
    echo "Trace file does not exist: $TRACE_PATH" >&2
    exit 2
fi

verify_jar() {
    local jar=$1
    local actual
    actual=$(sha256sum "$jar" | awk '{print $1}')
    if [[ "$actual" != "$TLA_SHA256" ]]; then
        echo "TLA+ tools checksum mismatch for $jar" >&2
        echo "expected: $TLA_SHA256" >&2
        echo "actual:   $actual" >&2
        return 1
    fi
}

if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
    JAR=$TLA2TOOLS_JAR
    if [[ ! -f "$JAR" ]]; then
        echo "TLA2TOOLS_JAR does not exist: $JAR" >&2
        exit 2
    fi
    verify_jar "$JAR"
else
    JAR=$DEFAULT_JAR
    mkdir -p "$(dirname "$JAR")"
    if [[ -f "$JAR" ]] && ! verify_jar "$JAR"; then
        rm -f -- "$JAR"
    fi
    if [[ ! -f "$JAR" ]]; then
        download="$JAR.download.$$"
        trap 'rm -f -- "${download:-}"' EXIT
        curl --fail --location --retry 3 --output "$download" "$TLA_URL"
        verify_jar "$download"
        mv "$download" "$JAR"
        trap - EXIT
    fi
fi

RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/ferrissearch-trace-d1.XXXXXX")
cleanup() {
    rm -rf -- "${RUN_DIR:?}"
}
trap cleanup EXIT

set +e
conversion_output=$(
    python3 "$CONVERTER" "$TRACE_PATH" \
        --output "$RUN_DIR/TraceInput.tla" \
        --config-output "$RUN_DIR/TraceD1.cfg" \
        --profile-output "$RUN_DIR/profile" 2>&1
)
conversion_status=$?
set -e
if [[ $conversion_status -ne 0 ]]; then
    echo "$conversion_output" >&2
    line=$(sed -n 's/.*line \([0-9][0-9]*\):.*/\1/p' <<<"$conversion_output" | tail -n 1)
    if [[ -n "$line" ]]; then
        if [[ "$line" -eq 1 ]]; then
            step=0
        else
            step=$((line - 1))
        fi
        event=$(
            python3 - "$TRACE_PATH" "$step" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        record = json.loads(line)
        if record.get("step") == int(sys.argv[2]):
            print(record.get("event", "schema"))
            break
PY
        )
        echo "Trace rejected at schema step $step (event ${event:-schema})" >&2
    fi
    exit 1
fi
echo "$conversion_output"
PROFILE=$(<"$RUN_DIR/profile")
case "$PROFILE" in
    d1-core)
        TRACE_MODULE="TraceD1"
        ACCEPT_INVARIANT="TraceNotAccepted"
        TYPE_INVARIANT="TraceTypeOK"
        SAFETY_INVARIANT="TraceCoreSafety"
        ;;
    d1-combined)
        TRACE_MODULE="TraceD1"
        ACCEPT_INVARIANT="TraceNotAccepted"
        TYPE_INVARIANT="TraceTypeOK"
        SAFETY_INVARIANT="TraceCoreSafety"
        ;;
    d1-authority)
        TRACE_MODULE="TraceD1Authority"
        ACCEPT_INVARIANT="TraceAuthorityNotAccepted"
        TYPE_INVARIANT="TraceAuthorityTypeOK"
        SAFETY_INVARIANT="TraceAuthoritySafety"
        ;;
    d1-collision)
        TRACE_MODULE="TraceD1Collision"
        ACCEPT_INVARIANT="TraceCollisionNotAccepted"
        TYPE_INVARIANT="TraceCollisionTypeOK"
        SAFETY_INVARIANT="TraceCollisionSafety"
        ;;
    d1-recovery)
        TRACE_MODULE="TraceD1Recovery"
        ACCEPT_INVARIANT="TraceRecoveryNotAccepted"
        TYPE_INVARIANT="TraceRecoveryTypeOK"
        SAFETY_INVARIANT="TraceRecoverySafety"
        ;;
    *)
        echo "Converter selected unknown profile: $PROFILE" >&2
        exit 2
        ;;
esac
cp \
    "$SPEC_DIR/$TRACE_MODULE.tla" \
    "$SPEC_DIR/MC_D1_SeqNoApply.tla" \
    "$SPEC_DIR/MC_D1_TermCollision.tla" \
    "$SPEC_DIR/Invariants.tla" \
    "$SPEC_DIR/Faults.tla" \
    "$SPEC_DIR/PeerRecovery.tla" \
    "$SPEC_DIR/ShardReplication.tla" \
    "$SPEC_DIR/RaftLog.tla" \
    "$RUN_DIR/"
mkdir -p "$RUN_DIR/java-tmp" "$RUN_DIR/states"

set +e
(
    cd "$RUN_DIR"
    timeout "${TIMEOUT_SECONDS}s" \
        java \
        -Djava.io.tmpdir="$RUN_DIR/java-tmp" \
        -Xmx"$HEAP_SIZE" \
        -XX:+UseParallelGC \
        -cp "$JAR" \
        tlc2.TLC \
        -deadlock \
        -difftrace \
        -workers 1 \
        -dump "$RUN_DIR/states.dump" \
        -metadir "$RUN_DIR/states" \
        -config TraceD1.cfg \
        "$TRACE_MODULE.tla"
) >"$RUN_DIR/tlc.log" 2>&1
status=$?
set -e

if [[ $status -eq 124 ]]; then
    cat "$RUN_DIR/tlc.log" >&2
    inconclusive \
        "trace validation exceeded ${TIMEOUT_SECONDS}s: $TRACE_PATH"
fi

if [[ $status -eq 137 ]] ||
    grep -Eiq \
        'OutOfMemoryError|Java ran out of memory|Java heap space|GC overhead limit exceeded|Could not reserve enough space|Cannot allocate memory|insufficient memory|Too small maximum heap' \
        "$RUN_DIR/tlc.log"; then
    cat "$RUN_DIR/tlc.log" >&2
    inconclusive "trace validation exhausted memory: $TRACE_PATH"
fi

report_rejection_at_position() {
    local position=$1
    local failed_step
    local failed_event
    if [[ $position -lt 1 ]]; then
        position=1
    fi
    failed_step=$(
        python3 - "$TRACE_PATH" "$position" <<'PY'
import json
import sys

path, position = sys.argv[1], int(sys.argv[2])
events = []
with open(path, encoding="utf-8") as handle:
    for line in handle:
        record = json.loads(line)
        if record.get("event") not in {"trace_start", "trace_end"}:
            events.append(record)
if position <= len(events):
    print(events[position - 1]["step"])
else:
    print(len(events) + 1)
PY
    )
    failed_event=$(
        python3 - "$TRACE_PATH" "$failed_step" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        record = json.loads(line)
        if record.get("step") == int(sys.argv[2]):
            print(record.get("event", "trace_end"))
            break
PY
    )
    echo "Trace rejected at schema step $failed_step (event ${failed_event:-trace_end}): $TRACE_PATH" >&2
}

if grep -Fq "Invariant $ACCEPT_INVARIANT is violated." "$RUN_DIR/tlc.log" &&
    ! grep -Fq "Invariant $TYPE_INVARIANT is violated." "$RUN_DIR/tlc.log" &&
    ! grep -Fq "Invariant $SAFETY_INVARIANT is violated." "$RUN_DIR/tlc.log"; then
    echo "Trace accepted by $TRACE_MODULE: $TRACE_PATH"
    exit 0
fi

if grep -Fq "Invariant $SAFETY_INVARIANT is violated." "$RUN_DIR/tlc.log"; then
    max_position=$(
        sed -n 's/.*tracePos = \([0-9][0-9]*\).*/\1/p' "$RUN_DIR/states.dump" |
            sort -n |
            tail -n 1
    )
    report_rejection_at_position "$(( ${max_position:-2} - 1 ))"
    exit 1
fi

if [[ $status -eq 0 ]] &&
    ! grep -Fq "Error:" "$RUN_DIR/tlc.log" &&
    grep -Fq "Model checking completed. No error has been found." "$RUN_DIR/tlc.log"; then
    max_position=$(
        sed -n 's/.*tracePos = \([0-9][0-9]*\).*/\1/p' "$RUN_DIR/states.dump" |
            sort -n |
            tail -n 1
    )
    if [[ -z "$max_position" ]]; then
        max_position=1
    fi
    report_rejection_at_position "$max_position"
    exit 1
fi

cat "$RUN_DIR/tlc.log" >&2
inconclusive "TLC failed unexpectedly: $TRACE_PATH"
