#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SPEC_DIR="$ROOT_DIR/specs/tla"
TRACE_MODEL="$SPEC_DIR/TraceD1.tla"
TRACE_CONFIG="$SPEC_DIR/TraceD1.cfg"
CONVERTER="$ROOT_DIR/scripts/tla/trace_to_tla.py"
TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
DEFAULT_JAR="${XDG_CACHE_HOME:-$HOME/.cache}/ferrissearch-tla/v${TLA_VERSION}/tla2tools.jar"
TIMEOUT_SECONDS="${TLA_TRACE_TIMEOUT_SECONDS:-60}"

usage() {
    echo "Usage: $0 TRACE.jsonl" >&2
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

python3 "$CONVERTER" "$TRACE_PATH" --output "$RUN_DIR/TraceInput.tla"
cp "$TRACE_MODEL" "$TRACE_CONFIG" "$RUN_DIR/"
mkdir -p "$RUN_DIR/java-tmp" "$RUN_DIR/states"

set +e
(
    cd "$RUN_DIR"
    timeout "${TIMEOUT_SECONDS}s" \
        java \
        -Djava.io.tmpdir="$RUN_DIR/java-tmp" \
        -XX:+UseParallelGC \
        -cp "$JAR" \
        tlc2.TLC \
        -deadlock \
        -difftrace \
        -workers 1 \
        -metadir "$RUN_DIR/states" \
        -config TraceD1.cfg \
        TraceD1.tla
) >"$RUN_DIR/tlc.log" 2>&1
status=$?
set -e

if [[ $status -eq 124 ]]; then
    cat "$RUN_DIR/tlc.log" >&2
    echo "Trace validation exceeded ${TIMEOUT_SECONDS}s: $TRACE_PATH" >&2
    exit 1
fi

if [[ $status -eq 0 ]] &&
    ! grep -Fq "Error:" "$RUN_DIR/tlc.log" &&
    grep -Fq "Model checking completed. No error has been found." "$RUN_DIR/tlc.log"; then
    echo "Trace accepted by TraceD1: $TRACE_PATH"
    exit 0
fi

if grep -Fq "Invariant TraceFailureFree is violated." "$RUN_DIR/tlc.log"; then
    failed_step=$(
        sed -n 's/.*failedStep |-> \([0-9][0-9]*\).*/\1/p' "$RUN_DIR/tlc.log" |
            tail -n 1
    )
    if [[ -z "$failed_step" ]]; then
        failed_step=$(
            sed -n 's/.*failedStep = \([0-9][0-9]*\).*/\1/p' "$RUN_DIR/tlc.log" |
                tail -n 1
        )
    fi
    if [[ -n "$failed_step" ]]; then
        failed_event=$(
            python3 - "$TRACE_PATH" "$failed_step" <<'PY'
import json
import sys

path, step = sys.argv[1], int(sys.argv[2])
with open(path, encoding="utf-8") as handle:
    for line in handle:
        record = json.loads(line)
        if record.get("step") == step:
            print(record.get("event", "unknown"))
            break
PY
        )
        echo "Trace rejected at schema step $failed_step (event ${failed_event:-unknown}): $TRACE_PATH" >&2
    else
        echo "Trace rejected, but TLC did not expose failedStep: $TRACE_PATH" >&2
    fi
    if [[ "${TLA_TRACE_VERBOSE:-0}" == "1" ]]; then
        cat "$RUN_DIR/tlc.log" >&2
    fi
    exit 1
fi

cat "$RUN_DIR/tlc.log" >&2
echo "Trace validation failed unexpectedly: $TRACE_PATH" >&2
exit 1
