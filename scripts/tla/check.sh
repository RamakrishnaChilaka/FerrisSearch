#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SPEC_DIR="$ROOT_DIR/specs/tla"
TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
DEFAULT_JAR="${XDG_CACHE_HOME:-$HOME/.cache}/ferrissearch-tla/v${TLA_VERSION}/tla2tools.jar"
WORKERS="${TLA_WORKERS:-4}"
TIMEOUT_SECONDS="${TLA_TIMEOUT_SECONDS:-300}"

RUN_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/ferrissearch-tla.XXXXXX")
cleanup() {
    rm -rf -- "${RUN_ROOT:?}"
}
trap cleanup EXIT

if [[ -n "${TLA_LOG_DIR:-}" ]]; then
    LOG_DIR="$TLA_LOG_DIR"
else
    LOG_DIR="$RUN_ROOT/logs"
fi
mkdir -p "$LOG_DIR"

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
        exit 1
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
        trap 'rm -f -- "${download:-}"; cleanup' EXIT
        curl --fail --location --retry 3 --output "$download" "$TLA_URL"
        verify_jar "$download"
        mv "$download" "$JAR"
        trap cleanup EXIT
    fi
fi

default_configs=(
    c1-fast
    c1-recovery
    c1-aba
    c1-aba-fixed
    c2
    c2-allocation-ids
    c3
    c3-allocation-ids
    c4
    l1
)

list_configs() {
    cat <<'EOF'
c1-fast                 pass: crash-only replication smoke check
c1-recovery             pass: snapshot, suffix, finalize, and admission
c1-aba                  expected NoPartialServe violation: assignment ABA
c1-aba-fixed            pass: allocation-ID handshake variant
c2                      expected UniqueAckedSeq violation: stale primary
c2-allocation-ids       expected UniqueAckedSeq violation with allocation IDs
c3                      expected NoAckedLoss violation: same-name empty disk
c3-allocation-ids       pass: empty disk fails closed on missing local identity
c4                      expected NoAckedLoss violation: asynchronous durability
l1                      pass: fair fault-free recovery liveness
EOF
}

if [[ "${1:-}" == "--list" ]]; then
    list_configs
    exit 0
fi

if [[ $# -eq 0 || "${1:-}" == "fast" || "${1:-}" == "all" ]]; then
    configs=("${default_configs[@]}")
else
    configs=("$@")
fi

run_config() {
    local name=$1
    local module
    local cfg
    local expected

    case "$name" in
        c1-fast|MC_C1_fast)
            module="Invariants.tla"
            cfg="MC_C1_fast.cfg"
            expected="pass"
            ;;
        c1-recovery|MC_C1_recovery_smoke)
            module="Invariants.tla"
            cfg="MC_C1_recovery_smoke.cfg"
            expected="pass"
            ;;
        c1-aba|MC_C1_ABA)
            module="Invariants.tla"
            cfg="MC_C1_ABA.cfg"
            expected="NoPartialServe"
            ;;
        c1-aba-fixed|MC_C1_ABA_fixed)
            module="Invariants.tla"
            cfg="MC_C1_ABA_fixed.cfg"
            expected="pass"
            ;;
        c2|MC_C2_fast)
            module="MC_C2.tla"
            cfg="MC_C2_fast.cfg"
            expected="UniqueAckedSeq"
            ;;
        c2-allocation-ids|MC_C2_allocation_ids)
            module="MC_C2.tla"
            cfg="MC_C2_allocation_ids.cfg"
            expected="UniqueAckedSeq"
            ;;
        c3|MC_C3)
            module="MC_C3.tla"
            cfg="MC_C3.cfg"
            expected="NoAckedLoss"
            ;;
        c3-allocation-ids|MC_C3_allocation_ids)
            module="MC_C3.tla"
            cfg="MC_C3_allocation_ids.cfg"
            expected="pass"
            ;;
        c4|MC_C4)
            module="MC_C4.tla"
            cfg="MC_C4.cfg"
            expected="NoAckedLoss"
            ;;
        l1|MC_L1)
            module="MC_L1.tla"
            cfg="MC_L1.cfg"
            expected="pass"
            ;;
        *)
            echo "Unknown TLA+ configuration: $name" >&2
            list_configs >&2
            return 2
            ;;
    esac

    local run_dir="$RUN_ROOT/$name"
    local java_tmp="$run_dir/java-tmp"
    local states="$run_dir/states"
    local log="$LOG_DIR/tla-${name}.log"
    mkdir -p "$java_tmp" "$states"

    echo "=== TLA+ $name ($expected) ==="
    set +e
    (
        cd "$SPEC_DIR"
        timeout "${TIMEOUT_SECONDS}s" \
            java \
            -Djava.io.tmpdir="$java_tmp" \
            -XX:+UseParallelGC \
            -cp "$JAR" \
            tlc2.TLC \
            -deadlock \
            -difftrace \
            -workers "$WORKERS" \
            -metadir "$states" \
            -config "$cfg" \
            "$module"
    ) >"$log" 2>&1
    local status=$?
    set -e
    cat "$log"

    if [[ $status -eq 124 ]]; then
        echo "TLA+ configuration '$name' exceeded ${TIMEOUT_SECONDS}s" >&2
        return 1
    fi

    if [[ "$expected" == "pass" ]]; then
        if [[ $status -ne 0 ]] ||
            ! grep -Fq "Model checking completed. No error has been found." "$log"; then
            echo "TLA+ configuration '$name' was expected to pass" >&2
            return 1
        fi
    else
        if [[ $status -eq 0 ]] ||
            ! grep -Fq "Error: Invariant $expected is violated." "$log"; then
            echo "TLA+ configuration '$name' must retain $expected as an expected violation" >&2
            return 1
        fi
    fi
}

for config in "${configs[@]}"; do
    run_config "$config"
done

echo "All requested TLA+ configurations matched their expected results."
