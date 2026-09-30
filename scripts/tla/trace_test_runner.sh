#!/usr/bin/env bash

declare -a TRACE_TEST_KINDS=()
declare -a TRACE_TEST_LABELS=()
declare -a TRACE_TEST_TRACES=()
declare -a TRACE_TEST_STEPS=()
declare -a TRACE_TEST_EVENTS=()
declare -a TRACE_TEST_EXPECTED=()
declare -a TRACE_TEST_TIMEOUTS=()
declare -a TRACE_TEST_HEAPS=()

trace_test_cleanup() {
    if [[ "${BASHPID:-$$}" -ne "${TRACE_TEST_OWNER_PID:-$$}" ]]; then
        return
    fi
    if [[ -n "${TRACE_TEST_RUN_ROOT:-}" && -d "$TRACE_TEST_RUN_ROOT" ]]; then
        rm -rf -- "${TRACE_TEST_RUN_ROOT:?}"
    fi
}

trace_test_init() {
    : "${VALIDATOR:?VALIDATOR must name validate_trace.sh}"
    : "${FIXTURES:?FIXTURES must name the trace fixture directory}"

    local cpu_count
    local default_jobs
    cpu_count=$(nproc 2>/dev/null || echo 1)
    default_jobs=$cpu_count
    if ((default_jobs > 4)); then
        default_jobs=4
    fi
    TRACE_TEST_JOBS="${TLA_TRACE_JOBS:-$default_jobs}"
    if [[ ! "$TRACE_TEST_JOBS" =~ ^[1-9][0-9]*$ ]]; then
        echo "TLA_TRACE_JOBS must be a positive integer" >&2
        return 2
    fi
    if ((TRACE_TEST_JOBS > 4)); then
        TRACE_TEST_JOBS=4
    fi
    if ((TRACE_TEST_JOBS > cpu_count)); then
        TRACE_TEST_JOBS=$cpu_count
    fi

    TRACE_TEST_PREFIX="${TRACE_TEST_PREFIX:-review}"
    TRACE_TEST_DEFAULT_TIMEOUT="${TRACE_TEST_DEFAULT_TIMEOUT:-${TLA_TRACE_TIMEOUT_SECONDS:-120}}"
    TRACE_TEST_DEFAULT_HEAP="${TLA_TRACE_HEAP:-2g}"
    TRACE_TEST_OWNER_PID="${BASHPID:-$$}"
    TRACE_TEST_RUN_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/ferrissearch-trace-tests.XXXXXX")
    trap trace_test_cleanup EXIT
}

trace_test_add_valid() {
    TRACE_TEST_KINDS+=("valid")
    TRACE_TEST_LABELS+=("$1")
    TRACE_TEST_TRACES+=("$2")
    TRACE_TEST_STEPS+=("")
    TRACE_TEST_EVENTS+=("")
    TRACE_TEST_EXPECTED+=("")
    TRACE_TEST_TIMEOUTS+=("${3:-$TRACE_TEST_DEFAULT_TIMEOUT}")
    TRACE_TEST_HEAPS+=("$TRACE_TEST_DEFAULT_HEAP")
}

trace_test_add_invalid() {
    TRACE_TEST_KINDS+=("invalid")
    TRACE_TEST_LABELS+=("$1")
    TRACE_TEST_TRACES+=("$2")
    TRACE_TEST_STEPS+=("$3")
    TRACE_TEST_EVENTS+=("$4")
    TRACE_TEST_EXPECTED+=("")
    TRACE_TEST_TIMEOUTS+=("${5:-$TRACE_TEST_DEFAULT_TIMEOUT}")
    TRACE_TEST_HEAPS+=("$TRACE_TEST_DEFAULT_HEAP")
}

trace_test_add_inconclusive() {
    TRACE_TEST_KINDS+=("inconclusive")
    TRACE_TEST_LABELS+=("$1")
    TRACE_TEST_TRACES+=("$3")
    TRACE_TEST_STEPS+=("")
    TRACE_TEST_EVENTS+=("")
    TRACE_TEST_EXPECTED+=("$2")
    TRACE_TEST_TIMEOUTS+=("$4")
    TRACE_TEST_HEAPS+=("$5")
}

trace_test_validate() {
    local trace=$1
    local timeout_seconds=$2
    local heap_size=$3
    local expected=$4
    env \
        TLA_TRACE_EXPECTED="$expected" \
        TLA_TRACE_TIMEOUT_SECONDS="$timeout_seconds" \
        TLA_TRACE_HEAP="$heap_size" \
        "$VALIDATOR" "$FIXTURES/$trace"
}

trace_test_run_valid() {
    local label=$1
    local trace=$2
    local timeout_seconds=$3
    local heap_size=$4
    local output
    output=$(mktemp "$TRACE_TEST_RUN_ROOT/valid.XXXXXX")
    if ! trace_test_validate "$trace" "$timeout_seconds" "$heap_size" accepted >"$output" 2>&1; then
        cat "$output" >&2
        echo "Expected accepted trace: $label ($trace)" >&2
        return 1
    fi
    cat "$output"
    echo "$TRACE_TEST_PREFIX $label expected=accepted actual=accepted"
}

trace_test_run_invalid() {
    local label=$1
    local trace=$2
    local step=$3
    local event=$4
    local timeout_seconds=$5
    local heap_size=$6
    local output
    local status
    output=$(mktemp "$TRACE_TEST_RUN_ROOT/invalid.XXXXXX")
    if trace_test_validate "$trace" "$timeout_seconds" "$heap_size" rejected >"$output" 2>&1; then
        status=0
    else
        status=$?
    fi
    if [[ $status -ne 1 ]] ||
        ! grep -Fq "Trace rejected at schema step $step (event $event)" "$output"; then
        cat "$output" >&2
        echo "Expected rejection at step $step ($event): $label ($trace)" >&2
        return 1
    fi
    cat "$output"
    echo "$TRACE_TEST_PREFIX $label expected=rejected actual=rejected step=$step event=$event"
}

trace_test_run_inconclusive() {
    local label=$1
    local expected=$2
    local trace=$3
    local timeout_seconds=$4
    local heap_size=$5
    local output
    local status
    output=$(mktemp "$TRACE_TEST_RUN_ROOT/inconclusive.XXXXXX")
    if trace_test_validate "$trace" "$timeout_seconds" "$heap_size" inconclusive >"$output" 2>&1; then
        status=0
    else
        status=$?
    fi
    if [[ $status -ne 3 ]] || ! grep -Fq "INCONCLUSIVE: $expected" "$output"; then
        cat "$output" >&2
        echo "Expected inconclusive result: $label" >&2
        return 1
    fi
    cat "$output"
    echo "$TRACE_TEST_PREFIX $label expected=inconclusive actual=inconclusive"
}

trace_test_run_case() {
    local index=$1
    local kind=${TRACE_TEST_KINDS[$index]}
    case "$kind" in
        valid)
            trace_test_run_valid \
                "${TRACE_TEST_LABELS[$index]}" \
                "${TRACE_TEST_TRACES[$index]}" \
                "${TRACE_TEST_TIMEOUTS[$index]}" \
                "${TRACE_TEST_HEAPS[$index]}"
            ;;
        invalid)
            trace_test_run_invalid \
                "${TRACE_TEST_LABELS[$index]}" \
                "${TRACE_TEST_TRACES[$index]}" \
                "${TRACE_TEST_STEPS[$index]}" \
                "${TRACE_TEST_EVENTS[$index]}" \
                "${TRACE_TEST_TIMEOUTS[$index]}" \
                "${TRACE_TEST_HEAPS[$index]}"
            ;;
        inconclusive)
            trace_test_run_inconclusive \
                "${TRACE_TEST_LABELS[$index]}" \
                "${TRACE_TEST_EXPECTED[$index]}" \
                "${TRACE_TEST_TRACES[$index]}" \
                "${TRACE_TEST_TIMEOUTS[$index]}" \
                "${TRACE_TEST_HEAPS[$index]}"
            ;;
        *)
            echo "Unknown trace test kind: $kind" >&2
            return 2
            ;;
    esac
}

trace_test_run_all() {
    local summary=$1
    local count=${#TRACE_TEST_KINDS[@]}
    local active=0
    local index
    local status
    local failed=0

    echo "Running $count trace cases with jobs=$TRACE_TEST_JOBS heap=$TRACE_TEST_DEFAULT_HEAP"
    for ((index = 0; index < count; index++)); do
        (
            set +e
            trace_test_run_case "$index"
            status=$?
            printf '%s\n' "$status" >"$TRACE_TEST_RUN_ROOT/$index.status"
            exit 0
        ) >"$TRACE_TEST_RUN_ROOT/$index.log" 2>&1 &
        active=$((active + 1))
        if ((active >= TRACE_TEST_JOBS)); then
            wait -n
            active=$((active - 1))
        fi
    done
    while ((active > 0)); do
        wait -n
        active=$((active - 1))
    done

    for ((index = 0; index < count; index++)); do
        cat "$TRACE_TEST_RUN_ROOT/$index.log"
        if [[ ! -f "$TRACE_TEST_RUN_ROOT/$index.status" ]]; then
            echo "Trace case did not record a status: ${TRACE_TEST_LABELS[$index]}" >&2
            failed=1
            continue
        fi
        status=$(<"$TRACE_TEST_RUN_ROOT/$index.status")
        if [[ $status -ne 0 ]]; then
            failed=1
        fi
    done
    if [[ $failed -ne 0 ]]; then
        echo "One or more trace cases failed." >&2
        return 1
    fi
    echo "$summary"
}
