#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SPEC_DIR="$ROOT_DIR/specs/tla"
TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
DEFAULT_JAR="${XDG_CACHE_HOME:-$HOME/.cache}/ferrissearch-tla/v${TLA_VERSION}/tla2tools.jar"
WORKERS="${TLA_WORKERS:-12}"
TIMEOUT_SECONDS="${TLA_TIMEOUT_SECONDS:-300}"
LONG_TIMEOUT_SECONDS="${TLA_LONG_TIMEOUT_SECONDS:-1800}"
SIMULATION_TRACES="${TLA_SIMULATION_TRACES:-10000}"
SIMULATION_DEPTH="${TLA_SIMULATION_DEPTH:-80}"
SIMULATION_SEED="${TLA_SIMULATION_SEED:-20260926}"

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
    c2-fixed
    fence-volatile
    fence-durable
    c3
    c3-allocation-ids
    c4
    g1-empty-store
    g2-replica
    g2-primary
    g2-primary-no-replica
    g2-liveness
    pending-restart-legacy
    pending-restart-fixed
    l1
    l1-bump
    l2-primary-no-trigger
    l2-primary-idle
    l2-promotion
    l2
    storage-replica
    storage-primary
    storage-primary-no-replica
    storage-apply-replica
    storage-apply-primary
    storage-apply-primary-no-replica
    storage-apply-no-escalation
    s1-combined-replica
    s1-combined-primary
    s1-combined-liveness
    s1-combined-liveness-no-timeout
    d1-order-historical
    d1-order-fixed
    d1-replay-historical
    d1-replay-fixed
    d1-no-durable-tombstone
    d1-term-collision-seq-only
    d1-term-collision-fixed
    d1-gaps
    d1-term-collision-restart-committed
    d1-term-collision-restart-identity
    d1-primary-gap-max
    d1-primary-gap-processed
    d1-promotion-replay-noop
    two-shard
)

all_configs=(
    "${default_configs[@]}"
    fixed-crash
    fixed-partition
    fixed-simulation
)

list_configs() {
    cat <<'EOF'
c1-fast                 pass: crash-only replication smoke check
c1-recovery             pass: snapshot, suffix, finalize, and admission
c1-aba                  expected NoPartialServe violation: assignment ABA
c1-aba-fixed            pass: allocation-ID handshake variant
c2                      expected stale replication rejection violation
c2-allocation-ids       expected stale rejection violation with allocation IDs
c2-fixed                pass: allocation IDs plus replica-term fencing
fence-volatile          expected stale-probe violation after replica restart
fence-durable           pass: durable replica fence survives restart
c3                      expected NoAckedLoss violation: same-name empty disk
c3-allocation-ids       pass: empty disk fails closed on missing local identity
c4                      expected NoAckedLoss violation: asynchronous durability
g1-empty-store          pass: pre-activation empty-store disk loss is harmless
g2-replica              pass: in-sync replica disk loss and copy failure
g2-primary              pass: primary disk loss promotes an in-sync replica
g2-primary-no-replica   pass: no-survivor primary failure report is rejected
g2-liveness             pass: fair failure report, stale rejection, and recovery
pending-restart-legacy  expected NoPartialServe: pending marker ignored on restart
pending-restart-fixed   pass: matching pending marker is restored before recovery
l1                      pass: fair fault-free recovery liveness
l1-bump                 pass: settlement deadline resolves pending target
l2-primary-no-trigger   expected temporal failure without lifecycle activation
l2-primary-idle         pass: lifecycle activation resolves idle pending target
l2-primary-restart      alias: lifecycle activation resolves idle pending target
l2-promotion            pass: pending target resolves after another replica promotes
l2                      pass: fair recovery after one crash and restart
storage-replica         pass: persistent replica storage failure is removed
storage-primary         pass: persistent primary storage failure promotes
storage-primary-no-replica pass: promote-only primary report is rejected
storage-apply-replica   pass: open replica apply I/O escalates and is removed
storage-apply-primary   pass: open primary apply I/O escalates and promotes
storage-apply-primary-no-replica pass: no-candidate apply report is rejected
storage-apply-no-escalation expected temporal failure without apply escalation
s1-combined-replica     pass: storage fault, crash/reset, repair, and recovery
s1-combined-primary     pass: promote-only report across primary/leader crash
s1-combined-liveness    pass: timeout, redetection, recovery, and resumed write
s1-combined-liveness-no-timeout expected temporal failure: timeout assumption
d1-order-historical     expected NoCopyBehindAcked: arrival-order replica apply
d1-order-fixed          pass: concurrent writes converge under seq-aware apply
d1-replay-historical    expected replay loss from highest committed sequence
d1-replay-fixed         pass: processed-checkpoint replay through D1 planner
d1-no-durable-tombstone pass: replayed delete fences a late older index
d1-term-collision-seq-only expected B1NoCopyBehindAcked: term/seq collision
d1-term-collision-fixed pass: newer-term collision fails and re-recovers copy
d1-gaps                 pass: pull, recovery, and promotion NoOp close gaps
d1-term-collision-restart-committed expected B1RNoCopyBehindAcked
d1-term-collision-restart-identity pass: identity restores fence collision max
d1-primary-gap-max      expected B3NoRecoveryLoop: max-based detector loops
d1-primary-gap-processed pass: compare replica and primary processed checkpoints
d1-promotion-replay-noop pass: replay, fill NoOp, activate despite replica gap
two-shard               pass: red sibling does not block failover/allocation
fixed-crash             pass: exhaustive full fixed design with one crash
fixed-partition         pass: exhaustive full fixed design with one partition
fixed-simulation        pass: seeded depth-80 simulation of larger fixed bounds
EOF
}

if [[ "${1:-}" == "--list" ]]; then
    list_configs
    exit 0
fi

if [[ $# -eq 0 || "${1:-}" == "fast" ]]; then
    configs=("${default_configs[@]}")
elif [[ "${1:-}" == "all" ]]; then
    configs=("${all_configs[@]}")
else
    configs=("$@")
fi

run_config() {
    local name=$1
    local module
    local cfg
    local expected
    local mode="check"
    local timeout_seconds=$TIMEOUT_SECONDS

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
            expected="C2RejectsStaleMessage"
            ;;
        c2-allocation-ids|MC_C2_allocation_ids)
            module="MC_C2.tla"
            cfg="MC_C2_allocation_ids.cfg"
            expected="C2RejectsStaleMessage"
            ;;
        c2-fixed|MC_C2_fixed)
            module="MC_C2.tla"
            cfg="MC_C2_fixed.cfg"
            expected="pass"
            ;;
        fence-volatile|MC_Fence_volatile)
            module="MC_FenceDurability.tla"
            cfg="MC_Fence_volatile.cfg"
            expected="FenceRejectsStaleProbe"
            ;;
        fence-durable|MC_Fence_durable)
            module="MC_FenceDurability.tla"
            cfg="MC_Fence_durable.cfg"
            expected="pass"
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
        g1-empty-store|MC_G1_EmptyStore)
            module="MC_G1_EmptyStore.tla"
            cfg="MC_G1_EmptyStore.cfg"
            expected="pass"
            ;;
        g2-replica|MC_G2_Replica)
            module="MC_G2_CopyFailure.tla"
            cfg="MC_G2_Replica.cfg"
            expected="pass"
            ;;
        g2-primary|MC_G2_Primary)
            module="MC_G2_CopyFailure.tla"
            cfg="MC_G2_Primary.cfg"
            expected="pass"
            ;;
        g2-primary-no-replica|g2-primary-red|MC_G2_PrimaryNoReplica|MC_G2_PrimaryRed)
            module="MC_G2_CopyFailure.tla"
            cfg="MC_G2_PrimaryNoReplica.cfg"
            expected="pass"
            ;;
        g2-liveness|MC_G2_Liveness)
            module="MC_G2_Liveness.tla"
            cfg="MC_G2_Liveness.cfg"
            expected="pass"
            ;;
        pending-restart-legacy|MC_PendingRestart_legacy)
            module="MC_PendingRestart.tla"
            cfg="MC_PendingRestart_legacy.cfg"
            expected="NoPartialServe"
            ;;
        pending-restart-fixed|MC_PendingRestart_fixed)
            module="MC_PendingRestart.tla"
            cfg="MC_PendingRestart_fixed.cfg"
            expected="pass"
            ;;
        l1|MC_L1)
            module="MC_L1.tla"
            cfg="MC_L1.cfg"
            expected="pass"
            ;;
        l1-bump|MC_L1_Bump)
            module="MC_L1_Bump.tla"
            cfg="MC_L1_Bump.cfg"
            expected="pass"
            ;;
        l2-primary-no-trigger|MC_L2_PrimaryRestart_NoTrigger)
            module="MC_L2_PrimaryRestart_NoTrigger.tla"
            cfg="MC_L2_PrimaryRestart_NoTrigger.cfg"
            expected="temporal"
            ;;
        l2-primary-restart|MC_L2_PrimaryRestart)
            module="MC_L2_PrimaryRestart.tla"
            cfg="MC_L2_PrimaryRestart.cfg"
            expected="pass"
            ;;
        l2-primary-idle|MC_L2_PrimaryRestart_IdleShard)
            module="MC_L2_PrimaryRestart_IdleShard.tla"
            cfg="MC_L2_PrimaryRestart_IdleShard.cfg"
            expected="pass"
            ;;
        l2-promotion|MC_L2_Promotion)
            module="MC_L2_Promotion.tla"
            cfg="MC_L2_Promotion.cfg"
            expected="pass"
            ;;
        l2|MC_L2)
            module="MC_L2.tla"
            cfg="MC_L2.cfg"
            expected="pass"
            ;;
        storage-replica|MC_StorageReplica)
            module="MC_StorageFailure.tla"
            cfg="MC_StorageReplica.cfg"
            expected="pass"
            ;;
        storage-primary|MC_StoragePrimary)
            module="MC_StorageFailure.tla"
            cfg="MC_StoragePrimary.cfg"
            expected="pass"
            ;;
        storage-primary-no-replica|MC_StoragePrimaryNoReplica)
            module="MC_StorageFailure.tla"
            cfg="MC_StoragePrimaryNoReplica.cfg"
            expected="pass"
            ;;
        storage-apply-replica|MC_ApplyStorageReplica)
            module="MC_ApplyStorageFailure.tla"
            cfg="MC_ApplyStorageReplica.cfg"
            expected="pass"
            ;;
        storage-apply-primary|MC_ApplyStoragePrimary)
            module="MC_ApplyStorageFailure.tla"
            cfg="MC_ApplyStoragePrimary.cfg"
            expected="pass"
            ;;
        storage-apply-primary-no-replica|MC_ApplyStoragePrimaryNoReplica)
            module="MC_ApplyStorageFailure.tla"
            cfg="MC_ApplyStoragePrimaryNoReplica.cfg"
            expected="pass"
            ;;
        storage-apply-no-escalation|MC_ApplyStorageReplicaNoEscalation)
            module="MC_ApplyStorageFailure.tla"
            cfg="MC_ApplyStorageReplicaNoEscalation.cfg"
            expected="temporal"
            ;;
        s1-combined-replica|MC_S1_CombinedReplica)
            module="MC_S1_Combined.tla"
            cfg="MC_S1_CombinedReplica.cfg"
            expected="pass"
            ;;
        s1-combined-primary|MC_S1_CombinedPrimary)
            module="MC_S1_Combined.tla"
            cfg="MC_S1_CombinedPrimary.cfg"
            expected="pass"
            ;;
        s1-combined-liveness|MC_S1_CombinedLiveness)
            module="MC_S1_Combined.tla"
            cfg="MC_S1_CombinedLiveness.cfg"
            expected="pass"
            ;;
        s1-combined-liveness-no-timeout|MC_S1_CombinedLivenessNoTimeout)
            module="MC_S1_Combined.tla"
            cfg="MC_S1_CombinedLivenessNoTimeout.cfg"
            expected="temporal"
            ;;
        d1-order-historical|MC_D1_OrderHistorical)
            module="MC_D1_SeqNoApply.tla"
            cfg="MC_D1_OrderHistorical.cfg"
            expected="NoCopyBehindAcked"
            ;;
        d1-order-fixed|MC_D1_OrderFixed)
            module="MC_D1_SeqNoApply.tla"
            cfg="MC_D1_OrderFixed.cfg"
            expected="pass"
            ;;
        d1-replay-historical|MC_D1_ReplayHistorical)
            module="MC_D1_SeqNoApply.tla"
            cfg="MC_D1_ReplayHistorical.cfg"
            expected="D1ReplayPreservesAcknowledged"
            ;;
        d1-replay-fixed|MC_D1_ReplayFixed)
            module="MC_D1_SeqNoApply.tla"
            cfg="MC_D1_ReplayFixed.cfg"
            expected="pass"
            ;;
        d1-no-durable-tombstone|MC_D1_NoDurableTombstone)
            module="MC_D1_SeqNoApply.tla"
            cfg="MC_D1_NoDurableTombstone.cfg"
            expected="pass"
            ;;
        d1-term-collision-seq-only|MC_D1_TermCollisionSeqOnly)
            module="MC_D1_TermCollision.tla"
            cfg="MC_D1_TermCollisionSeqOnly.cfg"
            expected="B1NoCopyBehindAcked"
            ;;
        d1-term-collision-fixed|MC_D1_TermCollisionFixed)
            module="MC_D1_TermCollision.tla"
            cfg="MC_D1_TermCollisionFixed.cfg"
            expected="pass"
            ;;
        d1-gaps|MC_D1_Gaps)
            module="MC_D1_Gaps.tla"
            cfg="MC_D1_Gaps.cfg"
            expected="pass"
            ;;
        d1-term-collision-restart-committed|MC_D1_TermCollisionRestartCommitted)
            module="MC_D1_TermCollisionRestart.tla"
            cfg="MC_D1_TermCollisionRestartCommitted.cfg"
            expected="B1RNoCopyBehindAcked"
            ;;
        d1-term-collision-restart-identity|MC_D1_TermCollisionRestartIdentity)
            module="MC_D1_TermCollisionRestart.tla"
            cfg="MC_D1_TermCollisionRestartIdentity.cfg"
            expected="pass"
            ;;
        d1-primary-gap-max|MC_D1_PrimaryGapMaxBased)
            module="MC_D1_PrimaryGap.tla"
            cfg="MC_D1_PrimaryGapMaxBased.cfg"
            expected="B3NoRecoveryLoop"
            ;;
        d1-primary-gap-processed|MC_D1_PrimaryGapProcessed)
            module="MC_D1_PrimaryGap.tla"
            cfg="MC_D1_PrimaryGapProcessed.cfg"
            expected="pass"
            ;;
        d1-promotion-replay-noop|MC_D1_PromotionReplayNoOp)
            module="MC_D1_PromotionReplayNoOp.tla"
            cfg="MC_D1_PromotionReplayNoOp.cfg"
            expected="pass"
            ;;
        two-shard|MC_TwoShardIsolation)
            module="MC_TwoShardIsolation.tla"
            cfg="MC_TwoShardIsolation.cfg"
            expected="pass"
            ;;
        fixed-crash|MC_Fixed_Crash)
            module="Invariants.tla"
            cfg="MC_Fixed_Crash.cfg"
            expected="pass"
            timeout_seconds=$LONG_TIMEOUT_SECONDS
            ;;
        fixed-partition|MC_Fixed_Partition)
            module="Invariants.tla"
            cfg="MC_Fixed_Partition.cfg"
            expected="pass"
            timeout_seconds=$LONG_TIMEOUT_SECONDS
            ;;
        fixed-simulation|MC_Fixed_Simulation)
            module="Invariants.tla"
            cfg="MC_Fixed_Simulation.cfg"
            expected="pass"
            mode="simulate"
            timeout_seconds=$LONG_TIMEOUT_SECONDS
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
        if [[ "$mode" == "simulate" ]]; then
            timeout "${timeout_seconds}s" \
                java \
                -Djava.io.tmpdir="$java_tmp" \
                -XX:+UseParallelGC \
                -cp "$JAR" \
                tlc2.TLC \
                -deadlock \
                -simulate "num=${SIMULATION_TRACES}" \
                -depth "$SIMULATION_DEPTH" \
                -seed "$SIMULATION_SEED" \
                -metadir "$states" \
                -config "$cfg" \
                "$module"
        else
            timeout "${timeout_seconds}s" \
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
        fi
    ) >"$log" 2>&1
    local status=$?
    set -e
    cat "$log"

    if [[ $status -eq 124 ]]; then
        echo "TLA+ configuration '$name' exceeded ${timeout_seconds}s" >&2
        return 1
    fi

    if [[ "$expected" == "pass" ]]; then
        if [[ $status -ne 0 ]] ||
            grep -Fq "Error:" "$log" ||
            ! grep -Fq "Finished in " "$log"; then
            echo "TLA+ configuration '$name' was expected to pass" >&2
            return 1
        fi
    elif [[ "$expected" == "temporal" ]]; then
        if [[ $status -eq 0 ]] ||
            ! grep -Fq "Error: Temporal properties were violated." "$log"; then
            echo "TLA+ configuration '$name' must retain its expected temporal violation" >&2
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
