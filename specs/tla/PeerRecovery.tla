---------------------------- MODULE PeerRecovery ----------------------------
\* Snapshot-plus-WAL peer recovery for one shard.  Source sessions are keyed
\* by target node because the Rust registry permits one active source session
\* per shard.  File chunks, hashes, and hard links are abstracted as one atomic
\* verified snapshot install; the snapshot boundary, retention pin, ordered
\* suffix, exclusive finalize barrier, Raft admission, and persistent target
\* observation are modeled explicitly.

EXTENDS ShardReplication

CONSTANTS MaxRecoveries, RestorePendingOnRestart

SessionPhases ==
    {"None", "Starting", "SetupFailed", "SnapshotReady", "Installing",
     "CatchingUp", "Ready", "Preparing", "Finalizing",
     "AwaitingComplete", "Settling"}

NoFetched == NoWrite

VARIABLES
    recoveryAttempts,
    sessionPhase,
    sessionSource,
    sessionSourceEpoch,
    sessionTerm,
    sessionAllocation,
    sessionBoundary,
    sessionCursor,
    sessionHead,
    sessionSnapshot,
    sessionFetched,
    sessionFinalizePreparing,
    sessionMarkSubmitted,
    sessionSettlementRunning,
    sessionBumpSubmitted,
    pendingPrimary,
    pendingTerm,
    pendingAllocation,
    authoritativeWipeSafe

PeerRecoveryVars ==
    <<recoveryAttempts, sessionPhase, sessionSource, sessionSourceEpoch,
      sessionTerm, sessionAllocation, sessionBoundary, sessionCursor, sessionHead,
      sessionSnapshot, sessionFetched, sessionFinalizePreparing,
      sessionMarkSubmitted, sessionSettlementRunning, sessionBumpSubmitted,
      pendingPrimary, pendingTerm, pendingAllocation,
      authoritativeWipeSafe>>

PeerRecoveryInit ==
    /\ recoveryAttempts = 0
    /\ sessionPhase = [n \in Nodes |-> "None"]
    /\ sessionSource = [n \in Nodes |-> NoNode]
    /\ sessionSourceEpoch = [n \in Nodes |-> 0]
    /\ sessionTerm = [n \in Nodes |-> NoTerm]
    /\ sessionAllocation = [n \in Nodes |-> 0]
    /\ sessionBoundary = [n \in Nodes |-> 0]
    /\ sessionCursor = [n \in Nodes |-> 0]
    /\ sessionHead = [n \in Nodes |-> 0]
    /\ sessionSnapshot = [n \in Nodes |-> {}]
    /\ sessionFetched = [n \in Nodes |-> NoFetched]
    /\ sessionFinalizePreparing = [n \in Nodes |-> FALSE]
    /\ sessionMarkSubmitted = [n \in Nodes |-> FALSE]
    /\ sessionSettlementRunning = [n \in Nodes |-> FALSE]
    /\ sessionBumpSubmitted = [n \in Nodes |-> FALSE]
    /\ pendingPrimary = [n \in Nodes |-> NoNode]
    /\ pendingTerm = [n \in Nodes |-> NoTerm]
    /\ pendingAllocation = [n \in Nodes |-> 0]
    /\ authoritativeWipeSafe = TRUE

PendingMarkerPresent(target) ==
    pendingAllocation[target] > 0

\* The one-shard model has one fixed IndexUuid, so UUID matching is represented
\* by the durable local copy UUID equaling IndexUuid.  Allocation matching is
\* explicit against both durable copy identity and the target's applied view.
PendingMarkerMatchesCopy(target) ==
    /\ PendingMarkerPresent(target)
    /\ copyExists[target]
    /\ copyUuid[target] = IndexUuid
    /\ copyAllocation[target] = pendingAllocation[target]

PendingMarkerMatchesView(target) ==
    /\ PendingMarkerMatchesCopy(target)
    /\ views[target].allocations[target] = pendingAllocation[target]

ActiveRecoveryTargets ==
    {target \in Nodes : sessionPhase[target] # "None"}

RecoverySessionsClearedByCrash(node) ==
    {target \in Nodes :
        \/ sessionSource[target] = node
        \/ /\ target = node
           /\ copyMode[target] = "Recovering"}

CrashRecoveryState(node) ==
    LET cleared == RecoverySessionsClearedByCrash(node)
        releasedBoundaries(source) ==
            LET sourceTargets ==
                    {target \in cleared : sessionSource[target] = source}
            IN {sessionBoundary[target] : target \in sourceTargets}
    IN
    /\ sessionPhase' =
          [target \in Nodes |->
              IF target \in cleared THEN "None" ELSE sessionPhase[target]]
    /\ sessionSource' =
          [target \in Nodes |->
              IF target \in cleared THEN NoNode ELSE sessionSource[target]]
    /\ sessionSourceEpoch' =
          [target \in Nodes |->
              IF target \in cleared THEN 0 ELSE sessionSourceEpoch[target]]
    /\ sessionTerm' =
          [target \in Nodes |->
              IF target \in cleared THEN NoTerm ELSE sessionTerm[target]]
    /\ sessionAllocation' =
          [target \in Nodes |->
              IF target \in cleared THEN 0 ELSE sessionAllocation[target]]
    /\ sessionBoundary' =
          [target \in Nodes |->
              IF target \in cleared THEN 0 ELSE sessionBoundary[target]]
    /\ sessionCursor' =
          [target \in Nodes |->
              IF target \in cleared THEN 0 ELSE sessionCursor[target]]
    /\ sessionHead' =
          [target \in Nodes |->
              IF target \in cleared THEN 0 ELSE sessionHead[target]]
    /\ sessionSnapshot' =
          [target \in Nodes |->
              IF target \in cleared THEN {} ELSE sessionSnapshot[target]]
    /\ sessionFetched' =
          [target \in Nodes |->
              IF target \in cleared THEN NoFetched ELSE sessionFetched[target]]
    /\ sessionFinalizePreparing' =
          [target \in Nodes |->
              IF target \in cleared
              THEN FALSE
              ELSE sessionFinalizePreparing[target]]
    /\ sessionMarkSubmitted' =
          [target \in Nodes |->
              IF target \in cleared THEN FALSE ELSE sessionMarkSubmitted[target]]
    /\ sessionSettlementRunning' =
          [target \in Nodes |->
              IF target \in cleared
              THEN FALSE
              ELSE sessionSettlementRunning[target]]
    /\ sessionBumpSubmitted' =
          [target \in Nodes |->
              IF target \in cleared THEN FALSE ELSE sessionBumpSubmitted[target]]
    /\ pins' =
          [source \in Nodes |->
              IF source = node
              THEN {}
              ELSE pins[source] \ releasedBoundaries(source)]
    /\ exclusiveHolder' =
          [source \in Nodes |->
              IF source = node \/ exclusiveHolder[source] \in cleared
              THEN NoNode
              ELSE exclusiveHolder[source]]
    /\ copyMode' =
          [target \in Nodes |->
              IF target \in cleared /\ copyMode[target] = "Recovering"
              THEN "InstallMarker"
              ELSE IF target = node /\ copyMode[target] = "Pending"
              THEN "Active"
              ELSE copyMode[target]]
    /\ installMarker' =
          [target \in Nodes |->
              IF target \in cleared /\ copyMode[target] = "Recovering"
              THEN TRUE
              ELSE installMarker[target]]
    /\ UNCHANGED
          <<recoveryAttempts, pendingPrimary, pendingTerm, pendingAllocation,
            authoritativeWipeSafe>>

PreFinalizePhases ==
    {"Starting", "SetupFailed", "SnapshotReady", "Installing",
     "CatchingUp", "Ready"}

SourceAuthorityValid(target) ==
    LET source == sessionSource[target]
        local == views[source]
    IN
    /\ source \in Nodes
    /\ alive[source]
    /\ epoch[source] = sessionSourceEpoch[target]
    /\ local.primary = source
    /\ local.term = sessionTerm[target]
    /\ target \in local.replicas
    /\ target \notin local.inSync
    /\ IF AllocationIds
          THEN local.allocations[target] = sessionAllocation[target]
          ELSE TRUE

TargetNeedsRecovery(target, source) ==
    LET targetView == views[target]
    IN
    /\ alive[target]
    /\ targetView.primary = source
    /\ target \in targetView.replicas
    /\ target \notin targetView.inSync
    /\ copyMode[target] \notin {"StorageRetrying", "StorageFailed"}

AvailableRecoveryWrites(target, upperBound) ==
    LET source == sessionSource[target]
    IN {writeId \in ops[source] :
           /\ writeSeq[writeId] >= sessionCursor[target]
           /\ writeSeq[writeId] < upperBound
           /\ writeSeq[writeId] >= truncBelow[source]}

NextRecoveryWrite(target, upperBound) ==
    LET candidates == AvailableRecoveryWrites(target, upperBound)
    IN CHOOSE writeId \in candidates :
           \A other \in candidates :
               \/ writeSeq[other] > writeSeq[writeId]
               \/ /\ writeSeq[other] = writeSeq[writeId]
                  /\ other >= writeId

SourceObservation(target) ==
    LET source == sessionSource[target]
        local == views[source]
    IN
    CASE target \in local.inSync -> "InSync"
      [] /\ local.primary = source
         /\ local.term <= sessionTerm[target]
         /\ target \in local.replicas -> "Pending"
      [] OTHER -> "Impossible"

TargetObservation(target) ==
    LET local == views[target]
        allocationMatches ==
            /\ pendingAllocation[target] > 0
            /\ local.allocations[target] = pendingAllocation[target]
    IN
    IF AllocationIds
    THEN
        CASE target = local.primary -> "Admitted"
          [] /\ target \in local.inSync
             /\ allocationMatches -> "Admitted"
          [] \/ local.allocations[target] = 0
             \/ ~allocationMatches
             \/ local.term > pendingTerm[target]
             \/ local.primary # pendingPrimary[target] -> "Rejected"
          [] OTHER -> "Unknown"
    ELSE
        CASE target = local.primary \/ target \in local.inSync -> "Admitted"
          [] /\ target \in local.replicas
             /\ local.primary = pendingPrimary[target]
             /\ local.term <= pendingTerm[target] -> "Unknown"
          [] OTHER -> "Rejected"

ClearSession(target) ==
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "None"]
    /\ sessionSource' = [sessionSource EXCEPT ![target] = NoNode]
    /\ sessionSourceEpoch' = [sessionSourceEpoch EXCEPT ![target] = 0]
    /\ sessionTerm' = [sessionTerm EXCEPT ![target] = NoTerm]
    /\ sessionAllocation' =
          [sessionAllocation EXCEPT ![target] = 0]
    /\ sessionBoundary' = [sessionBoundary EXCEPT ![target] = 0]
    /\ sessionCursor' = [sessionCursor EXCEPT ![target] = 0]
    /\ sessionHead' = [sessionHead EXCEPT ![target] = 0]
    /\ sessionSnapshot' = [sessionSnapshot EXCEPT ![target] = {}]
    /\ sessionFetched' = [sessionFetched EXCEPT ![target] = NoFetched]
    /\ sessionFinalizePreparing' =
          [sessionFinalizePreparing EXCEPT ![target] = FALSE]
    /\ sessionMarkSubmitted' =
          [sessionMarkSubmitted EXCEPT ![target] = FALSE]
    /\ sessionSettlementRunning' =
          [sessionSettlementRunning EXCEPT ![target] = FALSE]
    /\ sessionBumpSubmitted' =
          [sessionBumpSubmitted EXCEPT ![target] = FALSE]

\* src/node/peer_recovery.rs::recovery_candidates +
\* run_peer_recovery and
\* src/transport/server/peer_recovery.rs::start_peer_recovery_inner.
\* Start is asynchronous and pollable; the target is not destructive yet.
StartRecovery(target, source) ==
    LET targetAllocation == views[target].allocations[target]
        sourceAllocation == views[source].allocations[target]
    IN
    /\ EnableRecovery
    /\ recoveryAttempts < MaxRecoveries
    /\ target \in Nodes
    /\ source \in Nodes
    /\ target # source
    /\ ActiveRecoveryTargets = {}
    /\ sessionPhase[target] = "None"
    /\ TargetNeedsRecovery(target, source)
    /\ alive[source]
    /\ raftConnected[source]
    /\ views[source].primary = source
    /\ views[source].term = activated[source]
    /\ target \in views[source].replicas
    /\ target \notin views[source].inSync
    /\ IF RestorePendingOnRestart
          THEN ~PendingMarkerMatchesCopy(target)
          ELSE TRUE
    \* Allocation-aware StartPeerRecovery carries the target-observed ID.
    \* The source rejects a stale request until both views name the same
    \* current assignment, after which the target retries.
    /\ IF AllocationIds
          THEN /\ targetAllocation > 0
               /\ targetAllocation = sourceAllocation
          ELSE TRUE
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Starting"]
    /\ sessionSource' = [sessionSource EXCEPT ![target] = source]
    /\ sessionSourceEpoch' =
          [sessionSourceEpoch EXCEPT ![target] = epoch[source]]
    /\ sessionTerm' =
          [sessionTerm EXCEPT ![target] = views[source].term]
    /\ sessionAllocation' =
          [sessionAllocation EXCEPT
              ![target] =
                  IF AllocationIds THEN targetAllocation ELSE sourceAllocation]
    /\ recoveryAttempts' = recoveryAttempts + 1
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, pendingAllocation, authoritativeWipeSafe>>

\* src/engine/tantivy.rs::prepare_peer_recovery_snapshot.  B is captured
\* under the translog lock, the writer is committed, and a retention pin is
\* registered before the snapshot is exposed.
SourceSnapshot(target) ==
    LET source == sessionSource[target]
        boundary == nextSeq[source]
        snapshot == {writeId \in ops[source] :
                         writeSeq[writeId] < boundary}
    IN
    /\ sessionPhase[target] = "Starting"
    /\ SourceAuthorityValid(target)
    /\ sessionPhase' =
          [sessionPhase EXCEPT ![target] = "SnapshotReady"]
    /\ sessionBoundary' =
          [sessionBoundary EXCEPT ![target] = boundary]
    /\ sessionCursor' = [sessionCursor EXCEPT ![target] = boundary]
    /\ sessionSnapshot' =
          [sessionSnapshot EXCEPT ![target] = snapshot]
    /\ committed' = [committed EXCEPT ![source] = boundary]
    /\ durableOps' = [durableOps EXCEPT ![source] = ops[source]]
    /\ pins' = [pins EXCEPT ![source] = @ \cup {boundary}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, docValue, nextSeq, truncBelow,
            copyExists, copyMode, installMarker, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, recoveryAttempts,
            sessionSource, sessionSourceEpoch, sessionTerm, sessionHead,
            sessionFetched, sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* launch_source_setup records a setup failure and source_start_status returns
\* it exactly once on the next poll.
SourceSetupFailure(target) ==
    /\ EnableRecoveryFailures
    /\ sessionPhase[target] = "Starting"
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "SetupFailed"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* source_start_status returns the stored launch_source_setup failure once.
PollSetupFailure(target) ==
    /\ sessionPhase[target] = "SetupFailed"
    /\ ClearSession(target)
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* src/shard/mod.rs::{begin_peer_recovery_target,
\* prepare_peer_recovery_target_blocking}.  Recovering begins only after Start
\* has completed, immediately before the old copy is wiped.
TargetBeginInstall(target) ==
    /\ sessionPhase[target] = "SnapshotReady"
    /\ alive[target]
    /\ copyMode[target]
       \notin {"Pending", "StorageRetrying", "StorageFailed"}
    /\ IF RestorePendingOnRestart
          THEN ~PendingMarkerMatchesCopy(target)
          ELSE TRUE
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Installing"]
    /\ copyMode' = [copyMode EXCEPT ![target] = "Recovering"]
    /\ installMarker' = [installMarker EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* src/shard/mod.rs::finalize_peer_recovery_target_blocking.  File transfer,
\* hashes, fsyncs, and strict Tantivy open are one verified atomic install.
InstallSnapshot(target) ==
    LET snapshot == sessionSnapshot[target]
    IN
    /\ sessionPhase[target] = "Installing"
    /\ alive[target]
    /\ authoritativeWipeSafe' =
          authoritativeWipeSafe
          /\ target # routing.primary
          /\ target \notin routing.inSync
    /\ ops' = [ops EXCEPT ![target] = snapshot]
    /\ durableOps' = [durableOps EXCEPT ![target] = snapshot]
    /\ docValue' =
          [docValue EXCEPT ![target] = RebuiltDocValue(snapshot)]
    /\ nextSeq' =
          [nextSeq EXCEPT ![target] = sessionBoundary[target]]
    /\ committed' =
          [committed EXCEPT ![target] = sessionBoundary[target]]
    /\ truncBelow' =
          [truncBelow EXCEPT ![target] = sessionBoundary[target]]
    /\ copyExists' = [copyExists EXCEPT ![target] = TRUE]
    /\ copyAllocation' =
          [copyAllocation EXCEPT ![target] = sessionAllocation[target]]
    /\ copyUuid' = [copyUuid EXCEPT ![target] = IndexUuid]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![target] = IF ReplicaFencing THEN sessionTerm[target] ELSE 0]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![target] =
                  IF ReplicaFencing /\ DurableReplicaFence
                  THEN sessionTerm[target]
                  ELSE 0]
    /\ installMarker' = [installMarker EXCEPT ![target] = FALSE]
    /\ sessionPhase' =
          [sessionPhase EXCEPT ![target] = "CatchingUp"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, pins, copyMode, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm>>

\* src/transport/server/peer_recovery.rs::fetch_recovery_ops_inner.
\* One operation represents a bounded response; choosing the least sequence
\* preserves strict ordering while allowing gaps.
FetchOps(target) ==
    LET upper ==
            IF sessionPhase[target] = "Finalizing"
            THEN sessionHead[target]
            ELSE nextSeq[sessionSource[target]]
        nextOperation == NextRecoveryWrite(target, upper)
    IN
    /\ sessionPhase[target] \in {"CatchingUp", "Finalizing"}
    /\ sessionFetched[target] = NoFetched
    /\ SourceAuthorityValid(target)
    /\ AvailableRecoveryWrites(target, upper) # {}
    /\ sessionFetched' =
          [sessionFetched EXCEPT ![target] = nextOperation]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionPhase,
            sessionSource, sessionSourceEpoch, sessionTerm, sessionBoundary,
            sessionCursor, sessionHead, sessionSnapshot,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* src/node/peer_recovery.rs::apply_recovery_operations.  Sequence numbers
\* must strictly advance; gaps are allowed.
ApplyOps(target) ==
    LET writeId == sessionFetched[target]
        sequenceNumber == writeSeq[writeId]
    IN
    /\ sessionPhase[target] \in {"CatchingUp", "Finalizing"}
    /\ writeId \in WriteIds
    /\ alive[target]
    /\ copyMode[target] = "Recovering"
    /\ sequenceNumber >= sessionCursor[target]
    /\ ops' = [ops EXCEPT ![target] = @ \cup {writeId}]
    /\ durableOps' =
          IF FaultMode = "C4"
          THEN durableOps
          ELSE [durableOps EXCEPT ![target] = @ \cup {writeId}]
    /\ docValue' =
          [docValue EXCEPT ![target][writeDoc[writeId]] = writeId]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![target] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ sessionCursor' =
          [sessionCursor EXCEPT ![target] = sequenceNumber + 1]
    /\ sessionFetched' =
          [sessionFetched EXCEPT ![target] = NoFetched]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            termMonotonic, recoveryAttempts, sessionPhase, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionHead,
            sessionSnapshot, sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* run_peer_recovery observes FetchRecoveryOps.complete at the current source
\* head after apply_recovery_operations has advanced its cursor.
FinishCatchUp(target) ==
    LET source == sessionSource[target]
        upper == nextSeq[source]
    IN
    /\ sessionPhase[target] = "CatchingUp"
    /\ sessionFetched[target] = NoFetched
    /\ SourceAuthorityValid(target)
    /\ AvailableRecoveryWrites(target, upper) = {}
    /\ sessionCursor' = [sessionCursor EXCEPT ![target] = upper]
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Ready"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionHead,
            sessionSnapshot, sessionFetched, sessionFinalizePreparing,
            sessionMarkSubmitted, sessionSettlementRunning,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* src/transport/server/peer_recovery.rs::
\* prepare_finalize_recovery_inner sets finalize_preparing before awaiting the
\* exclusive write guard.  Cancellation clears it.
BeginPrepareFinalize(target) ==
    /\ sessionPhase[target] = "Ready"
    /\ SourceAuthorityValid(target)
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Preparing"]
    /\ sessionFinalizePreparing' =
          [sessionFinalizePreparing EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionMarkSubmitted, sessionSettlementRunning,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* Dropping FinalizePreparingGuard after a cancelled or failed
\* prepare_finalize_recovery_inner clears finalize_preparing.
CancelPrepareFinalize(target) ==
    /\ EnableRecoveryFailures
    /\ sessionPhase[target] = "Preparing"
    /\ sessionFinalizePreparing[target]
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Ready"]
    /\ sessionFinalizePreparing' =
          [sessionFinalizePreparing EXCEPT ![target] = FALSE]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionMarkSubmitted, sessionSettlementRunning,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* The exclusive guard drains every shared write holder, then captures H.
AcquireFinalizeBarrier(target) ==
    LET source == sessionSource[target]
    IN
    /\ sessionPhase[target] = "Preparing"
    /\ sessionFinalizePreparing[target]
    /\ SourceAuthorityValid(target)
    /\ sharedHolders[source] = {}
    /\ exclusiveHolder[source] = NoNode
    /\ exclusiveHolder' =
          [exclusiveHolder EXCEPT ![source] = target]
    /\ sessionHead' =
          [sessionHead EXCEPT ![target] = nextSeq[source]]
    /\ sessionPhase' =
          [sessionPhase EXCEPT ![target] = "Finalizing"]
    /\ sessionFinalizePreparing' =
          [sessionFinalizePreparing EXCEPT ![target] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionSource, sessionSourceEpoch, sessionTerm,
            sessionBoundary, sessionCursor, sessionSnapshot, sessionFetched,
            sessionMarkSubmitted, sessionSettlementRunning,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* prepare_finalize_recovery_inner returns a complete tail through the captured
\* barrier head; apply_recovery_operations advances the target to that head.
FinishFinalizeTail(target) ==
    LET upper == sessionHead[target]
    IN
    /\ sessionPhase[target] = "Finalizing"
    /\ sessionFetched[target] = NoFetched
    /\ AvailableRecoveryWrites(target, upper) = {}
    /\ sessionCursor' = [sessionCursor EXCEPT ![target] = upper]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionPhase,
            sessionSource, sessionSourceEpoch, sessionTerm, sessionBoundary,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, authoritativeWipeSafe>>

\* src/shard/mod.rs::mark_peer_recovery_awaiting_membership_blocking.
\* The durable pending marker is written before CompleteFinalize.  Pending
\* copies are open and accept live replication.
TargetComplete(target) ==
    /\ sessionPhase[target] = "Finalizing"
    /\ sessionCursor[target] = sessionHead[target]
    /\ sessionFetched[target] = NoFetched
    /\ copyMode[target] = "Recovering"
    /\ copyMode' = [copyMode EXCEPT ![target] = "Pending"]
    /\ pendingPrimary' =
          [pendingPrimary EXCEPT ![target] = sessionSource[target]]
    /\ pendingTerm' =
          [pendingTerm EXCEPT ![target] = sessionTerm[target]]
    /\ pendingAllocation' =
          [pendingAllocation EXCEPT ![target] = sessionAllocation[target]]
    /\ sessionPhase' =
          [sessionPhase EXCEPT ![target] = "AwaitingComplete"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionSource, sessionSourceEpoch, sessionTerm,
            sessionBoundary, sessionCursor, sessionHead, sessionSnapshot,
            sessionFetched, sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted,
            authoritativeWipeSafe>>

\* node::reconciliation::open_local_assigned_shards calls
\* ShardManager::open_assigned_shard_with_settings, which reloads the durable
\* awaiting-membership marker. The runtime registration is restored only when
\* UUID and allocation match.
RestorePendingMarker(target) ==
    /\ RestorePendingOnRestart
    /\ alive[target]
    /\ copyMode[target] # "Pending"
    /\ PendingMarkerMatchesView(target)
    /\ pendingPrimary[target] \in Nodes
    /\ pendingTerm[target] > NoTerm
    /\ copyMode' = [copyMode EXCEPT ![target] = "Pending"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionPhase, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionAllocation,
            sessionBoundary, sessionCursor, sessionHead, sessionSnapshot,
            sessionFetched, sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, pendingAllocation, authoritativeWipeSafe>>

\* src/transport/server/peer_recovery.rs::
\* complete_finalize_recovery_inner starts the settlement task.
BeginSettlement(target) ==
    /\ sessionPhase[target] = "AwaitingComplete"
    /\ sessionHead[target] = sessionCursor[target]
    /\ exclusiveHolder[sessionSource[target]] = target
    /\ sessionPhase' = [sessionPhase EXCEPT ![target] = "Settling"]
    /\ sessionSettlementRunning' =
          [sessionSettlementRunning EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, recoveryAttempts, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

MarkInSyncCommand(target) ==
    LET source == sessionSource[target]
    IN RaftCommand("MarkReplicaInSync", source, target, source,
                   sessionTerm[target], source, {}, 0,
                   sessionAllocation[target], EmptyAllocations)

\* settle_peer_recovery submits MarkReplicaInSync(primary, term) repeatedly
\* until the local source view observes admission or impossibility.
ProposeMarkInSync(target) ==
    LET command == MarkInSyncCommand(target)
    IN
    /\ sessionPhase[target] = "Settling"
    /\ sessionSettlementRunning[target]
    /\ SourceObservation(target) = "Pending"
    /\ CanReachRaft(sessionSource[target])
    /\ QueueRaft(command)
    /\ sessionMarkSubmitted' =
          [sessionMarkSubmitted EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionPhase, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionSettlementRunning,
            sessionBumpSubmitted, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

SettlementBumpCommand(target) ==
    LET source == sessionSource[target]
        sourceAllocation == views[source].allocations[source]
    IN RaftCommand("ActivatePrimary", source, source, source,
                   sessionTerm[target], source, {}, 0, sourceAllocation,
                   EmptyAllocations)

\* After the settlement deadline, ActivatePrimary is used as a conditional
\* term bump so a delayed MarkReplicaInSync becomes impossible.
SettlementDeadline(target) ==
    LET command == SettlementBumpCommand(target)
    IN
    /\ sessionPhase[target] = "Settling"
    /\ sessionMarkSubmitted[target]
    /\ ~sessionBumpSubmitted[target]
    /\ SourceObservation(target) = "Pending"
    /\ CanReachRaft(sessionSource[target])
    /\ QueueRaft(command)
    /\ sessionBumpSubmitted' =
          [sessionBumpSubmitted EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionPhase, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* observe_membership releases the exclusive barrier only after the source's
\* local view shows membership or makes the old admission impossible.
ObserveAdmission(target) ==
    LET source == sessionSource[target]
        boundary == sessionBoundary[target]
    IN
    /\ sessionPhase[target] = "Settling"
    /\ SourceObservation(target) \in {"InSync", "Impossible"}
    /\ exclusiveHolder' =
          [exclusiveHolder EXCEPT ![source] = NoNode]
    /\ pins' = [pins EXCEPT ![source] = @ \ {boundary}]
    /\ ClearSession(target)
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, copyExists, copyMode, installMarker,
            messages, sharedHolders, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

\* The target's pending marker survives restart and is cleared only after its
\* own view observes admission (including promotion).
TargetObserveAdmitted(target) ==
    /\ copyMode[target] = "Pending"
    /\ TargetObservation(target) = "Admitted"
    /\ copyMode' = [copyMode EXCEPT ![target] = "Active"]
    /\ pendingPrimary' = [pendingPrimary EXCEPT ![target] = NoNode]
    /\ pendingTerm' = [pendingTerm EXCEPT ![target] = NoTerm]
    /\ pendingAllocation' =
          [pendingAllocation EXCEPT ![target] = 0]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic,
            recoveryAttempts, sessionPhase, sessionSource,
            sessionSourceEpoch, sessionTerm, sessionBoundary, sessionCursor,
            sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted,
            authoritativeWipeSafe>>

\* Definitive rejection closes the copy and restores the destructive marker
\* so ordinary shard open cannot serve it.
TargetObserveRejected(target) ==
    /\ copyMode[target] = "Pending"
    /\ TargetObservation(target) = "Rejected"
    /\ copyMode' = [copyMode EXCEPT ![target] = "InstallMarker"]
    /\ installMarker' = [installMarker EXCEPT ![target] = TRUE]
    /\ pendingPrimary' = [pendingPrimary EXCEPT ![target] = NoNode]
    /\ pendingTerm' = [pendingTerm EXCEPT ![target] = NoTerm]
    /\ pendingAllocation' =
          [pendingAllocation EXCEPT ![target] = 0]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, recoveryAttempts, sessionPhase,
            sessionSource, sessionSourceEpoch, sessionTerm, sessionBoundary,
            sessionCursor, sessionHead, sessionSnapshot, sessionFetched,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted,
            authoritativeWipeSafe>>

\* Dynamic-mapping reopen, index deletion, stale target replacement, and idle
\* expiry all abort only pre-finalize sessions.  This one action abstracts the
\* nondeterministic reason; the comments preserve the code boundary.
AbortSession(target) ==
    LET source == sessionSource[target]
        boundary == sessionBoundary[target]
    IN
    /\ EnableRecoveryFailures
    /\ sessionPhase[target] \in PreFinalizePhases
    /\ sessionPhase[target] # "SetupFailed"
    /\ IF boundary \in pins[source]
          THEN pins' = [pins EXCEPT ![source] = @ \ {boundary}]
          ELSE pins' = pins
    /\ copyMode' =
          IF copyMode[target] = "Recovering"
          THEN [copyMode EXCEPT ![target] = "InstallMarker"]
          ELSE copyMode
    /\ installMarker' =
          IF copyMode[target] = "Recovering"
          THEN [installMarker EXCEPT ![target] = TRUE]
          ELSE installMarker
    /\ ClearSession(target)
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, copyExists, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, recoveryAttempts,
            pendingPrimary, pendingTerm, authoritativeWipeSafe>>

\* Source-session idle reaping uses the same eligibility as AbortSession and
\* never touches preparing, guard-holding, or settling sessions.
ExpireSession(target) ==
    /\ sessionPhase[target] \in
          {"Starting", "SetupFailed", "SnapshotReady", "Installing",
           "CatchingUp", "Ready"}
    /\ AbortSession(target)

\* settle_abandoned_finalize: if Complete never submitted a mark, release the
\* barrier first, clean up the source session, and asynchronously bump the term.
ExpireFinalizeWithoutMark(target) ==
    LET source == sessionSource[target]
        boundary == sessionBoundary[target]
        command == SettlementBumpCommand(target)
    IN
    /\ EnableRecoveryFailures
    /\ sessionPhase[target] \in {"Finalizing", "AwaitingComplete"}
    /\ ~sessionMarkSubmitted[target]
    /\ exclusiveHolder[source] = target
    /\ CanReachRaft(source)
    /\ QueueRaft(command)
    /\ exclusiveHolder' =
          [exclusiveHolder EXCEPT ![source] = NoNode]
    /\ pins' = [pins EXCEPT ![source] = @ \ {boundary}]
    /\ copyMode' =
          IF copyMode[target] = "Recovering"
          THEN [copyMode EXCEPT ![target] = "InstallMarker"]
          ELSE copyMode
    /\ installMarker' =
          IF copyMode[target] = "Recovering"
          THEN [installMarker EXCEPT ![target] = TRUE]
          ELSE installMarker
    /\ ClearSession(target)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, copyExists, messages, sharedHolders,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            termMonotonic, recoveryAttempts, pendingPrimary, pendingTerm,
            authoritativeWipeSafe>>

PeerRecoveryTypeOK ==
    /\ recoveryAttempts \in 0..MaxRecoveries
    /\ RestorePendingOnRestart \in BOOLEAN
    /\ sessionPhase \in [Nodes -> SessionPhases]
    /\ sessionSource \in [Nodes -> Nodes \cup {NoNode}]
    /\ sessionSourceEpoch \in [Nodes -> Nat]
    /\ sessionTerm \in [Nodes -> 0..MaxTerm]
    /\ sessionAllocation \in [Nodes -> 0..MaxAllocationId]
    /\ sessionBoundary \in [Nodes -> 0..MaxWrites]
    /\ sessionCursor \in [Nodes -> 0..MaxWrites]
    /\ sessionHead \in [Nodes -> 0..MaxWrites]
    /\ sessionSnapshot \in [Nodes -> SUBSET WriteIds]
    /\ sessionFetched \in [Nodes -> 0..MaxWrites]
    /\ sessionFinalizePreparing \in [Nodes -> BOOLEAN]
    /\ sessionMarkSubmitted \in [Nodes -> BOOLEAN]
    /\ sessionSettlementRunning \in [Nodes -> BOOLEAN]
    /\ sessionBumpSubmitted \in [Nodes -> BOOLEAN]
    /\ pendingPrimary \in [Nodes -> Nodes \cup {NoNode}]
    /\ pendingTerm \in [Nodes -> 0..MaxTerm]
    /\ pendingAllocation \in [Nodes -> 0..MaxAllocationId]
    /\ authoritativeWipeSafe \in BOOLEAN
    /\ Cardinality(ActiveRecoveryTargets) <= 1

PeerRecoveryCoreNext ==
    \/ \E target \in Nodes, source \in Nodes : StartRecovery(target, source)
    \/ \E target \in Nodes :
           /\ SourceSnapshot(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ SourceSetupFailure(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ PollSetupFailure(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ TargetBeginInstall(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ InstallSnapshot(target)
           /\ UNCHANGED <<sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ FetchOps(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ ApplyOps(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ FinishCatchUp(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ BeginPrepareFinalize(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ CancelPrepareFinalize(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ AcquireFinalizeBarrier(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ FinishFinalizeTail(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ TargetComplete(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation>>
    \/ \E target \in Nodes : RestorePendingMarker(target)
    \/ \E target \in Nodes :
           /\ BeginSettlement(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ ProposeMarkInSync(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ SettlementDeadline(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ ObserveAdmission(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ TargetObserveAdmitted(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation>>
    \/ \E target \in Nodes :
           /\ TargetObserveRejected(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, sessionAllocation>>
    \/ \E target \in Nodes :
           /\ AbortSession(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ ExpireSession(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, pendingAllocation>>
    \/ \E target \in Nodes :
           /\ ExpireFinalizeWithoutMark(target)
           /\ UNCHANGED
                 <<copyAllocation, copyUuid, replicaFence,
                   durableReplicaFence, pendingAllocation>>

PeerRecoveryNext ==
    /\ PeerRecoveryCoreNext
    /\ UNCHANGED ApplySafetyVars

=============================================================================
