-------------------------------- MODULE Faults -------------------------------
\* Crash/restart and network/failure-detector actions.  Crash keeps durable
\* state in request-durability configurations; C4 rolls back to the last
\* explicitly flushed durable set.  A metadata partition blocks local Raft
\* view application but intentionally leaves data RPC delivery possible,
\* matching the stale-primary counterexample described in the recovery plan.

EXTENDS PeerRecovery

VARIABLES
    crashCount,
    partitionCount,
    diskLost,
    faultsStopped,
    lifecyclePhase,
    storageFaultInjected

LifecyclePhases ==
    {"Idle", "RoutingProposed", "RoutingCommitted", "RoutingRejected",
     "MembershipRemoved", "RemoveProposed", "Removed",
     "RejoinProposed", "Rejoined", "AllocationProposed"}

FaultVars ==
    <<crashCount, partitionCount, diskLost, faultsStopped, lifecyclePhase,
      storageFaultInjected>>

FaultInit ==
    /\ crashCount = 0
    /\ partitionCount = 0
    /\ diskLost = [n \in Nodes |-> FALSE]
    /\ faultsStopped = (FaultMode = "L1")
    /\ lifecyclePhase = [n \in Nodes |-> "Idle"]
    /\ storageFaultInjected = FALSE

WritesOwnedBy(node) ==
    {w \in WriteIds :
        /\ writePrimary[w] = node
        /\ writeStatus[w] \in {"Routed", "Replicating"}}

DropNodeMessages(node) ==
    \* Requests or responses already handed to the transport may arrive after
    \* the sender crashes. Only messages addressed to the crashed process are
    \* lost immediately.
    {m \in messages : m.to = node}

\* src/node process lifecycle + src/wal/mod.rs::open. Transport requests and
\* responses already emitted by the crashing process stay in flight; only
\* traffic addressed to that stopped incarnation is dropped immediately.
Crash(node) ==
    LET lostWrites == WritesOwnedBy(node)
        survivingOps ==
            IF FaultMode = "C4" THEN durableOps[node] ELSE ops[node]
    IN
    /\ ~faultsStopped
    /\ node \in Nodes
    /\ alive[node]
    /\ crashCount < MaxCrashes
    /\ alive' = [alive EXCEPT ![node] = FALSE]
    /\ raftConnected' = [raftConnected EXCEPT ![node] = FALSE]
    /\ activated' = [activated EXCEPT ![node] = NoTerm]
    /\ activationPending' =
          [activationPending EXCEPT ![node] = NoTerm]
    /\ writeStatus' =
          [w \in WriteIds |->
              IF w \in lostWrites THEN "Failed" ELSE writeStatus[w]]
    /\ failed' = failed \cup lostWrites
    /\ messages' = messages \ DropNodeMessages(node)
    /\ sharedHolders' =
          [n \in Nodes |->
              IF n = node
              THEN {}
              ELSE sharedHolders[n] \ lostWrites]
    /\ ops' = [ops EXCEPT ![node] = survivingOps]
    /\ docValue' =
          [docValue EXCEPT ![node] = RebuiltDocValue(survivingOps)]
    /\ nextSeq' =
          [nextSeq EXCEPT ![node] = NextSequenceAfter(survivingOps)]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![node] =
                  IF ReplicaFencing THEN durableReplicaFence[node] ELSE 0]
    /\ CrashRecoveryState(node)
    /\ raftLeader' = IF raftLeader = node THEN NoNode ELSE raftLeader
    /\ crashCount' = crashCount + 1
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters,
            routing, epoch, nextWrite, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, durableOps, committed, truncBelow,
            copyExists, copyAllocation, copyUuid, durableReplicaFence,
            acked, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, diskLost, partitionCount,
            faultsStopped, lifecyclePhase, storageFaultInjected>>

\* src/node startup plus HotTranslog::open/replay starts a new process
\* incarnation while durable shard and pending-recovery markers survive.
Restart(node) ==
    /\ ~faultsStopped
    /\ node \in Nodes
    /\ ~alive[node]
    /\ alive' = [alive EXCEPT ![node] = TRUE]
    /\ epoch' = [epoch EXCEPT ![node] = @ + 1]
    /\ raftConnected' = [raftConnected EXCEPT ![node] = TRUE]
    /\ activated' = [activated EXCEPT ![node] = NoTerm]
    /\ activationPending' =
          [activationPending EXCEPT ![node] = NoTerm]
    /\ UNCHANGED
          <<RaftVars, routing, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation,
            copyUuid, replicaFence, durableReplicaFence, copyMode,
            installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, storageFaultInjected, PeerRecoveryVars>>

\* C2 only: metadata failure detector may suspect a live node whose local
\* ClusterManager view stops advancing.  Data-plane RPC messages remain usable.
\* Lost/delayed Ping and Raft connectivity as observed by the leader's
\* src/node/mod.rs failure detector; data-plane RPC connectivity is separate.
PartitionMetadata(node) ==
    /\ ~faultsStopped
    /\ FaultMode = "C2"
    /\ alive[node]
    /\ raftConnected[node]
    /\ partitionCount < MaxPartitions
    /\ raftConnected' = [raftConnected EXCEPT ![node] = FALSE]
    /\ raftLeader' = IF raftLeader = node THEN NoNode ELSE raftLeader
    /\ partitionCount' = partitionCount + 1
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters,
            routing, alive, epoch, activated, activationPending,
            nextWrite, writeStatus, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, crashCount,
            diskLost, faultsStopped, lifecyclePhase, storageFaultInjected,
            PeerRecoveryVars>>

\* Successful Ping/JoinCluster/Raft connectivity restores metadata delivery.
HealMetadata(node) ==
    /\ ~faultsStopped
    /\ FaultMode = "C2"
    /\ alive[node]
    /\ ~raftConnected[node]
    /\ raftConnected' = [raftConnected EXCEPT ![node] = TRUE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, activated, activationPending,
            nextWrite, writeStatus, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe,             ackMembershipSafe, termMonotonic, crashCount, partitionCount,
            diskLost, faultsStopped, lifecyclePhase, storageFaultInjected,
            PeerRecoveryVars>>

\* transport timeout/drop.  Delay is represented by simply not choosing a
\* delivery action.
\* A TransportClient request or response times out or is dropped.
LoseMsg(message) ==
    /\ ~faultsStopped
    /\ message \in messages
    /\ messages' = messages \ {message}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe,             ackMembershipSafe, termMonotonic, crashCount, partitionCount,
            diskLost, faultsStopped, lifecyclePhase, storageFaultInjected,
            PeerRecoveryVars>>

FailureDetectorMayRemove(node) ==
    CASE FaultMode = "C2" -> ~alive[node] \/ ~raftConnected[node]
      [] OTHER -> ~alive[node]

CommittedResult(kind, target, accepted) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command.kind = kind
        /\ raftLog[position].command.target = target
        /\ raftLog[position].accepted = accepted

CommittedRoutingRemoval(node) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command.kind = "UpdateRouting"
        /\ raftLog[position].command.target = node
        /\ raftLog[position].accepted
        /\ node # raftLog[position].state.primary
        /\ node \notin raftLog[position].state.replicas

CommittedAllocation(node) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command.kind = "UpdateRouting"
        /\ raftLog[position].command.target = node
        /\ raftLog[position].accepted
        /\ node \in raftLog[position].state.replicas

FailShardCopyCommand(node, allocationId, promotionCandidate) ==
    RaftCommand("FailShardCopy", node, node, NoNode, NoTerm,
                promotionCandidate, {}, 0,
                allocationId, EmptyAllocations)

LeaderVisiblePromotionCandidates(failedNode) ==
    LET leaderNode == IF raftLeader \in Nodes THEN raftLeader ELSE failedNode
        leaderView == views[leaderNode]
    IN {candidate \in leaderView.inSync \ {failedNode} : alive[candidate]}

\* The leader's observed checkpoint tracker is abstracted by nextSeq.  Any
\* highest-checkpoint tie remains nondeterministic, while the Raft state
\* machine later validates only current in-sync membership.
LeaderHighestCheckpointCandidates(failedNode) ==
    LET candidates == LeaderVisiblePromotionCandidates(failedNode)
    IN {candidate \in candidates :
           \A other \in candidates : nextSeq[other] <= nextSeq[candidate]}

LeaderSelectedFailureCandidate(failedNode, candidate) ==
    LET leaderNode == IF raftLeader \in Nodes THEN raftLeader ELSE failedNode
        leaderView == views[leaderNode]
        candidates == LeaderVisiblePromotionCandidates(failedNode)
    IN IF failedNode = leaderView.primary /\ candidates # {}
       THEN candidate \in LeaderHighestCheckpointCandidates(failedNode)
       ELSE candidate = NoNode

LocalCopyMatchesView(node, local) ==
    /\ copyExists[node]
    /\ copyUuid[node] = IndexUuid
    /\ copyAllocation[node] > 0
    /\ copyAllocation[node] = local.allocations[node]

CopyFailureReportRequired(node) ==
    LET local == views[node]
        assigned ==
            /\ local.allocations[node] > 0
            /\ (node = local.primary \/ node \in local.replicas)
        authoritative ==
            \/ node = local.primary
            \/ node \in local.inSync
        failedInstall == installMarker[node]
    IN
    /\ AllocationIds
    /\ local.initialized
    /\ assigned
    \* A newly allocated out-of-sync replica is intentionally missing or may
    \* retain an old copy until recovery replaces it.  Authoritative copies
    \* and failed installs report; ordinary recovery targets do not churn.
    /\ (authoritative \/ failedInstall)
    \* Persistent local I/O is retryable until the bounded retry/window
    \* abstraction reaches StorageFailed or ApplyFailed. Corruption is
    \* StorageCorrupt immediately.
    /\ \/ ~LocalCopyMatchesView(node, local)
       \/ copyMode[node] \in ReportableStorageFailureModes
    /\ copyMode[node] \notin StorageRetryModes

\* node::reconciliation::open_local_assigned_shards failure handling,
\* TransportService::fail_shard_copy, and
\* TransportClient::forward_fail_shard_copy.  The request carries the
\* target-observed allocation ID.  For a failed primary, the Raft leader also
\* carries a live highest-checkpoint candidate from its view; the state machine
\* performs the exact allocation and current in-sync checks.
ReportShardCopyFailure(node, candidate) ==
    LET local == views[node]
        command ==
            FailShardCopyCommand(node, local.allocations[node], candidate)
    IN
    /\ node \in Nodes
    /\ candidate \in Nodes \cup {NoNode}
    /\ alive[node]
    /\ CanReachRaft(node)
    /\ CopyFailureReportRequired(node)
    /\ LeaderSelectedFailureCandidate(node, candidate)
    /\ QueueRaft(command)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, storageFaultInjected, PeerRecoveryVars>>

\* node::reconciliation storage-open classification. Corrupt manifest, frame,
\* Tantivy metadata/segments, or marker decoding is definitive immediately.
CorruptShardStorage(node) ==
    /\ FaultMode = "S1"
    /\ ~faultsStopped
    /\ ~storageFaultInjected
    /\ alive[node]
    /\ copyExists[node]
    /\ copyMode[node] = "Active"
    /\ copyExists' = [copyExists EXCEPT ![node] = FALSE]
    /\ copyMode' = [copyMode EXCEPT ![node] = "StorageCorrupt"]
    /\ storageFaultInjected' = TRUE
    /\ ops' = [ops EXCEPT ![node] = {}]
    /\ durableOps' = [durableOps EXCEPT ![node] = {}]
    /\ docValue' =
          [docValue EXCEPT ![node] = [doc \in Docs |-> NoWrite]]
    /\ nextSeq' = [nextSeq EXCEPT ![node] = 0]
    /\ committed' = [committed EXCEPT ![node] = 0]
    /\ truncBelow' = [truncBelow EXCEPT ![node] = 0]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, pins, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, PeerRecoveryVars>>

\* Persistent local I/O while opening a copy, persisting its fence, or reading
\* recovery markers first enters bounded retry/backoff.  This copy-unavailable
\* abstraction deliberately clears copyExists; apply-level failure below keeps
\* the already-open copy readable.
BeginPersistentStorageFailure(node) ==
    /\ FaultMode = "S1"
    /\ ~faultsStopped
    /\ ~storageFaultInjected
    /\ alive[node]
    /\ copyExists[node]
    /\ copyMode[node] = "Active"
    /\ copyExists' = [copyExists EXCEPT ![node] = FALSE]
    /\ copyMode' = [copyMode EXCEPT ![node] = "StorageRetrying"]
    /\ storageFaultInjected' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, PeerRecoveryVars>>

\* ShardManager::{ensure_local_apply_allowed,record_local_apply_result}, called
\* by apply_replica_operation and the primary write handlers: the engine and
\* identity are still open/readable, but every WAL/fsync/engine mutation fails.
\* The first failed mutation moves ApplyFailing to ApplyRetrying.
BeginPersistentApplyFailure(node) ==
    /\ FaultMode = "S1"
    /\ ~faultsStopped
    /\ ~storageFaultInjected
    /\ alive[node]
    /\ copyExists[node]
    /\ copyMode[node] = "Active"
    /\ copyMode' = [copyMode EXCEPT ![node] = "ApplyFailing"]
    /\ storageFaultInjected' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, PeerRecoveryVars>>

\* Process restart redetects the same persistent open/fence/marker fault with
\* a fresh per-process retry budget.
RedetectPersistentStorageFailure(node) ==
    /\ FaultMode = "S1"
    /\ alive[node]
    /\ ~copyExists[node]
    /\ copyMode[node] = "StorageFailing"
    /\ copyMode' = [copyMode EXCEPT ![node] = "StorageRetrying"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            FaultVars, PeerRecoveryVars>>

EscalatePersistentStorageFailure(node) ==
    /\ FaultMode = "S1"
    /\ alive[node]
    /\ copyMode[node] = "StorageRetrying"
    /\ copyMode' = [copyMode EXCEPT ![node] = "StorageFailed"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            FaultVars, PeerRecoveryVars>>

EscalatePersistentApplyFailure(node) ==
    /\ FaultMode = "S1"
    /\ alive[node]
    /\ copyExists[node]
    /\ copyMode[node] = "ApplyRetrying"
    /\ copyMode' = [copyMode EXCEPT ![node] = "ApplyFailed"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            FaultVars, PeerRecoveryVars>>

\* After an exact-allocation failure has committed, an operator repair clears
\* the persistent local fault. It remains enabled if allocation races ahead of
\* repair; only peer recovery may install the fresh allocation identity.
RepairPersistentStorageFault(node) ==
    /\ FaultMode = "S1"
    /\ alive[node]
    /\ CommittedResult("FailShardCopy", node, TRUE)
    /\ copyMode[node] \in AllStorageFailureModes
    /\ copyMode' = [copyMode EXCEPT ![node] = "Active"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            FaultVars, PeerRecoveryVars>>

\* src/node/mod.rs lifecycle reconciliation proactively invokes
\* ensure_primary_activated for every local primary whose incarnation-local
\* activation record does not match its applied routing term.
LifecycleActivationNeeded(node) ==
    /\ node \in Nodes
    /\ alive[node]
    /\ views[node].primary = node
    /\ activated[node] # views[node].term

LifecycleProposeActivation(node) ==
    /\ LifecycleActivationNeeded(node)
    /\ ProposeActivate(node)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

\* openraft leader election abstraction.  Election details are delegated to
\* openraft; the model elects one connected live voter only after the previous
\* leader is absent and only when a live voter majority exists.
ElectLeader(candidate) ==
    /\ candidate \in LiveConnectedVoters
    /\ HasRaftQuorum
    /\ \/ raftLeader = NoNode
       \/ raftLeader \notin LiveConnectedVoters
    /\ raftLeader' = candidate
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters,
            ReplicationVars, PeerRecoveryVars, crashCount, partitionCount,
            diskLost, faultsStopped, lifecyclePhase, storageFaultInjected>>

\* src/node/mod.rs dead-node loop + IndexMetadata::{remove_node,
\* select_promotion_candidate,promote_replica_to}.  Checkpoint ranking is
\* abstracted as nondeterministic choice among the authoritative in-sync set.
SuspectAndRemove(leader, node, candidate) ==
    LET local == views[leader]
        lostReplica == node \in local.replicas
        availableCandidates == local.inSync \ {node}
        losingPrimary == local.primary = node
        chosenPrimary ==
            IF losingPrimary THEN candidate ELSE local.primary
        replicasWithoutNode == local.replicas \ {node}
        proposedReplicas ==
            IF losingPrimary
            THEN replicasWithoutNode \ {candidate}
            ELSE replicasWithoutNode
        proposedUnassigned ==
            local.unassigned
            + IF lostReplica THEN 1 ELSE 0
            + IF losingPrimary THEN 1 ELSE 0
        proposedAllocations ==
            [assigned \in Nodes |->
                IF assigned = chosenPrimary
                   \/ assigned \in proposedReplicas
                THEN local.allocations[assigned]
                ELSE 0]
        command ==
            RaftCommand("UpdateRouting", leader, node, NoNode, NoTerm,
                        chosenPrimary, proposedReplicas, proposedUnassigned,
                        0, proposedAllocations)
    IN
    /\ ~faultsStopped
    /\ leader \in Nodes
    /\ node \in Nodes
    /\ candidate \in Nodes
    /\ leader # node
    /\ leader = raftLeader
    /\ CanReachRaft(leader)
    /\ lifecyclePhase[node] = "Idle"
    /\ FailureDetectorMayRemove(node)
    /\ node = local.primary \/ node \in local.replicas
    /\ IF losingPrimary
          THEN candidate \in availableCandidates
          ELSE candidate = local.primary
    /\ proposedUnassigned <= Cardinality(Nodes)
    /\ QueueRaft(command)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RoutingProposed"]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            storageFaultInjected, PeerRecoveryVars>>

\* client_write_checked(UpdateIndex) returned success to the dead-node loop.
ObserveRoutingAccepted(node) ==
    /\ lifecyclePhase[node] = "RoutingProposed"
    /\ CommittedRoutingRemoval(node)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RoutingCommitted"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* client_write_checked(UpdateIndex) returned the state-machine rejection.
ObserveRoutingRejected(node) ==
    /\ lifecyclePhase[node] = "RoutingProposed"
    /\ CommittedResult("UpdateRouting", node, FALSE)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RoutingRejected"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* src/node/mod.rs::dead-node loop defers RemoveNode after a rejected
\* UpdateIndex and retries from a fresh view on a later lifecycle tick.
DeferRejectedRouting(node) ==
    /\ lifecyclePhase[node] = "RoutingRejected"
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Idle"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* src/node/mod.rs calls openraft::change_membership only after the routing
\* update succeeds.  The abstraction performs the committed voter removal
\* atomically once both the old and new voter sets have a live majority.
ChangeRaftMembership(leader, node) ==
    LET remaining == raftVoters \ {node}
        liveRemaining ==
            {voter \in remaining : alive[voter] /\ raftConnected[voter]}
        remainingQuorum == (Cardinality(remaining) \div 2) + 1
    IN
    /\ lifecyclePhase[node] = "RoutingCommitted"
    /\ leader = raftLeader
    /\ CanReachRaft(leader)
    /\ remaining # {}
    /\ Cardinality(liveRemaining) >= remainingQuorum
    /\ raftVoters' = remaining
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "MembershipRemoved"]
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftLeader,
            ReplicationVars, PeerRecoveryVars, crashCount, partitionCount,
            diskLost, faultsStopped, storageFaultInjected>>

\* src/node/mod.rs submits ClusterCommand::RemoveNode after membership removal.
ProposeRemoveNode(leader, node) ==
    LET command ==
            RaftCommand("RemoveNode", leader, node, NoNode, NoTerm, NoNode,
                        {}, 0, 0, EmptyAllocations)
    IN
    /\ ~faultsStopped
    /\ lifecyclePhase[node] = "MembershipRemoved"
    /\ leader = raftLeader
    /\ CanReachRaft(leader)
    /\ node \in views[leader].members
    /\ node # views[leader].primary
    /\ node \notin views[leader].replicas
    /\ QueueRaft(command)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RemoveProposed"]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            storageFaultInjected, PeerRecoveryVars>>

\* The leader observes successful ClusterCommand::RemoveNode application.
ObserveNodeRemoved(node) ==
    /\ lifecyclePhase[node] = "RemoveProposed"
    /\ CommittedResult("RemoveNode", node, TRUE)
    /\ node \notin routing.members
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Removed"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* src/node/mod.rs follower JoinCluster retry.  A removed process must commit
\* AddNode before the allocator may use it again.
Rejoin(node) ==
    LET command ==
            RaftCommand("AddNode", node, node, NoNode, NoTerm, NoNode, {}, 0,
                        0, EmptyAllocations)
    IN
    /\ ~faultsStopped
    /\ lifecyclePhase[node] = "Removed"
    /\ CanReachRaft(node)
    /\ node \notin routing.members
    /\ QueueRaft(command)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RejoinProposed"]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            storageFaultInjected, PeerRecoveryVars>>

\* JoinCluster observes committed ClusterCommand::AddNode registration.
ObserveRejoin(node) ==
    /\ lifecyclePhase[node] = "RejoinProposed"
    /\ CommittedResult("AddNode", node, TRUE)
    /\ node \in routing.members
    /\ node \in raftVoters
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Rejoined"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* src/cluster/state.rs::allocate_unassigned_replicas and the allocator phase
\* at the end of src/node/mod.rs's leader tick.  The target must be alive and
\* registered in the current leader's applied view.  A previously removed node
\* reaches this action only after committed AddNode observation.
AllocateAfterLifecycle(leader, target) ==
    LET local == views[leader]
        allocationId ==
            IF AllocationIds THEN Len(raftLog) + 2 ELSE 1
        proposedAllocations ==
            [local.allocations EXCEPT ![target] = allocationId]
        command ==
            RaftCommand("UpdateRouting", leader, target, NoNode, NoTerm,
                        local.primary, local.replicas \cup {target},
                        local.unassigned - 1, 0, proposedAllocations)
    IN
    /\ EnableRecovery
    /\ leader = raftLeader
    /\ CanReachRaft(leader)
    /\ alive[target]
    /\ lifecyclePhase[target] \in {"Idle", "Rejoined"}
    /\ local.unassigned > 0
    /\ local.allocations[local.primary] > 0
    /\ target \in local.members
    /\ target # local.primary
    /\ target \notin local.replicas
    /\ allocationId <= MaxAllocationId
    /\ QueueRaft(command)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![target] = "AllocationProposed"]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            storageFaultInjected, PeerRecoveryVars>>

\* The leader allocator observes successful UpdateIndex assignment.
ObserveAllocationAccepted(target) ==
    /\ lifecyclePhase[target] = "AllocationProposed"
    /\ CommittedAllocation(target)
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![target] = "Idle"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* The leader allocator observes a rejected stale UpdateIndex proposal.
ObserveAllocationRejected(target) ==
    /\ lifecyclePhase[target] = "AllocationProposed"
    /\ CommittedResult("UpdateRouting", target, FALSE)
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![target] = "Rejoined"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped, storageFaultInjected>>

\* src/engine/tantivy.rs::flush_with_global_checkpoint and
\* src/wal/mod.rs::{truncate,truncate_below}.  The model retains logical
\* operation history but advances the WAL floor no farther than any pin.
Flush(node) ==
    LET retentionBound ==
            IF pins[node] = {}
            THEN nextSeq[node]
            ELSE MinNatSet(pins[node] \cup {nextSeq[node]})
    IN
    /\ alive[node]
    /\ copyExists[node]
    /\ \/ durableOps[node] # ops[node]
       \/ committed[node] # nextSeq[node]
       \/ truncBelow[node] < retentionBound
    /\ durableOps' = [durableOps EXCEPT ![node] = ops[node]]
    /\ committed' = [committed EXCEPT ![node] = nextSeq[node]]
    /\ truncBelow' =
          [truncBelow EXCEPT
              ![node] = IF @ < retentionBound THEN retentionBound ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, docValue, nextSeq, pins,
            copyExists, copyAllocation, copyUuid, replicaFence,
            durableReplicaFence, copyMode, installMarker, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, crashCount, partitionCount, diskLost,
            faultsStopped, lifecyclePhase, storageFaultInjected,
            PeerRecoveryVars>>

\* C3 fault: the process identity survives while its shard disk is destroyed.
DiskLoss(node) ==
    /\ ~faultsStopped
    /\ FaultMode \in {"C3", "G1", "G2"}
    /\ node \in Nodes
    /\ ~alive[node]
    /\ copyExists[node]
    /\ copyExists' = [copyExists EXCEPT ![node] = FALSE]
    /\ diskLost' = [diskLost EXCEPT ![node] = TRUE]
    /\ ops' = [ops EXCEPT ![node] = {}]
    /\ durableOps' = [durableOps EXCEPT ![node] = {}]
    /\ docValue' =
          [docValue EXCEPT ![node] = [d \in Docs |-> NoWrite]]
    /\ nextSeq' = [nextSeq EXCEPT ![node] = 0]
    /\ committed' = [committed EXCEPT ![node] = 0]
    /\ truncBelow' = [truncBelow EXCEPT ![node] = 0]
    /\ copyAllocation' = [copyAllocation EXCEPT ![node] = 0]
    /\ copyUuid' = [copyUuid EXCEPT ![node] = NoIndexUuid]
    /\ replicaFence' = [replicaFence EXCEPT ![node] = 0]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT ![node] = 0]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, pins, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, crashCount,
            partitionCount, faultsStopped, lifecyclePhase,
            storageFaultInjected, PeerRecoveryVars>>

\* node::reconciliation::open_local_assigned_shards and
\* ShardManager::open_assigned_shard_with_settings.  Without allocation IDs
\* this preserves the C3 implementation-faithful empty-store behavior.  With
\* allocation IDs, G1 permits a fresh empty copy only for the initial
\* CreateIndex allocation observed before first activation.  Later out-of-sync
\* assignments are populated only by InstallSnapshot.
OpenAssignedEmptyCopy(node) ==
    LET local == views[node]
        assigned ==
            /\ local.allocations[node] > 0
            /\ (node = local.primary \/ node \in local.replicas)
        initialCreateIndexAllocation ==
            /\ ~local.initialized
            /\ local.allocations[node] = 1
    IN
    /\ node \in Nodes
    /\ alive[node]
    /\ ~copyExists[node]
    /\ copyMode[node] \notin AllStorageFailureModes
    /\ IF AllocationIds
          THEN /\ assigned
               /\ initialCreateIndexAllocation
          ELSE node = routing.primary \/ node \in routing.replicas
    /\ copyExists' = [copyExists EXCEPT ![node] = TRUE]
    /\ copyAllocation' =
          [copyAllocation EXCEPT
              ![node] = IF AllocationIds THEN local.allocations[node] ELSE 0]
    /\ copyUuid' = [copyUuid EXCEPT ![node] = IndexUuid]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![node] = IF ReplicaFencing THEN local.term ELSE 0]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![node] =
                  IF ReplicaFencing /\ DurableReplicaFence
                  THEN local.term
                  ELSE 0]
    /\ copyMode' = [copyMode EXCEPT ![node] = "Active"]
    /\ installMarker' = [installMarker EXCEPT ![node] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, crashCount, partitionCount, diskLost,
            faultsStopped, lifecyclePhase, storageFaultInjected,
            PeerRecoveryVars>>

FaultTypeOK ==
    /\ crashCount \in 0..MaxCrashes
    /\ partitionCount \in 0..MaxPartitions
    /\ diskLost \in [Nodes -> BOOLEAN]
    /\ faultsStopped \in BOOLEAN
    /\ lifecyclePhase \in [Nodes -> LifecyclePhases]
    /\ storageFaultInjected \in BOOLEAN

FaultCoreNext ==
    \/ \E node \in Nodes : Crash(node)
    \/ \E node \in Nodes : Restart(node)
    \/ \E node \in Nodes : PartitionMetadata(node)
    \/ \E node \in Nodes : HealMetadata(node)
    \/ \E candidate \in Nodes : ElectLeader(candidate)
    \/ \E message \in messages : LoseMsg(message)
    \/ \E leader \in Nodes, node \in Nodes, candidate \in Nodes :
           SuspectAndRemove(leader, node, candidate)
    \/ \E node \in Nodes : ObserveRoutingAccepted(node)
    \/ \E node \in Nodes : ObserveRoutingRejected(node)
    \/ \E node \in Nodes : DeferRejectedRouting(node)
    \/ \E leader \in Nodes, node \in Nodes :
           ChangeRaftMembership(leader, node)
    \/ \E leader \in Nodes, node \in Nodes :
           ProposeRemoveNode(leader, node)
    \/ \E node \in Nodes : ObserveNodeRemoved(node)
    \/ \E node \in Nodes : Rejoin(node)
    \/ \E node \in Nodes : ObserveRejoin(node)
    \/ \E leader \in Nodes, target \in Nodes :
           AllocateAfterLifecycle(leader, target)
    \/ \E target \in Nodes : ObserveAllocationAccepted(target)
    \/ \E target \in Nodes : ObserveAllocationRejected(target)
    \/ \E node \in Nodes, candidate \in Nodes \cup {NoNode} :
           ReportShardCopyFailure(node, candidate)
    \/ \E node \in Nodes : CorruptShardStorage(node)
    \/ \E node \in Nodes : BeginPersistentStorageFailure(node)
    \/ \E node \in Nodes : BeginPersistentApplyFailure(node)
    \/ \E node \in Nodes : RedetectPersistentStorageFailure(node)
    \/ \E node \in Nodes : EscalatePersistentStorageFailure(node)
    \/ \E node \in Nodes : EscalatePersistentApplyFailure(node)
    \/ \E node \in Nodes : RepairPersistentStorageFault(node)
    \/ \E node \in Nodes : LifecycleProposeActivation(node)
    \/ \E node \in Nodes : Flush(node)
    \/ \E node \in Nodes : DiskLoss(node)
    \/ \E node \in Nodes : OpenAssignedEmptyCopy(node)

FaultNext ==
    /\ FaultCoreNext
    /\ UNCHANGED ApplySafetyVars

=============================================================================
