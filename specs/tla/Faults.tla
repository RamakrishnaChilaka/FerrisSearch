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
    lifecyclePhase

LifecyclePhases ==
    {"Idle", "RoutingProposed", "RoutingCommitted", "RoutingRejected",
     "MembershipRemoved", "RemoveProposed", "Removed",
     "RejoinProposed", "Rejoined", "AllocationProposed"}

FaultVars ==
    <<crashCount, partitionCount, diskLost, faultsStopped, lifecyclePhase>>

FaultInit ==
    /\ crashCount = 0
    /\ partitionCount = 0
    /\ diskLost = [n \in Nodes |-> FALSE]
    /\ faultsStopped = (FaultMode = "L1")
    /\ lifecyclePhase = [n \in Nodes |-> "Idle"]

WritesOwnedBy(node) ==
    {w \in WriteIds :
        /\ writePrimary[w] = node
        /\ writeStatus[w] \in {"Routed", "Replicating"}}

DropNodeMessages(node) ==
    {m \in messages : m.from = node \/ m.to = node}

\* src/node process lifecycle + src/wal/mod.rs::open.
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
    /\ CrashRecoveryState(node)
    /\ raftLeader' = IF raftLeader = node THEN NoNode ELSE raftLeader
    /\ crashCount' = crashCount + 1
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters,
            routing, epoch, nextWrite, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, durableOps, committed, truncBelow,
            copyExists, acked, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, diskLost, partitionCount,
            faultsStopped, lifecyclePhase>>

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
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            lifecyclePhase, PeerRecoveryVars>>

\* C2 only: metadata failure detector may suspect a live node whose local
\* ClusterManager view stops advancing.  Data-plane RPC messages remain usable.
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
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, crashCount,
            diskLost, faultsStopped, lifecyclePhase, PeerRecoveryVars>>

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
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe,             ackMembershipSafe, termMonotonic, crashCount, partitionCount,
            diskLost, faultsStopped, lifecyclePhase, PeerRecoveryVars>>

\* transport timeout/drop.  Delay is represented by simply not choosing a
\* delivery action.
LoseMsg(message) ==
    /\ ~faultsStopped
    /\ message \in messages
    /\ messages' = messages \ {message}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe,             ackMembershipSafe, termMonotonic, crashCount, partitionCount,
            diskLost, faultsStopped, lifecyclePhase, PeerRecoveryVars>>

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
            diskLost, faultsStopped, lifecyclePhase>>

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
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            PeerRecoveryVars>>

ObserveRoutingAccepted(node) ==
    /\ lifecyclePhase[node] = "RoutingProposed"
    /\ CommittedRoutingRemoval(node)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RoutingCommitted"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

ObserveRoutingRejected(node) ==
    /\ lifecyclePhase[node] = "RoutingProposed"
    /\ CommittedResult("UpdateRouting", node, FALSE)
    /\ lifecyclePhase' =
          [lifecyclePhase EXCEPT ![node] = "RoutingRejected"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

\* src/node/mod.rs::dead-node loop defers RemoveNode after a rejected
\* UpdateIndex and retries from a fresh view on a later lifecycle tick.
DeferRejectedRouting(node) ==
    /\ lifecyclePhase[node] = "RoutingRejected"
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Idle"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

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
            diskLost, faultsStopped>>

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
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            PeerRecoveryVars>>

ObserveNodeRemoved(node) ==
    /\ lifecyclePhase[node] = "RemoveProposed"
    /\ CommittedResult("RemoveNode", node, TRUE)
    /\ node \notin routing.members
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Removed"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

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
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            PeerRecoveryVars>>

ObserveRejoin(node) ==
    /\ lifecyclePhase[node] = "RejoinProposed"
    /\ CommittedResult("AddNode", node, TRUE)
    /\ node \in routing.members
    /\ node \in raftVoters
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![node] = "Rejoined"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

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
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped,
            PeerRecoveryVars>>

ObserveAllocationAccepted(target) ==
    /\ lifecyclePhase[target] = "AllocationProposed"
    /\ CommittedAllocation(target)
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![target] = "Idle"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

ObserveAllocationRejected(target) ==
    /\ lifecyclePhase[target] = "AllocationProposed"
    /\ CommittedResult("UpdateRouting", target, FALSE)
    /\ lifecyclePhase' = [lifecyclePhase EXCEPT ![target] = "Rejoined"]
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, faultsStopped>>

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
            copyExists, copyMode, installMarker, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic, crashCount, partitionCount, diskLost,
            faultsStopped, lifecyclePhase, PeerRecoveryVars>>

\* C3 fault: the process identity survives while its shard disk is destroyed.
DiskLoss(node) ==
    /\ ~faultsStopped
    /\ FaultMode = "C3"
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
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, pins, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, crashCount,
            partitionCount, faultsStopped, lifecyclePhase, PeerRecoveryVars>>

\* C3 abstraction of an assigned same-name node opening a newly empty local
\* copy without an allocation identity.  Recovery can subsequently replace it,
\* but metadata still considers an old in-sync assignment authoritative.
OpenAssignedEmptyCopy(node) ==
    /\ ~faultsStopped
    /\ FaultMode = "C3"
    /\ node \in Nodes
    /\ alive[node]
    /\ ~copyExists[node]
    /\ node = routing.primary \/ node \in routing.replicas
    /\ copyExists' = [copyExists EXCEPT ![node] = TRUE]
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
            faultsStopped, lifecyclePhase, PeerRecoveryVars>>

FaultTypeOK ==
    /\ crashCount \in 0..MaxCrashes
    /\ partitionCount \in 0..MaxPartitions
    /\ diskLost \in [Nodes -> BOOLEAN]
    /\ faultsStopped \in BOOLEAN
    /\ lifecyclePhase \in [Nodes -> LifecyclePhases]

FaultNext ==
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
    \/ \E node \in Nodes : Flush(node)
    \/ \E node \in Nodes : DiskLoss(node)
    \/ \E node \in Nodes : OpenAssignedEmptyCopy(node)

=============================================================================
