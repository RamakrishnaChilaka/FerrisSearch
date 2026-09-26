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
    faultsStopped

FaultVars == <<crashCount, partitionCount, diskLost, faultsStopped>>

FaultInit ==
    /\ crashCount = 0
    /\ partitionCount = 0
    /\ diskLost = [n \in Nodes |-> FALSE]
    /\ faultsStopped = (FaultMode = "L1")

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
    /\ exclusiveHolder' =
          [exclusiveHolder EXCEPT ![node] = NoNode]
    /\ ops' = [ops EXCEPT ![node] = survivingOps]
    /\ docValue' =
          [docValue EXCEPT ![node] = RebuiltDocValue(survivingOps)]
    /\ nextSeq' =
          [nextSeq EXCEPT ![node] = NextSequenceAfter(survivingOps)]
    /\ crashCount' = crashCount + 1
    /\ UNCHANGED
          <<RaftVars, routing, epoch, nextWrite, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, durableOps, committed, truncBelow, pins,
            copyExists, copyMode, installMarker, acked, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, diskLost,
            partitionCount, faultsStopped>>

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
            crashCount, partitionCount, diskLost, faultsStopped>>

\* C2 only: metadata failure detector may suspect a live node whose local
\* ClusterManager view stops advancing.  Data-plane RPC messages remain usable.
PartitionMetadata(node) ==
    /\ ~faultsStopped
    /\ FaultMode = "C2"
    /\ alive[node]
    /\ raftConnected[node]
    /\ partitionCount < MaxPartitions
    /\ raftConnected' = [raftConnected EXCEPT ![node] = FALSE]
    /\ partitionCount' = partitionCount + 1
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, activated, activationPending,
            nextWrite, writeStatus, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic, crashCount,
            diskLost, faultsStopped>>

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
            diskLost, faultsStopped>>

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
            diskLost, faultsStopped>>

FailureDetectorMayRemove(node) ==
    CASE FaultMode = "C2" -> ~alive[node] \/ ~raftConnected[node]
      [] OTHER -> ~alive[node]

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
        command ==
            RaftCommand("UpdateRouting", leader, node, NoNode, NoTerm,
                        chosenPrimary, proposedReplicas, proposedUnassigned)
    IN
    /\ ~faultsStopped
    /\ leader \in Nodes
    /\ node \in Nodes
    /\ candidate \in Nodes
    /\ leader # node
    /\ alive[leader]
    /\ raftConnected[leader]
    /\ FailureDetectorMayRemove(node)
    /\ node = local.primary \/ node \in local.replicas
    /\ IF losingPrimary
          THEN candidate \in availableCandidates
          ELSE candidate = local.primary
    /\ proposedUnassigned <= Cardinality(Nodes)
    /\ QueueRaft(command)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped>>

\* src/node/mod.rs only submits RemoveNode after the routing UpdateIndex
\* response succeeded.  The guard uses committed routing, not a stale view.
RemoveNodeAfterRouting(leader, node) ==
    LET command ==
            RaftCommand("RemoveNode", leader, node, NoNode, NoTerm, NoNode,
                        {}, 0)
    IN
    /\ ~faultsStopped
    /\ alive[leader]
    /\ raftConnected[leader]
    /\ node \in routing.members
    /\ node # routing.primary
    /\ node \notin routing.replicas
    /\ QueueRaft(command)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped>>

\* src/node/mod.rs follower JoinCluster retry.
Rejoin(node) ==
    LET command ==
            RaftCommand("AddNode", node, node, NoNode, NoTerm, NoNode, {}, 0)
    IN
    /\ ~faultsStopped
    /\ alive[node]
    /\ raftConnected[node]
    /\ node \notin routing.members
    /\ QueueRaft(command)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            crashCount, partitionCount, diskLost, faultsStopped>>

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
            faultsStopped>>

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
            partitionCount, faultsStopped>>

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
            faultsStopped>>

FaultTypeOK ==
    /\ crashCount \in 0..MaxCrashes
    /\ partitionCount \in 0..MaxPartitions
    /\ diskLost \in [Nodes -> BOOLEAN]
    /\ faultsStopped \in BOOLEAN

FaultNext ==
    \/ \E node \in Nodes : Crash(node)
    \/ \E node \in Nodes : Restart(node)
    \/ \E node \in Nodes : PartitionMetadata(node)
    \/ \E node \in Nodes : HealMetadata(node)
    \/ \E message \in messages : LoseMsg(message)
    \/ \E leader \in Nodes, node \in Nodes, candidate \in Nodes :
           SuspectAndRemove(leader, node, candidate)
    \/ \E leader \in Nodes, node \in Nodes :
           RemoveNodeAfterRouting(leader, node)
    \/ \E node \in Nodes : Rejoin(node)
    \/ \E node \in Nodes : Flush(node)
    \/ \E node \in Nodes : DiskLoss(node)
    \/ \E node \in Nodes : OpenAssignedEmptyCopy(node)

=============================================================================
