-------------------------- MODULE ShardReplication --------------------------
\* One FerrisSearch local_shards shard.  Most configurations begin immediately
\* after the initial primary has activated at normalized term 1.  G1
\* configurations begin at CreateIndex with initialized = FALSE and exercise
\* the first activation explicitly.  Promotion and activation preserve the
\* implemented relative term ordering.

EXTENDS RaftLog

CONSTANTS
    Docs,
    MaxWrites,
    MaxCrashes,
    MaxPartitions,
    MaxTerm,
    MaxMessages,
    MaxViewLag,
    MaxAllocationId,
    FaultMode,
    InitialOutOfSync,
    InitialInitialized,
    EnableRecovery,
    AllocationIds,
    ReplicaFencing,
    DurableReplicaFence,
    AllowedWriteKinds,
    EnableRecoveryFailures

WriteIds == 1..MaxWrites
NoWrite == 0
DefaultDoc == CHOOSE d \in Docs : TRUE

WriteStatuses == {"Unused", "Routed", "Replicating", "Acked", "Failed"}
AllWriteKinds == {"Put", "Delete"}
WriteKinds == AllowedWriteKinds
CopyModes ==
    {"Active", "Recovering", "Pending", "InstallMarker",
     "StorageCorrupt", "StorageFailing", "StorageRetrying", "StorageFailed",
     "ApplyFailing", "ApplyRetrying", "ApplyFailed"}
ApplyFailureModes == {"ApplyFailing", "ApplyRetrying", "ApplyFailed"}
ReportableOpenStorageFailureModes == {"StorageCorrupt", "StorageFailed"}
StorageRetryModes ==
    {"StorageFailing", "StorageRetrying", "ApplyFailing", "ApplyRetrying"}
ReportableStorageFailureModes ==
    ReportableOpenStorageFailureModes \cup {"ApplyFailed"}
AllStorageFailureModes ==
    StorageRetryModes \cup ReportableStorageFailureModes
MessageKinds == {"Replicate", "ReplicaAck", "ReplicaNack"}
IndexUuid == "INDEX_UUID"
NoIndexUuid == "NO_INDEX_UUID"

Event(eventName, actorNode, targetNode, writeId, sequenceNumber,
      primaryTerm, eventDetail) ==
    [name   |-> eventName,
     actor  |-> actorNode,
     target |-> targetNode,
     write  |-> writeId,
     seq    |-> sequenceNumber,
     term   |-> primaryTerm,
     detail |-> eventDetail]

Message(messageKind, writeId, sourceNode, targetNode, sequenceNumber,
        sourceEpoch, targetEpoch, primaryTerm, messageIndexUuid,
        targetAllocationId) ==
    [kind      |-> messageKind,
     write     |-> writeId,
     from      |-> sourceNode,
     to        |-> targetNode,
     seq       |-> sequenceNumber,
     fromEpoch |-> sourceEpoch,
     toEpoch   |-> targetEpoch,
     term      |-> primaryTerm,
     indexUuid |-> messageIndexUuid,
     targetAllocation |-> targetAllocationId]

VARIABLES
    routing,
    alive,
    epoch,
    raftConnected,
    activated,
    activationPending,
    nextWrite,
    writeStatus,
    writeDoc,
    writeKind,
    writeTarget,
    writePrimary,
    writeEpoch,
    writeSeq,
    writeTerm,
    writeRequired,
    writeWait,
    ops,
    durableOps,
    docValue,
    nextSeq,
    committed,
    truncBelow,
    pins,
    copyExists,
    copyAllocation,
    copyUuid,
    replicaFence,
    durableReplicaFence,
    copyMode,
    installMarker,
    messages,
    sharedHolders,
    exclusiveHolder,
    acked,
    failed,
    promotionSafe,
    admissionSafe,
    ackMembershipSafe,
    staleApplySafe,
    activePrimaryApplySafe,
    termMonotonic

ReplicationVars ==
    <<routing, alive, epoch, raftConnected, activated, activationPending,
      nextWrite, writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
      writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
      durableOps, docValue, nextSeq, committed, truncBelow, pins, copyExists,
      copyAllocation, copyUuid, replicaFence, durableReplicaFence,
      copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
      acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
      staleApplySafe, activePrimaryApplySafe, termMonotonic>>

ApplySafetyVars == <<staleApplySafe, activePrimaryApplySafe>>

RoutingWellFormedValue(r) ==
    /\ r.primary \in Nodes
    /\ r.term \in 1..MaxTerm
    /\ r.replicas \subseteq Nodes \ {r.primary}
    /\ r.inSync \subseteq r.replicas
    /\ r.unassigned \in 0..Cardinality(Nodes)
    /\ r.members \subseteq Nodes
    /\ r.allocations \in [Nodes -> 0..MaxAllocationId]
    /\ r.initialized \in BOOLEAN
    /\ ~r.initialized => r.inSync = {}
    \* A cleared primary allocation represents a red shard.  No in-sync
    \* promotion candidate may remain in that state.
    /\ r.allocations[r.primary] = 0 => r.inSync = {}
    /\ \A node \in Nodes :
           IF node = r.primary
           THEN node \notin r.replicas
           ELSE (node \in r.replicas) <=> r.allocations[node] > 0

LatestWriteForDoc(writeSet, doc) ==
    LET candidates == {w \in writeSet : writeDoc[w] = doc}
    IN IF candidates = {}
       THEN NoWrite
       ELSE CHOOSE w \in candidates :
                \A other \in candidates :
                    \/ writeSeq[other] < writeSeq[w]
                    \/ /\ writeSeq[other] = writeSeq[w]
                       /\ other <= w

RebuiltDocValue(writeSet) ==
    [d \in Docs |-> LatestWriteForDoc(writeSet, d)]

NextSequenceAfter(writeSet) ==
    IF writeSet = {}
    THEN 0
    ELSE 1 + (CHOOSE s \in {writeSeq[w] : w \in writeSet} :
                  \A other \in {writeSeq[w] : w \in writeSet} : other <= s)

MinNatSet(values) ==
    CHOOSE value \in values : \A other \in values : value <= other

AllAckedOn(node) ==
    \A w \in acked : w \in ops[node]

ActiveWrites ==
    {w \in WriteIds :
        writeStatus[w] \in {"Routed", "Replicating"}}

WriteMessages(writeId) ==
    {m \in messages : m.write = writeId}

BlocksLiveReplication(node) ==
    copyMode[node] \in
        {"Recovering", "InstallMarker"} \cup AllStorageFailureModes

ApplyMutationFails(node) ==
    copyMode[node] \in ApplyFailureModes

CopyAssignmentValid(node) ==
    \/ ~AllocationIds
    \/ /\ copyAllocation[node] > 0
       /\ copyAllocation[node] = views[node].allocations[node]

ReplicaMessageValid(message) ==
    LET replica == message.to
        observedTerm ==
            IF views[replica].term > replicaFence[replica]
            THEN views[replica].term
            ELSE replicaFence[replica]
    IN
    /\ message.indexUuid = copyUuid[replica]
    /\ message.indexUuid = IndexUuid
    /\ views[replica].initialized
    /\ IF AllocationIds
          THEN /\ message.targetAllocation > 0
               /\ message.targetAllocation = copyAllocation[replica]
               /\ message.targetAllocation =
                  views[replica].allocations[replica]
          ELSE TRUE
    /\ IF ReplicaFencing
          THEN message.term >= observedTerm
          ELSE TRUE

LiveConnectedVoters ==
    {node \in raftVoters : alive[node] /\ raftConnected[node]}

RaftQuorumSize ==
    (Cardinality(raftVoters) \div 2) + 1

HasRaftQuorum ==
    Cardinality(LiveConnectedVoters) >= RaftQuorumSize

LeaderCanCommit ==
    /\ raftLeader \in LiveConnectedVoters
    /\ HasRaftQuorum

CanReachRaft(node) ==
    /\ node \in Nodes
    /\ alive[node]
    /\ raftConnected[node]
    /\ LeaderCanCommit

ReplicationInit ==
    \E initialPrimary \in Nodes :
      \E initialInSync \in SUBSET (Nodes \ {initialPrimary}) :
        \E initialLeader \in Nodes :
          LET initialReplicas == Nodes \ {initialPrimary}
              initialAllocations == [node \in Nodes |-> 1]
              initialRouting ==
                  RoutingState(initialPrimary, 1, initialReplicas,
                               initialInSync, 0, Nodes, initialAllocations,
                               InitialInitialized)
          IN
          /\ IF InitialInitialized
                THEN IF InitialOutOfSync
                     THEN Cardinality(initialInSync) =
                          Cardinality(initialReplicas) - 1
                     ELSE initialInSync = initialReplicas
                ELSE initialInSync = {}
          /\ routing = initialRouting
          /\ RaftInit(initialRouting, initialLeader)
          /\ alive = [n \in Nodes |-> TRUE]
          /\ epoch = [n \in Nodes |-> 0]
          /\ raftConnected = [n \in Nodes |-> TRUE]
          /\ activated =
                [n \in Nodes |->
                    IF InitialInitialized /\ n = initialPrimary
                    THEN 1
                    ELSE NoTerm]
          /\ activationPending = [n \in Nodes |-> NoTerm]
          /\ nextWrite = 1
          /\ writeStatus = [w \in WriteIds |-> "Unused"]
          /\ writeDoc = [w \in WriteIds |-> DefaultDoc]
          /\ writeKind = [w \in WriteIds |-> "Put"]
          /\ writeTarget = [w \in WriteIds |-> NoNode]
          /\ writePrimary = [w \in WriteIds |-> NoNode]
          /\ writeEpoch = [w \in WriteIds |-> 0]
          /\ writeSeq = [w \in WriteIds |-> 0]
          /\ writeTerm = [w \in WriteIds |-> NoTerm]
          /\ writeRequired = [w \in WriteIds |-> {}]
          /\ writeWait = [w \in WriteIds |-> {}]
          /\ ops = [n \in Nodes |-> {}]
          /\ durableOps = [n \in Nodes |-> {}]
          /\ docValue = [n \in Nodes |-> [d \in Docs |-> NoWrite]]
          /\ nextSeq = [n \in Nodes |-> 0]
          /\ committed = [n \in Nodes |-> 0]
          /\ truncBelow = [n \in Nodes |-> 0]
          /\ pins = [n \in Nodes |-> {}]
          /\ copyExists = [n \in Nodes |-> InitialInitialized]
          /\ copyAllocation =
                [n \in Nodes |->
                    IF InitialInitialized
                    THEN initialRouting.allocations[n]
                    ELSE 0]
          /\ copyUuid =
                [n \in Nodes |->
                    IF InitialInitialized THEN IndexUuid ELSE NoIndexUuid]
          /\ replicaFence =
                [n \in Nodes |->
                    IF InitialInitialized /\ ReplicaFencing THEN 1 ELSE 0]
          /\ durableReplicaFence =
                [n \in Nodes |->
                    IF InitialInitialized
                       /\ ReplicaFencing
                       /\ DurableReplicaFence
                    THEN 1
                    ELSE 0]
          /\ copyMode = [n \in Nodes |-> "Active"]
          /\ installMarker = [n \in Nodes |-> FALSE]
          /\ messages = {}
          /\ sharedHolders = [n \in Nodes |-> {}]
          /\ exclusiveHolder = [n \in Nodes |-> NoNode]
          /\ acked = {}
          /\ failed = {}
          /\ promotionSafe = TRUE
          /\ admissionSafe = TRUE
          /\ ackMembershipSafe = TRUE
          /\ staleApplySafe = TRUE
          /\ activePrimaryApplySafe = TRUE
          /\ termMonotonic = TRUE

\* src/api/index document and bulk handlers resolve the coordinator's local
\* routing view and forward to that view's primary.
ClientWrite(coordinator, doc, kind) ==
    LET writeId == nextWrite
        target == views[coordinator].primary
    IN
    /\ writeId \in WriteIds
    /\ coordinator \in Nodes
    /\ alive[coordinator]
    /\ doc \in Docs
    /\ kind \in WriteKinds
    \* One client operation may be in flight.  Writes still interleave with
    \* every Raft, recovery, network, and fault action.
    /\ ActiveWrites = {}
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Routed"]
    /\ writeDoc' = [writeDoc EXCEPT ![writeId] = doc]
    /\ writeKind' = [writeKind EXCEPT ![writeId] = kind]
    /\ writeTarget' = [writeTarget EXCEPT ![writeId] = target]
    /\ nextWrite' = nextWrite + 1
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic>>

CanPrimaryReachMutation(writeId) ==
    LET primaryNode == writeTarget[writeId]
    IN
    /\ writeStatus[writeId] = "Routed"
    /\ primaryNode \in Nodes
    /\ alive[primaryNode]
    /\ copyExists[primaryNode]
    /\ copyUuid[primaryNode] = IndexUuid
    /\ CopyAssignmentValid(primaryNode)
    /\ views[primaryNode].initialized
    /\ views[primaryNode].allocations[primaryNode] > 0
    /\ IF ReplicaFencing
          THEN replicaFence[primaryNode] >= views[primaryNode].term
          ELSE TRUE
    /\ views[primaryNode].primary = primaryNode
    /\ activated[primaryNode] = views[primaryNode].term
    /\ exclusiveHolder[primaryNode] = NoNode
    /\ nextSeq[primaryNode] < MaxWrites

CanPrimaryAccept(writeId) ==
    LET primaryNode == writeTarget[writeId]
    IN
    /\ CanPrimaryReachMutation(writeId)
    /\ ~ApplyMutationFails(primaryNode)

\* src/transport/server/mod.rs::{index_doc,bulk_index,delete_doc}
\* ensure_primary_activated + peer_recovery_write_guard +
\* validated_primary_write_state are represented by the guards below.
PrimaryAccept(writeId) ==
    LET primaryNode == writeTarget[writeId]
        sequenceNumber == nextSeq[primaryNode]
        requiredReplicas == views[primaryNode].inSync
        requests ==
            {Message("Replicate", writeId, primaryNode, replica,
                     sequenceNumber, epoch[primaryNode], epoch[replica],
                     views[primaryNode].term, IndexUuid,
                     views[primaryNode].allocations[replica]) :
                replica \in requiredReplicas}
    IN
    /\ CanPrimaryAccept(writeId)
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Replicating"]
    /\ writePrimary' = [writePrimary EXCEPT ![writeId] = primaryNode]
    /\ writeEpoch' = [writeEpoch EXCEPT ![writeId] = epoch[primaryNode]]
    /\ writeSeq' = [writeSeq EXCEPT ![writeId] = sequenceNumber]
    /\ writeTerm' =
          [writeTerm EXCEPT ![writeId] = views[primaryNode].term]
    /\ writeRequired' =
          [writeRequired EXCEPT ![writeId] = requiredReplicas]
    /\ writeWait' = [writeWait EXCEPT ![writeId] = requiredReplicas]
    /\ ops' = [ops EXCEPT ![primaryNode] = @ \cup {writeId}]
    /\ durableOps' =
          IF FaultMode = "C4"
          THEN durableOps
          ELSE [durableOps EXCEPT ![primaryNode] = @ \cup {writeId}]
    /\ docValue' =
          [docValue EXCEPT ![primaryNode][writeDoc[writeId]] = writeId]
    /\ nextSeq' = [nextSeq EXCEPT ![primaryNode] = @ + 1]
    /\ messages' = messages \cup requests
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![primaryNode] = @ \cup {writeId}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic>>

\* Failure return from TransportService::{index_doc,bulk_index,delete_doc}
\* when activation or the in-guard routing/UUID/term revalidation fails.
PrimaryReject(writeId) ==
    /\ writeId \in WriteIds
    /\ writeStatus[writeId] = "Routed"
    /\ ~CanPrimaryReachMutation(writeId)
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Failed"]
    /\ failed' = failed \cup {writeId}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic>>

\* TransportService::{index_doc,bulk_index,delete_doc} after authority
\* validation, plus ShardManager::record_local_apply_result, when the local
\* primary's WAL/fsync/engine mutation returns persistent local-storage I/O.
\* The first observed failure enters the shared retry state; no operation or
\* replication message is published.
PrimaryApplyFailure(writeId) ==
    LET primaryNode == writeTarget[writeId]
    IN
    /\ writeId \in WriteIds
    /\ CanPrimaryReachMutation(writeId)
    /\ ApplyMutationFails(primaryNode)
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Failed"]
    /\ failed' = failed \cup {writeId}
    /\ copyMode' =
          [copyMode EXCEPT
              ![primaryNode] =
                  IF @ = "ApplyFailing" THEN "ApplyRetrying" ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic>>

\* src/transport/server/mod.rs::{replicate_doc,replicate_bulk}.  With
\* ReplicaFencing = FALSE this preserves the merged no-term-check behavior.
\* The proposed variant validates UUID, target allocation, and primary term
\* before the WAL/engine mutation. Pending copies still accept valid live writes.
ReplicaApply(message) ==
    LET replica == message.to
        writeId == message.write
        response ==
            Message("ReplicaAck", writeId, replica, message.from, message.seq,
                    epoch[replica], message.fromEpoch, message.term,
                    message.indexUuid, message.targetAllocation)
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ alive[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ~BlocksLiveReplication(replica)
    /\ ReplicaMessageValid(message)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ ops' = [ops EXCEPT ![replica] = @ \cup {writeId}]
    /\ durableOps' =
          IF FaultMode = "C4"
          THEN durableOps
          ELSE [durableOps EXCEPT ![replica] = @ \cup {writeId}]
    /\ docValue' =
          [docValue EXCEPT ![replica][writeDoc[writeId]] = writeId]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![replica] =
                  IF @ < message.seq + 1 THEN message.seq + 1 ELSE @]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ DurableReplicaFence /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ staleApplySafe' =
          (staleApplySafe
           /\ message.term >= durableReplicaFence[replica])
    /\ activePrimaryApplySafe' =
          (activePrimaryApplySafe
           /\ (activated[replica] = NoTerm
               \/ message.term >= activated[replica]))
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked,
            failed, promotionSafe, admissionSafe, ackMembershipSafe,
            termMonotonic>>

\* ShardManager::{apply_replica_operation,record_local_apply_result} after
\* UUID/allocation/term validation, when WAL/fsync/engine mutation fails with
\* persistent local-storage I/O. Validation may already have durably raised
\* the replica fence, but the operation itself is not applied and the primary
\* receives a NACK.
ReplicaApplyFailure(message) ==
    LET replica == message.to
        response ==
            Message("ReplicaNack", message.write, replica, message.from,
                    message.seq, epoch[replica], message.fromEpoch,
                    message.term, message.indexUuid,
                    message.targetAllocation)
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ alive[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ApplyMutationFails(replica)
    /\ ReplicaMessageValid(message)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ copyMode' =
          [copyMode EXCEPT
              ![replica] =
                  IF @ = "ApplyFailing" THEN "ApplyRetrying" ELSE @]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ DurableReplicaFence /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation,
            copyUuid, installMarker, sharedHolders, exclusiveHolder, acked,
            failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic>>

\* Proposed transport/server replica fencing for
\* TransportService::{replicate_doc,replicate_bulk}: reject UUID, allocation,
\* recovery-gate, or stale-primary-term mismatches before WAL/engine mutation.
ReplicaReject(message) ==
    LET replica == message.to
        response ==
            Message("ReplicaNack", message.write, replica, message.from,
                    message.seq, epoch[replica], message.fromEpoch,
                    message.term, message.indexUuid,
                    message.targetAllocation)
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ alive[replica]
    /\ copyExists[replica]
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ \/ /\ BlocksLiveReplication(replica)
          /\ ~ApplyMutationFails(replica)
       \/ ~ReplicaMessageValid(message)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation,
            copyUuid, replicaFence, durableReplicaFence, copyMode,
            installMarker, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic>>

\* Completion of replication::{replicate_write,replicate_bulk}'s concurrent
\* TransportClient RPC and collection of the replica checkpoint.
DeliverReplicaAck(message) ==
    LET writeId == message.write
        primaryNode == message.to
    IN
    /\ message \in messages
    /\ message.kind = "ReplicaAck"
    /\ writeStatus[writeId] = "Replicating"
    /\ alive[primaryNode]
    /\ epoch[primaryNode] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ messages' = messages \ {message}
    /\ writeWait' =
          [writeWait EXCEPT ![writeId] = @ \ {message.from}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic>>

\* src/transport/server/mod.rs returns success only after every in-sync target
\* from the validated routing snapshot has acknowledged.
PrimaryAck(writeId) ==
    LET primaryNode == writePrimary[writeId]
    IN
    /\ writeId \in WriteIds
    /\ writeStatus[writeId] = "Replicating"
    /\ writeWait[writeId] = {}
    /\ alive[primaryNode]
    /\ epoch[primaryNode] = writeEpoch[writeId]
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Acked"]
    /\ acked' = acked \cup {writeId}
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![primaryNode] = @ \ {writeId}]
    /\ ackMembershipSafe' =
          ackMembershipSafe
          /\ routing.inSync \subseteq writeRequired[writeId]
          /\ \A replica \in routing.inSync : writeId \in ops[replica]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            exclusiveHolder, failed, promotionSafe, admissionSafe,
            termMonotonic>>

\* TransportService::{index_doc,bulk_index,delete_doc} returns a request
\* failure when any required synchronous replica did not acknowledge.
PrimaryFail(writeId) ==
    LET primaryNode == writePrimary[writeId]
    IN
    /\ writeId \in WriteIds
    /\ writeStatus[writeId] = "Replicating"
    /\ writeWait[writeId] # {}
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Failed"]
    /\ failed' = failed \cup {writeId}
    /\ messages' = messages \ WriteMessages(writeId)
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![primaryNode] = @ \ {writeId}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker,
            exclusiveHolder, acked, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic>>

\* replication::{replicate_write,replicate_bulk} reports a rejected replica
\* response as a request failure; partial replication is never success-shaped.
DeliverReplicaNack(message) ==
    LET writeId == message.write
        primaryNode == message.to
    IN
    /\ message \in messages
    /\ message.kind = "ReplicaNack"
    /\ writeStatus[writeId] = "Replicating"
    /\ alive[primaryNode]
    /\ epoch[primaryNode] = message.toEpoch
    /\ messages' = messages \ WriteMessages(writeId)
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Failed"]
    /\ failed' = failed \cup {writeId}
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![primaryNode] = @ \ {writeId}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            exclusiveHolder, acked, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic>>

ActivateCommand(primaryNode, expectedPrimaryTerm, expectedAllocationId) ==
    RaftCommand("ActivatePrimary", primaryNode, primaryNode, primaryNode,
                expectedPrimaryTerm, primaryNode, {}, 0,
                expectedAllocationId,
                EmptyAllocations)

\* src/transport/server/mod.rs::ensure_primary_activated
ProposeActivate(primaryNode) ==
    LET local == views[primaryNode]
        command ==
            ActivateCommand(primaryNode, local.term,
                            local.allocations[primaryNode])
    IN
    /\ primaryNode \in Nodes
    /\ CanReachRaft(primaryNode)
    /\ local.primary = primaryNode
    /\ local.allocations[primaryNode] > 0
    /\ copyExists[primaryNode]
    /\ copyUuid[primaryNode] = IndexUuid
    /\ CopyAssignmentValid(primaryNode)
    /\ local.term < MaxTerm
    /\ activated[primaryNode] # local.term
    /\ activationPending[primaryNode] = NoTerm
    /\ QueueRaft(command)
    /\ activationPending' =
          [activationPending EXCEPT ![primaryNode] = local.term]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
            durableOps, docValue, nextSeq, committed, truncBelow, pins,
            copyExists, copyMode, installMarker, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic>>

\* TransportService::ensure_primary_activated observes its local
\* ClusterManager view after the conditional ActivatePrimary command.
ObserveActivation(primaryNode) ==
    LET expected == activationPending[primaryNode]
        local == views[primaryNode]
    IN
    /\ expected # NoTerm
    /\ local.primary = primaryNode
    /\ local.term > expected
    /\ local.initialized
    /\ local.allocations[primaryNode] = copyAllocation[primaryNode]
    /\ activated' = [activated EXCEPT ![primaryNode] = local.term]
    /\ activationPending' =
          [activationPending EXCEPT ![primaryNode] = NoTerm]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![primaryNode] =
                  IF ReplicaFencing /\ @ < local.term THEN local.term ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![primaryNode] =
                  IF ReplicaFencing /\ DurableReplicaFence /\ @ < local.term
                  THEN local.term
                  ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
            durableOps, docValue, nextSeq, committed, truncBelow, pins,
            copyExists, copyAllocation, copyUuid, copyMode, installMarker,
            messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic>>

\* TransportService::ensure_primary_activated aborts when a newer term or
\* different primary makes the requested activation impossible.
CancelActivation(primaryNode) ==
    LET expected == activationPending[primaryNode]
        local == views[primaryNode]
    IN
    /\ expected # NoTerm
    /\ \/ local.primary # primaryNode
       \/ local.term > expected + 1
       \/ local.allocations[primaryNode] # copyAllocation[primaryNode]
    /\ activationPending' =
          [activationPending EXCEPT ![primaryNode] = NoTerm]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            nextWrite, writeStatus, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, termMonotonic>>

UpdateRoutingAccepted(current, command) ==
    LET nextInSync ==
            (current.inSync \cap command.newReplicas) \ {command.newPrimary}
        nextTerm ==
            IF command.newPrimary # current.primary
            THEN current.term + 1
            ELSE current.term
        proposed ==
            RoutingState(command.newPrimary, nextTerm, command.newReplicas,
                         nextInSync, command.newUnassigned, current.members,
                         command.newAllocations, current.initialized)
    IN
    /\ command.newPrimary \in Nodes
    /\ command.newReplicas \subseteq Nodes
    /\ command.newUnassigned \in 0..Cardinality(Nodes)
    /\ command.newAllocations[command.newPrimary] > 0
    /\ IF command.newPrimary # current.primary
          THEN /\ command.newPrimary \in current.inSync
               /\ current.term < MaxTerm
          ELSE TRUE
    /\ RoutingWellFormedValue(proposed)

SurvivingInSync(current, failedNode) ==
    current.inSync \ {failedNode}

\* ClusterStateMachine::apply_command for ClusterCommand::FailShardCopy.
\* For a failed primary, command.newPrimary carries the leader-selected live,
\* highest-checkpoint candidate.  The state machine deliberately validates
\* only current authoritative in-sync membership and the allocation CAS.
FailShardCopyAccepted(current, command) ==
    /\ AllocationIds
    /\ current.initialized
    /\ command.target \in Nodes
    /\ \/ command.target = current.primary
       \/ command.target \in current.replicas
    /\ command.expectedAllocation > 0
    /\ command.expectedAllocation =
       current.allocations[command.target]
    /\ current.unassigned < Cardinality(Nodes)
    /\ IF command.target = current.primary
       THEN /\ command.newPrimary
               \in SurvivingInSync(current, command.target)
            /\ current.term < MaxTerm
       ELSE command.newPrimary = NoNode

AfterFailShardCopy(current, command) ==
    LET failedNode == command.target
        failedPrimary == failedNode = current.primary
        survivors == SurvivingInSync(current, failedNode)
        canPromote ==
            /\ failedPrimary
            /\ command.newPrimary \in survivors
        nextPrimary ==
            IF canPromote
            THEN command.newPrimary
            ELSE current.primary
        nextReplicas ==
            IF canPromote
            THEN current.replicas \ {nextPrimary}
            ELSE IF failedPrimary
                 THEN current.replicas
                 ELSE current.replicas \ {failedNode}
        nextInSync ==
            IF canPromote
            THEN survivors \ {nextPrimary}
            ELSE current.inSync \ {failedNode}
        nextTerm == IF canPromote THEN current.term + 1 ELSE current.term
        nextAllocations ==
            [current.allocations EXCEPT ![failedNode] = 0]
    IN RoutingState(nextPrimary, nextTerm, nextReplicas, nextInSync,
                    current.unassigned + 1, current.members,
                    nextAllocations, current.initialized)

CommandAccepted(current, command) ==
    CASE command.kind = "ActivatePrimary" ->
            /\ command.target = current.primary
            /\ command.expectedPrimary = current.primary
            /\ command.expectedTerm = current.term
            /\ current.allocations[current.primary] > 0
            /\ IF AllocationIds
                  THEN /\ command.expectedAllocation > 0
                       /\ command.expectedAllocation =
                          current.allocations[current.primary]
                  ELSE TRUE
            /\ current.term < MaxTerm
      [] command.kind = "UpdateRouting" ->
            UpdateRoutingAccepted(current, command)
      [] command.kind = "MarkReplicaInSync" ->
            /\ current.initialized
            /\ current.allocations[current.primary] > 0
            /\ command.target \in current.replicas
            /\ command.target \notin current.inSync
            /\ command.expectedPrimary = current.primary
            /\ command.expectedTerm = current.term
            /\ IF AllocationIds
                  THEN /\ command.expectedAllocation > 0
                       /\ command.expectedAllocation =
                          current.allocations[command.target]
                  ELSE TRUE
      [] command.kind = "FailShardCopy" ->
            FailShardCopyAccepted(current, command)
      [] command.kind = "RemoveNode" -> command.target \in current.members
      [] command.kind = "AddNode" ->
            /\ command.target \in Nodes
            /\ command.target \notin current.members
      [] OTHER -> FALSE

AfterAcceptedCommand(current, command) ==
    CASE command.kind = "ActivatePrimary" ->
            RoutingState(current.primary, current.term + 1, current.replicas,
                         current.inSync, current.unassigned, current.members,
                         current.allocations, TRUE)
      [] command.kind = "UpdateRouting" ->
            LET nextTerm ==
                    IF command.newPrimary # current.primary
                    THEN current.term + 1
                    ELSE current.term
                nextInSync ==
                    (current.inSync \cap command.newReplicas)
                    \ {command.newPrimary}
            IN RoutingState(command.newPrimary, nextTerm,
                            command.newReplicas, nextInSync,
                            command.newUnassigned, current.members,
                            command.newAllocations, current.initialized)
      [] command.kind = "MarkReplicaInSync" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync \cup {command.target},
                         current.unassigned, current.members,
                         current.allocations, current.initialized)
      [] command.kind = "FailShardCopy" ->
            AfterFailShardCopy(current, command)
      [] command.kind = "RemoveNode" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync, current.unassigned,
                         current.members \ {command.target},
                         current.allocations, current.initialized)
      [] command.kind = "AddNode" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync, current.unassigned,
                         current.members \cup {command.target},
                         current.allocations, current.initialized)
      [] OTHER -> current

AfterCommand(current, command) ==
    IF CommandAccepted(current, command)
    THEN AfterAcceptedCommand(current, command)
    ELSE current

\* src/consensus/state_machine.rs::apply_command.  A rejected conditional
\* command still occupies a committed log position with unchanged state.
CommitRaft(command) ==
    LET accepted == CommandAccepted(routing, command)
        after == AfterCommand(routing, command)
        promoted ==
            /\ accepted
            /\ after.primary # routing.primary
        admitted ==
            /\ accepted
            /\ command.kind = "MarkReplicaInSync"
    IN
    /\ command \in pendingRaft
    /\ Len(raftLog) < MaxRaftEntries
    /\ LeaderCanCommit
    /\ raftLog' = Append(raftLog, RaftEntry(command, accepted, after))
    /\ pendingRaft' = pendingRaft \ {command}
    /\ routing' = after
    /\ applied' =
          [applied EXCEPT ![raftLeader] = Len(raftLog) + 1]
    /\ views' = [views EXCEPT ![raftLeader] = after]
    /\ raftVoters' =
          IF accepted /\ command.kind = "AddNode"
          THEN raftVoters \cup {command.target}
          ELSE raftVoters
    /\ UNCHANGED raftLeader
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![raftLeader] =
                  IF ReplicaFencing
                     /\ after.primary = raftLeader
                     /\ after.allocations[raftLeader] > 0
                     /\ @ < after.term
                  THEN after.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![raftLeader] =
                  IF ReplicaFencing
                     /\ DurableReplicaFence
                     /\ after.primary = raftLeader
                     /\ after.allocations[raftLeader] > 0
                     /\ @ < after.term
                  THEN after.term
                  ELSE @]
    /\ promotionSafe' =
          promotionSafe
          /\ IF promoted THEN AllAckedOn(after.primary) ELSE TRUE
    /\ admissionSafe' =
          admissionSafe
          /\ IF admitted THEN AllAckedOn(command.target) ELSE TRUE
    /\ termMonotonic' = termMonotonic /\ after.term >= routing.term
    /\ UNCHANGED
          <<alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            ackMembershipSafe, ApplySafetyVars>>

\* ClusterManager applies one more committed Raft entry on this node.
\* ClusterManager's state-machine-backed local view advances after a committed
\* openraft log entry is applied on this node.
DeliverView(node) ==
    LET nextView == raftLog[applied[node] + 1].state
    IN
    /\ node \in Nodes
    /\ alive[node]
    /\ raftConnected[node]
    /\ DeliverRaftView(node)
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![node] =
                  IF ReplicaFencing
                     /\ nextView.primary = node
                     /\ nextView.allocations[node] > 0
                     /\ @ < nextView.term
                  THEN nextView.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![node] =
                  IF ReplicaFencing
                     /\ DurableReplicaFence
                     /\ nextView.primary = node
                     /\ nextView.allocations[node] > 0
                     /\ @ < nextView.term
                  THEN nextView.term
                  ELSE @]
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic>>

ReplicationTypeOK ==
    /\ WriteKinds # {}
    /\ WriteKinds \subseteq AllWriteKinds
    /\ InitialInitialized \in BOOLEAN
    /\ routing \in RoutingType
    /\ RoutingWellFormedValue(routing)
    /\ alive \in [Nodes -> BOOLEAN]
    /\ epoch \in [Nodes -> Nat]
    /\ raftConnected \in [Nodes -> BOOLEAN]
    /\ activated \in [Nodes -> 0..MaxTerm]
    /\ activationPending \in [Nodes -> 0..MaxTerm]
    /\ nextWrite \in 1..(MaxWrites + 1)
    /\ writeStatus \in [WriteIds -> WriteStatuses]
    /\ writeDoc \in [WriteIds -> Docs]
    /\ writeKind \in [WriteIds -> WriteKinds]
    /\ writeTarget \in [WriteIds -> Nodes \cup {NoNode}]
    /\ writePrimary \in [WriteIds -> Nodes \cup {NoNode}]
    /\ writeEpoch \in [WriteIds -> Nat]
    /\ writeSeq \in [WriteIds -> 0..MaxWrites]
    /\ writeTerm \in [WriteIds -> 0..MaxTerm]
    /\ writeRequired \in [WriteIds -> SUBSET Nodes]
    /\ writeWait \in [WriteIds -> SUBSET Nodes]
    /\ ops \in [Nodes -> SUBSET WriteIds]
    /\ durableOps \in [Nodes -> SUBSET WriteIds]
    /\ docValue \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ nextSeq \in [Nodes -> 0..MaxWrites]
    /\ committed \in [Nodes -> 0..MaxWrites]
    /\ truncBelow \in [Nodes -> 0..MaxWrites]
    /\ pins \in [Nodes -> SUBSET (0..MaxWrites)]
    /\ copyExists \in [Nodes -> BOOLEAN]
    /\ copyAllocation \in [Nodes -> 0..MaxAllocationId]
    /\ copyUuid \in [Nodes -> {IndexUuid, NoIndexUuid}]
    /\ replicaFence \in [Nodes -> 0..MaxTerm]
    /\ durableReplicaFence \in [Nodes -> 0..MaxTerm]
    /\ copyMode \in [Nodes -> CopyModes]
    /\ installMarker \in [Nodes -> BOOLEAN]
    /\ messages \subseteq
          [kind      : MessageKinds,
           write     : WriteIds,
           from      : Nodes,
           to        : Nodes,
           seq       : 0..MaxWrites,
           fromEpoch : Nat,
           toEpoch   : Nat,
           term      : 0..MaxTerm,
           indexUuid : {IndexUuid, NoIndexUuid},
           targetAllocation : 0..MaxAllocationId]
    /\ sharedHolders \in [Nodes -> SUBSET WriteIds]
    /\ exclusiveHolder \in [Nodes -> Nodes \cup {NoNode}]
    /\ acked \subseteq WriteIds
    /\ failed \subseteq WriteIds
    /\ promotionSafe \in BOOLEAN
    /\ admissionSafe \in BOOLEAN
    /\ ackMembershipSafe \in BOOLEAN
    /\ staleApplySafe \in BOOLEAN
    /\ activePrimaryApplySafe \in BOOLEAN
    /\ termMonotonic \in BOOLEAN

ReplicationStableNext ==
    \/ \E coordinator \in Nodes, doc \in Docs, kind \in WriteKinds :
           ClientWrite(coordinator, doc, kind)
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E writeId \in WriteIds : PrimaryApplyFailure(writeId)
    \/ \E message \in messages : ReplicaReject(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E message \in messages : DeliverReplicaNack(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)
    \/ \E node \in Nodes : ProposeActivate(node)
    \/ \E node \in Nodes : CancelActivation(node)

ReplicationFenceChangingNext ==
    \/ \E message \in messages : ReplicaApply(message)
    \/ \E message \in messages : ReplicaApplyFailure(message)
    \/ \E node \in Nodes : ObserveActivation(node)
    \/ \E command \in pendingRaft : CommitRaft(command)
    \/ \E node \in Nodes : DeliverView(node)

ReplicationNext ==
    \/ /\ ReplicationStableNext
       /\ UNCHANGED
             <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
               ApplySafetyVars>>
    \/ /\ ReplicationFenceChangingNext
       /\ UNCHANGED <<copyAllocation, copyUuid>>

=============================================================================
