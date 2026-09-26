-------------------------- MODULE ShardReplication --------------------------
\* One FerrisSearch local_shards shard.  Term values are normalized so Init is
\* the state immediately after the initial primary has activated at term 1.
\* Promotion increments to term 2 and the promoted primary activates at term 3.
\* Equality, ordering, and conditional-commit behavior match the Rust terms;
\* only the omitted bootstrap increment is renumbered.

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
    EnableRecovery,
    AllocationIds

WriteIds == 1..MaxWrites
NoWrite == 0
DefaultDoc == CHOOSE d \in Docs : TRUE

WriteStatuses == {"Unused", "Routed", "Replicating", "Acked", "Failed"}
WriteKinds == {"Put", "Delete"}
CopyModes == {"Active", "Recovering", "Pending", "InstallMarker"}
MessageKinds == {"Replicate", "ReplicaAck"}

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
        sourceEpoch, targetEpoch) ==
    [kind      |-> messageKind,
     write     |-> writeId,
     from      |-> sourceNode,
     to        |-> targetNode,
     seq       |-> sequenceNumber,
     fromEpoch |-> sourceEpoch,
     toEpoch   |-> targetEpoch]

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
    termMonotonic

ReplicationVars ==
    <<routing, alive, epoch, raftConnected, activated, activationPending,
      nextWrite, writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
      writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
      durableOps, docValue, nextSeq, committed, truncBelow, pins, copyExists,
      copyAllocation, copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
      acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
      termMonotonic>>

RoutingWellFormedValue(r) ==
    /\ r.primary \in Nodes
    /\ r.term \in 1..MaxTerm
    /\ r.replicas \subseteq Nodes \ {r.primary}
    /\ r.inSync \subseteq r.replicas
    /\ r.unassigned \in 0..Cardinality(Nodes)
    /\ r.members \subseteq Nodes
    /\ r.allocations \in [Nodes -> 0..MaxAllocationId]
    /\ \A node \in Nodes :
           (node = r.primary \/ node \in r.replicas)
           <=> r.allocations[node] > 0

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
    copyMode[node] \in {"Recovering", "InstallMarker"}

CopyAssignmentValid(node) ==
    \/ ~AllocationIds
    \/ /\ copyAllocation[node] > 0
       /\ copyAllocation[node] = views[node].allocations[node]

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
                               initialInSync, 0, Nodes, initialAllocations)
          IN
          /\ IF InitialOutOfSync
                THEN Cardinality(initialInSync) =
                     Cardinality(initialReplicas) - 1
                ELSE initialInSync = initialReplicas
          /\ routing = initialRouting
          /\ RaftInit(initialRouting, initialLeader)
          /\ alive = [n \in Nodes |-> TRUE]
          /\ epoch = [n \in Nodes |-> 0]
          /\ raftConnected = [n \in Nodes |-> TRUE]
          /\ activated =
                [n \in Nodes |-> IF n = initialPrimary THEN 1 ELSE NoTerm]
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
          /\ copyExists = [n \in Nodes |-> TRUE]
          /\ copyAllocation =
                [n \in Nodes |-> initialRouting.allocations[n]]
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

CanPrimaryAccept(writeId) ==
    LET primaryNode == writeTarget[writeId]
    IN
    /\ writeStatus[writeId] = "Routed"
    /\ primaryNode \in Nodes
    /\ alive[primaryNode]
    /\ copyExists[primaryNode]
    /\ CopyAssignmentValid(primaryNode)
    /\ views[primaryNode].primary = primaryNode
    /\ activated[primaryNode] = views[primaryNode].term
    /\ exclusiveHolder[primaryNode] = NoNode
    /\ nextSeq[primaryNode] < MaxWrites

\* src/transport/server/mod.rs::{index_doc,bulk_index,delete_doc}
\* ensure_primary_activated + peer_recovery_write_guard +
\* validated_primary_write_state are represented by the guards below.
PrimaryAccept(writeId) ==
    LET primaryNode == writeTarget[writeId]
        sequenceNumber == nextSeq[primaryNode]
        requiredReplicas == views[primaryNode].inSync
        requests ==
            {Message("Replicate", writeId, primaryNode, replica,
                     sequenceNumber, epoch[primaryNode], epoch[replica]) :
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
    /\ ~CanPrimaryAccept(writeId)
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

\* src/transport/server/mod.rs::{replicate_doc,replicate_bulk}
\* Deliberately no primary-term check.  Only the pre-finalize recovery gate
\* blocks live apply; Pending copies accept live writes.
ReplicaApply(message) ==
    LET replica == message.to
        writeId == message.write
        response ==
            Message("ReplicaAck", writeId, replica, message.from, message.seq,
                    epoch[replica], message.fromEpoch)
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ alive[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ~BlocksLiveReplication(replica)
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
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyMode, installMarker, sharedHolders, exclusiveHolder, acked,
            failed, promotionSafe, admissionSafe, ackMembershipSafe,
            termMonotonic>>

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

ActivateCommand(primaryNode, expectedPrimaryTerm) ==
    RaftCommand("ActivatePrimary", primaryNode, primaryNode, primaryNode,
                expectedPrimaryTerm, primaryNode, {}, 0, 0,
                EmptyAllocations)

\* src/transport/server/mod.rs::ensure_primary_activated
ProposeActivate(primaryNode) ==
    LET local == views[primaryNode]
        command == ActivateCommand(primaryNode, local.term)
    IN
    /\ primaryNode \in Nodes
    /\ CanReachRaft(primaryNode)
    /\ local.primary = primaryNode
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
    /\ activated' = [activated EXCEPT ![primaryNode] = local.term]
    /\ activationPending' =
          [activationPending EXCEPT ![primaryNode] = NoTerm]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
            durableOps, docValue, nextSeq, committed, truncBelow, pins,
            copyExists, copyMode, installMarker, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, termMonotonic>>

\* TransportService::ensure_primary_activated aborts when a newer term or
\* different primary makes the requested activation impossible.
CancelActivation(primaryNode) ==
    LET expected == activationPending[primaryNode]
        local == views[primaryNode]
    IN
    /\ expected # NoTerm
    /\ \/ local.primary # primaryNode
       \/ local.term > expected + 1
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
                         command.newAllocations)
    IN
    /\ command.newPrimary \in Nodes
    /\ command.newReplicas \subseteq Nodes
    /\ command.newUnassigned \in 0..Cardinality(Nodes)
    /\ IF command.newPrimary # current.primary
          THEN /\ command.newPrimary \in current.inSync
               /\ current.term < MaxTerm
          ELSE TRUE
    /\ RoutingWellFormedValue(proposed)

CommandAccepted(current, command) ==
    CASE command.kind = "ActivatePrimary" ->
            /\ command.target = current.primary
            /\ command.expectedPrimary = current.primary
            /\ command.expectedTerm = current.term
            /\ current.term < MaxTerm
      [] command.kind = "UpdateRouting" ->
            UpdateRoutingAccepted(current, command)
      [] command.kind = "MarkReplicaInSync" ->
            /\ command.target \in current.replicas
            /\ command.target \notin current.inSync
            /\ command.expectedPrimary = current.primary
            /\ command.expectedTerm = current.term
            /\ IF AllocationIds
                  THEN /\ command.expectedAllocation > 0
                       /\ command.expectedAllocation =
                          current.allocations[command.target]
                  ELSE TRUE
      [] command.kind = "RemoveNode" -> command.target \in current.members
      [] command.kind = "AddNode" ->
            /\ command.target \in Nodes
            /\ command.target \notin current.members
      [] OTHER -> FALSE

AfterAcceptedCommand(current, command) ==
    CASE command.kind = "ActivatePrimary" ->
            RoutingState(current.primary, current.term + 1, current.replicas,
                         current.inSync, current.unassigned, current.members,
                         current.allocations)
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
                            command.newAllocations)
      [] command.kind = "MarkReplicaInSync" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync \cup {command.target},
                         current.unassigned, current.members,
                         current.allocations)
      [] command.kind = "RemoveNode" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync, current.unassigned,
                         current.members \ {command.target},
                         current.allocations)
      [] command.kind = "AddNode" ->
            RoutingState(current.primary, current.term, current.replicas,
                         current.inSync, current.unassigned,
                         current.members \cup {command.target},
                         current.allocations)
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
            /\ command.kind = "UpdateRouting"
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
            ackMembershipSafe>>

\* ClusterManager applies one more committed Raft entry on this node.
\* ClusterManager's state-machine-backed local view advances after a committed
\* openraft log entry is applied on this node.
DeliverView(node) ==
    /\ node \in Nodes
    /\ alive[node]
    /\ raftConnected[node]
    /\ DeliverRaftView(node)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic>>

ReplicationTypeOK ==
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
    /\ copyMode \in [Nodes -> CopyModes]
    /\ installMarker \in [Nodes -> BOOLEAN]
    /\ messages \subseteq
          [kind      : MessageKinds,
           write     : WriteIds,
           from      : Nodes,
           to        : Nodes,
           seq       : 0..MaxWrites,
           fromEpoch : Nat,
           toEpoch   : Nat]
    /\ sharedHolders \in [Nodes -> SUBSET WriteIds]
    /\ exclusiveHolder \in [Nodes -> Nodes \cup {NoNode}]
    /\ acked \subseteq WriteIds
    /\ failed \subseteq WriteIds
    /\ promotionSafe \in BOOLEAN
    /\ admissionSafe \in BOOLEAN
    /\ ackMembershipSafe \in BOOLEAN
    /\ termMonotonic \in BOOLEAN

ReplicationNext ==
    /\ UNCHANGED copyAllocation
    /\ \/ \E coordinator \in Nodes, doc \in Docs, kind \in WriteKinds :
              ClientWrite(coordinator, doc, kind)
       \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
       \/ \E writeId \in WriteIds : PrimaryReject(writeId)
       \/ \E message \in messages : ReplicaApply(message)
       \/ \E message \in messages : DeliverReplicaAck(message)
       \/ \E writeId \in WriteIds : PrimaryAck(writeId)
       \/ \E writeId \in WriteIds : PrimaryFail(writeId)
       \/ \E node \in Nodes : ProposeActivate(node)
       \/ \E node \in Nodes : ObserveActivation(node)
       \/ \E node \in Nodes : CancelActivation(node)
       \/ \E command \in pendingRaft : CommitRaft(command)
       \/ \E node \in Nodes : DeliverView(node)

=============================================================================
