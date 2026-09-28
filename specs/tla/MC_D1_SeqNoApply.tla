------------------------- MODULE MC_D1_SeqNoApply ---------------------------
\* D1 replica/replay ordering slice.  It reuses the shard replication model's
\* client, primary, message, acknowledgement, and operation metadata state,
\* while adding the local version/checkpoint/WAL state needed to distinguish
\* historical arrival-order apply from the proposed sequence-aware planner.

EXTENDS Invariants

CONSTANTS PrimaryNode, ReplicaNode, DocX, DocY

D1Fixed == FaultMode = "D1Fixed"
D1Historical == FaultMode = "D1Historical"
D1Seqs == 0..(MaxWrites - 1)

VARIABLES
    walOrder,
    processedSeqs,
    processedNext,
    maxSeqNext,
    persistedProcessedNext,
    persistedMaxSeqNext,
    docSeqNext,
    tombstoneSeqNext,
    tombstoneOld,
    persistedOps,
    persistedDocValue,
    persistedDocSeqNext,
    persistedTombstoneSeqNext,
    replaying,
    replayPos,
    replayBoundary,
    replayComplete,
    replaySafe,
    commitDone,
    crashDone,
    duplicateSent,
    tombstonePruneSafe,
    pruneDone

D1Vars ==
    <<walOrder, processedSeqs, processedNext, maxSeqNext,
      persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
      tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
      persistedDocSeqNext, persistedTombstoneSeqNext, replaying, replayPos,
      replayBoundary, replayComplete, replaySafe, commitDone, crashDone,
      duplicateSent, tombstonePruneSafe, pruneDone>>

d1vars == <<vars, D1Vars>>

SeqNext(writeId) == writeSeq[writeId] + 1

ContiguousNext(processed) ==
    CHOOSE boundary \in 0..MaxWrites :
        /\ {seq \in D1Seqs : seq < boundary} \subseteq processed
        /\ (boundary = MaxWrites \/ boundary \notin processed)

ProcessedPrefix(boundary) ==
    {seq \in D1Seqs : seq < boundary}

ReplicaAckFor(message) ==
    Message("ReplicaAck", message.write, message.to, message.from, message.seq,
            epoch[message.to], message.fromEpoch, message.term,
            message.indexUuid, message.targetAllocation)

LatestAckedWriteForDoc(doc) ==
    LatestWriteForDoc(acked, doc)

LatestAckedSeqNextForDoc(doc) ==
    LET acknowledged == {writeId \in acked : writeDoc[writeId] = doc}
    IN IF acknowledged = {}
       THEN 0
       ELSE 1 + (CHOOSE sequenceNumber
                    \in {writeSeq[writeId] : writeId \in acknowledged} :
                  \A other
                    \in {writeSeq[writeId] : writeId \in acknowledged} :
                      other <= sequenceNumber)

AvailableInSyncCopies ==
    {node \in {routing.primary} \cup routing.inSync :
        /\ alive[node]
        /\ copyExists[node]
        /\ ~replaying[node]}

D1Init ==
    /\ Init
    /\ PrimaryNode # ReplicaNode
    /\ DocX \in Docs
    /\ DocY \in Docs
    /\ DocX # DocY
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ routing.inSync = {ReplicaNode}
    /\ Nodes = {PrimaryNode, ReplicaNode}
    /\ MaxWrites = 3
    /\ walOrder = [node \in Nodes |-> <<>>]
    /\ processedSeqs = [node \in Nodes |-> {}]
    /\ processedNext = [node \in Nodes |-> 0]
    /\ maxSeqNext = [node \in Nodes |-> 0]
    /\ persistedProcessedNext = [node \in Nodes |-> 0]
    /\ persistedMaxSeqNext = [node \in Nodes |-> 0]
    /\ docSeqNext = [node \in Nodes |-> [doc \in Docs |-> 0]]
    /\ tombstoneSeqNext = [node \in Nodes |-> [doc \in Docs |-> 0]]
    /\ tombstoneOld = [node \in Nodes |-> {}]
    /\ persistedOps = [node \in Nodes |-> {}]
    /\ persistedDocValue =
          [node \in Nodes |-> [doc \in Docs |-> NoWrite]]
    /\ persistedDocSeqNext =
          [node \in Nodes |-> [doc \in Docs |-> 0]]
    /\ persistedTombstoneSeqNext =
          [node \in Nodes |-> [doc \in Docs |-> 0]]
    /\ replaying = [node \in Nodes |-> FALSE]
    /\ replayPos = [node \in Nodes |-> 1]
    /\ replayBoundary = [node \in Nodes |-> 0]
    /\ replayComplete = [node \in Nodes |-> FALSE]
    /\ replaySafe = TRUE
    /\ commitDone = FALSE
    /\ crashDone = FALSE
    /\ duplicateSent = FALSE
    /\ tombstonePruneSafe = TRUE
    /\ pruneDone = FALSE

D1TypeOK ==
    /\ TypeOK
    /\ walOrder \in [Nodes -> Seq(WriteIds)]
    /\ processedSeqs \in [Nodes -> SUBSET D1Seqs]
    /\ processedNext \in [Nodes -> 0..MaxWrites]
    /\ maxSeqNext \in [Nodes -> 0..MaxWrites]
    /\ persistedProcessedNext \in [Nodes -> 0..MaxWrites]
    /\ persistedMaxSeqNext \in [Nodes -> 0..MaxWrites]
    /\ docSeqNext \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ tombstoneSeqNext \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ tombstoneOld \in [Nodes -> SUBSET Docs]
    /\ persistedOps \in [Nodes -> SUBSET WriteIds]
    /\ persistedDocValue \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ persistedDocSeqNext \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ persistedTombstoneSeqNext \in
          [Nodes -> [Docs -> 0..MaxWrites]]
    /\ replaying \in [Nodes -> BOOLEAN]
    /\ replayPos \in [Nodes -> Nat]
    /\ replayBoundary \in [Nodes -> 0..MaxWrites]
    /\ replayComplete \in [Nodes -> BOOLEAN]
    /\ replaySafe \in BOOLEAN
    /\ commitDone \in BOOLEAN
    /\ crashDone \in BOOLEAN
    /\ duplicateSent \in BOOLEAN
    /\ tombstonePruneSafe \in BOOLEAN
    /\ pruneDone \in BOOLEAN

D1ClientWrite(doc, kind) ==
    /\ ClientWrite(PrimaryNode, doc, kind)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars, D1Vars>>

D1PrimaryAccept(writeId) ==
    LET sequenceNumber == nextSeq[PrimaryNode]
        nextProcessed == processedSeqs[PrimaryNode] \cup {sequenceNumber}
        doc == writeDoc[writeId]
    IN
    /\ PrimaryAccept(writeId)
    /\ walOrder' =
          [walOrder EXCEPT ![PrimaryNode] = Append(@, writeId)]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![PrimaryNode] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![PrimaryNode] = ContiguousNext(nextProcessed)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT ![PrimaryNode] = sequenceNumber + 1]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![PrimaryNode][doc] = sequenceNumber + 1]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![PrimaryNode][doc] =
                  IF writeKind[writeId] = "Delete"
                  THEN sequenceNumber + 1
                  ELSE 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![PrimaryNode] = @ \ {doc}]
    /\ UNCHANGED
          <<persistedProcessedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, ApplySafetyVars,
            PeerRecoveryVars, FaultVars>>

D1ReplicaMessageBase(message) ==
    LET replica == message.to
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ replica = ReplicaNode
    /\ alive[replica]
    /\ ~replaying[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ~BlocksLiveReplication(replica)
    /\ ReplicaMessageValid(message)

\* Current Rust behavior: append and apply in message-arrival order.
D1HistoricalReplicaApply(message) ==
    LET replica == message.to
        writeId == message.write
        sequenceNumber == message.seq
        doc == writeDoc[writeId]
        response == ReplicaAckFor(message)
        nextProcessed == processedSeqs[replica] \cup {sequenceNumber}
    IN
    /\ D1Historical
    /\ D1ReplicaMessageBase(message)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ ops' = [ops EXCEPT ![replica] = @ \cup {writeId}]
    /\ durableOps' = [durableOps EXCEPT ![replica] = @ \cup {writeId}]
    /\ docValue' = [docValue EXCEPT ![replica][doc] = writeId]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ DurableReplicaFence
                     /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ walOrder' = [walOrder EXCEPT ![replica] = Append(@, writeId)]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![replica] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![replica] = ContiguousNext(nextProcessed)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![replica][doc] = sequenceNumber + 1]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![replica][doc] =
                  IF writeKind[writeId] = "Delete"
                  THEN sequenceNumber + 1
                  ELSE 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![replica] = @ \ {doc}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, copyMode, installMarker, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic,
            persistedProcessedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

\* D1 planner: a previously unprocessed sequence is always retained in the
\* WAL and checkpoint tracker, but only a per-document newer operation mutates
\* the logical document/tombstone state.
D1FixedReplicaProcess(message) ==
    LET replica == message.to
        writeId == message.write
        sequenceNumber == message.seq
        doc == writeDoc[writeId]
        response == ReplicaAckFor(message)
        nextProcessed == processedSeqs[replica] \cup {sequenceNumber}
        newer == sequenceNumber + 1 > docSeqNext[replica][doc]
    IN
    /\ D1Fixed
    /\ D1ReplicaMessageBase(message)
    /\ sequenceNumber \notin processedSeqs[replica]
    /\ messages' = (messages \ {message}) \cup {response}
    /\ ops' = [ops EXCEPT ![replica] = @ \cup {writeId}]
    /\ durableOps' = [durableOps EXCEPT ![replica] = @ \cup {writeId}]
    /\ docValue' =
          IF newer
          THEN [docValue EXCEPT ![replica][doc] = writeId]
          ELSE docValue
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ DurableReplicaFence
                     /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ walOrder' = [walOrder EXCEPT ![replica] = Append(@, writeId)]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![replica] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![replica] = ContiguousNext(nextProcessed)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ docSeqNext' =
          IF newer
          THEN [docSeqNext EXCEPT
                    ![replica][doc] = sequenceNumber + 1]
          ELSE docSeqNext
    /\ tombstoneSeqNext' =
          IF newer
          THEN [tombstoneSeqNext EXCEPT
                    ![replica][doc] =
                        IF writeKind[writeId] = "Delete"
                        THEN sequenceNumber + 1
                        ELSE 0]
          ELSE tombstoneSeqNext
    /\ tombstoneOld' =
          IF newer
          THEN [tombstoneOld EXCEPT ![replica] = @ \ {doc}]
          ELSE tombstoneOld
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, copyMode, installMarker, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic,
            persistedProcessedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

\* D1 redelivery: acknowledge without a second WAL entry or engine mutation.
D1FixedReplicaRedelivery(message) ==
    LET replica == message.to
        response == ReplicaAckFor(message)
    IN
    /\ D1Fixed
    /\ D1ReplicaMessageBase(message)
    /\ message.seq \in processedSeqs[replica]
    /\ messages' = (messages \ {message}) \cup {response}
    /\ replicaFence' =
          [replicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![replica] =
                  IF ReplicaFencing /\ DurableReplicaFence
                     /\ @ < message.term
                  THEN message.term
                  ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            copyMode, installMarker, sharedHolders, exclusiveHolder, acked,
            failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, D1Vars, PeerRecoveryVars,
            FaultVars>>

D1DeliverAck(message) ==
    /\ DeliverReplicaAck(message)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, D1Vars, PeerRecoveryVars, FaultVars>>

D1PrimaryAck(writeId) ==
    /\ PrimaryAck(writeId)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, D1Vars, PeerRecoveryVars, FaultVars>>

D1Redeliver(writeId) ==
    LET message ==
            Message("Replicate", writeId, PrimaryNode, ReplicaNode,
                    writeSeq[writeId], writeEpoch[writeId],
                    epoch[ReplicaNode], writeTerm[writeId], IndexUuid,
                    copyAllocation[ReplicaNode])
    IN
    /\ ~duplicateSent
    /\ writeId \in WriteIds
    /\ writeStatus[writeId] = "Replicating"
    /\ writeSeq[writeId] \in processedSeqs[ReplicaNode]
    /\ message \notin messages
    /\ messages' = messages \cup {message}
    /\ duplicateSent' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1CommitReplica ==
    LET boundary ==
            IF D1Fixed
            THEN processedNext[ReplicaNode]
            ELSE maxSeqNext[ReplicaNode]
    IN
    /\ ~commitDone
    /\ alive[ReplicaNode]
    /\ persistedProcessedNext' =
          [persistedProcessedNext EXCEPT ![ReplicaNode] = boundary]
    /\ persistedMaxSeqNext' =
          [persistedMaxSeqNext EXCEPT
              ![ReplicaNode] = maxSeqNext[ReplicaNode]]
    /\ persistedOps' =
          [persistedOps EXCEPT ![ReplicaNode] = ops[ReplicaNode]]
    /\ persistedDocValue' =
          [persistedDocValue EXCEPT
              ![ReplicaNode] = docValue[ReplicaNode]]
    /\ persistedDocSeqNext' =
          [persistedDocSeqNext EXCEPT
              ![ReplicaNode] = docSeqNext[ReplicaNode]]
    /\ persistedTombstoneSeqNext' =
          [persistedTombstoneSeqNext EXCEPT
              ![ReplicaNode] = tombstoneSeqNext[ReplicaNode]]
    /\ committed' = [committed EXCEPT ![ReplicaNode] = boundary]
    /\ commitDone' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, processedSeqs, processedNext, maxSeqNext,
            docSeqNext, tombstoneSeqNext, tombstoneOld, replaying, replayPos,
            replayBoundary, replayComplete, replaySafe, crashDone,
            duplicateSent, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1AgeTombstone ==
    /\ D1Fixed
    /\ tombstoneSeqNext[ReplicaNode][DocX] > 0
    /\ DocX \notin tombstoneOld[ReplicaNode]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![ReplicaNode] = @ \cup {DocX}]
    /\ UNCHANGED
          <<vars, walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, pruneDone>>

D1PruneTombstone ==
    LET tombstoneBoundary == tombstoneSeqNext[ReplicaNode][DocX]
    IN
    /\ D1Fixed
    /\ tombstoneBoundary > 0
    /\ DocX \in tombstoneOld[ReplicaNode]
    /\ tombstoneBoundary <= processedNext[ReplicaNode]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT ![ReplicaNode][DocX] = 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![ReplicaNode] = @ \ {DocX}]
    /\ tombstonePruneSafe' =
          tombstonePruneSafe
          /\ tombstoneBoundary <= processedNext[ReplicaNode]
    /\ pruneDone' = TRUE
    /\ UNCHANGED
          <<vars, walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            persistedOps, persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent>>

D1CrashReplica ==
    /\ commitDone
    /\ ~crashDone
    /\ alive[ReplicaNode]
    /\ acked = WriteIds
    /\ messages = {}
    /\ IF D1Fixed THEN pruneDone ELSE TRUE
    /\ alive' = [alive EXCEPT ![ReplicaNode] = FALSE]
    /\ raftConnected' =
          [raftConnected EXCEPT ![ReplicaNode] = FALSE]
    /\ crashDone' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, epoch, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
            durableOps, docValue, nextSeq, committed, truncBelow, pins,
            copyExists, copyAllocation, copyUuid, replicaFence,
            durableReplicaFence, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            duplicateSent, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1RestartReplica ==
    LET boundary == persistedProcessedNext[ReplicaNode]
    IN
    /\ crashDone
    /\ ~alive[ReplicaNode]
    /\ alive' = [alive EXCEPT ![ReplicaNode] = TRUE]
    /\ raftConnected' =
          [raftConnected EXCEPT ![ReplicaNode] = TRUE]
    /\ epoch' = [epoch EXCEPT ![ReplicaNode] = @ + 1]
    /\ ops' = [ops EXCEPT ![ReplicaNode] = persistedOps[ReplicaNode]]
    /\ durableOps' =
          [durableOps EXCEPT ![ReplicaNode] = persistedOps[ReplicaNode]]
    /\ docValue' =
          [docValue EXCEPT
              ![ReplicaNode] = persistedDocValue[ReplicaNode]]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![ReplicaNode] = persistedMaxSeqNext[ReplicaNode]]
    /\ committed' = [committed EXCEPT ![ReplicaNode] = boundary]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![ReplicaNode] = ProcessedPrefix(boundary)]
    /\ processedNext' =
          [processedNext EXCEPT ![ReplicaNode] = boundary]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![ReplicaNode] = persistedMaxSeqNext[ReplicaNode]]
    /\ docSeqNext' =
          [docSeqNext EXCEPT
              ![ReplicaNode] = persistedDocSeqNext[ReplicaNode]]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![ReplicaNode] = persistedTombstoneSeqNext[ReplicaNode]]
    /\ tombstoneOld' = [tombstoneOld EXCEPT ![ReplicaNode] = {}]
    /\ replaying' = [replaying EXCEPT ![ReplicaNode] = TRUE]
    /\ replayPos' = [replayPos EXCEPT ![ReplicaNode] = 1]
    /\ replayBoundary' =
          [replayBoundary EXCEPT ![ReplicaNode] = boundary]
    /\ replayComplete' =
          [replayComplete EXCEPT ![ReplicaNode] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, persistedProcessedNext,
            persistedMaxSeqNext, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaySafe,
            commitDone, crashDone, duplicateSent, tombstonePruneSafe,
            pruneDone, PeerRecoveryVars, FaultVars>>

D1ReplaySkip ==
    LET position == replayPos[ReplicaNode]
        writeId == walOrder[ReplicaNode][position]
    IN
    /\ replaying[ReplicaNode]
    /\ position <= Len(walOrder[ReplicaNode])
    /\ writeSeq[writeId] < replayBoundary[ReplicaNode]
    /\ replayPos' = [replayPos EXCEPT ![ReplicaNode] = @ + 1]
    /\ UNCHANGED
          <<vars, walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayBoundary, replayComplete, replaySafe, commitDone, crashDone,
            duplicateSent, tombstonePruneSafe, pruneDone>>

D1HistoricalReplayApply ==
    LET position == replayPos[ReplicaNode]
        writeId == walOrder[ReplicaNode][position]
        sequenceNumber == writeSeq[writeId]
        doc == writeDoc[writeId]
        nextProcessed == processedSeqs[ReplicaNode] \cup {sequenceNumber}
    IN
    /\ D1Historical
    /\ replaying[ReplicaNode]
    /\ position <= Len(walOrder[ReplicaNode])
    /\ sequenceNumber >= replayBoundary[ReplicaNode]
    /\ ops' = [ops EXCEPT ![ReplicaNode] = @ \cup {writeId}]
    /\ durableOps' =
          [durableOps EXCEPT ![ReplicaNode] = @ \cup {writeId}]
    /\ docValue' = [docValue EXCEPT ![ReplicaNode][doc] = writeId]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![ReplicaNode] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![ReplicaNode] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![ReplicaNode] = ContiguousNext(nextProcessed)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![ReplicaNode] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ docSeqNext' =
          [docSeqNext EXCEPT
              ![ReplicaNode][doc] = sequenceNumber + 1]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![ReplicaNode][doc] =
                  IF writeKind[writeId] = "Delete"
                  THEN sequenceNumber + 1
                  ELSE 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![ReplicaNode] = @ \ {doc}]
    /\ replayPos' = [replayPos EXCEPT ![ReplicaNode] = @ + 1]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder,
            persistedProcessedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

D1FixedReplayApply ==
    LET position == replayPos[ReplicaNode]
        writeId == walOrder[ReplicaNode][position]
        sequenceNumber == writeSeq[writeId]
        doc == writeDoc[writeId]
        redelivery == sequenceNumber \in processedSeqs[ReplicaNode]
        nextProcessed == processedSeqs[ReplicaNode] \cup {sequenceNumber}
        newer == sequenceNumber + 1 > docSeqNext[ReplicaNode][doc]
    IN
    /\ D1Fixed
    /\ replaying[ReplicaNode]
    /\ position <= Len(walOrder[ReplicaNode])
    /\ sequenceNumber >= replayBoundary[ReplicaNode]
    /\ ops' =
          IF redelivery
          THEN ops
          ELSE [ops EXCEPT ![ReplicaNode] = @ \cup {writeId}]
    /\ durableOps' =
          IF redelivery
          THEN durableOps
          ELSE [durableOps EXCEPT ![ReplicaNode] = @ \cup {writeId}]
    /\ docValue' =
          IF ~redelivery /\ newer
          THEN [docValue EXCEPT ![ReplicaNode][doc] = writeId]
          ELSE docValue
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![ReplicaNode] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedSeqs' =
          IF redelivery
          THEN processedSeqs
          ELSE [processedSeqs EXCEPT ![ReplicaNode] = nextProcessed]
    /\ processedNext' =
          IF redelivery
          THEN processedNext
          ELSE [processedNext EXCEPT
                    ![ReplicaNode] = ContiguousNext(nextProcessed)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![ReplicaNode] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ docSeqNext' =
          IF ~redelivery /\ newer
          THEN [docSeqNext EXCEPT
                    ![ReplicaNode][doc] = sequenceNumber + 1]
          ELSE docSeqNext
    /\ tombstoneSeqNext' =
          IF ~redelivery /\ newer
          THEN [tombstoneSeqNext EXCEPT
                    ![ReplicaNode][doc] =
                        IF writeKind[writeId] = "Delete"
                        THEN sequenceNumber + 1
                        ELSE 0]
          ELSE tombstoneSeqNext
    /\ tombstoneOld' =
          IF ~redelivery /\ newer
          THEN [tombstoneOld EXCEPT ![ReplicaNode] = @ \ {doc}]
          ELSE tombstoneOld
    /\ replayPos' = [replayPos EXCEPT ![ReplicaNode] = @ + 1]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder,
            persistedProcessedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

D1FinishReplay ==
    LET boundary == replayBoundary[ReplicaNode]
        covered ==
            \A position \in 1..Len(walOrder[ReplicaNode]) :
                LET writeId == walOrder[ReplicaNode][position]
                IN writeSeq[writeId] < boundary
                   \/ writeSeq[writeId] \in processedSeqs[ReplicaNode]
    IN
    /\ replaying[ReplicaNode]
    /\ replayPos[ReplicaNode] > Len(walOrder[ReplicaNode])
    /\ replaying' = [replaying EXCEPT ![ReplicaNode] = FALSE]
    /\ replayComplete' =
          [replayComplete EXCEPT ![ReplicaNode] = TRUE]
    /\ replaySafe' = replaySafe /\ covered
    /\ UNCHANGED
          <<vars, walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replayPos,
            replayBoundary, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

D1ProcessedCheckpointGapAware ==
    \A node \in Nodes :
        processedNext[node] = ContiguousNext(processedSeqs[node])

D1WalHasNoDuplicateSeq ==
    \A node \in Nodes :
      \A first \in 1..Len(walOrder[node]) :
        \A second \in 1..Len(walOrder[node]) :
            writeSeq[walOrder[node][first]]
            = writeSeq[walOrder[node][second]]
            => first = second

\* Retired: a copy may safely contain a newer unacknowledged operation while an
\* older acknowledgement is delivered.  The retained trace documents why
\* exact equality at every acknowledgement boundary is too strong.
RetiredD1AcknowledgedCopiesConverge ==
    \A doc \in Docs :
        LET expected == LatestAckedWriteForDoc(doc)
        IN IF expected = NoWrite
           THEN TRUE
           ELSE /\ docValue[PrimaryNode][doc] = expected
                /\ IF alive[ReplicaNode] /\ ~replaying[ReplicaNode]
                      THEN docValue[ReplicaNode][doc] = expected
                      ELSE TRUE

NoCopyBehindAcked ==
    \A doc \in Docs :
      \A node \in AvailableInSyncCopies :
        docSeqNext[node][doc] >= LatestAckedSeqNextForDoc(doc)

D1LogicalDocStateEqual(first, second, doc) ==
    LET firstWrite == docValue[first][doc]
        secondWrite == docValue[second][doc]
        firstDeleted ==
            firstWrite # NoWrite /\ writeKind[firstWrite] = "Delete"
        secondDeleted ==
            secondWrite # NoWrite /\ writeKind[secondWrite] = "Delete"
        firstExists ==
            firstWrite # NoWrite /\ writeKind[firstWrite] = "Put"
        secondExists ==
            secondWrite # NoWrite /\ writeKind[secondWrite] = "Put"
    IN CASE firstDeleted ->
                secondDeleted
         [] firstExists ->
                /\ secondExists
                /\ secondWrite = firstWrite
                /\ docSeqNext[second][doc] = docSeqNext[first][doc]
         [] OTHER ->
                secondWrite = NoWrite

D1QuiescentConvergence ==
    LET everyPrimaryWalWriteAcked == ops[PrimaryNode] \subseteq acked
        quiescent ==
            /\ ActiveWrites = {}
            /\ messages = {}
            /\ everyPrimaryWalWriteAcked
    IN quiescent =>
       \A replica \in routing.inSync :
         IF alive[replica] /\ copyExists[replica] /\ ~replaying[replica]
         THEN \A doc \in Docs :
                 D1LogicalDocStateEqual(PrimaryNode, replica, doc)
         ELSE TRUE

D1ReplayPreservesAcknowledged ==
    replayComplete[ReplicaNode] =>
        /\ NoCopyBehindAcked
        /\ D1QuiescentConvergence

D1DeleteNotResurrected ==
    replayComplete[ReplicaNode] =>
        /\ docValue[ReplicaNode][DocX] = 3
        /\ writeKind[docValue[ReplicaNode][DocX]] = "Delete"

D1ReplayCovered == replaySafe
D1TombstonePruningSafe == tombstonePruneSafe

D1OrderNext ==
    \/ \E doc \in Docs, kind \in WriteKinds : D1ClientWrite(doc, kind)
    \/ \E writeId \in WriteIds : D1PrimaryAccept(writeId)
    \/ \E message \in messages :
           \/ D1HistoricalReplicaApply(message)
           \/ D1FixedReplicaProcess(message)
           \/ D1FixedReplicaRedelivery(message)
    \/ \E message \in messages : D1DeliverAck(message)
    \/ \E writeId \in WriteIds : D1PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : D1Redeliver(writeId)

D1OrderSpec == D1Init /\ [][D1OrderNext]_d1vars

D1OrderConstraint ==
    /\ Cardinality(messages) <= 6
    /\ Cardinality(ActiveWrites) <= 3

\* Fixed replay scenario:
\*   seq0 Put Y, seq1 Put X, seq2 Delete X;
\*   all three overlap, the replica receives/commits seq2 first, then receives
\*   the two older writes and a duplicate before crash/replay.
D1ScenarioSubmit1 ==
    /\ nextWrite = 1
    /\ D1ClientWrite(DocY, "Put")

D1ScenarioSubmit2 ==
    /\ nextWrite = 2
    /\ writeStatus[1] = "Routed"
    /\ D1ClientWrite(DocX, "Put")

D1ScenarioSubmit3 ==
    /\ nextWrite = 3
    /\ writeStatus[1] = "Routed"
    /\ writeStatus[2] = "Routed"
    /\ D1ClientWrite(DocX, "Delete")

D1ScenarioAccept1 ==
    /\ writeStatus[1] = "Routed"
    /\ writeStatus[2] = "Routed"
    /\ writeStatus[3] = "Routed"
    /\ D1PrimaryAccept(1)

D1ScenarioAccept2 ==
    /\ writeStatus[1] = "Replicating"
    /\ writeStatus[2] = "Routed"
    /\ D1PrimaryAccept(2)

D1ScenarioAccept3 ==
    /\ writeStatus[2] = "Replicating"
    /\ writeStatus[3] = "Routed"
    /\ D1PrimaryAccept(3)

D1ScenarioReplicaApply(message) ==
    /\ IF ~commitDone
          THEN message.write = 3
          ELSE TRUE
    /\ \/ D1HistoricalReplicaApply(message)
       \/ D1FixedReplicaProcess(message)
       \/ D1FixedReplicaRedelivery(message)

D1ConcurrentOrderNext ==
    \/ D1ScenarioSubmit1
    \/ D1ScenarioSubmit2
    \/ D1ScenarioSubmit3
    \/ D1ScenarioAccept1
    \/ D1ScenarioAccept2
    \/ D1ScenarioAccept3
    \/ \E message \in messages :
           \/ D1HistoricalReplicaApply(message)
           \/ D1FixedReplicaProcess(message)
           \/ D1FixedReplicaRedelivery(message)
    \/ \E message \in messages : D1DeliverAck(message)
    \/ \E writeId \in WriteIds : D1PrimaryAck(writeId)

D1ScenarioCommit ==
    /\ 2 \in processedSeqs[ReplicaNode]
    /\ 0 \notin processedSeqs[ReplicaNode]
    /\ 1 \notin processedSeqs[ReplicaNode]
    /\ D1CommitReplica

D1ScenarioDeliverAck(message) ==
    /\ IF message.write = 2 THEN duplicateSent ELSE TRUE
    /\ D1DeliverAck(message)

D1ScenarioRedeliver ==
    /\ commitDone
    /\ processedSeqs[ReplicaNode] = D1Seqs
    /\ D1Redeliver(2)

D1ScenarioAgeTombstone ==
    /\ processedNext[ReplicaNode] = MaxWrites
    /\ D1AgeTombstone

D1ScenarioPruneTombstone ==
    D1PruneTombstone

D1ScenarioCrash ==
    D1CrashReplica

D1ScenarioRestart ==
    D1RestartReplica

D1ReplayNext ==
    \/ D1ReplaySkip
    \/ D1HistoricalReplayApply
    \/ D1FixedReplayApply
    \/ D1FinishReplay

D1ScenarioNext ==
    \/ D1ScenarioSubmit1
    \/ D1ScenarioSubmit2
    \/ D1ScenarioSubmit3
    \/ D1ScenarioAccept1
    \/ D1ScenarioAccept2
    \/ D1ScenarioAccept3
    \/ \E message \in messages : D1ScenarioReplicaApply(message)
    \/ \E message \in messages : D1ScenarioDeliverAck(message)
    \/ \E writeId \in WriteIds : D1PrimaryAck(writeId)
    \/ D1ScenarioCommit
    \/ D1ScenarioRedeliver
    \/ D1ScenarioAgeTombstone
    \/ D1ScenarioPruneTombstone
    \/ D1ScenarioCrash
    \/ D1ScenarioRestart
    \/ D1ReplayNext

D1ScenarioSpec == D1Init /\ [][D1ScenarioNext]_d1vars

\* No-durable-tombstone scenario:
\*   seq0 Put Y and seq2 Delete X reach the replica; seq1 Put X is delayed.
\*   The processed checkpoint is 1, so truncation removes seq0 but must retain
\*   seq2. Restart restores no tombstone metadata, replaying seq2 recreates it,
\*   and the delayed seq1 index remains stale after restart.
D1NoTombstoneReplicaApply(message) ==
    /\ ~commitDone
    /\ message.write \in {1, 3}
    /\ D1FixedReplicaProcess(message)

D1NoTombstoneDeliverAck(message) ==
    /\ message.write \in {1, 3}
    /\ D1DeliverAck(message)

D1NoTombstoneCommit ==
    /\ processedSeqs[ReplicaNode] = {0, 2}
    /\ D1CommitReplica

D1TruncateToProcessedCheckpoint ==
    LET boundary == persistedProcessedNext[ReplicaNode]
        retained ==
            {writeId \in {walOrder[ReplicaNode][position] :
                            position \in 1..Len(walOrder[ReplicaNode])} :
                writeSeq[writeId] >= boundary}
    IN
    /\ D1Fixed
    /\ commitDone
    /\ ~pruneDone
    /\ boundary = 1
    /\ Cardinality(retained) = 1
    /\ walOrder' =
          [walOrder EXCEPT
              ![ReplicaNode] =
                  <<CHOOSE writeId \in retained : TRUE>>]
    /\ truncBelow' = [truncBelow EXCEPT ![ReplicaNode] = boundary]
    /\ pruneDone' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, PeerRecoveryVars,
            FaultVars>>

D1NoTombstoneCrash ==
    /\ commitDone
    /\ pruneDone
    /\ ~crashDone
    /\ acked = {1, 3}
    /\ writeStatus[2] = "Replicating"
    /\ alive[ReplicaNode]
    /\ alive' = [alive EXCEPT ![ReplicaNode] = FALSE]
    /\ raftConnected' =
          [raftConnected EXCEPT ![ReplicaNode] = FALSE]
    /\ messages' = {}
    /\ crashDone' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, epoch, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, ops,
            durableOps, docValue, nextSeq, committed, truncBelow, pins,
            copyExists, copyAllocation, copyUuid, replicaFence,
            durableReplicaFence, copyMode, installMarker, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic, walOrder,
            processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            duplicateSent, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1RestartWithoutDurableTombstone ==
    LET boundary == persistedProcessedNext[ReplicaNode]
        restoredValues ==
            [doc \in Docs |->
                LET writeId == persistedDocValue[ReplicaNode][doc]
                IN IF writeId # NoWrite
                      /\ writeKind[writeId] = "Delete"
                   THEN NoWrite
                   ELSE writeId]
        restoredSeqs ==
            [doc \in Docs |->
                LET writeId == persistedDocValue[ReplicaNode][doc]
                IN IF writeId # NoWrite
                      /\ writeKind[writeId] = "Delete"
                   THEN 0
                   ELSE persistedDocSeqNext[ReplicaNode][doc]]
    IN
    /\ crashDone
    /\ ~alive[ReplicaNode]
    /\ alive' = [alive EXCEPT ![ReplicaNode] = TRUE]
    /\ raftConnected' =
          [raftConnected EXCEPT ![ReplicaNode] = TRUE]
    /\ epoch' = [epoch EXCEPT ![ReplicaNode] = @ + 1]
    /\ ops' = [ops EXCEPT ![ReplicaNode] = persistedOps[ReplicaNode]]
    /\ durableOps' =
          [durableOps EXCEPT ![ReplicaNode] = persistedOps[ReplicaNode]]
    /\ docValue' = [docValue EXCEPT ![ReplicaNode] = restoredValues]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![ReplicaNode] = persistedMaxSeqNext[ReplicaNode]]
    /\ committed' = [committed EXCEPT ![ReplicaNode] = boundary]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![ReplicaNode] = ProcessedPrefix(boundary)]
    /\ processedNext' =
          [processedNext EXCEPT ![ReplicaNode] = boundary]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![ReplicaNode] = persistedMaxSeqNext[ReplicaNode]]
    /\ docSeqNext' = [docSeqNext EXCEPT ![ReplicaNode] = restoredSeqs]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![ReplicaNode] = [doc \in Docs |-> 0]]
    /\ tombstoneOld' = [tombstoneOld EXCEPT ![ReplicaNode] = {}]
    /\ replaying' = [replaying EXCEPT ![ReplicaNode] = TRUE]
    /\ replayPos' = [replayPos EXCEPT ![ReplicaNode] = 1]
    /\ replayBoundary' =
          [replayBoundary EXCEPT ![ReplicaNode] = boundary]
    /\ replayComplete' =
          [replayComplete EXCEPT ![ReplicaNode] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, persistedProcessedNext,
            persistedMaxSeqNext, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaySafe,
            commitDone, crashDone, duplicateSent, tombstonePruneSafe,
            pruneDone, PeerRecoveryVars, FaultVars>>

D1SendLateOlderIndex ==
    LET writeId == 2
        message ==
            Message("Replicate", writeId, PrimaryNode, ReplicaNode,
                    writeSeq[writeId], writeEpoch[writeId],
                    epoch[ReplicaNode], writeTerm[writeId], IndexUuid,
                    copyAllocation[ReplicaNode])
    IN
    /\ replayComplete[ReplicaNode]
    /\ ~duplicateSent
    /\ writeStatus[writeId] = "Replicating"
    /\ message \notin messages
    /\ messages' = messages \cup {message}
    /\ duplicateSent' = TRUE
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            walOrder, processedSeqs, processedNext, maxSeqNext,
            persistedProcessedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1NoDurableTombstoneAtRestart ==
    /\ replaying[ReplicaNode]
    /\ replayPos[ReplicaNode] = 1
    => tombstoneSeqNext[ReplicaNode] = [doc \in Docs |-> 0]

D1WalTruncatedToBoundary ==
    pruneDone =>
        /\ truncBelow[ReplicaNode] = persistedProcessedNext[ReplicaNode]
        /\ \A position \in 1..Len(walOrder[ReplicaNode]) :
               writeSeq[walOrder[ReplicaNode][position]]
               >= persistedProcessedNext[ReplicaNode]

D1NoTombstoneNext ==
    \/ D1ScenarioSubmit1
    \/ D1ScenarioSubmit2
    \/ D1ScenarioSubmit3
    \/ D1ScenarioAccept1
    \/ D1ScenarioAccept2
    \/ D1ScenarioAccept3
    \/ \E message \in messages : D1NoTombstoneReplicaApply(message)
    \/ \E message \in messages : D1NoTombstoneDeliverAck(message)
    \/ \E writeId \in WriteIds : D1PrimaryAck(writeId)
    \/ D1NoTombstoneCommit
    \/ D1TruncateToProcessedCheckpoint
    \/ D1NoTombstoneCrash
    \/ D1RestartWithoutDurableTombstone
    \/ D1ReplayNext
    \/ D1SendLateOlderIndex
    \/ \E message \in messages :
           \/ D1FixedReplicaProcess(message)
              /\ replayComplete[ReplicaNode]
           \/ D1FixedReplicaRedelivery(message)
              /\ replayComplete[ReplicaNode]

D1NoTombstoneSpec == D1Init /\ [][D1NoTombstoneNext]_d1vars

=============================================================================
