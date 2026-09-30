------------------------- MODULE MC_D1_SeqNoApply ---------------------------
\* D1 replica/replay ordering slice.  It reuses the shard replication model's
\* client, primary, message, acknowledgement, and operation metadata state,
\* while adding the local version/checkpoint/WAL state needed to distinguish
\* historical arrival-order apply from the proposed sequence-aware planner.

EXTENDS Invariants

CONSTANTS PrimaryNode, ReplicaNode, DocX, DocY

D1Fixed == FaultMode \in {"D1Fixed", "D1Async"}
D1Historical == FaultMode = "D1Historical"
D1RequestDurability == FaultMode # "D1Async"
D1Seqs == 0..(MaxWrites - 1)

VARIABLES
    walOrder,
    noopTerm,
    processedSeqs,
    processedNext,
    persistedSeqs,
    persistedNext,
    maxSeqNext,
    persistedProcessedNext,
    persistedCommittedNext,
    persistedMaxSeqNext,
    d1FenceTerm,
    fenceMaxSeqNext,
    processedTerm,
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
    <<walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs, persistedNext,
      maxSeqNext, persistedProcessedNext, persistedCommittedNext,
      persistedMaxSeqNext, d1FenceTerm, fenceMaxSeqNext, processedTerm,
      docSeqNext,
      tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
      persistedDocSeqNext, persistedTombstoneSeqNext, replaying, replayPos,
      replayBoundary, replayComplete, replaySafe, commitDone, crashDone,
      duplicateSent, tombstonePruneSafe, pruneDone>>

d1vars == <<vars, D1Vars>>

NoOpEntry(sequenceNumber) == MaxWrites + sequenceNumber + 1
NoOpEntries == {NoOpEntry(sequenceNumber) : sequenceNumber \in D1Seqs}
WalEntries == WriteIds \cup NoOpEntries

IsNoOpEntry(entry) == entry \in NoOpEntries

WalEntrySeq(entry) ==
    IF entry \in WriteIds
    THEN writeSeq[entry]
    ELSE entry - MaxWrites - 1

WalEntryTerm(node, entry) ==
    IF entry \in WriteIds
    THEN writeTerm[entry]
    ELSE noopTerm[node][WalEntrySeq(entry)]

SortedNoOpEntries(sequences) ==
    [ordinal \in 1..Cardinality(sequences) |->
        NoOpEntry(
            CHOOSE sequenceNumber \in sequences :
                Cardinality(
                    {other \in sequences : other < sequenceNumber})
                = ordinal - 1)]

SeqNext(writeId) == writeSeq[writeId] + 1

ContiguousNext(processed) ==
    CHOOSE boundary \in 0..MaxWrites :
        /\ {seq \in D1Seqs : seq < boundary} \subseteq processed
        /\ (boundary = MaxWrites \/ boundary \notin processed)

ProcessedPrefix(boundary) ==
    {seq \in D1Seqs : seq < boundary}

D1FenceMaxNext(node) ==
    IF maxSeqNext[node] > nextSeq[node]
    THEN maxSeqNext[node]
    ELSE nextSeq[node]

D1TermCollision(node, term, sequenceNumber) ==
    /\ term = d1FenceTerm[node]
    /\ sequenceNumber < fenceMaxSeqNext[node]
    /\ sequenceNumber \in processedSeqs[node]
    /\ processedTerm[node][sequenceNumber] < term

\* ShardManager::raise_copy_fence_blocking persists the term and captured
\* maximum before a higher-term replica WAL append or primary activation.
D1ObserveFence(node, term, capturedMaxNext) ==
    /\ node \in Nodes
    /\ term > 0
    /\ capturedMaxNext = D1FenceMaxNext(node)
    /\ term >= d1FenceTerm[node]
    /\ d1FenceTerm' = [d1FenceTerm EXCEPT ![node] = term]
    /\ fenceMaxSeqNext' =
          [fenceMaxSeqNext EXCEPT
              ![node] =
                  IF term > d1FenceTerm[node] THEN capturedMaxNext ELSE @]
    /\ replicaFence' =
          [replicaFence EXCEPT ![node] = IF @ < term THEN term ELSE @]
    /\ durableReplicaFence' =
          [durableReplicaFence EXCEPT
              ![node] = IF @ < term THEN term ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder, noopTerm, processedSeqs,
            processedNext, persistedSeqs, persistedNext, maxSeqNext,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, processedTerm, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, pruneDone,
            PeerRecoveryVars, FaultVars>>

D1PromotionGaps(node) ==
    {seq \in D1Seqs :
        /\ seq < maxSeqNext[node]
        /\ seq \notin processedSeqs[node]}

\* TransportService::ensure_primary_activated publishes availability only
\* after HotEngine::prepare_primary_activation has replayed and closed gaps.
D1ObserveActivation(node) ==
    /\ node \in Nodes
    /\ ~replaying[node]
    /\ D1PromotionGaps(node) = {}
    /\ d1FenceTerm[node] >= views[node].term
    /\ durableReplicaFence[node] >= views[node].term
    /\ ObserveActivation(node)
    /\ UNCHANGED D1Vars

D1WalSequences(node) ==
    {WalEntrySeq(walOrder[node][position]) :
        position \in 1..Len(walOrder[node])}

\* HotEngine::prepare_primary_activation replays the complete local WAL, then
\* appends and syncs one NoOp for every remaining gap through the local maximum.
\* Fan-out happens only after activation from a fresh routing snapshot.
\* Summary-only traces use this atomic fill action. Faithful traces use the
\* append/process/fill-observation actions below.
D1FillPromotionNoOps(node, filledSeqs) ==
    LET nextProcessed == processedSeqs[node] \cup filledSeqs
        nextPersisted == persistedSeqs[node] \cup filledSeqs
        noOpEntries == SortedNoOpEntries(filledSeqs)
    IN
    /\ D1Fixed
    /\ node = routing.primary
    /\ alive[node]
    /\ copyExists[node]
    /\ CopyAssignmentValid(node)
    /\ ~BlocksLiveReplication(node)
    /\ ~replaying[node]
    /\ views[node].primary = node
    /\ views[node].term = routing.term
    /\ views[node].initialized
    /\ d1FenceTerm[node] = routing.term
    /\ filledSeqs = D1PromotionGaps(node)
    /\ filledSeqs \cap D1WalSequences(node) = {}
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT ![node] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = ContiguousNext(nextPersisted)]
    /\ walOrder' =
          [walOrder EXCEPT ![node] = @ \o noOpEntries]
    /\ noopTerm' =
          [noopTerm EXCEPT
              ![node] =
                  [seq \in D1Seqs |->
                      IF seq \in filledSeqs
                      THEN routing.term
                      ELSE noopTerm[node][seq]]]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![node] =
                  [seq \in D1Seqs |->
                      IF seq \in filledSeqs
                      THEN routing.term
                      ELSE processedTerm[node][seq]]]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, docSeqNext, tombstoneSeqNext, tombstoneOld,
            persistedOps, persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

\* Physical promotion-fill WAL append. All batch entries are appended before
\* any corresponding processing event.
D1AppendPromotionNoOp(node, sequenceNumber, term) ==
    /\ D1Fixed
    /\ node = routing.primary
    /\ alive[node]
    /\ copyExists[node]
    /\ CopyAssignmentValid(node)
    /\ ~BlocksLiveReplication(node)
    /\ ~replaying[node]
    /\ views[node].primary = node
    /\ views[node].term = routing.term
    /\ views[node].initialized
    /\ d1FenceTerm[node] = routing.term
    /\ term = routing.term
    /\ sequenceNumber \in D1PromotionGaps(node)
    /\ sequenceNumber \notin D1WalSequences(node)
    /\ walOrder' =
          [walOrder EXCEPT ![node] = Append(@, NoOpEntry(sequenceNumber))]
    /\ noopTerm' =
          [noopTerm EXCEPT ![node][sequenceNumber] = term]
    /\ UNCHANGED
          <<vars, processedSeqs, processedNext, persistedSeqs, persistedNext,
            maxSeqNext, persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, d1FenceTerm, fenceMaxSeqNext, processedTerm,
            docSeqNext, tombstoneSeqNext, tombstoneOld, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

\* The planner completes one already-appended promotion NoOp. Request
\* durability marks it persisted here; async durability waits for the fill
\* summary's explicit translog sync.
D1ProcessPromotionNoOp(node, sequenceNumber, term) ==
    LET nextProcessed == processedSeqs[node] \cup {sequenceNumber}
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[node] \cup {sequenceNumber}
            ELSE persistedSeqs[node]
    IN
    /\ D1Fixed
    /\ node = routing.primary
    /\ alive[node]
    /\ term = routing.term
    /\ d1FenceTerm[node] = term
    /\ sequenceNumber \in D1WalSequences(node)
    /\ noopTerm[node][sequenceNumber] = term
    /\ sequenceNumber \notin processedSeqs[node]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT ![node] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = ContiguousNext(nextPersisted)]
    /\ processedTerm' =
          [processedTerm EXCEPT ![node][sequenceNumber] = term]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, docSeqNext, tombstoneSeqNext, tombstoneOld,
            persistedOps, persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

\* The summary event follows append and processing. It observes the exact
\* filled set and models the explicit sync that makes every fill entry durable.
D1ObservePromotionNoOpFill(node, filledSeqs, term) ==
    LET nextPersisted == persistedSeqs[node] \cup filledSeqs
    IN
    /\ D1Fixed
    /\ node = routing.primary
    /\ alive[node]
    /\ term = routing.term
    /\ filledSeqs # {}
    /\ D1PromotionGaps(node) = {}
    /\ \A sequenceNumber \in filledSeqs :
           /\ sequenceNumber \in processedSeqs[node]
           /\ sequenceNumber \in D1WalSequences(node)
           /\ noopTerm[node][sequenceNumber] = term
           /\ processedTerm[node][sequenceNumber] = term
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = ContiguousNext(nextPersisted)]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext,
            maxSeqNext, persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, d1FenceTerm, fenceMaxSeqNext, processedTerm,
            docSeqNext, tombstoneSeqNext, tombstoneOld, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

ReplicaAckFor(message) ==
    Message("ReplicaAck", message.write, message.to, message.from, message.seq,
            epoch[message.to], message.fromEpoch, message.term,
            message.indexUuid, message.targetAllocation)

NoOpAckFor(message) ==
    Message("NoOpAck", NoWrite, message.to, message.from, message.seq,
            epoch[message.to], message.fromEpoch, message.term,
            message.indexUuid, message.targetAllocation)

NoOpNackFor(message) ==
    Message("NoOpNack", NoWrite, message.to, message.from, message.seq,
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
        /\ ~replaying[node]
        /\ ~BlocksLiveReplication(node)}

D1DataInit ==
    /\ walOrder = [node \in Nodes |-> <<>>]
    /\ noopTerm = [node \in Nodes |-> [seq \in D1Seqs |-> 0]]
    /\ processedSeqs = [node \in Nodes |-> {}]
    /\ processedNext = [node \in Nodes |-> 0]
    /\ persistedSeqs = [node \in Nodes |-> {}]
    /\ persistedNext = [node \in Nodes |-> 0]
    /\ maxSeqNext = [node \in Nodes |-> 0]
    /\ persistedProcessedNext = [node \in Nodes |-> 0]
    /\ persistedCommittedNext = [node \in Nodes |-> 0]
    /\ persistedMaxSeqNext = [node \in Nodes |-> 0]
    /\ d1FenceTerm =
          [node \in Nodes |->
              IF InitialInitialized /\ ReplicaFencing THEN 1 ELSE 0]
    /\ fenceMaxSeqNext = [node \in Nodes |-> 0]
    /\ processedTerm =
          [node \in Nodes |-> [seq \in D1Seqs |-> 0]]
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
    /\ D1DataInit

D1TypeOK ==
    /\ TypeOK
    /\ walOrder \in [Nodes -> Seq(WalEntries)]
    /\ noopTerm \in [Nodes -> [D1Seqs -> 0..MaxTerm]]
    /\ processedSeqs \in [Nodes -> SUBSET D1Seqs]
    /\ processedNext \in [Nodes -> 0..MaxWrites]
    /\ persistedSeqs \in [Nodes -> SUBSET D1Seqs]
    /\ persistedNext \in [Nodes -> 0..MaxWrites]
    /\ maxSeqNext \in [Nodes -> 0..MaxWrites]
    /\ persistedProcessedNext \in [Nodes -> 0..MaxWrites]
    /\ persistedCommittedNext \in [Nodes -> 0..MaxWrites]
    /\ persistedMaxSeqNext \in [Nodes -> 0..MaxWrites]
    /\ d1FenceTerm \in [Nodes -> 0..MaxTerm]
    /\ fenceMaxSeqNext \in [Nodes -> 0..MaxWrites]
    /\ processedTerm \in [Nodes -> [D1Seqs -> 0..MaxTerm]]
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

D1ClientWriteFrom(coordinator, doc, kind) ==
    /\ ClientWrite(coordinator, doc, kind)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars, D1Vars>>

D1ClientWrite(doc, kind) ==
    D1ClientWriteFrom(PrimaryNode, doc, kind)

D1ObserveBatchPlan(node, plannedMaxNext) ==
    /\ node \in Nodes
    /\ plannedMaxNext \in 0..MaxWrites
    /\ plannedMaxNext > maxSeqNext[node]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT ![node] = plannedMaxNext]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext,
            persistedSeqs, persistedNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, docSeqNext, tombstoneSeqNext,
            tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, pruneDone,
            PeerRecoveryVars, FaultVars>>

D1PrimaryAccept(writeId) ==
    LET primaryNode == writeTarget[writeId]
        sequenceNumber == nextSeq[primaryNode]
        nextProcessed == processedSeqs[primaryNode] \cup {sequenceNumber}
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[primaryNode] \cup {sequenceNumber}
            ELSE persistedSeqs[primaryNode]
        doc == writeDoc[writeId]
    IN
    /\ d1FenceTerm[primaryNode] >= views[primaryNode].term
    /\ durableReplicaFence[primaryNode] >= views[primaryNode].term
    /\ PrimaryAccept(writeId)
    /\ walOrder' =
          [walOrder EXCEPT ![primaryNode] = Append(@, writeId)]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![primaryNode] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![primaryNode] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![primaryNode] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![primaryNode] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![primaryNode] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![primaryNode][sequenceNumber] = views[primaryNode].term]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![primaryNode][doc] = sequenceNumber + 1]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![primaryNode][doc] =
                  IF writeKind[writeId] = "Delete"
                  THEN sequenceNumber + 1
                  ELSE 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![primaryNode] = @ \ {doc}]
    /\ UNCHANGED
          <<persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, d1FenceTerm,
            fenceMaxSeqNext, noopTerm, ApplySafetyVars,
            PeerRecoveryVars, FaultVars>>

D1ReplicaMessageEnabled(message) ==
    LET replica == message.to
    IN
    /\ message \in messages
    /\ message.kind = "Replicate"
    /\ alive[replica]
    /\ ~replaying[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ~BlocksLiveReplication(replica)
    /\ IF D1Fixed
          THEN /\ d1FenceTerm[replica] >= message.term
               /\ durableReplicaFence[replica] >= message.term
          ELSE TRUE
    /\ ReplicaMessageValid(message)

D1ReplicaMessageBase(message) ==
    D1ReplicaMessageEnabled(message)

\* Current Rust behavior: append and apply in message-arrival order.
D1HistoricalReplicaApply(message) ==
    LET replica == message.to
        writeId == message.write
        sequenceNumber == message.seq
        doc == writeDoc[writeId]
        response == ReplicaAckFor(message)
        nextProcessed == processedSeqs[replica] \cup {sequenceNumber}
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[replica] \cup {sequenceNumber}
            ELSE persistedSeqs[replica]
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
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![replica] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![replica] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![replica][sequenceNumber] = message.term]
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
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, noopTerm, PeerRecoveryVars, FaultVars>>

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
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[replica] \cup {sequenceNumber}
            ELSE persistedSeqs[replica]
        newer == sequenceNumber + 1 > docSeqNext[replica][doc]
    IN
    /\ D1Fixed
    /\ D1ReplicaMessageBase(message)
    /\ sequenceNumber \notin processedSeqs[replica]
    /\ ~D1TermCollision(replica, message.term, sequenceNumber)
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
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![replica] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![replica] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![replica][sequenceNumber] = message.term]
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
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, noopTerm, PeerRecoveryVars, FaultVars>>

\* D1 redelivery: acknowledge without a second WAL entry or engine mutation.
D1FixedReplicaRedelivery(message) ==
    LET replica == message.to
        response == ReplicaAckFor(message)
    IN
    /\ D1Fixed
    /\ D1ReplicaMessageBase(message)
    /\ message.seq \in processedSeqs[replica]
    /\ ~D1TermCollision(replica, message.term, message.seq)
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

D1FixedReplicaCollision(message) ==
    LET replica == message.to
        response ==
            Message("ReplicaNack", message.write, replica, message.from,
                    message.seq, epoch[replica], message.fromEpoch,
                    message.term, message.indexUuid,
                    message.targetAllocation)
    IN
    /\ D1Fixed
    /\ D1ReplicaMessageEnabled(message)
    /\ D1TermCollision(replica, message.term, message.seq)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ copyMode' = [copyMode EXCEPT ![replica] = "ApplyFailed"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic, D1Vars,
            PeerRecoveryVars, FaultVars>>

\* Promotion NoOps use the same fencing, WAL, and checkpoint planner as
\* ordinary replication, but they do not have a client write ID or mutate a
\* document. The durable fence is raised separately before this action.
D1NoOpMessageEnabled(message) ==
    LET replica == message.to
    IN
    /\ message \in messages
    /\ message.kind = "ReplicateNoOp"
    /\ message.write = NoWrite
    /\ message.seq \in D1Seqs
    /\ alive[replica]
    /\ ~replaying[replica]
    /\ copyExists[replica]
    /\ CopyAssignmentValid(replica)
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ ~BlocksLiveReplication(replica)
    /\ d1FenceTerm[replica] >= message.term
    /\ durableReplicaFence[replica] >= message.term
    /\ ReplicaMessageValid(message)

D1FixedReplicaNoOpProcess(message) ==
    LET replica == message.to
        sequenceNumber == message.seq
        response == NoOpAckFor(message)
        nextProcessed == processedSeqs[replica] \cup {sequenceNumber}
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[replica] \cup {sequenceNumber}
            ELSE persistedSeqs[replica]
    IN
    /\ D1Fixed
    /\ D1NoOpMessageEnabled(message)
    /\ sequenceNumber \notin processedSeqs[replica]
    /\ ~D1TermCollision(replica, message.term, sequenceNumber)
    /\ messages' = (messages \ {message}) \cup {response}
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
    /\ staleApplySafe' =
          staleApplySafe /\ message.term >= durableReplicaFence[replica]
    /\ activePrimaryApplySafe' =
          activePrimaryApplySafe
          /\ (activated[replica] = NoTerm
              \/ message.term >= activated[replica])
    /\ walOrder' =
          [walOrder EXCEPT ![replica] = Append(@, NoOpEntry(sequenceNumber))]
    /\ noopTerm' =
          [noopTerm EXCEPT ![replica][sequenceNumber] = message.term]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![replica] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![replica] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![replica] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![replica] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![replica] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![replica][sequenceNumber] = message.term]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid, copyMode,
            installMarker, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, termMonotonic,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, d1FenceTerm, fenceMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, pruneDone,
            PeerRecoveryVars, FaultVars>>

\* A matching duplicate is acknowledged without another WAL append.
D1FixedReplicaNoOpRedelivery(message) ==
    LET replica == message.to
        response == NoOpAckFor(message)
    IN
    /\ D1Fixed
    /\ D1NoOpMessageEnabled(message)
    /\ message.seq \in processedSeqs[replica]
    /\ ~D1TermCollision(replica, message.term, message.seq)
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

\* A newer-term NoOp colliding with an older operation identity fails the copy
\* closed exactly like an ordinary write collision.
D1FixedReplicaNoOpCollision(message) ==
    LET replica == message.to
        response == NoOpNackFor(message)
    IN
    /\ D1Fixed
    /\ D1NoOpMessageEnabled(message)
    /\ D1TermCollision(replica, message.term, message.seq)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ copyMode' = [copyMode EXCEPT ![replica] = "ApplyFailed"]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, installMarker, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic, D1Vars,
            PeerRecoveryVars, FaultVars>>

\* A stale allocation, stale term, or unavailable copy rejects a promotion
\* NoOp without changing the local WAL or document state.
D1FixedReplicaNoOpReject(message) ==
    LET replica == message.to
        response == NoOpNackFor(message)
    IN
    /\ D1Fixed
    /\ message \in messages
    /\ message.kind = "ReplicateNoOp"
    /\ message.write = NoWrite
    /\ alive[replica]
    /\ copyExists[replica]
    /\ epoch[replica] = message.toEpoch
    /\ epoch[message.from] = message.fromEpoch
    /\ \/ BlocksLiveReplication(replica)
       \/ ~ReplicaMessageValid(message)
    /\ messages' = (messages \ {message}) \cup {response}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            D1Vars, PeerRecoveryVars, FaultVars>>

\* Promotion NoOp responses do not participate in a client write quorum.
\* Their success or failure is observed by the activation fan-out, then
\* discarded; collision/removal state is carried separately.
D1DeliverNoOpAck(message) ==
    /\ message \in messages
    /\ message.kind = "NoOpAck"
    /\ message.write = NoWrite
    /\ alive[message.to]
    /\ epoch[message.to] = message.toEpoch
    /\ messages' = messages \ {message}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            D1Vars, PeerRecoveryVars, FaultVars>>

D1DeliverNoOpNack(message) ==
    /\ message \in messages
    /\ message.kind = "NoOpNack"
    /\ message.write = NoWrite
    /\ alive[message.to]
    /\ epoch[message.to] = message.toEpoch
    /\ messages' = messages \ {message}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            D1Vars, PeerRecoveryVars, FaultVars>>

D1RedeliverPromotionNoOp(primaryNode, replica, sequenceNumber) ==
    LET message ==
            Message("ReplicateNoOp", NoWrite, primaryNode, replica,
                    sequenceNumber, epoch[primaryNode], epoch[replica],
                    noopTerm[primaryNode][sequenceNumber], IndexUuid,
                    views[primaryNode].allocations[replica])
    IN
    /\ D1Fixed
    /\ primaryNode \in Nodes
    /\ replica \in views[primaryNode].inSync
    /\ sequenceNumber \in processedSeqs[primaryNode]
    /\ noopTerm[primaryNode][sequenceNumber] > 0
    /\ alive[primaryNode]
    /\ views[primaryNode].primary = primaryNode
    /\ activated[primaryNode] = views[primaryNode].term
    /\ message \notin messages
    /\ messages' = messages \cup {message}
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe, ackMembershipSafe, ApplySafetyVars, termMonotonic,
            D1Vars, PeerRecoveryVars, FaultVars>>

D1DocSeqNextFor(writeSet) ==
    [doc \in Docs |->
        LET writeId == LatestWriteForDoc(writeSet, doc)
        IN IF writeId = NoWrite THEN 0 ELSE writeSeq[writeId] + 1]

D1TombstoneSeqNextFor(writeSet) ==
    [doc \in Docs |->
        LET writeId == LatestWriteForDoc(writeSet, doc)
        IN IF writeId # NoWrite /\ writeKind[writeId] = "Delete"
           THEN writeSeq[writeId] + 1
           ELSE 0]

D1SnapshotSequences(source, writeSet, boundary) ==
    {writeSeq[writeId] : writeId \in writeSet}
    \cup
    {seq \in D1Seqs :
        /\ seq < boundary
        /\ seq \in processedSeqs[source]
        /\ noopTerm[source][seq] > 0}

D1VisibleDocValue(writeSet) ==
    [doc \in Docs |->
        LET writeId == LatestWriteForDoc(writeSet, doc)
        IN IF writeId = NoWrite \/ writeKind[writeId] = "Delete"
           THEN NoWrite
           ELSE writeId]

D1InstallRecoverySnapshot(target) ==
    LET source == sessionSource[target]
        snapshot == sessionSnapshot[target]
        boundary == sessionBoundary[target]
        sequences == D1SnapshotSequences(source, snapshot, boundary)
        snapshotDocValue == RebuiltDocValue(snapshot)
        snapshotDocSeqNext == D1DocSeqNextFor(snapshot)
        snapshotTombstoneSeqNext == D1TombstoneSeqNextFor(snapshot)
        snapshotProcessedTerm ==
            [seq \in D1Seqs |->
                LET matching ==
                    {writeId \in snapshot : writeSeq[writeId] = seq}
                IN IF matching = {}
                   THEN noopTerm[source][seq]
                   ELSE writeTerm[CHOOSE writeId \in matching : TRUE]]
    IN
    /\ D1Fixed
    /\ InstallSnapshot(target)
    /\ walOrder' = [walOrder EXCEPT ![target] = <<>>]
    /\ noopTerm' =
          [noopTerm EXCEPT
              ![target] =
                  [seq \in D1Seqs |->
                      IF seq \in sequences
                      THEN noopTerm[source][seq]
                      ELSE 0]]
    /\ processedSeqs' = [processedSeqs EXCEPT ![target] = sequences]
    /\ processedNext' =
          [processedNext EXCEPT ![target] = ContiguousNext(sequences)]
    /\ persistedSeqs' = [persistedSeqs EXCEPT ![target] = sequences]
    /\ persistedNext' =
          [persistedNext EXCEPT ![target] = ContiguousNext(sequences)]
    /\ maxSeqNext' = [maxSeqNext EXCEPT ![target] = boundary]
    /\ persistedProcessedNext' =
          [persistedProcessedNext EXCEPT
              ![target] = ContiguousNext(sequences)]
    /\ persistedCommittedNext' =
          [persistedCommittedNext EXCEPT
              ![target] = ContiguousNext(sequences)]
    /\ persistedMaxSeqNext' =
          [persistedMaxSeqNext EXCEPT ![target] = boundary]
    /\ d1FenceTerm' =
          [d1FenceTerm EXCEPT ![target] = sessionTerm[target]]
    /\ fenceMaxSeqNext' =
          [fenceMaxSeqNext EXCEPT ![target] = boundary]
    /\ processedTerm' =
          [processedTerm EXCEPT ![target] = snapshotProcessedTerm]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![target] = snapshotDocSeqNext]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![target] = snapshotTombstoneSeqNext]
    /\ tombstoneOld' = [tombstoneOld EXCEPT ![target] = {}]
    /\ persistedOps' = [persistedOps EXCEPT ![target] = snapshot]
    /\ persistedDocValue' =
          [persistedDocValue EXCEPT ![target] = snapshotDocValue]
    /\ persistedDocSeqNext' =
          [persistedDocSeqNext EXCEPT
              ![target] = snapshotDocSeqNext]
    /\ persistedTombstoneSeqNext' =
          [persistedTombstoneSeqNext EXCEPT
              ![target] = snapshotTombstoneSeqNext]
    /\ replaying' = [replaying EXCEPT ![target] = FALSE]
    /\ replayPos' = [replayPos EXCEPT ![target] = 1]
    /\ replayBoundary' = [replayBoundary EXCEPT ![target] = boundary]
    /\ replayComplete' = [replayComplete EXCEPT ![target] = TRUE]
    /\ UNCHANGED
          <<replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

D1FixedRecoveryApply(target) ==
    LET writeId == sessionFetched[target]
        sequenceNumber == writeSeq[writeId]
        doc == writeDoc[writeId]
        nextProcessed == processedSeqs[target] \cup {sequenceNumber}
        nextPersisted ==
            IF D1RequestDurability
            THEN persistedSeqs[target] \cup {sequenceNumber}
            ELSE persistedSeqs[target]
        newer == sequenceNumber + 1 > docSeqNext[target][doc]
    IN
    /\ D1Fixed
    /\ sessionPhase[target] \in {"CatchingUp", "Finalizing"}
    /\ writeId \in WriteIds
    /\ alive[target]
    /\ copyMode[target] = "Recovering"
    /\ sequenceNumber >= sessionCursor[target]
    /\ sequenceNumber \notin processedSeqs[target]
    /\ ops' = [ops EXCEPT ![target] = @ \cup {writeId}]
    /\ durableOps' =
          [durableOps EXCEPT ![target] = @ \cup {writeId}]
    /\ docValue' =
          IF newer
          THEN [docValue EXCEPT ![target][doc] = writeId]
          ELSE docValue
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![target] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ walOrder' = [walOrder EXCEPT ![target] = Append(@, writeId)]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![target] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![target] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![target] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![target] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![target] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![target][sequenceNumber] = writeTerm[writeId]]
    /\ docSeqNext' =
          IF newer
          THEN [docSeqNext EXCEPT
                    ![target][doc] = sequenceNumber + 1]
          ELSE docSeqNext
    /\ tombstoneSeqNext' =
          IF newer
          THEN [tombstoneSeqNext EXCEPT
                    ![target][doc] =
                        IF writeKind[writeId] = "Delete"
                        THEN sequenceNumber + 1
                        ELSE 0]
          ELSE tombstoneSeqNext
    /\ tombstoneOld' =
          IF newer
          THEN [tombstoneOld EXCEPT ![target] = @ \ {doc}]
          ELSE tombstoneOld
    /\ sessionCursor' =
          [sessionCursor EXCEPT ![target] = sequenceNumber + 1]
    /\ sessionFetched' =
          [sessionFetched EXCEPT ![target] = NoFetched]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayPos, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, d1FenceTerm, fenceMaxSeqNext,
            noopTerm,
            recoveryAttempts, sessionPhase,
            sessionSource, sessionSourceEpoch, sessionTerm,
            sessionAllocation, sessionBoundary, sessionHead, sessionSnapshot,
            sessionFinalizePreparing, sessionMarkSubmitted,
            sessionSettlementRunning, sessionBumpSubmitted, pendingPrimary,
            pendingTerm, pendingAllocation, authoritativeWipeSafe, FaultVars>>

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
            walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1PersistBoundary(node, boundary, capturedPersisted, capturedMax, capturedOps,
                  capturedDocValue, capturedDocSeqNext,
                  capturedTombstoneSeqNext, nextCommitDone) ==
    /\ node \in Nodes
    /\ alive[node]
    /\ persistedProcessedNext' =
          [persistedProcessedNext EXCEPT ![node] = boundary]
    /\ persistedCommittedNext' =
          [persistedCommittedNext EXCEPT ![node] = capturedPersisted]
    /\ persistedMaxSeqNext' =
          [persistedMaxSeqNext EXCEPT ![node] = capturedMax]
    /\ persistedOps' =
          [persistedOps EXCEPT ![node] = capturedOps]
    /\ persistedDocValue' =
          [persistedDocValue EXCEPT ![node] = capturedDocValue]
    /\ persistedDocSeqNext' =
          [persistedDocSeqNext EXCEPT ![node] = capturedDocSeqNext]
    /\ persistedTombstoneSeqNext' =
          [persistedTombstoneSeqNext EXCEPT
              ![node] = capturedTombstoneSeqNext]
    /\ committed' = [committed EXCEPT ![node] = boundary]
    /\ commitDone' = nextCommitDone
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, noopTerm, processedSeqs, processedNext,
            persistedSeqs, persistedNext, maxSeqNext,
            docSeqNext, tombstoneSeqNext, tombstoneOld, replaying, replayPos,
            replayBoundary, replayComplete, replaySafe, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, crashDone,
            duplicateSent, tombstonePruneSafe, pruneDone, PeerRecoveryVars,
            FaultVars>>

D1CommitCopy(node) ==
    LET boundary ==
            IF D1Fixed
            THEN processedNext[node]
            ELSE maxSeqNext[node]
    IN
    /\ ~commitDone
    /\ D1PersistBoundary(
           node,
           boundary,
           persistedNext[node],
           maxSeqNext[node],
           ops[node],
           docValue[node],
           docSeqNext[node],
           tombstoneSeqNext[node],
           TRUE)

D1CommitReplica ==
    D1CommitCopy(ReplicaNode)

D1AgeTombstone ==
    /\ D1Fixed
    /\ tombstoneSeqNext[ReplicaNode][DocX] > 0
    /\ DocX \notin tombstoneOld[ReplicaNode]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![ReplicaNode] = @ \cup {DocX}]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, docSeqNext,
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
          <<vars, walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, docSeqNext,
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
            walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            duplicateSent, tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, PeerRecoveryVars, FaultVars>>

D1CrashCopy(node) ==
    /\ node \in Nodes
    /\ Crash(node)
    /\ UNCHANGED <<ApplySafetyVars, D1Vars>>

D1RestoredDocValues(node) ==
    [doc \in Docs |->
        LET writeId == persistedDocValue[node][doc]
        IN IF writeId # NoWrite
              /\ writeKind[writeId] = "Delete"
           THEN NoWrite
           ELSE writeId]

D1RestoredDocSeqNext(node) ==
    [doc \in Docs |->
        LET writeId == persistedDocValue[node][doc]
        IN IF writeId # NoWrite
              /\ writeKind[writeId] = "Delete"
           THEN 0
           ELSE persistedDocSeqNext[node][doc]]

D1RestartCopy(node) ==
    LET boundary == persistedProcessedNext[node]
        persistedBoundary == persistedCommittedNext[node]
    IN
    /\ node \in Nodes
    /\ ~alive[node]
    /\ alive' = [alive EXCEPT ![node] = TRUE]
    /\ raftConnected' =
          [raftConnected EXCEPT ![node] = TRUE]
    /\ epoch' = [epoch EXCEPT ![node] = @ + 1]
    /\ ops' = [ops EXCEPT ![node] = persistedOps[node]]
    /\ durableOps' =
          [durableOps EXCEPT ![node] = persistedOps[node]]
    /\ docValue' =
          [docValue EXCEPT ![node] = D1RestoredDocValues(node)]
    /\ nextSeq' =
          [nextSeq EXCEPT ![node] = persistedMaxSeqNext[node]]
    /\ committed' = [committed EXCEPT ![node] = boundary]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = ProcessedPrefix(boundary)]
    /\ processedNext' =
          [processedNext EXCEPT ![node] = boundary]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT
              ![node] = ProcessedPrefix(persistedBoundary)]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = persistedBoundary]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT ![node] = persistedMaxSeqNext[node]]
    /\ d1FenceTerm' =
          [d1FenceTerm EXCEPT ![node] = durableReplicaFence[node]]
    /\ fenceMaxSeqNext' = fenceMaxSeqNext
    /\ processedTerm' =
          [processedTerm EXCEPT ![node] = [seq \in D1Seqs |-> 0]]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![node] = D1RestoredDocSeqNext(node)]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT ![node] = [doc \in Docs |-> 0]]
    /\ tombstoneOld' = [tombstoneOld EXCEPT ![node] = {}]
    /\ replaying' = [replaying EXCEPT ![node] = TRUE]
    /\ replayPos' = [replayPos EXCEPT ![node] = 1]
    /\ replayBoundary' =
          [replayBoundary EXCEPT ![node] = boundary]
    /\ replayComplete' =
          [replayComplete EXCEPT ![node] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, noopTerm, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaySafe,
            commitDone, crashDone, duplicateSent, tombstonePruneSafe,
            pruneDone, PeerRecoveryVars, FaultVars>>

\* Writer rebuilds during activation, refresh, flush, or a later apply replay
\* the retained WAL without restarting the process or changing its epoch.
D1StartInPlaceReplay(node) ==
    LET boundary == persistedProcessedNext[node]
        persistedBoundary == persistedCommittedNext[node]
    IN
    /\ node \in Nodes
    /\ alive[node]
    /\ copyExists[node]
    /\ ~replaying[node]
    /\ ops' = [ops EXCEPT ![node] = persistedOps[node]]
    /\ durableOps' =
          [durableOps EXCEPT ![node] = persistedOps[node]]
    /\ docValue' =
          [docValue EXCEPT ![node] = D1RestoredDocValues(node)]
    /\ nextSeq' =
          [nextSeq EXCEPT ![node] = persistedMaxSeqNext[node]]
    /\ committed' = [committed EXCEPT ![node] = boundary]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = ProcessedPrefix(boundary)]
    /\ processedNext' =
          [processedNext EXCEPT ![node] = boundary]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT
              ![node] = ProcessedPrefix(persistedBoundary)]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = persistedBoundary]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT ![node] = persistedMaxSeqNext[node]]
    /\ processedTerm' =
          [processedTerm EXCEPT ![node] = [seq \in D1Seqs |-> 0]]
    /\ docSeqNext' =
          [docSeqNext EXCEPT ![node] = D1RestoredDocSeqNext(node)]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT ![node] = [doc \in Docs |-> 0]]
    /\ tombstoneOld' = [tombstoneOld EXCEPT ![node] = {}]
    /\ replaying' = [replaying EXCEPT ![node] = TRUE]
    /\ replayPos' = [replayPos EXCEPT ![node] = 1]
    /\ replayBoundary' =
          [replayBoundary EXCEPT ![node] = boundary]
    /\ replayComplete' =
          [replayComplete EXCEPT ![node] = FALSE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder, noopTerm,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, d1FenceTerm,
            fenceMaxSeqNext, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

D1RestartReplica ==
    /\ crashDone
    /\ D1RestartCopy(ReplicaNode)

\* HotTranslog::open sees no records from deleted generations. The trace does
\* not emit replay_entry for deleted generations or entries already covered by
\* the committed processed boundary. One hidden action advances over either
\* kind of unobserved prefix.
D1SkipTruncatedReplayPrefix(node, nextPosition) ==
    /\ node \in Nodes
    /\ replaying[node]
    /\ replayPos[node] <= Len(walOrder[node])
    /\ nextPosition \in (replayPos[node] + 1)..(Len(walOrder[node]) + 1)
    /\ \A position \in replayPos[node]..(nextPosition - 1) :
           \/ WalEntrySeq(walOrder[node][position]) < truncBelow[node]
              \/ WalEntrySeq(walOrder[node][position]) < replayBoundary[node]
    /\ replayPos' = [replayPos EXCEPT ![node] = nextPosition]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext,
            persistedSeqs, persistedNext, maxSeqNext,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, d1FenceTerm, fenceMaxSeqNext, processedTerm,
            docSeqNext, tombstoneSeqNext, tombstoneOld, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

D1ReplaySkipAt(node) ==
    LET position == replayPos[node]
        entry == walOrder[node][position]
    IN
    /\ node \in Nodes
    /\ replaying[node]
    /\ position <= Len(walOrder[node])
    /\ WalEntrySeq(entry) < replayBoundary[node]
    /\ replayPos' = [replayPos EXCEPT ![node] = @ + 1]
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayBoundary, replayComplete, replaySafe, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, commitDone, crashDone,
            duplicateSent, tombstonePruneSafe, pruneDone>>

D1ReplaySkip ==
    D1ReplaySkipAt(ReplicaNode)

D1HistoricalReplayApplyAt(node) ==
    LET position == replayPos[node]
        entry == walOrder[node][position]
        writeId == entry
        sequenceNumber == writeSeq[writeId]
        doc == writeDoc[writeId]
        nextProcessed == processedSeqs[node] \cup {sequenceNumber}
        nextPersisted == persistedSeqs[node] \cup {sequenceNumber}
    IN
    /\ D1Historical
    /\ node \in Nodes
    /\ replaying[node]
    /\ position <= Len(walOrder[node])
    /\ entry \in WriteIds
    /\ sequenceNumber >= replayBoundary[node]
    /\ ops' = [ops EXCEPT ![node] = @ \cup {writeId}]
    /\ durableOps' =
          [durableOps EXCEPT ![node] = @ \cup {writeId}]
    /\ docValue' = [docValue EXCEPT ![node][doc] = writeId]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![node] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![node] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![node][sequenceNumber] = writeTerm[writeId]]
    /\ docSeqNext' =
          [docSeqNext EXCEPT
              ![node][doc] = sequenceNumber + 1]
    /\ tombstoneSeqNext' =
          [tombstoneSeqNext EXCEPT
              ![node][doc] =
                  IF writeKind[writeId] = "Delete"
                  THEN sequenceNumber + 1
                  ELSE 0]
    /\ tombstoneOld' =
          [tombstoneOld EXCEPT ![node] = @ \ {doc}]
    /\ replayPos' = [replayPos EXCEPT ![node] = @ + 1]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder, noopTerm,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, PeerRecoveryVars, FaultVars>>

D1HistoricalReplayApply ==
    D1HistoricalReplayApplyAt(ReplicaNode)

D1FixedReplayApplyAt(node) ==
    LET position == replayPos[node]
        entry == walOrder[node][position]
        writeId == entry
        sequenceNumber == writeSeq[writeId]
        doc == writeDoc[writeId]
        redelivery == sequenceNumber \in processedSeqs[node]
        nextProcessed == processedSeqs[node] \cup {sequenceNumber}
        nextPersisted == persistedSeqs[node] \cup {sequenceNumber}
        newer == sequenceNumber + 1 > docSeqNext[node][doc]
    IN
    /\ D1Fixed
    /\ node \in Nodes
    /\ replaying[node]
    /\ position <= Len(walOrder[node])
    /\ entry \in WriteIds
    /\ sequenceNumber >= replayBoundary[node]
    /\ ops' =
          IF redelivery
          THEN ops
          ELSE [ops EXCEPT ![node] = @ \cup {writeId}]
    /\ durableOps' =
          IF redelivery
          THEN durableOps
          ELSE [durableOps EXCEPT ![node] = @ \cup {writeId}]
    /\ docValue' =
          IF ~redelivery /\ newer
          THEN [docValue EXCEPT ![node][doc] = writeId]
          ELSE docValue
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedSeqs' =
          IF redelivery
          THEN processedSeqs
          ELSE [processedSeqs EXCEPT ![node] = nextProcessed]
    /\ processedNext' =
          IF redelivery
          THEN processedNext
          ELSE [processedNext EXCEPT
                    ![node] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT ![node] = ContiguousNext(nextPersisted)]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          IF redelivery
          THEN processedTerm
          ELSE [processedTerm EXCEPT
                    ![node][sequenceNumber] = writeTerm[writeId]]
    /\ docSeqNext' =
          IF ~redelivery /\ newer
          THEN [docSeqNext EXCEPT
                    ![node][doc] = sequenceNumber + 1]
          ELSE docSeqNext
    /\ tombstoneSeqNext' =
          IF ~redelivery /\ newer
          THEN [tombstoneSeqNext EXCEPT
                    ![node][doc] =
                        IF writeKind[writeId] = "Delete"
                        THEN sequenceNumber + 1
                        ELSE 0]
          ELSE tombstoneSeqNext
    /\ tombstoneOld' =
          IF ~redelivery /\ newer
          THEN [tombstoneOld EXCEPT ![node] = @ \ {doc}]
          ELSE tombstoneOld
    /\ replayPos' = [replayPos EXCEPT ![node] = @ + 1]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, committed, truncBelow, pins, copyExists,
            copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            copyMode, installMarker, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            ApplySafetyVars, termMonotonic, walOrder, noopTerm,
            persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, persistedOps,
            persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, PeerRecoveryVars, FaultVars>>

D1FixedReplayApply ==
    D1FixedReplayApplyAt(ReplicaNode)

\* HotEngine::writer_state_with_replay re-applies a retained promotion NoOp
\* after restart without changing logical document state.
D1ReplayNoOpAt(node) ==
    LET position == replayPos[node]
        entry == walOrder[node][position]
        sequenceNumber == WalEntrySeq(entry)
        nextProcessed == processedSeqs[node] \cup {sequenceNumber}
        nextPersisted == persistedSeqs[node] \cup {sequenceNumber}
    IN
    /\ D1Fixed
    /\ node \in Nodes
    /\ replaying[node]
    /\ position <= Len(walOrder[node])
    /\ IsNoOpEntry(entry)
    /\ sequenceNumber >= replayBoundary[node]
    /\ processedSeqs' =
          [processedSeqs EXCEPT ![node] = nextProcessed]
    /\ processedNext' =
          [processedNext EXCEPT
              ![node] = ContiguousNext(nextProcessed)]
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT ![node] = nextPersisted]
    /\ persistedNext' =
          [persistedNext EXCEPT
              ![node] = ContiguousNext(nextPersisted)]
    /\ nextSeq' =
          [nextSeq EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![node] =
                  IF @ < sequenceNumber + 1 THEN sequenceNumber + 1 ELSE @]
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![node][sequenceNumber] = WalEntryTerm(node, entry)]
    /\ replayPos' = [replayPos EXCEPT ![node] = @ + 1]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, walOrder, noopTerm, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, docSeqNext, tombstoneSeqNext, tombstoneOld,
            persistedOps, persistedDocValue, persistedDocSeqNext,
            persistedTombstoneSeqNext, replaying, replayBoundary,
            replayComplete, replaySafe, commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

D1FinishReplayAt(node) ==
    LET boundary == replayBoundary[node]
        covered ==
            \A position \in 1..Len(walOrder[node]) :
                LET entry == walOrder[node][position]
                IN WalEntrySeq(entry) < boundary
                   \/ WalEntrySeq(entry) \in processedSeqs[node]
    IN
    /\ node \in Nodes
    /\ replaying[node]
    /\ replayPos[node] > Len(walOrder[node])
    /\ replaying' = [replaying EXCEPT ![node] = FALSE]
    /\ replayComplete' =
          [replayComplete EXCEPT ![node] = TRUE]
    /\ replaySafe' = replaySafe /\ covered
    /\ UNCHANGED
          <<vars, walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replayPos,
            replayBoundary, d1FenceTerm, fenceMaxSeqNext, processedTerm,
            commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone>>

D1FinishReplay ==
    D1FinishReplayAt(ReplicaNode)

D1FailReplayAt(node) ==
    /\ node \in Nodes
    /\ replaying[node]
    /\ replaying' = [replaying EXCEPT ![node] = FALSE]
    /\ replayComplete' = [replayComplete EXCEPT ![node] = FALSE]
    /\ replaySafe' = FALSE
    /\ copyMode' = [copyMode EXCEPT ![node] = "InstallMarker"]
    /\ installMarker' = [installMarker EXCEPT ![node] = TRUE]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, messages, sharedHolders,
            exclusiveHolder, acked, failed, promotionSafe, admissionSafe,
            ackMembershipSafe, ApplySafetyVars, termMonotonic, walOrder,
            noopTerm,
            processedSeqs, processedNext, persistedSeqs, persistedNext,
            maxSeqNext, persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replayPos,
            replayBoundary, d1FenceTerm, fenceMaxSeqNext, processedTerm,
            commitDone, crashDone, duplicateSent,
            tombstonePruneSafe, pruneDone, PeerRecoveryVars, FaultVars>>

\* HotTranslog truncation advances the durable bound. Generation deletion may
\* remove every older entry or leave some files for a later cleanup attempt.
D1RecordTruncation(node, boundary) ==
    /\ node \in Nodes
    /\ boundary <= persistedProcessedNext[node]
    /\ truncBelow' =
          [truncBelow EXCEPT
              ![node] = IF @ < boundary THEN boundary ELSE @]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, pins, copyExists, copyAllocation, copyUuid, replicaFence,
            durableReplicaFence, copyMode, installMarker, messages,
            sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
            admissionSafe,             ackMembershipSafe, ApplySafetyVars, termMonotonic, walOrder,
            noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, docSeqNext, tombstoneSeqNext,
            tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, pruneDone,
            PeerRecoveryVars, FaultVars>>

D1ProcessedCheckpointGapAware ==
    \A node \in Nodes :
        processedNext[node] = ContiguousNext(processedSeqs[node])

D1PersistedCheckpointGapAware ==
    \A node \in Nodes :
        /\ persistedSeqs[node] \subseteq processedSeqs[node]
        /\ persistedNext[node] = ContiguousNext(persistedSeqs[node])
        /\ persistedNext[node] <= processedNext[node]

D1WalHasNoDuplicateSeq ==
    \A node \in Nodes :
      \A first \in 1..Len(walOrder[node]) :
        \A second \in 1..Len(walOrder[node]) :
            WalEntrySeq(walOrder[node][first])
            = WalEntrySeq(walOrder[node][second])
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

D1NoAcknowledgedDeleteResurrection ==
    \A doc \in Docs :
      LET latest == LatestAckedWriteForDoc(doc)
      IN IF latest = NoWrite \/ writeKind[latest] # "Delete"
         THEN TRUE
         ELSE \A node \in AvailableInSyncCopies :
                LET current == docValue[node][doc]
                IN /\ current # NoWrite
                   /\ \/ writeSeq[current] > writeSeq[latest]
                      \/ /\ writeSeq[current] = writeSeq[latest]
                         /\ writeKind[current] = "Delete"

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
         IF replica \in AvailableInSyncCopies
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
    \/ \E nextPosition \in 1..(2 * MaxWrites + 1) :
           D1SkipTruncatedReplayPrefix(ReplicaNode, nextPosition)
    \/ D1ReplaySkip
    \/ D1HistoricalReplayApply
    \/ D1FixedReplayApply
    \/ D1ReplayNoOpAt(ReplicaNode)
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
            {entry \in {walOrder[ReplicaNode][position] :
                          position \in 1..Len(walOrder[ReplicaNode])} :
                WalEntrySeq(entry) >= boundary}
    IN
    /\ D1Fixed
    /\ commitDone
    /\ ~pruneDone
    /\ boundary = 1
    /\ Cardinality(retained) = 1
    /\ walOrder' =
          [walOrder EXCEPT
              ![ReplicaNode] =
                  <<CHOOSE entry \in retained : TRUE>>]
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
            termMonotonic, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, duplicateSent, tombstonePruneSafe, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, PeerRecoveryVars, FaultVars>>

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
            noopTerm,
            processedSeqs, processedNext, persistedSeqs, persistedNext,
            maxSeqNext, persistedProcessedNext, persistedCommittedNext,
            persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            duplicateSent, tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, PeerRecoveryVars, FaultVars>>

D1RestartWithoutDurableTombstone ==
    LET boundary == persistedProcessedNext[ReplicaNode]
        persistedBoundary == persistedCommittedNext[ReplicaNode]
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
    /\ persistedSeqs' =
          [persistedSeqs EXCEPT
              ![ReplicaNode] = ProcessedPrefix(persistedBoundary)]
    /\ persistedNext' =
          [persistedNext EXCEPT ![ReplicaNode] = persistedBoundary]
    /\ maxSeqNext' =
          [maxSeqNext EXCEPT
              ![ReplicaNode] = persistedMaxSeqNext[ReplicaNode]]
    /\ d1FenceTerm' =
          [d1FenceTerm EXCEPT
              ![ReplicaNode] = durableReplicaFence[ReplicaNode]]
    /\ fenceMaxSeqNext' = fenceMaxSeqNext
    /\ processedTerm' =
          [processedTerm EXCEPT
              ![ReplicaNode] = [seq \in D1Seqs |-> 0]]
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
            termMonotonic, walOrder, noopTerm, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, persistedOps,
            persistedDocValue,
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
            walOrder, noopTerm, processedSeqs, processedNext, persistedSeqs,
            persistedNext, maxSeqNext, persistedProcessedNext,
            persistedCommittedNext, persistedMaxSeqNext, docSeqNext,
            tombstoneSeqNext, tombstoneOld, persistedOps, persistedDocValue,
            persistedDocSeqNext, persistedTombstoneSeqNext, replaying,
            replayPos, replayBoundary, replayComplete, replaySafe, commitDone,
            crashDone, tombstonePruneSafe, pruneDone, d1FenceTerm,
            fenceMaxSeqNext, processedTerm, PeerRecoveryVars, FaultVars>>

D1NoDurableTombstoneAtRestart ==
    /\ replaying[ReplicaNode]
    /\ replayPos[ReplicaNode] = 1
    => tombstoneSeqNext[ReplicaNode] = [doc \in Docs |-> 0]

D1WalTruncatedToBoundary ==
    pruneDone =>
        /\ truncBelow[ReplicaNode] = persistedProcessedNext[ReplicaNode]
        /\ \A position \in 1..Len(walOrder[ReplicaNode]) :
               WalEntrySeq(walOrder[ReplicaNode][position])
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
