--------------------------- MODULE MC_D2_WriteAck ---------------------------
\* Proposed D2/D4/D5/D14 control policy, not implemented Rust behavior.
\* Reuses D1 matching, WAL, operation durability, checkpoints, and Raft actions.
\* Copy I/O is an abstract post-WAL permit failure; torn bytes are not modeled.

EXTENDS MC_D1_SeqNoApply

CONSTANTS AckPolicy, AckScenario, MinimumDurableCopies, MaxReplicaFailures

VARIABLES
    clientOutcome,
    allocationAtStart,
    exclusionDebt,
    confirmedRemovals,
    localFenced,
    ackProof,
    ackAboveGap,
    selfFenceSafe,
    metadataFaultActive,
    replicaFailureCount,
    postWalFailureSeen,
    removalRejectedSeen

D2Vars ==
    <<clientOutcome, allocationAtStart, exclusionDebt, confirmedRemovals,
      localFenced, ackProof, ackAboveGap, selfFenceSafe, metadataFaultActive,
      replicaFailureCount, postWalFailureSeen, removalRejectedSeen>>

d2vars == <<d1vars, D2Vars>>

\* Metadata connectivity changes must not frame raftConnected as data.
D2DataVars ==
    <<routing, alive, epoch, activated, activationPending, nextWrite,
      writeStatus, writeDoc, writeKind, writeTarget, writePrimary, writeEpoch,
      writeSeq, writeTerm, writeRequired, writeWait, ops, durableOps, docValue,
      nextSeq, committed, truncBelow, pins, copyExists, copyAllocation,
      copyUuid, replicaFence, durableReplicaFence, copyMode, installMarker,
      messages, sharedHolders, exclusiveHolder, acked, failed, promotionSafe,
      admissionSafe, ackMembershipSafe, termMonotonic, ApplySafetyVars,
      PeerRecoveryVars, D1Vars>>

D2Policies ==
    {"Current", "Proposed", "EarlyExclusion", "NoSticky",
     "NoSelfFence", "IgnoreMinimum", "Prefix"}

D2Scenarios == {"Safety", "Debt", "Stale", "Gap"}
D2Outcomes ==
    {"Pending", "Acknowledged", "Rejected", "NotExecuted", "Indeterminate"}

D2EmptyProof ==
    [copies |-> 0, committed |-> TRUE, debtClear |-> TRUE,
     durable |-> TRUE, primaryOpen |-> TRUE, metadataDown |-> FALSE]

D2Removal(primaryNode, term, target, allocation) ==
    RaftCommand("FailShardCopy", primaryNode, target, primaryNode, term,
                NoNode, {}, 0, allocation, EmptyAllocations)

D2PossibleRemovals ==
    {D2Removal(PrimaryNode, term, target, 1) :
        term \in 1..MaxTerm, target \in Nodes \ {PrimaryNode}}

D2DebtFor(writeId) ==
    {command \in exclusionDebt :
        /\ command.actor = writePrimary[writeId]
        /\ command.expectedTerm = writeTerm[writeId]}

D2AdmissionAllowed(node) ==
    /\ (AckPolicy = "NoSelfFence" \/ ~localFenced[node])
    /\ (AckPolicy \in {"Current", "NoSticky", "EarlyExclusion"} \/
          {command \in exclusionDebt :
              /\ command.actor = node
              /\ command.expectedTerm = views[node].term} = {})
    /\ (AckPolicy \in {"Current", "IgnoreMinimum"} \/
          Cardinality({node} \cup views[node].inSync) >= MinimumDurableCopies)

D2ExcludedBy(writeId, commands) ==
    {target \in writeRequired[writeId] :
        \E command \in commands :
            /\ command.actor = writePrimary[writeId]
            /\ command.expectedTerm = writeTerm[writeId]
            /\ command.target = target
            /\ command.expectedAllocation = allocationAtStart[writeId][target]}

D2Excluded(writeId) ==
    D2ExcludedBy(
        writeId,
        IF AckPolicy = "EarlyExclusion"
        THEN confirmedRemovals \cup exclusionDebt
        ELSE confirmedRemovals)

D2CertificateCopies(writeId) ==
    {writePrimary[writeId]} \cup (writeRequired[writeId] \ D2Excluded(writeId))

D2OperationDurable(writeId) ==
    \A node \in D2CertificateCopies(writeId) :
        /\ writeId \in ops[node]
        /\ writeId \in durableOps[node]
        /\ writeSeq[writeId] \in processedSeqs[node]
        /\ writeSeq[writeId] \in persistedSeqs[node]
        /\ ~localFenced[node]

D2RemovalResult(command, accepted) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command = command
        /\ raftLog[position].accepted = accepted

D2KnownResult(command) ==
    D2RemovalResult(command, TRUE) \/ D2RemovalResult(command, FALSE)

D2RemovalAccepted(command) ==
    /\ command.actor = routing.primary
    /\ command.expectedPrimary = routing.primary
    /\ command.expectedTerm = routing.term
    /\ command.target \in routing.inSync
    /\ FailShardCopyAccepted(routing, command)

D2Stable(action) ==
    /\ action
    /\ UNCHANGED D2Vars

D2CoreFrame ==
    UNCHANGED <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
               ApplySafetyVars, D1Vars, PeerRecoveryVars, FaultVars>>

D2Init ==
    /\ Init
    /\ Cardinality(Nodes) = 3
    /\ PrimaryNode \in Nodes
    /\ ReplicaNode \in Nodes \ {PrimaryNode}
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ routing.inSync = Nodes \ {PrimaryNode}
    /\ DocX \in Docs
    /\ DocY \in Docs \ {DocX}
    /\ AckPolicy \in D2Policies
    /\ AckScenario \in D2Scenarios
    /\ MinimumDurableCopies \in {1, 2}
    /\ FaultMode = "D1Fixed"
    /\ D1DataInit
    /\ clientOutcome = [writeId \in WriteIds |-> "Pending"]
    /\ allocationAtStart = [writeId \in WriteIds |-> EmptyAllocations]
    /\ exclusionDebt = {}
    /\ confirmedRemovals = {}
    /\ localFenced = [node \in Nodes |-> FALSE]
    /\ ackProof = [writeId \in WriteIds |-> D2EmptyProof]
    /\ ackAboveGap = FALSE
    /\ selfFenceSafe = TRUE
    /\ metadataFaultActive = FALSE
    /\ replicaFailureCount = 0
    /\ postWalFailureSeen = FALSE
    /\ removalRejectedSeen = FALSE

D2Submit ==
    \E doc \in Docs, kind \in WriteKinds :
        /\ IF AckScenario = "Gap"
           THEN /\ doc = DocX
                /\ kind = IF nextWrite = 1 THEN "Put" ELSE "Delete"
           ELSE /\ doc = IF nextWrite = 1 THEN DocX ELSE DocY
                /\ kind = "Put"
        /\ (AckScenario = "Stale" /\ nextWrite > 1 => removalRejectedSeen)
        /\ D2Stable(D1ClientWriteFrom(PrimaryNode, doc, kind))

D2Accept(writeId) ==
    LET primaryNode == writeTarget[writeId]
    IN
    /\ writeStatus[writeId] = "Routed"
    /\ primaryNode \in Nodes
    /\ D2AdmissionAllowed(primaryNode)
    /\ D1PrimaryAccept(writeId)
    /\ allocationAtStart' =
          [allocationAtStart EXCEPT ![writeId] = views[primaryNode].allocations]
    /\ selfFenceSafe' = (selfFenceSafe /\ ~localFenced[primaryNode])
    /\ UNCHANGED
          <<clientOutcome, exclusionDebt, confirmedRemovals, localFenced,
            ackProof, ackAboveGap, metadataFaultActive, replicaFailureCount,
            postWalFailureSeen, removalRejectedSeen>>

D2RejectBeforeWal(writeId) ==
    /\ AckScenario = "Safety"
    /\ PrimaryVersionConflict(writeId)
    /\ D2CoreFrame
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Rejected"]
    /\ UNCHANGED
          <<allocationAtStart, exclusionDebt, confirmedRemovals, localFenced,
            ackProof, ackAboveGap, selfFenceSafe, metadataFaultActive,
            replicaFailureCount, postWalFailureSeen, removalRejectedSeen>>

D2NotExecuted(writeId) ==
    /\ writeStatus[writeId] = "Routed"
    /\ writeTarget[writeId] \in Nodes
    /\ ~D2AdmissionAllowed(writeTarget[writeId])
    /\ PrimaryVersionConflict(writeId)
    /\ D2CoreFrame
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "NotExecuted"]
    /\ UNCHANGED
          <<allocationAtStart, exclusionDebt, confirmedRemovals, localFenced,
            ackProof, ackAboveGap, selfFenceSafe, metadataFaultActive,
            replicaFailureCount, postWalFailureSeen, removalRejectedSeen>>

D2ReplicaStep ==
    \E message \in messages :
      /\ message.kind = "Replicate"
      /\ ~localFenced[message.to]
      /\ (AckScenario = "Gap" => message.write = 2)
      /\ D2Stable(
             D1FixedReplicaProcess(message) \/ D1FixedReplicaRedelivery(message))

D2AckReply ==
    \E message \in messages :
        /\ D2Stable(D1DeliverAck(message))

\* A timed-out request can miss the target entirely, or its ACK can be lost
\* after the target persisted it. Both cases create the same exclusion debt.
D2ReplicaFailure(message, storageFailure) ==
    LET writeId == message.write
        target == IF message.kind = "Replicate" THEN message.to ELSE message.from
        command ==
            D2Removal(writePrimary[writeId], writeTerm[writeId], target,
                      allocationAtStart[writeId][target])
    IN
    /\ AckScenario # "Gap"
    /\ message \in messages
    /\ message.kind \in {"Replicate", "ReplicaAck"}
    /\ writeStatus[writeId] = "Replicating"
    /\ target \in writeWait[writeId]
    /\ replicaFailureCount < MaxReplicaFailures
    /\ (AckScenario \in {"Debt", "Stale"} =>
          /\ writeId = 1
          /\ target = ReplicaNode
          /\ ~storageFailure)
    /\ (AckScenario = "Debt" => metadataFaultActive)
    /\ IF AckPolicy = "Current"
       THEN /\ PrimaryFail(writeId)
            /\ D2CoreFrame
            /\ clientOutcome' =
                  [clientOutcome EXCEPT ![writeId] = "Indeterminate"]
            /\ UNCHANGED <<exclusionDebt, localFenced>>
       ELSE /\ LoseMsg(message)
            /\ UNCHANGED <<D1Vars, ApplySafetyVars>>
            /\ exclusionDebt' = exclusionDebt \cup {command}
            /\ localFenced' =
                  IF storageFailure
                  THEN [localFenced EXCEPT ![target] = TRUE]
                  ELSE localFenced
            /\ UNCHANGED clientOutcome
    /\ replicaFailureCount' = replicaFailureCount + 1
    /\ UNCHANGED
          <<allocationAtStart, confirmedRemovals, ackProof, ackAboveGap,
            selfFenceSafe, metadataFaultActive, postWalFailureSeen,
            removalRejectedSeen>>

D2ProposeRemoval(command) ==
    /\ command \in exclusionDebt
    /\ CanReachRaft(command.actor)
    /\ ~D2KnownResult(command)
    /\ QueueRaft(command)
    /\ UNCHANGED <<ReplicationVars, ApplySafetyVars, PeerRecoveryVars,
                  FaultVars, D1Vars, D2Vars>>

\* The owning allocation CAS is reused. The proposed control policy adds
\* source-primary/term checks instead of changing current CommitRaft semantics.
D2CommitRemoval(command) ==
    /\ command \in exclusionDebt
    /\ command \in pendingRaft
    /\ IF D2RemovalAccepted(command)
       THEN D2Stable(
                CommitRaft(command) /\
                UNCHANGED <<copyAllocation, copyUuid, D1Vars,
                            PeerRecoveryVars, FaultVars>>)
       ELSE /\ Len(raftLog) < MaxRaftEntries
            /\ LeaderCanCommit
            /\ raftLog' = Append(raftLog, RaftEntry(command, FALSE, routing))
            /\ pendingRaft' = pendingRaft \ {command}
            /\ applied' =
                  [applied EXCEPT ![raftLeader] = Len(raftLog) + 1]
            /\ views' = [views EXCEPT ![raftLeader] = routing]
            /\ UNCHANGED
                  <<raftLeader, raftVoters, ReplicationVars, ApplySafetyVars,
                    PeerRecoveryVars, FaultVars, D1Vars, D2Vars>>

\* This is an observed committed result, not merely a queued proposal.
D2ObserveRemoval(command) ==
    /\ command \in exclusionDebt
    /\ alive[command.actor]
    /\ raftConnected[command.actor]
    /\ D2KnownResult(command)
    /\ exclusionDebt' = exclusionDebt \ {command}
    /\ IF D2RemovalResult(command, TRUE)
       THEN /\ confirmedRemovals' = confirmedRemovals \cup {command}
            /\ UNCHANGED <<localFenced, clientOutcome, removalRejectedSeen>>
       ELSE /\ localFenced' = [localFenced EXCEPT ![command.actor] = TRUE]
            /\ clientOutcome' =
                  [writeId \in WriteIds |->
                      IF writePrimary[writeId] = command.actor
                         /\ writeTerm[writeId] = command.expectedTerm
                         /\ clientOutcome[writeId] = "Pending"
                      THEN "Indeterminate"
                      ELSE clientOutcome[writeId]]
            /\ removalRejectedSeen' = TRUE
            /\ UNCHANGED confirmedRemovals
    /\ UNCHANGED
          <<d1vars, allocationAtStart, ackProof, ackAboveGap, selfFenceSafe,
            metadataFaultActive, replicaFailureCount, postWalFailureSeen>>

D2CanAcknowledge(writeId) ==
    /\ writeStatus[writeId] = "Replicating"
    /\ clientOutcome[writeId] = "Pending"
    /\ writePrimary[writeId] = routing.primary
    /\ writeTerm[writeId] = routing.term
    /\ ~localFenced[writePrimary[writeId]]
    /\ D2OperationDurable(writeId)
    /\ writeWait[writeId] \ D2Excluded(writeId) = {}
    /\ (AckPolicy \in {"EarlyExclusion", "NoSticky", "Current"} \/
          D2DebtFor(writeId) = {})
    /\ (AckPolicy = "IgnoreMinimum" \/
          Cardinality(D2CertificateCopies(writeId)) >= MinimumDurableCopies)
    /\ (AckPolicy = "Prefix" =>
          \A node \in D2CertificateCopies(writeId) :
              persistedNext[node] > writeSeq[writeId])

D2Acknowledge(writeId) ==
    LET primaryNode == writePrimary[writeId]
        copies == D2CertificateCopies(writeId)
        proof ==
            [copies |-> Cardinality(copies),
             committed |->
                 D2Excluded(writeId)
                   \subseteq D2ExcludedBy(writeId, confirmedRemovals),
             debtClear |-> D2DebtFor(writeId) = {},
             durable |-> D2OperationDurable(writeId),
             primaryOpen |-> ~localFenced[primaryNode],
             metadataDown |-> metadataFaultActive]
    IN
    /\ D2CanAcknowledge(writeId)
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Acked"]
    /\ acked' = acked \cup {writeId}
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![primaryNode] = @ \ {writeId}]
    /\ ackMembershipSafe' =
          (ackMembershipSafe
          /\ routing.inSync \subseteq writeRequired[writeId]
          /\ \A replica \in routing.inSync : writeId \in ops[replica])
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Acknowledged"]
    /\ ackProof' = [ackProof EXCEPT ![writeId] = proof]
    /\ ackAboveGap' =
          (ackAboveGap \/
            \E node \in copies : persistedNext[node] <= writeSeq[writeId])
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeDoc, writeKind, writeTarget,
            writePrimary, writeEpoch, writeSeq, writeTerm, writeRequired,
            writeWait, ops, durableOps, docValue, nextSeq, committed,
            truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, exclusiveHolder, failed, promotionSafe, admissionSafe,
            ApplySafetyVars, termMonotonic, PeerRecoveryVars, FaultVars, D1Vars,
            allocationAtStart, exclusionDebt, confirmedRemovals, localFenced,
            selfFenceSafe, metadataFaultActive, replicaFailureCount,
            postWalFailureSeen, removalRejectedSeen>>

D2Deadline(writeId) ==
    /\ AckScenario \in {"Safety", "Debt"}
    /\ (AckScenario = "Debt" =>
          /\ writeId = 1
          /\ writeWait[writeId] = {ReplicaNode}
          /\ metadataFaultActive
          /\ D2DebtFor(writeId) # {})
    /\ clientOutcome[writeId] = "Pending"
    /\ PrimaryFail(writeId)
    /\ D2CoreFrame
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Indeterminate"]
    /\ exclusionDebt' =
          IF AckPolicy = "Current"
          THEN exclusionDebt
          ELSE exclusionDebt \cup
               {D2Removal(writePrimary[writeId], writeTerm[writeId], target,
                          allocationAtStart[writeId][target]) :
                   target \in writeWait[writeId]}
    /\ UNCHANGED
          <<allocationAtStart, confirmedRemovals, localFenced, ackProof,
            ackAboveGap, selfFenceSafe, metadataFaultActive,
            replicaFailureCount, postWalFailureSeen, removalRejectedSeen>>

\* A lost coordinator response does not undo a completed durable operation.
D2LoseClientResponse(writeId) ==
    /\ AckScenario = "Safety"
    /\ clientOutcome[writeId] = "Acknowledged"
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Indeterminate"]
    /\ UNCHANGED
          <<d1vars, allocationAtStart, exclusionDebt, confirmedRemovals,
            localFenced, ackProof, ackAboveGap, selfFenceSafe,
            metadataFaultActive, replicaFailureCount, postWalFailureSeen,
            removalRejectedSeen>>

D2InvalidPermitReply(writeId) ==
    /\ writeStatus[writeId] = "Replicating"
    /\ clientOutcome[writeId] = "Pending"
    /\ \/ localFenced[writePrimary[writeId]]
       \/ writePrimary[writeId] # routing.primary
       \/ writeTerm[writeId] # routing.term
       \/ Cardinality(D2CertificateCopies(writeId)) < MinimumDurableCopies
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Indeterminate"]
    /\ UNCHANGED
          <<d1vars, allocationAtStart, exclusionDebt, confirmedRemovals,
            localFenced, ackProof, ackAboveGap, selfFenceSafe,
            metadataFaultActive, replicaFailureCount, postWalFailureSeen,
            removalRejectedSeen>>

\* Atomic D1 durability may already have happened. The policy must still
\* revoke the local write permit; no byte-level fsync failure proof is claimed.
D2PostWalFailure(writeId) ==
    LET primaryNode == writePrimary[writeId]
    IN
    /\ AckScenario = "Safety"
    /\ AckPolicy # "Current"
    /\ ~postWalFailureSeen
    /\ clientOutcome[writeId] = "Pending"
    /\ PrimaryFail(writeId)
    /\ D2CoreFrame
    /\ localFenced' = [localFenced EXCEPT ![primaryNode] = TRUE]
    /\ clientOutcome' = [clientOutcome EXCEPT ![writeId] = "Indeterminate"]
    /\ postWalFailureSeen' = TRUE
    /\ UNCHANGED
          <<allocationAtStart, exclusionDebt, confirmedRemovals, ackProof,
            ackAboveGap, selfFenceSafe, metadataFaultActive,
            replicaFailureCount, removalRejectedSeen>>

D2LoseMetadata ==
    /\ AckScenario \in {"Safety", "Debt"}
    /\ ~metadataFaultActive
    /\ partitionCount < MaxPartitions
    /\ raftConnected' = [node \in Nodes |-> FALSE]
    /\ raftLeader' = NoNode
    /\ partitionCount' = partitionCount + 1
    /\ metadataFaultActive' = TRUE
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters, D2DataVars,
            crashCount, diskLost, faultsStopped,
            lifecyclePhase, storageFaultInjected, clientOutcome,
            allocationAtStart, exclusionDebt, confirmedRemovals, localFenced,
            ackProof, ackAboveGap, selfFenceSafe, replicaFailureCount,
            postWalFailureSeen, removalRejectedSeen>>

D2RestoreMetadata ==
    /\ metadataFaultActive
    /\ raftConnected' = [node \in Nodes |-> TRUE]
    /\ raftLeader' = routing.primary
    /\ metadataFaultActive' = FALSE
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters, D2DataVars,
            FaultVars, clientOutcome,
            allocationAtStart, exclusionDebt, confirmedRemovals, localFenced,
            ackProof, ackAboveGap, selfFenceSafe, replicaFailureCount,
            postWalFailureSeen, removalRejectedSeen>>

D2PromotionCommand ==
    RaftCommand("FailShardCopy", PrimaryNode, PrimaryNode, NoNode, NoTerm,
                ReplicaNode, {}, 0, routing.allocations[PrimaryNode],
                EmptyAllocations)

D2ProposePromotion ==
    /\ AckScenario = "Stale"
    /\ routing.primary = PrimaryNode
    /\ raftLeader # PrimaryNode
    /\ exclusionDebt # {}
    /\ QueueRaft(D2PromotionCommand)
    /\ UNCHANGED <<ReplicationVars, ApplySafetyVars, PeerRecoveryVars,
                  FaultVars, D1Vars, D2Vars>>

D2CommitPromotion ==
    /\ AckScenario = "Stale"
    /\ D2Stable(
           CommitRaft(D2PromotionCommand) /\
           UNCHANGED <<copyAllocation, copyUuid, D1Vars,
                       PeerRecoveryVars, FaultVars>>)

D2OtherViews ==
    \E node \in Nodes \ {PrimaryNode} :
        /\ AckScenario = "Stale"
        /\ D2Stable(
               DeliverView(node) /\
               UNCHANGED <<copyAllocation, copyUuid, D1Vars,
                           PeerRecoveryVars, FaultVars>>)

D2MoveMetadataLeader ==
    /\ AckScenario = "Stale"
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ exclusionDebt # {}
    /\ raftLeader' = ReplicaNode
    /\ UNCHANGED
          <<raftLog, pendingRaft, applied, views, raftVoters, raftConnected,
            D2DataVars, FaultVars, D2Vars>>

D2Next ==
    \/ D2Submit
    \/ \E writeId \in WriteIds : D2Accept(writeId)
    \/ \E writeId \in WriteIds : D2RejectBeforeWal(writeId)
    \/ \E writeId \in WriteIds : D2NotExecuted(writeId)
    \/ D2ReplicaStep
    \/ D2AckReply
    \/ \E message \in messages, storageFailure \in BOOLEAN :
           D2ReplicaFailure(message, storageFailure)
    \/ \E command \in D2PossibleRemovals : D2ProposeRemoval(command)
    \/ \E command \in D2PossibleRemovals : D2CommitRemoval(command)
    \/ \E command \in D2PossibleRemovals : D2ObserveRemoval(command)
    \/ \E writeId \in WriteIds : D2Acknowledge(writeId)
    \/ \E writeId \in WriteIds : D2Deadline(writeId)
    \/ \E writeId \in WriteIds : D2LoseClientResponse(writeId)
    \/ \E writeId \in WriteIds : D2InvalidPermitReply(writeId)
    \/ \E writeId \in WriteIds : D2PostWalFailure(writeId)
    \/ D2LoseMetadata
    \/ D2RestoreMetadata
    \/ D2ProposePromotion
    \/ D2CommitPromotion
    \/ D2OtherViews
    \/ D2MoveMetadataLeader

D2Spec == D2Init /\ [][D2Next]_d2vars

\* Counterfactual witness control, not the selected acknowledgement policy.
D2MetadataRequiredNext ==
    /\ D2Next
    /\ (metadataFaultActive => UNCHANGED acked)

D2ReplicaSymmetry ==
    {permutation \in Permutations(Nodes) :
        permutation[PrimaryNode] = PrimaryNode}

D2TypeOK ==
    /\ D1TypeOK
    /\ clientOutcome \in [WriteIds -> D2Outcomes]
    /\ allocationAtStart \in [WriteIds -> [Nodes -> 0..MaxAllocationId]]
    /\ exclusionDebt \subseteq D2PossibleRemovals
    /\ confirmedRemovals \subseteq D2PossibleRemovals
    /\ localFenced \in [Nodes -> BOOLEAN]
    /\ ackProof \in
          [WriteIds ->
              [copies : 0..Cardinality(Nodes), committed : BOOLEAN,
               debtClear : BOOLEAN, durable : BOOLEAN, primaryOpen : BOOLEAN,
               metadataDown : BOOLEAN]]
    /\ ackAboveGap \in BOOLEAN
    /\ selfFenceSafe \in BOOLEAN
    /\ metadataFaultActive \in BOOLEAN
    /\ replicaFailureCount \in 0..MaxReplicaFailures
    /\ postWalFailureSeen \in BOOLEAN
    /\ removalRejectedSeen \in BOOLEAN

D2ExclusionCommitted ==
    \A writeId \in acked : ackProof[writeId].committed

D2NoAckWithDebt ==
    \A writeId \in acked : ackProof[writeId].debtClear

D2MinimumCopies ==
    \A writeId \in acked :
        ackProof[writeId].copies >= MinimumDurableCopies

D2DurableOperationProof ==
    \A writeId \in acked :
        /\ ackProof[writeId].durable
        /\ ackProof[writeId].primaryOpen

D2NoPostFenceAdmission == selfFenceSafe

D2RejectedHasNoWal ==
    \A writeId \in WriteIds :
        clientOutcome[writeId] \in {"Rejected", "NotExecuted"} =>
            \A node \in Nodes : writeId \notin ops[node]

\* Negated targets retain witnesses; metadata availability is captured at ACK.
D2NoQuorumIndependentAck ==
    ~(\E writeId \in acked :
          /\ clientOutcome[writeId] = "Acknowledged"
          /\ ackProof[writeId].metadataDown
          /\ ackProof[writeId].copies = Cardinality(Nodes)
          /\ exclusionDebt = {}
          /\ confirmedRemovals = {})

D2NoSingleCopyExclusionAck ==
    ~(\E writeId \in acked :
          /\ clientOutcome[writeId] = "Acknowledged"
          /\ ackProof[writeId].copies = 1
          /\ D2ExcludedBy(writeId, confirmedRemovals) # {}
          /\ exclusionDebt = {})

D2NoPostWalFenceOutcome ==
    ~(postWalFailureSeen /\
      localFenced[PrimaryNode] /\
      clientOutcome[1] = "Indeterminate" /\
      clientOutcome[2] = "NotExecuted")

D2GapProgress == <> (2 \in acked)

D2GapFairSpec ==
    /\ D2Spec
    /\ WF_d2vars(D2Submit)
    /\ \A writeId \in WriteIds : WF_d2vars(D2Accept(writeId))
    /\ WF_d2vars(D2ReplicaStep)
    /\ WF_d2vars(D2AckReply)
    /\ \A writeId \in WriteIds : WF_d2vars(D2Acknowledge(writeId))

D2DebtProgress ==
    \A command \in D2PossibleRemovals :
        (command \in exclusionDebt) ~> (command \notin exclusionDebt)

D2DebtFairSpec ==
    /\ D2Spec
    /\ WF_d2vars(D2RestoreMetadata)
    /\ \A command \in D2PossibleRemovals :
           /\ WF_d2vars(D2ProposeRemoval(command))
           /\ WF_d2vars(D2CommitRemoval(command))
           /\ WF_d2vars(D2ObserveRemoval(command))

=============================================================================
