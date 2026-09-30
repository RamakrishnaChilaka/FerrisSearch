--------------------------- MODULE TraceD1Recovery ---------------------------
\* Exact composition for source snapshot, target install, ordered catch-up,
\* finalize barrier, admission, and observed target document state.

EXTENDS MC_D1_SeqNoApply, TraceInput

VARIABLES
    tracePos,
    hiddenSteps,
    finished,
    replicaResponsePersisted

TraceRecoveryVars ==
    <<tracePos, hiddenSteps, finished, replicaResponsePersisted>>
traceRecoveryVars == <<d1vars, TraceRecoveryVars>>

TraceRecoveryInit ==
    /\ Init
    /\ D1DataInit
    /\ routing.primary = TraceInitialPrimary
    /\ routing.inSync = TraceInitialInSync
    /\ raftLeader = TraceInitialPrimary
    /\ tracePos = 1
    /\ hiddenSteps = 0
    /\ finished = FALSE
    /\ replicaResponsePersisted =
          [writeId \in WriteIds |-> [node \in Nodes |-> 0]]

StableReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars, D1Vars>>

FenceChangingReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars, D1Vars>>

RecoveryAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, FaultVars, D1Vars>>

FaultAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, D1Vars>>

RecoveryStable(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            sessionAllocation, pendingAllocation, ApplySafetyVars,
            FaultVars, D1Vars>>

RecoveryInstall(action) ==
    /\ action
    /\ UNCHANGED
          <<sessionAllocation, pendingAllocation, ApplySafetyVars, FaultVars>>

RecoveryTargetComplete(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            sessionAllocation, ApplySafetyVars, FaultVars, D1Vars>>

RecoveryObserveAdmission(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            pendingAllocation, ApplySafetyVars, FaultVars, D1Vars>>

MessageFor(writeId, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "Replicate"
        /\ message.write = writeId
        /\ message.to = replica

AckFor(writeId, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "ReplicaAck"
        /\ message.write = writeId
        /\ message.from = replica

HasMessage(writeId, replica) ==
    \E message \in messages :
        /\ message.kind = "Replicate"
        /\ message.write = writeId
        /\ message.to = replica

HasAck(writeId, replica) ==
    \E message \in messages :
        /\ message.kind = "ReplicaAck"
        /\ message.write = writeId
        /\ message.from = replica

CopyStateMatches(node, event) ==
    /\ docValue[node] = event.docValue
    /\ docSeqNext[node] = event.docSeqNext
    /\ \A doc \in Docs :
           LET writeId == event.docValue[doc]
           IN IF writeId = NoWrite
              THEN TRUE
              ELSE /\ writeSeq[writeId] = event.docSeqNext[doc] - 1
                   /\ writeTerm[writeId] = event.docTerm[doc]

ViewMatches(view, event) ==
    /\ view.primary = event.viewPrimary
    /\ view.term = event.viewTerm
    /\ view.inSync = event.viewInSync
    /\ view.allocations = event.viewAllocations
    /\ view.initialized = event.initialized

ClientWriteEvent(event) ==
    /\ event.writeId = nextWrite
    /\ D1ClientWriteFrom(event.node, event.doc, event.writeKind)
    /\ writeTarget'[event.writeId] = event.peer

PrimaryWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ CanPrimaryAccept(event.writeId)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars

PrimaryProcessEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.outcome = "applied_newer"
    /\ D1PrimaryAccept(event.writeId)
    /\ writeSeq'[event.writeId] = event.seq
    /\ writeTerm'[event.writeId] = event.term
    /\ processedNext'[event.node] = event.processedNext
    /\ persistedNext'[event.node] = event.persistedNext
    /\ maxSeqNext'[event.node] = event.maxNext

PrimaryReplicationObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ writeStatus[event.writeId] = "Replicating"
    /\ writeSeq[event.writeId] = event.seq
    /\ writeTerm[event.writeId] = event.term
    /\ writeRequired[event.writeId] = event.required
    /\ event.required = views[event.node].inSync
    /\ {message \in messages :
            /\ message.kind = "Replicate"
            /\ message.write = event.writeId}
          = event.requiredMessages
    /\ UNCHANGED d1vars

ReplicaReceiveObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ event.transportMessage.kind = "Replicate"
    /\ event.transportMessage.write = event.writeId
    /\ event.transportMessage.to = event.node
    /\ UNCHANGED d1vars

ReplicaRejectedEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
       IN /\ message.kind = "Replicate"
          /\ message.write = event.writeId
          /\ message.to = event.node
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ message.targetAllocation = event.allocation
          /\ IF event.reason = "apply_failure"
                THEN FenceChangingReplication(ReplicaApplyFailure(message))
                ELSE StableReplication(ReplicaReject(message))

ReplicaWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
       IN /\ ReplicaMessageValid(message)
          /\ message.term >= durableReplicaFence[event.node]
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars

ReplicaApplyEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
           beforeDoc == docValue[event.node][event.doc]
       IN /\ message.write = event.writeId
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ CASE event.outcome = "redelivery" ->
                    D1FixedReplicaRedelivery(message)
             [] event.outcome \in {"applied_newer", "stale", "noop"} ->
                    /\ D1FixedReplicaProcess(message)
                    /\ IF event.outcome = "applied_newer"
                          THEN docValue'[event.node][event.doc] = event.writeId
                          ELSE IF event.outcome = "stale"
                               THEN docValue'[event.node][event.doc] = beforeDoc
                               ELSE TRUE
             [] OTHER -> FALSE
    /\ event.writeId \in durableOps'[event.node]
    /\ replicaResponsePersisted' =
          [replicaResponsePersisted EXCEPT
              ![event.writeId][event.node] = event.persistedNext]
    /\ processedNext'[event.node] = event.processedNext
    /\ persistedNext'[event.node] = event.persistedNext
    /\ maxSeqNext'[event.node] = event.maxNext

ReplicaResultEvent(event) ==
    /\ CASE event.outcome = "acknowledged" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "ReplicaAck"
              /\ replicaResponsePersisted[event.writeId][event.peer]
                    <= event.resultPersistedNext
              /\ event.resultPersistedNext <= persistedNext[event.peer]
              /\ D1DeliverAck(event.transportMessage)
       [] event.outcome = "failed" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "ReplicaNack"
              /\ StableReplication(
                    DeliverReplicaNack(event.transportMessage))
       [] event.outcome \in {"dropped", "timeout"} ->
              IF event.hasTransportMessage
              THEN /\ event.transportMessage \in messages
                   /\ FaultAction(LoseMsg(event.transportMessage))
              ELSE /\ event.peer \in writeWait[event.writeId]
                   /\ UNCHANGED d1vars
       [] OTHER -> FALSE

ClientResultEvent(event) ==
    /\ event.outcome = "acknowledged"
    /\ D1PrimaryAck(event.writeId)

SnapshotEvent(event) ==
    /\ event.source \in Nodes
    /\ event.target \in Nodes
    /\ sessionSource[event.target] = event.source
    /\ RecoveryStable(SourceSnapshot(event.target))
    /\ sessionBoundary'[event.target] = event.snapshotNext
    /\ D1SnapshotSequences(
           event.source,
           sessionSnapshot'[event.target],
           sessionBoundary'[event.target])
          = event.observedProcessed
    /\ D1VisibleDocValue(sessionSnapshot'[event.target])
          = event.snapshotDocValue

RecoveryStartEvent(event) ==
    /\ RecoveryStable(TargetBeginInstall(event.target))

RecoveryInstallEvent(event) ==
    /\ RecoveryInstall(D1InstallRecoverySnapshot(event.target))
    /\ nextSeq'[event.target] = event.snapshotNext

RecoveryWalObservation(event) ==
    /\ sessionFetched[event.node] = event.writeId
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars

RecoveryApplyEvent(event) ==
    /\ sessionFetched[event.node] = event.writeId
    /\ D1FixedRecoveryApply(event.node)
    /\ IF event.outcome = "applied_newer"
          THEN docValue'[event.node][event.doc] = event.writeId
          ELSE IF event.outcome = "stale"
               THEN docValue'[event.node][event.doc]
                    = docValue[event.node][event.doc]
               ELSE FALSE
    /\ event.writeId \in durableOps'[event.node]
    /\ processedNext'[event.node] = event.processedNext
    /\ persistedNext'[event.node] = event.persistedNext
    /\ maxSeqNext'[event.node] = event.maxNext

RecoveryBarrierEvent(event) ==
    /\ sessionHead[event.target] = event.barrierNext
    /\ sessionCursor[event.target] = event.barrierNext
    /\ processedSeqs[event.target] = event.observedProcessed
    /\ RecoveryTargetComplete(TargetComplete(event.target))

RecoveryMembershipEvent(event) ==
    /\ event.outcome = "admitted"
    /\ RecoveryTargetComplete(TargetObserveAdmitted(event.target))
    /\ event.target \in routing.inSync

CopyStateObservation(event) ==
    /\ CopyStateMatches(event.node, event)
    /\ UNCHANGED d1vars

RoutingViewObservation(event) ==
    /\ \/ /\ ViewMatches(views[event.node], event)
          /\ UNCHANGED d1vars
       \/ /\ FenceChangingReplication(DeliverView(event.node))
          /\ ViewMatches(views'[event.node], event)

RecoveryTraceEventCore(event) ==
    CASE event.kind = "client_write_routed" -> ClientWriteEvent(event)
      [] event.kind = "wal_appended" ->
            IF event.origin = "primary"
            THEN PrimaryWalObservation(event)
            ELSE IF event.origin = "live_replication"
                 THEN ReplicaWalObservation(event)
                 ELSE RecoveryWalObservation(event)
      [] event.kind = "operation_processed" ->
            IF event.origin = "primary"
            THEN PrimaryProcessEvent(event)
            ELSE IF event.origin = "live_replication"
                 THEN ReplicaApplyEvent(event)
                 ELSE RecoveryApplyEvent(event)
      [] event.kind = "primary_replication_started" ->
            PrimaryReplicationObservation(event)
      [] event.kind = "replica_received" ->
            ReplicaReceiveObservation(event)
      [] event.kind = "replica_rejected" ->
            ReplicaRejectedEvent(event)
      [] event.kind = "replica_result" -> ReplicaResultEvent(event)
      [] event.kind = "client_result" -> ClientResultEvent(event)
      [] event.kind = "recovery_snapshot" -> SnapshotEvent(event)
      [] event.kind = "recovery_started" -> RecoveryStartEvent(event)
      [] event.kind = "recovery_installed" -> RecoveryInstallEvent(event)
      [] event.kind = "recovery_barrier" -> RecoveryBarrierEvent(event)
      [] event.kind = "recovery_membership" ->
            RecoveryMembershipEvent(event)
      [] event.kind = "copy_state" -> CopyStateObservation(event)
      [] event.kind = "routing_view" ->
            RoutingViewObservation(event)
      [] OTHER -> FALSE

RecoveryTraceEvent(event) ==
    /\ RecoveryTraceEventCore(event)
    /\ IF event.kind = "operation_processed"
          /\ event.origin = "live_replication"
          THEN TRUE
          ELSE UNCHANGED replicaResponsePersisted

ConsumeRecoveryEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ RecoveryTraceEvent(event)
    /\ tracePos' = tracePos + 1
    /\ hiddenSteps' = 0
    /\ UNCHANGED finished

HiddenRecoveryAction ==
    \/ \E target \in Nodes, source \in Nodes :
           RecoveryAction(StartRecovery(target, source))
    \/ \E target \in Nodes : RecoveryStable(FetchOps(target))
    \/ \E target \in Nodes : RecoveryStable(FinishCatchUp(target))
    \/ \E target \in Nodes :
           RecoveryStable(BeginPrepareFinalize(target))
    \/ \E target \in Nodes :
           RecoveryStable(AcquireFinalizeBarrier(target))
    \/ \E target \in Nodes :
           RecoveryStable(FinishFinalizeTail(target))
    \/ \E target \in Nodes : RecoveryStable(BeginSettlement(target))
    \/ \E target \in Nodes : RecoveryStable(ProposeMarkInSync(target))
    \/ \E command \in pendingRaft :
           FenceChangingReplication(CommitRaft(command))
    \/ \E node \in Nodes :
           FenceChangingReplication(DeliverView(node))
    \/ \E target \in Nodes :
           RecoveryObserveAdmission(ObserveAdmission(target))

HiddenRecoveryStep ==
    /\ tracePos <= Len(Trace)
    /\ hiddenSteps < MaxHiddenSteps
    /\ HiddenRecoveryAction
    /\ hiddenSteps' = hiddenSteps + 1
    /\ UNCHANGED <<tracePos, finished, replicaResponsePersisted>>

FinishRecoveryTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ (~TraceQuiescent \/ messages = {})
    /\ finished' = TRUE
    /\ UNCHANGED
          <<d1vars, tracePos, hiddenSteps, replicaResponsePersisted>>

TraceRecoveryNext ==
    \/ ConsumeRecoveryEvent
    \/ HiddenRecoveryStep
    \/ FinishRecoveryTrace

TraceRecoverySpec ==
    /\ TraceRecoveryInit
    /\ [][TraceRecoveryNext]_traceRecoveryVars

TraceRecoveryTypeOK ==
    /\ D1TypeOK
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ hiddenSteps \in 0..MaxHiddenSteps
    /\ finished \in BOOLEAN
    /\ replicaResponsePersisted \in
          [WriteIds -> [Nodes -> 0..MaxWrites]]

TraceRecoverySafety ==
    /\ RoutingWellFormed
    /\ NoCopyBehindAcked
    /\ D1NoAcknowledgedDeleteResurrection
    /\ D1ProcessedCheckpointGapAware
    /\ D1PersistedCheckpointGapAware
    /\ D1WalHasNoDuplicateSeq
    /\ PromotionComplete
    /\ AdmissionComplete
    /\ NoPartialServe
    /\ NoApplyBelowObservedFence
    /\ ActivePrimaryRejectsOldTerm
    /\ (finished /\ TraceQuiescent => D1QuiescentConvergence)

TraceRecoveryNotAccepted ==
    ~finished

=============================================================================
