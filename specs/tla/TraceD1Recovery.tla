--------------------------- MODULE TraceD1Recovery ---------------------------
\* Exact composition for source snapshot, target install, ordered catch-up,
\* finalize barrier, admission, and observed target document state.

EXTENDS Invariants, TraceInput

VARIABLES
    tracePos,
    hiddenSteps,
    finished

TraceRecoveryVars == <<tracePos, hiddenSteps, finished>>
traceRecoveryVars == <<vars, TraceRecoveryVars>>

TraceRecoveryInit ==
    /\ Init
    /\ tracePos = 1
    /\ hiddenSteps = 0
    /\ finished = FALSE

StableReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

FenceChangingReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

RecoveryAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, FaultVars>>

RecoveryStable(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            sessionAllocation, pendingAllocation, ApplySafetyVars,
            FaultVars>>

RecoveryInstall(action) ==
    /\ action
    /\ UNCHANGED
          <<sessionAllocation, pendingAllocation, ApplySafetyVars,
            FaultVars>>

RecoveryTargetComplete(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            sessionAllocation, ApplySafetyVars, FaultVars>>

RecoveryObserveAdmission(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            pendingAllocation, ApplySafetyVars, FaultVars>>

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
    /\ \A doc \in Docs :
           LET writeId == event.docValue[doc]
           IN IF writeId = NoWrite
              THEN TRUE
              ELSE /\ writeSeq[writeId] = event.docSeqNext[doc] - 1
                   /\ writeTerm[writeId] = event.docTerm[doc]

ClientWriteEvent(event) ==
    /\ event.writeId = nextWrite
    /\ StableReplication(
           ClientWrite(event.node, event.doc, event.writeKind))
    /\ writeTarget'[event.writeId] = event.peer

PrimaryWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ CanPrimaryAccept(event.writeId)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED vars

PrimaryProcessObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ CanPrimaryAccept(event.writeId)
    /\ event.outcome = "applied_newer"
    /\ UNCHANGED vars

PrimaryAcceptEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ StableReplication(PrimaryAccept(event.writeId))
    /\ writeSeq'[event.writeId] = event.seq
    /\ writeTerm'[event.writeId] = event.term
    /\ writeRequired'[event.writeId] = event.required
    /\ event.required = views[event.node].inSync
    /\ event.writeId \in durableOps'[event.node]

ReplicaReceiveObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ UNCHANGED vars

ReplicaWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ LET message == MessageFor(event.writeId, event.node)
       IN /\ ReplicaMessageValid(message)
          /\ message.term >= durableReplicaFence[event.node]
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED vars

ReplicaApplyEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ event.outcome = "applied_newer"
    /\ FenceChangingReplication(
           ReplicaApply(MessageFor(event.writeId, event.node)))
    /\ docValue'[event.node][event.doc] = event.writeId
    /\ event.writeId \in durableOps'[event.node]

ReplicaResultEvent(event) ==
    /\ HasAck(event.writeId, event.peer)
    /\ StableReplication(
           DeliverReplicaAck(AckFor(event.writeId, event.peer)))

ClientResultEvent(event) ==
    /\ event.outcome = "acknowledged"
    /\ StableReplication(PrimaryAck(event.writeId))

SnapshotEvent(event) ==
    /\ event.source \in Nodes
    /\ event.target \in Nodes
    /\ sessionSource[event.target] = event.source
    /\ RecoveryStable(SourceSnapshot(event.target))
    /\ sessionBoundary'[event.target] = event.snapshotNext
    /\ {writeSeq[writeId] : writeId \in sessionSnapshot'[event.target]}
          = event.observedProcessed
    /\ RebuiltDocValue(sessionSnapshot'[event.target])
          = event.snapshotDocValue

RecoveryStartEvent(event) ==
    /\ RecoveryStable(TargetBeginInstall(event.target))

RecoveryInstallEvent(event) ==
    /\ RecoveryInstall(InstallSnapshot(event.target))
    /\ nextSeq'[event.target] = event.snapshotNext

RecoveryWalObservation(event) ==
    /\ sessionFetched[event.node] = event.writeId
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED vars

RecoveryApplyEvent(event) ==
    /\ sessionFetched[event.node] = event.writeId
    /\ event.outcome = "applied_newer"
    /\ RecoveryStable(ApplyOps(event.node))
    /\ docValue'[event.node][event.doc] = event.writeId
    /\ event.writeId \in durableOps'[event.node]

RecoveryBarrierEvent(event) ==
    /\ sessionHead[event.target] = event.barrierNext
    /\ sessionCursor[event.target] = event.barrierNext
    /\ {writeSeq[writeId] : writeId \in ops[event.target]}
          = event.observedProcessed
    /\ RecoveryTargetComplete(TargetComplete(event.target))

RecoveryMembershipEvent(event) ==
    /\ event.outcome = "admitted"
    /\ RecoveryTargetComplete(TargetObserveAdmitted(event.target))
    /\ event.target \in routing.inSync

CopyStateObservation(event) ==
    /\ CopyStateMatches(event.node, event)
    /\ UNCHANGED vars

RecoveryTraceEvent(event) ==
    CASE event.kind = "client_write_routed" -> ClientWriteEvent(event)
      [] event.kind = "wal_appended" ->
            IF event.origin = "primary"
            THEN PrimaryWalObservation(event)
            ELSE IF event.origin = "live_replication"
                 THEN ReplicaWalObservation(event)
                 ELSE RecoveryWalObservation(event)
      [] event.kind = "operation_processed" ->
            IF event.origin = "primary"
            THEN PrimaryProcessObservation(event)
            ELSE IF event.origin = "live_replication"
                 THEN ReplicaApplyEvent(event)
                 ELSE RecoveryApplyEvent(event)
      [] event.kind = "primary_replication_started" ->
            PrimaryAcceptEvent(event)
      [] event.kind = "replica_received" ->
            ReplicaReceiveObservation(event)
      [] event.kind = "replica_result" -> ReplicaResultEvent(event)
      [] event.kind = "client_result" -> ClientResultEvent(event)
      [] event.kind = "recovery_snapshot" -> SnapshotEvent(event)
      [] event.kind = "recovery_started" -> RecoveryStartEvent(event)
      [] event.kind = "recovery_installed" -> RecoveryInstallEvent(event)
      [] event.kind = "recovery_barrier" -> RecoveryBarrierEvent(event)
      [] event.kind = "recovery_membership" ->
            RecoveryMembershipEvent(event)
      [] event.kind = "copy_state" -> CopyStateObservation(event)
      [] OTHER -> FALSE

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
    /\ UNCHANGED <<tracePos, finished>>

FinishRecoveryTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ finished' = TRUE
    /\ UNCHANGED <<vars, tracePos, hiddenSteps>>

TraceRecoveryNext ==
    \/ ConsumeRecoveryEvent
    \/ HiddenRecoveryStep
    \/ FinishRecoveryTrace

TraceRecoverySpec ==
    /\ TraceRecoveryInit
    /\ [][TraceRecoveryNext]_traceRecoveryVars

TraceRecoveryTypeOK ==
    /\ TypeOK
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ hiddenSteps \in 0..MaxHiddenSteps
    /\ finished \in BOOLEAN

TraceRecoveryNotAccepted ==
    ~finished

=============================================================================
