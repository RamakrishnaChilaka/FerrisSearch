----------------------------- MODULE TraceD1 -----------------------------
\* Existential implementation-trace composition for the real D1 actions.
\*
\* TraceInput.tla is generated from one schema-v4 JSONL file.  Low-level
\* implementation observations either constrain a real D1 action or advance
\* by a D1 stuttering step.  TLC searches bounded real hidden D1 actions
\* between observations.  It accepts a trace only by reaching `finished`.

EXTENDS MC_D1_SeqNoApply, TraceInput

VARIABLES
    tracePos,
    hiddenSteps,
    finished,
    captureActive,
    captureBoundary,
    capturePersisted,
    captureMax,
    captureOps,
    captureDocValue,
    captureDocSeqNext,
    captureTombstoneSeqNext,
    replicaResponsePersisted

TraceVars ==
    <<tracePos, hiddenSteps, finished, captureActive, captureBoundary,
      capturePersisted, captureMax, captureOps, captureDocValue, captureDocSeqNext,
      captureTombstoneSeqNext, replicaResponsePersisted>>

CaptureVars ==
    <<captureActive, captureBoundary, capturePersisted, captureMax, captureOps,
      captureDocValue, captureDocSeqNext, captureTombstoneSeqNext>>

AuxVars == <<CaptureVars, replicaResponsePersisted>>

traceVars == <<d1vars, TraceVars>>

EmptyCapturedOps ==
    [node \in Nodes |-> {}]

EmptyCapturedDocValue ==
    [node \in Nodes |-> [doc \in Docs |-> NoWrite]]

EmptyCapturedDocSeq ==
    [node \in Nodes |-> [doc \in Docs |-> 0]]

TraceInit ==
    /\ Init
    /\ D1DataInit
    /\ routing.primary = TraceInitialPrimary
    /\ routing.inSync = TraceInitialInSync
    /\ raftLeader = TraceInitialPrimary
    /\ tracePos = 1
    /\ hiddenSteps = 0
    /\ finished = FALSE
    /\ captureActive = [node \in Nodes |-> FALSE]
    /\ captureBoundary = [node \in Nodes |-> 0]
    /\ capturePersisted = [node \in Nodes |-> 0]
    /\ captureMax = [node \in Nodes |-> 0]
    /\ captureOps = EmptyCapturedOps
    /\ captureDocValue = EmptyCapturedDocValue
    /\ captureDocSeqNext = EmptyCapturedDocSeq
    /\ captureTombstoneSeqNext = EmptyCapturedDocSeq
    /\ replicaResponsePersisted =
          [writeId \in WriteIds |-> [node \in Nodes |-> 0]]

WriteRequestMessages(writeId) ==
    {message \in messages :
        /\ message.kind = "Replicate"
        /\ message.write = writeId}

CheckpointMatches(node, event) ==
    /\ processedNext[node] = event.processedNext
    /\ persistedNext[node] = event.persistedNext
    /\ maxSeqNext[node] = event.maxNext

CheckpointMatchesPrime(node, event) ==
    /\ processedNext'[node] = event.processedNext
    /\ persistedNext'[node] = event.persistedNext
    /\ maxSeqNext'[node] = event.maxNext

LiveCheckpointMatches(node, event) ==
    /\ processedNext[node] = event.processedNext
    /\ persistedNext[node] = event.persistedNext
    /\ maxSeqNext[node] = event.maxNext

LiveCheckpointMatchesPrime(node, event) ==
    /\ processedNext'[node] = event.processedNext
    /\ persistedNext'[node] = event.persistedNext
    /\ maxSeqNext'[node] = event.maxNext

CopyStateMatches(node, event) ==
    /\ docValue[node] = event.docValue
    /\ docSeqNext[node] = event.docSeqNext
    /\ \A doc \in Docs :
           LET writeId == event.docValue[doc]
           IN IF writeId = NoWrite
              THEN TRUE
              ELSE /\ writeSeq[writeId] = event.docSeqNext[doc] - 1
                   /\ writeTerm[writeId] = event.docTerm[doc]

StableReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars, D1Vars>>

FenceChangingReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars, D1Vars>>

FaultAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, D1Vars>>

RecoveryAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, FaultVars, D1Vars>>

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
    /\ UNCHANGED AuxVars

\* WAL and primary planner observations precede the atomic D1PrimaryAccept
\* abstraction.  They may stutter only while that exact real action is enabled.
PrimaryWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ writeStatus[event.writeId] = "Routed"
    /\ CanPrimaryAccept(event.writeId)
    /\ event.term = views[event.node].term
    /\ event.seq >= nextSeq[event.node]
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

PrimaryProcessEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ writeStatus[event.writeId] = "Routed"
    /\ event.outcome = "applied_newer"
    /\ D1PrimaryAccept(event.writeId)
    /\ writePrimary'[event.writeId] = event.node
    /\ writeSeq'[event.writeId] = event.seq
    /\ writeTerm'[event.writeId] = event.term
    /\ event.writeId \in durableOps'[event.node]
    /\ CheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

PrimaryReplicationObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ writeStatus[event.writeId] = "Replicating"
    /\ writePrimary[event.writeId] = event.node
    /\ writeSeq[event.writeId] = event.seq
    /\ writeTerm[event.writeId] = event.term
    /\ writeRequired[event.writeId] = event.required
    /\ event.required = views[event.node].inSync
    /\ WriteRequestMessages(event.writeId) = event.requiredMessages
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

ReplicaReceiveObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
       IN /\ message.from = event.peer
          /\ message.to = event.node
          /\ message.kind = "Replicate"
          /\ message.write = event.writeId
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ message.targetAllocation = event.allocation
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

ReplicaRejectedEvent(event) ==
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
       IN /\ message.from = event.peer
          /\ message.to = event.node
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ message.targetAllocation = event.allocation
          /\ CASE event.writeId = NoWrite ->
                    /\ message.kind = "ReplicateNoOp"
                    /\ D1FixedReplicaNoOpReject(message)
             [] event.reason = "apply_failure" ->
                    /\ message.kind = "Replicate"
                    /\ message.write = event.writeId
                    /\ FenceChangingReplication(ReplicaApplyFailure(message))
             [] OTHER ->
                    /\ message.kind = "Replicate"
                    /\ message.write = event.writeId
                    /\ StableReplication(ReplicaReject(message))
    /\ UNCHANGED AuxVars

ReplicaWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
       IN D1ReplicaMessageEnabled(message)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

ReplicaProcessEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ LET message == event.transportMessage
           beforeDoc == docValue[event.node][event.doc]
       IN /\ message.from = event.peer
          /\ message.to = event.node
          /\ message.kind = "Replicate"
          /\ message.write = event.writeId
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ CASE event.outcome = "redelivery" ->
                    D1FixedReplicaRedelivery(message)
             [] event.outcome = "collision" ->
                    D1FixedReplicaCollision(message)
             [] event.outcome \in {"applied_newer", "stale", "noop"} ->
                    /\ D1FixedReplicaProcess(message)
                    /\ IF event.outcome = "applied_newer"
                          THEN docValue'[event.node][event.doc] = event.writeId
                          ELSE IF event.outcome = "stale"
                               THEN docValue'[event.node][event.doc] = beforeDoc
                               ELSE TRUE
             [] OTHER -> FALSE
    /\ IF event.outcome = "collision"
          THEN TRUE
          ELSE event.writeId \in durableOps'[event.node]
    /\ replicaResponsePersisted' =
          [replicaResponsePersisted EXCEPT
              ![event.writeId][event.node] = event.persistedNext]
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED CaptureVars

ReplicaResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "ReplicaAck"
              /\ event.transportMessage.write = event.writeId
              /\ event.transportMessage.from = event.peer
              /\ D1DeliverAck(event.transportMessage)
       [] event.outcome = "failed" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "ReplicaNack"
              /\ event.transportMessage.write = event.writeId
              /\ event.transportMessage.from = event.peer
              /\ StableReplication(
                    DeliverReplicaNack(event.transportMessage))
       [] event.outcome \in {"dropped", "timeout"} ->
              IF event.hasTransportMessage
              THEN /\ event.transportMessage \in messages
                   /\ FaultAction(LoseMsg(event.transportMessage))
              ELSE /\ event.peer \in writeWait[event.writeId]
                   /\ UNCHANGED d1vars
       [] OTHER -> FALSE
    /\ IF event.outcome = "acknowledged"
          THEN /\ replicaResponsePersisted[event.writeId][event.peer]
                    <= event.resultPersistedNext
               /\ event.resultPersistedNext <= persistedNext[event.peer]
          ELSE TRUE
    /\ UNCHANGED AuxVars

PromotionNoOpSendObservation(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.hasTransportMessage
    /\ D1RedeliverPromotionNoOp(event.node, event.peer, event.seq)
    /\ event.transportMessage \in messages'
    /\ event.transportMessage.kind = "ReplicateNoOp"
    /\ event.transportMessage.from = event.node
    /\ event.transportMessage.to = event.peer
    /\ event.transportMessage.term = event.term
    /\ event.transportMessage.seq = event.seq
    /\ UNCHANGED AuxVars

PromotionNoOpReceiveObservation(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ event.transportMessage.kind = "ReplicateNoOp"
    /\ event.transportMessage.from = event.peer
    /\ event.transportMessage.to = event.node
    /\ event.transportMessage.term = event.term
    /\ event.transportMessage.seq = event.seq
    /\ event.transportMessage.targetAllocation = event.allocation
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

PromotionNoOpWalObservation(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ D1NoOpMessageEnabled(event.transportMessage)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

PromotionFillWalEvent(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.origin \in {"primary", "promotion"}
    /\ D1AppendPromotionNoOp(event.node, event.seq, event.term)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED AuxVars

PromotionFillProcessEvent(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.origin \in {"primary", "promotion"}
    /\ event.outcome = "noop"
    /\ D1ProcessPromotionNoOp(event.node, event.seq, event.term)
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

PromotionNoOpProcessEvent(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ event.hasTransportMessage
    /\ event.transportMessage \in messages
    /\ event.transportMessage.to = event.node
    /\ event.transportMessage.term = event.term
    /\ event.transportMessage.seq = event.seq
    /\ CASE event.outcome = "noop" ->
              D1FixedReplicaNoOpProcess(event.transportMessage)
       [] event.outcome = "redelivery" ->
              D1FixedReplicaNoOpRedelivery(event.transportMessage)
       [] event.outcome = "collision" ->
              D1FixedReplicaNoOpCollision(event.transportMessage)
       [] OTHER -> FALSE
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

PromotionNoOpResultEvent(event) ==
    /\ TraceCombined
    /\ event.writeId = NoWrite
    /\ CASE event.outcome = "acknowledged" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "NoOpAck"
              /\ D1DeliverNoOpAck(event.transportMessage)
       [] event.outcome = "failed" ->
              /\ event.hasTransportMessage
              /\ event.transportMessage \in messages
              /\ event.transportMessage.kind = "NoOpNack"
              /\ D1DeliverNoOpNack(event.transportMessage)
       [] event.outcome \in {"dropped", "timeout"} ->
              IF event.hasTransportMessage
              THEN /\ event.transportMessage \in messages
                   /\ FaultAction(LoseMsg(event.transportMessage))
              ELSE UNCHANGED d1vars
       [] OTHER -> FALSE
    /\ IF event.outcome = "acknowledged"
          THEN event.resultPersistedNext <= persistedNext[event.peer]
          ELSE TRUE
    /\ UNCHANGED AuxVars

ClientResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              D1PrimaryAck(event.writeId)
       [] event.outcome = "failed" ->
              IF event.preWalVersionConflict
              THEN StableReplication(PrimaryVersionConflict(event.writeId))
              ELSE \/ StableReplication(PrimaryFail(event.writeId))
                   \/ StableReplication(PrimaryReject(event.writeId))
                   \/ /\ writeStatus[event.writeId] = "Failed"
                      /\ UNCHANGED d1vars
       [] OTHER -> FALSE
    /\ UNCHANGED AuxVars

CommitCaptureEvent(event) ==
    /\ event.node \in Nodes
    /\ alive[event.node]
    /\ LiveCheckpointMatches(event.node, event)
    /\ ~captureActive[event.node]
    /\ captureActive' =
          [captureActive EXCEPT ![event.node] = TRUE]
    /\ captureBoundary' =
          [captureBoundary EXCEPT ![event.node] = event.processedNext]
    /\ capturePersisted' =
          [capturePersisted EXCEPT ![event.node] = event.persistedNext]
    /\ captureMax' =
          [captureMax EXCEPT ![event.node] = event.maxNext]
    /\ captureOps' =
          [captureOps EXCEPT ![event.node] = ops[event.node]]
    /\ captureDocValue' =
          [captureDocValue EXCEPT ![event.node] = docValue[event.node]]
    /\ captureDocSeqNext' =
          [captureDocSeqNext EXCEPT
              ![event.node] = docSeqNext[event.node]]
    /\ captureTombstoneSeqNext' =
          [captureTombstoneSeqNext EXCEPT
              ![event.node] = tombstoneSeqNext[event.node]]
    /\ UNCHANGED d1vars
    /\ UNCHANGED replicaResponsePersisted

CommitPersistEvent(event) ==
    /\ event.node \in Nodes
    /\ CASE captureActive[event.node] ->
              /\ event.processedNext = captureBoundary[event.node]
              /\ event.persistedNext = capturePersisted[event.node]
              /\ event.maxNext = captureMax[event.node]
              /\ D1PersistBoundary(
                     event.node,
                     captureBoundary[event.node],
                     capturePersisted[event.node],
                     captureMax[event.node],
                     captureOps[event.node],
                     captureDocValue[event.node],
                     captureDocSeqNext[event.node],
                     captureTombstoneSeqNext[event.node],
                     TRUE)
              /\ captureActive' =
                    [captureActive EXCEPT ![event.node] = FALSE]
              /\ UNCHANGED
                    <<captureBoundary, capturePersisted, captureMax, captureOps,
                      captureDocValue, captureDocSeqNext,
                      captureTombstoneSeqNext>>
       [] ~captureActive[event.node] ->
              /\ event.processedNext = persistedProcessedNext[event.node]
              /\ event.persistedNext = persistedCommittedNext[event.node]
              /\ event.maxNext = persistedMaxSeqNext[event.node]
              /\ UNCHANGED d1vars
              /\ UNCHANGED CaptureVars
    /\ UNCHANGED replicaResponsePersisted

CrashEvent(event) ==
    /\ WritesOwnedBy(event.node) = event.crashFailedWrites
    /\ DropNodeMessages(event.node) = event.crashDroppedMessages
    /\ D1CrashCopy(event.node)
    /\ UNCHANGED AuxVars

RestartEvent(event) ==
    /\ D1RestartCopy(event.node)
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

ReplayStartObservation(event) ==
    /\ event.node \in Nodes
    /\ CASE replaying[event.node] ->
              /\ replayBoundary[event.node] = event.processedNext
              /\ LiveCheckpointMatches(event.node, event)
              /\ UNCHANGED d1vars
       [] ~replaying[event.node] ->
              /\ D1StartInPlaceReplay(event.node)
              /\ replayBoundary'[event.node] = event.processedNext
              /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

ReplayEntryEvent(event) ==
    /\ event.node \in Nodes
    /\ replaying[event.node]
    /\ replayPos[event.node] = event.walPosition
    /\ replayPos[event.node] <= Len(walOrder[event.node])
    /\ LET entry == walOrder[event.node][replayPos[event.node]]
       IN /\ event.seq = WalEntrySeq(entry)
          /\ event.term = WalEntryTerm(event.node, entry)
          /\ CASE event.outcome = "skip_committed" ->
                    /\ IF event.writeId = NoWrite
                          THEN IsNoOpEntry(entry)
                          ELSE entry = event.writeId
                    /\ D1ReplaySkipAt(event.node)
             [] event.outcome = "noop" ->
                    /\ event.writeId = NoWrite
                    /\ IsNoOpEntry(entry)
                    /\ D1ReplayNoOpAt(event.node)
             [] event.outcome \in {"applied_newer", "stale", "redelivery"} ->
                    /\ entry = event.writeId
                    /\ LET beforeDoc == docValue[event.node][event.doc]
                       IN /\ D1FixedReplayApplyAt(event.node)
                          /\ IF event.outcome = "applied_newer"
                                THEN docValue'[event.node][event.doc] =
                                     event.writeId
                                ELSE IF event.outcome = "stale"
                                     THEN docValue'[event.node][event.doc] =
                                          beforeDoc
                                     ELSE docValue'[event.node][event.doc] =
                                          beforeDoc
             [] OTHER -> FALSE
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

ReplayFinishEvent(event) ==
    /\ event.node \in Nodes
    /\ replayPos[event.node] = event.walPosition
    /\ CASE event.outcome = "completed" ->
              /\ D1FinishReplayAt(event.node)
              /\ replaySafe'
       [] event.outcome = "failed" ->
              D1FailReplayAt(event.node)
       [] OTHER -> FALSE
    /\ UNCHANGED AuxVars

TruncateEvent(event) ==
    /\ D1RecordTruncation(event.node, event.truncateNext)
    /\ UNCHANGED AuxVars

CopyStateObservation(event) ==
    /\ event.node \in Nodes
    /\ CopyStateMatches(event.node, event)
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

FenceObservation(event) ==
    /\ event.node \in Nodes
    /\ D1ObserveFence(event.node, event.term, event.fenceMaxNext)
    /\ UNCHANGED AuxVars

RoutingViewObservation(event) ==
    /\ ViewMatches(views[event.node], event)
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

PromotionObservation(event) ==
    /\ TraceCombined
    /\ routing.primary = event.newPrimary
    /\ routing.term = event.term
    /\ routing.inSync = event.viewInSync
    /\ raftLeader = event.emitter
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

InSyncRemovalObservation(event) ==
    /\ TraceCombined
    /\ event.removedNode \notin routing.inSync
    /\ routing.inSync = event.viewInSync
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

PromotionNoOpFillObservation(event) ==
    /\ TraceCombined
    /\ IF event.fillPhysical
          THEN D1ObservePromotionNoOpFill(
                   event.node, event.observedProcessed, event.term)
          ELSE D1FillPromotionNoOps(event.node, event.observedProcessed)
    /\ event.term = routing.term
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

ActivationEvent(event) ==
    /\ TraceCombined
    /\ event.term = views[event.node].term
    /\ FenceChangingReplication(D1ObserveActivation(event.node))
    /\ activated'[event.node] = event.term
    /\ UNCHANGED AuxVars

RecoverySnapshotEvent(event) ==
    /\ EnableRecovery
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
    /\ UNCHANGED AuxVars

RecoveryStartEvent(event) ==
    /\ EnableRecovery
    /\ RecoveryStable(TargetBeginInstall(event.target))
    /\ UNCHANGED AuxVars

RecoveryInstallEvent(event) ==
    /\ EnableRecovery
    /\ RecoveryInstall(D1InstallRecoverySnapshot(event.target))
    /\ nextSeq'[event.target] = event.snapshotNext
    /\ UNCHANGED AuxVars

RecoveryWalObservation(event) ==
    /\ EnableRecovery
    /\ sessionFetched[event.node] = event.writeId
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED AuxVars

RecoveryApplyEvent(event) ==
    /\ EnableRecovery
    /\ sessionFetched[event.node] = event.writeId
    /\ D1FixedRecoveryApply(event.node)
    /\ IF event.outcome = "applied_newer"
          THEN docValue'[event.node][event.doc] = event.writeId
          ELSE IF event.outcome = "stale"
               THEN docValue'[event.node][event.doc]
                    = docValue[event.node][event.doc]
               ELSE FALSE
    /\ event.writeId \in durableOps'[event.node]
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED AuxVars

RecoveryBarrierEvent(event) ==
    /\ EnableRecovery
    /\ sessionHead[event.target] = event.barrierNext
    /\ sessionCursor[event.target] = event.barrierNext
    /\ processedSeqs[event.target] = event.observedProcessed
    /\ RecoveryTargetComplete(TargetComplete(event.target))
    /\ UNCHANGED AuxVars

RecoveryMembershipEvent(event) ==
    /\ EnableRecovery
    /\ event.outcome = "admitted"
    /\ RecoveryTargetComplete(TargetObserveAdmitted(event.target))
    /\ event.target \in routing.inSync
    /\ UNCHANGED AuxVars

CoreEvent(event) ==
    CASE event.kind = "client_write_routed" -> ClientWriteEvent(event)
      [] event.kind = "wal_appended" ->
            IF event.writeId = NoWrite
                  /\ event.origin \in {"primary", "promotion"}
            THEN PromotionFillWalEvent(event)
            ELSE IF event.origin = "primary"
            THEN PrimaryWalObservation(event)
            ELSE IF event.origin = "recovery"
                 THEN RecoveryWalObservation(event)
            ELSE IF event.writeId = NoWrite
                 THEN PromotionNoOpWalObservation(event)
                 ELSE ReplicaWalObservation(event)
      [] event.kind = "operation_processed" ->
            IF event.writeId = NoWrite
                  /\ event.origin \in {"primary", "promotion"}
            THEN PromotionFillProcessEvent(event)
            ELSE IF event.origin = "primary"
            THEN PrimaryProcessEvent(event)
            ELSE IF event.origin = "recovery"
                 THEN RecoveryApplyEvent(event)
            ELSE IF event.writeId = NoWrite
                 THEN PromotionNoOpProcessEvent(event)
                 ELSE ReplicaProcessEvent(event)
      [] event.kind = "primary_replication_started" ->
            PrimaryReplicationObservation(event)
      [] event.kind = "replica_received" ->
            ReplicaReceiveObservation(event)
      [] event.kind = "replica_rejected" ->
            ReplicaRejectedEvent(event)
      [] event.kind = "replica_result" ->
            ReplicaResultEvent(event)
      [] event.kind = "client_result" ->
            ClientResultEvent(event)
      [] event.kind = "commit_captured" ->
            CommitCaptureEvent(event)
      [] event.kind = "commit_persisted" ->
            CommitPersistEvent(event)
      [] event.kind = "node_crashed" -> CrashEvent(event)
      [] event.kind = "node_restarted" -> RestartEvent(event)
      [] event.kind = "replay_started" ->
            ReplayStartObservation(event)
      [] event.kind = "replay_entry" ->
            ReplayEntryEvent(event)
      [] event.kind = "replay_finished" ->
            ReplayFinishEvent(event)
      [] event.kind = "wal_truncated" -> TruncateEvent(event)
      [] event.kind = "copy_state" ->
            CopyStateObservation(event)
      [] event.kind = "fence_persisted" ->
            FenceObservation(event)
      [] event.kind = "routing_view" ->
            RoutingViewObservation(event)
      [] event.kind = "routing_promoted" ->
            PromotionObservation(event)
      [] event.kind = "in_sync_removed" ->
            InSyncRemovalObservation(event)
      [] event.kind = "promotion_noop_fill" ->
            PromotionNoOpFillObservation(event)
      [] event.kind = "promotion_noop_replication_started" ->
            PromotionNoOpSendObservation(event)
      [] event.kind = "promotion_noop_received" ->
            PromotionNoOpReceiveObservation(event)
      [] event.kind = "promotion_noop_result" ->
            PromotionNoOpResultEvent(event)
      [] event.kind = "primary_activated" ->
            ActivationEvent(event)
      [] event.kind = "recovery_snapshot" ->
            RecoverySnapshotEvent(event)
      [] event.kind = "recovery_started" ->
            RecoveryStartEvent(event)
      [] event.kind = "recovery_installed" ->
            RecoveryInstallEvent(event)
      [] event.kind = "recovery_barrier" ->
            RecoveryBarrierEvent(event)
      [] event.kind = "recovery_membership" ->
            RecoveryMembershipEvent(event)
      [] OTHER -> FALSE

ConsumeEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ CoreEvent(event)
    /\ tracePos' = tracePos + 1
    /\ hiddenSteps' = 0
    /\ UNCHANGED finished

\* Hidden actions are selected by the next observation. This retains the real
\* D1/Raft actions while eliminating unrelated node, command, message, and
\* replay-position choices.
PromotionCommandMatches(command, event) ==
    /\ command.kind = "UpdateRouting"
    /\ command.actor = event.emitter
    /\ command.target = routing.primary
    /\ command.newPrimary = event.newPrimary

FailureCommandMatches(command, event) ==
    /\ command.kind = "FailShardCopy"
    /\ command.target = event.removedNode
    /\ command.expectedAllocation = event.removedAllocation

DesiredActivationTerm(event) ==
    IF event.kind = "routing_view" THEN event.viewTerm ELSE event.term

ActivationCommandMatches(command, event) ==
    /\ command.kind = "ActivatePrimary"
    /\ command.target = event.node
    /\ command.expectedTerm + 1 = DesiredActivationTerm(event)

HiddenReplayAction(event) ==
    /\ event.kind \in {"replay_entry", "replay_finished"}
    /\ event.walPosition > replayPos[event.node]
    /\ D1SkipTruncatedReplayPrefix(event.node, event.walPosition)

HiddenBatchPlanning(event) ==
    /\ event.kind \in {"operation_processed", "replay_entry"}
    /\ event.outcome # "collision"
    /\ event.maxNext > maxSeqNext[event.node]
    /\ D1ObserveBatchPlan(event.node, event.maxNext)
    /\ UNCHANGED AuxVars

HiddenCoreMaintenance(event) ==
    /\ ~TraceCombined
    /\ \/ /\ event.kind \in {"node_crashed", "copy_state"}
          /\ \/ D1ScenarioAgeTombstone
             \/ D1ScenarioPruneTombstone
       \/ /\ event.kind = "replica_received"
          /\ event.writeId = 2
          /\ event.node = ReplicaNode
          /\ D1ScenarioRedeliver

HiddenPromotionAction(event) ==
    /\ TraceCombined
    /\ event.kind = "routing_promoted"
    /\ \/ /\ raftLeader # event.emitter
          /\ FaultAction(ElectLeader(event.emitter))
       \/ /\ raftLeader = event.emitter
          /\ routing.primary # event.newPrimary
          /\ ~(\E command \in pendingRaft :
                    PromotionCommandMatches(command, event))
          /\ FaultAction(
                SuspectAndRemove(
                    event.emitter, routing.primary, event.newPrimary))
       \/ \E command \in pendingRaft :
              /\ PromotionCommandMatches(command, event)
              /\ FenceChangingReplication(CommitRaft(command))

HiddenActivationAction(event) ==
    /\ TraceCombined
    /\ event.kind \in {"routing_view", "fence_persisted", "primary_activated"}
    /\ event.node \in Nodes
    /\ DesiredActivationTerm(event) > views[event.node].term
    /\ \/ /\ raftLeader \notin LiveConnectedVoters
          /\ FaultAction(ElectLeader(event.node))
       \/ /\ activationPending[event.node] = NoTerm
          /\ views[event.node].primary = event.node
          /\ StableReplication(ProposeActivate(event.node))
       \/ \E command \in pendingRaft :
              /\ ActivationCommandMatches(command, event)
              /\ FenceChangingReplication(CommitRaft(command))

HiddenViewDelivery(event) ==
    /\ TraceCombined
    /\ event.kind = "routing_view"
    /\ applied[event.node] < Len(raftLog)
    /\ ViewMatches(raftLog[applied[event.node] + 1].state, event)
    /\ FenceChangingReplication(DeliverView(event.node))

HiddenRemovalAction(event) ==
    /\ TraceCombined
    /\ event.kind = "in_sync_removed"
    /\ \/ /\ ~(\E command \in pendingRaft :
                    FailureCommandMatches(command, event))
          /\ FaultAction(
                ReportShardCopyFailure(event.removedNode, NoNode))
       \/ \E command \in pendingRaft :
              /\ FailureCommandMatches(command, event)
              /\ FenceChangingReplication(CommitRaft(command))

HiddenRecoveryAction(event) ==
    /\ EnableRecovery
    /\ event.kind \in
          {"recovery_snapshot", "recovery_started", "recovery_installed",
           "recovery_barrier", "recovery_membership", "routing_view",
           "wal_appended", "operation_processed"}
    /\ \/ \E target \in Nodes, source \in Nodes :
              /\ event.kind = "recovery_snapshot"
              /\ target = event.target
              /\ source = event.source
              /\ RecoveryAction(StartRecovery(target, source))
       \/ \E leader \in Nodes, target \in Nodes :
              /\ event.kind = "routing_view"
              /\ FaultAction(AllocateAfterLifecycle(leader, target))
       \/ \E target \in Nodes :
              /\ event.kind \in {"routing_view", "recovery_snapshot"}
              /\ FaultAction(ObserveAllocationAccepted(target))
       \/ \E target \in Nodes :
              /\ event.kind \in
                    {"wal_appended", "operation_processed", "recovery_barrier"}
              /\ RecoveryStable(FetchOps(target))
       \/ \E target \in Nodes :
              /\ event.kind = "recovery_barrier"
              /\ RecoveryStable(FinishCatchUp(target))
       \/ \E target \in Nodes :
              /\ event.kind = "recovery_barrier"
              /\ RecoveryStable(BeginPrepareFinalize(target))
       \/ \E target \in Nodes :
              /\ event.kind = "recovery_barrier"
              /\ RecoveryStable(AcquireFinalizeBarrier(target))
       \/ \E target \in Nodes :
              /\ event.kind = "recovery_barrier"
              /\ RecoveryStable(FinishFinalizeTail(target))
       \/ \E target \in Nodes :
              /\ event.kind \in {"routing_view", "recovery_membership"}
              /\ RecoveryStable(BeginSettlement(target))
       \/ \E target \in Nodes :
              /\ event.kind \in {"routing_view", "recovery_membership"}
              /\ RecoveryStable(ProposeMarkInSync(target))
       \/ \E command \in pendingRaft :
              /\ event.kind \in {"routing_view", "recovery_membership"}
              /\ FenceChangingReplication(CommitRaft(command))
       \/ \E node \in Nodes :
              /\ event.kind = "routing_view"
              /\ FenceChangingReplication(DeliverView(node))
       \/ \E target \in Nodes :
              /\ event.kind = "recovery_membership"
              /\ RecoveryObserveAdmission(ObserveAdmission(target))

HiddenD1Action(event) ==
    \/ HiddenBatchPlanning(event)
    \/ HiddenReplayAction(event)
    \/ HiddenCoreMaintenance(event)
    \/ HiddenPromotionAction(event)
    \/ HiddenActivationAction(event)
    \/ HiddenViewDelivery(event)
    \/ HiddenRemovalAction(event)
    \/ HiddenRecoveryAction(event)

HiddenStep ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ hiddenSteps < MaxHiddenSteps
    /\ HiddenD1Action(event)
    /\ hiddenSteps' = hiddenSteps + 1
    /\ UNCHANGED
          <<tracePos, finished, captureActive, captureBoundary,
            capturePersisted, captureMax, captureOps, captureDocValue,
            captureDocSeqNext, captureTombstoneSeqNext,
            replicaResponsePersisted>>

FinishTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ ~(\E node \in Nodes : captureActive[node])
    /\ (~TraceQuiescent \/ messages = {})
    /\ finished' = TRUE
    /\ UNCHANGED
          <<d1vars, tracePos, hiddenSteps, captureActive, captureBoundary,
            capturePersisted, captureMax, captureOps, captureDocValue, captureDocSeqNext,
            captureTombstoneSeqNext, replicaResponsePersisted>>

TraceNext ==
    \/ ConsumeEvent
    \/ HiddenStep
    \/ FinishTrace

TraceSpec ==
    /\ TraceInit
    /\ [][TraceNext]_traceVars

TraceTypeOK ==
    /\ D1TypeOK
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ hiddenSteps \in 0..MaxHiddenSteps
    /\ finished \in BOOLEAN
    /\ captureActive \in [Nodes -> BOOLEAN]
    /\ captureBoundary \in [Nodes -> 0..MaxWrites]
    /\ capturePersisted \in [Nodes -> 0..MaxWrites]
    /\ captureMax \in [Nodes -> 0..MaxWrites]
    /\ captureOps \in [Nodes -> SUBSET WriteIds]
    /\ captureDocValue \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ captureDocSeqNext \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ captureTombstoneSeqNext \in
          [Nodes -> [Docs -> 0..MaxWrites]]
    /\ replicaResponsePersisted \in
          [WriteIds -> [Nodes -> 0..MaxWrites]]

TraceCoreSafety ==
    /\ RoutingWellFormed
    /\ NoCopyBehindAcked
    /\ D1NoAcknowledgedDeleteResurrection
    /\ D1ProcessedCheckpointGapAware
    /\ D1PersistedCheckpointGapAware
    /\ D1WalHasNoDuplicateSeq
    /\ PromotionComplete
    /\ AdmissionComplete
    /\ NoApplyBelowObservedFence
    /\ ActivePrimaryRejectsOldTerm
    /\ (finished /\ TraceQuiescent => D1QuiescentConvergence)

\* Validation is existential: TLC must find a state that violates this
\* invariant by reaching the end of the trace.
TraceNotAccepted ==
    ~finished

=============================================================================
