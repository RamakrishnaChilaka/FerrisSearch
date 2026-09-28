----------------------------- MODULE TraceD1 -----------------------------
\* Existential implementation-trace composition for the real D1 actions.
\*
\* TraceInput.tla is generated from one schema-v2 JSONL file.  Low-level
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
    captureMax,
    captureOps,
    captureDocValue,
    captureDocSeqNext,
    captureTombstoneSeqNext

TraceVars ==
    <<tracePos, hiddenSteps, finished, captureActive, captureBoundary,
      captureMax, captureOps, captureDocValue, captureDocSeqNext,
      captureTombstoneSeqNext>>

CaptureVars ==
    <<captureActive, captureBoundary, captureMax, captureOps,
      captureDocValue, captureDocSeqNext, captureTombstoneSeqNext>>

traceVars == <<d1vars, TraceVars>>

EmptyCapturedOps ==
    [node \in Nodes |-> {}]

EmptyCapturedDocValue ==
    [node \in Nodes |-> [doc \in Docs |-> NoWrite]]

EmptyCapturedDocSeq ==
    [node \in Nodes |-> [doc \in Docs |-> 0]]

TraceInit ==
    /\ D1Init
    /\ tracePos = 1
    /\ hiddenSteps = 0
    /\ finished = FALSE
    /\ captureActive = [node \in Nodes |-> FALSE]
    /\ captureBoundary = [node \in Nodes |-> 0]
    /\ captureMax = [node \in Nodes |-> 0]
    /\ captureOps = EmptyCapturedOps
    /\ captureDocValue = EmptyCapturedDocValue
    /\ captureDocSeqNext = EmptyCapturedDocSeq
    /\ captureTombstoneSeqNext = EmptyCapturedDocSeq

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

NackFor(writeId, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "ReplicaNack"
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

HasNack(writeId, replica) ==
    \E message \in messages :
        /\ message.kind = "ReplicaNack"
        /\ message.write = writeId
        /\ message.from = replica

CheckpointMatches(node, event) ==
    /\ processedNext[node] = event.processedNext
    /\ persistedProcessedNext[node] = event.persistedNext
    /\ maxSeqNext[node] = event.maxNext

CheckpointMatchesPrime(node, event) ==
    /\ processedNext'[node] = event.processedNext
    /\ persistedProcessedNext'[node] = event.persistedNext
    /\ maxSeqNext'[node] = event.maxNext

LiveCheckpointMatches(node, event) ==
    /\ processedNext[node] = event.processedNext
    /\ maxSeqNext[node] = event.maxNext

LiveCheckpointMatchesPrime(node, event) ==
    /\ processedNext'[node] = event.processedNext
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

ClientWriteEvent(event) ==
    /\ event.writeId = nextWrite
    /\ D1ClientWriteFrom(event.node, event.doc, event.writeKind)
    /\ writeTarget'[event.writeId] = event.peer
    /\ UNCHANGED CaptureVars

\* WAL and primary planner observations precede the atomic D1PrimaryAccept
\* abstraction.  They may stutter only while that exact real action is enabled.
PrimaryWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ writeStatus[event.writeId] = "Routed"
    /\ CanPrimaryAccept(event.writeId)
    /\ event.term = views[event.node].term
    /\ event.seq = nextSeq[event.node]
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

PrimaryProcessObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ writeStatus[event.writeId] = "Routed"
    /\ CanPrimaryAccept(event.writeId)
    /\ event.outcome = "applied_newer"
    /\ event.term = views[event.node].term
    /\ event.seq = nextSeq[event.node]
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

PrimaryAcceptEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ D1PrimaryAccept(event.writeId)
    /\ writePrimary'[event.writeId] = event.node
    /\ writeSeq'[event.writeId] = event.seq
    /\ writeTerm'[event.writeId] = event.term
    /\ writeRequired'[event.writeId] = event.required
    /\ event.required = views[event.node].inSync
    /\ event.writeId \in durableOps'[event.node]
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED CaptureVars

ReplicaReceiveObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ LET message == MessageFor(event.writeId, event.node)
       IN /\ message.from = event.peer
          /\ message.term = event.term
          /\ message.seq = event.seq
          /\ message.targetAllocation = event.allocation
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

ReplicaWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ LET message == MessageFor(event.writeId, event.node)
       IN /\ D1ReplicaMessageEnabled(message)
          /\ message.term >= durableReplicaFence[event.node]
          /\ IF message.term > durableReplicaFence[event.node]
                THEN event.fenceObservedTerm = message.term
                ELSE TRUE
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

ReplicaProcessEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ HasMessage(event.writeId, event.node)
    /\ LET message == MessageFor(event.writeId, event.node)
           beforeDoc == docValue[event.node][event.doc]
       IN /\ message.from = event.peer
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
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED CaptureVars

ReplicaResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              /\ HasAck(event.writeId, event.peer)
              /\ D1DeliverAck(AckFor(event.writeId, event.peer))
       [] event.outcome = "failed" ->
              /\ HasNack(event.writeId, event.peer)
              /\ DeliverReplicaNack(NackFor(event.writeId, event.peer))
              /\ UNCHANGED D1Vars
       [] OTHER -> FALSE
    /\ UNCHANGED CaptureVars

ClientResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              D1PrimaryAck(event.writeId)
       [] event.outcome = "failed" ->
              /\ \/ PrimaryFail(event.writeId)
                 \/ PrimaryReject(event.writeId)
              /\ UNCHANGED D1Vars
       [] OTHER -> FALSE
    /\ UNCHANGED CaptureVars

CommitCaptureEvent(event) ==
    /\ event.node \in Nodes
    /\ alive[event.node]
    /\ LiveCheckpointMatches(event.node, event)
    /\ ~captureActive[event.node]
    /\ captureActive' =
          [captureActive EXCEPT ![event.node] = TRUE]
    /\ captureBoundary' =
          [captureBoundary EXCEPT ![event.node] = event.processedNext]
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

CommitPersistEvent(event) ==
    /\ event.node \in Nodes
    /\ captureActive[event.node]
    /\ event.processedNext = captureBoundary[event.node]
    /\ event.maxNext = captureMax[event.node]
    /\ D1PersistBoundary(
           event.node,
           captureBoundary[event.node],
           captureMax[event.node],
           captureOps[event.node],
           captureDocValue[event.node],
           captureDocSeqNext[event.node],
           captureTombstoneSeqNext[event.node],
           TRUE)
    /\ captureActive' =
          [captureActive EXCEPT ![event.node] = FALSE]
    /\ UNCHANGED
          <<captureBoundary, captureMax, captureOps, captureDocValue,
            captureDocSeqNext,
            captureTombstoneSeqNext>>

CrashEvent(event) ==
    /\ event.node = ReplicaNode
    /\ D1CrashReplica
    /\ UNCHANGED CaptureVars

RestartEvent(event) ==
    /\ event.node = ReplicaNode
    /\ D1RestartReplica
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED CaptureVars

ReplayStartObservation(event) ==
    /\ event.node = ReplicaNode
    /\ replaying[event.node]
    /\ replayBoundary[event.node] = event.processedNext
    /\ LiveCheckpointMatches(event.node, event)
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

ReplayEntryEvent(event) ==
    /\ event.node = ReplicaNode
    /\ replaying[event.node]
    /\ replayPos[event.node] <= Len(walOrder[event.node])
    /\ walOrder[event.node][replayPos[event.node]] = event.writeId
    /\ CASE event.outcome = "skip_committed" ->
              D1ReplaySkip
       [] event.outcome \in {"applied_newer", "stale", "redelivery", "noop"} ->
              LET beforeDoc == docValue[event.node][event.doc]
              IN /\ D1FixedReplayApply
                 /\ IF event.outcome = "applied_newer"
                       THEN docValue'[event.node][event.doc] = event.writeId
                       ELSE IF event.outcome = "stale"
                            THEN docValue'[event.node][event.doc] = beforeDoc
                            ELSE IF event.outcome = "redelivery"
                                 THEN docValue'[event.node][event.doc] = beforeDoc
                                 ELSE TRUE
       [] OTHER -> FALSE
    /\ LiveCheckpointMatchesPrime(event.node, event)
    /\ UNCHANGED CaptureVars

ReplayFinishEvent(event) ==
    /\ event.node = ReplicaNode
    /\ CASE event.outcome = "completed" ->
              /\ D1FinishReplay
              /\ replaySafe'
       [] event.outcome = "failed" ->
              D1FailReplayAt(event.node)
       [] OTHER -> FALSE
    /\ UNCHANGED CaptureVars

TruncateEvent(event) ==
    /\ D1RecordTruncation(event.node, event.truncateNext)
    /\ UNCHANGED CaptureVars

CopyStateObservation(event) ==
    /\ event.node \in Nodes
    /\ CopyStateMatches(event.node, event)
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

FenceObservation(event) ==
    /\ event.node \in Nodes
    /\ event.term > durableReplicaFence[event.node]
    /\ event.fenceMaxNext =
          IF maxSeqNext[event.node] > nextSeq[event.node]
          THEN maxSeqNext[event.node]
          ELSE nextSeq[event.node]
    /\ UNCHANGED d1vars
    /\ UNCHANGED CaptureVars

CoreEvent(event) ==
    CASE event.kind = "client_write_routed" -> ClientWriteEvent(event)
      [] event.kind = "wal_appended" ->
            IF event.origin = "primary"
            THEN PrimaryWalObservation(event)
            ELSE ReplicaWalObservation(event)
      [] event.kind = "operation_processed" ->
            IF event.origin = "primary"
            THEN PrimaryProcessObservation(event)
            ELSE ReplicaProcessEvent(event)
      [] event.kind = "primary_replication_started" ->
            PrimaryAcceptEvent(event)
      [] event.kind = "replica_received" ->
            ReplicaReceiveObservation(event)
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
      [] OTHER -> FALSE

ConsumeEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ CoreEvent(event)
    /\ tracePos' = tracePos + 1
    /\ hiddenSteps' = 0
    /\ UNCHANGED finished

\* Real unobserved D1 actions only.  These are bounded between observations.
HiddenD1Action ==
    \/ D1ScenarioAgeTombstone
    \/ D1ScenarioPruneTombstone
    \/ D1ScenarioRedeliver

HiddenStep ==
    /\ tracePos <= Len(Trace)
    /\ hiddenSteps < MaxHiddenSteps
    /\ HiddenD1Action
    /\ hiddenSteps' = hiddenSteps + 1
    /\ UNCHANGED
          <<tracePos, finished, captureActive, captureBoundary, captureMax,
            captureOps, captureDocValue, captureDocSeqNext,
            captureTombstoneSeqNext>>

FinishTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ ~(\E node \in Nodes : captureActive[node])
    /\ finished' = TRUE
    /\ UNCHANGED
          <<d1vars, tracePos, hiddenSteps, captureActive, captureBoundary,
            captureMax, captureOps, captureDocValue, captureDocSeqNext,
            captureTombstoneSeqNext>>

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
    /\ captureMax \in [Nodes -> 0..MaxWrites]
    /\ captureOps \in [Nodes -> SUBSET WriteIds]
    /\ captureDocValue \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ captureDocSeqNext \in [Nodes -> [Docs -> 0..MaxWrites]]
    /\ captureTombstoneSeqNext \in
          [Nodes -> [Docs -> 0..MaxWrites]]

\* Validation is existential: TLC must find a state that violates this
\* invariant by reaching the end of the trace.
TraceNotAccepted ==
    ~finished

=============================================================================
