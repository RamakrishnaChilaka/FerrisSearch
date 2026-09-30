-------------------------- MODULE TraceD1Authority --------------------------
\* Exact composition for routing-view, promotion, durable-fence, activation,
\* and pre-activation write observations.  All protocol transitions are
\* actions from Invariants/ShardReplication/Faults.

EXTENDS Invariants, TraceInput

VARIABLES
    tracePos,
    hiddenSteps,
    finished,
    observedFenceTerms

TraceAuthorityVars ==
    <<tracePos, hiddenSteps, finished, observedFenceTerms>>

traceAuthorityVars == <<vars, TraceAuthorityVars>>

TraceAuthorityInit ==
    /\ Init
    /\ tracePos = 1
    /\ hiddenSteps = 0
    /\ finished = FALSE
    /\ observedFenceTerms = [node \in Nodes |-> {1}]

ViewMatches(view, event) ==
    /\ view.primary = event.viewPrimary
    /\ view.term = event.viewTerm
    /\ view.inSync = event.viewInSync
    /\ view.allocations = event.viewAllocations
    /\ view.initialized = event.initialized

CopyStateMatches(node, event) ==
    /\ docValue[node] = event.docValue
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
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

FenceChangingReplication(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

FaultAction(action) ==
    /\ action
    /\ UNCHANGED ApplySafetyVars

CrashEvent(event) ==
    /\ WritesOwnedBy(event.node) = event.crashFailedWrites
    /\ DropNodeMessages(event.node) = event.crashDroppedMessages
    /\ FaultAction(Crash(event.node))
    /\ UNCHANGED observedFenceTerms

PromotionObservation(event) ==
    /\ routing.primary = event.newPrimary
    /\ routing.term = event.term
    /\ routing.inSync = event.viewInSync
    /\ raftLeader = event.emitter
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

RoutingViewEvent(event) ==
    /\ ViewMatches(views[event.node], event)
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

FenceObservation(event) ==
    /\ durableReplicaFence[event.node] = event.term
    /\ observedFenceTerms' =
          [observedFenceTerms EXCEPT ![event.node] = @ \cup {event.term}]
    /\ UNCHANGED vars

ActivationEvent(event) ==
    /\ event.term \in observedFenceTerms[event.node]
    /\ FenceChangingReplication(ObserveActivation(event.node))
    /\ activated'[event.node] = event.term
    /\ durableReplicaFence'[event.node] = event.term
    /\ UNCHANGED observedFenceTerms

ClientWriteEvent(event) ==
    /\ event.writeId = nextWrite
    /\ StableReplication(
           ClientWrite(event.node, event.doc, event.writeKind))
    /\ writeTarget'[event.writeId] = event.peer
    /\ UNCHANGED observedFenceTerms

PrimaryWalObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ CanPrimaryAccept(event.writeId)
    /\ IF RequestDurability THEN event.durable ELSE TRUE
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

PrimaryProcessEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ event.node = writeTarget[event.writeId]
    /\ event.outcome = "applied_newer"
    /\ StableReplication(PrimaryAccept(event.writeId))
    /\ writePrimary'[event.writeId] = event.node
    /\ writeSeq'[event.writeId] = event.seq
    /\ writeTerm'[event.writeId] = event.term
    /\ event.writeId \in durableOps'[event.node]
    /\ UNCHANGED observedFenceTerms

PrimaryReplicationObservation(event) ==
    /\ event.writeId \in WriteIds
    /\ writeStatus[event.writeId] = "Replicating"
    /\ writePrimary[event.writeId] = event.node
    /\ writeSeq[event.writeId] = event.seq
    /\ writeTerm[event.writeId] = event.term
    /\ writeRequired[event.writeId] = event.required
    /\ event.required = views[event.node].inSync
    /\ {message \in messages :
            /\ message.kind = "Replicate"
            /\ message.write = event.writeId}
          = event.requiredMessages
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

ClientResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              StableReplication(PrimaryAck(event.writeId))
       [] event.outcome = "failed" ->
              IF event.preWalVersionConflict
              THEN StableReplication(PrimaryVersionConflict(event.writeId))
              ELSE \/ StableReplication(PrimaryFail(event.writeId))
                   \/ StableReplication(PrimaryReject(event.writeId))
       [] OTHER -> FALSE
    /\ UNCHANGED observedFenceTerms

CopyStateObservation(event) ==
    /\ CopyStateMatches(event.node, event)
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

AuthorityEvent(event) ==
    CASE event.kind = "node_crashed" -> CrashEvent(event)
      [] event.kind = "routing_promoted" ->
            PromotionObservation(event)
      [] event.kind = "routing_view" -> RoutingViewEvent(event)
      [] event.kind = "fence_persisted" -> FenceObservation(event)
      [] event.kind = "primary_activated" -> ActivationEvent(event)
      [] event.kind = "client_write_routed" -> ClientWriteEvent(event)
      [] event.kind = "wal_appended" -> PrimaryWalObservation(event)
      [] event.kind = "operation_processed" ->
            PrimaryProcessEvent(event)
      [] event.kind = "primary_replication_started" ->
            PrimaryReplicationObservation(event)
      [] event.kind = "client_result" -> ClientResultEvent(event)
      [] event.kind = "copy_state" -> CopyStateObservation(event)
      [] OTHER -> FALSE

ConsumeAuthorityEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ AuthorityEvent(event)
    /\ tracePos' = tracePos + 1
    /\ hiddenSteps' = 0
    /\ UNCHANGED finished

PromotionCommandMatches(command, event) ==
    /\ command.kind = "UpdateRouting"
    /\ command.actor = event.emitter
    /\ command.target = routing.primary
    /\ command.newPrimary = event.newPrimary

DesiredActivationTerm(event) ==
    IF event.kind = "routing_view" THEN event.viewTerm ELSE event.term

ActivationCommandMatches(command, event) ==
    /\ command.kind = "ActivatePrimary"
    /\ command.target = event.node
    /\ command.expectedTerm + 1 = DesiredActivationTerm(event)

HiddenPromotionAction(event) ==
    /\ event.kind = "routing_promoted"
    /\ \/ /\ raftLeader # event.emitter
          /\ raftLeader \in LiveConnectedVoters
          /\ FaultAction(PartitionMetadata(raftLeader))
       \/ /\ raftLeader # event.emitter
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
    /\ event.kind \in {"routing_view", "fence_persisted", "primary_activated"}
    /\ DesiredActivationTerm(event) > views[event.node].term
    /\ \/ /\ activationPending[event.node] = NoTerm
          /\ views[event.node].primary = event.node
          /\ StableReplication(ProposeActivate(event.node))
       \/ \E command \in pendingRaft :
              /\ ActivationCommandMatches(command, event)
              /\ FenceChangingReplication(CommitRaft(command))

HiddenViewDelivery(event) ==
    /\ event.kind = "routing_view"
    /\ applied[event.node] < Len(raftLog)
    /\ ViewMatches(raftLog[applied[event.node] + 1].state, event)
    /\ FenceChangingReplication(DeliverView(event.node))

HiddenAuthorityAction(event) ==
    \/ HiddenPromotionAction(event)
    \/ HiddenActivationAction(event)
    \/ HiddenViewDelivery(event)

HiddenAuthorityStep ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ hiddenSteps < MaxHiddenSteps
    /\ HiddenAuthorityAction(event)
    /\ hiddenSteps' = hiddenSteps + 1
    /\ UNCHANGED <<tracePos, finished, observedFenceTerms>>

FinishAuthorityTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ (~TraceQuiescent \/ messages = {})
    /\ finished' = TRUE
    /\ UNCHANGED <<vars, tracePos, hiddenSteps, observedFenceTerms>>

TraceAuthorityNext ==
    \/ ConsumeAuthorityEvent
    \/ HiddenAuthorityStep
    \/ FinishAuthorityTrace

TraceAuthoritySpec ==
    /\ TraceAuthorityInit
    /\ [][TraceAuthorityNext]_traceAuthorityVars

TraceAuthorityTypeOK ==
    /\ TypeOK
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ hiddenSteps \in 0..MaxHiddenSteps
    /\ finished \in BOOLEAN
    /\ observedFenceTerms \in [Nodes -> SUBSET Nat]

TraceAuthoritySafety ==
    /\ RoutingWellFormed
    /\ InitializationMonotonic
    /\ InitializationBeforeAcknowledgement
    /\ NoAckedLoss
    /\ PromotionComplete
    /\ AdmissionComplete
    /\ NoPartialServe
    /\ NoApplyBelowObservedFence
    /\ ActivePrimaryRejectsOldTerm

TraceAuthorityNotAccepted ==
    ~finished

=============================================================================
