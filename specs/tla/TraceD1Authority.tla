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
    /\ \/ /\ ViewMatches(views[event.node], event)
          /\ UNCHANGED vars
       \/ /\ FenceChangingReplication(DeliverView(event.node))
          /\ ViewMatches(views'[event.node], event)
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
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

ClientResultEvent(event) ==
    /\ event.writeId \in WriteIds
    /\ CASE event.outcome = "acknowledged" ->
              StableReplication(PrimaryAck(event.writeId))
       [] event.outcome = "failed" ->
              \/ StableReplication(PrimaryFail(event.writeId))
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

HiddenAuthorityAction ==
    \/ \E candidate \in Nodes : FaultAction(ElectLeader(candidate))
    \/ \E node \in Nodes : FaultAction(PartitionMetadata(node))
    \/ \E leader \in Nodes, node \in Nodes, candidate \in Nodes :
           FaultAction(SuspectAndRemove(leader, node, candidate))
    \/ \E command \in pendingRaft :
           FenceChangingReplication(CommitRaft(command))
    \/ \E node \in Nodes :
           FenceChangingReplication(DeliverView(node))
    \/ \E node \in Nodes :
           StableReplication(ProposeActivate(node))

HiddenAuthorityStep ==
    /\ tracePos <= Len(Trace)
    /\ hiddenSteps < MaxHiddenSteps
    /\ HiddenAuthorityAction
    /\ hiddenSteps' = hiddenSteps + 1
    /\ UNCHANGED <<tracePos, finished, observedFenceTerms>>

FinishAuthorityTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
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
