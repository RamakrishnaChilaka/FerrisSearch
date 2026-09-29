-------------------------- MODULE TraceD1Collision --------------------------
\* Trace composition for the bounded B1 term/sequence collision slice.
\* The protocol transitions are exactly MC_D1_TermCollision actions.

EXTENDS MC_D1_TermCollision, TraceInput

VARIABLES
    tracePos,
    finished,
    observedFenceTerms

TraceCollisionVars ==
    <<tracePos, finished, observedFenceTerms>>

traceCollisionVars == <<vars, TraceCollisionVars>>

TraceCollisionInit ==
    /\ B1Init
    /\ tracePos = 1
    /\ finished = FALSE
    /\ observedFenceTerms = [node \in Nodes |-> {1}]

WalObservation(event) ==
    /\ event.seq = CollisionSeq
    /\ event.term = 1
    /\ event.node = R2
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

OldApplyEvent(event) ==
    /\ event.seq = CollisionSeq
    /\ event.term = 1
    /\ event.node = R2
    /\ event.outcome = "applied_newer"
    /\ B1OldWritePartiallyReplicated
    /\ UNCHANGED observedFenceTerms

PromotionEvent(event) ==
    /\ event.newPrimary = R1
    /\ event.term = 2
    /\ B1PromoteR1
    /\ UNCHANGED observedFenceTerms

FenceObservation(event) ==
    /\ event.term >= fenceTerm[event.node]
    /\ event.fenceMaxNext = maxSeqNo[event.node] + 1
    /\ observedFenceTerms' =
          [observedFenceTerms EXCEPT ![event.node] = @ \cup {event.term}]
    /\ UNCHANGED vars

CollisionEvent(event) ==
    /\ event.node = R2
    /\ event.term = 2
    /\ event.seq = CollisionSeq
    /\ event.term \in observedFenceTerms[R2]
    /\ event.outcome = "collision"
    /\ B1TermAwareNewWrite
    /\ UNCHANGED observedFenceTerms

InSyncRemovalObservation(event) ==
    /\ event.removedNode = R2
    /\ R2 \notin inSync
    /\ R2 \in failed
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

RoutingViewObservation(event) ==
    /\ event.viewPrimary = primary
    /\ event.viewTerm = primaryTerm
    /\ event.viewInSync = inSync
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

CopyStateObservation(event) ==
    /\ event.node = R1
    /\ event.docTerm[event.doc] = 2
    /\ docOp[R1] = NewOp
    /\ UNCHANGED vars
    /\ UNCHANGED observedFenceTerms

CollisionTraceEvent(event) ==
    CASE event.kind = "wal_appended" -> WalObservation(event)
      [] event.kind = "operation_processed" ->
            IF event.term = 1
            THEN OldApplyEvent(event)
            ELSE CollisionEvent(event)
      [] event.kind = "routing_promoted" -> PromotionEvent(event)
      [] event.kind = "fence_persisted" -> FenceObservation(event)
      [] event.kind = "in_sync_removed" ->
            InSyncRemovalObservation(event)
      [] event.kind = "copy_state" -> CopyStateObservation(event)
      [] event.kind = "routing_view" ->
            RoutingViewObservation(event)
      [] OTHER -> FALSE

ConsumeCollisionEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ CollisionTraceEvent(event)
    /\ tracePos' = tracePos + 1
    /\ UNCHANGED finished

FinishCollisionTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ finished' = TRUE
    /\ UNCHANGED <<vars, tracePos, observedFenceTerms>>

TraceCollisionNext ==
    \/ ConsumeCollisionEvent
    \/ FinishCollisionTrace

TraceCollisionSpec ==
    /\ TraceCollisionInit
    /\ [][TraceCollisionNext]_traceCollisionVars

TraceCollisionTypeOK ==
    /\ B1TypeOK
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ finished \in BOOLEAN
    /\ observedFenceTerms \in [Nodes -> SUBSET Nat]

TraceCollisionSafety ==
    /\ B1CollisionFailsClosed
    /\ B1RecoveredBeforePromotion

TraceCollisionNotAccepted ==
    ~finished

=============================================================================
