----------------------------- MODULE TraceD1 -----------------------------
\* Trace validator for the D1 implementation event schema.
\*
\* TraceInput.tla is generated from one JSONL trace.  This module projects the
\* D1 model onto observable protocol state.  Each trace record either advances
\* the projected state or records the schema step at which no D1 transition is
\* enabled.  Unobserved scheduling, Raft delivery, and storage bookkeeping are
\* collapsed into the fixed event macros documented in trace/SCHEMA.md.

EXTENDS Naturals, Sequences, FiniteSets, TLC, TraceInput

NoNode == "NO_NODE"
NoRequest == "NO_REQUEST"
NoKey == "NO_KEY"
NoDoc == "NO_DOC"
NoHash == "NO_HASH"
NoReplay == "NO_REPLAY"

RequestStates == {"Unused", "Routed", "Replicating", "Acknowledged", "Failed"}
RecoveryStates == {"none", "recovering", "pending", "rejected"}

VARIABLES
    tracePos,
    failedStep,
    failedEvent,
    finished,
    model

vars == <<tracePos, failedStep, failedEvent, finished, model>>

Prefix(boundary) ==
    IF boundary = 0 THEN {} ELSE 0..(boundary - 1)

NatMax(left, right) ==
    IF left >= right THEN left ELSE right

ContiguousNext(values) ==
    CHOOSE boundary \in 0..(MaxSeqBound + 1) :
        /\ Prefix(boundary) \subseteq values
        /\ (boundary = MaxSeqBound + 1 \/ boundary \notin values)

ProcessedNext(m, node) ==
    ContiguousNext(m.processed[node])

PersistedNext(m, node) ==
    ContiguousNext(m.persisted[node])

WalSeqNexts(m, node) ==
    {OpSeq[key] + 1 : key \in m.walOps[node]}

WalMaxNext(m, node) ==
    LET values == WalSeqNexts(m, node)
    IN IF values = {}
       THEN 0
       ELSE CHOOSE maximum \in values :
                \A value \in values : value <= maximum

LocalMaxNext(m, node) ==
    NatMax(m.maxNext[node], WalMaxNext(m, node))

WalPosition(m, node, key) ==
    CHOOSE position \in 1..Len(m.walOrder[node]) :
        m.walOrder[node][position] = key

AvailableCopies(m) ==
    {node \in Nodes :
        /\ m.alive[node]
        /\ m.copyExists[node]
        /\ ~m.replaying[node]
        /\ m.recoveryState[node] = "none"
        /\ \/ node \in m.inSync
           \/ /\ node = m.primary
              /\ m.activatedTerm[node] = m.primaryTerm}

NoCopyBehindAcked(m) ==
    \A key \in m.ackedOps :
        \/ OpKind[key] = "noop"
        \/ \A node \in AvailableCopies(m) :
               m.docSeqNext[node][OpDoc[key]] >= OpSeq[key] + 1

CheckpointSetsCoherent(m) ==
    \A node \in Nodes :
        /\ m.persisted[node] \subseteq m.processed[node]
        /\ ProcessedNext(m, node) <= m.maxNext[node]
        /\ PersistedNext(m, node) <= ProcessedNext(m, node)

WalSequenceIdentityUnique(m) ==
    \A node \in Nodes :
      \A first \in m.walOps[node] :
        \A second \in m.walOps[node] :
            OpSeq[first] = OpSeq[second] => first = second

Safety(m) ==
    /\ NoCopyBehindAcked(m)
    /\ CheckpointSetsCoherent(m)
    /\ WalSequenceIdentityUnique(m)

InitialRequestStatus ==
    [request \in Requests |-> "Unused"]

InitialRequestNode ==
    [request \in Requests |-> NoNode]

InitialRequestTerm ==
    [request \in Requests |-> 0]

InitialRequestKey ==
    [request \in Requests |-> NoKey]

InitialRequestDoc ==
    [request \in Requests |-> NoDoc]

InitialRequestKind ==
    [request \in Requests |-> "none"]

InitialRequestHash ==
    [request \in Requests |-> NoHash]

InitialRequestSets ==
    [request \in Requests |-> {}]

InitialRequiredValues ==
    [request \in Requests |-> [node \in Nodes |-> 0]]

InitialNodeKeySets ==
    [node \in Nodes |-> {}]

InitialNodeKeySequences ==
    [node \in Nodes |-> <<>>]

InitialTermProcessed ==
    [node \in Nodes |-> {}]

InitialReplayId ==
    [node \in Nodes |-> NoReplay]

InitialReplayNat ==
    [node \in Nodes |-> 0]

InitialRecoveryState ==
    [node \in Nodes |-> "none"]

InitialModel ==
    [primary |-> InitialPrimary,
     primaryTerm |-> InitialPrimaryTerm,
     inSync |-> InitialInSync,
     alive |-> InitialAlive,
     incarnation |-> InitialIncarnation,
     allocation |-> InitialAllocation,
     copyExists |-> InitialCopyExists,
     activatedTerm |-> InitialActivatedTerm,
     fenceTerm |-> InitialFenceTerm,
     fenceMaxNext |-> InitialFenceMaxNext,
     termProcessed |-> InitialTermProcessed,
     requestStatus |-> InitialRequestStatus,
     requestCoordinator |-> InitialRequestNode,
     requestTarget |-> InitialRequestNode,
     requestTerm |-> InitialRequestTerm,
     requestKey |-> InitialRequestKey,
     requestDoc |-> InitialRequestDoc,
     requestKind |-> InitialRequestKind,
     requestHash |-> InitialRequestHash,
     requestRequired |-> InitialRequestSets,
     requestRequiredAllocation |-> InitialRequiredValues,
     requestRequiredIncarnation |-> InitialRequiredValues,
     requestAcks |-> InitialRequestSets,
     walOps |-> InitialNodeKeySets,
     walDurable |-> InitialNodeKeySets,
     walOrder |-> InitialNodeKeySequences,
     receivedOps |-> InitialNodeKeySets,
     processedOps |-> InitialNodeKeySets,
     processed |-> InitialProcessed,
     persisted |-> InitialPersisted,
     maxNext |-> InitialMaxNext,
     docKey |-> InitialDocKey,
     docSeqNext |-> InitialDocSeqNext,
     reportedProcessedNext |-> InitialProcessedNext,
     reportedPersistedNext |-> InitialPersistedNext,
     reportedMaxNext |-> InitialMaxNext,
     commitProcessedNext |-> InitialProcessedNext,
     commitPersistedNext |-> InitialPersistedNext,
     commitMaxNext |-> InitialMaxNext,
     commitDocKey |-> InitialDocKey,
     commitDocSeqNext |-> InitialDocSeqNext,
     commitTerm |-> InitialFenceTerm,
     commitFenceMaxNext |-> InitialFenceMaxNext,
     commitTermProcessed |-> InitialTermProcessed,
     truncNext |-> [node \in Nodes |-> 0],
     replaying |-> [node \in Nodes |-> FALSE],
     replayId |-> InitialReplayId,
     replayBoundary |-> InitialReplayNat,
     replaySeen |-> InitialNodeKeySets,
     replayLastWalPosition |-> InitialReplayNat,
     recoveryState |-> InitialRecoveryState,
     ackedOps |-> {}]

Init ==
    /\ tracePos = 1
    /\ failedStep = 0
    /\ failedEvent = "none"
    /\ finished = FALSE
    /\ model = InitialModel
    /\ Safety(model)

OperationEvent(e) ==
    /\ e.key \in OpKeys
    /\ e.term = OpTerm[e.key]
    /\ e.seq = OpSeq[e.key]
    /\ e.doc = OpDoc[e.key]
    /\ e.op = OpKind[e.key]
    /\ e.hash = OpHash[e.key]

CheckpointsMatch(m, e, node) ==
    /\ e.cpProcessedNext = ProcessedNext(m, node)
    /\ e.cpPersistedNext = PersistedNext(m, node)
    /\ e.cpMaxNext = m.maxNext[node]

RequestForKeySet(m, key) ==
    {request \in Requests : m.requestKey[request] = key}

RequestForKey(m, key) ==
    CHOOSE request \in Requests : m.requestKey[request] = key

RouteCan(m, e) ==
    /\ e.request \in Requests
    /\ m.requestStatus[e.request] = "Unused"
    /\ e.node \in Nodes
    /\ e.peer \in Nodes
    /\ e.peer = m.primary
    /\ e.peerAllocation = m.allocation[e.peer]
    /\ e.term = m.primaryTerm
    /\ e.doc \in Docs
    /\ e.op \in {"index", "delete"}
    /\ e.hash # NoHash
    /\ e.outcome = "routed"
    /\ m.alive[e.node]

RouteModel(m, e) ==
    [m EXCEPT
        !.requestStatus[e.request] = "Routed",
        !.requestCoordinator[e.request] = e.node,
        !.requestTarget[e.request] = e.peer,
        !.requestTerm[e.request] = e.term,
        !.requestDoc[e.request] = e.doc,
        !.requestKind[e.request] = e.op,
        !.requestHash[e.request] = e.hash]

WalCan(m, e) ==
    /\ OperationEvent(e)
    /\ e.node \in Nodes
    /\ m.alive[e.node]
    /\ m.copyExists[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ e.outcome = "appended"
    /\ e.key \notin m.walOps[e.node]
    /\ \A existing \in m.walOps[e.node] :
           OpSeq[existing] # e.seq
    /\ IF e.origin = "primary"
          THEN /\ e.request \in Requests
               /\ m.requestStatus[e.request] = "Routed"
               /\ m.requestTarget[e.request] = e.node
               /\ m.requestTerm[e.request] = e.term
               /\ m.requestDoc[e.request] = e.doc
               /\ m.requestKind[e.request] = e.op
               /\ m.requestHash[e.request] = e.hash
               /\ e.node = m.primary
               /\ e.term = m.primaryTerm
          ELSE TRUE

WalModel(m, e) ==
    [m EXCEPT
        !.walOps[e.node] = @ \cup {e.key},
        !.walOrder[e.node] = Append(@, e.key),
        !.walDurable[e.node] =
            IF e.durable THEN @ \cup {e.key} ELSE @]

FenceCollision(m, e) ==
    /\ e.seq \in m.processed[e.node]
    /\ e.term = m.fenceTerm[e.node]
    /\ e.seq < m.fenceMaxNext[e.node]
    /\ e.seq \notin m.termProcessed[e.node]

ExpectedOperationOutcome(m, e) ==
    IF FenceCollision(m, e)
    THEN "collision"
    ELSE IF e.seq \in m.processed[e.node]
         THEN IF e.key \in m.processedOps[e.node]
              THEN "redelivery"
              ELSE "collision"
         ELSE IF e.op = "noop"
              THEN "noop"
              ELSE LET current == m.docSeqNext[e.node][e.doc]
                   IN IF current > e.seq + 1
                      THEN "stale"
                      ELSE IF current = e.seq + 1
                           THEN IF m.docKey[e.node][e.doc] = e.key
                                THEN "redelivery"
                                ELSE "collision"
                           ELSE "applied_newer"

OperationOriginCan(m, e) ==
    CASE e.origin = "primary" ->
            /\ e.request \in Requests
            /\ m.requestStatus[e.request] = "Routed"
            /\ m.requestTarget[e.request] = e.node
            /\ m.requestTerm[e.request] = e.term
            /\ m.requestDoc[e.request] = e.doc
            /\ m.requestKind[e.request] = e.op
            /\ m.requestHash[e.request] = e.hash
            /\ e.node = m.primary
            /\ e.term = m.primaryTerm
      [] e.origin = "live_replication" ->
            /\ e.key \in m.receivedOps[e.node]
            /\ e.term = m.fenceTerm[e.node]
      [] e.origin = "recovery" ->
            m.recoveryState[e.node] \in {"recovering", "pending"}
      [] e.origin = "promotion_noop_fill" ->
            /\ e.node = m.primary
            /\ e.op = "noop"
            /\ ~m.replaying[e.node]
      [] OTHER -> FALSE

OperationFlagsMatch(m, e) ==
    CASE e.outcome \in {"collision", "apply_failed"} ->
            /\ ~e.operationProcessed
            /\ ~e.operationPersisted
      [] e.outcome = "redelivery" ->
            /\ e.operationProcessed
            /\ e.operationPersisted =
                  IF e.seq \in m.processed[e.node]
                  THEN e.seq \in m.persisted[e.node]
                  ELSE e.key \in m.walDurable[e.node]
      [] OTHER ->
            /\ e.operationProcessed
            /\ e.operationPersisted =
                  (e.key \in m.walDurable[e.node])

OperationBaseCan(m, e) ==
    /\ OperationEvent(e)
    /\ e.node \in Nodes
    /\ m.alive[e.node]
    /\ m.copyExists[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ OperationOriginCan(m, e)
    /\ e.outcome = ExpectedOperationOutcome(m, e)
    /\ OperationFlagsMatch(m, e)
    /\ IF e.outcome \in {"applied_newer", "stale", "noop", "apply_failed"}
          THEN e.key \in m.walOps[e.node]
          ELSE TRUE

OperationModel(m, e) ==
    IF e.outcome = "collision"
    THEN m
    ELSE IF e.outcome = "redelivery"
         THEN IF e.seq \in m.processed[e.node]
              THEN m
              ELSE [m EXCEPT
                        !.processedOps[e.node] = @ \cup {e.key},
                        !.processed[e.node] = @ \cup {e.seq},
                        !.persisted[e.node] =
                            IF e.operationPersisted
                            THEN @ \cup {e.seq}
                            ELSE @,
                        !.maxNext[e.node] =
                            NatMax(@, e.seq + 1),
                        !.termProcessed[e.node] =
                            IF e.term = m.fenceTerm[e.node]
                               /\ e.seq < m.fenceMaxNext[e.node]
                            THEN @ \cup {e.seq}
                            ELSE @]
         ELSE IF e.outcome = "apply_failed"
              THEN [m EXCEPT
                        !.maxNext[e.node] =
                            NatMax(@, e.seq + 1)]
              ELSE LET withSequence ==
                           [m EXCEPT
                               !.processedOps[e.node] = @ \cup {e.key},
                               !.processed[e.node] = @ \cup {e.seq},
                               !.persisted[e.node] =
                                   IF e.operationPersisted
                                   THEN @ \cup {e.seq}
                                   ELSE @,
                               !.maxNext[e.node] =
                                   NatMax(@, e.seq + 1),
                               !.termProcessed[e.node] =
                                   IF e.term = m.fenceTerm[e.node]
                                      /\ e.seq < m.fenceMaxNext[e.node]
                                   THEN @ \cup {e.seq}
                                   ELSE @]
                   IN IF e.outcome = "applied_newer"
                      THEN [withSequence EXCEPT
                                !.docKey[e.node][e.doc] = e.key,
                                !.docSeqNext[e.node][e.doc] = e.seq + 1]
                      ELSE withSequence

OperationCan(m, e) ==
    /\ OperationBaseCan(m, e)
    /\ LET next == OperationModel(m, e)
       IN CheckpointsMatch(next, e, e.node)

PrimaryAssignedCan(m, e) ==
    /\ OperationEvent(e)
    /\ e.request \in Requests
    /\ m.requestStatus[e.request] = "Routed"
    /\ m.requestTarget[e.request] = e.node
    /\ m.requestTerm[e.request] = e.term
    /\ m.requestDoc[e.request] = e.doc
    /\ m.requestKind[e.request] = e.op
    /\ m.requestHash[e.request] = e.hash
    /\ e.node = m.primary
    /\ e.allocation = m.allocation[e.node]
    /\ e.term = m.primaryTerm
    /\ e.key \in m.walOps[e.node]
    /\ e.key \in m.processedOps[e.node]
    /\ e.outcome = "assigned"
    /\ e.required \subseteq Nodes \ {e.node}
    /\ \A replica \in e.required :
           /\ e.requiredAllocation[replica] = m.allocation[replica]
           /\ e.requiredIncarnation[replica] = m.incarnation[replica]
    /\ CheckpointsMatch(m, e, e.node)

PrimaryAssignedModel(m, e) ==
    [m EXCEPT
        !.requestStatus[e.request] = "Replicating",
        !.requestKey[e.request] = e.key,
        !.requestRequired[e.request] = e.required,
        !.requestRequiredAllocation[e.request] = e.requiredAllocation,
        !.requestRequiredIncarnation[e.request] = e.requiredIncarnation,
        !.requestAcks[e.request] = {}]

ReplicaReceiveCan(m, e) ==
    /\ OperationEvent(e)
    /\ e.node \in Nodes
    /\ e.peer \in Nodes
    /\ m.alive[e.node]
    /\ e.outcome \in {"accepted", "rejected"}
    /\ IF e.outcome = "accepted"
          THEN /\ RequestForKeySet(m, e.key) # {}
               /\ LET request == RequestForKey(m, e.key)
                  IN /\ e.peer = m.requestTarget[request]
                     /\ e.node \in m.requestRequired[request]
                     /\ e.allocation =
                           m.requestRequiredAllocation[request][e.node]
                     /\ e.incarnation =
                           m.requestRequiredIncarnation[request][e.node]
          ELSE TRUE
    /\ CheckpointsMatch(m, e, e.node)

ReplicaReceiveModel(m, e) ==
    IF e.outcome = "accepted"
    THEN [m EXCEPT !.receivedOps[e.node] = @ \cup {e.key}]
    ELSE m

ReplicaResultCan(m, e) ==
    /\ OperationEvent(e)
    /\ RequestForKeySet(m, e.key) # {}
    /\ LET request == RequestForKey(m, e.key)
       IN /\ m.requestStatus[request] = "Replicating"
          /\ e.node = m.requestTarget[request]
          /\ e.peer \in m.requestRequired[request]
          /\ e.peerAllocation =
                m.requestRequiredAllocation[request][e.peer]
          /\ e.outcome \in {"acknowledged", "failed", "timeout", "dropped"}
          /\ IF e.outcome = "acknowledged"
                THEN /\ e.key \in m.receivedOps[e.peer]
                     /\ e.key \in m.processedOps[e.peer]
                ELSE TRUE

ReplicaResultModel(m, e) ==
    IF e.outcome = "acknowledged"
    THEN LET request == RequestForKey(m, e.key)
         IN [m EXCEPT
                !.requestAcks[request] = @ \cup {e.peer}]
    ELSE m

ClientResultCan(m, e) ==
    /\ e.request \in Requests
    /\ m.requestStatus[e.request] \in {"Routed", "Replicating"}
    /\ e.node = m.requestCoordinator[e.request]
    /\ e.outcome \in {"acknowledged", "failed"}
    /\ IF e.outcome = "acknowledged"
          THEN /\ m.requestStatus[e.request] = "Replicating"
               /\ e.key = m.requestKey[e.request]
               /\ m.requestRequired[e.request]
                    \subseteq m.requestAcks[e.request]
          ELSE TRUE

ClientResultModel(m, e) ==
    IF e.outcome = "acknowledged"
    THEN [m EXCEPT
             !.requestStatus[e.request] = "Acknowledged",
             !.ackedOps = @ \cup {m.requestKey[e.request]}]
    ELSE [m EXCEPT
             !.requestStatus[e.request] = "Failed",
             !.requestKey[e.request] =
                 IF e.key \in OpKeys THEN e.key ELSE @]

CheckpointCan(m, e) ==
    /\ e.node \in Nodes
    /\ e.allocation = m.allocation[e.node]
    /\ e.outcome \in {"changed", "restored"}
    /\ e.prevProcessedNext = m.reportedProcessedNext[e.node]
    /\ e.prevPersistedNext = m.reportedPersistedNext[e.node]
    /\ e.prevMaxNext = m.reportedMaxNext[e.node]
    /\ CheckpointsMatch(m, e, e.node)
    /\ \/ e.cpProcessedNext # e.prevProcessedNext
       \/ e.cpPersistedNext # e.prevPersistedNext
       \/ e.cpMaxNext # e.prevMaxNext

CheckpointModel(m, e) ==
    [m EXCEPT
        !.reportedProcessedNext[e.node] = e.cpProcessedNext,
        !.reportedPersistedNext[e.node] = e.cpPersistedNext,
        !.reportedMaxNext[e.node] = e.cpMaxNext]

FenceCan(m, e) ==
    /\ e.node \in Nodes
    /\ m.alive[e.node]
    /\ m.copyExists[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ e.term > m.fenceTerm[e.node]
    /\ e.fenceMaxNext = LocalMaxNext(m, e.node)
    /\ e.outcome = "raised"
    /\ CheckpointsMatch(m, e, e.node)

FenceModel(m, e) ==
    [m EXCEPT
        !.fenceTerm[e.node] = e.term,
        !.fenceMaxNext[e.node] = e.fenceMaxNext,
        !.termProcessed[e.node] = {}]

CommitCan(m, e) ==
    /\ e.node \in Nodes
    /\ m.copyExists[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ e.term = m.fenceTerm[e.node]
    /\ e.termStateCurrent = m.fenceTerm[e.node]
    /\ e.termStateMaxNext = m.fenceMaxNext[e.node]
    /\ e.termStateProcessed = m.termProcessed[e.node]
    /\ e.outcome = "persisted"
    /\ CheckpointsMatch(m, e, e.node)

CommitModel(m, e) ==
    [m EXCEPT
        !.commitProcessedNext[e.node] = e.cpProcessedNext,
        !.commitPersistedNext[e.node] = e.cpPersistedNext,
        !.commitMaxNext[e.node] = e.cpMaxNext,
        !.commitDocKey[e.node] = m.docKey[e.node],
        !.commitDocSeqNext[e.node] = m.docSeqNext[e.node],
        !.commitTerm[e.node] = e.termStateCurrent,
        !.commitFenceMaxNext[e.node] = e.termStateMaxNext,
        !.commitTermProcessed[e.node] = e.termStateProcessed]

TruncateCan(m, e) ==
    /\ e.node \in Nodes
    /\ e.allocation = m.allocation[e.node]
    /\ e.truncateThroughNext <= m.commitProcessedNext[e.node]
    /\ e.outcome = "completed"

TruncateModel(m, e) ==
    [m EXCEPT
        !.truncNext[e.node] =
            NatMax(@, e.truncateThroughNext)]

CrashCan(m, e) ==
    /\ e.node \in Nodes
    /\ m.alive[e.node]
    /\ e.incarnation = m.incarnation[e.node]
    /\ e.outcome \in {"unclean", "clean"}

CrashModel(m, e) ==
    [m EXCEPT
        !.alive[e.node] = FALSE,
        !.activatedTerm[e.node] = 0,
        !.replaying[e.node] = FALSE,
        !.replayId[e.node] = NoReplay,
        !.replaySeen[e.node] = {},
        !.replayLastWalPosition[e.node] = 0]

RestartCan(m, e) ==
    /\ e.node \in Nodes
    /\ ~m.alive[e.node]
    /\ e.incarnation = m.incarnation[e.node] + 1
    /\ e.outcome = "started"

RestartModel(m, e) ==
    [m EXCEPT
        !.alive[e.node] = TRUE,
        !.incarnation[e.node] = e.incarnation,
        !.activatedTerm[e.node] = 0]

ReplayStartModel(m, e) ==
    [m EXCEPT
        !.processed[e.node] = Prefix(m.commitProcessedNext[e.node]),
        !.persisted[e.node] = Prefix(m.commitPersistedNext[e.node]),
        !.maxNext[e.node] = m.commitMaxNext[e.node],
        !.docKey[e.node] = m.commitDocKey[e.node],
        !.docSeqNext[e.node] = m.commitDocSeqNext[e.node],
        !.termProcessed[e.node] =
            IF m.fenceTerm[e.node] = m.commitTerm[e.node]
            THEN m.commitTermProcessed[e.node]
            ELSE {},
        !.replaying[e.node] = TRUE,
        !.replayId[e.node] = e.replayId,
        !.replayBoundary[e.node] =
            m.commitProcessedNext[e.node],
        !.replaySeen[e.node] = {},
        !.replayLastWalPosition[e.node] = 0,
        !.activatedTerm[e.node] = 0]

ReplayStartCan(m, e) ==
    /\ e.node \in Nodes
    /\ m.alive[e.node]
    /\ m.copyExists[e.node]
    /\ ~m.replaying[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ e.term = m.fenceTerm[e.node]
    /\ e.replayId # NoReplay
    /\ e.outcome = "started"
    /\ LET next == ReplayStartModel(m, e)
       IN CheckpointsMatch(next, e, e.node)

ReplayOperationModel(m, e) ==
    LET operationEvent ==
            [e EXCEPT
                !.operationProcessed =
                    e.outcome \notin {"collision", "apply_failed"},
                !.operationPersisted =
                    e.outcome \notin {"collision", "apply_failed"}]
    IN OperationModel(m, operationEvent)

ReplayEntryCan(m, e) ==
    /\ OperationEvent(e)
    /\ e.node \in Nodes
    /\ m.replaying[e.node]
    /\ e.replayId = m.replayId[e.node]
    /\ e.replayOrdinal = Cardinality(m.replaySeen[e.node])
    /\ e.key \in m.walOps[e.node]
    /\ e.key \notin m.replaySeen[e.node]
    /\ WalPosition(m, e.node, e.key) >
          m.replayLastWalPosition[e.node]
    /\ IF e.outcome = "skip_committed"
          THEN /\ e.seq < m.replayBoundary[e.node]
               /\ CheckpointsMatch(m, e, e.node)
          ELSE /\ e.seq >= m.replayBoundary[e.node]
               /\ e.outcome = ExpectedOperationOutcome(m, e)
               /\ IF e.outcome \in {"applied_newer", "stale", "noop",
                                     "apply_failed"}
                     THEN e.key \in m.walOps[e.node]
                     ELSE TRUE
               /\ LET next == ReplayOperationModel(m, e)
                  IN CheckpointsMatch(next, e, e.node)

ReplayEntryModel(m, e) ==
    LET applied ==
            IF e.outcome = "skip_committed"
            THEN m
            ELSE ReplayOperationModel(m, e)
    IN [applied EXCEPT
            !.replaySeen[e.node] = @ \cup {e.key},
            !.replayLastWalPosition[e.node] =
                WalPosition(m, e.node, e.key)]

RequiredRetainedReplayOps(m, node) ==
    {key \in m.walOps[node] :
        /\ OpSeq[key] + 1 > m.truncNext[node]
        /\ OpSeq[key] >= m.replayBoundary[node]}

ReplayFinishCan(m, e) ==
    /\ e.node \in Nodes
    /\ m.replaying[e.node]
    /\ e.replayId = m.replayId[e.node]
    /\ e.entriesExamined = Cardinality(m.replaySeen[e.node])
    /\ e.outcome \in {"completed", "failed"}
    /\ IF e.outcome = "completed"
          THEN RequiredRetainedReplayOps(m, e.node)
                 \subseteq m.replaySeen[e.node]
          ELSE TRUE
    /\ CheckpointsMatch(m, e, e.node)

ReplayFinishModel(m, e) ==
    [m EXCEPT
        !.replaying[e.node] = FALSE,
        !.replayId[e.node] = NoReplay]

PromotionCan(m, e) ==
    /\ e.node \in m.inSync
    /\ e.peer = m.primary
    /\ e.allocation = m.allocation[e.node]
    /\ e.term > m.primaryTerm
    /\ e.outcome = "committed"

PromotionModel(m, e) ==
    [m EXCEPT
        !.primary = e.node,
        !.primaryTerm = e.term,
        !.inSync = @ \ {e.node},
        !.activatedTerm[e.node] = 0]

ActivationCan(m, e) ==
    /\ e.node = m.primary
    /\ m.alive[e.node]
    /\ m.copyExists[e.node]
    /\ ~m.replaying[e.node]
    /\ e.allocation = m.allocation[e.node]
    /\ e.term > m.primaryTerm
    /\ ProcessedNext(m, e.node) = m.maxNext[e.node]
    /\ e.outcome = "activated"
    /\ CheckpointsMatch(m, e, e.node)

ActivationModel(m, e) ==
    [m EXCEPT
        !.primaryTerm = e.term,
        !.activatedTerm[e.node] = e.term]

RecoveryStartCan(m, e) ==
    /\ e.node \in Nodes
    /\ e.peer = m.primary
    /\ e.node # e.peer
    /\ m.alive[e.node]
    /\ m.alive[e.peer]
    /\ e.term = m.primaryTerm
    /\ e.snapshotNext <= LocalMaxNext(m, e.peer)
    /\ e.outcome = "started"
    /\ m.recoveryState[e.node] = "none"

RecoveryStartModel(m, e) ==
    [m EXCEPT
        !.allocation[e.node] = e.allocation,
        !.copyExists[e.node] = TRUE,
        !.activatedTerm[e.node] = 0,
        !.fenceTerm[e.node] = e.term,
        !.fenceMaxNext[e.node] = e.snapshotNext,
        !.termProcessed[e.node] = {},
        !.processed[e.node] = Prefix(e.snapshotNext),
        !.persisted[e.node] = Prefix(e.snapshotNext),
        !.maxNext[e.node] = e.snapshotNext,
        !.docKey[e.node] = m.docKey[e.peer],
        !.docSeqNext[e.node] = m.docSeqNext[e.peer],
        !.walOps[e.node] = {},
        !.walDurable[e.node] = {},
        !.walOrder[e.node] = <<>>,
        !.recoveryState[e.node] = "recovering"]

RecoveryInstallCan(m, e) ==
    /\ e.node \in Nodes
    /\ e.peer = m.primary
    /\ e.allocation = m.allocation[e.node]
    /\ e.term = m.primaryTerm
    /\ m.recoveryState[e.node] = "recovering"
    /\ e.barrierNext <= m.maxNext[e.peer]
    /\ e.outcome = "pending_membership"
    /\ CheckpointsMatch(m, e, e.node)

RecoveryInstallModel(m, e) ==
    [m EXCEPT !.recoveryState[e.node] = "pending"]

RecoveryMembershipCan(m, e) ==
    /\ e.node \in Nodes
    /\ e.peer \in Nodes
    /\ e.allocation = m.allocation[e.node]
    /\ m.recoveryState[e.node] = "pending"
    /\ e.outcome \in {"admitted", "promoted", "rejected", "unknown"}

RecoveryMembershipModel(m, e) ==
    CASE e.outcome = "admitted" ->
            [m EXCEPT
                !.inSync = @ \cup {e.node},
                !.recoveryState[e.node] = "none"]
      [] e.outcome = "promoted" ->
            [m EXCEPT
                !.primary = e.node,
                !.primaryTerm = e.term,
                !.inSync = @ \ {e.node},
                !.recoveryState[e.node] = "none",
                !.activatedTerm[e.node] = 0]
      [] e.outcome = "rejected" ->
            [m EXCEPT
                !.inSync = @ \ {e.node},
                !.recoveryState[e.node] = "rejected"]
      [] OTHER -> m

ObservationCan(m, e) ==
    /\ e.node \in Nodes
    /\ IF e.allocation = 0
          THEN TRUE
          ELSE e.allocation = m.allocation[e.node]

RawCanEvent(m, e) ==
    CASE e.kind = "client_write_routed" -> RouteCan(m, e)
      [] e.kind = "wal_appended" -> WalCan(m, e)
      [] e.kind = "operation_applied" -> OperationCan(m, e)
      [] e.kind = "primary_assigned" -> PrimaryAssignedCan(m, e)
      [] e.kind = "replica_received" -> ReplicaReceiveCan(m, e)
      [] e.kind = "replica_result" -> ReplicaResultCan(m, e)
      [] e.kind = "client_result" -> ClientResultCan(m, e)
      [] e.kind = "checkpoint_changed" -> CheckpointCan(m, e)
      [] e.kind = "fence_persisted" -> FenceCan(m, e)
      [] e.kind = "commit_persisted" -> CommitCan(m, e)
      [] e.kind = "wal_truncated" -> TruncateCan(m, e)
      [] e.kind = "node_crashed" -> CrashCan(m, e)
      [] e.kind = "node_restarted" -> RestartCan(m, e)
      [] e.kind = "replay_started" -> ReplayStartCan(m, e)
      [] e.kind = "replay_entry" -> ReplayEntryCan(m, e)
      [] e.kind = "replay_finished" -> ReplayFinishCan(m, e)
      [] e.kind = "routing_promoted" -> PromotionCan(m, e)
      [] e.kind = "primary_activated" -> ActivationCan(m, e)
      [] e.kind = "recovery_started" -> RecoveryStartCan(m, e)
      [] e.kind = "recovery_installed" -> RecoveryInstallCan(m, e)
      [] e.kind = "recovery_membership" -> RecoveryMembershipCan(m, e)
      [] OTHER -> ObservationCan(m, e)

EventModel(m, e) ==
    CASE e.kind = "client_write_routed" -> RouteModel(m, e)
      [] e.kind = "wal_appended" -> WalModel(m, e)
      [] e.kind = "operation_applied" -> OperationModel(m, e)
      [] e.kind = "primary_assigned" -> PrimaryAssignedModel(m, e)
      [] e.kind = "replica_received" -> ReplicaReceiveModel(m, e)
      [] e.kind = "replica_result" -> ReplicaResultModel(m, e)
      [] e.kind = "client_result" -> ClientResultModel(m, e)
      [] e.kind = "checkpoint_changed" -> CheckpointModel(m, e)
      [] e.kind = "fence_persisted" -> FenceModel(m, e)
      [] e.kind = "commit_persisted" -> CommitModel(m, e)
      [] e.kind = "wal_truncated" -> TruncateModel(m, e)
      [] e.kind = "node_crashed" -> CrashModel(m, e)
      [] e.kind = "node_restarted" -> RestartModel(m, e)
      [] e.kind = "replay_started" -> ReplayStartModel(m, e)
      [] e.kind = "replay_entry" -> ReplayEntryModel(m, e)
      [] e.kind = "replay_finished" -> ReplayFinishModel(m, e)
      [] e.kind = "routing_promoted" -> PromotionModel(m, e)
      [] e.kind = "primary_activated" -> ActivationModel(m, e)
      [] e.kind = "recovery_started" -> RecoveryStartModel(m, e)
      [] e.kind = "recovery_installed" -> RecoveryInstallModel(m, e)
      [] e.kind = "recovery_membership" -> RecoveryMembershipModel(m, e)
      [] OTHER -> m

CanEvent(m, e) ==
    /\ RawCanEvent(m, e)
    /\ Safety(EventModel(m, e))

ConsumeEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ CanEvent(model, event)
    /\ model' = EventModel(model, event)
    /\ tracePos' = tracePos + 1
    /\ UNCHANGED <<failedStep, failedEvent, finished>>

RejectEvent ==
    LET event == Trace[tracePos]
    IN
    /\ tracePos <= Len(Trace)
    /\ ~CanEvent(model, event)
    /\ failedStep' = event.step
    /\ failedEvent' = event.kind
    /\ tracePos' = tracePos + 1
    /\ UNCHANGED <<model, finished>>

FinishTrace ==
    /\ tracePos > Len(Trace)
    /\ ~finished
    /\ finished' = TRUE
    /\ UNCHANGED <<tracePos, failedStep, failedEvent, model>>

Advance ==
    /\ failedStep = 0
    /\ ~finished
    /\ \/ ConsumeEvent
       \/ RejectEvent
       \/ FinishTrace

Next ==
    Advance

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Advance)

TraceTypeOK ==
    /\ tracePos \in 1..(Len(Trace) + 1)
    /\ failedStep \in Nat
    /\ failedEvent \in EventKinds \cup {"none"}
    /\ finished \in BOOLEAN
    /\ model.primary \in Nodes
    /\ model.primaryTerm \in Nat
    /\ model.inSync \subseteq Nodes
    /\ model.alive \in [Nodes -> BOOLEAN]
    /\ model.incarnation \in [Nodes -> Nat]
    /\ model.allocation \in [Nodes -> Nat]
    /\ model.copyExists \in [Nodes -> BOOLEAN]
    /\ model.activatedTerm \in [Nodes -> Nat]
    /\ model.fenceTerm \in [Nodes -> Nat]
    /\ model.fenceMaxNext \in [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.termProcessed \in [Nodes -> SUBSET Seqs]
    /\ model.requestStatus \in [Requests -> RequestStates]
    /\ model.requestCoordinator \in [Requests -> Nodes \cup {NoNode}]
    /\ model.requestTarget \in [Requests -> Nodes \cup {NoNode}]
    /\ model.requestTerm \in [Requests -> Nat]
    /\ model.requestKey \in [Requests -> OpKeys \cup {NoKey}]
    /\ model.requestDoc \in [Requests -> Docs \cup {NoDoc}]
    /\ model.requestKind \in
          [Requests -> {"none", "index", "delete", "noop"}]
    /\ model.requestHash \in [Requests -> Hashes \cup {NoHash}]
    /\ model.requestRequired \in [Requests -> SUBSET Nodes]
    /\ model.requestRequiredAllocation \in
          [Requests -> [Nodes -> Nat]]
    /\ model.requestRequiredIncarnation \in
          [Requests -> [Nodes -> Nat]]
    /\ model.requestAcks \in [Requests -> SUBSET Nodes]
    /\ model.walOps \in [Nodes -> SUBSET OpKeys]
    /\ model.walDurable \in [Nodes -> SUBSET OpKeys]
    /\ model.walOrder \in [Nodes -> Seq(OpKeys)]
    /\ model.receivedOps \in [Nodes -> SUBSET OpKeys]
    /\ model.processedOps \in [Nodes -> SUBSET OpKeys]
    /\ model.processed \in [Nodes -> SUBSET Seqs]
    /\ model.persisted \in [Nodes -> SUBSET Seqs]
    /\ model.maxNext \in [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.docKey \in [Nodes -> [Docs -> OpKeys \cup {NoKey}]]
    /\ model.docSeqNext \in
          [Nodes -> [Docs -> 0..(MaxSeqBound + 1)]]
    /\ model.reportedProcessedNext \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.reportedPersistedNext \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.reportedMaxNext \in [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.commitProcessedNext \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.commitPersistedNext \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.commitMaxNext \in [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.commitDocKey \in
          [Nodes -> [Docs -> OpKeys \cup {NoKey}]]
    /\ model.commitDocSeqNext \in
          [Nodes -> [Docs -> 0..(MaxSeqBound + 1)]]
    /\ model.commitTerm \in [Nodes -> Nat]
    /\ model.commitFenceMaxNext \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.commitTermProcessed \in [Nodes -> SUBSET Seqs]
    /\ model.truncNext \in [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.replaying \in [Nodes -> BOOLEAN]
    /\ model.replayId \in [Nodes -> ReplayIds \cup {NoReplay}]
    /\ model.replayBoundary \in
          [Nodes -> 0..(MaxSeqBound + 1)]
    /\ model.replaySeen \in [Nodes -> SUBSET OpKeys]
    /\ model.replayLastWalPosition \in [Nodes -> Nat]
    /\ model.recoveryState \in [Nodes -> RecoveryStates]
    /\ model.ackedOps \subseteq OpKeys

TraceFailureFree ==
    failedStep = 0

TraceSafety ==
    Safety(model)

TraceEventuallyCompletes ==
    <>finished

=============================================================================
