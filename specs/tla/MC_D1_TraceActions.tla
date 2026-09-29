------------------------ MODULE MC_D1_TraceActions ------------------------
\* Coverage scenario for D1 actions introduced for implementation traces.
\* It persists an earlier captured boundary after a later write, records safe
\* truncation, restarts both primary and replica, completes primary replay, and
\* leaves a failed replica replay unavailable.

EXTENDS MC_D1_SeqNoApply

VARIABLES
    tracePhase,
    capturedBoundary,
    capturedPersisted,
    capturedMax,
    capturedOps,
    capturedDocValue,
    capturedDocSeqNext,
    capturedTombstoneSeqNext

TraceActionVars ==
    <<tracePhase, capturedBoundary, capturedPersisted, capturedMax,
      capturedOps, capturedDocValue, capturedDocSeqNext,
      capturedTombstoneSeqNext>>

traceActionVars == <<d1vars, TraceActionVars>>

TraceActionsInit ==
    /\ D1Init
    /\ tracePhase = 0
    /\ capturedBoundary = 0
    /\ capturedPersisted = 0
    /\ capturedMax = 0
    /\ capturedOps = {}
    /\ capturedDocValue = [doc \in Docs |-> NoWrite]
    /\ capturedDocSeqNext = [doc \in Docs |-> 0]
    /\ capturedTombstoneSeqNext = [doc \in Docs |-> 0]

CaptureInitialBoundary ==
    /\ tracePhase = 0
    /\ capturedBoundary' = processedNext[ReplicaNode]
    /\ capturedPersisted' = persistedNext[ReplicaNode]
    /\ capturedMax' = maxSeqNext[ReplicaNode]
    /\ capturedOps' = ops[ReplicaNode]
    /\ capturedDocValue' = docValue[ReplicaNode]
    /\ capturedDocSeqNext' = docSeqNext[ReplicaNode]
    /\ capturedTombstoneSeqNext' = tombstoneSeqNext[ReplicaNode]
    /\ tracePhase' = 1
    /\ UNCHANGED d1vars

SubmitWrite ==
    /\ tracePhase = 1
    /\ D1ClientWrite(DocX, "Put")
    /\ tracePhase' = 2
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

AcceptWrite ==
    /\ tracePhase = 2
    /\ D1PrimaryAccept(1)
    /\ tracePhase' = 3
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

ApplyReplica ==
    /\ tracePhase = 3
    /\ \E message \in messages :
           /\ message.kind = "Replicate"
           /\ message.write = 1
           /\ D1FixedReplicaProcess(message)
    /\ tracePhase' = 4
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

DeliverAck ==
    /\ tracePhase = 4
    /\ \E message \in messages :
           /\ message.kind = "ReplicaAck"
           /\ message.write = 1
           /\ D1DeliverAck(message)
    /\ tracePhase' = 5
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

AckWrite ==
    /\ tracePhase = 5
    /\ D1PrimaryAck(1)
    /\ tracePhase' = 6
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

PersistEarlierCapture ==
    /\ tracePhase = 6
    /\ D1PersistBoundary(
           ReplicaNode,
           capturedBoundary,
           capturedPersisted,
           capturedMax,
           capturedOps,
           capturedDocValue,
           capturedDocSeqNext,
           capturedTombstoneSeqNext,
           TRUE)
    /\ tracePhase' = 7
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

RecordSafeTruncation ==
    /\ tracePhase = 7
    /\ D1RecordTruncation(ReplicaNode, capturedBoundary)
    /\ tracePhase' = 8
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

CrashPrimary ==
    /\ tracePhase = 8
    /\ D1CrashCopy(PrimaryNode)
    /\ tracePhase' = 9
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

RestartPrimary ==
    /\ tracePhase = 9
    /\ D1RestartCopy(PrimaryNode)
    /\ tracePhase' = 10
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

ReplayPrimary ==
    /\ tracePhase = 10
    /\ D1FixedReplayApplyAt(PrimaryNode)
    /\ tracePhase' = 11
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

FinishPrimaryReplay ==
    /\ tracePhase = 11
    /\ D1FinishReplayAt(PrimaryNode)
    /\ tracePhase' = 12
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

CrashReplica ==
    /\ tracePhase = 12
    /\ D1CrashCopy(ReplicaNode)
    /\ tracePhase' = 13
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

RestartReplica ==
    /\ tracePhase = 13
    /\ D1RestartCopy(ReplicaNode)
    /\ tracePhase' = 14
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

FailReplicaReplay ==
    /\ tracePhase = 14
    /\ D1FailReplayAt(ReplicaNode)
    /\ tracePhase' = 15
    /\ UNCHANGED
          <<capturedBoundary, capturedPersisted, capturedMax, capturedOps,
            capturedDocValue, capturedDocSeqNext,
            capturedTombstoneSeqNext>>

TraceActionsNext ==
    \/ CaptureInitialBoundary
    \/ SubmitWrite
    \/ AcceptWrite
    \/ ApplyReplica
    \/ DeliverAck
    \/ AckWrite
    \/ PersistEarlierCapture
    \/ RecordSafeTruncation
    \/ CrashPrimary
    \/ RestartPrimary
    \/ ReplayPrimary
    \/ FinishPrimaryReplay
    \/ CrashReplica
    \/ RestartReplica
    \/ FailReplicaReplay

TraceActionsTypeOK ==
    /\ D1TypeOK
    /\ tracePhase \in 0..15
    /\ capturedBoundary \in 0..MaxWrites
    /\ capturedPersisted \in 0..MaxWrites
    /\ capturedMax \in 0..MaxWrites
    /\ capturedOps \subseteq WriteIds
    /\ capturedDocValue \in [Docs -> 0..MaxWrites]
    /\ capturedDocSeqNext \in [Docs -> 0..MaxWrites]
    /\ capturedTombstoneSeqNext \in [Docs -> 0..MaxWrites]

EarlierCapturePersists ==
    tracePhase >= 7 =>
        /\ persistedProcessedNext[ReplicaNode] = capturedBoundary
        /\ persistedCommittedNext[ReplicaNode] = capturedPersisted
        /\ processedNext[ReplicaNode] >= persistedProcessedNext[ReplicaNode]

BothNodesRestarted ==
    tracePhase >= 14 =>
        /\ epoch[PrimaryNode] = 1
        /\ epoch[ReplicaNode] = 1

FailedReplayUnavailable ==
    tracePhase = 15 =>
        /\ copyMode[ReplicaNode] = "InstallMarker"
        /\ installMarker[ReplicaNode]
        /\ ReplicaNode \notin AvailableInSyncCopies

TraceActionsSpec ==
    TraceActionsInit /\ [][TraceActionsNext]_traceActionVars

=============================================================================
