----------------------- MODULE MC_ApplyStorageFailure -----------------------
\* R3-1 persistent apply-I/O model.  The copy remains open and readable while
\* every WAL/fsync/engine mutation fails.  A failed replica NACKs synchronous
\* replication; a failed primary rejects its own write before replication.
\* The first observed apply failure enters retry/backoff, and a separate fair
\* action represents exhaustion of the bounded retry count/time window.

EXTENDS Invariants

CONSTANTS PrimaryNode, FailedNode, MetadataLeader, CandidateNode

ApplyStorageInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ MetadataLeader # FailedNode
    /\ FailedNode = PrimaryNode \/ FailedNode \in routing.inSync
    /\ IF FailedNode = PrimaryNode
          THEN CandidateNode \in routing.inSync
          ELSE CandidateNode = PrimaryNode

ApplyStorageClientWrite1 ==
    /\ nextWrite = 1
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageClientWrite2 ==
    /\ nextWrite = 2
    /\ copyMode[FailedNode] = "ApplyFailing"
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageClientWrite3 ==
    /\ nextWrite = 3
    /\ CommittedResult("FailShardCopy", FailedNode, TRUE)
    /\ FailedNode # routing.primary
    /\ FailedNode \notin routing.inSync
    /\ activated[routing.primary] = views[routing.primary].term
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

\* Historical retry while the persistently failing replica remains in sync.
\* The primary can begin another request, but the same replica apply must NACK.
ApplyStorageRetryClientWrite ==
    /\ nextWrite = 3
    /\ copyMode[FailedNode] = "ApplyRetrying"
    /\ FailedNode \in routing.inSync
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStoragePrimaryAccept ==
    \E writeId \in WriteIds :
        /\ PrimaryAccept(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

\* TransportService::{index_doc,bulk_index,delete_doc} reaches the local
\* primary mutation and ShardManager records its persistent apply I/O failure.
ApplyStoragePrimaryMutationFails ==
    \E writeId \in WriteIds :
        /\ PrimaryApplyFailure(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageReplicaApply ==
    \E message \in messages :
        /\ ReplicaApply(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

\* TransportService::{replicate_doc,replicate_bulk} invokes
\* ShardManager::apply_replica_operation; the validated operation returns a
\* local-storage error, leaves the operation unapplied, and returns a NACK.
ApplyStorageReplicaMutationFails ==
    \E message \in messages :
        /\ ReplicaApplyFailure(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

ApplyStorageDeliverAck ==
    \E message \in messages :
        /\ DeliverReplicaAck(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageDeliverNack ==
    \E message \in messages :
        /\ DeliverReplicaNack(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStoragePrimaryAck ==
    \E writeId \in WriteIds :
        /\ PrimaryAck(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageFailureOccurs ==
    /\ 1 \in acked
    /\ copyMode[FailedNode] = "Active"
    /\ BeginPersistentApplyFailure(FailedNode)
    /\ UNCHANGED ApplySafetyVars

ApplyStorageEscalates ==
    /\ 2 \in failed
    /\ EscalatePersistentApplyFailure(FailedNode)
    /\ UNCHANGED ApplySafetyVars

ApplyStorageReports ==
    /\ copyMode[FailedNode] = "ApplyFailed"
    /\ (FailedNode = routing.primary \/ FailedNode \in routing.replicas)
    /\ ReportShardCopyFailure(
          FailedNode,
          IF FailedNode = routing.primary THEN CandidateNode ELSE NoNode)
    /\ UNCHANGED ApplySafetyVars

ApplyStorageCommit ==
    /\ \E command \in pendingRaft : CommitRaft(command)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

ApplyStorageLifecycleActivation ==
    /\ LifecycleProposeActivation(routing.primary)
    /\ UNCHANGED <<copyAllocation, copyUuid>>
    /\ UNCHANGED <<replicaFence, durableReplicaFence>>
    /\ UNCHANGED PeerRecoveryVars
    /\ UNCHANGED ApplySafetyVars

ApplyStorageObserveActivation ==
    /\ ObserveActivation(routing.primary)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

ApplyStorageNext ==
    \/ ApplyStorageClientWrite1
    \/ ApplyStorageClientWrite2
    \/ ApplyStorageClientWrite3
    \/ ApplyStoragePrimaryAccept
    \/ ApplyStoragePrimaryMutationFails
    \/ ApplyStorageReplicaApply
    \/ ApplyStorageReplicaMutationFails
    \/ ApplyStorageDeliverAck
    \/ ApplyStorageDeliverNack
    \/ ApplyStoragePrimaryAck
    \/ ApplyStorageFailureOccurs
    \/ ApplyStorageEscalates
    \/ ApplyStorageReports
    \/ ApplyStorageCommit
    \/ ApplyStorageLifecycleActivation
    \/ ApplyStorageObserveActivation

ApplyStorageNoEscalationNext ==
    \/ ApplyStorageClientWrite1
    \/ ApplyStorageClientWrite2
    \/ ApplyStorageRetryClientWrite
    \/ ApplyStoragePrimaryAccept
    \/ ApplyStoragePrimaryMutationFails
    \/ ApplyStorageReplicaApply
    \/ ApplyStorageReplicaMutationFails
    \/ ApplyStorageDeliverAck
    \/ ApplyStorageDeliverNack
    \/ ApplyStoragePrimaryAck
    \/ ApplyStorageFailureOccurs

ApplyFailureCopyRemainsOpen ==
    \A node \in Nodes :
        copyMode[node] \in ApplyFailureModes => copyExists[node]

FailedApplyNeverMutatesFailedCopy ==
    2 \in failed => 2 \notin ops[FailedNode]

ApplyFailureEscalates ==
    (copyMode[FailedNode] = "ApplyRetrying")
    ~> (copyMode[FailedNode] = "ApplyFailed")

ApplyFailedReplicaRemoved ==
    FailedNode # PrimaryNode =>
        (copyMode[FailedNode] = "ApplyFailed")
        ~> /\ FailedNode \notin routing.replicas
           /\ FailedNode \notin routing.inSync

ApplyFailedPrimaryReplaced ==
    FailedNode = PrimaryNode =>
        (copyMode[FailedNode] = "ApplyFailed")
        ~> routing.primary = CandidateNode

WritesResumeAfterApplyFailure ==
    (copyMode[FailedNode] \in {"ApplyRetrying", "ApplyFailed"})
    ~> (3 \in acked)

ApplyStorageLivenessSpec ==
    /\ ApplyStorageInit
    /\ [][ApplyStorageNext]_vars
    /\ WF_vars(ApplyStorageClientWrite1)
    /\ WF_vars(ApplyStorageClientWrite2)
    /\ WF_vars(ApplyStorageRetryClientWrite)
    /\ WF_vars(ApplyStorageClientWrite3)
    /\ WF_vars(ApplyStoragePrimaryAccept)
    /\ WF_vars(ApplyStoragePrimaryMutationFails)
    /\ WF_vars(ApplyStorageReplicaApply)
    /\ WF_vars(ApplyStorageReplicaMutationFails)
    /\ WF_vars(ApplyStorageDeliverAck)
    /\ WF_vars(ApplyStorageDeliverNack)
    /\ WF_vars(ApplyStoragePrimaryAck)
    /\ WF_vars(ApplyStorageFailureOccurs)
    /\ WF_vars(ApplyStorageEscalates)
    /\ WF_vars(ApplyStorageReports)
    /\ WF_vars(ApplyStorageCommit)
    /\ WF_vars(ApplyStorageLifecycleActivation)
    /\ WF_vars(ApplyStorageObserveActivation)

\* Historical R3-1 behavior: an open copy's repeated apply failures never
\* consume the copy-failure budget, so no report can become enabled.
ApplyStorageNoEscalationSpec ==
    /\ ApplyStorageInit
    /\ [][ApplyStorageNoEscalationNext]_vars
    /\ WF_vars(ApplyStorageClientWrite1)
    /\ WF_vars(ApplyStorageClientWrite2)
    /\ WF_vars(ApplyStoragePrimaryAccept)
    /\ WF_vars(ApplyStoragePrimaryMutationFails)
    /\ WF_vars(ApplyStorageReplicaApply)
    /\ WF_vars(ApplyStorageReplicaMutationFails)
    /\ WF_vars(ApplyStorageDeliverAck)
    /\ WF_vars(ApplyStorageDeliverNack)
    /\ WF_vars(ApplyStoragePrimaryAck)
    /\ WF_vars(ApplyStorageFailureOccurs)

ApplyStorageNoReplicaInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ PrimaryNode = FailedNode
    /\ MetadataLeader # PrimaryNode
    /\ routing.inSync = {}

ApplyStorageNoReplicaClientWrite2 ==
    /\ nextWrite = 2
    /\ copyMode[PrimaryNode] = "ApplyFailing"
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

ApplyStorageNoReplicaReports ==
    /\ copyMode[PrimaryNode] = "ApplyFailed"
    /\ ReportShardCopyFailure(PrimaryNode, NoNode)
    /\ UNCHANGED ApplySafetyVars

ApplyStorageNoReplicaNext ==
    \/ ApplyStorageClientWrite1
    \/ ApplyStorageNoReplicaClientWrite2
    \/ ApplyStoragePrimaryAccept
    \/ ApplyStoragePrimaryMutationFails
    \/ ApplyStoragePrimaryAck
    \/ ApplyStorageFailureOccurs
    \/ ApplyStorageEscalates
    \/ ApplyStorageNoReplicaReports
    \/ ApplyStorageCommit

ApplyPrimaryReportNeverMakesRed ==
    copyMode[PrimaryNode] = "ApplyFailed" =>
        /\ routing.primary = PrimaryNode
        /\ routing.allocations[PrimaryNode] > 0

ApplyPrimaryReportEventuallyRejected ==
    (copyMode[PrimaryNode] = "ApplyFailed")
    ~> CommittedResult("FailShardCopy", PrimaryNode, FALSE)

ApplyStorageNoReplicaSpec ==
    /\ ApplyStorageNoReplicaInit
    /\ [][ApplyStorageNoReplicaNext]_vars
    /\ WF_vars(ApplyStorageClientWrite1)
    /\ WF_vars(ApplyStorageNoReplicaClientWrite2)
    /\ WF_vars(ApplyStoragePrimaryAccept)
    /\ WF_vars(ApplyStoragePrimaryMutationFails)
    /\ WF_vars(ApplyStoragePrimaryAck)
    /\ WF_vars(ApplyStorageFailureOccurs)
    /\ WF_vars(ApplyStorageEscalates)
    /\ WF_vars(ApplyStorageNoReplicaReports)
    /\ WF_vars(ApplyStorageCommit)

=============================================================================
