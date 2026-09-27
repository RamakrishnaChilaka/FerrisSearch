-------------------------- MODULE MC_StorageFailure -------------------------
\* R2-1 persistent storage-failure model. A live copy becomes unavailable
\* because of either definitive corruption or persistent I/O. Persistent I/O
\* first enters retry/backoff and then escalates nondeterministically after the
\* bounded retry window. Replica reports remove the copy; primary reports are
\* promote-only and require an in-sync survivor.

EXTENDS Invariants

CONSTANTS PrimaryNode, FailedNode, MetadataLeader, CandidateNode

StorageInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ MetadataLeader # FailedNode
    /\ FailedNode = PrimaryNode \/ FailedNode \in routing.inSync
    /\ IF FailedNode = PrimaryNode
          THEN CandidateNode \in routing.inSync
          ELSE CandidateNode = PrimaryNode

StorageClientWrite1 ==
    /\ nextWrite = 1
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

StorageClientWrite2 ==
    /\ nextWrite = 2
    /\ copyMode[FailedNode] = "StorageFailed"
    /\ FailedNode # routing.primary
    /\ FailedNode \notin routing.inSync
    /\ activated[routing.primary] = views[routing.primary].term
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

StoragePrimaryAccept ==
    \E writeId \in WriteIds :
        /\ PrimaryAccept(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

StorageReplicaApply ==
    \E message \in messages :
        /\ ReplicaApply(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

StorageDeliverAck ==
    \E message \in messages :
        /\ DeliverReplicaAck(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

StoragePrimaryAck ==
    \E writeId \in WriteIds :
        /\ PrimaryAck(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

StorageFailureOccurs ==
    /\ 1 \in acked
    /\ copyMode[FailedNode] = "Active"
    /\ \/ CorruptShardStorage(FailedNode)
       \/ BeginPersistentStorageFailure(FailedNode)
    /\ UNCHANGED ApplySafetyVars

StorageEscalates ==
    /\ EscalatePersistentStorageFailure(FailedNode)
    /\ UNCHANGED ApplySafetyVars

StorageReports ==
    /\ copyMode[FailedNode] = "StorageFailed"
    /\ (FailedNode = routing.primary \/ FailedNode \in routing.replicas)
    /\ ReportShardCopyFailure(FailedNode)
    /\ UNCHANGED ApplySafetyVars

StorageCommit ==
    /\ \E command \in pendingRaft : CommitRaft(command)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

StorageLifecycleActivation ==
    /\ LifecycleProposeActivation(routing.primary)
    /\ UNCHANGED <<copyAllocation, copyUuid>>
    /\ UNCHANGED <<replicaFence, durableReplicaFence>>
    /\ UNCHANGED PeerRecoveryVars
    /\ UNCHANGED ApplySafetyVars

StorageObserveActivation ==
    /\ ObserveActivation(routing.primary)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

StorageNext ==
    \/ StorageClientWrite1
    \/ StorageClientWrite2
    \/ StoragePrimaryAccept
    \/ StorageReplicaApply
    \/ StorageDeliverAck
    \/ StoragePrimaryAck
    \/ StorageFailureOccurs
    \/ StorageEscalates
    \/ StorageReports
    \/ StorageCommit
    \/ StorageLifecycleActivation
    \/ StorageObserveActivation

FailedReplicaRemoved ==
    FailedNode # PrimaryNode =>
        (copyMode[FailedNode] = "StorageFailed")
        ~> /\ FailedNode \notin routing.replicas
           /\ FailedNode \notin routing.inSync

FailedPrimaryReplaced ==
    FailedNode = PrimaryNode =>
        (copyMode[FailedNode] = "StorageFailed")
        ~> routing.primary = CandidateNode

WritesResumeAfterStorageFailure ==
    (copyMode[FailedNode] = "StorageFailed")
    ~> (2 \in acked)

StorageLivenessSpec ==
    /\ StorageInit
    /\ [][StorageNext]_vars
    /\ WF_vars(StorageClientWrite1)
    /\ WF_vars(StoragePrimaryAccept)
    /\ WF_vars(StorageReplicaApply)
    /\ WF_vars(StorageDeliverAck)
    /\ WF_vars(StoragePrimaryAck)
    /\ WF_vars(StorageFailureOccurs)
    /\ WF_vars(StorageEscalates)
    /\ WF_vars(StorageReports)
    /\ WF_vars(StorageCommit)
    /\ WF_vars(StorageLifecycleActivation)
    /\ WF_vars(StorageObserveActivation)
    /\ WF_vars(StorageClientWrite2)

StorageNoReplicaInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ PrimaryNode = FailedNode
    /\ MetadataLeader # PrimaryNode
    /\ routing.inSync = {}

StorageNoReplicaNext ==
    \/ StorageClientWrite1
    \/ StoragePrimaryAccept
    \/ StoragePrimaryAck
    \/ StorageFailureOccurs
    \/ StorageEscalates
    \/ StorageReports
    \/ StorageCommit

PrimaryReportNeverMakesRed ==
    copyMode[PrimaryNode] = "StorageFailed" =>
        /\ routing.primary = PrimaryNode
        /\ routing.allocations[PrimaryNode] > 0

PrimaryReportEventuallyRejected ==
    (copyMode[PrimaryNode] = "StorageFailed")
    ~> CommittedResult("FailShardCopy", PrimaryNode, FALSE)

StorageNoReplicaSpec ==
    /\ StorageNoReplicaInit
    /\ [][StorageNoReplicaNext]_vars
    /\ WF_vars(StorageClientWrite1)
    /\ WF_vars(StoragePrimaryAccept)
    /\ WF_vars(StoragePrimaryAck)
    /\ WF_vars(StorageFailureOccurs)
    /\ WF_vars(StorageEscalates)
    /\ WF_vars(StorageReports)
    /\ WF_vars(StorageCommit)

=============================================================================
