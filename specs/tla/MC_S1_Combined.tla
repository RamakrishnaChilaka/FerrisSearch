-------------------------- MODULE MC_S1_Combined ----------------------------
\* Combined S1 storage-failure, crash/restart, leadership-change, allocation,
\* and peer-recovery model.  The safety relation allows persistent open or
\* apply I/O, target crash before or after escalation, and a failed primary or
\* Raft leader crash while a promote-only report is pending.  Repair happens
\* only after exact-allocation removal; recovery then installs the fresh
\* assignment on the same three-node topology.

EXTENDS Invariants

CONSTANTS PrimaryNode, TargetNode, MetadataLeader

FailurePending(target) ==
    \E command \in pendingRaft :
        /\ command.kind = "FailShardCopy"
        /\ command.target = target

PromoteOnlyFailurePending(target) ==
    \E command \in pendingRaft :
        /\ command.kind = "FailShardCopy"
        /\ command.target = target
        /\ command.newPrimary \in Nodes

FailureAccepted(target) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command.kind = "FailShardCopy"
        /\ raftLog[position].command.target = target
        /\ raftLog[position].accepted

CommittedFailureReportCount(target) ==
    Cardinality(
        {position \in 1..Len(raftLog) :
            /\ raftLog[position].command.kind = "FailShardCopy"
            /\ raftLog[position].command.target = target})

S1CombinedInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ \/ TargetNode = PrimaryNode
       \/ TargetNode \in routing.inSync

S1ClientWrite1 ==
    /\ nextWrite = 1
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

\* The first failed apply consumes one request.  If the target then crashes,
\* CrashRecoveryState resets ApplyRetrying to ApplyFailing, and the next
\* request redetects the persistent fault with a fresh process-local budget.
S1ClientWriteForApplyFailure ==
    /\ nextWrite \in 2..MaxWrites
    /\ copyMode[TargetNode] = "ApplyFailing"
    /\ routing.allocations[TargetNode] > 0
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1ClientWriteAfterRecovery ==
    /\ nextWrite = MaxWrites
    /\ TargetNode \in routing.inSync
    /\ routing.allocations[TargetNode] > 1
    /\ copyMode[TargetNode] = "Active"
    /\ ClientWrite(routing.primary, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1LivenessFirstApplyWrite ==
    /\ crashCount = 0
    /\ nextWrite = 2
    /\ S1ClientWriteForApplyFailure

\* This request starts while the still-routed in-sync target is down. Rust's
\* TransportClient timeout must eventually fail it.
S1LivenessUnavailableWrite ==
    /\ crashCount = 1
    /\ ~alive[TargetNode]
    /\ nextWrite = 3
    /\ S1ClientWriteForApplyFailure

S1LivenessRedetectWrite ==
    /\ crashCount = 1
    /\ alive[TargetNode]
    /\ faultsStopped
    /\ nextWrite = 4
    /\ S1ClientWriteForApplyFailure

S1PrimaryAccept ==
    \E writeId \in WriteIds :
        /\ PrimaryAccept(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1PrimaryReject ==
    \E writeId \in WriteIds :
        /\ PrimaryReject(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1PrimaryMutationFails ==
    \E writeId \in WriteIds :
        /\ PrimaryApplyFailure(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1ReplicaApply ==
    \E message \in messages :
        /\ ReplicaApply(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

S1ReplicaMutationFails ==
    \E message \in messages :
        /\ ReplicaApplyFailure(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

S1ReplicaReject ==
    \E message \in messages :
        /\ ReplicaReject(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1DeliverAck ==
    \E message \in messages :
        /\ DeliverReplicaAck(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1DeliverNack ==
    \E message \in messages :
        /\ DeliverReplicaNack(message)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1PrimaryAck ==
    \E writeId \in WriteIds :
        /\ PrimaryAck(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1PrimaryFail ==
    \E writeId \in WriteIds :
        /\ PrimaryFail(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

OutstandingReplicaTraffic(writeId, replica) ==
    {message \in messages :
        /\ message.write = writeId
        /\ \/ /\ message.kind = "Replicate"
              /\ message.to = replica
           \/ /\ message.kind \in {"ReplicaAck", "ReplicaNack"}
              /\ message.from = replica}

ReplicaCannotRespond(writeId, replica) ==
    /\ replica \in writeWait[writeId]
    /\ \/ ~alive[replica]
       \/ \E message \in messages :
             /\ message.kind = "Replicate"
             /\ message.write = writeId
             /\ message.to = replica
             /\ epoch[replica] # message.toEpoch
       \/ OutstandingReplicaTraffic(writeId, replica) = {}

\* replication::replicate_write returns a request error after the transport
\* timeout when a required replica is down, has restarted past the request's
\* target epoch, or the request/response was already dropped. Healthy targets
\* are assumed to respond before the timeout.
S1TimedOutReplicationFails ==
    \E writeId \in WriteIds :
        /\ writeStatus[writeId] = "Replicating"
        /\ \E replica \in writeWait[writeId] :
              ReplicaCannotRespond(writeId, replica)
        /\ PrimaryFail(writeId)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
                ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1OpenFailureOccurs ==
    /\ 1 \in acked
    /\ BeginPersistentStorageFailure(TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1ApplyFailureOccurs ==
    /\ 1 \in acked
    /\ BeginPersistentApplyFailure(TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1RedetectOpenFailure ==
    /\ RedetectPersistentStorageFailure(TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1EscalateFailure ==
    /\ \/ EscalatePersistentStorageFailure(TargetNode)
       \/ EscalatePersistentApplyFailure(TargetNode)
    /\ UNCHANGED ApplySafetyVars

\* The failed process can restart with a fresh retry budget before escalation.
S1CrashBeforeEscalation ==
    /\ crashCount = 0
    /\ copyMode[TargetNode] \in {"StorageRetrying", "ApplyRetrying"}
    /\ Crash(TargetNode)
    /\ UNCHANGED ApplySafetyVars

\* It can also crash after escalation but before the report is queued.
S1CrashAfterEscalation ==
    /\ crashCount = 0
    /\ copyMode[TargetNode] \in ReportableStorageFailureModes
    /\ ~FailurePending(TargetNode)
    /\ Crash(TargetNode)
    /\ UNCHANGED ApplySafetyVars

\* For a promote-only primary report, either the failed primary or the current
\* Raft leader may crash after queueing.  The pending command remains durable
\* and a later leader still performs the allocation CAS and in-sync check.
S1CrashWithPromoteReportInFlight ==
    /\ crashCount = 0
    /\ TargetNode = PrimaryNode
    /\ PromoteOnlyFailurePending(TargetNode)
    /\ \E node \in Nodes :
          /\ node \in {TargetNode, raftLeader}
          /\ Crash(node)
    /\ UNCHANGED ApplySafetyVars

S1Restart ==
    /\ \E node \in Nodes : Restart(node)
    /\ UNCHANGED ApplySafetyVars

S1LivenessRestart ==
    /\ nextWrite = 4
    /\ writeStatus[3] = "Failed"
    /\ S1Restart

S1ElectLeader ==
    /\ \E node \in Nodes : ElectLeader(node)
    /\ UNCHANGED ApplySafetyVars

S1StopFaults ==
    /\ storageFaultInjected
    /\ crashCount = MaxCrashes
    /\ \A node \in Nodes : alive[node]
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase, storageFaultInjected>>

S1ReportFailure ==
    /\ copyMode[TargetNode] \in ReportableStorageFailureModes
    /\ \E candidate \in Nodes \cup {NoNode} :
          ReportShardCopyFailure(TargetNode, candidate)
    /\ UNCHANGED ApplySafetyVars

\* The bounded liveness scenario permits the accepted report plus one delayed
\* duplicate that commits as rejected. Safety checks retain unrestricted
\* duplicate submissions.
S1LivenessReportFailure ==
    /\ CommittedFailureReportCount(TargetNode) < 2
    /\ S1ReportFailure

S1Commit ==
    /\ \E command \in pendingRaft : CommitRaft(command)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

S1RepairStorage ==
    /\ FailureAccepted(TargetNode)
    /\ RepairPersistentStorageFault(TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1AllocateReplacement ==
    /\ FailureAccepted(TargetNode)
    /\ \E leader \in Nodes :
          AllocateAfterLifecycle(leader, TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1ObserveAllocation ==
    /\ \/ ObserveAllocationAccepted(TargetNode)
       \/ ObserveAllocationRejected(TargetNode)
    /\ UNCHANGED ApplySafetyVars

S1DeliverView ==
    /\ \E node \in Nodes : DeliverView(node)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

S1LifecycleActivation ==
    /\ \E node \in Nodes : LifecycleProposeActivation(node)
    /\ UNCHANGED <<copyAllocation, copyUuid>>
    /\ UNCHANGED <<replicaFence, durableReplicaFence>>
    /\ UNCHANGED PeerRecoveryVars
    /\ UNCHANGED ApplySafetyVars

S1ObserveActivation ==
    /\ \E node \in Nodes : ObserveActivation(node)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

S1CancelActivation ==
    /\ \E node \in Nodes : CancelActivation(node)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

S1StartRecovery ==
    /\ StartRecovery(TargetNode, routing.primary)
    /\ UNCHANGED FaultVars

S1Snapshot ==
    /\ SourceSnapshot(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1BeginInstall ==
    /\ TargetBeginInstall(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1Install ==
    /\ InstallSnapshot(TargetNode)
    /\ UNCHANGED
          <<ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1FetchOps ==
    /\ FetchOps(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1ApplyOps ==
    /\ ApplyOps(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1FinishCatchUp ==
    /\ FinishCatchUp(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1BeginPrepare ==
    /\ BeginPrepareFinalize(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1AcquireBarrier ==
    /\ AcquireFinalizeBarrier(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1FinishTail ==
    /\ FinishFinalizeTail(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1TargetComplete ==
    /\ TargetComplete(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, FaultVars>>

S1BeginSettlement ==
    /\ BeginSettlement(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1ProposeMark ==
    /\ ProposeMarkInSync(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

S1ObserveAdmission ==
    /\ ObserveAdmission(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, pendingAllocation, FaultVars>>

S1TargetAdmitted ==
    /\ TargetObserveAdmitted(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, FaultVars>>

S1TargetRejected ==
    /\ TargetObserveRejected(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, FaultVars>>

S1PersistentFaultSurvivesRestart ==
    /\ storageFaultInjected
    /\ epoch[TargetNode] > 0
    /\ routing.allocations[TargetNode] = 1
    => copyMode[TargetNode] \in AllStorageFailureModes

S1CrashedFaultUsesFreshBudget ==
    /\ storageFaultInjected
    /\ ~alive[TargetNode]
    => copyMode[TargetNode]
       \in {"StorageCorrupt", "StorageFailing", "ApplyFailing"}

S1RecoveredCopyUsesFreshAllocation ==
    /\ FailureAccepted(TargetNode)
    /\ TargetNode \in routing.inSync
    => routing.allocations[TargetNode] > 1

S1SafetyNext ==
    \/ S1ClientWrite1
    \/ S1ClientWriteForApplyFailure
    \/ S1ClientWriteAfterRecovery
    \/ S1PrimaryAccept
    \/ S1PrimaryReject
    \/ S1PrimaryMutationFails
    \/ S1ReplicaApply
    \/ S1ReplicaMutationFails
    \/ S1ReplicaReject
    \/ S1DeliverAck
    \/ S1DeliverNack
    \/ S1PrimaryAck
    \/ S1PrimaryFail
    \/ S1OpenFailureOccurs
    \/ S1ApplyFailureOccurs
    \/ S1RedetectOpenFailure
    \/ S1EscalateFailure
    \/ S1CrashBeforeEscalation
    \/ S1CrashAfterEscalation
    \/ S1CrashWithPromoteReportInFlight
    \/ S1Restart
    \/ S1ElectLeader
    \/ S1StopFaults
    \/ S1ReportFailure
    \/ S1Commit
    \/ S1RepairStorage
    \/ S1AllocateReplacement
    \/ S1ObserveAllocation
    \/ S1DeliverView
    \/ S1LifecycleActivation
    \/ S1ObserveActivation
    \/ S1CancelActivation
    \/ S1StartRecovery
    \/ S1Snapshot
    \/ S1BeginInstall
    \/ S1Install
    \/ S1FetchOps
    \/ S1ApplyOps
    \/ S1FinishCatchUp
    \/ S1BeginPrepare
    \/ S1AcquireBarrier
    \/ S1FinishTail
    \/ S1TargetComplete
    \/ S1BeginSettlement
    \/ S1ProposeMark
    \/ S1ObserveAdmission
    \/ S1TargetAdmitted
    \/ S1TargetRejected

\* Liveness deliberately forces the harder apply-failure path: one failed
\* request, target crash/reset, restart, another failed request, escalation,
\* exact removal, repair, fresh allocation, recovery, and a final write.
S1LivenessEscalate ==
    /\ crashCount = 1
    /\ faultsStopped
    /\ S1EscalateFailure

S1LivenessNoTimeoutNext ==
    \/ S1ClientWrite1
    \/ S1LivenessFirstApplyWrite
    \/ S1LivenessUnavailableWrite
    \/ S1LivenessRedetectWrite
    \/ S1ClientWriteAfterRecovery
    \/ S1PrimaryAccept
    \/ S1PrimaryMutationFails
    \/ S1ReplicaApply
    \/ S1ReplicaMutationFails
    \/ S1DeliverAck
    \/ S1DeliverNack
    \/ S1PrimaryAck
    \/ S1ApplyFailureOccurs
    \/ S1CrashBeforeEscalation
    \/ S1LivenessRestart
    \/ S1StopFaults
    \/ S1LivenessEscalate
    \/ S1LivenessReportFailure
    \/ S1Commit
    \/ S1RepairStorage
    \/ S1AllocateReplacement
    \/ S1ObserveAllocation
    \/ S1DeliverView
    \/ S1StartRecovery
    \/ S1Snapshot
    \/ S1BeginInstall
    \/ S1Install
    \/ S1FetchOps
    \/ S1ApplyOps
    \/ S1FinishCatchUp
    \/ S1BeginPrepare
    \/ S1AcquireBarrier
    \/ S1FinishTail
    \/ S1TargetComplete
    \/ S1BeginSettlement
    \/ S1ProposeMark
    \/ S1ObserveAdmission
    \/ S1TargetAdmitted

S1LivenessNext ==
    \/ S1LivenessNoTimeoutNext
    \/ S1TimedOutReplicationFails

S1WritesResume ==
    faultsStopped ~> (MaxWrites \in acked)

S1ReplacementEventuallyInSync ==
    FailureAccepted(TargetNode)
    ~> /\ TargetNode \in routing.inSync
       /\ routing.allocations[TargetNode] > 1

S1UnavailableWriteCompletes ==
    (writeStatus[3] = "Replicating")
    ~> (writeStatus[3] = "Failed")

S1CombinedLivenessSpec ==
    /\ S1CombinedInit
    /\ [][S1LivenessNext]_vars
    /\ WF_vars(S1ClientWrite1)
    /\ WF_vars(S1LivenessFirstApplyWrite)
    /\ WF_vars(S1LivenessUnavailableWrite)
    /\ WF_vars(S1LivenessRedetectWrite)
    /\ WF_vars(S1ClientWriteAfterRecovery)
    /\ WF_vars(S1PrimaryAccept)
    /\ WF_vars(S1PrimaryMutationFails)
    /\ WF_vars(S1ReplicaApply)
    /\ WF_vars(S1ReplicaMutationFails)
    /\ WF_vars(S1DeliverAck)
    /\ WF_vars(S1DeliverNack)
    /\ WF_vars(S1PrimaryAck)
    /\ WF_vars(S1TimedOutReplicationFails)
    /\ WF_vars(S1ApplyFailureOccurs)
    /\ WF_vars(S1CrashBeforeEscalation)
    /\ WF_vars(S1LivenessRestart)
    /\ WF_vars(S1StopFaults)
    /\ WF_vars(S1LivenessEscalate)
    /\ WF_vars(S1LivenessReportFailure)
    /\ WF_vars(S1Commit)
    /\ WF_vars(S1RepairStorage)
    /\ WF_vars(S1AllocateReplacement)
    /\ WF_vars(S1ObserveAllocation)
    /\ WF_vars(S1DeliverView)
    /\ WF_vars(S1StartRecovery)
    /\ WF_vars(S1Snapshot)
    /\ WF_vars(S1BeginInstall)
    /\ WF_vars(S1Install)
    /\ WF_vars(S1FetchOps)
    /\ WF_vars(S1ApplyOps)
    /\ WF_vars(S1FinishCatchUp)
    /\ WF_vars(S1BeginPrepare)
    /\ WF_vars(S1AcquireBarrier)
    /\ WF_vars(S1FinishTail)
    /\ WF_vars(S1TargetComplete)
    /\ WF_vars(S1BeginSettlement)
    /\ WF_vars(S1ProposeMark)
    /\ WF_vars(S1ObserveAdmission)
    /\ WF_vars(S1TargetAdmitted)

S1CombinedLivenessNoTimeoutSpec ==
    /\ S1CombinedInit
    /\ [][S1LivenessNoTimeoutNext]_vars
    /\ WF_vars(S1ClientWrite1)
    /\ WF_vars(S1LivenessFirstApplyWrite)
    /\ WF_vars(S1LivenessUnavailableWrite)
    /\ WF_vars(S1LivenessRedetectWrite)
    /\ WF_vars(S1ClientWriteAfterRecovery)
    /\ WF_vars(S1PrimaryAccept)
    /\ WF_vars(S1PrimaryMutationFails)
    /\ WF_vars(S1ReplicaApply)
    /\ WF_vars(S1ReplicaMutationFails)
    /\ WF_vars(S1DeliverAck)
    /\ WF_vars(S1DeliverNack)
    /\ WF_vars(S1PrimaryAck)
    /\ WF_vars(S1ApplyFailureOccurs)
    /\ WF_vars(S1CrashBeforeEscalation)
    /\ WF_vars(S1LivenessRestart)
    /\ WF_vars(S1StopFaults)
    /\ WF_vars(S1LivenessEscalate)
    /\ WF_vars(S1LivenessReportFailure)
    /\ WF_vars(S1Commit)
    /\ WF_vars(S1RepairStorage)
    /\ WF_vars(S1AllocateReplacement)
    /\ WF_vars(S1ObserveAllocation)
    /\ WF_vars(S1DeliverView)
    /\ WF_vars(S1StartRecovery)
    /\ WF_vars(S1Snapshot)
    /\ WF_vars(S1BeginInstall)
    /\ WF_vars(S1Install)
    /\ WF_vars(S1FetchOps)
    /\ WF_vars(S1ApplyOps)
    /\ WF_vars(S1FinishCatchUp)
    /\ WF_vars(S1BeginPrepare)
    /\ WF_vars(S1AcquireBarrier)
    /\ WF_vars(S1FinishTail)
    /\ WF_vars(S1TargetComplete)
    /\ WF_vars(S1BeginSettlement)
    /\ WF_vars(S1ProposeMark)
    /\ WF_vars(S1ObserveAdmission)
    /\ WF_vars(S1TargetAdmitted)

=============================================================================
