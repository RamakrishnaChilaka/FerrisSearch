-------------------------- MODULE MC_G2_Liveness ---------------------------
\* Fair replica disk-loss recovery.  One in-sync replica crashes, loses its
\* shard disk, restarts, reports the exact failed allocation, and faults stop.
\* The leader removes it, a fresh allocation is assigned, a delayed old-ID
\* failure report is rejected, a write succeeds, and peer recovery admits the
\* replacement.

EXTENDS MC_L1

CONSTANT MetadataLeader

FailureAccepted(target) ==
    \E position \in 1..Len(raftLog) :
        /\ raftLog[position].command.kind = "FailShardCopy"
        /\ raftLog[position].command.target = target
        /\ raftLog[position].accepted

RejectedStaleFailure(target) ==
    \E position \in 1..Len(raftLog) :
        LET entry == raftLog[position]
            command == entry.command
        IN /\ command.kind = "FailShardCopy"
           /\ command.target = target
           /\ command.expectedAllocation = 1
           /\ PriorAllocation(position, target) # 1
           /\ ~entry.accepted

G2LivenessInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ MetadataLeader = PrimaryNode
    /\ TargetNode \in routing.inSync
    /\ ~faultsStopped

G2CrashReplica ==
    /\ crashCount = 0
    /\ Crash(TargetNode)
    /\ UNCHANGED ApplySafetyVars

G2LoseReplicaDisk ==
    /\ DiskLoss(TargetNode)
    /\ UNCHANGED ApplySafetyVars

G2RestartReplica ==
    /\ Restart(TargetNode)
    /\ UNCHANGED ApplySafetyVars

G2StopFaults ==
    /\ crashCount = 1
    /\ diskLost[TargetNode]
    /\ alive[TargetNode]
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase, storageFaultInjected>>

G2ReportFailure ==
    /\ ~FailureAccepted(TargetNode)
    /\ ReportShardCopyFailure(TargetNode, NoNode)
    /\ UNCHANGED ApplySafetyVars

G2Commit ==
    /\ CommitPending
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

G2AllocateReplacement ==
    /\ FailureAccepted(TargetNode)
    /\ AllocateAfterLifecycle(MetadataLeader, TargetNode)
    /\ UNCHANGED ApplySafetyVars

G2ObserveAllocation ==
    /\ ObserveAllocationAccepted(TargetNode)
    /\ UNCHANGED ApplySafetyVars

\* A delayed request from the failed allocation arrives after the allocator
\* has installed a fresh routing identity.
G2SubmitStaleFailure ==
    LET stale ==
            FailShardCopyCommand(TargetNode, 1, NoNode)
    IN
    /\ routing.allocations[TargetNode] > 1
    /\ ~RejectedStaleFailure(TargetNode)
    /\ QueueRaft(stale)
    /\ UNCHANGED
          <<routing, alive, epoch, raftConnected, activated,
            activationPending, nextWrite, writeStatus, writeDoc, writeKind,
            writeTarget, writePrimary, writeEpoch, writeSeq, writeTerm,
            writeRequired, writeWait, ops, durableOps, docValue, nextSeq,
            committed, truncBelow, pins, copyExists, copyAllocation, copyUuid,
            replicaFence, durableReplicaFence, copyMode, installMarker,
            messages, sharedHolders, exclusiveHolder, acked, failed,
            promotionSafe, admissionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, PeerRecoveryVars, FaultVars>>

G2DeliverTargetView ==
    /\ DeliverView(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

G2ClientWrite ==
    /\ faultsStopped
    /\ FailureAccepted(TargetNode)
    /\ nextWrite = 1
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G2PrimaryAccept ==
    /\ PrimaryAccept(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G2PrimaryAck ==
    /\ PrimaryAck(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G2StartRecovery ==
    /\ 1 \in acked
    /\ RejectedStaleFailure(TargetNode)
    /\ L1Start

G2LivenessNext ==
    \/ G2CrashReplica
    \/ G2LoseReplicaDisk
    \/ G2RestartReplica
    \/ G2StopFaults
    \/ G2ReportFailure
    \/ G2Commit
    \/ G2AllocateReplacement
    \/ G2ObserveAllocation
    \/ G2SubmitStaleFailure
    \/ G2DeliverTargetView
    \/ G2ClientWrite
    \/ G2PrimaryAccept
    \/ G2PrimaryAck
    \/ G2StartRecovery
    \/ L1Snapshot
    \/ L1BeginInstall
    \/ L1Install
    \/ L1FetchOps
    \/ L1ApplyOps
    \/ L1FinishCatchUp
    \/ L1BeginPrepare
    \/ L1AcquireBarrier
    \/ L1FinishTail
    \/ L1TargetComplete
    \/ L1BeginSettlement
    \/ L1ProposeMark
    \/ L1ObserveAdmission
    \/ L1TargetAdmitted

WritesResumeAfterReplicaDiskLoss ==
    diskLost[TargetNode] ~> (1 \in acked)

ReplacementEventuallyInSync ==
    diskLost[TargetNode]
    ~> /\ TargetNode \in routing.inSync
       /\ routing.allocations[TargetNode] > 1

StaleFailureEventuallyRejected ==
    (routing.allocations[TargetNode] > 1)
    ~> RejectedStaleFailure(TargetNode)

G2LivenessSpec ==
    /\ G2LivenessInit
    /\ [][G2LivenessNext]_vars
    /\ WF_vars(G2CrashReplica)
    /\ WF_vars(G2LoseReplicaDisk)
    /\ WF_vars(G2RestartReplica)
    /\ WF_vars(G2StopFaults)
    /\ WF_vars(G2ReportFailure)
    /\ WF_vars(G2Commit)
    /\ WF_vars(G2AllocateReplacement)
    /\ WF_vars(G2ObserveAllocation)
    /\ WF_vars(G2SubmitStaleFailure)
    /\ WF_vars(G2DeliverTargetView)
    /\ WF_vars(G2ClientWrite)
    /\ WF_vars(G2PrimaryAccept)
    /\ WF_vars(G2PrimaryAck)
    /\ WF_vars(G2StartRecovery)
    /\ WF_vars(L1Snapshot)
    /\ WF_vars(L1BeginInstall)
    /\ WF_vars(L1Install)
    /\ WF_vars(L1FetchOps)
    /\ WF_vars(L1ApplyOps)
    /\ WF_vars(L1FinishCatchUp)
    /\ WF_vars(L1BeginPrepare)
    /\ WF_vars(L1AcquireBarrier)
    /\ WF_vars(L1FinishTail)
    /\ WF_vars(L1TargetComplete)
    /\ WF_vars(L1BeginSettlement)
    /\ WF_vars(L1ProposeMark)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1TargetAdmitted)

=============================================================================
