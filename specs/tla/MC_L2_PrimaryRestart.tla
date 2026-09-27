------------------------ MODULE MC_L2_PrimaryRestart ------------------------
\* B2 primary re-activation slice.  A target is pending with an admission
\* command queued when the source primary crashes.  After restart and election,
\* ensure_primary_activated commits a newer term.  The old pending record must
\* be admitted if its mark won first or definitively rejected after reactivation.

EXTENDS MC_L1

PrimaryRestartInit ==
    /\ Init
    /\ PrimaryNode # TargetNode
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ TargetNode \in routing.replicas
    /\ TargetNode \notin routing.inSync
    /\ ~faultsStopped

PR2CrashPrimary ==
    /\ sessionMarkSubmitted[TargetNode]
    /\ copyMode[TargetNode] = "Pending"
    /\ Crash(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

PR2RestartPrimary ==
    /\ Restart(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

PR2ElectPrimary ==
    /\ ElectLeader(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

PR2StopFaults ==
    /\ crashCount = 1
    /\ alive[PrimaryNode]
    /\ raftLeader = PrimaryNode
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase, storageFaultInjected>>

PR2LifecycleActivation ==
    LifecycleProposeActivation(PrimaryNode)

PR2ObserveActivation ==
    /\ ObserveActivation(PrimaryNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

PR2Commit ==
    \E command \in pendingRaft :
        /\ IF command.kind = "MarkReplicaInSync"
              THEN routing.term > command.expectedTerm
              ELSE TRUE
        /\ CommitRaft(command)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

PR2Next ==
    \/ L1RecoveryNext
    \/ PR2CrashPrimary
    \/ PR2RestartPrimary
    \/ PR2ElectPrimary
    \/ PR2StopFaults
    \/ PR2LifecycleActivation
    \/ PR2Commit
    \/ PR2ObserveActivation

PrimaryRestartSpec ==
    /\ PrimaryRestartInit
    /\ [][PR2Next]_vars
    /\ WF_vars(L1Start)
    /\ WF_vars(L1Snapshot)
    /\ WF_vars(L1BeginInstall)
    /\ WF_vars(L1Install)
    /\ WF_vars(L1FinishCatchUp)
    /\ WF_vars(L1BeginPrepare)
    /\ WF_vars(L1AcquireBarrier)
    /\ WF_vars(L1TargetComplete)
    /\ WF_vars(L1BeginSettlement)
    /\ WF_vars(L1ProposeMark)
    /\ WF_vars(PR2CrashPrimary)
    /\ WF_vars(PR2RestartPrimary)
    /\ WF_vars(PR2ElectPrimary)
    /\ WF_vars(PR2StopFaults)
    /\ WF_vars(PR2LifecycleActivation)
    /\ WF_vars(PR2Commit)
    /\ WF_vars(PR2ObserveActivation)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1DeliverTargetView)
    /\ WF_vars(L1TargetAdmitted)
    /\ WF_vars(L1TargetRejected)

=============================================================================
