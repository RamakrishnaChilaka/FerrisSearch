------------------------------- MODULE MC_L2 -------------------------------
\* Crash/restart liveness slice.  One target crash is weakly fair, restart is
\* weakly fair, and faults then stop permanently.  The crash is transient and
\* completes before the failure detector removes the assignment; membership
\* removal/reallocation liveness is covered by separate safety configurations.

EXTENDS MC_L1

L2Init ==
    /\ L1Init
    /\ ~faultsStopped

L2Crash ==
    /\ crashCount = 0
    /\ Crash(TargetNode)
    /\ UNCHANGED ApplySafetyVars

L2Restart ==
    /\ crashCount = 1
    /\ Restart(TargetNode)
    /\ UNCHANGED ApplySafetyVars

L2StopFaults ==
    /\ crashCount = 1
    /\ alive[TargetNode]
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase>>

L2Next ==
    \/ L1Next
    \/ L2Crash
    \/ L2Restart
    \/ L2StopFaults

CrashLivenessSpec ==
    /\ L2Init
    /\ [][L2Next]_vars
    /\ WF_vars(L2Crash)
    /\ WF_vars(L2Restart)
    /\ WF_vars(L2StopFaults)
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
    /\ WF_vars(L1Commit)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1DeliverTargetView)
    /\ WF_vars(L1TargetAdmitted)

=============================================================================
