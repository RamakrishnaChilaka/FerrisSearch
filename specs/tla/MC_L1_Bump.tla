----------------------------- MODULE MC_L1_Bump -----------------------------
\* Review-derived liveness check: the fault-free L1 slice plus the settlement
\* deadline ActivatePrimary term bump.  Before B2, the target observed the
\* newer term as Unknown forever.  With the restored ordering it rejects the
\* stale pending marker and resolves.

EXTENDS MC_L1

L1Deadline ==
    /\ SettlementDeadline(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, sessionAllocation, pendingAllocation, FaultVars>>

BumpNext == L1Next \/ L1Deadline

BumpLivenessSpec ==
    /\ L1Init
    /\ [][BumpNext]_vars
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
    /\ WF_vars(L1TargetRejected)

=============================================================================
