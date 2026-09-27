------------------ MODULE MC_L2_PrimaryRestart_NoTrigger -------------------
\* Historical R2-2 variant. The proactive lifecycle activation transition is
\* present but deliberately has no fairness assumption. An idle restarted
\* primary can therefore stutter forever while its target remains pending.

EXTENDS MC_L2_PrimaryRestart

NoTriggerSpec ==
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
    /\ WF_vars(PR2Commit)
    /\ WF_vars(PR2ObserveActivation)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1DeliverTargetView)
    /\ WF_vars(L1TargetAdmitted)
    /\ WF_vars(L1TargetRejected)

=============================================================================
