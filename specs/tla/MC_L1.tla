------------------------------- MODULE MC_L1 -------------------------------
\* Fault-free liveness slice.  This excludes setup failure, cancellation,
\* expiry, message loss, and settlement timeout: the liveness assumptions are
\* that faults have stopped, the primary stays alive, and Raft/view delivery
\* continues.  Safety configurations cover the excluded failure branches.

EXTENDS Invariants

CONSTANTS PrimaryNode, TargetNode

L1Init ==
    /\ Init
    /\ PrimaryNode # TargetNode
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ TargetNode \in routing.replicas
    /\ TargetNode \notin routing.inSync

L1Start ==
    /\ StartRecovery(TargetNode, PrimaryNode)
    /\ UNCHANGED FaultVars

L1Snapshot ==
    /\ SourceSnapshot(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1BeginInstall ==
    /\ TargetBeginInstall(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1Install ==
    /\ InstallSnapshot(TargetNode)
    /\ UNCHANGED <<sessionAllocation, pendingAllocation, FaultVars>>

L1FetchOps ==
    /\ FetchOps(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1ApplyOps ==
    /\ ApplyOps(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1FinishCatchUp ==
    /\ FinishCatchUp(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1BeginPrepare ==
    /\ BeginPrepareFinalize(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1AcquireBarrier ==
    /\ AcquireFinalizeBarrier(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1FinishTail ==
    /\ FinishFinalizeTail(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1TargetComplete ==
    /\ TargetComplete(TargetNode)
    /\ UNCHANGED <<copyAllocation, sessionAllocation, FaultVars>>

L1BeginSettlement ==
    /\ BeginSettlement(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1ProposeMark ==
    /\ ProposeMarkInSync(TargetNode)
    /\ UNCHANGED
          <<copyAllocation, sessionAllocation, pendingAllocation, FaultVars>>

L1ObserveAdmission ==
    /\ ObserveAdmission(TargetNode)
    /\ UNCHANGED <<copyAllocation, pendingAllocation, FaultVars>>

L1TargetAdmitted ==
    /\ TargetObserveAdmitted(TargetNode)
    /\ UNCHANGED <<copyAllocation, sessionAllocation, FaultVars>>

CommitPending ==
    \E command \in pendingRaft : CommitRaft(command)

L1Commit ==
    /\ CommitPending
    /\ UNCHANGED <<copyAllocation, PeerRecoveryVars, FaultVars>>

L1DeliverTargetView ==
    /\ DeliverView(TargetNode)
    /\ UNCHANGED <<copyAllocation, PeerRecoveryVars, FaultVars>>

L1Next ==
    \/ L1Start
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
    \/ L1Commit
    \/ L1ObserveAdmission
    \/ L1DeliverTargetView
    \/ L1TargetAdmitted

LivenessSpec ==
    /\ L1Init
    /\ [][L1Next]_vars
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
