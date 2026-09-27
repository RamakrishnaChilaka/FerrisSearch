--------------------------- MODULE MC_L2_Promotion --------------------------
\* B2 different-primary slice.  A target is pending under PrimaryNode when
\* that source crashes.  MetadataLeader promotes the distinct in-sync
\* CandidateNode.  The target's ordered view either already contains admission
\* or definitively rejects the old pending primary/term.

EXTENDS MC_L1

CONSTANTS MetadataLeader, CandidateNode

PromotionInit ==
    /\ Init
    /\ PrimaryNode # TargetNode
    /\ PrimaryNode # CandidateNode
    /\ TargetNode # CandidateNode
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ MetadataLeader = CandidateNode
    /\ CandidateNode \in routing.inSync
    /\ TargetNode \in routing.replicas
    /\ TargetNode \notin routing.inSync
    /\ ~faultsStopped

PromotionCrashPrimary ==
    /\ sessionMarkSubmitted[TargetNode]
    /\ copyMode[TargetNode] = "Pending"
    /\ Crash(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

PromotionPropose ==
    /\ SuspectAndRemove(MetadataLeader, PrimaryNode, CandidateNode)
    /\ UNCHANGED ApplySafetyVars

PromotionStopFaults ==
    /\ crashCount = 1
    /\ routing.primary = CandidateNode
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase>>

PromotionCommit ==
    \E command \in pendingRaft :
        /\ IF command.kind = "MarkReplicaInSync"
              THEN routing.primary = CandidateNode
              ELSE TRUE
        /\ CommitRaft(command)
        /\ UNCHANGED
              <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

PromotionNext ==
    \/ L1RecoveryNext
    \/ PromotionCrashPrimary
    \/ PromotionPropose
    \/ PromotionCommit
    \/ PromotionStopFaults

PromotionLivenessSpec ==
    /\ PromotionInit
    /\ [][PromotionNext]_vars
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
    /\ WF_vars(PromotionCrashPrimary)
    /\ WF_vars(PromotionPropose)
    /\ WF_vars(PromotionCommit)
    /\ WF_vars(PromotionStopFaults)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1DeliverTargetView)
    /\ WF_vars(L1TargetAdmitted)
    /\ WF_vars(L1TargetRejected)

=============================================================================
