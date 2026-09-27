------------------------- MODULE MC_PendingRestart --------------------------
\* B3 restart regression.  An acknowledged write is copied into a finalized
\* pending target, the source queues MarkReplicaInSync, and the target restarts
\* before that command commits.  The fixed variant restores runtime Pending
\* state from the matching durable marker.  The historical variant reattaches
\* to the settling source session and wipes the finalized copy.

EXTENDS MC_L1

PendingRestartInit ==
    /\ Init
    /\ PrimaryNode # TargetNode
    /\ routing.primary = PrimaryNode
    /\ raftLeader = PrimaryNode
    /\ TargetNode \in routing.replicas
    /\ TargetNode \notin routing.inSync
    /\ ~faultsStopped

PRClientWrite ==
    /\ nextWrite = 1
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

PRPrimaryAccept ==
    /\ PrimaryAccept(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

PRPrimaryAck ==
    /\ PrimaryAck(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

PRStart ==
    /\ 1 \in acked
    /\ L1Start

PRCrashTarget ==
    /\ sessionMarkSubmitted[TargetNode]
    /\ copyMode[TargetNode] = "Pending"
    /\ Crash(TargetNode)
    /\ UNCHANGED ApplySafetyVars

PRRestartTarget ==
    /\ Restart(TargetNode)
    /\ UNCHANGED ApplySafetyVars

PRStopFaults ==
    /\ crashCount = 1
    /\ alive[TargetNode]
    /\ ~faultsStopped
    /\ faultsStopped' = TRUE
    /\ UNCHANGED
          <<RaftVars, ReplicationVars, PeerRecoveryVars, crashCount,
            partitionCount, diskLost, lifecyclePhase>>

PRRestorePending ==
    /\ RestorePendingMarker(TargetNode)
    /\ UNCHANGED <<ApplySafetyVars, FaultVars>>

PRCommitFixed ==
    /\ RestorePendingOnRestart
    /\ alive[TargetNode]
    /\ epoch[TargetNode] = 1
    /\ copyMode[TargetNode] = "Pending"
    /\ L1Commit

PRLegacyWipeAndDelayedAdmission ==
    LET command == MarkInSyncCommand(TargetNode)
        after == AfterAcceptedCommand(routing, command)
    IN
    /\ ~RestorePendingOnRestart
    /\ alive[TargetNode]
    /\ epoch[TargetNode] = 1
    /\ sessionPhase[TargetNode] = "Settling"
    /\ copyMode[TargetNode] # "Pending"
    /\ PendingMarkerMatchesCopy(TargetNode)
    /\ command \in pendingRaft
    /\ CommandAccepted(routing, command)
    /\ pendingRaft' = pendingRaft \ {command}
    /\ routing' = after
    /\ views' = [views EXCEPT ![raftLeader] = after]
    /\ copyMode' = [copyMode EXCEPT ![TargetNode] = "Recovering"]
    /\ installMarker' = [installMarker EXCEPT ![TargetNode] = TRUE]
    /\ ops' = [ops EXCEPT ![TargetNode] = {}]
    /\ durableOps' = [durableOps EXCEPT ![TargetNode] = {}]
    /\ docValue' =
          [docValue EXCEPT ![TargetNode] = [doc \in Docs |-> NoWrite]]
    /\ nextSeq' = [nextSeq EXCEPT ![TargetNode] = 0]
    /\ committed' = [committed EXCEPT ![TargetNode] = 0]
    /\ truncBelow' = [truncBelow EXCEPT ![TargetNode] = 0]
    /\ admissionSafe' = admissionSafe /\ AllAckedOn(TargetNode)
    /\ UNCHANGED
          <<raftLog, applied, raftLeader, raftVoters, alive, epoch,
            raftConnected, activated, activationPending, nextWrite,
            writeStatus, writeDoc, writeKind, writeTarget, writePrimary,
            writeEpoch, writeSeq, writeTerm, writeRequired, writeWait, pins,
            copyExists, copyAllocation, copyUuid, replicaFence,
            durableReplicaFence, messages, sharedHolders, exclusiveHolder,
            acked, failed, promotionSafe, ackMembershipSafe, ApplySafetyVars,
            termMonotonic, PeerRecoveryVars, FaultVars>>

PRNext ==
    \/ PRClientWrite
    \/ PRPrimaryAccept
    \/ PRPrimaryAck
    \/ PRStart
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
    \/ PRCrashTarget
    \/ PRRestartTarget
    \/ PRStopFaults
    \/ PRRestorePending
    \/ PRCommitFixed
    \/ PRLegacyWipeAndDelayedAdmission
    \/ L1ObserveAdmission
    \/ L1DeliverTargetView
    \/ L1TargetAdmitted
    \/ L1TargetRejected

PendingMarkerBlocksNewRun ==
    RestorePendingOnRestart =>
        /\ PendingMarkerMatchesCopy(TargetNode) =>
              copyMode[TargetNode] # "Recovering"
        /\ PendingMarkerMatchesCopy(TargetNode) =>
              sessionPhase[TargetNode] \notin PreFinalizePhases

PendingRestartSpec ==
    /\ PendingRestartInit
    /\ [][PRNext]_vars
    /\ WF_vars(PRClientWrite)
    /\ WF_vars(PRPrimaryAccept)
    /\ WF_vars(PRPrimaryAck)
    /\ WF_vars(PRStart)
    /\ WF_vars(L1Snapshot)
    /\ WF_vars(L1BeginInstall)
    /\ WF_vars(L1Install)
    /\ WF_vars(L1FinishCatchUp)
    /\ WF_vars(L1BeginPrepare)
    /\ WF_vars(L1AcquireBarrier)
    /\ WF_vars(L1TargetComplete)
    /\ WF_vars(L1BeginSettlement)
    /\ WF_vars(L1ProposeMark)
    /\ WF_vars(PRCrashTarget)
    /\ WF_vars(PRRestartTarget)
    /\ WF_vars(PRStopFaults)
    /\ WF_vars(PRRestorePending)
    /\ WF_vars(PRCommitFixed)
    /\ WF_vars(PRLegacyWipeAndDelayedAdmission)
    /\ WF_vars(L1ObserveAdmission)
    /\ WF_vars(L1DeliverTargetView)
    /\ WF_vars(L1TargetAdmitted)
    /\ WF_vars(L1TargetRejected)

=============================================================================
