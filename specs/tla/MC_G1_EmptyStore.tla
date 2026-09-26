-------------------------- MODULE MC_G1_EmptyStore --------------------------
\* CreateIndex/first-activation slice for the G1 empty-store rule.  The initial
\* primary creates its permitted empty copy, loses that shard disk before first
\* activation, recreates the same initial allocation, activates, and then
\* acknowledges a write.  No write can be acknowledged before initialized.

EXTENDS Invariants

CONSTANTS PrimaryNode, MetadataLeader

G1Init ==
    /\ Init
    /\ PrimaryNode # MetadataLeader
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ ~routing.initialized
    /\ ~copyExists[PrimaryNode]

G1OpenInitialCopy ==
    /\ OpenAssignedEmptyCopy(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

G1CrashBeforeActivation ==
    /\ crashCount = 0
    /\ copyExists[PrimaryNode]
    /\ ~routing.initialized
    /\ Crash(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

G1LoseInitialCopy ==
    /\ DiskLoss(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

G1RestartPrimary ==
    /\ Restart(PrimaryNode)
    /\ UNCHANGED ApplySafetyVars

G1ProposeFirstActivation ==
    /\ diskLost[PrimaryNode]
    /\ epoch[PrimaryNode] = 1
    /\ ProposeActivate(PrimaryNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G1Commit ==
    /\ \E command \in pendingRaft : CommitRaft(command)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

G1DeliverPrimaryView ==
    /\ DeliverView(PrimaryNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

G1ObserveFirstActivation ==
    /\ ObserveActivation(PrimaryNode)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>

G1ClientWrite ==
    /\ routing.initialized
    /\ activated[PrimaryNode] = views[PrimaryNode].term
    /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G1PrimaryAccept ==
    /\ PrimaryAccept(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G1PrimaryAck ==
    /\ PrimaryAck(1)
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars>>

G1Next ==
    \/ G1OpenInitialCopy
    \/ G1CrashBeforeActivation
    \/ G1LoseInitialCopy
    \/ G1RestartPrimary
    \/ G1ProposeFirstActivation
    \/ G1Commit
    \/ G1DeliverPrimaryView
    \/ G1ObserveFirstActivation
    \/ G1ClientWrite
    \/ G1PrimaryAccept
    \/ G1PrimaryAck

PreActivationDiskLossHarmless ==
    diskLost[PrimaryNode]
    ~> /\ routing.initialized
       /\ 1 \in acked
       /\ 1 \in ops[PrimaryNode]

G1Spec ==
    /\ G1Init
    /\ [][G1Next]_vars
    /\ WF_vars(G1OpenInitialCopy)
    /\ WF_vars(G1CrashBeforeActivation)
    /\ WF_vars(G1LoseInitialCopy)
    /\ WF_vars(G1RestartPrimary)
    /\ WF_vars(G1ProposeFirstActivation)
    /\ WF_vars(G1Commit)
    /\ WF_vars(G1DeliverPrimaryView)
    /\ WF_vars(G1ObserveFirstActivation)
    /\ WF_vars(G1ClientWrite)
    /\ WF_vars(G1PrimaryAccept)
    /\ WF_vars(G1PrimaryAck)

=============================================================================
