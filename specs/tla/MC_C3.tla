------------------------------- MODULE MC_C3 -------------------------------
\* Canonical disk-loss schedule.  A request-durable write is acknowledged on
\* every in-sync copy, one replica crashes and loses its shard disk, then the
\* same process identity restarts and attempts to open an empty replacement.

EXTENDS Invariants

CONSTANTS PrimaryNode, LostNode, MetadataLeader

C3Init ==
    /\ Init
    /\ PrimaryNode # LostNode
    /\ routing.primary = PrimaryNode
    /\ LostNode \in routing.inSync
    /\ raftLeader = MetadataLeader

C3StableNext ==
    \/ /\ nextWrite = 1
       /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E message \in messages : ReplicaReject(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E message \in messages : DeliverReplicaNack(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)

C3Next ==
    \/ /\ C3StableNext
       /\ UNCHANGED
             <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
               ApplySafetyVars, PeerRecoveryVars, FaultVars>>
    \/ /\ \E message \in messages : ReplicaApply(message)
       /\ UNCHANGED
             <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>
    \/ /\ writeStatus[1] = "Acked"
       /\ Crash(LostNode)
       /\ UNCHANGED ApplySafetyVars
    \/ /\ DiskLoss(LostNode)
       /\ UNCHANGED ApplySafetyVars
    \/ /\ Restart(LostNode)
       /\ UNCHANGED ApplySafetyVars
    \/ /\ OpenAssignedEmptyCopy(LostNode)
       /\ UNCHANGED ApplySafetyVars

=============================================================================
