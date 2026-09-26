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

C3ReplicationNext ==
    \/ /\ nextWrite = 1
       /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E message \in messages : ReplicaApply(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)

C3Next ==
    \/ /\ C3ReplicationNext
       /\ UNCHANGED <<copyAllocation, PeerRecoveryVars, FaultVars>>
    \/ /\ writeStatus[1] = "Acked"
       /\ Crash(LostNode)
    \/ DiskLoss(LostNode)
    \/ Restart(LostNode)
    \/ OpenAssignedEmptyCopy(LostNode)

=============================================================================
