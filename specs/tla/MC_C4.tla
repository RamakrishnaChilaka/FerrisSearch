------------------------------- MODULE MC_C4 -------------------------------
\* Canonical asynchronous-durability schedule.  An acknowledged write remains
\* outside the durable WAL prefix and is lost when the committed primary
\* crashes.  This documents the weaker configured durability contract.

EXTENDS Invariants

CONSTANTS PrimaryNode, MetadataLeader

C4Init ==
    /\ Init
    /\ PrimaryNode # MetadataLeader
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader

C4StableNext ==
    \/ /\ nextWrite = 1
       /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E message \in messages : ReplicaReject(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E message \in messages : DeliverReplicaNack(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)

C4Next ==
    \/ /\ C4StableNext
       /\ UNCHANGED
             <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
               ApplySafetyVars, PeerRecoveryVars, FaultVars>>
    \/ /\ \E message \in messages : ReplicaApply(message)
       /\ UNCHANGED
             <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>
    \/ /\ writeStatus[1] = "Acked"
       /\ Crash(PrimaryNode)
       /\ UNCHANGED ApplySafetyVars

=============================================================================
