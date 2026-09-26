------------------------------- MODULE MC_C2 -------------------------------
\* Canonical role selection for the partition counterexample.  Nodes are
\* otherwise symmetric, but fixing the old shard primary and metadata leader
\* avoids exploring equivalent initial permutations.  No SYMMETRY directive is
\* used with these distinguished constants.

EXTENDS Invariants

CONSTANTS PrimaryNode, MetadataLeader

C2Init ==
    /\ Init
    /\ PrimaryNode # MetadataLeader
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader

C2ClientWrite ==
    \/ /\ nextWrite = 1
       /\ routing.primary = MetadataLeader
       /\ activated[MetadataLeader] = routing.term
       /\ ClientWrite(MetadataLeader, DefaultDoc, "Put")
    \/ /\ nextWrite = 2
       /\ writeStatus[1] = "Acked"
       /\ ~raftConnected[PrimaryNode]
       /\ ClientWrite(PrimaryNode, DefaultDoc, "Put")

\* This wrapper removes lifecycle/recovery branches irrelevant to the known
\* stale-primary defect while preserving every data-plane delivery ordering.
C2ReplicationNext ==
    \/ C2ClientWrite
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E message \in messages : ReplicaApply(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)
    \/ ProposeActivate(MetadataLeader)
    \/ ObserveActivation(MetadataLeader)
    \/ \E command \in pendingRaft : CommitRaft(command)

C2Next ==
    \/ /\ C2ReplicationNext
       /\ UNCHANGED <<copyAllocation, PeerRecoveryVars, FaultVars>>
    \/ PartitionMetadata(PrimaryNode)
    \/ SuspectAndRemove(MetadataLeader, PrimaryNode, MetadataLeader)

=============================================================================
