------------------------- MODULE MC_G2_CopyFailure --------------------------
\* Bounded safety slice for an authoritative local copy that becomes
\* unopenable.  The node reports its target-observed allocation ID, the state
\* machine conditionally removes that exact copy, promotion preserves the
\* acknowledged history, and allocation/recovery may create a fresh replica.

EXTENDS Invariants

CONSTANTS PrimaryNode, LostNode, MetadataLeader

G2SafetyInit ==
    /\ Init
    /\ routing.initialized
    /\ routing.primary = PrimaryNode
    /\ raftLeader = MetadataLeader
    /\ MetadataLeader # LostNode
    /\ \/ LostNode = PrimaryNode
       \/ LostNode \in routing.inSync

G2ReplicationNext ==
    /\ ReplicationNext
    /\ UNCHANGED <<PeerRecoveryVars, FaultVars>>

G2RecoveryNext ==
    /\ PeerRecoveryNext
    /\ UNCHANGED FaultVars

G2Crash ==
    /\ acked # {}
    /\ Crash(LostNode)

G2DiskLoss ==
    DiskLoss(LostNode)

G2Restart ==
    Restart(LostNode)

G2EmptyOpenProbe ==
    OpenAssignedEmptyCopy(LostNode)

G2ReportFailure ==
    ReportShardCopyFailure(LostNode)

G2AllocateReplacement ==
    AllocateAfterLifecycle(MetadataLeader, LostNode)

G2ObserveAllocation ==
    \/ ObserveAllocationAccepted(LostNode)
    \/ ObserveAllocationRejected(LostNode)

G2FaultNext ==
    /\ \/ G2Crash
       \/ G2DiskLoss
       \/ G2Restart
       \/ G2EmptyOpenProbe
       \/ G2ReportFailure
       \/ G2AllocateReplacement
       \/ G2ObserveAllocation
    /\ UNCHANGED ApplySafetyVars

G2SafetyNext ==
    \/ G2ReplicationNext
    \/ G2RecoveryNext
    \/ G2FaultNext

\* Once an initialized allocation has lost its disk, the same allocation may
\* not reappear as an empty local copy.  Recovery uses a fresh allocation ID.
NoLostAllocationReopenedEmpty ==
    \A node \in Nodes :
        /\ diskLost[node]
        /\ routing.initialized
        /\ routing.allocations[node] = 1
        => ~copyExists[node]

FailedPrimaryPromotesOrStaysRed ==
    /\ diskLost[PrimaryNode]
    /\ routing.allocations[PrimaryNode] = 0
    => \/ routing.primary # PrimaryNode
       \/ /\ routing.primary = PrimaryNode
          /\ routing.inSync = {}

=============================================================================
