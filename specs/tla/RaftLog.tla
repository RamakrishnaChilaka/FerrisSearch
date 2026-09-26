------------------------------ MODULE RaftLog ------------------------------
\* Abstract Raft support shared by the shard-replication and recovery model.
\*
\* openraft itself is assumed correct.  The model keeps a totally ordered
\* committed log and an explicit voter/leader configuration.  A command may
\* remain pending, be appended after later invocations, or commit after its
\* proposer crashes, but committing requires a live connected leader and a
\* live majority of the current voters.  The leader applies its committed
\* command before the synchronous write returns; followers may retain lagging
\* ClusterManager views.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes, MaxRaftEntries, MaxPendingRaft

NoNode == "NO_NODE"
NoTerm == 0

RoutingState(primaryNode, primaryTerm, replicaNodes, inSyncNodes,
             unassignedCount, memberNodes, allocationMap) ==
    [primary   |-> primaryNode,
     term      |-> primaryTerm,
     replicas  |-> replicaNodes,
     inSync    |-> inSyncNodes,
     unassigned|-> unassignedCount,
     members   |-> memberNodes,
     allocations |-> allocationMap]

EmptyAllocations == [node \in Nodes |-> 0]

\* Every command uses one record shape so TLC can enumerate pending commands
\* without record-field normalization surprises.
RaftCommand(commandKind, actorNode, targetNode, expectedPrimaryNode,
            expectedPrimaryTerm, proposedPrimaryNode, proposedReplicaNodes,
            proposedUnassignedCount, expectedAllocationId,
            proposedAllocations) ==
    [kind             |-> commandKind,
     actor            |-> actorNode,
     target           |-> targetNode,
     expectedPrimary  |-> expectedPrimaryNode,
     expectedTerm     |-> expectedPrimaryTerm,
     newPrimary       |-> proposedPrimaryNode,
     newReplicas      |-> proposedReplicaNodes,
     newUnassigned    |-> proposedUnassignedCount,
     expectedAllocation |-> expectedAllocationId,
     newAllocations   |-> proposedAllocations]

RaftEntry(command, wasAccepted, resultingState) ==
    [command  |-> command,
     accepted |-> wasAccepted,
     state    |-> resultingState]

VARIABLES
    raftLog,
    pendingRaft,
    applied,
    views,
    raftLeader,
    raftVoters

RaftVars == <<raftLog, pendingRaft, applied, views, raftLeader, raftVoters>>

RaftInit(initialRouting, initialLeader) ==
    /\ raftLog = <<>>
    /\ pendingRaft = {}
    /\ applied = [n \in Nodes |-> 0]
    /\ views = [n \in Nodes |-> initialRouting]
    /\ raftLeader = initialLeader
    /\ raftVoters = Nodes

CanQueueRaft ==
    /\ Len(raftLog) < MaxRaftEntries
    /\ Cardinality(pendingRaft) < MaxPendingRaft

QueueRaft(command) ==
    /\ CanQueueRaft
    /\ command \notin pendingRaft
    /\ pendingRaft' = pendingRaft \cup {command}
    /\ UNCHANGED <<raftLog, applied, views, raftLeader, raftVoters>>

\* ClusterManager applies committed entries in order, one entry at a time.
DeliverRaftView(node) ==
    /\ node \in Nodes
    /\ applied[node] < Len(raftLog)
    /\ applied' = [applied EXCEPT ![node] = @ + 1]
    /\ views' = [views EXCEPT ![node] = raftLog[applied[node] + 1].state]
    /\ UNCHANGED <<raftLog, pendingRaft, raftLeader, raftVoters>>

RoutingType ==
    [primary    : Nodes,
     term       : Nat,
     replicas   : SUBSET Nodes,
     inSync     : SUBSET Nodes,
     unassigned : Nat,
     members    : SUBSET Nodes,
     allocations: [Nodes -> Nat]]

RaftTypeOK ==
    /\ raftLog \in Seq(
           [command  : [kind            : STRING,
                        actor           : Nodes \cup {NoNode},
                        target          : Nodes \cup {NoNode},
                        expectedPrimary : Nodes \cup {NoNode},
                        expectedTerm    : Nat,
                        newPrimary      : Nodes \cup {NoNode},
                        newReplicas     : SUBSET Nodes,
                        newUnassigned   : Nat,
                        expectedAllocation : Nat,
                        newAllocations  : [Nodes -> Nat]],
            accepted : BOOLEAN,
            state    : RoutingType])
    /\ pendingRaft \subseteq
           [kind            : STRING,
            actor           : Nodes \cup {NoNode},
            target          : Nodes \cup {NoNode},
            expectedPrimary : Nodes \cup {NoNode},
            expectedTerm    : Nat,
            newPrimary      : Nodes \cup {NoNode},
            newReplicas     : SUBSET Nodes,
            newUnassigned   : Nat,
            expectedAllocation : Nat,
            newAllocations  : [Nodes -> Nat]]
    /\ applied \in [Nodes -> Nat]
    /\ views \in [Nodes -> RoutingType]
    /\ raftLeader \in Nodes \cup {NoNode}
    /\ raftVoters \subseteq Nodes
    /\ raftVoters # {}
    /\ \A n \in Nodes : applied[n] <= Len(raftLog)

\* Safety configurations use this symmetry set.  Liveness configurations do
\* not declare SYMMETRY because symmetry reduction is unsound for liveness.
NodeSymmetry == Permutations(Nodes)

=============================================================================
