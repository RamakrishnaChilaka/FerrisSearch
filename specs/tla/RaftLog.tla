------------------------------ MODULE RaftLog ------------------------------
\* Abstract Raft support shared by the shard-replication and recovery model.
\*
\* openraft itself is assumed correct.  The model keeps a totally ordered
\* committed log, but proposals may remain pending, commit in any order, or
\* commit after the proposer crashes.  Each node applies a prefix of the
\* committed log and therefore makes protocol decisions from a possibly stale
\* ClusterManager view.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes, MaxRaftEntries, MaxPendingRaft

NoNode == "NO_NODE"
NoTerm == 0

RoutingState(primaryNode, primaryTerm, replicaNodes, inSyncNodes,
             unassignedCount, memberNodes) ==
    [primary   |-> primaryNode,
     term      |-> primaryTerm,
     replicas  |-> replicaNodes,
     inSync    |-> inSyncNodes,
     unassigned|-> unassignedCount,
     members   |-> memberNodes]

\* Every command uses one record shape so TLC can enumerate pending commands
\* without record-field normalization surprises.
RaftCommand(commandKind, actorNode, targetNode, expectedPrimaryNode,
            expectedPrimaryTerm, proposedPrimaryNode, proposedReplicaNodes,
            proposedUnassignedCount) ==
    [kind             |-> commandKind,
     actor            |-> actorNode,
     target           |-> targetNode,
     expectedPrimary  |-> expectedPrimaryNode,
     expectedTerm     |-> expectedPrimaryTerm,
     newPrimary       |-> proposedPrimaryNode,
     newReplicas      |-> proposedReplicaNodes,
     newUnassigned    |-> proposedUnassignedCount]

RaftEntry(command, wasAccepted, resultingState) ==
    [command  |-> command,
     accepted |-> wasAccepted,
     state    |-> resultingState]

VARIABLES
    raftLog,
    pendingRaft,
    applied,
    views

RaftVars == <<raftLog, pendingRaft, applied, views>>

RaftInit(initialRouting) ==
    /\ raftLog = <<>>
    /\ pendingRaft = {}
    /\ applied = [n \in Nodes |-> 0]
    /\ views = [n \in Nodes |-> initialRouting]

CanQueueRaft ==
    /\ Len(raftLog) < MaxRaftEntries
    /\ Cardinality(pendingRaft) < MaxPendingRaft

CommittedCommands ==
    {raftLog[position].command : position \in 1..Len(raftLog)}

QueueRaft(command) ==
    /\ CanQueueRaft
    /\ command \notin pendingRaft
    /\ command \notin CommittedCommands
    /\ pendingRaft' = pendingRaft \cup {command}
    /\ UNCHANGED <<raftLog, applied, views>>

\* ClusterManager applies committed entries in order, one entry at a time.
DeliverRaftView(node) ==
    /\ node \in Nodes
    /\ applied[node] < Len(raftLog)
    /\ applied' = [applied EXCEPT ![node] = @ + 1]
    /\ views' = [views EXCEPT ![node] = raftLog[applied[node] + 1].state]
    /\ UNCHANGED <<raftLog, pendingRaft>>

RoutingType ==
    [primary    : Nodes,
     term       : Nat,
     replicas   : SUBSET Nodes,
     inSync     : SUBSET Nodes,
     unassigned : Nat,
     members    : SUBSET Nodes]

RaftTypeOK ==
    /\ raftLog \in Seq(
           [command  : [kind            : STRING,
                        actor           : Nodes \cup {NoNode},
                        target          : Nodes \cup {NoNode},
                        expectedPrimary : Nodes \cup {NoNode},
                        expectedTerm    : Nat,
                        newPrimary      : Nodes \cup {NoNode},
                        newReplicas     : SUBSET Nodes,
                        newUnassigned   : Nat],
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
            newUnassigned   : Nat]
    /\ applied \in [Nodes -> Nat]
    /\ views \in [Nodes -> RoutingType]
    /\ \A n \in Nodes : applied[n] <= Len(raftLog)

\* Safety configurations use this symmetry set.  Liveness configurations do
\* not declare SYMMETRY because symmetry reduction is unsound for liveness.
NodeSymmetry == Permutations(Nodes)

=============================================================================
