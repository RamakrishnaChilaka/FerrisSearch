------------------------ MODULE MC_TwoShardIsolation ------------------------
\* Minimal B1 control-plane slice.  One index has a red shard whose unchanged
\* primary allocation is absent and a healthy shard whose primary fails.
\* ClusterStateMachine::UpdateIndex must validate each proposed shard relative
\* to its own current routing: carrying an unchanged missing primary
\* allocation is valid, while changing a primary still requires an allocated
\* in-sync candidate.  The healthy shard then receives a fresh replica
\* allocation even though its sibling remains red.

EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, RedPrimary, FailedPrimary, CandidateNode, ReplacementNode

Shards == {"red", "healthy"}
MaxAllocation == 3

ShardRouting(primaryNode, primaryTerm, replicaNodes, inSyncNodes,
             unassignedCount, allocationMap) ==
    [primary |-> primaryNode,
     term |-> primaryTerm,
     replicas |-> replicaNodes,
     inSync |-> inSyncNodes,
     unassigned |-> unassignedCount,
     allocations |-> allocationMap]

NoAllocations == [node \in Nodes |-> 0]

VARIABLES shardRouting, alive

TwoShardVars == <<shardRouting, alive>>

RedInitial ==
    ShardRouting(RedPrimary, 1, {}, {}, 1, NoAllocations)

HealthyInitial ==
    LET allocations ==
            [NoAllocations EXCEPT
                ![FailedPrimary] = 1,
                ![CandidateNode] = 1]
    IN ShardRouting(FailedPrimary, 1, {CandidateNode}, {CandidateNode}, 0,
                    allocations)

HealthyFailoverProposal ==
    LET current == shardRouting["healthy"]
        allocations ==
            [current.allocations EXCEPT ![FailedPrimary] = 0]
    IN ShardRouting(CandidateNode, current.term + 1, {}, {}, 1, allocations)

ShardWellFormed(shard) ==
    /\ shard.primary \in Nodes
    /\ shard.term \in 1..2
    /\ shard.replicas \subseteq Nodes \ {shard.primary}
    /\ shard.inSync \subseteq shard.replicas
    /\ shard.unassigned \in 0..Cardinality(Nodes)
    /\ shard.allocations \in [Nodes -> 0..MaxAllocation]
    /\ \A node \in Nodes :
           IF node = shard.primary
           THEN node \notin shard.replicas
           ELSE (node \in shard.replicas) <=> shard.allocations[node] > 0

ShardUpdateAccepted(current, proposed) ==
    /\ ShardWellFormed(proposed)
    /\ IF proposed.primary = current.primary
          THEN /\ proposed.term = current.term
               \* B1 fix: carry the current allocation exactly, including 0.
               /\ proposed.allocations[current.primary] =
                  current.allocations[current.primary]
          ELSE /\ proposed.primary \in current.inSync
               /\ current.allocations[proposed.primary] > 0
               /\ proposed.allocations[proposed.primary] =
                  current.allocations[proposed.primary]
               /\ proposed.term = current.term + 1

IndexUpdateAccepted(current, proposed) ==
    \A shard \in Shards :
        ShardUpdateAccepted(current[shard], proposed[shard])

TwoShardInit ==
    /\ Cardinality(Nodes) = 3
    /\ RedPrimary # FailedPrimary
    /\ RedPrimary # CandidateNode
    /\ FailedPrimary # CandidateNode
    /\ ReplacementNode = RedPrimary
    /\ shardRouting =
          [shard \in Shards |->
              IF shard = "red" THEN RedInitial ELSE HealthyInitial]
    /\ alive = [node \in Nodes |-> TRUE]

LoseHealthyPrimary ==
    /\ alive[FailedPrimary]
    /\ alive' = [alive EXCEPT ![FailedPrimary] = FALSE]
    /\ UNCHANGED shardRouting

PromoteHealthyShard ==
    LET proposed ==
            [shardRouting EXCEPT !["healthy"] = HealthyFailoverProposal]
    IN
    /\ ~alive[FailedPrimary]
    /\ shardRouting["healthy"].primary = FailedPrimary
    /\ IndexUpdateAccepted(shardRouting, proposed)
    /\ shardRouting' = proposed
    /\ UNCHANGED alive

AllocateHealthyReplica ==
    LET current == shardRouting["healthy"]
        allocation == 2
        allocations ==
            [current.allocations EXCEPT ![ReplacementNode] = allocation]
        proposed ==
            ShardRouting(current.primary, current.term,
                         current.replicas \cup {ReplacementNode},
                         current.inSync, current.unassigned - 1, allocations)
    IN
    /\ current.primary = CandidateNode
    /\ current.unassigned = 1
    /\ alive[ReplacementNode]
    /\ ReplacementNode # current.primary
    /\ ShardUpdateAccepted(current, proposed)
    /\ shardRouting' = [shardRouting EXCEPT !["healthy"] = proposed]
    /\ UNCHANGED alive

TwoShardNext ==
    \/ LoseHealthyPrimary
    \/ PromoteHealthyShard
    \/ AllocateHealthyReplica

TwoShardTypeOK ==
    /\ shardRouting \in
       [Shards ->
          [primary : Nodes,
           term : 1..2,
           replicas : SUBSET Nodes,
           inSync : SUBSET Nodes,
           unassigned : 0..Cardinality(Nodes),
           allocations : [Nodes -> 0..MaxAllocation]]]
    /\ \A shard \in Shards : ShardWellFormed(shardRouting[shard])
    /\ alive \in [Nodes -> BOOLEAN]

RedSiblingPreserved ==
    shardRouting["red"] = RedInitial

RedSiblingDoesNotBlock ==
    /\ ~alive[FailedPrimary]
    /\ shardRouting["healthy"].primary = FailedPrimary
    => LET proposed ==
               [shardRouting EXCEPT
                   !["healthy"] = HealthyFailoverProposal]
       IN IndexUpdateAccepted(shardRouting, proposed)

HealthyFailoverCompletes ==
    ~alive[FailedPrimary]
    ~> shardRouting["healthy"].primary = CandidateNode

HealthyAllocationCompletes ==
    (shardRouting["healthy"].primary = CandidateNode)
    ~> /\ ReplacementNode \in shardRouting["healthy"].replicas
       /\ shardRouting["healthy"].allocations[ReplacementNode] = 2

TwoShardSpec ==
    /\ TwoShardInit
    /\ [][TwoShardNext]_TwoShardVars
    /\ WF_TwoShardVars(LoseHealthyPrimary)
    /\ WF_TwoShardVars(PromoteHealthyShard)
    /\ WF_TwoShardVars(AllocateHealthyReplica)

=============================================================================
