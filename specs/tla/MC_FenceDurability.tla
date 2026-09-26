------------------------- MODULE MC_FenceDurability -------------------------
\* Targeted durability check for the proposed replica-side primary-term fence.
\* A lagging replica first accepts a new-primary operation, then crashes and
\* restarts before its Raft view advances.  A stale primary subsequently sends
\* a lower-term operation.  Only a durable local fence can reject that apply.

EXTENDS MC_C2

CONSTANT ReplicaNode

FenceDurabilityInit ==
    /\ C2Init
    /\ ReplicaNode \in routing.inSync
    /\ ReplicaNode # PrimaryNode
    /\ ReplicaNode # MetadataLeader

FenceClientWrite ==
    /\ nextWrite = 1
    /\ routing.primary = MetadataLeader
    /\ activated[MetadataLeader] = routing.term
    /\ ClientWrite(MetadataLeader, DefaultDoc, "Put")

\* TransportClient::replicate_to_shard retries an old-primary operation after
\* the replica restarts.  The request carries the old primary's term, UUID,
\* and target allocation but is isolated from the promoted primary's NACK so
\* this configuration tests the replica's local durable fence directly.
SendStaleReplicaProbe ==
    LET writeId == 2
        sequenceNumber == nextSeq[PrimaryNode]
        request ==
            Message("Replicate", writeId, PrimaryNode, ReplicaNode,
                    sequenceNumber, epoch[PrimaryNode], epoch[ReplicaNode],
                    views[PrimaryNode].term, IndexUuid,
                    views[PrimaryNode].allocations[ReplicaNode])
    IN
    /\ nextWrite = writeId
    /\ writeStatus[1] = "Acked"
    /\ writeStatus[writeId] = "Unused"
    /\ epoch[ReplicaNode] = 1
    /\ alive[PrimaryNode]
    /\ alive[ReplicaNode]
    /\ ~raftConnected[PrimaryNode]
    /\ nextSeq[PrimaryNode] < MaxWrites
    /\ nextWrite' = nextWrite + 1
    /\ writeStatus' = [writeStatus EXCEPT ![writeId] = "Replicating"]
    /\ writeTarget' = [writeTarget EXCEPT ![writeId] = PrimaryNode]
    /\ writePrimary' = [writePrimary EXCEPT ![writeId] = PrimaryNode]
    /\ writeEpoch' = [writeEpoch EXCEPT ![writeId] = epoch[PrimaryNode]]
    /\ writeSeq' = [writeSeq EXCEPT ![writeId] = sequenceNumber]
    /\ writeTerm' =
          [writeTerm EXCEPT ![writeId] = views[PrimaryNode].term]
    /\ writeRequired' = [writeRequired EXCEPT ![writeId] = {ReplicaNode}]
    /\ writeWait' = [writeWait EXCEPT ![writeId] = {ReplicaNode}]
    /\ ops' = [ops EXCEPT ![PrimaryNode] = @ \cup {writeId}]
    /\ durableOps' =
          [durableOps EXCEPT ![PrimaryNode] = @ \cup {writeId}]
    /\ docValue' =
          [docValue EXCEPT ![PrimaryNode][writeDoc[writeId]] = writeId]
    /\ nextSeq' = [nextSeq EXCEPT ![PrimaryNode] = @ + 1]
    /\ messages' = messages \cup {request}
    /\ sharedHolders' =
          [sharedHolders EXCEPT ![PrimaryNode] = @ \cup {writeId}]
    /\ UNCHANGED
          <<RaftVars, routing, alive, epoch, raftConnected, activated,
            activationPending, writeDoc, writeKind, committed, truncBelow,
            pins, copyExists, copyMode, installMarker, exclusiveHolder,
            acked, failed, promotionSafe, admissionSafe, ackMembershipSafe,
            staleApplySafe, termMonotonic>>

FenceStableNext ==
    \/ FenceClientWrite
    \/ SendStaleReplicaProbe
    \/ \E writeId \in WriteIds : PrimaryAccept(writeId)
    \/ \E writeId \in WriteIds : PrimaryReject(writeId)
    \/ \E message \in messages : ReplicaReject(message)
    \/ \E message \in messages : DeliverReplicaAck(message)
    \/ \E message \in messages : DeliverReplicaNack(message)
    \/ \E writeId \in WriteIds : PrimaryAck(writeId)
    \/ \E writeId \in WriteIds : PrimaryFail(writeId)
    \/ ProposeActivate(MetadataLeader)

FenceChangingNext ==
    \/ \E message \in messages : ReplicaApply(message)
    \/ ObserveActivation(MetadataLeader)
    \/ \E command \in pendingRaft : CommitRaft(command)

FenceCrash ==
    /\ writeStatus[1] = "Acked"
    /\ replicaFence[ReplicaNode] = routing.term
    /\ Crash(ReplicaNode)
    /\ UNCHANGED staleApplySafe

FenceRestart ==
    /\ Restart(ReplicaNode)
    /\ UNCHANGED staleApplySafe

FencePartition ==
    /\ PartitionMetadata(PrimaryNode)
    /\ UNCHANGED staleApplySafe

FenceSuspect ==
    /\ SuspectAndRemove(MetadataLeader, PrimaryNode, MetadataLeader)
    /\ UNCHANGED staleApplySafe

FenceDurabilityNext ==
    \/ /\ FenceStableNext
       /\ UNCHANGED
             <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
               staleApplySafe, PeerRecoveryVars, FaultVars>>
    \/ /\ FenceChangingNext
       /\ UNCHANGED
             <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars>>
    \/ FencePartition
    \/ FenceSuspect
    \/ FenceCrash
    \/ FenceRestart

FenceRejectsStaleProbe ==
    \A message \in messages :
        /\ message.kind = "Replicate"
        /\ message.write = 2
        /\ message.to = ReplicaNode
        /\ message.term < routing.term
        => ~ReplicaMessageValid(message)

=============================================================================
