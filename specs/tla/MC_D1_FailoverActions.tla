----------------------- MODULE MC_D1_FailoverActions -----------------------
\* Small end-to-end action coverage for the D1 failover gates added for trace
\* validation. A three-copy shard creates an unacknowledged gap on promotion
\* candidate Q and an unacknowledged term/sequence identity on R. After P
\* crashes, Q is promoted, durably fences, fills the gap with a replayable
\* NoOp, and activates. The NoOp is applied and redelivered to R before Q
\* reuses R's old sequence. R must fail closed on the later write collision.

EXTENDS MC_D1_SeqNoApply

CONSTANTS P, Q, R, X, Y

VARIABLE phase

failoverVars == <<d1vars, phase>>

ReplicateFor(writeId, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "Replicate"
        /\ message.write = writeId
        /\ message.to = replica

AckForWrite(writeId, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "ReplicaAck"
        /\ message.write = writeId
        /\ message.from = replica

NoOpFor(sequenceNumber, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "ReplicateNoOp"
        /\ message.write = NoWrite
        /\ message.seq = sequenceNumber
        /\ message.to = replica

NoOpAckMessageFor(sequenceNumber, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "NoOpAck"
        /\ message.write = NoWrite
        /\ message.seq = sequenceNumber
        /\ message.from = replica

Stable(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, replicaFence, durableReplicaFence,
            ApplySafetyVars, PeerRecoveryVars, FaultVars, D1Vars>>

FenceChanging(action) ==
    /\ action
    /\ UNCHANGED
          <<copyAllocation, copyUuid, PeerRecoveryVars, FaultVars, D1Vars>>

FaultAction(action) ==
    /\ action
    /\ UNCHANGED <<ApplySafetyVars, D1Vars>>

Advance(from, to, action) ==
    /\ phase = from
    /\ action
    /\ phase' = to

FailoverInit ==
    /\ Init
    /\ D1DataInit
    /\ Nodes = {P, Q, R}
    /\ P # Q
    /\ P # R
    /\ Q # R
    /\ X \in Docs
    /\ Y \in Docs
    /\ X # Y
    /\ routing.primary = P
    /\ routing.inSync = {Q, R}
    /\ raftLeader = P
    /\ phase = 0

Submit1 ==
    /\ Advance(0, 1, D1ClientWriteFrom(P, X, "Put"))

Accept1 ==
    /\ Advance(1, 2, D1PrimaryAccept(1))

Apply1Q ==
    /\ Advance(2, 3, D1FixedReplicaProcess(ReplicateFor(1, Q)))

Apply1R ==
    /\ Advance(3, 4, D1FixedReplicaProcess(ReplicateFor(1, R)))

Ack1Q ==
    /\ Advance(4, 5, D1DeliverAck(AckForWrite(1, Q)))

Ack1R ==
    /\ Advance(5, 6, D1DeliverAck(AckForWrite(1, R)))

Finish1 ==
    /\ Advance(6, 7, D1PrimaryAck(1))

Submit2 ==
    /\ Advance(7, 8, D1ClientWriteFrom(P, Y, "Put"))

Accept2 ==
    /\ Advance(8, 9, D1PrimaryAccept(2))

Drop2Q ==
    /\ Advance(9, 10, FaultAction(LoseMsg(ReplicateFor(2, Q))))

Drop2R ==
    /\ Advance(10, 11, FaultAction(LoseMsg(ReplicateFor(2, R))))

Fail2 ==
    /\ Advance(11, 12, Stable(PrimaryFail(2)))

Submit3 ==
    /\ Advance(12, 13, D1ClientWriteFrom(P, X, "Put"))

Accept3 ==
    /\ Advance(13, 14, D1PrimaryAccept(3))

Apply3Q ==
    /\ Advance(14, 15, D1FixedReplicaProcess(ReplicateFor(3, Q)))

Apply3R ==
    /\ Advance(15, 16, D1FixedReplicaProcess(ReplicateFor(3, R)))

Ack3Q ==
    /\ Advance(16, 17, D1DeliverAck(AckForWrite(3, Q)))

Ack3R ==
    /\ Advance(17, 18, D1DeliverAck(AckForWrite(3, R)))

Finish3 ==
    /\ Advance(18, 19, D1PrimaryAck(3))

Submit4 ==
    /\ Advance(19, 20, D1ClientWriteFrom(P, Y, "Put"))

Accept4 ==
    /\ Advance(20, 21, D1PrimaryAccept(4))

Apply4R ==
    /\ Advance(21, 22, D1FixedReplicaProcess(ReplicateFor(4, R)))

Ack4R ==
    /\ Advance(22, 23, D1DeliverAck(AckForWrite(4, R)))

Drop4Q ==
    /\ Advance(23, 24, FaultAction(LoseMsg(ReplicateFor(4, Q))))

Fail4 ==
    /\ Advance(24, 25, Stable(PrimaryFail(4)))

CrashP ==
    /\ Advance(25, 26, D1CrashCopy(P))

ElectQ ==
    /\ Advance(26, 27, FaultAction(ElectLeader(Q)))

ProposePromotion ==
    /\ Advance(27, 28, FaultAction(SuspectAndRemove(Q, P, Q)))

CommitPromotion ==
    /\ phase = 28
    /\ \E command \in pendingRaft :
           /\ command.kind = "UpdateRouting"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 29

ProposeActivation ==
    /\ Advance(29, 30, Stable(ProposeActivate(Q)))

CommitActivation ==
    /\ phase = 30
    /\ \E command \in pendingRaft :
           /\ command.kind = "ActivatePrimary"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 31

FenceQ ==
    /\ Advance(31, 32, D1ObserveFence(Q, 3, 3))

AppendQGap ==
    /\ Advance(32, 33, D1AppendPromotionNoOp(Q, 1, 3))

ProcessQGap ==
    /\ Advance(33, 34, D1ProcessPromotionNoOp(Q, 1, 3))

ObserveQGapFill ==
    /\ Advance(34, 35, D1ObservePromotionNoOpFill(Q, {1}, 3))

ActivateQ ==
    /\ Advance(35, 36, FenceChanging(D1ObserveActivation(Q)))

SendNoOpR ==
    /\ Advance(36, 37, D1RedeliverPromotionNoOp(Q, R, 1))

FenceRForNoOp ==
    /\ Advance(37, 38, D1ObserveFence(R, 3, 4))

ApplyNoOpR ==
    /\ Advance(38, 39, D1FixedReplicaNoOpProcess(NoOpFor(1, R)))

AckNoOpR ==
    /\ Advance(39, 40, D1DeliverNoOpAck(NoOpAckMessageFor(1, R)))

RedeliverNoOpR ==
    /\ Advance(40, 41, D1RedeliverPromotionNoOp(Q, R, 1))

ProcessNoOpRedeliveryR ==
    /\ Advance(41, 42, D1FixedReplicaNoOpRedelivery(NoOpFor(1, R)))

AckNoOpRedeliveryR ==
    /\ Advance(42, 43, D1DeliverNoOpAck(NoOpAckMessageFor(1, R)))

Submit5 ==
    /\ Advance(43, 44, D1ClientWriteFrom(Q, Y, "Put"))

Accept5 ==
    /\ Advance(44, 45, D1PrimaryAccept(5))

FenceR ==
    /\ Advance(45, 46, D1ObserveFence(R, 3, 4))

CollideR ==
    /\ Advance(46, 47, D1FixedReplicaCollision(ReplicateFor(5, R)))

FailoverNext ==
    \/ Submit1
    \/ Accept1
    \/ Apply1Q
    \/ Apply1R
    \/ Ack1Q
    \/ Ack1R
    \/ Finish1
    \/ Submit2
    \/ Accept2
    \/ Drop2Q
    \/ Drop2R
    \/ Fail2
    \/ Submit3
    \/ Accept3
    \/ Apply3Q
    \/ Apply3R
    \/ Ack3Q
    \/ Ack3R
    \/ Finish3
    \/ Submit4
    \/ Accept4
    \/ Apply4R
    \/ Ack4R
    \/ Drop4Q
    \/ Fail4
    \/ CrashP
    \/ ElectQ
    \/ ProposePromotion
    \/ CommitPromotion
    \/ ProposeActivation
    \/ CommitActivation
    \/ FenceQ
    \/ AppendQGap
    \/ ProcessQGap
    \/ ObserveQGapFill
    \/ ActivateQ
    \/ SendNoOpR
    \/ FenceRForNoOp
    \/ ApplyNoOpR
    \/ AckNoOpR
    \/ RedeliverNoOpR
    \/ ProcessNoOpRedeliveryR
    \/ AckNoOpRedeliveryR
    \/ Submit5
    \/ Accept5
    \/ FenceR
    \/ CollideR

FailoverTypeOK ==
    /\ D1TypeOK
    /\ phase \in 0..47

FailoverProgressEnabled ==
    \/ phase = 47
    \/ ENABLED FailoverNext

FailoverActionsCovered ==
    phase = 47 =>
        /\ routing.primary = Q
        /\ routing.term = 3
        /\ activated[Q] = 3
        /\ D1PromotionGaps(Q) = {}
        /\ noopTerm[Q][1] = 3
        /\ noopTerm[R][1] = 3
        /\ 1 \in processedSeqs[R]
        /\ Cardinality(
              {position \in 1..Len(walOrder[R]) :
                  WalEntrySeq(walOrder[R][position]) = 1}) = 1
        /\ D1TermCollision(R, 3, 3)
        /\ copyMode[R] = "ApplyFailed"

FailoverSpec ==
    FailoverInit /\ [][FailoverNext]_failoverVars

=============================================================================
