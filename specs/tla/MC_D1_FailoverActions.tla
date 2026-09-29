----------------------- MODULE MC_D1_FailoverActions -----------------------
\* Small end-to-end action coverage for the D1 failover gates added for trace
\* validation. A three-copy shard creates an unacknowledged gap on promotion
\* candidate Q and an unacknowledged term/sequence identity on R. After P
\* crashes, Q is promoted, durably fences, fills the gap with a replayable
\* NoOp, activates, and reuses R's old sequence. R must fail closed on the
\* collision.

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

Apply2R ==
    /\ Advance(9, 10, D1FixedReplicaProcess(ReplicateFor(2, R)))

Ack2R ==
    /\ Advance(10, 11, D1DeliverAck(AckForWrite(2, R)))

Drop2Q ==
    /\ Advance(11, 12, FaultAction(LoseMsg(ReplicateFor(2, Q))))

Fail2 ==
    /\ Advance(12, 13, Stable(PrimaryFail(2)))

Submit3 ==
    /\ Advance(13, 14, D1ClientWriteFrom(P, X, "Put"))

Accept3 ==
    /\ Advance(14, 15, D1PrimaryAccept(3))

Apply3Q ==
    /\ Advance(15, 16, D1FixedReplicaProcess(ReplicateFor(3, Q)))

Apply3R ==
    /\ Advance(16, 17, D1FixedReplicaProcess(ReplicateFor(3, R)))

Ack3Q ==
    /\ Advance(17, 18, D1DeliverAck(AckForWrite(3, Q)))

Ack3R ==
    /\ Advance(18, 19, D1DeliverAck(AckForWrite(3, R)))

Finish3 ==
    /\ Advance(19, 20, D1PrimaryAck(3))

Submit4 ==
    /\ Advance(20, 21, D1ClientWriteFrom(P, Y, "Put"))

Accept4 ==
    /\ Advance(21, 22, D1PrimaryAccept(4))

Apply4R ==
    /\ Advance(22, 23, D1FixedReplicaProcess(ReplicateFor(4, R)))

Ack4R ==
    /\ Advance(23, 24, D1DeliverAck(AckForWrite(4, R)))

Drop4Q ==
    /\ Advance(24, 25, FaultAction(LoseMsg(ReplicateFor(4, Q))))

Fail4 ==
    /\ Advance(25, 26, Stable(PrimaryFail(4)))

CrashP ==
    /\ Advance(26, 27, D1CrashCopy(P))

ElectQ ==
    /\ Advance(27, 28, FaultAction(ElectLeader(Q)))

ProposePromotion ==
    /\ Advance(28, 29, FaultAction(SuspectAndRemove(Q, P, Q)))

CommitPromotion ==
    /\ phase = 29
    /\ \E command \in pendingRaft :
           /\ command.kind = "UpdateRouting"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 30

DeliverPromotion ==
    /\ Advance(30, 31, FenceChanging(DeliverView(Q)))

ProposeActivation ==
    /\ Advance(31, 32, Stable(ProposeActivate(Q)))

CommitActivation ==
    /\ phase = 32
    /\ \E command \in pendingRaft :
           /\ command.kind = "ActivatePrimary"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 33

DeliverActivatedView ==
    /\ Advance(33, 34, FenceChanging(DeliverView(Q)))

FenceQ ==
    /\ Advance(34, 35, D1ObserveFence(Q, 3, 3))

FillQGap ==
    /\ Advance(35, 36, D1FillPromotionNoOps(Q, {1}))

ActivateQ ==
    /\ Advance(36, 37, FenceChanging(D1ObserveActivation(Q)))

Submit5 ==
    /\ Advance(37, 38, D1ClientWriteFrom(Q, Y, "Put"))

Accept5 ==
    /\ Advance(38, 39, D1PrimaryAccept(5))

FenceR ==
    /\ Advance(39, 40, D1ObserveFence(R, 3, 4))

CollideR ==
    /\ Advance(40, 41, D1FixedReplicaCollision(ReplicateFor(5, R)))

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
    \/ Apply2R
    \/ Ack2R
    \/ Drop2Q
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
    \/ DeliverPromotion
    \/ ProposeActivation
    \/ CommitActivation
    \/ DeliverActivatedView
    \/ FenceQ
    \/ FillQGap
    \/ ActivateQ
    \/ Submit5
    \/ Accept5
    \/ FenceR
    \/ CollideR

FailoverTypeOK ==
    /\ D1TypeOK
    /\ phase \in 0..41

FailoverActionsCovered ==
    phase = 41 =>
        /\ routing.primary = Q
        /\ routing.term = 3
        /\ activated[Q] = 3
        /\ D1PromotionGaps(Q) = {}
        /\ noopTerm[Q][1] = 3
        /\ D1TermCollision(R, 3, 3)
        /\ copyMode[R] = "ApplyFailed"

FailoverSpec ==
    FailoverInit /\ [][FailoverNext]_failoverVars

=============================================================================
