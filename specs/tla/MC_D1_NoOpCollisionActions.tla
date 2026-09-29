-------------------- MODULE MC_D1_NoOpCollisionActions ---------------------
\* Scripted action coverage for promotion-NoOp identity collision and exact
\* failed-copy removal. P leaves seq0 only on R and seq1 only on Q. Promotion
\* fills Q's seq0 gap, but R already owns seq0 from the older term, so R must
\* reject the NoOp, fail closed, and leave the in-sync set through Raft.

EXTENDS MC_D1_SeqNoApply

CONSTANTS P, Q, R, X, Y

VARIABLE phase

collisionVars == <<d1vars, phase>>

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

NoOpNackMessageFor(sequenceNumber, replica) ==
    CHOOSE message \in messages :
        /\ message.kind = "NoOpNack"
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

CollisionInit ==
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

Apply1R ==
    /\ Advance(2, 3, D1FixedReplicaProcess(ReplicateFor(1, R)))

Ack1R ==
    /\ Advance(3, 4, D1DeliverAck(AckForWrite(1, R)))

Drop1Q ==
    /\ Advance(4, 5, FaultAction(LoseMsg(ReplicateFor(1, Q))))

Fail1 ==
    /\ Advance(5, 6, Stable(PrimaryFail(1)))

Submit2 ==
    /\ Advance(6, 7, D1ClientWriteFrom(P, Y, "Put"))

Accept2 ==
    /\ Advance(7, 8, D1PrimaryAccept(2))

Apply2Q ==
    /\ Advance(8, 9, D1FixedReplicaProcess(ReplicateFor(2, Q)))

Ack2Q ==
    /\ Advance(9, 10, D1DeliverAck(AckForWrite(2, Q)))

Drop2R ==
    /\ Advance(10, 11, FaultAction(LoseMsg(ReplicateFor(2, R))))

Fail2 ==
    /\ Advance(11, 12, Stable(PrimaryFail(2)))

CrashP ==
    /\ Advance(12, 13, D1CrashCopy(P))

ElectQ ==
    /\ Advance(13, 14, FaultAction(ElectLeader(Q)))

ProposePromotion ==
    /\ Advance(14, 15, FaultAction(SuspectAndRemove(Q, P, Q)))

CommitPromotion ==
    /\ phase = 15
    /\ \E command \in pendingRaft :
           /\ command.kind = "UpdateRouting"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 16

ProposeActivation ==
    /\ Advance(16, 17, Stable(ProposeActivate(Q)))

CommitActivation ==
    /\ phase = 17
    /\ \E command \in pendingRaft :
           /\ command.kind = "ActivatePrimary"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 18

FenceQ ==
    /\ Advance(18, 19, D1ObserveFence(Q, 3, 2))

FillQGap ==
    /\ Advance(19, 20, D1FillPromotionNoOps(Q, {0}))

ActivateQ ==
    /\ Advance(20, 21, FenceChanging(D1ObserveActivation(Q)))

FenceR ==
    /\ Advance(21, 22, D1ObserveFence(R, 3, 1))

CollideNoOpR ==
    /\ Advance(22, 23, D1FixedReplicaNoOpCollision(NoOpFor(0, R)))

IgnoreNoOpNack ==
    /\ Advance(23, 24, D1DeliverNoOpNack(NoOpNackMessageFor(0, R)))

ReportR ==
    /\ Advance(24, 25, FaultAction(ReportShardCopyFailure(R, NoNode)))

CommitRemoval ==
    /\ phase = 25
    /\ \E command \in pendingRaft :
           /\ command.kind = "FailShardCopy"
           /\ FenceChanging(CommitRaft(command))
    /\ phase' = 26

CollisionNext ==
    \/ Submit1
    \/ Accept1
    \/ Apply1R
    \/ Ack1R
    \/ Drop1Q
    \/ Fail1
    \/ Submit2
    \/ Accept2
    \/ Apply2Q
    \/ Ack2Q
    \/ Drop2R
    \/ Fail2
    \/ CrashP
    \/ ElectQ
    \/ ProposePromotion
    \/ CommitPromotion
    \/ ProposeActivation
    \/ CommitActivation
    \/ FenceQ
    \/ FillQGap
    \/ ActivateQ
    \/ FenceR
    \/ CollideNoOpR
    \/ IgnoreNoOpNack
    \/ ReportR
    \/ CommitRemoval

CollisionTypeOK ==
    /\ D1TypeOK
    /\ phase \in 0..26

CollisionProgressEnabled ==
    \/ phase = 26
    \/ ENABLED CollisionNext

NoOpCollisionActionsCovered ==
    phase = 26 =>
        /\ routing.primary = Q
        /\ routing.term = 3
        /\ activated[Q] = 3
        /\ noopTerm[Q][0] = 3
        /\ processedTerm[R][0] = 1
        /\ copyMode[R] = "ApplyFailed"
        /\ R \notin routing.inSync
        /\ R \notin routing.replicas
        /\ routing.allocations[R] = 0
        /\ messages = {}

CollisionSpec ==
    CollisionInit /\ [][CollisionNext]_collisionVars

=============================================================================
