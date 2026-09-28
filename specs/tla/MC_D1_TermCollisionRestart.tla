-------------------- MODULE MC_D1_TermCollisionRestart ----------------------
\* B1 restart/rebuild variant. R raises its fence to term 2 and durably stores
\* fence_max_seq_no 11 before any term-2 operation arrives. It then restarts.
\* Restoring collision state only from committed max 10 permits seq-only false
\* redelivery; restoring the identity's fence maximum fails the collision.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Replica, RestoreMode

NoOp == "NONE"
OldOp == "OLD_TERM_1_SEQ_11"
NewOp == "NEW_TERM_2_SEQ_11"

VARIABLES
    phase,
    fenceTerm,
    activeFenceMax,
    identityFenceTerm,
    identityFenceMax,
    committedMax,
    processed,
    docOp,
    newAcknowledged,
    failed,
    recovered,
    promoted

vars ==
    <<phase, fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
      committedMax, processed, docOp, newAcknowledged, failed, recovered,
      promoted>>

B1RInit ==
    /\ phase = 0
    /\ fenceTerm = 1
    /\ activeFenceMax = 10
    /\ identityFenceTerm = 1
    /\ identityFenceMax = 10
    /\ committedMax = 10
    /\ processed = {}
    /\ docOp = NoOp
    /\ newAcknowledged = FALSE
    /\ failed = FALSE
    /\ recovered = FALSE
    /\ promoted = FALSE

B1RTypeOK ==
    /\ phase \in 0..6
    /\ fenceTerm \in 1..2
    /\ activeFenceMax \in 10..11
    /\ identityFenceTerm \in 1..2
    /\ identityFenceMax \in 10..11
    /\ committedMax \in 10..11
    /\ processed \subseteq {11}
    /\ docOp \in {NoOp, OldOp, NewOp}
    /\ newAcknowledged \in BOOLEAN
    /\ failed \in BOOLEAN
    /\ recovered \in BOOLEAN
    /\ promoted \in BOOLEAN

B1RApplyOldTermWrite ==
    /\ phase = 0
    /\ processed' = {11}
    /\ docOp' = OldOp
    /\ phase' = 1
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, newAcknowledged, failed, recovered, promoted>>

\* Durable copy identity is updated before acknowledging the fence raise.
B1RRaiseFence ==
    /\ phase = 1
    /\ fenceTerm' = 2
    /\ activeFenceMax' = 11
    /\ identityFenceTerm' = 2
    /\ identityFenceMax' = 11
    /\ phase' = 2
    /\ UNCHANGED
          <<committedMax, processed, docOp, newAcknowledged, failed,
            recovered, promoted>>

\* Startup replay restores the old operation. The historical variant rebuilds
\* collision max from the last commit (10); the fixed variant loads 11 from
\* SHARD_COPY_IDENTITY before serving replication.
B1RRestart ==
    /\ phase = 2
    /\ fenceTerm' = identityFenceTerm
    /\ activeFenceMax' =
          IF RestoreMode = "CommittedOnly"
          THEN committedMax
          ELSE identityFenceMax
    /\ phase' = 3
    /\ UNCHANGED
          <<identityFenceTerm, identityFenceMax, committedMax, processed,
            docOp, newAcknowledged, failed, recovered, promoted>>

B1RCommittedOnlyNewWrite ==
    /\ RestoreMode = "CommittedOnly"
    /\ phase = 3
    /\ 11 \in processed
    /\ 11 > activeFenceMax
    /\ newAcknowledged' = TRUE
    /\ phase' = 4
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, processed, docOp, failed, recovered, promoted>>

B1RIdentityCollision ==
    /\ RestoreMode = "Identity"
    /\ phase = 3
    /\ 11 \in processed
    /\ 11 <= activeFenceMax
    /\ failed' = TRUE
    /\ newAcknowledged' = TRUE
    /\ phase' = 4
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, processed, docOp, recovered, promoted>>

B1RCommittedReady ==
    /\ RestoreMode = "CommittedOnly"
    /\ phase = 4
    /\ phase' = 5
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, processed, docOp, newAcknowledged, failed,
            recovered, promoted>>

B1RRecover ==
    /\ RestoreMode = "Identity"
    /\ phase = 4
    /\ failed
    /\ docOp' = NewOp
    /\ processed' = {11}
    /\ failed' = FALSE
    /\ recovered' = TRUE
    /\ phase' = 5
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, newAcknowledged, promoted>>

B1RPromote ==
    /\ phase = 5
    /\ promoted' = TRUE
    /\ phase' = 6
    /\ UNCHANGED
          <<fenceTerm, activeFenceMax, identityFenceTerm, identityFenceMax,
            committedMax, processed, docOp, newAcknowledged, failed,
            recovered>>

B1RNext ==
    \/ B1RApplyOldTermWrite
    \/ B1RRaiseFence
    \/ B1RRestart
    \/ B1RCommittedOnlyNewWrite
    \/ B1RIdentityCollision
    \/ B1RCommittedReady
    \/ B1RRecover
    \/ B1RPromote

B1RNoCopyBehindAcked ==
    /\ promoted
    /\ newAcknowledged
    => docOp = NewOp

B1RRestoresIdentityCollisionState ==
    /\ RestoreMode = "Identity"
    /\ phase >= 3
    => /\ fenceTerm = 2
       /\ activeFenceMax = 11

B1RFailsBeforeRecovery ==
    /\ RestoreMode = "Identity"
    /\ phase = 4
    => failed

B1RRecoveredBeforePromotion ==
    /\ RestoreMode = "Identity"
    /\ promoted
    => recovered

=============================================================================
