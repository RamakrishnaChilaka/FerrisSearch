------------------------ MODULE MC_D1_TermCollision -------------------------
\* B1 bounded term/sequence collision at parameterized CollisionSeq.  The old
\* primary assigns that sequence at term 1 and only R2 applies it.  R1 is
\* promoted one sequence behind and reuses CollisionSeq at term 2.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS P, R1, R2, CollisionMode, CollisionSeq

Nodes == {P, R1, R2}
NoOp == "NONE"
OldOp == "OLD_TERM_OPERATION"
NewOp == "NEW_TERM_OPERATION"
PriorSeq == CollisionSeq - 1

VARIABLES
    phase,
    primary,
    primaryTerm,
    docOp,
    docSeq,
    docTerm,
    maxSeqNo,
    durableMaxSeqNo,
    fenceTerm,
    inSync,
    newAcknowledged,
    failed,
    recovered

vars ==
    <<phase, primary, primaryTerm, docOp, docSeq, docTerm, maxSeqNo,
      durableMaxSeqNo, fenceTerm, inSync, newAcknowledged, failed, recovered>>

B1Init ==
    /\ phase = 0
    /\ primary = P
    /\ primaryTerm = 1
    /\ docOp = [node \in Nodes |-> NoOp]
    /\ CollisionSeq > 0
    /\ docSeq = [node \in Nodes |-> PriorSeq]
    /\ docTerm = [node \in Nodes |-> 1]
    /\ maxSeqNo = [node \in Nodes |-> PriorSeq]
    /\ durableMaxSeqNo = [node \in Nodes |-> PriorSeq]
    /\ fenceTerm = [node \in Nodes |-> 1]
    /\ inSync = {R1, R2}
    /\ newAcknowledged = FALSE
    /\ failed = {}
    /\ recovered = FALSE

B1TypeOK ==
    /\ phase \in 0..5
    /\ primary \in Nodes
    /\ primaryTerm \in 1..2
    /\ docOp \in [Nodes -> {NoOp, OldOp, NewOp}]
    /\ docSeq \in [Nodes -> PriorSeq..CollisionSeq]
    /\ docTerm \in [Nodes -> 1..2]
    /\ maxSeqNo \in [Nodes -> PriorSeq..CollisionSeq]
    /\ durableMaxSeqNo \in [Nodes -> PriorSeq..CollisionSeq]
    /\ fenceTerm \in [Nodes -> 1..2]
    /\ inSync \subseteq Nodes
    /\ newAcknowledged \in BOOLEAN
    /\ failed \subseteq Nodes
    /\ recovered \in BOOLEAN

\* TransportService primary write + replication::replicate_write: R2 applies
\* the term-1 operation, R1's RPC fails, and the request is not acknowledged.
B1OldWritePartiallyReplicated ==
    /\ phase = 0
    /\ docOp' = [docOp EXCEPT ![P] = OldOp, ![R2] = OldOp]
    /\ docSeq' =
          [docSeq EXCEPT ![P] = CollisionSeq, ![R2] = CollisionSeq]
    /\ docTerm' = [docTerm EXCEPT ![P] = 1, ![R2] = 1]
    /\ maxSeqNo' =
          [maxSeqNo EXCEPT ![P] = CollisionSeq, ![R2] = CollisionSeq]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT
              ![P] = CollisionSeq, ![R2] = CollisionSeq]
    /\ phase' = 1
    /\ UNCHANGED
          <<primary, primaryTerm, fenceTerm, inSync, newAcknowledged, failed,
            recovered>>

\* P crashes. R1 is promoted to term 2 and durably records both the fence and
\* its current max_seq_no before assigning the collision sequence.
B1PromoteR1 ==
    /\ phase = 1
    /\ primary' = R1
    /\ primaryTerm' = 2
    /\ fenceTerm' = [fenceTerm EXCEPT ![R1] = 2]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT ![R1] = maxSeqNo[R1]]
    /\ inSync' = {R2}
    /\ phase' = 2
    /\ UNCHANGED
          <<docOp, docSeq, docTerm, maxSeqNo, newAcknowledged, failed,
            recovered>>

\* Historical redelivery detection uses sequence only. R2 sees CollisionSeq as
\* processed, acknowledges without applying the term-2 value, and remains
\* eligible.
B1SeqOnlyNewWrite ==
    /\ CollisionMode = "SeqOnly"
    /\ phase = 2
    /\ docOp' = [docOp EXCEPT ![R1] = NewOp]
    /\ docSeq' = [docSeq EXCEPT ![R1] = CollisionSeq]
    /\ docTerm' = [docTerm EXCEPT ![R1] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R1] = CollisionSeq]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT ![R1] = CollisionSeq]
    /\ fenceTerm' = [fenceTerm EXCEPT ![R2] = 2]
    /\ newAcknowledged' = TRUE
    /\ phase' = 3
    /\ UNCHANGED <<primary, primaryTerm, inSync, failed, recovered>>

\* Fixed D1: raising R2's fence persists CollisionSeq. A term-2 operation at
\* that processed sequence is a definitive collision, so R2 fails instead of
\* returning a false redelivery acknowledgement.
B1TermAwareNewWrite ==
    /\ CollisionMode = "TermAware"
    /\ phase = 2
    /\ docOp' = [docOp EXCEPT ![R1] = NewOp]
    /\ docSeq' = [docSeq EXCEPT ![R1] = CollisionSeq]
    /\ docTerm' = [docTerm EXCEPT ![R1] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R1] = CollisionSeq]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT
              ![R1] = CollisionSeq, ![R2] = maxSeqNo[R2]]
    /\ fenceTerm' = [fenceTerm EXCEPT ![R2] = 2]
    /\ inSync' = {}
    /\ failed' = {R2}
    /\ newAcknowledged' = TRUE
    /\ phase' = 3
    /\ UNCHANGED <<primary, primaryTerm, recovered>>

B1SeqOnlyReadyForPromotion ==
    /\ CollisionMode = "SeqOnly"
    /\ phase = 3
    /\ phase' = 4
    /\ UNCHANGED
          <<primary, primaryTerm, docOp, docSeq, docTerm, maxSeqNo,
            durableMaxSeqNo, fenceTerm, inSync, newAcknowledged, failed,
            recovered>>

\* Exact recovery installs R1's term-2 value before R2 becomes eligible again.
B1RecoverR2 ==
    /\ CollisionMode = "TermAware"
    /\ phase = 3
    /\ R2 \in failed
    /\ docOp' = [docOp EXCEPT ![R2] = NewOp]
    /\ docSeq' = [docSeq EXCEPT ![R2] = CollisionSeq]
    /\ docTerm' = [docTerm EXCEPT ![R2] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R2] = CollisionSeq]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT ![R2] = CollisionSeq]
    /\ fenceTerm' = [fenceTerm EXCEPT ![R2] = 2]
    /\ inSync' = {R2}
    /\ failed' = {}
    /\ recovered' = TRUE
    /\ phase' = 4
    /\ UNCHANGED <<primary, primaryTerm, newAcknowledged>>

B1PromoteR2 ==
    /\ phase = 4
    /\ R2 \in inSync
    /\ primary' = R2
    /\ phase' = 5
    /\ UNCHANGED
          <<primaryTerm, docOp, docSeq, docTerm, maxSeqNo, durableMaxSeqNo,
            fenceTerm, inSync, newAcknowledged, failed, recovered>>

B1Next ==
    \/ B1OldWritePartiallyReplicated
    \/ B1PromoteR1
    \/ B1SeqOnlyNewWrite
    \/ B1TermAwareNewWrite
    \/ B1SeqOnlyReadyForPromotion
    \/ B1RecoverR2
    \/ B1PromoteR2

B1NoCopyBehindAcked ==
    /\ newAcknowledged
    /\ primary = R2
    => docOp[R2] = NewOp

B1CollisionFailsClosed ==
    CollisionMode = "TermAware" =>
        (phase >= 3 =>
            /\ fenceTerm[R2] = 2
            /\ durableMaxSeqNo[R2] = CollisionSeq
            /\ \/ R2 \in failed
               \/ docOp[R2] = NewOp)

B1RecoveredBeforePromotion ==
    /\ CollisionMode = "TermAware"
    /\ primary = R2
    => recovered

=============================================================================
