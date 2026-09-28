------------------------ MODULE MC_D1_TermCollision -------------------------
\* B1 bounded term/sequence collision.  The old primary assigns sequence 11 at
\* term 1 and only R2 applies it.  R1 is promoted with max_seq_no 10, reuses
\* sequence 11 at term 2, and acknowledges a different operation.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS P, R1, R2, CollisionMode

Nodes == {P, R1, R2}
NoOp == "NONE"
OldOp == "OLD_TERM_1_SEQ_11"
NewOp == "NEW_TERM_2_SEQ_11"

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
    /\ docSeq = [node \in Nodes |-> 10]
    /\ docTerm = [node \in Nodes |-> 1]
    /\ maxSeqNo = [node \in Nodes |-> 10]
    /\ durableMaxSeqNo = [node \in Nodes |-> 10]
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
    /\ docSeq \in [Nodes -> 10..11]
    /\ docTerm \in [Nodes -> 1..2]
    /\ maxSeqNo \in [Nodes -> 10..11]
    /\ durableMaxSeqNo \in [Nodes -> 10..11]
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
    /\ docSeq' = [docSeq EXCEPT ![P] = 11, ![R2] = 11]
    /\ docTerm' = [docTerm EXCEPT ![P] = 1, ![R2] = 1]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![P] = 11, ![R2] = 11]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT ![P] = 11, ![R2] = 11]
    /\ phase' = 1
    /\ UNCHANGED
          <<primary, primaryTerm, fenceTerm, inSync, newAcknowledged, failed,
            recovered>>

\* P crashes. R1 is promoted to term 2 and durably records both the fence and
\* its current max_seq_no (10) before assigning a new sequence.
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

\* Historical redelivery detection uses sequence only. R2 sees sequence 11 as
\* processed, acknowledges without applying the term-2 value, and remains
\* eligible.
B1SeqOnlyNewWrite ==
    /\ CollisionMode = "SeqOnly"
    /\ phase = 2
    /\ docOp' = [docOp EXCEPT ![R1] = NewOp]
    /\ docSeq' = [docSeq EXCEPT ![R1] = 11]
    /\ docTerm' = [docTerm EXCEPT ![R1] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R1] = 11]
    /\ durableMaxSeqNo' = [durableMaxSeqNo EXCEPT ![R1] = 11]
    /\ fenceTerm' = [fenceTerm EXCEPT ![R2] = 2]
    /\ newAcknowledged' = TRUE
    /\ phase' = 3
    /\ UNCHANGED <<primary, primaryTerm, inSync, failed, recovered>>

\* Fixed D1: raising R2's fence persists max_seq_no 11. A term-2 operation at
\* that processed sequence is a definitive collision, so R2 fails instead of
\* returning a false redelivery acknowledgement.
B1TermAwareNewWrite ==
    /\ CollisionMode = "TermAware"
    /\ phase = 2
    /\ docOp' = [docOp EXCEPT ![R1] = NewOp]
    /\ docSeq' = [docSeq EXCEPT ![R1] = 11]
    /\ docTerm' = [docTerm EXCEPT ![R1] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R1] = 11]
    /\ durableMaxSeqNo' =
          [durableMaxSeqNo EXCEPT ![R1] = 11, ![R2] = maxSeqNo[R2]]
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
    /\ docSeq' = [docSeq EXCEPT ![R2] = 11]
    /\ docTerm' = [docTerm EXCEPT ![R2] = 2]
    /\ maxSeqNo' = [maxSeqNo EXCEPT ![R2] = 11]
    /\ durableMaxSeqNo' = [durableMaxSeqNo EXCEPT ![R2] = 11]
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
            /\ durableMaxSeqNo[R2] = 11
            /\ \/ R2 \in failed
               \/ docOp[R2] = NewOp)

B1RecoveredBeforePromotion ==
    /\ CollisionMode = "TermAware"
    /\ primary = R2
    => recovered

=============================================================================
