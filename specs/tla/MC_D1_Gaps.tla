----------------------------- MODULE MC_D1_Gaps -----------------------------
\* B2 bounded gap outcomes. Sequence 1 fails on the replica, while sequences 0
\* and 2 are processed. The missing operation is unacknowledged; sequences 0
\* and 2 are acknowledged. The gap is resolved by pulling sequence 1,
\* re-recovery, or promotion-time NoOp insertion.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Primary, GapCopy

Seqs == 0..2

VARIABLES
    phase,
    primary,
    processed,
    processedNext,
    maxSeqNo,
    inSync,
    acknowledged,
    noOps,
    outcome

vars ==
    <<phase, primary, processed, processedNext, maxSeqNo, inSync,
      acknowledged, noOps, outcome>>

ContiguousNext(values) ==
    CHOOSE boundary \in 0..3 :
        /\ {seq \in Seqs : seq < boundary} \subseteq values
        /\ (boundary = 3 \/ boundary \notin values)

B2Init ==
    /\ phase = 0
    /\ primary = Primary
    /\ processed = {}
    /\ processedNext = 0
    /\ maxSeqNo = 0
    /\ inSync = {GapCopy}
    /\ acknowledged = {}
    /\ noOps = {}
    /\ outcome = "None"

B2TypeOK ==
    /\ phase \in 0..2
    /\ primary \in {Primary, GapCopy}
    /\ processed \subseteq Seqs
    /\ processedNext \in 0..3
    /\ maxSeqNo \in 0..2
    /\ inSync \subseteq {GapCopy}
    /\ acknowledged \subseteq Seqs
    /\ noOps \subseteq Seqs
    /\ outcome \in {"None", "Pull", "Recovery", "Promotion"}

\* replication::replicate_write for sequence 1 fails permanently on GapCopy.
\* It still processes acknowledged sequences 0 and 2, leaving checkpoint 1.
B2CreateGap ==
    /\ phase = 0
    /\ processed' = {0, 2}
    /\ processedNext' = 1
    /\ maxSeqNo' = 2
    /\ acknowledged' = {0, 2}
    /\ phase' = 1
    /\ UNCHANGED <<primary, inSync, noOps, outcome>>

\* A bounded retry/pull obtains the missing operation before the copy is used.
B2PullMissing ==
    /\ phase = 1
    /\ processed' = Seqs
    /\ processedNext' = 3
    /\ outcome' = "Pull"
    /\ phase' = 2
    /\ UNCHANGED
          <<primary, maxSeqNo, inSync, acknowledged, noOps>>

\* Timeout removes the copy, and full peer recovery installs the complete
\* processed history before re-admission.
B2TimeoutAndRecover ==
    /\ phase = 1
    /\ processed' = Seqs
    /\ processedNext' = 3
    /\ inSync' = {GapCopy}
    /\ outcome' = "Recovery"
    /\ phase' = 2
    /\ UNCHANGED <<primary, maxSeqNo, acknowledged, noOps>>

\* Promotion is safe because the missing sequence was not acknowledged. The
\* promoted copy writes a term-local NoOp for sequence 1 and closes the gap.
B2PromoteAndFillNoOp ==
    /\ phase = 1
    /\ primary' = GapCopy
    /\ processed' = Seqs
    /\ processedNext' = 3
    /\ noOps' = {1}
    /\ outcome' = "Promotion"
    /\ phase' = 2
    /\ UNCHANGED <<maxSeqNo, inSync, acknowledged>>

B2Next ==
    \/ B2CreateGap
    \/ B2PullMissing
    \/ B2TimeoutAndRecover
    \/ B2PromoteAndFillNoOp

B2NoCopyBehindAcked ==
    acknowledged \subseteq processed

B2CheckpointGapAware ==
    processedNext = ContiguousNext(processed)

B2ResolvedCheckpointAdvances ==
    phase = 2 => processedNext = 3

B2PromotionFillsNoOp ==
    outcome = "Promotion" => 1 \in noOps

=============================================================================
