# Retired D1 tombstone-retention convergence property

**Date:** September 28, 2026

**Configuration:** early `MC_D1_ReplayFixed.cfg`

**TLC result:** unexpected violation of an over-strong quiescent convergence
formulation

**Raw trace:** [`D1-retired-tombstone-retention-convergence.log`](D1-retired-tombstone-retention-convergence.log)

**Raw trace SHA-256:** `a98892f5c1f1fd0333994956826b4216cec574a37ce87c5da386af843d8ac258`

TLC generated 1,583 states and found 531 distinct states before the depth-29
violation.

After replay, primary and replica agreed that `X` was deleted at sequence 2
and that `Y` contained sequence 0. The replica had safely pruned its retained
tombstone metadata while the primary still retained its tombstone. Comparing
that internal retention state incorrectly classified the logically identical
copies as divergent.

Quiescent convergence now compares only semantic document state:

- absent, live, or deleted;
- value and applied sequence for live documents; and
- deletion status for deleted documents.

Retention, cache, and bookkeeping metadata are intentionally excluded.

