# D1 primary max-based gap recovery loop

**Date:** September 28, 2026

**Configuration:** `MC_D1_PrimaryGapMaxBased.cfg`

**TLC result:** expected violation of `B3NoRecoveryLoop`

**Raw trace:** [`D1-primary-gap-max-recovery-loop.log`](D1-primary-gap-max-recovery-loop.log)

**Raw trace SHA-256:** `5526c17b02055b26ff8523306ad9620ba47c747ba95115c76de53478a051534d`

TLC generated four states and produced a depth-4 counterexample.

The primary WAL contains sequences `{0, 1, 2}`, but sequence 1 failed engine
apply. Primary and replica have both processed `{0, 2}` with checkpoint 1,
while the primary maximum is 3 in exclusive form. Comparing the replica
checkpoint to the primary maximum triggers re-recovery. Recovery copies the
same legitimate primary gap, so the comparison triggers again.

The fixed configuration compares processed checkpoints. Both copies report
checkpoint 1, so no recovery loop begins. Acknowledged operations 0 and 2 are
present on both copies despite the checkpoint gap.

