# D1 restart restores collision state from committed max only

**Date:** September 28, 2026

**Configuration:** `MC_D1_TermCollisionRestartCommitted.cfg`

**TLC result:** expected violation of `B1RNoCopyBehindAcked`

**Raw trace:** [`D1-term-collision-restart-committed-only.log`](D1-term-collision-restart-committed-only.log)

**Raw trace SHA-256:** `3ad73d83c60444093dd8cf9dd0d83d354da6055c36a802a6fbbbc08c32e468f6`

TLC generated seven states and produced a depth-7 counterexample.

R applied term-1 sequence 11, raised its fence to term 2, and persisted
identity `fence_max_seq_no = 11`. It then restarted before receiving a term-2
operation. The historical restart path restored collision maximum 10 from the
last committed record while WAL replay restored sequence 11 as processed.
Term-2 sequence 11 was therefore misclassified as redelivery and skipped.
Promoting R rolled back the acknowledged term-2 value.

The fixed configuration restores both fence term and fence maximum from
durable copy identity before serving replication. It detects the collision,
fails the copy, and requires recovery before promotion.
