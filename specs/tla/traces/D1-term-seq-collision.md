# D1 term/sequence collision with seq-only redelivery

**Date:** September 28, 2026

**Configuration:** `MC_D1_TermCollisionSeqOnly.cfg`

**TLC result:** expected violation of `B1NoCopyBehindAcked`

**Raw trace:** [`D1-term-seq-collision.log`](D1-term-seq-collision.log)

**Raw trace SHA-256:** `b7756434b7b94f63af2e8e6c81c21aba8cfdf6decff3dcf44b0d217e6a7e472b`

TLC generated six states and produced a depth-6 counterexample.

## Trace

| TLC state | Model action | Rust/D1 boundary represented |
| --- | --- | --- |
| 1-2 | `B1OldWritePartiallyReplicated` | Primary `P` assigns sequence 11 at term 1. R2 applies it, R1's replica RPC fails, and the request is not acknowledged. |
| 3 | `B1PromoteR1` | P crashes. R1 is promoted to term 2 with WAL maximum 10 and durably raises its fence. |
| 4 | `B1SeqOnlyNewWrite` | R1 reuses sequence 11 for a different term-2 operation. R2 treats sequence 11 alone as redelivery, skips the new value, and returns success. |
| 5-6 | `B1PromoteR2` | R2 later becomes primary while still holding the old term-1 sequence-11 operation. The acknowledged term-2 value is rolled back. |

## Resolution

The fixed variant persists local `max_seq_no` when raising the replica fence.
An already-processed sequence at or below that maximum, received under a newer
term, is a definitive identity collision rather than redelivery. The copy is
failed, removed from eligibility, and peer-recovered before promotion.

