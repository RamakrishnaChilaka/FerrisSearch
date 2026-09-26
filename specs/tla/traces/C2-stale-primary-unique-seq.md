# C2 stale-primary counterexample: `UniqueAckedSeq`

**Date:** September 26, 2026  
**Configurations:** `MC_C2_fast.cfg`, `MC_C2_allocation_ids.cfg`  
**TLC result:** expected violation of `UniqueAckedSeq` in both variants

Raw traces:

- [`C2-stale-primary-unique-seq.log`](C2-stale-primary-unique-seq.log),
  SHA-256 `65380361d5b61b6392e52c84fe463b307066d35d6c8e8e8c7dfdefc9a641185a`
- [`C2-stale-primary-allocation-ids.log`](C2-stale-primary-allocation-ids.log),
  SHA-256 `21c23be74bef4132ce496f4046100aff0e49804ca00c47f6c7a0146736c9c11f`

The two traces have the same protocol behavior. Allocation IDs protect replica
assignment and recovery admission; they do not fence primary-term data-plane
operations.

| State | Action | Rust behavior represented |
| --- | --- | --- |
| 1 | `Init` | `n1` is shard primary at term 1; `n2` is Raft leader; `n2` and `n3` are in sync. |
| 2 | `PartitionMetadata(n1)` | The old primary loses Raft/view connectivity but data-plane RPC delivery remains possible. |
| 3-4 | `SuspectAndRemove`; committed `UpdateIndex` | `src/node/mod.rs` selects in-sync `n2`; `ClusterStateMachine::UpdateIndex` promotes it and advances the term to 2. |
| 5-7 | `ProposeActivate(n2)`; commit; observation | `ensure_primary_activated` commits `ActivatePrimary`, advancing the new primary to term 3. |
| 8-12 | New-primary write | `n2` allocates sequence 0, replicates to current in-sync `n3`, and acknowledges write 1. |
| 13-18 | Stale-primary write | Partitioned `n1` still sees itself primary at term 1, independently allocates sequence 0, and sends the operation to the old in-sync set `{n2,n3}`. |
| 15-16 | `ReplicaApply` | `replicate_doc` on `n2` and `n3` accepts the old-term operation because the current implementation carries no primary term and performs no term check. |
| 19 | `PrimaryAck` | The stale primary receives both replica acknowledgements. Writes 1 and 2 are both acknowledged with sequence 0, violating `UniqueAckedSeq`. |

The result is intentionally retained as a regression until stale-primary and
replica-apply fencing is implemented. The allocation-ID recovery variant is
not expected to change this result.
