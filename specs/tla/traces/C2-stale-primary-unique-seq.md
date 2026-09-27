# C2 stale-primary counterexample: `UniqueAckedSeq`

**Date:** September 27, 2026

**Configurations:** `MC_C2_fast.cfg`, `MC_C2_allocation_ids.cfg`

**TLC result:** expected stale-primary fencing violation in both unfenced variants

Raw traces:

- [`C2-stale-primary-unfenced-message.log`](C2-stale-primary-unfenced-message.log),
  SHA-256 `a4d1cfdbc8953d69bee3d5f079833cebf0411662bf417c3608e943c6f52f362f`
- [`C2-stale-primary-allocation-only-message.log`](C2-stale-primary-allocation-only-message.log),
  SHA-256 `1170e3cef6613bba346b57778aa655d6d78059cad796e8ee9f0c1c87bc3ab4f1`

These current traces stop at `C2RejectsStaleMessage`: the lower-term
replication request is accepted by the replica validation predicate. The
following earlier traces continue the same behavior through acknowledgement
and demonstrate `UniqueAckedSeq`:

- [`C2-stale-primary-unique-seq.log`](C2-stale-primary-unique-seq.log),
  SHA-256 `fba4992a0bc5be765a907c48a78e354709f0a8021d0414ae8c34d20a70fec936`
- [`C2-stale-primary-allocation-ids.log`](C2-stale-primary-allocation-ids.log),
  SHA-256 `f0c3a9a78ed704f9e765f008b074589d339c8f3c7d7ba487e927dbe43f4d3c95`

Allocation IDs protect replica assignment and recovery admission; they do not
fence primary-term data-plane operations. `MC_C2_fixed.cfg` enables both
allocation IDs and replica fencing and passes the same bounded scenario.

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
replica-apply fencing is implemented.
