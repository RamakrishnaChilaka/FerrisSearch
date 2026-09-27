# Retired fixed-partition property trace: pre-promotion in-flight apply

**Date:** September 26, 2026

**Configuration:** `MC_Fixed_Partition.cfg`

**TLC result:** unexpected violation of `NoStaleReplicaApply`

**Raw trace:** [`Fixed-partition-prepromotion-inflight-apply.log`](Fixed-partition-prepromotion-inflight-apply.log)

**Raw trace SHA-256:** `0a309415572431695ecf2c7318e3b14143ed5aeaeb6f30ca38e10b203dbeeece`

This was an expected-pass configuration, so the modeling task stopped at this
trace before the property was refined. No Rust source was changed.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1 | `Init` | `n1` is primary at term 1, `n2` is in sync, `n3` is an assigned out-of-sync replica and Raft leader. |
| 2 | `ClientWrite` | A write is routed to `n1` while its term-1 authority is still current. |
| 3 | `PrimaryAccept` | `TransportService::index_doc` validates term 1, mutates `n1`, and sends term-1 replication to required replica `n2`. |
| 4 | `PartitionMetadata(n1)` | The already-sent data-plane message remains in flight while `n1` loses metadata connectivity. |
| 5-6 | `SuspectAndRemove`; committed `UpdateIndex` | Leader `n3` promotes `n2` and advances committed routing to term 2. `n2` has not yet applied that Raft entry. |
| 7 | `ReplicaApply(n2)` | `n2` still sees term 1 and has local fence 1, so the requested rule accepts the already-in-flight term-1 operation. The history variable compares it to global committed term 2 and reports `NoStaleReplicaApply`. |

## Assessment

This is a **model-property error**, not evidence that the proposed fencing
protocol failed.

`NoStaleReplicaApply` rejected every apply whose message term was below
the globally committed term. That is stronger than the modeled and requested
replica rule, which rejects below:

```text
max(term in the replica's applied local view, durable local fence)
```

At state 7, both local values are still 1. The operation was accepted by the
old primary before the partition and was already in flight. The promoted copy
has neither observed promotion nor activated or served a new-term write.
Retaining this indeterminate pre-promotion operation is not, by itself, an
acknowledged-write loss or conflicting stale-primary success.

The replacement properties distinguish:

- an operation accepted and sent while its term was still authoritative; from
- a new operation accepted by an obsolete primary after promotion.

`NoApplyBelowObservedFence` now checks the durable local fence at apply time,
and `ActivePrimaryRejectsOldTerm` checks the activated primary term in the
current incarnation. The replica-fence rule itself was not weakened. After
this correction, both unrestricted fixed-design configurations pass.
