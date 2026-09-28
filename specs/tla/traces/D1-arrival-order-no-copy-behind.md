# D1 historical arrival-order rollback

**Date:** September 28, 2026

**Configuration:** `MC_D1_OrderHistorical.cfg`

**TLC result:** expected violation of `NoCopyBehindAcked`

**Raw trace:** [`D1-arrival-order-no-copy-behind.log`](D1-arrival-order-no-copy-behind.log)

**Raw trace SHA-256:** `21b6ba5373a992e1466157f0c747e106f81fe5f0971d8c7ae73f5555351d9af5`

TLC generated 308 states, found 181 distinct states before the violation, and
produced a depth-13 trace.

## Trace

| TLC state | Model action | Rust boundary represented |
| --- | --- | --- |
| 1-4 | Three overlapping `ClientWrite` actions | Concurrent `index_doc`/`bulk_index`/`delete_doc` requests enter the same shard before earlier requests finish replication. |
| 5-7 | `D1PrimaryAccept` | The primary WAL critical section assigns sequences 0, 1, and 2 and applies them locally. Replication begins only after that critical section is released. |
| 8-10 | Replica receives and acknowledges sequence 2 first | `replication::replicate_write` delivers out of order; `ShardManager::apply_replica_operation` and `append_with_seq` apply in arrival order. The delete is acknowledged. |
| 11-13 | Older sequence 1 arrives later | Arrival-order apply overwrites the acknowledged delete with the older index. The replica's per-document applied sequence falls below the highest acknowledged sequence. |

## Interpretation

This is implementation-faithful evidence for the pre-D1 code. The primary may
be ahead safely, but an available in-sync replica may never be behind the
highest acknowledged sequence for a document.

