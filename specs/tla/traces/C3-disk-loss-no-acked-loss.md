# C3 disk-loss counterexample: `NoAckedLoss`

**Date:** September 27, 2026

**Configuration:** `MC_C3.cfg`

**TLC result:** expected violation of `NoAckedLoss`

**Raw trace:** [`C3-disk-loss-no-acked-loss.log`](C3-disk-loss-no-acked-loss.log)

**Raw trace SHA-256:** `e53e77c57e9eb14c5deae56f6ff0cf4b44f2d6ad21c66d874c9165516599c6fb`

| State range | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | Primary write and synchronous replication | `index_doc` and `replicate_write` durably place write 1 on the primary and both in-sync replicas before acknowledgement. |
| 9 | `Crash(n3)` | The replica process stops while metadata still lists it in sync. |
| 10 | `DiskLoss(n3)` | The shard directory and durable local assignment identity are lost, while the stable node name remains. |
| 11 | `Restart(n3)` | The same node identity returns. |
| 12 | `OpenAssignedEmptyCopy(n3)` | With `AllocationIds = FALSE`, node-name routing is sufficient to open a new empty copy as active. The committed in-sync replica lacks acknowledged write 1, violating `NoAckedLoss`. |

`MC_C3_allocation_ids.cfg` uses the same schedule with
`AllocationIds = TRUE`. Disk loss clears the durable local allocation identity,
so the empty copy cannot satisfy `CopyAssignmentValid` and cannot open as the
authoritative assignment. That bounded run passes, representing fail-closed
unavailability pending explicit recovery rather than silent empty-copy reuse.
