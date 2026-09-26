# C1 recovery/crash counterexample: `NoPartialServe`

**Date:** September 26, 2026  
**Configuration:** `MC_C1_recovery_crash.cfg`  
**TLC result:** unexpected violation of `NoPartialServe`  
**Raw trace:** [`C1-recovery-crash-no-partial-serve.log`](C1-recovery-crash-no-partial-serve.log)  
**Raw trace SHA-256:** `ffb36304c9e63cbf61302eb904e4c4c8b78b31592017919d70d151143e4ca32a`

This was an expected-pass configuration, so modeling work stopped at this
trace. The model was not weakened and Rust source was not changed.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1 | `Init` | Primary `n1`, assigned out-of-sync replica `n2`, term 1. |
| 2 | `ClientWrite` | A client write is routed but remains queued behind the later exclusive recovery barrier. |
| 3 | `StartRecovery(n2, n1)` | `node::peer_recovery::run_peer_recovery` polls `TransportService::start_peer_recovery_inner`; no destructive target work has started. |
| 4 | `SourceSnapshot(n2)` | `HotEngine::prepare_peer_recovery_snapshot` captures boundary 0 and registers retention pin 0 under the translog lock. |
| 5 | `TargetBeginInstall(n2)` | `ShardManager::begin_peer_recovery_target` and `prepare_peer_recovery_target_blocking` mark the target recovering and create the install marker immediately before wiping. |
| 6 | `InstallSnapshot(n2)` | `ShardManager::finalize_peer_recovery_target_blocking` strictly opens the verified snapshot and removes the install marker; the in-memory recovering gate remains. |
| 7 | `FinishCatchUp(n2)` | `apply_recovery_operations` has reached the source head. |
| 8 | `BeginPrepareFinalize(n2)` | `prepare_finalize_recovery_inner` sets `finalize_preparing`. |
| 9 | `AcquireFinalizeBarrier(n2)` | `prepare_finalize_recovery_inner` drains shared writers, takes the exclusive guard, and captures head 0. |
| 10 | `TargetComplete(n2)` | `mark_peer_recovery_awaiting_membership_blocking` persists the pending marker; pending copies accept live replication. |
| 11 | `BeginSettlement(n2)` | `complete_finalize_recovery_inner` starts `settle_peer_recovery`; source `n1` still holds the exclusive barrier. |
| 12 | `Crash(n2)` | The target process crashes. The persistent pending marker survives. |
| 13-14 | `SuspectAndRemove`; `CommitRaft(UpdateRouting)` | The model removes `n2` from replica assignment while preserving primary `n1` and term 1. |
| 15-16 | `Restart(n2)`; `DeliverView(n2)` | The target restarts, reloads its persistent pending marker, and observes that its assignment is gone. |
| 17-18 | `Allocate`; `CommitRaft(UpdateRouting)` | The model reassigns the same node name `n2` as an out-of-sync replica, still at primary term 1. |
| 19 | `ProposeMarkInSync(n2)` | The old source settlement submits `MarkReplicaInSync(n2, n1, term 1)`. The command has no allocation/recovery identity. |
| 20 | `TargetObserveRejected(n2)` | `observe_target_membership` uses the target's lagging removal view, returns `Rejected`, and `abort_peer_recovery_target_blocking` creates the destructive install marker. |
| 21 | `CommitRaft(MarkReplicaInSync)` | The delayed command sees `n2` assigned again with the same primary and term, so `ClusterStateMachine::apply_command` admits it. The committed in-sync set now contains a copy with the install marker, violating `NoPartialServe`. |

## Assessment

The exact two-node behavior is a **model error**, not implementation evidence:

1. `RaftLog.tla` currently allows the routing update to commit after one of two
   modeled voters crashes. Real Raft cannot commit with no majority.
2. `Allocate` is independently enabled from any live node and can run before
   the merged `src/node/mod.rs` dead-node loop completes Raft membership and
   `RemoveNode` processing. The implementation performs those operations before
   the allocator phase in the same lifecycle tick.

The trace therefore cannot certify a FerrisSearch defect as written.

It does expose an implementation risk that the corrected model must retain:
`MarkReplicaInSync` validates index UUID, node name, primary, term, and current
assignment, but has no allocation or recovery-attempt identity. A delayed old
admission could therefore match a later same-node assignment (the documented
remove/re-add ABA limitation) if a valid three-node schedule reaches the same
ordering. That requires a corrected quorum-aware, leader-sequenced C1 model
before drawing an implementation conclusion.

That corrected run now exists and reproduces the ABA with three voters and the
merged lifecycle order. See
[`C1-allocation-aba-no-partial-serve.md`](C1-allocation-aba-no-partial-serve.md).
