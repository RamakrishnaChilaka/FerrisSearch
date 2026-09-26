# C1 allocation ABA counterexample: `NoPartialServe`

**Date:** September 26, 2026  
**Configuration:** `MC_C1_ABA.cfg`  
**TLC result:** expected implementation-faithful violation of `NoPartialServe`  
**Raw trace:** [`C1-allocation-aba-no-partial-serve.log`](C1-allocation-aba-no-partial-serve.log)  
**Raw trace SHA-256:** `102d79366eaaac3f28a15fdf0f612840f6a7e60412e21b04b225c14ee48ae652`

This trace uses three Raft voters. Every committed command has a live leader
and a live voter majority. Dead-node processing follows the merged
`src/node/mod.rs` order: routing update, voter removal, `RemoveNode`, committed
`AddNode`, then allocation from the leader's applied view.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1 | `Init` | `n1` is shard primary and Raft leader; `n2` is in sync; `n3` is assigned out of sync. |
| 2-3 | `StartRecovery`; `SourceSnapshot` | `run_peer_recovery` polls `start_peer_recovery_inner`; `prepare_peer_recovery_snapshot` captures boundary 0 and pins it. |
| 4-6 | `TargetBeginInstall`; `InstallSnapshot`; `FinishCatchUp` | The target enters `Recovering` immediately before the wipe, installs the verified snapshot, removes the file-install marker, and catches up. |
| 7-8 | `BeginPrepareFinalize`; `AcquireFinalizeBarrier` | `prepare_finalize_recovery_inner` sets `finalize_preparing`, drains shared writers, takes the exclusive guard, and captures the head. |
| 9-10 | `TargetComplete`; `BeginSettlement` | `mark_peer_recovery_awaiting_membership_blocking` persists the pending marker and `complete_finalize_recovery_inner` starts settlement. |
| 11 | `Crash(n3)` | The target process crashes; the awaiting-membership marker and installed files persist. |
| 12-15 | `SuspectAndRemove`; committed `UpdateIndex`; `Restart`; `ObserveRoutingAccepted` | After failure detection, the leader removes `n3` from routing. The target can restart before lifecycle cleanup finishes and later observes the removal. |
| 16-17 | `DeliverView(n3)`; `TargetObserveRejected(n3)` | `observe_target_membership` sees assignment gone and returns `Rejected`; `abort_peer_recovery_target_blocking` closes the copy and writes the install marker. |
| 18-21 | `ChangeRaftMembership`; `ProposeRemoveNode`; committed `RemoveNode`; `ObserveNodeRemoved` | The leader completes the merged dead-node sequence. Voters are `{n1,n2}` and `n3` is absent from cluster-state membership. |
| 22-24 | `Rejoin`; committed `AddNode`; `ObserveRejoin` | Restarted `n3` rejoins and is again a registered voter/data node. |
| 25-26 | `AllocateAfterLifecycle`; committed `UpdateIndex` | The allocator creates a new out-of-sync assignment for the same node name `n3`, with the same primary `n1` and primary term 1. |
| 27 | `ProposeMarkInSync` | The old source settlement re-submits `MarkReplicaInSync(n3, n1, 1)`. No allocation or recovery-attempt identity distinguishes the removed assignment from the new one. |
| 28 | committed `MarkReplicaInSync` | `ClusterStateMachine::apply_command` sees the same node assigned, the same primary, and the same term, so it admits `n3`. The copy still has the destructive install marker, violating `NoPartialServe`. |

## Timing interpretation

The model abstracts wall-clock time but the ordering fits the implemented
timers:

- source settlement retries `MarkReplicaInSync` every 100 ms and keeps the
  exclusive barrier until its approximately 20-second finalize deadline;
- dead-node detection fires after approximately 15 seconds without pings;
- restart and `AddNode` can occur while cleanup proceeds; and
- the allocator runs on the approximately five-second leader lifecycle tick.

The vulnerable interval is narrow, but bounded model checking needs only one
legal interleaving. The model does not assert that this schedule is likely.

## Code gap

`ClusterStateMachine::apply_command` validates `MarkReplicaInSync` using index
UUID, replica node name, primary node, primary term, current assignment, and
current in-sync membership. It does not bind the command to the assignment
that recovery started against. `observe_target_membership` similarly has no
assignment identity. Removal followed by same-node reallocation is therefore
an ABA transition.

The `AllocationIds = TRUE` model variant binds recovery and admission to a
fresh assignment identity and is expected to eliminate this trace.
