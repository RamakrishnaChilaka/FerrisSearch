# B3 restarted pending-target wipe counterexample

**Date:** September 27, 2026

**Configuration:** `MC_PendingRestart_legacy.cfg`

**TLC result:** expected historical violation of `NoPartialServe`

**Raw trace:** [`B3-pending-restart-wipe.log`](B3-pending-restart-wipe.log)

**Raw trace SHA-256:** `41d753ebbd6a3f4833ca9669e252a691edd91d838caba5232f1b03abeae5f49b`

TLC generated 20 states, found 19 distinct states, and reached the violation
at depth 18.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-4 | Client write and acknowledgement | The primary acknowledges write 1 while the target is still out of sync. |
| 5-12 | Recovery through `TargetComplete` | Snapshot install copies write 1. The target persists a marker bound to UUID, allocation 1, primary `n1`, and term 1. |
| 13-14 | Settlement and `ProposeMarkInSync` | The source holds the finalize barrier and queues the delayed admission command. |
| 15-16 | Target crash and restart | Runtime pending registration is lost, but the durable marker and finalized files survive. |
| 17 | `PRLegacyWipeAndDelayedAdmission` | Historical restart behavior ignores the matching marker, reattaches a new target run to the still-settling source session, wipes the finalized contents, and then applies the already-queued conditional admission. The target is in sync while its install marker blocks serving, violating `NoPartialServe`; it also lacks acknowledged write 1. |

The final action is a scenario-wrapper compression of two adjacent Rust
boundaries: target reattach/prepare in
`start_peer_recovery_inner` and
`prepare_peer_recovery_target_blocking`, followed by the delayed
`MarkReplicaInSync` state-machine application. The general model continues to
use the ordinary Raft actions.

## Resolution

With `RestorePendingOnRestart = TRUE`, crash removes only volatile runtime
registration. Restart reconciliation reloads the marker only when its fixed
index UUID and allocation ID match the durable copy and applied assignment.
While such a matching marker exists, `StartRecovery` and
`TargetBeginInstall` cannot begin a destructive run. The fixed configuration
passes 31 distinct states to depth 22 and clears the marker only after
authoritative admission or definitive rejection.
