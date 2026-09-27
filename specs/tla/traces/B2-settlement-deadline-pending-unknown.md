# B2 settlement-deadline pending-target liveness counterexample

**Date:** September 27, 2026

**Configuration:** reviewer `MC_L1_Bump.cfg`, before restoring the rejection rule

**TLC result:** expected historical violation of temporal recovery properties

**Raw trace:** [`B2-settlement-deadline-pending-unknown.log`](B2-settlement-deadline-pending-unknown.log)

**Raw trace SHA-256:** `29996179156c42c4573a21d27a24a85b3bda62e0bf4cf828774305dc93b02f96`

The reviewer extended the two-node L1 recovery slice with
`SettlementDeadline` and raised `MaxTerm` from 1 to 2. TLC explored 44 distinct
states and produced an 18-state lasso.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-10 | Recovery through `BeginSettlement` | The target installs the snapshot, persists its awaiting-membership marker for primary `n1`, term 1, allocation 1, and remains non-destructively pending. |
| 11 | `ProposeMarkInSync(n2)` | `settle_peer_recovery` queues the conditional admission command. |
| 12 | `SettlementDeadline(n2)` | The source queues allocation-bound `ActivatePrimary` after admission has not settled. |
| 13 | Commit deadline bump | `ClusterStateMachine::apply_command` accepts the term bump and advances routing to term 2. |
| 14 | Commit delayed mark | The old term-1 `MarkReplicaInSync` is rejected. |
| 15-16 | `DeliverView(n2)` | The target applies the term-2 routing state, with the same primary and allocation but without admission. |
| 17-18 | Source cleanup; stuttering | The source releases its barrier, but the old `TargetObservation` returns `Unknown` forever because it ignored the strictly newer term. |

## Resolution

`TargetObservation` now evaluates in this order:

1. admit the same allocation when it is in sync, or admit the target after
   promotion;
2. otherwise reject a missing or different target allocation, a strictly newer
   term, or a different primary;
3. otherwise return `Unknown`.

The ordering is important: if admission committed before the bump, the same
ordered view already contains the in-sync membership and wins before the
newer-term rejection test.

The checked-in `l1-bump` configuration preserves the reviewer bounds
(`MaxTerm = 2`) and now passes 50 distinct states to depth 18. Separate bounded
checks cover source-primary restart/reactivation and promotion of another
in-sync replica.
