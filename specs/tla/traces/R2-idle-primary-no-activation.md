# R2 idle-primary activation liveness counterexample

**Date:** September 27, 2026

**Configuration:** `MC_L2_PrimaryRestart_NoTrigger.cfg`

**TLC result:** expected historical temporal-property violation

**Raw trace:** [`R2-idle-primary-no-activation.log`](R2-idle-primary-no-activation.log)

**Raw trace SHA-256:** `b168036e8658feee8fa4e66ee59d8fb8288e675e52d61a7b08b949dce5a67f0f`

TLC generated 94 states, found 49 distinct states, and produced a 16-state
lasso.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-11 | Recovery through `ProposeMarkInSync` | The idle shard has no writes. The target finalizes, persists its pending marker, and the source queues admission at term 1. |
| 12 | `PR2CrashPrimary` | The source primary crashes before admission commits. Its incarnation-local activation state and source session are lost. |
| 13-14 | `PR2RestartPrimary`; `PR2ElectPrimary` | The same allocated primary restarts and becomes Raft leader with its term-1 view, but remains unactivated in the new incarnation. |
| 15 | `PR2StopFaults` | Faults cease permanently. The target remains pending and no client write or recovery start occurs to call `ensure_primary_activated`. |
| 16 | Stuttering | Without fairness on a lifecycle activation trigger, the primary may remain unactivated forever and the pending target never resolves. |

## Resolution

`LifecycleProposeActivation` models the node lifecycle proactively invoking
`ensure_primary_activated` whenever the local applied view names the node as
primary but the current incarnation has not activated that routing term.
`MC_L2_PrimaryRestart_IdleShard.cfg` applies weak fairness to that lifecycle
action and passes 49 distinct states to depth 22.

The historical `l2-primary-no-trigger` configuration intentionally omits only
that fairness assumption and remains an expected temporal violation.
