# D1 historical replay from highest committed sequence

**Date:** September 28, 2026

**Configuration:** `MC_D1_ReplayHistorical.cfg`

**TLC result:** expected violation of `D1ReplayPreservesAcknowledged`

**Raw trace:** [`D1-highest-commit-replay-loss.log`](D1-highest-commit-replay-loss.log)

**Raw trace SHA-256:** `e6c8abe4cf5536faffc23dc2bfe05097340ee3e059d687a20da93b66f1c729ad`

TLC generated 457 states, found 201 distinct states, and produced a depth-26
trace.

## Trace

| TLC state | Model action | Rust boundary represented |
| --- | --- | --- |
| 1-7 | Three overlapping writes | Sequence 0 indexes `Y`; sequence 1 indexes `X`; sequence 2 deletes `X`. |
| 8-11 | Sequence 2 arrives and is committed first | Arrival-order replica apply advances the historical commit/replay boundary to the highest observed sequence plus one despite gaps. |
| 12-19 | Older writes and a duplicate arrive | The replica acknowledges all writes, but their WAL file order is out of sequence. |
| 20-21 | Crash and restart | Startup restores the commit and begins replay from the historical highest-sequence boundary. |
| 22-25 | `D1ReplaySkip` | Every WAL entry has a sequence below the boundary and is skipped, including acknowledged sequence 0 for `Y`. |
| 26 | Replay completes | The replica permanently lacks acknowledged `Y`, matching the refresh-plus-restart probe. |

## Interpretation

The trace maps to `HotEngine` commit metadata and
`HotTranslog::for_each_from`. D1 instead persists the gap-aware processed
checkpoint and replays every retained operation above it through the same
sequence-aware planner.

