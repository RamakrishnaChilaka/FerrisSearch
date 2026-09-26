# C4 asynchronous-durability counterexample: `NoAckedLoss`

**Date:** September 26, 2026

**Configuration:** `MC_C4.cfg`

**TLC result:** expected violation of `NoAckedLoss`

**Raw trace:** [`C4-async-durability-no-acked-loss.log`](C4-async-durability-no-acked-loss.log)

**Raw trace SHA-256:** `a653fa1ecc4ec96d4ea77fb0f43272a1f2e54eff1994efa9c3a4343dd6ee7141`

| State range | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | Primary write and synchronous replication | Every in-sync process applies write 1 and the primary acknowledges it, but `FaultMode = "C4"` leaves each WAL operation outside the modeled durable prefix. |
| 9 | `Crash(n1)` | `src/wal/mod.rs` asynchronous durability permits the unsynced primary tail to be lost. The committed primary copy still exists but no longer contains acknowledged write 1, violating `NoAckedLoss`. |

This is not a claim about request durability, which synchronizes each WAL
append before acknowledgement in the model and implementation. It records the
weaker configured asynchronous mode.
