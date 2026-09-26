# C4 asynchronous-durability counterexample: `NoAckedLoss`

**Date:** September 26, 2026  
**Configuration:** `MC_C4.cfg`  
**TLC result:** expected violation of `NoAckedLoss`  
**Raw trace:** [`C4-async-durability-no-acked-loss.log`](C4-async-durability-no-acked-loss.log)  
**Raw trace SHA-256:** `a2e9da867069fcd37f0ae1336c99a50faa384cbd9426fcfac97e9f1c9bef0077`

| State range | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | Primary write and synchronous replication | Every in-sync process applies write 1 and the primary acknowledges it, but `FaultMode = "C4"` leaves each WAL operation outside the modeled durable prefix. |
| 9 | `Crash(n1)` | `src/wal/mod.rs` asynchronous durability permits the unsynced primary tail to be lost. The committed primary copy still exists but no longer contains acknowledged write 1, violating `NoAckedLoss`. |

This is not a claim about request durability, which synchronizes each WAL
append before acknowledgement in the model and implementation. It records the
weaker configured asynchronous mode.
