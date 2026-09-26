# C4 asynchronous-durability counterexample: `NoAckedLoss`

**Date:** September 26, 2026  
**Configuration:** `MC_C4.cfg`  
**TLC result:** expected violation of `NoAckedLoss`  
**Raw trace:** [`C4-async-durability-no-acked-loss.log`](C4-async-durability-no-acked-loss.log)  
**Raw trace SHA-256:** `76898ffc253611f60da5faa43798bd6e5c7bfc514d7d0ec543a4bfbbdfee6fbd`

| State range | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | Primary write and synchronous replication | Every in-sync process applies write 1 and the primary acknowledges it, but `FaultMode = "C4"` leaves each WAL operation outside the modeled durable prefix. |
| 9 | `Crash(n1)` | `src/wal/mod.rs` asynchronous durability permits the unsynced primary tail to be lost. The committed primary copy still exists but no longer contains acknowledged write 1, violating `NoAckedLoss`. |

This is not a claim about request durability, which synchronizes each WAL
append before acknowledgement in the model and implementation. It records the
weaker configured asynchronous mode.
