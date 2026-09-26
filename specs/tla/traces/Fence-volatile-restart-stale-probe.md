# Volatile replica-fence counterexample

**Date:** September 26, 2026

**Configuration:** `MC_Fence_volatile.cfg`

**TLC result:** expected violation of `FenceRejectsStaleProbe`

**Raw trace:** [`Fence-volatile-restart-stale-probe.log`](Fence-volatile-restart-stale-probe.log)

**Raw trace SHA-256:** `4e5f33e90fddbd82b7a506cd0e272ccf94258d856de624b72dd39bab6d51bf1e`

| State range | Model action | Rust requirement represented |
| --- | --- | --- |
| 1-7 | Partition, promotion, activation | `n2` becomes primary and activates at term 3 while `n3` retains a term-1 cluster view. |
| 8-12 | New-primary write | `n3` accepts a term-3 replication request and raises its local fence to 3. |
| 13-14 | `Crash(n3)`; `Restart(n3)` | With `DurableReplicaFence = FALSE`, the process loses fence 3 and restarts with only its term-1 view. |
| 15 | `SendStaleReplicaProbe` | Old primary `n1` sends a term-1 replication retry to `n3`. UUID and allocation ID match, and the volatile fence has been lost, so `ReplicaMessageValid` permits the request. |

The model stops at the acceptance predicate rather than requiring the client
request to succeed. Mutating one current in-sync replica with an old-primary
operation is already unsafe even if another target rejects the request and the
client receives failure.

`MC_Fence_durable.cfg` persists fence 3 before acknowledging the term-3 apply.
After restart, the same term-1 probe is rejected and the bounded configuration
passes. The proposed Rust fence must therefore be durable.
