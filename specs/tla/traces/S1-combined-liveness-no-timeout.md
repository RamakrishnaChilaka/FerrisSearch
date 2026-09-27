# S1 combined liveness without transport timeout

**Date:** September 27, 2026

**Configuration:** `MC_S1_CombinedLivenessNoTimeout.cfg`

**TLC result:** expected temporal-property violation of the transport-timeout
modeling assumption

**Raw trace:** [`S1-combined-liveness-no-timeout.log`](S1-combined-liveness-no-timeout.log)

**Raw trace SHA-256:** `6152a73c2baa781a4412c49f96d7e4c4e24bc87cc1a27ed6e91d856f5067dae9`

TLC generated 49 states, found 41 distinct states, and produced a 21-state
lasso.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | First acknowledged write | The primary and both in-sync replicas durably apply write 1. |
| 9-13 | Persistent apply failure | Replica `n2` develops persistent apply I/O. Write 2 receives a replica NACK and fails. |
| 14 | `S1CrashBeforeEscalation` | `n2` crashes. Its durable fault survives, while the process-local retry budget resets from `ApplyRetrying` to `ApplyFailing`. |
| 15-18 | Write sent while `n2` is down | The primary applies write 3 and the healthy replica acknowledges it. The request to `n2` remains outstanding. |
| 19-21 | Stuttering | Without the guarded timeout action, the synchronous replication request never completes, so restart, redetection, removal, and recovery cannot progress. |

## Interpretation

This is a check of a **modeling assumption**, not a historical Rust bug.
`TransportClient` configures a 30-second endpoint timeout and a 5-second
connect timeout in `src/transport/client.rs` (around lines 122 and 163-164).
`replication::replicate_write` converts every RPC error or timeout into a
request error in `src/replication/mod.rs` (around lines 98-127).

`S1TimedOutReplicationFails` models that boundary only when a required target
is down, has restarted beyond the request's target epoch, or its request or
response has been dropped. The passing combined liveness configuration puts
weak fairness on that guarded action; it does not put fairness on the
unguarded `PrimaryFail`.
