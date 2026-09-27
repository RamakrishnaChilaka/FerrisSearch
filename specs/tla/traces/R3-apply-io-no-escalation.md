# R3 apply-I/O escalation liveness counterexample

**Date:** September 27, 2026

**Configuration:** `MC_ApplyStorageReplicaNoEscalation.cfg`

**TLC result:** expected historical temporal-property violation

**Raw trace:** [`R3-apply-io-no-escalation.log`](R3-apply-io-no-escalation.log)

**Raw trace SHA-256:** `6306b7d44bcde52e3b8426a2313a4da09fc298739ee21aa1335af8d8dcce58a4`

TLC generated 65 states, found 53 distinct states, and produced a 19-state
lasso.

## Trace

| TLC state | Model action | Rust behavior represented |
| --- | --- | --- |
| 1-8 | Initial write through `ApplyStoragePrimaryAck` | `TransportService::index_doc` applies write 1 on the primary, both `replicate_doc` calls succeed through `ShardManager::apply_replica_operation`, and the request is acknowledged. |
| 9 | `ApplyStorageFailureOccurs` | The replica's copy remains open and readable, but its mutation path now persistently returns local-storage I/O errors such as ENOSPC or a read-only remount. |
| 10-14 | First post-fault write | The primary accepts write 2, the healthy replica applies it, the affected replica returns a mutation-I/O NACK, and `replication::replicate_write` fails the request rather than making it success-shaped. |
| 15-18 | `ApplyStorageRetryClientWrite` and second failed replication | A new client request becomes write 3. The primary applies it, but the same in-sync failed replica again returns a NACK, so the retry also fails before all required acknowledgements arrive. |
| 19 | Stuttering | In the historical behavior, apply-path I/O was not recorded against the per-copy retry budget. The copy remains in sync, repeated replication attempts fail, `FailShardCopy` is never enabled, and writes never resume. |

## Resolution

`BeginPersistentApplyFailure` keeps `copyExists = TRUE`, distinguishing this
case from an open-level failure. `PrimaryApplyFailure` and
`ReplicaApplyFailure` make every affected mutation fail without applying it.
`EscalatePersistentApplyFailure` represents exhaustion of the shared bounded
retry count/time window and enables an exact-allocation `FailShardCopy`.

`MC_ApplyStorageReplica.cfg` adds weak fairness for escalation, reporting, Raft
commit, and the resumed write. The fixed variant removes the failed replica and
acknowledges write 3. The primary variants use the same escalation: a
leader-selected live highest-checkpoint in-sync candidate is carried in the
command and validated by the state machine, or the report is rejected when no
candidate exists.
