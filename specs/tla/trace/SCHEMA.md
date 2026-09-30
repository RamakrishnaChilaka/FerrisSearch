# D1 implementation trace schema

**Current and only supported version:** `ferrissearch.d1.trace/v4`

Versions 1 through 3 are retired. The converter rejects them. FerrisSearch is
pre-1.0, so emitters and fixtures must move to version 4 without compatibility
shims.

## What validation means

`scripts/tla/trace_to_tla.py` validates the JSON Lines contract, derives finite
constants, and generates `TraceInput.tla`. TLC then searches for a behavior of
the selected trace composition that consumes every observation. Each
composition reuses the applicable D1, authority, collision, or recovery model
actions; there is no single shared D1 `Next` relation.

The converter infers one composition from the event vocabulary:

| Composition | TLC module | Scope |
| --- | --- | --- |
| `d1-core` | `TraceD1.tla` | Primary acceptance, exact-message replica apply, acknowledgements, commit, truncation, crash, restart, and replay. |
| `d1-authority` | `TraceD1Authority.tla` | Crash, election, promotion, routing-view delivery, fencing, activation, and primary-write gating. |
| `d1-combined` | `TraceD1.tla` | Core plus failover, promotion NoOp fill and fan-out, exact in-sync removal, and later-term collision handling. |
| `d1-full` | `TraceD1.tla` | Combined restart/failover behavior plus fresh-allocation peer recovery, exact snapshot and barrier sequence sets, and conditional admission. |
| `d1-collision` | `TraceD1Collision.tla` | Durable fence, definitive sequence collision, and exact in-sync removal. |
| `d1-recovery` | `TraceD1Recovery.tla` | Snapshot installation, pinned WAL suffix, final barrier, conditional membership, and sequence-aware live replication. |

Validation is existential. A pass means that this finite observed execution has
a witness in the bounded model and satisfies that composition's invariants. It
does not prove unlogged Rust executions correct.

The current Rust fault harness covers runs with periodic refresh and automatic
flush disabled (`refresh_interval_ms = 600000` and
`flush_threshold_bytes = u64::MAX`). Explicit commits performed by replay,
activation, recovery, and final copy-state capture are traced, but background
refresh, maintenance/API flush, force merge, and their WAL truncation are not
yet in the supported trace envelope because those tasks do not carry a stable
copy identity into the engine/WAL callbacks. Enabling those maintenance paths
during a traced run can therefore produce an incomplete trace. Extending the
engine-owned trace identity and adding randomized refresh/flush actions is
follow-up work.

`scripts/tla/validate_trace.sh` defaults to a 120-second timeout and a 4 GiB
Java heap. Exit codes are:

- `0`: accepted;
- `1`: rejected; and
- `3`: `INCONCLUSIVE` because TLC timed out, exhausted memory, or did not
  complete normally.

An inconclusive result is neither acceptance nor rejection.

## File framing

The file is UTF-8 JSON Lines. Each line is one JSON object followed by a
newline. Blank lines, logging prefixes, unknown fields, and unknown events are
errors.

Every record contains:

| Field | Type | Rule |
| --- | --- | --- |
| `schema` | string | Exactly `ferrissearch.d1.trace/v4`. |
| `run_id` | non-empty string | Identical on every line. |
| `step` | non-negative integer | Starts at `0` and increases by exactly one. |
| `event` | string | One version-4 event defined below. |

### `trace_start`

The first record is `trace_start` at step 0. Its additional fields are:

| Field | Type | Rule |
| --- | --- | --- |
| `test` | string | Stable test/scenario name. |
| `durability` | string | `request` or `async`. |
| `nodes` | array | Two or three unique `{ "node", "incarnation" }` records. |
| `shard_state` | object | Initial one-shard authority and copy state. |

`shard_state` contains:

- `index_uuid`, `shard`, `primary`, `term`, and `activated`;
- sorted `in_sync` replica node IDs; and
- one `copies` entry per node with `node`, positive `allocation`, `exists`, and
  `fence_term`.

The current validator starts with term 1, an activated primary, every declared
copy present, and every fence at term 1.

### `trace_end`

The final record is:

```json
{
  "schema": "ferrissearch.d1.trace/v4",
  "run_id": "...",
  "step": 42,
  "event": "trace_end",
  "outcome": "completed",
  "quiescent": true,
  "records_before_end": 42
}
```

Every completed trace ends with `copy_state` immediately before `trace_end`.
When `quiescent` is true, no replay may remain active and every copy still
available at the end must have a final `copy_state` after the last
state-changing event.

Rust harness traces also write `<trace-stem>.actual.json`. This sidecar uses
schema `ferrissearch.d1.trace.actual/v1` and the same `run_id`. It contains one
final record per observed copy:

- exact copy identity;
- actual live and retained-delete document identities after refresh; and
- every retained physical WAL entry in on-disk scan order.

The sidecar is evidence, not a source of model choices.
`check_d1_trace_invariants.py` reconstructs document state from trace effects
and physical WAL from `wal_appended` plus `wal_truncated`, then requires exact
agreement with the sidecar. Missing and invented events both fail. Fixtures do
not require a sidecar; every Rust fault-harness run does.

## Shared value contracts

### Operation identity

An operation is identified by `(term, seq_no)`. Every occurrence must retain
the same document ID, operation kind, and `content_hash`.

- `index`: hash the shared recursively key-sorted trace encoding of the logical
  source.
- `delete`: hash the operation kind and document ID.
- `noop`: hash the NoOp reason.

`content_hash` is lowercase hexadecimal SHA-256. Payloads are not logged.
`doc` is JSON `null` only for `noop`.

### Checkpoints

`checkpoints` is exactly:

```json
{
  "processed": 7,
  "persisted": 7,
  "max_seq_no": 9
}
```

Each value is an inclusive sequence number or JSON `null`. `persisted` cannot
exceed `processed`; `processed` cannot exceed `max_seq_no`.

### Receipts, requests, batches, and messages

- `request_id` identifies one client request. It is null only for a promotion
  NoOp.
- `receipt_id` identifies one operation identity from primary acceptance
  through WAL, replica apply, and replay. Every event using one receipt must
  repeat the same term, sequence, operation, document, and content hash.
- `batch_id` identifies one promotion gap-fill batch.
- `message_id` identifies one sequence-target transport attempt. It is never
  reused.

Ordinary `primary_replication_started.required_replicas` is sorted by node and
contains exactly:

```json
{
  "node": "r",
  "allocation": 12,
  "incarnation": 0,
  "message_id": "write/w7/r"
}
```

The set must equal the primary's latest traced in-sync routing view. Promotion
NoOps emit one `promotion_noop_replication_started` for every NoOp/target pair.

`message_phase` records the exact in-flight phase at result or crash
linearization:

- `request`: the request is addressed to the replica;
- `ack`: the replica produced an acknowledgement addressed to the primary;
- `nack`: the replica produced a rejection addressed to the primary; and
- `none`: no message for this attempt remains in flight.

An acknowledged result requires `ack`; a failed result requires `nack`.
`dropped` and `timeout` may name `request`, `ack`, `nack`, or `none` according
to the trace transport state.

### Incarnations

Every send captures `source_incarnation` and target `incarnation` from the
send-time view. A restarted node increments its incarnation by exactly one.
Late messages keep their captured incarnations; they are not rewritten to the
current process incarnation.

## Required trace sink and lock order

Implement version 4 behind the test-only trace feature. Use one
process-global synchronous trace-state mutex that owns:

- the next step;
- the JSONL record buffer;
- request state;
- message IDs and current phases; and
- node incarnations used by the seeded fault harness.

Do not use asynchronous `tracing` output as protocol evidence.

The emitter must not suppress or deduplicate production effects. When a trace
session is active, `record_wal_appended`, `record_operation_processed`, and
`record_operation_collision` fail the operation if no apply scope is active.
Repeated production effects produce repeated records. Every invocation of
`HotEngine::replay_translog_suffix_locked` produces one replay lifecycle,
including activation, refresh/rebuild, startup, and recovery-triggered writer
rebuilds.

For a state mutation:

1. acquire the existing lock that protects the effect;
2. perform the effect;
3. while still holding that lock, acquire the trace-state mutex;
4. update trace-only request/message state and append the complete record with
   the next step; and
5. release the trace-state mutex before releasing the effect lock.

The trace-state mutex never calls production code and never acquires an effect
lock. This one-way order prevents deadlock and makes step order match effect
order. Immutable values may be carried into later records, but an event must
not reread mutable fields under a second lock.

The fault-test scheduler owns a separate operation gate. Crash and
`copy_state` observations hold that gate exclusively so no node effect can
race the observation. They then acquire the trace-state mutex. Ordinary
operations hold the gate in shared mode.

## Event reference and Rust emission contract

The field lists below are in addition to `schema`, `run_id`, `step`, and
`event`.

### Client and primary apply

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `client_write_routed` | `node`, `index_uuid`, `shard`, `request_id`, `target_node`, `doc`, `op`, `content_hash` | `TransportService::index_doc`, `TransportService::delete_doc`, and per-item in `TransportService::bulk_index`, after `validated_primary_write_state` selects the target and before the write-pool task is submitted. | Hold the peer-recovery write guard and the test scheduler's shared operation gate. The routing snapshot and request identity are immutable inputs to the event. |
| `wal_appended` | `node`, `index_uuid`, `shard`, `allocation`, nullable `request_id`, `receipt_id`, `term`, `seq_no`, nullable `doc`, `op`, `content_hash`, `origin`, `durable` | For primary single writes, `HotEngine::add_primary_index_with_side_effect` and `HotEngine::delete_primary_with_side_effect`, immediately after `HotTranslog::append`; for primary bulk, `HotEngine::add_primary_bulk_with_side_effect`, immediately after `write_bulk_with_receipt` and operation construction; for replica, recovery, and promotion writes, `HotEngine::apply_sequenced_batch_locked`, immediately after `append_batch_with_seq`. Emit batch entries in physical append order and before engine mutation. Replay does not append and therefore emits no `wal_appended`. | Hold the engine translog lock and the active apply scope. The replica/recovery/promotion path also holds `apply_state`. `origin` is `primary`, `live_replication`, `recovery`, or `promotion`. |
| `operation_processed` | All operation identity fields above, `origin`, `outcome`, `checkpoints`, optional `batch_max_seq_no` | `HotEngine::apply_sequenced_batch_locked`, after the planner effect, `complete_operation`, and any persisted-checkpoint update. Emit a collision after restoring the rejected planning snapshot. Promotion NoOp fill emits the physical `promotion` processing effect before the summary `promotion_noop_fill`. Replay uses `replay_entry` instead. | Hold `apply_state` and the active apply scope. `checkpoints` is the real engine `SequenceStats`; when present, `batch_max_seq_no` equals its `max_seq_no`. The planner advances the maximum for the whole batch before applying any item, and no reader can observe a partial batch because every sequence-state reader takes `apply_state`. The trace model therefore permits one hidden batch-planning maximum advance before the first observed item while retaining per-item processed and persisted effects. The allowed outcomes are `applied_newer`, `stale`, `redelivery`, `noop`, `collision`, and `apply_failed`. For live transport, atomically change the trace message phase from `request` to `ack` or `nack`. |
| `primary_replication_started` | `node`, `index_uuid`, `shard`, `allocation`, `source_incarnation`, `request_id`, `receipt_id`, `term`, `seq_no`, sorted `required_replicas`, `routing_version` | `replicate_write_with_durability` or `replicate_explicit_batch_with_durability`, after validating the immutable `ClusterState` snapshot and constructing every target request, immediately before spawning RPC futures. | Acquire the trace-state mutex once for the complete target set. Register every `message_id` in phase `request` before any RPC can run. |
| `client_result` | `node`, `index_uuid`, `shard`, `request_id`, `outcome`, nullable `failure_stage` | `TransportService::index_doc`, `delete_doc`, or `bulk_index`, immediately before the terminal response is returned. | Hold the trace-state mutex. `acknowledged` requires every replica in the captured required set to have acknowledged; `failed` names a non-null stage. |

Bulk may append all item WAL entries before processing any item. Emit the
physical order while the one translog critical section remains held.

### Replica transport and fencing

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `replica_received` | `node`, `index_uuid`, `shard`, `allocation`, `source_node`, `source_incarnation`, `message_id`, `receipt_id`, `term`, `seq_no`, `doc`, `op`, `content_hash` | Inside the write-pool closures in `TransportService::replicate_doc` and `TransportService::replicate_bulk`, through `ShardManager::apply_replica_operation`, after UUID/allocation and term validation but before fence persistence, WAL append, or planner mutation. | Hold the per-shard `shard_open_lock`. The attempt must exist in phase `request` and match its send-time source and target incarnations. |
| `replica_rejected` | `node`, `index_uuid`, `shard`, `allocation`, `message_id`, `receipt_id`, `term`, `seq_no`, `reason` | In `TransportService::replicate_doc` and `TransportService::replicate_bulk`, immediately after a routing, recovery-gate, assigned-open, or write-task rejection is known and before returning the RPC result. The write-task path records only messages still in `request`; an item already changed to `nack` by `operation_processed/collision` is not duplicated. | Routing/open checks have just completed their per-shard linearization. Worker rejections are observed before releasing the RPC result, after `ShardManager::apply_replica_operation` releases `shard_open_lock`. Under the trace-state mutex, change the exact message from `request` to `nack` and append the event. `reason` is `quarantined`, `term_fence`, `identity_mismatch`, `recovery_gate`, `copy_unavailable`, `batch_rejected`, or `apply_failure`. |
| `replica_result` | `node`, `index_uuid`, `shard`, `request_id`, `receipt_id`, `message_id`, `replica`, `replica_incarnation`, `outcome`, `message_phase`, nullable `persisted_checkpoint` | On the primary in `replicate_write_with_durability` or `replicate_explicit_batch_with_durability`, immediately after the target RPC resolves and before its result is merged into the request result. | Use the response-carried checkpoint; do not reread replica state. For an acknowledgement, item-local persisted checkpoint ≤ response checkpoint ≤ the replica checkpoint when the primary records the result. Under the trace-state mutex, verify and clear the named phase. |
| `fence_persisted` | `node`, `index_uuid`, `shard`, `allocation`, `term`, nullable `fence_max_seq_no`, `reason` | In `ShardManager::apply_replica_operation` or `ShardManager::raise_copy_fence_blocking`, after `persist_copy_identity` has durably replaced the identity record and before later apply/activation work. | Hold `shard_open_lock`. `reason` is `replication`, `activation`, or `recovery`. |

The replica receive event is intentionally inside the per-shard write task.
Emitting it at gRPC ingress is incorrect because concurrent requests can enter
the write pool in a different order. Every live-replication
`operation_processed` must be preceded by the exact message's
`replica_received` or `promotion_noop_received` event. A
`replica_rejected` event is the only terminal request-phase observation that
does not require a receive event, because it records rejection before apply.

One `ReplicateBulkRequest` still represents one transport result, but trace
state is per sequence-target message. A rejected bulk RPC emits
`replica_rejected` for every item whose message remains in `request`, in wire
order. If the batch-wide planner precheck already emitted one
`operation_processed/collision`, that item remains the collision NACK and the
other items emit `batch_rejected`. The primary then emits one
`replica_result` or `promotion_noop_result` per item, all carrying the RPC's
same outcome and response checkpoint.

### Commit and truncation

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `commit_captured` | `node`, `index_uuid`, `shard`, `allocation`, `commit_id`, `checkpoints`, `term_state` | `HotEngine::commit_writer_at_boundary`, after the successful Tantivy commit and after the immutable `CommittedBoundaryRecord` is derived. | Hold the writer lock and `apply_state`. `term_state` contains `current_term`, nullable `max_seq_no_at_term_start`, and normalized, sorted `processed_in_current_term_below_start_max` ranges. The validator reconstructs these values from fence, processing, commit-persistence, restart, and replay events and requires an exact match. |
| `commit_persisted` | `node`, `index_uuid`, `shard`, `allocation`, `commit_id` | `HotEngine::persist_committed_boundary`, after every successful `CommittedBoundaryRecord::persist` call has fsynced the temporary file, renamed it, and fsynced the parent directory. | Carry the immutable `commit_id`; do not reread checkpoints. Re-persisting the same captured boundary emits another event with the same `commit_id`. |
| `wal_truncated` | `node`, `index_uuid`, `shard`, `allocation`, `truncate_through` | `HotTranslog::truncate_below`, after the rolled manifest and sequence high-watermark are persisted and generation deletion attempts finish. | The translog state mutex linearizes manifest mutation. Emit after deletion attempts so the event describes the completed truncation operation. |

Capture and persistence are separate events because a fence or write may occur
between them. Truncation may not exceed the persisted processed checkpoint.

### Crash, restart, and replay

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `node_crashed` | `node`, `incarnation`, `outcome`, sorted `failed_request_ids`, sorted `dropped_messages` | The seeded fault harness crash function, after it has exclusively stopped ingress and trace-producing tasks for that incarnation. | Hold the scheduler's exclusive operation gate and trace-state mutex. Compute, do not guess, both exact sets from trace state before clearing them. `outcome` is `clean` or `unclean`. |
| `node_restarted` | `node`, new `incarnation`, `index_uuid`, `shard`, `allocation`, `checkpoints` | Inside `HotEngine::new_with_mappings_mode`, after the startup `ApplyState` is initialized from `translog.committed` and immediately before `engine.replay_translog()`. | No replay entry may run first. Allocate the new incarnation and stamp while startup still has exclusive ownership of the engine. |
| `replay_started` | `node`, `index_uuid`, `shard`, `allocation`, `replay_id`, `checkpoints` | Every invocation of `HotEngine::replay_translog_suffix_locked`, immediately after `reset_apply_state_to_commit` and before scanning the WAL. | Hold translog, writer, and `apply_state` locks. Emit even when the scan is empty or was triggered by activation, refresh, rebuild, or recovery rather than process restart. |
| `replay_entry` | `node`, `index_uuid`, `shard`, `allocation`, `replay_id`, `ordinal`, `receipt_id`, operation identity fields, `outcome`, `checkpoints`, optional `batch_max_seq_no` | The replay path through `HotEngine::apply_sequenced_batch_locked`, after one physical WAL entry is classified and its checkpoint effect is complete. | Hold translog and `apply_state`. `checkpoints` is the real engine state and the optional `batch_max_seq_no` must equal its maximum. `ordinal` starts at 0 and follows physical WAL scan order. Outcomes are `skip_committed`, `applied_newer`, `stale`, `redelivery`, or `noop`. |
| `replay_finished` | `node`, `index_uuid`, `shard`, `allocation`, `replay_id`, `outcome` | `HotEngine::replay_translog_suffix_locked`, after successful reader reload and committed-boundary persistence, or after a terminal replay error marks the writer/copy failed. | Emit exactly once per replay ID. `outcome` is `completed` or `failed`. |

`node_restarted` is not a harness observation emitted after open. It must occur
inside engine construction between apply-state restoration and replay.

`node_crashed.dropped_messages` contains sorted unique
`{ "message_id", "message_phase" }` records for every message whose current
destination is the crashed node. `failed_request_ids` is the sorted exact set
of requests in `Replicating` state whose primary is the crashed node.

### Routing, activation, and promotion NoOps

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `routing_view` | `node`, `index_uuid`, `shard`, `primary`, `term`, sorted `in_sync`, every `{node, allocation}`, `initialized` | `ClusterManager::record_protocol_trace_routing_views` at initial harness capture and `ClusterManager::update_state` whenever a node installs a newer view. | Hold that node's cluster-state read/write lock for one coherent view. |
| `routing_promoted` | `emitter`, `index_uuid`, `shard`, `new_primary`, `term`, sorted `in_sync` | `ClusterStateMachine::apply_command_at`, after a successful Raft-applied routing mutation promotes the primary. | Hold `state.write()`. Emit once from the applying Raft state machine, not once per observer. |
| `in_sync_removed` | `emitter`, `index_uuid`, `shard`, `removed_node`, `removed_allocation`, sorted resulting `in_sync` | `ClusterStateMachine::apply_command_at`, after successful exact-allocation `FailShardCopy` application. | Hold `state.write()`. The allocation must be the one removed by that command. |
| `promotion_noop_fill` | `node`, `index_uuid`, `shard`, `allocation`, `batch_id`, `term`, sorted `noops`, `checkpoints` | `HotEngine::prepare_primary_activation`, after full local replay, gap computation, NoOp WAL append, translog sync, planner completion, and persisted-checkpoint marking. | Hold the activation maintenance guard, translog lock, and `apply_state`. Each `noops` item is `{receipt_id, seq_no, content_hash}`. |
| `primary_activated` | `node`, `index_uuid`, `shard`, `allocation`, `term` | `TransportService::prepare_local_primary_activation`, immediately after inserting the term into `primary_activation_state.activated_terms`. | Hold `activated_terms.write()` and the peer-recovery exclusive guard. Emit before dropping the guard and before NoOp fan-out. |
| `promotion_noop_replication_started` | `node`, `index_uuid`, `shard`, `allocation`, `source_incarnation`, `batch_id`, `receipt_id`, `message_id`, `term`, `seq_no`, `content_hash`, `replica`, `replica_allocation`, `replica_incarnation` | `replicate_noop_batch_with_durability` through `replicate_explicit_batch_with_durability`, after constructing the exact per-target RPC and immediately before it can run. | Under the trace-state mutex, register one request-phase message for each sequence/target pair. Use the immutable validated routing snapshot. |
| `promotion_noop_received` | `node`, `index_uuid`, `shard`, `allocation`, `source_node`, `source_incarnation`, `batch_id`, `receipt_id`, `message_id`, `term`, `seq_no`, `content_hash` | The NoOp branch of `TransportService::replicate_bulk`, through `ShardManager::apply_replica_operation`, at the same boundary as `replica_received`. | Hold `shard_open_lock`, after identity/term validation and before fence/WAL/planner mutation. |
| `promotion_noop_result` | `node`, `index_uuid`, `shard`, `allocation`, `batch_id`, `receipt_id`, `message_id`, `term`, `seq_no`, `replica`, `replica_incarnation`, `outcome`, `message_phase`, nullable `persisted_checkpoint` | `replicate_explicit_batch_with_durability`, immediately after the target result resolves. | Use the response-time checkpoint and clear the exact trace message phase under the trace-state mutex. |

For a faithfully emitted non-empty fill, the required source order is:

1. every promotion-origin `wal_appended` in physical batch order;
2. every promotion-origin `operation_processed`;
3. `promotion_noop_fill` after explicit sync and persisted-checkpoint marking;
4. `primary_activated`;
5. all `promotion_noop_replication_started` events for the validated send-time
   routing snapshot; and
6. each replica's receive, optional `fence_persisted`, WAL/process or collision,
   and result events.

Activation does not depend on successful NoOp fan-out. A failed NoOp may cause
exact in-sync removal. Fill does not create transport messages. The first send
captures the then-current validated local routing view; every NoOp in the batch
must have exactly one send to every replica in that snapshot. A primary crash
before the snapshot is taken may leave the batch unsent. A replica crash does
not remove an already selected target and therefore may produce a later
`dropped/request` result with no crash-time message loss.

### Recovery

| Event | Required fields | Emit in current Rust code | Required lock and ordering |
| --- | --- | --- | --- |
| `recovery_snapshot` | `source_node`, `target_node`, `index_uuid`, `shard`, `session_id`, `snapshot_next_seq_no`, sorted `processed_seqs`, sorted semantic `documents` | `HotEngine::prepare_peer_recovery_snapshot` copies the processed set and one refreshed reader snapshot while capturing the committed files; `TransportService::launch_source_setup` emits the immutable values when installing the ready source session. | Hold the engine maintenance/translog boundary while copying the values, then hold the source recovery registry mutex while publishing the session and event. The validator freezes the source's reconstructed primary-term sequence state at this event. |
| `recovery_started` | `source_node`, `target_node`, `index_uuid`, `shard`, `allocation`, `session_id` | `ShardManager::prepare_peer_recovery_target_blocking_traced`, after the exact-allocation target install marker is fsynced and renamed. | Hold the target's per-shard `shard_open_lock`. |
| `recovery_installed` | Same identities plus `snapshot_next_seq_no` | `ShardManager::finalize_peer_recovery_target_blocking_traced`, after verified snapshot installation, durable identity replacement, marker removal, and strict target open. | Hold the target's per-shard `shard_open_lock`. The target inherits the term-sequence state frozen by the matching `recovery_snapshot`. |
| `recovery_barrier` | Same identities plus `barrier_next_seq_no` and exact sorted `processed_seqs` | `node::peer_recovery::run_peer_recovery`, after suffix replay reaches the pinned source barrier and the target checkpoint matches the response. | Read the target processed set under its apply-state mutex while the source admission barrier remains held. |
| `recovery_membership` | Same identities plus `outcome` | `TransportService::settle_peer_recovery` after observing admission, promotion, or rejection; `node::peer_recovery::run_peer_recovery` records `unknown` after its bounded completion wait. | Keep the source session in settlement state until a source verdict is emitted. The target emits `unknown` before releasing its pending target state. Allowed values are `admitted`, `promoted`, `rejected`, and `unknown`. |

Recovery remains a separate composition. The target is unavailable until
admitted or promoted. A promoted target still requires primary activation.

### `copy_state`

`copy_state` fields are `node`, `index_uuid`, `shard`, `allocation`, `reason`,
and sorted `documents`.

`reason` is `quiescent`, `replay`, `admission`, or `trace_end`. Each document
contains:

```json
{
  "doc": "d1",
  "state": "live",
  "seq_no": 7,
  "term": 2,
  "content_hash": "..."
}
```

`state` is `absent`, `live`, or `deleted`. Identity fields are null only for
`absent`.

`ShardManager::capture_protocol_trace_copy_state` performs the refresh and
captures the immutable live-document snapshot, retained delete versions, and
physical WAL while the copy remains quiescent. `record_copy_snapshot` stores
that independent evidence in the actual-state sidecar and emits `copy_state`.
The harness emits all final `copy_state` records only after every available
copy has been captured, so a later copy's commit events cannot make an earlier
observation stale.

The fault harness must:

1. hold the scheduler's exclusive operation gate;
2. call `SearchEngine::refresh` (`HotEngine::refresh_with_pruned_tombstones`);
3. use one reader snapshot to enumerate live documents;
4. capture retained delete identities from `LiveVersionMap` and physical WAL
   entries from the live `WriteAheadLog`;
5. emit `copy_state` from live data plus trace-known absent/delete identities;
6. write the independent actual-state evidence; and
7. run `check_d1_trace_invariants.py --completeness-only` before declaring the
   harness run successful.

Refreshing first is mandatory. A pre-refresh read is not valid evidence.

## Converter-only checks

Before TLC, `trace_to_tla.py` rejects, among other structural failures:

- any schema other than v4;
- unknown or missing fields, non-consecutive steps, and malformed hashes;
- changed `(term, seq_no)` content identity;
- stale or reused message IDs, wrong send-time incarnations, and mismatched
  message phases;
- required replica sets that differ from the primary's traced routing view;
- request-durability acknowledgement without durable WAL on the primary and
  every required replica;
- replica apply or acknowledgement without the exact transport attempt;
- failed replica results without a matching collision/apply-failure or
  `replica_rejected` transition to `nack`;
- NoOp batches missing any sequence/target send from the validated post-
  activation snapshot while the primary remains able to send;
- crash lost sets that differ from exact trace transport/request state;
- malformed replay ordinals, unknown replay receipt IDs, or lifecycle pairs;
- commit persistence without a captured immutable boundary;
- quiescent traces with active replay or pending messages; and
- traces without final `copy_state` for each available copy.

The generated trace modules compose observations with the applicable D1,
authority, collision, and recovery actions. Outcome labels and converter
bookkeeping do not replace model transitions.

## Version 4 changes

- Added exact ordinary and promotion-NoOp message IDs and send-time
  incarnations.
- Added primary-side NoOp sends, replica NoOp receipt, apply/collision, result,
  and fence evidence.
- Added exact crash `failed_request_ids` and phase-qualified
  `dropped_messages`.
- Added exact NoOp fill batches and replay `receipt_id`s.
- Split promotion fill, activation, and send-time NoOp message creation.
- Added faithful promotion-fill WAL/process effects, alive-copy replay, and
  idempotent repeated persistence of one captured commit.
- Directed hidden promotion, activation, view-delivery, failure-removal, replay,
  and transport actions from emitted evidence rather than unconstrained
  choices.
- Removed version-3 support.
