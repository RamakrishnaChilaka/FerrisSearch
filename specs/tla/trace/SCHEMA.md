# D1 implementation trace schema

**Status:** version 1 is implemented by `TraceD1.tla`,
`scripts/tla/trace_to_tla.py`, and `scripts/tla/validate_trace.sh`. Rust
instrumentation will emit this schema in a later implementation commit.

This schema defines the events that instrumented Rust tests must emit so TLC can
check whether the observed execution is a behavior permitted by the D1 model.
It follows the partial-observation approach of Cirstea et al.,
*Validating Traces of Distributed Programs Against TLA+ Specifications*
(arXiv:2404.16075): a trace records selected state updates, not a complete
implementation-state snapshot, and TLC searches for values and transitions of
unobserved model state between those updates.

Passing trace validation means that the logged finite execution can be embedded
in a behavior of the model. It is not an unbounded proof, does not establish
that the instrumentation itself is correct, and does not replace the bounded
model checks or Rust result-level tests.

## Version 1 scope

Version 1 traces:

- cover one `local_shards` index UUID and one shard per trace;
- may contain two or three nodes and concurrent writes;
- record one event per logical bulk item rather than one event for the bulk
  envelope;
- use exact allocation IDs, primary terms, and sequence numbers;
- support index, delete, and promotion-generated NoOp operations;
- support request-durable and async-durable WAL modes, although the first D1
  validation tests use request durability;
- may start from an empty shard or from a fully processed, persisted NoOp
  prefix declared by `initial_prefix_through`; and
- validate only operations and acknowledgements recorded in the trace.

The one-shard restriction is a trace-adapter bound, not a claim that Rust serves
only one shard. A later schema version may lift it without changing the event
meanings below.

## JSON Lines framing

The trace is UTF-8 JSON Lines. Each line is exactly one JSON object. The first
record is `trace_start`; the last is `trace_end`. No text, tracing prefix, or
partially serialized line may appear in the file.

Every record contains:

| Field | Type | Meaning |
|---|---|---|
| `schema` | string | Always `"ferrissearch.d1.trace/v1"`. |
| `run_id` | string | Stable UUID or other unique test-run identifier. |
| `step` | non-negative integer | Process-global total-order stamp. |
| `event` | string | One of the event names defined below. |

`step` starts at zero and is strictly increasing with no duplicates. The writer
must serialize records in `step` order. The converter rejects a missing
`trace_start`, missing `trace_end`, duplicate/out-of-order steps, unknown event,
unknown outcome, or malformed required field.

### `trace_start`

`trace_start` declares the finite universe and the state from which trace
matching begins:

```json
{
  "schema": "ferrissearch.d1.trace/v1",
  "run_id": "b7be08f1-dfc0-4e93-a998-7b8210a0f7fd",
  "step": 0,
  "event": "trace_start",
  "test": "concurrent-delete-late-index",
  "durability": "request",
  "initial_prefix_through": null,
  "nodes": [
    {"node": "n1", "incarnation": 0},
    {"node": "n2", "incarnation": 0}
  ],
  "shard_state": {
    "index_uuid": "idx-uuid",
    "shard": 0,
    "primary": "n1",
    "term": 1,
    "activated": true,
    "in_sync": ["n2"],
    "copies": [
      {
        "node": "n1",
        "allocation": 11,
        "exists": true,
        "fence_term": 1,
        "fence_max_seq_no": null,
        "checkpoints": {
          "processed": null,
          "persisted": null,
          "max_seq_no": null
        }
      },
      {
        "node": "n2",
        "allocation": 12,
        "exists": true,
        "fence_term": 1,
        "fence_max_seq_no": null,
        "checkpoints": {
          "processed": null,
          "persisted": null,
          "max_seq_no": null
        }
      }
    ]
  }
}
```

`initial_prefix_through` is either `null` or an inclusive sequence number. A
number `N` means every existing copy has already processed and persisted
sequences `0..N`, those entries have no logical document effect, and they may
have been truncated from the WAL. This permits compact term-collision fixtures
that begin with maximum sequence 10 without inventing ten user documents. Copy
checkpoint and fence fields must agree with that prefix.

Version 1 otherwise starts with no document values, no retained WAL entries, no
active requests, and no messages in flight. Tests needing other pre-existing
logical state must emit its creating operations instead of hiding it in the
header.

### `trace_end`

`trace_end` is written only after the test has stopped trace-producing work and
flushed the synchronous trace sink:

```json
{
  "schema": "ferrissearch.d1.trace/v1",
  "run_id": "b7be08f1-dfc0-4e93-a998-7b8210a0f7fd",
  "step": 42,
  "event": "trace_end",
  "outcome": "completed",
  "quiescent": true,
  "records_before_end": 42
}
```

`records_before_end` must equal the number of preceding records.
`quiescent=true` means every routed client write has a terminal client result,
no replica RPC is still awaiting a result, no replay is active, and no recovery
is between start and membership settlement. Non-quiescent completed prefixes
may be validated with `quiescent=false`, but they cannot be used as evidence for
the model's quiescent-convergence property.

## Protocol event fields

Every protocol event contains the following keys. A key that does not apply to
that event is present with JSON `null`; this makes accidental instrumentation
omissions distinguishable from an intentionally unobserved value.

| Field | Type | Meaning |
|---|---|---|
| `node` | string | Node on which the observed effect occurs. |
| `incarnation` | integer | Node incarnation; incremented by each successful restart. |
| `index_uuid` | string | Exact index UUID, never the index name. |
| `shard` | integer | Shard ID. |
| `allocation` | positive integer or `null` | Exact allocation ID of `node` for this effect. |
| `peer` | string or `null` | Remote/source/target node when the event crosses nodes. |
| `peer_incarnation` | integer or `null` | Incarnation captured for the peer, when known. |
| `peer_allocation` | positive integer or `null` | Exact allocation ID of the peer copy, when relevant. |
| `request_id` | string or `null` | Unique logical client item ID, stable from routing through client result. |
| `term` | positive integer or `null` | Primary term carried by or installed for the effect. |
| `seq_no` | non-negative integer or `null` | Exact operation sequence number. |
| `doc` | string or `null` | Document ID; `null` only for NoOps or non-operation events. |
| `op` | string or `null` | `"index"`, `"delete"`, `"noop"`, or `null`. |
| `content_hash` | string or `null` | Lowercase SHA-256 of the canonical logical operation content. |
| `origin` | string or `null` | `"primary"`, `"live_replication"`, `"replay"`, `"recovery"`, or `"promotion_noop_fill"`. |
| `outcome` | string | Event-specific outcome from the tables below. |
| `checkpoints` | object | Inclusive post-effect checkpoints, with the shape below. |

Checkpoint objects always contain:

```json
{
  "processed": null,
  "persisted": null,
  "max_seq_no": 2
}
```

Each value is either `null` or an inclusive sequence number, matching
`SequenceStats`. Thus `processed=null` means no contiguous processed prefix,
whereas `processed=0` means sequence zero is processed. The converter maps an
inclusive Rust checkpoint `N` to the model's exclusive boundary `N + 1`.

`request_id` is assigned by the test/transport request context before routing.
For bulk requests use one ID per item, for example `bulk-7/0`, `bulk-7/1`.
It need not be stored in the WAL or sent over gRPC after `(term, seq_no)` has
been assigned. It is never reused; a client retry is a new logical request even
when its document and payload are identical.

`content_hash` distinguishes a valid redelivery from different content using
the same `(term, seq_no)`. All emitters must share one trace-only canonical
encoder over:

```text
(op, doc-or-null, recursively-key-sorted JSON source-or-null, noop-reason-or-null)
```

Payload bytes are not logged. Equal logical operations produce the same hash
even if their input JSON object key order differs.

## Required events and linearization points

### Client and primary write events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `client_write_routed` | `node`, `peer`, `peer_allocation`, `request_id`, `term`, `doc`, `op`, `content_hash` | `routed` | In the document/bulk/delete coordinator after selecting a routing-view primary and exact allocation, before forwarding or entering the primary handler. The captured term is that routing view's term. | `D1ClientWrite`; the chosen `peer` is `writeTarget`. |
| `primary_assigned` | `node`, `allocation`, `request_id`, `term`, `seq_no`, `doc`, `op`, `content_hash`, `origin="primary"`, `routing_version`, `required_replicas` | `assigned` | In `TransportService::{index_doc, bulk_index, delete_doc}` immediately after the successful local write receipt and before starting replica fan-out. The earlier `wal_appended` and `operation_applied` records are the sequence-assignment and local-apply linearization points. Emit one record per bulk item in sequence order. | Completes the fixed trace macro corresponding to `D1PrimaryAccept`, including its captured `writeRequired` set and creation of replication messages. |
| `client_result` | `node`, `request_id`, plus operation identity when assignment occurred | `acknowledged`, `failed` | In `TransportService::{index_doc, bulk_index, delete_doc}` immediately before constructing the terminal client response. An acknowledged result is emitted only after every required replica result is acknowledged. A failed post-WAL write retains `term` and `seq_no`; a pre-assignment failure leaves them `null`. | `D1PrimaryAck`, `PrimaryFail`, or `PrimaryReject`. |

`required_replicas` is the exact set captured from the primary's validated
routing view, encoded as an array sorted by node ID:

```json
[
  {"node": "n2", "incarnation": 0, "allocation": 12}
]
```

It may be empty. `routing_version` is the captured `ClusterState.version`.
These fields are required because the authoritative replica set may change
while a write is in flight; trace validation must not reconstruct required
acknowledgements from a later routing state.

`client_result` also contains `failure_stage`, which is `null` for
`acknowledged` and one of `routing`, `activation`, `validation`,
`primary_apply`, or `replication` for `failed`. A failed client operation that
already reached the primary WAL remains visible to replay; it is not converted
into an acknowledged operation by the trace adapter.

### WAL and planner events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `wal_appended` | `node`, `allocation`, `term`, `seq_no`, `op`, `content_hash`, `origin` | `appended` | At the engine call site immediately after `HotTranslog::{append, write_bulk_with_receipt, append_batch_with_seq}` has written the full frame, performed the configured sync, updated generation metadata/high-watermark, and returned, while the outer translog critical section is still held. Emit one record per batch entry in physical WAL order. | The `walOrder`, durable-operation, and maximum-sequence part of `D1PrimaryAccept`, `D1FixedReplicaProcess`, recovery apply, or NoOp fill. |
| `operation_applied` | Operation identity, `origin`, checkpoints, `operation_processed`, `operation_persisted` | `applied_newer`, `stale`, `redelivery`, `noop`, `collision`, `apply_failed` | In `HotEngine::apply_sequenced_batch_locked`, per operation, after the logical mutation/no-mutation decision and after `ApplyState::complete_operation` has updated term identity and checkpoints, while `apply_state` is still locked. For `collision`, emit after restoring the planning snapshot and before returning the error. For `apply_failed`, emit after the WAL-backed writer is marked failed and before returning the error. | `D1PrimaryAccept`, `D1FixedReplicaProcess`, `D1FixedReplicaRedelivery`, `D1FixedReplayApply`, B1 collision failure, B2/B4 NoOp fill, or recovery apply. |
| `checkpoint_changed` | `node`, `allocation`, `checkpoints`, `previous_checkpoints`, `cause` | `changed`, `restored` | While `apply_state` is still locked, immediately after a successful checkpoint tuple change or restart reset. Do not emit when all three values are unchanged. | Observation of `processedSeqs`, `processedNext`, `persistedProcessedNext`, and `maxSeqNext`; normally part of the same model action as the preceding operation/commit/restart. |

`wal_appended` additionally contains `durable`, a boolean. It is true only
after the configured durability requirement for that append has completed.
Request-durable D1 tests require it to be true before any acknowledgement for
that operation.

`operation_processed` and `operation_persisted` describe the specific sequence,
not whether the aggregate checkpoint has reached it. For example, a replica may
log sequence 2 as processed and persisted while both aggregate checkpoints are
still `null` because sequences 0 and 1 are missing. This is how the trace keeps
replica gaps from incorrectly blocking acknowledgement of later operations.

For `operation_applied`:

- `applied_newer` is Rust `ApplyOutcome::Applied` and mutates the logical
  document to this newer index/delete operation;
- `stale` records and processes the operation but leaves a newer document or
  tombstone unchanged;
- `redelivery` performs no second WAL append and no second logical mutation;
- `noop` records and processes a NoOp without document state;
- `collision` is a definitive `(term, seq_no)` identity collision and must not
  produce a replica acknowledgement; and
- `apply_failed` means the WAL entry may exist but the operation did not become
  processed.

### Replica transport events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `replica_received` | `node`, `allocation`, `peer`, `peer_incarnation`, `term`, `seq_no`, `doc`, `op`, `content_hash`, `origin="live_replication"` | `accepted`, `rejected` | In `TransportService::{replicate_doc, replicate_bulk}` after decoding and validating the request envelope and initial UUID/allocation routing, before submitting the per-shard write task. Emit one record per bulk operation. | Makes the existing `Replicate` message eligible for delivery. Actual delivery/apply order is fixed by `operation_applied`, not by handler-arrival order. |
| `replica_result` | `node`, `allocation`, `peer`, `peer_allocation`, `term`, `seq_no`, `doc`, `op`, `content_hash` | `acknowledged`, `failed`, `timeout`, `dropped` | On the primary in `replication::{replicate_write_with_term, replicate_bulk_with_term}` immediately after each target RPC resolves and before the fan-out join result is returned to the primary handler. | `acknowledged` maps to `D1DeliverAck`; other outcomes permit message loss/failure and leave the client write unacknowledged. |

For `replica_received` outcome `rejected`, include `rejection` from this closed
set: `uuid`, `allocation`, `term`, `recovery_install`, `validation`, or
`storage`. Revalidation inside the write-pool closure may turn an initially
accepted receive into a failed `replica_result`; no apply event is emitted when
no planner effect occurred.

The authoritative planner order is the order of `operation_applied` events
inside the per-copy apply critical section. `replica_received` records network
arrival only. This distinction is required because concurrent gRPC handlers can
arrive in one order and acquire the shard planner lock in another.

### Fence, commit, and truncation events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `fence_persisted` | `node`, `allocation`, `term`, `fence_max_seq_no` (nullable value, present key) | `raised` | In `ShardManager::{apply_replica_operation, raise_copy_fence_blocking}` after `SHARD_COPY_IDENTITY` and its parent directory are durable, while the per-shard open lock is still held and before the triggering operation can be served. | Durable-fence actions including `B1RRaiseFence`; establishes collision state across restart. |
| `commit_persisted` | `node`, `allocation`, `term`, `checkpoints`, `term_state`, `commit_context` | `persisted` | After the successful Tantivy commit boundary has been atomically persisted by `CommittedBoundaryRecord::persist`, including file and parent-directory sync, and before any WAL truncation based on it. | `D1CommitReplica` or the replay commit portion of `D1FixedReplayApply`. |
| `wal_truncated` | `node`, `allocation`, `truncate_through`, `retained_min_seq_no`, `retained_max_seq_no` | `completed` | After `HotTranslog::truncate_below` has persisted its manifest and sequence high-watermark and completed generation deletion attempts, before returning. | `D1TruncateToProcessedCheckpoint`. Extra old entries retained because a generation is mixed are permitted; replay events reveal what remains. |

`fence_max_seq_no` is the maximum of the engine sequence state and WAL maximum
captured for the new term. It is `null` only for a truly empty copy. The event
must be emitted for the durable identity write even if the process crashes
before the in-memory term-sequence state is reconciled; restart is required to
restore from this event's durable identity.

`term_state` has this exact shape:

```json
{
  "current_term": 2,
  "max_seq_no_at_term_start": 11,
  "processed_in_current_term_below_start_max": [
    {"start": 7, "end": 8}
  ]
}
```

`commit_context` is one of `refresh`, `flush`, `force_merge`, `replay`, or
`recovery`. `truncate_through` is inclusive. The retained minimum and maximum
are nullable when the WAL contains no retained operation.

### Crash, restart, and replay events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `node_crashed` | `node`, `incarnation` | `unclean`, `clean` | In the in-process test harness after ingress to the old node is fenced and its trace-producing tasks can no longer emit, at the exact point the node becomes unavailable. No later event may use that `(node, incarnation)`. | `D1CrashReplica` or `CrashNode`. |
| `node_restarted` | `node`, new `incarnation` | `started` | After constructing the new node incarnation and installing its durable directories, but before accepting traced client/replica traffic. | `D1RestartReplica` or `RestartNode`; volatile state is reset. |
| `replay_started` | `node`, `allocation`, `term`, `checkpoints`, `replay_id` | `started` | In `HotEngine::replay_translog_suffix_locked` after loading the committed boundary and durable copy identity and resetting apply state, before scanning the first WAL entry. | Establishes `replaying`, `replayBoundary`, and replay cursor. |
| `replay_entry` | Operation identity, `origin="replay"`, checkpoints, `replay_id`, `replay_ordinal` | `skip_committed`, `applied_newer`, `stale`, `redelivery`, `noop`, `collision`, `apply_failed` | Once for every physical WAL entry in scan order. Emit `skip_committed` immediately after the committed-boundary decision. Emit all other outcomes after that entry has passed through the same planner and its effect/error is final. | `D1ReplaySkip` or `D1FixedReplayApply`. |
| `replay_finished` | `node`, `allocation`, `term`, `checkpoints`, `replay_id`, `entries_examined` | `completed`, `failed` | After final replay commit/boundary persistence and reader reload for `completed`, or immediately before returning the terminal replay error for `failed`. A copy is not available before `completed`. | `D1FinishReplay`; failed replay leaves the copy unavailable. |

`replay_ordinal` starts at zero and increases by one for each physical entry
examined, including skipped entries. A batched replay emits per-entry records in
the original WAL iteration order after the batch planner succeeds. Replay must
not synthesize `wal_appended` events: the entries are already in the local WAL.

The restart event alone does not claim that a shard is available. Availability
begins only after any required `replay_finished(completed)`, promotion gap fill,
and primary activation.

### Promotion, NoOp fill, and activation events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `routing_promoted` | `node`, `allocation`, `peer` (old primary), `term` (new term) | `committed` | In the Raft state-machine apply path after the exact-allocation routing update and term increment are committed to `ClusterState`, before publishing the successful command result. | `B1PromoteR1`, `B1PromoteR2`, or the promotion part of `PromoteReplica`. |
| `primary_activated` | `node`, `allocation`, `term`, `checkpoints` | `activated` | In `TransportService::ensure_primary_activated` after WAL replay, any promotion NoOp fill, the activated-term Raft observation, the durable fence raise, and insertion into `activated_terms`, before returning an `ActivatedPrimary`. | `B4ActivatePrimary` or `ActivatePrimary`. |

A promotion gap fill uses the ordinary pair:

1. `wal_appended` with `op="noop"` and
   `origin="promotion_noop_fill"`; then
2. `operation_applied` with `outcome="noop"` and the same identity.

Those records are the NoOp-fill event; there is no second, redundant
`noop_filled` record. They must follow `replay_finished(completed)`. Failed NoOp
replication is represented by `replica_result(failed|timeout|dropped)` and does
not prevent `primary_activated`.

### Peer recovery events

| Event | Required fields | Outcomes | Emit after, before releasing/responding | Model mapping |
|---|---|---|---|---|
| `recovery_started` | target `node`/`allocation`, source `peer`/`peer_allocation`, `term`, `session_id`, `snapshot_next_seq_no` | `started` | In `node::peer_recovery::run_peer_recovery` after the source session has returned its snapshot boundary and the target's exact-allocation install marker is durable, immediately after `prepare_peer_recovery_target_blocking` succeeds. | Fixed macro `SourceSnapshot` then `TargetBeginInstall`. |
| `recovery_installed` | target/source identity, `term`, `session_id`, `snapshot_next_seq_no`, `barrier_next_seq_no`, checkpoints | `pending_membership` | After file install, ordered recovery-operation apply, final write barrier, refresh, and durable awaiting-membership marker, immediately after `mark_peer_recovery_awaiting_membership_blocking` succeeds. | `InstallSnapshot`, zero or more `FetchOps`/`ApplyOps`, `FinishFinalizeTail`, then `TargetComplete`. |
| `recovery_membership` | target/source identity, `term`, `session_id` | `admitted`, `promoted`, `rejected`, `unknown` | After `complete_with_observed_settlement` or restarted-pending observation returns. Emit before destructive rejection cleanup or admitted-marker cleanup. | `TargetObserveAdmitted`, `TargetObserveRejected`, or an allowed pending stutter. |

Recovery-applied WAL operations also emit `wal_appended` and
`operation_applied(origin="recovery")`. `recovery_membership=unknown` leaves the
copy unavailable. Only `admitted` or `promoted` makes the target an available
copy for `NoCopyBehindAcked`.

## Ordering and trace-writer requirements

The tests run multiple nodes in one process with real gRPC. Version 1 therefore
uses one process-global trace sink:

1. Acquire the trace-sink mutex.
2. Allocate `step` from a process-global `AtomicU64`.
3. Serialize and append the complete JSON line synchronously to the in-memory
   trace buffer.
4. Release the trace-sink mutex.

At test completion the buffer is written and synced as one artifact before
`trace_end` is considered durable. Do not use the normal asynchronous tracing
subscriber for protocol records: filtering, batching, dropped records, and
cross-thread reordering would invalidate the total order.

Each event is stamped **after** its named effect and, for shard-local durable or
planner effects, **before** releasing the lock that linearizes that effect.
The trace sink must never call back into shard, WAL, Raft, or transport code, so
holding an implementation lock while appending an in-memory record cannot
create a lock cycle.

The global order must preserve these causal edges:

- route before assignment or pre-assignment client failure;
- WAL append before the corresponding successful planner outcome;
- fence persistence before an operation accepted under the newer term;
- planner outcome before replica acknowledgement;
- all required replica acknowledgements before client acknowledgement;
- commit-boundary persistence before truncation;
- crash after the final event of the old incarnation;
- restart before replay of the new incarnation;
- replay completion before promotion NoOp fill;
- promotion NoOp fill before activation; and
- recovery start before install, and install before membership admission.

Independent concurrent effects may be ordered either way by the sink. That is
safe because TLC searches for a behavior matching the recorded total order.

## Unobservable model state

The Rust trace deliberately does not expose every TLA+ variable:

- `messages`, delivery delay, and drops are reconstructed from
  `replica_received` and `replica_result`;
- lagging per-node Raft views, queued conditional commands, and Raft log
  positions are existentially chosen between committed routing/fence events;
- worker-pool queues, Tokio scheduling, shared write holders, exclusive recovery
  barriers, and retention-pin IDs are unobserved;
- retry counters, timers, failure-detector samples, and transient network
  partitions are unobserved unless their protocol result is logged;
- `processed_above` and `persisted_above` interval-tree representation is
  hidden; per-operation processed/persisted booleans plus aggregate checkpoints
  constrain their semantic contents;
- Tantivy segment layout, version-map cache entries, tombstone-retention
  metadata, and merge state are hidden; planner outcomes constrain logical
  document state;
- WAL generation numbers, file offsets, and retained extra entries within a
  mixed generation are hidden; append order, replay order, and truncation
  observations constrain semantic WAL behavior;
- payloads are abstracted to `content_hash`; and
- recovery file manifests and byte transfer are abstracted to snapshot and
  barrier boundaries.

`TraceD1.tla` must treat the recorded events as an ordered subsequence of a full
model behavior. Between adjacent records TLC may take unobserved D1, Raft,
message, failure, or recovery actions, plus stuttering steps, provided they do
not contradict the next recorded update. An observed event maps to the one
action or fixed action macro named above; the validator may not reorder,
discard, or reinterpret it to make a trace pass.

Absence of an event is not evidence that an unobservable action did not occur.
Conversely, events identified above as mandatory effects cannot be inserted by
TLC when the implementation omitted them. For example, an acknowledged replica
operation requires its logged planner outcome, and a restart collision check
requires the logged durable fence maximum.

## Schema-level rejection rules

The converter rejects a trace before TLC when:

- a required identity field is `null`, zero, or inconsistent with
  `trace_start`;
- a `(term, seq_no)` changes document, operation kind, or `content_hash` across
  redelivery;
- a node emits after `node_crashed` in the same incarnation;
- an incarnation does not increase exactly once at restart;
- `wal_appended` is duplicated for a redelivery;
- a replica acknowledgement has no earlier accepted receive and successful
  planner outcome for that operation;
- a client acknowledgement lacks an acknowledgement from every replica that
  was authoritative in the primary's captured routing state;
- checkpoint values decrease except at a declared restart restore;
- `persisted > processed`, or either checkpoint exceeds `max_seq_no`;
- truncation precedes its persisted commit boundary;
- replay ordinals are missing, duplicated, or out of order;
- a promotion NoOp is filled before replay completes;
- activation precedes replay/NoOp completion; or
- a recovery target is treated as available before admitted/promoted
  membership.

These structural checks protect TLC from malformed input; they do not replace
the model checks. The trace specification must still reject semantically invalid
but well-formed histories, including:

- arrival-order application that lets a late older operation replace a newer
  document or tombstone;
- sequence-only redelivery that accepts a newer-term collision; and
- replay beginning after the highest observed/committed sequence instead of
  after the persisted gap-aware processed checkpoint.
