# D1 implementation trace schema

**Current version:** `ferrissearch.d1.trace/v2`

Version 1 is retired and rejected by the converter. Rust instrumentation has
not shipped yet, so version 2 is the first implementation contract.

## What validation means

The converter checks the JSON Lines contract and generates a finite
`TraceInput.tla` plus TLC constants. Validation is existential:

- an observed event either constrains a real model action or a model
  stuttering step;
- TLC may take a bounded number of real, profile-specific hidden actions
  between observations; and
- the trace is accepted only when TLC finds a behavior that consumes every
  observation.

The validator does not contain a second implementation of the D1 planner.
Different profiles compose with the owning model actions:

| Profile | TLC module | Exact actions used |
| --- | --- | --- |
| `d1-core` | `TraceD1.tla` | `MC_D1_SeqNoApply`: client routing, D1 primary acceptance, D1 replica processing/redelivery, acknowledgements, captured-boundary persistence, restart, replay, replay failure, and truncation. |
| `d1-authority` | `TraceD1Authority.tla` | `Invariants`: crash, election, routing promotion, per-node view delivery, activation proposal/commit/observation, and primary write gating. |
| `d1-collision` | `TraceD1Collision.tla` | `MC_D1_TermCollision`: partial old-term apply, promotion, durable collision fence, definitive collision, and in-sync removal. |
| `d1-recovery` | `TraceD1Recovery.tla` | `PeerRecovery` and base replication: source snapshot, target install, fetched catch-up operation, finalize barrier, Raft admission, and target observation. |

The recovery control-plane projection uses `PeerRecovery::ApplyOps`, whose
operations are strictly increasing. The D1 per-document planner is not jointly
instantiated in that projection. The checked-in
`valid-recovery-planner-sample.jsonl` separately sends the same ordered
newer-operation pattern through the real `MC_D1_SeqNoApply` actions. This is a
bounded sample, not a general refinement proof.

A pass means that the finite observed execution has a witness in these bounded
models. It does not prove the Rust implementation, the instrumentation, or
unlogged executions correct.

## File framing

The file is UTF-8 JSON Lines. Every line is one JSON object. There are no blank
lines or logging prefixes.

Every record contains:

| Field | Type | Rule |
| --- | --- | --- |
| `schema` | string | Exactly `ferrissearch.d1.trace/v2`. |
| `run_id` | string | Identical on every line. |
| `step` | integer | Starts at zero and is consecutive: `step = previous + 1`. |
| `event` | string | A version-2 event listed below. Unknown events fail conversion. |

The converter rejects unknown fields rather than ignoring them.

### `trace_start`

The first line contains:

- `test`;
- `profile`;
- `durability`: `request` or `async`;
- `max_hidden_steps`;
- `nodes`: node ID and initial incarnation;
- one `shard_state`: index UUID, shard, initial primary/term/activation,
  in-sync set, and each node's allocation, copy-presence, and fence term.

`d1-core` uses two nodes. Authority, collision, and recovery profiles may use
three. All version-2 profiles start from an initialized term-1 primary.

### `trace_end`

The final line contains `outcome="completed"`, `quiescent`, and
`records_before_end`.

For `quiescent=true`:

- no replay may remain active;
- the final protocol record is `copy_state`; and
- every copy the trace says is available after crash, promotion, replay,
  removal, activation, or admission has a final `copy_state` after the last
  state-changing event.

## Operation identity

An operation is identified by `(primary_term, seq_no)` for this one-shard
schema. Every occurrence must retain the same:

- document ID;
- operation kind (`index`, `delete`, or `noop`); and
- lowercase SHA-256 content hash.

For `index`, the hash is over a shared recursively-key-sorted trace encoding of
the logical source. For `delete`, it covers the document ID and operation kind.
For `noop`, it covers the reason. Payloads are not logged.

Bulk requests emit one logical request and operation sequence per item.

## Lock-linearized events

Every shared-state field in one event is read while holding the same lock that
protects the effect, before releasing that lock. The trace stamp is allocated
inside that critical section. Immutable request/receipt values may be carried
into a later event, but an event may not reread unrelated mutable state from a
second lock.

Use a process-global synchronous sink:

1. acquire the sink mutex;
2. allocate the next `AtomicU64` step;
3. append the complete record to the in-memory buffer; and
4. release the sink mutex.

Normal asynchronous `tracing` output is not valid protocol evidence.

### Client and primary

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `client_write_routed` | coordinator `node`, index/shard, `request_id`, `target_node`, document, operation, content hash | Under the coordinator routing-view snapshot used to select the target. |
| `wal_appended` | node/allocation, request and receipt IDs, term/sequence, operation identity, `origin`, `durable` | Under the translog lock after bytes, configured synchronization, generation metadata, and high-watermark update. |
| `operation_processed` | node/allocation, IDs, operation identity, origin, planner `outcome`, checkpoints | Under the apply-state lock after the planner effect and checkpoint update. |
| `primary_replication_started` | primary/allocation, request/receipt, term/sequence, routing version, sorted `required_replicas` with allocation and incarnation | From the immutable validated routing snapshot and successful local receipt, immediately before fan-out. |
| `client_result` | coordinator, request ID, `acknowledged` or `failed`, failure stage | Immediately before returning the terminal client result. |

Checkpoints are read only by events emitted under the apply-state lock:
`operation_processed`, `commit_captured`, `node_restarted`,
`replay_started`, and `replay_entry`. They are inclusive Rust
`processed`, `persisted`, and `max_seq_no` values; JSON `null` means no
checkpoint.

`primary_replication_started` contains no checkpoint. Its required replica set
must exactly equal the primary node's latest traced routing view, including
allocation IDs.

### Replica transport

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `replica_received` | target/allocation, source/incarnation, receipt and operation identity | Under the per-shard routing/allocation validation boundary, before enqueueing apply work. |
| `replica_result` | primary, request/receipt, replica, acknowledged/failed | On the primary immediately after the target RPC resolves. |
| `fence_persisted` | node/allocation, term, nullable fence maximum, reason | Under the shard-open/identity lock after identity and parent directory durability. |

A live-replication or recovery `wal_appended` must have the corresponding
receipt/fetched operation. A live operation below the durable local fence
cannot reach the planner. Older-term WAL replay may be redelivery when the
exact operation is already processed.

### Commit and truncation

Commit is deliberately split:

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `commit_captured` | node/allocation, `commit_id`, checkpoints, term state | Under the commit/apply-state boundary when the immutable committed-boundary record is captured. |
| `commit_persisted` | node/allocation, `commit_id` only | After atomic record persistence and directory sync. It refers to the earlier immutable capture and does not reread live checkpoints or term state. |
| `wal_truncated` | node/allocation, inclusive `truncate_through` | After manifest/high-watermark persistence and generation deletion attempts. |

An operation or fence raise may occur between capture and persistence. The
persisted record remains the captured one. Truncation cannot advance beyond
the persisted processed checkpoint.

### Crash and replay

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `node_crashed` | node/incarnation and clean/unclean outcome | After old-incarnation ingress and trace-producing tasks are stopped. |
| `node_restarted` | node/new incarnation, index/shard/allocation, restored checkpoints | While reopening the copy, after volatile D1 state is reset from the persisted boundary and before serving traffic. |
| `replay_started` | node/allocation, replay ID, restored checkpoints | Observation after restart has already installed replay state. It does not perform the reset. |
| `replay_entry` | replay ID/ordinal, operation identity, planner outcome, checkpoints | After one physical WAL entry passes through the D1 planner. |
| `replay_finished` | replay ID and completed/failed outcome | After successful reader reload, or after terminal replay failure marks the copy unavailable. |

Delete tombstones are not durable metadata. A committed delete above a gap is
absent from the restored version map and replays as `applied_newer`.

The current in-process async-durability test abstraction is lossless across its
simulated crash: `durable=false` is allowed in async mode, but the in-memory
WAL is retained. Version 2 does not claim to model power-loss durability for
that harness.

### Routing and activation

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `routing_view` | observing node, primary, term, in-sync nodes, every allocation, initialized flag | Under that node's `ClusterManager` view lock. |
| `routing_promoted` | single `emitter` (the Raft leader applying the command), new primary, term, in-sync set | Under Raft state-machine apply after routing mutation. |
| `in_sync_removed` | Raft-leader emitter, removed node/allocation, resulting in-sync set | Under successful exact-allocation `FailShardCopy` apply. |
| `primary_activated` | node/allocation/activated term | Under the activation-state lock after the matching durable fence and routing-term observation. |

Per-node routing views make stale coordinator routing and a stale primary's
local view expressible. `routing_promoted` is emitted once, by the leader apply,
not once per observing node.

### Recovery

| Event | Fields beyond framing | Linearization |
| --- | --- | --- |
| `recovery_snapshot` | source, target, session, snapshot-next boundary, exact processed sequence set, logical documents | On the source inside `prepare_peer_recovery_snapshot` while the translog lock holds the captured snapshot and retention pin. |
| `recovery_started` | source, target/allocation, session | After the exact-allocation target install marker is durable. |
| `recovery_installed` | source, target/allocation, session, snapshot boundary | After verified snapshot installation and strict open. No checkpoints are read here. |
| `recovery_barrier` | source, target/allocation, session, barrier-next boundary, target processed sequence set | Under the target apply-state boundary after every operation through the source's final barrier has been processed. |
| `recovery_membership` | source, target/allocation, session, admitted/promoted/rejected/unknown | After target membership observation. |

The target is unavailable until admitted or promoted; a promoted target still
requires activation before primary service. Recovery installation uses the
captured source snapshot, never the source's later current state.

### `copy_state`

`copy_state` is an observation, not a claimed planner outcome. It contains:

- node/allocation and reason (`quiescent`, `replay`, or `admission`);
- every traced document in sorted order;
- `absent`, `live`, or `deleted`;
- for live/deleted state, exact sequence, primary term, and content hash.

The instrumentation takes a trace-only copy snapshot while holding the
apply/maintenance guard, uses one reader snapshot for live state, and reads the
matching in-memory delete version under that same guard. The trace stamp is
allocated before releasing the guard.

TLC compares this semantic state with the model's document state. Outcome labels
alone therefore cannot make an incorrect history pass.

## Converter-only checks

Before TLC, `trace_to_tla.py` rejects:

- non-v2 schemas, unknown events/outcomes/fields, or missing fields;
- non-consecutive steps;
- reused request IDs or changed operation content;
- replica processing without the same-copy WAL observation, except definitive
  collision or redelivery;
- acknowledgements missing required replica results;
- in request durability, acknowledgement without a durable primary WAL and a
  durable WAL on every required replica;
- `required_replicas` differing from the primary's latest traced in-sync view;
- malformed replay ordinals or lifecycle pairs; and
- quiescent traces missing final `copy_state` for an available copy.

## Changelog

### Version 2

- Replaced deterministic replay of hand-written rules with existential TLC
  witness search over real D1, Raft/activation, B1, and recovery actions.
- Added bounded hidden actions and per-node routing views.
- Added semantic `copy_state`.
- Split commit capture from persistence.
- Added `in_sync_removed`, source-side `recovery_snapshot`, and final barrier
  evidence.
- Moved checkpoint reads to apply-state-linearized events.
- Reset D1 volatile state at restart rather than replay start.
- Defined failed replay as copy-unavailable.
- Made steps consecutive and request-durability checks structural.

### Version 1

Retired. It used a deterministic trace replay machine with duplicated protocol
rules and made claims about hidden-step search that it did not implement.
