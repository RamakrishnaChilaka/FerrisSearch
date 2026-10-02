---
description: "Use for integration, restart, SQL logic, transport, object-store, TLA+, and subsystem regression tests."
applyTo: "tests/**,src/**/tests.rs,specs/tla/**,scripts/tla/**"
---

# Testing Patterns

## Test Suite Shape
- Library unit tests (`cargo test --lib`)
- CLI unit tests (`cargo test --bin ferris-cli`)
- Raft consensus integration (`cargo test --test consensus_integration`)
- Replication and transport integration (`cargo test --test replication_integration`)
- REST API integration (`cargo test --test rest_api_integration`)
- Process-backed restart regression (`cargo test --test restart_regression`)
- Crash-isolated request regression (`cargo test --test request_crash_regression`)
- SQL correctness through sqllogictest (`cargo test --test sql_correctness`)
- S3-compatible remote-store integration (`cargo test --test remote_store_s3_integration`), skipped unless `FERRIS_RUSTFS_ENDPOINT` is set
- Bounded shard replication/recovery model checking (`./scripts/tla/check.sh`)

Do not hard-code suite or assertion counts in instructions or README. They
become stale after ordinary test additions; report the command and observed
result from the current run instead.

The `remote_store` automated coverage is split between REST integration tests (publish/search/verify behavior with in-process harnesses, including query-string `GET /_search`, match-all `/_count` on shardless indices, publish-time split-summary persistence for pruning, keyword-array/coercion and cap-exceeded no-prune behavior, field-specific invalid-keyword publication rejection without a manifest, term/range pruning regressions that assert non-candidate splits are not fetched into cache, response-level `remote_store.pruning` counters including unsupported-query no-prune behavior, and SQL EXPLAIN ANALYZE pruning metadata) and `remote_store_s3_integration` (real S3-compatible manifest/object operations via `StorageManager`). That still does not replace a full process-backed RustFS + `./dev_cluster_release.sh --nodes 3` live validation; keep a manual runbook for that flow and an isolated smoke script for the automatable pieces.

## Running Tests
```bash
cargo test                                      # All tests
cargo test --lib                                # Unit tests only
cargo test --test consensus_integration         # Raft consensus tests
cargo test --test replication_integration       # Replication tests
cargo test --test replication_integration --features transport-tls  # Replication tests with encrypted gRPC transport
cargo test --test rest_api_integration          # REST API integration tests
cargo test --test restart_regression            # Real restart/rejoin regression
cargo test -- test_name                         # Single test by name
./scripts/tla/check.sh                           # Fast bounded TLA+ matrix
./scripts/tla/check.sh c1-aba-fixed c2-fixed l2  # Selected fixed-design checks
./scripts/tla/check.sh g1-empty-store g2-replica g2-primary g2-liveness
./scripts/tla/check.sh l1-bump l2-primary-idle l2-promotion
./scripts/tla/check.sh pending-restart-legacy pending-restart-fixed two-shard
./scripts/tla/check.sh storage-replica storage-primary storage-primary-no-replica
./scripts/tla/check.sh storage-apply-replica storage-apply-primary storage-apply-primary-no-replica
./scripts/tla/check.sh s1-combined-replica s1-combined-primary s1-combined-liveness
./scripts/tla/check.sh trace-validator          # Converter and trace acceptance/rejection
./scripts/tla/validate_trace.sh path/to/trace.jsonl
./scripts/tla/check_d1_trace_invariants.py path/to/trace.jsonl
./scripts/tla/test_d1_protocol_trace.sh
./scripts/tla/test_trace_validator.sh
./scripts/tla/check.sh fixed-crash               # Long exhaustive local run
./scripts/tla/check.sh fixed-simulation          # Seeded depth simulation
```

## TLA+ Model Checks

- TLC passes are exhaustive only for the exact finite constants in the
  selected `.cfg`; never describe them as proofs for arbitrary cluster sizes.
- `scripts/tla/check.sh` verifies the pinned TLA+ tools jar before execution
  and uses isolated Java/TLC temporary directories.
- Historical counterexamples are living model regressions. The runner must fail if
  `c1-aba`, `c2`, `c2-allocation-ids`, `fence-volatile`, `c3`, `c4`, or
  `pending-restart-legacy` stops violating its named invariant, or if
  `l2-primary-no-trigger`, `storage-apply-no-escalation`, or
  `s1-combined-liveness-no-timeout` stops producing its temporal liveness
  violation. `d1-order-historical` must retain `NoCopyBehindAcked`, and
  `d1-replay-historical` must retain its acknowledged replay-loss violation.
  `d1-term-collision-seq-only` must retain `B1NoCopyBehindAcked`.
  `d1-term-collision-restart-committed` must retain
  `B1RNoCopyBehindAcked`, and `d1-primary-gap-max` must retain
  `B3NoRecoveryLoop`.
  The S1 no-timeout case is a modeling-assumption regression, not a historical
  Rust defect.
- `c2-fixed`, `fence-durable`, `g1-empty-store`, `g2-replica`, `g2-primary`,
  `g2-primary-no-replica`, `g2-liveness`, `pending-restart-fixed`, `l1-bump`,
  `l2-primary-idle`, `l2-promotion`, `storage-replica`, `storage-primary`,
  `storage-primary-no-replica`, `storage-apply-replica`,
  `storage-apply-primary`, `storage-apply-primary-no-replica`,
  `s1-combined-replica`, `s1-combined-primary`, `s1-combined-liveness`,
  `d1-order-fixed`, `d1-replay-fixed`, `d1-no-durable-tombstone`,
  `d1-term-collision-fixed`, `d1-gaps`,
  `d1-term-collision-restart-identity`, `d1-primary-gap-processed`,
  `d1-promotion-replay-noop`, `d1-trace-actions`, `trace-validator`,
  `two-shard`, `fixed-crash`, and
  `fixed-partition` are expected-pass configurations.
- An expected-pass failure stops the modeling task. Preserve the raw trace,
  decide whether the model or implementation is wrong, and do not weaken an
  invariant or transition merely to obtain green output.
- Numeric liveness-bound exhaustion is the exception: increase only the
  exhausted Raft/message/term/allocation/recovery bound, preserve the trace,
  and record the old and new values. Safety failures and non-bound liveness
  failures still stop immediately.
- A property comparing non-semantic internal retention/cache/bookkeeping state
  may be refined when logical state and every safety property already agree.
  Preserve and document the over-strong-property trace before continuing.
- A property applied to a copy that is not available under the Rust contract
  may be availability-scoped. Promotion candidates still replaying and
  recovery targets not yet admitted are not available copies.
- Safety runs may use a documented state constraint and valid node symmetry.
  Liveness runs use neither; declare the exact fairness assumptions instead.
- When you add a model variable, add it to every action's `UNCHANGED` tuple,
  including scenario-wrapper actions such as `G2StopFaults`. Round 3 missed
  `storageFaultInjected` in those wrappers and `g2-liveness` failed. Rerun the
  full fast matrix after adding a variable.
- `FaultMode` enables one fault class per configuration. Before you describe a
  fault combination as checked, confirm it in the fault-class coverage table in
  `specs/tla/README.md`. The `fixed-crash` and `fixed-partition` runs use C1
  and C2, so they include neither storage (S1) nor disk-loss faults.
- Keep action comments and `specs/tla/README.md` mapped to the current Rust
  functions. Label each protocol variant as current, historical, or proposed;
  never imply that a model-only transition is implemented.
- Replica-fencing checks use local knowledge: reject below the durable local
  fence or applied local term. Do not compare every in-flight apply against an
  unseen globally committed term; the retained retired-property trace explains
  why that assertion is too strong.
- G1/G2 checks must cover CreateIndex before first activation, disk loss of
  primary and in-sync replica copies, exact-allocation `FailShardCopy`,
  no-survivor primary-report rejection, stale-report rejection, and fair
  replacement recovery.
- Storage-failure checks must exercise both immediate corruption and
  persistent-I/O escalation at open, fence, marker, and apply boundaries.
  Apply-I/O checks keep the copy open while WAL/fsync/engine mutations fail,
  retain a no-escalation temporal counterexample, and require writes to resume
  after exact replica removal or primary promotion. The leader carries a live
  in-sync candidate and prefers its highest observed checkpoint only when it
  hosts the primary and therefore has observations; the state machine validates
  current in-sync membership. A primary report without a candidate must be
  rejected without clearing its allocation.
- Combined S1 checks must cover retry-budget reset across restart, reports
  pending across failed-primary or leader crash, repair that remains possible
  if allocation races ahead, fresh-allocation recovery, and a final
  acknowledged write. Timeout fairness is permitted only when a required
  target is down, has restarted past the request epoch, or its transport
  message was dropped.
- ADR D1 configurations must allow at least two same-shard client writes in
  flight and arbitrary replica delivery order. Check `NoCopyBehindAcked`
  during concurrency and logical-only convergence at quiescence when every
  primary-WAL operation was acknowledged. Replay checks cover gap-aware
  processed checkpoints, redelivery, tombstone pruning, crash/restart, and a
  late older index after delete.
- D1 term-collision checks distinguish operation identity by primary term and
  sequence. A newer-term collision at an already processed sequence must fail
  the copy and require recovery. Gap checks cover missing-operation pull,
  timeout/re-recovery, and promotion-time NoOp fill before checkpoint advance.
- Restart collision checks restore fence term and fence maximum from durable
  copy identity rather than commit metadata. Primary-gap detection compares
  processed checkpoints, not maximum sequence. Promotion checks replay all
  local WAL entries before NoOp fill; failed NoOp replication may leave a
  replica gap but cannot block local activation.
- D1 implementation traces follow `specs/tla/trace/SCHEMA.md`. The converter
  must accept only schema v4 and reject unknown versions, events, outcomes,
  fields, non-consecutive steps, invalid durability, required-replica/view
  mismatch, stale/reused message IDs, incorrect crash-lost sets, incomplete
  NoOp fan-out, or invented copy state before invoking TLC.
- The validator infers the composition from the event vocabulary; the emitter
  does not choose a profile or hidden-step bound. The inferred compositions
  use the owning actions:
  `TraceD1` with `MC_D1_SeqNoApply`, `TraceD1Authority` with Raft/activation,
  `TraceD1Collision` with the B1 slice, and `TraceD1Recovery` with
  `PeerRecovery`. Do not replace these with a deterministic replay machine or
  duplicate planner/routing rules in the trace module.
- One trace uses exactly one inferred composition. The combined composition
  permits core replication followed by crash/restart, failover, activation,
  collision removal, and later-term writes. The full composition adds
  fresh-allocation peer recovery and admission to that same real D1 relation;
  isolated recovery fixtures retain the recovery-only composition. Keep the
  exact event vocabularies synchronized with `SCHEMA.md`.
- TLC trace acceptance is existential witness search with a validator-owned
  hidden-action bound. A pass means only that the finite observation can be
  embedded in the selected bounded model; it is not an implementation proof.
- `validate_trace.sh` returns `0` for acceptance, `1` for rejection, and `3`
  with `INCONCLUSIVE` for timeout, memory exhaustion, or an incomplete TLC
  run. CI must never count exit `3` as an expected rejection. A standalone
  fixture defaults to 120 seconds and a 4 GiB Java heap. Self-test fixtures
  use at most `min(4,nproc)` concurrent 2 GiB JVMs, with isolated logs printed
  in declaration order; the deliberate OOM fixture retains its explicit small
  heap.
- The review mutation matrix must retain rejection for m1, m2, m3, m4, m5,
  m6, m6b, m7, m8, m8b, m9, m9b, m15, m18, and m19, while m13 and m14 remain
  accepted. Also retain the replay-stage invalid trace whose commit boundary is
  valid but replay behavior is not.
- Round-2 traces must reject n1, n3, n4, n7, n9, and n10; accept n1c, n2, n5,
  n6, n8, n11, n12, n13, n14, and n15; and keep the dedicated
  `d1-trace-actions` configuration green.
- `copy_state` is the final event even for non-quiescent traces, and is
  mandatory for every available copy at quiescence and after replay/admission.
  Refresh before taking it. Deleted-state identity is trace-owned and must not
  depend on the 60-second tombstone-retention cache.
- Bulk traces may append every item before any item processing event. A bulk
  replica response may carry the batch-final persisted checkpoint; require
  item-local persisted <= response persisted <= current replica persisted.
- Retain the 219-event schema-v4 combined witness and its invalid arrival-order,
  collision-redelivery, and post-promotion rollback variants. The valid
  witness must include a real missing sequence filled by a durable promotion
  NoOp plus exact sequence-target fan-out, not only an empty gap-fill stage.
  It does not itself contain a promoted-copy restart; retain v6c for
  replay-before-fill ordering. Retain the exact 500-event restart/failover
  performance fixture and keep every fixture under 120 seconds.
- Retain the slow round-4 restart matrix:
  m7 empty replay after committed truncation, a retained-entry
  `skip_committed` control and invalid re-apply, m10 replay of an uncommitted
  promotion NoOp and m10c omission, m8 activation without gap fill, m6b fill
  before replay completion, and m4c/m9 late request/ack delivery. Run it with
  `./scripts/tla/check.sh trace-validator-round4`.
- Keep `d1-failover-actions` and `d1-noop-collision-actions` green. They are
  scripted action-coverage paths, not exhaustive model checking. Together
  they exercise model-owned durable fence, NoOp fill/fan-out/apply/redelivery,
  activation, NoOp collision/NACK, exact removal, and D1 safety invariants.
- Retain accepted promotion-NoOp apply and collision/removal fixtures plus
  rejected mutations for an omitted apply, collision mislabeled as
  redelivery, and reviewer p7b's omitted fan-out. Reviewer p7a must remain
  accepted.
- Recovery catch-up ordering assumes an activated-primary source scanning the
  pinned physical WAL with one exclusive cursor. Do not claim support for
  duplicate or out-of-order catch-up traces.
- Protocol trace events must be synchronously ordered by the process-global
  trace sink and emitted after the named effect but before releasing its
  linearizing lock. Every mutable field in one event comes from that same lock;
  split commit capture/persistence and other cross-lock effects. Do not
  validate normal asynchronous tracing output.
- The `protocol-trace` feature is test-only. Its seeded three-node real-gRPC
  suite must accept the unmodified implementation and reject both
  `arrival-order` and `seq-only-redelivery` at the causal
  `operation_processed` event in both the independent invariant checker and
  TLC. Keep the seed in failure output so the run is reproducible.
- Pending-target liveness must cover the settlement deadline, source-primary
  restart/reactivation, promotion of a different replica, and target restart
  with durable marker restoration. `RecoveryConverges` means one attempt
  reaches admission, promotion, or definitive rejection; retry convergence is
  a separate configuration.
- Idle-primary liveness must attach weak fairness to the proactive node
  lifecycle activation action, not assume a future client or recovery request.
- The minimal `two-shard` check covers index-level routing isolation only; do
  not cite it as a two-shard WAL, replication, or recovery proof.

## Unit Test Conventions
- Write regressions must prove realtime/non-realtime visibility, delete
  tombstones, WAL-truncated reader fallback, and replayed identities.
  Check exact-one-winner CAS and unchanged WAL size/allocator on conflicts.
  Concurrent updates must correlate each acknowledgement with a unique field
  present in the final source; 409 losers must have no write effect.
  Mixed bulk coverage needs action boundaries, duplicate-ID order,
  create/conditional conflicts, no-op/upsert, malformed-action whole-request
  rejection, source-error item retention, and delete 404 without `errors=true`.
  Transport coverage must compare primary/replica sources, sequences, and WAL
  histories after conditional and mixed CRUD.
  Incarnation coverage must recreate an index with the same document
  sequence/term, reject the old UUID CAS and create-only upsert, preserve WAL
  and allocator state, and accept the new UUID. Also cover a CAS delayed
  inside the recovery write barrier. Missing GET documents retain their UUID.
- D1 conditional traces emit `failure_stage: "version_conflict"` only before
  primary WAL/sequence assignment. The owning `PrimaryVersionConflict` action
  preserves document, WAL, checkpoint, and allocator state. The converter
  rejects that stage after a primary WAL event. Capture all final copy
  snapshots before emitting their observations so later commits cannot
  invalidate earlier final-state evidence.
- Tests live in `#[cfg(test)] mod tests` at the bottom of each source file
- Use `#[tokio::test]` for async tests
- Use `tempfile::TempDir` for isolated data directories
- Test every code path: happy path, edge cases, error conditions, empty inputs
- For large bulk-ingest changes, add a router-level regression proving `POST /_bulk` accepts a body larger than Axum's default 2MB limit and reaches the handler instead of failing with `413` at the framework layer.
- For in-process REST integration harnesses, wait for both the HTTP listener and the gRPC transport listener before issuing the first request; Raft-backed index/settings handlers may still forward through transport before the harness is usable, and any readiness `Ping` must use a registered `source_node_id` because transport rejects unknown nodes.
- Multi-node REST harnesses that claim coordinator coverage must preserve real `raft_node_id` values in cluster state and route at least one request through a non-master node; otherwise follower-forwarding regressions can hide behind leader-only traffic.
- Multi-node `remote_store` tests must use a shared object-store root across nodes plus per-node local workdirs/caches; otherwise a coordinator-local read path can masquerade as distributed execution.
- The multi-node REST harness joins real in-memory Raft voters over gRPC.
  Wait for applied metadata instead of copying newly created indices into
  followers. Initial fixtures may still seed state before an operation.
  Keep direct transport rejection coverage for malformed or missing metadata.
- Create-then-write regressions use the `cfg(test)` one-shot apply gate in
  `ClusterStateMachine`. Block application, not log replication, and preserve
  the real quorum. Keep a short deadline test, exact document results, and
  the two-node 50-iteration no-hook loop. Do not lengthen client timeouts or
  weaken assertions to hide a forwarding race.
- A gate entering does not prove the intended CreateIndex committed: it may
  have paused an earlier bootstrap Normal entry. Deadline regressions must
  keep the explicit create request independently polled and observe the
  leader's committed index before sending writes. Keep the follower paused,
  assert its metadata/engine remain absent, and verify primary placement on a
  Data-role node. A primary term of 2 may be initial activation, not reassignment.
- For WAL generation/manifest changes, add regressions for manifest creation on new shards, manifest-required reopen, active-generation-only reopen, and ignored non-generation side files in the WAL directory.
- For WAL corruption hardening, add regressions that an unknown operation tag in the active generation returns `Err` on reopen instead of panicking, and that an internal active-generation mismatch fails before append writes bytes.
- For persistent Raft format errors, independently corrupt vote, committed-log,
  and last-purged metadata through their public storage reads. Each error must
  be `InvalidData`, name the component, use the exact wipe-node-data/recreate-
  cluster remedy, and never use recreate-index guidance.
- For primary sequence ownership changes, cover sequence zero versus missing
  optional wire fields, empty/non-empty bulk receipt consistency, document-ID
  order, concurrent single/bulk/delete identities across primary and replica
  WALs, and allocation/range overflow before WAL bytes are written.
- For mapped keyword arrays, cover nested flattening, scalar coercion, nulls,
  duplicate values counting once, `_source` preservation, reopen/replay, numeric
  arrays not becoming vectors, and object rejection before any single, bulk, or
  explicit-sequence WAL/writer mutation.
- For terms collector changes, exercise exact large integer/float bucket keys,
  both sides of the bounded dense/sparse threshold, filtered results, and
  invalid ordinal/dictionary failures rather than only plan selection.
- For HotEngine WAL-wrapper hardening, add unit tests that poisoned translog wrapper locks return `Err` on write and maintenance paths, and that grouped segment worker panics are surfaced as ordinary query errors.
- For WAL auto-flush or replay changes, add regressions for disabled thresholds (`flush_threshold_bytes = 0`), zero global checkpoint safety (no auto-truncate), and stale `translog.committed` checkpoints that force a replayed suffix after a prior batch commit.
- For auto-flush concurrency changes, add regressions proving maintenance ticks defer instead of blocking when the text flush path or vector persistence path is already busy.
- For maintenance scheduling changes, add a `#[tokio::test(flavor = "current_thread")]` regression that blocks a real Tantivy maintenance path with bounded synchronization and proves an unrelated write or replica apply still completes on the fixed write pool. Also preserve post-write `refresh=true` visibility and maintenance error propagation.
- For refresh/flush fan-out changes, add one regression that the coordinator still dispatches its local node through the per-node maintenance path, plus a transport regression that maintenance reopens persisted assigned shards but refuses to create missing UUID directories.
- For asynchronous `/{index}/_forcemerge` changes, add one API regression that the handler returns `202 Accepted` immediately and one transport regression that the gRPC force-merge RPC returns after enqueueing background work instead of waiting for segment compaction to finish.
- For force-merge coordination changes, deterministically overlap calls with a
  barrier, assert the final segment bound and exact document values/deletes,
  verify automatic merge-policy restoration, and cover zero/invalid bounds at
  engine, HTTP, and transport surfaces.
- For container-aware cache sizing, use fixture procfs/cgroupfs trees covering
  cgroup v2 and v1, tighter ancestors, unlimited and missing controllers,
  nested/namespaced mount roots, malformed authoritative values/mappings, and
  the zero-disabled path. Also cover startup cache wiring and exported gauges.
- For async maintenance task-tracking changes, add one REST/API regression that `POST /{index}/_forcemerge` returns a task id and `GET /_tasks/{task_id}` reaches a terminal state, plus one transport regression that `GetTaskStatus` returns the node-local snapshot for a queued/running/completed task.
- For async scheduling changes around shard open/close, orphan cleanup, translog fsync, redb-backed Raft storage, or other blocking wrappers, add a `#[tokio::test(flavor = "current_thread")]` regression that holds the relevant lock or resource from another thread and proves the runtime still advances while the wrapper waits.
- For index UUID / orphan-cleanup fixes, add a regression that an auto-created index opens its local shard with the same UUID stored in cluster state, plus a restart-path regression that missing expected UUID directories cause cleanup to bail out instead of deleting unknown shard data.
- For recovered-node startup guard changes, add a two-restart regression that proves a missing startup UUID dir never gets recreated on the first restart and therefore can never make the old UUID dir look orphaned on the second restart.
- For restart/rejoin data-loss fixes that depend on real process startup order, add or extend a process-backed `restart_regression` test that runs real `ferrissearch` binaries through create -> ingest -> flush -> restart -> verify count/UUID-dir invariants.
- For authoritative in-sync membership changes, cover creation-time admission,
  later allocation staying out of sync, removal, in-sync-only targeted and
  fallback promotion, out-of-sync-first replica reduction, strict persisted
  format rejection, strict proto roundtrip/rejection, and live replication
  targeting. Add a real three-process flush -> allocate replica -> primary loss
  -> red shard -> original-primary rejoin regression that verifies exact
  acknowledged values.
- For allocation identity and replica fencing, retain fail-first evidence for
  the C1 same-node admission ABA and C2 stale-primary apply. Cover log-position
  ID assignment, exact-ID recovery start/session/install/pending/admission,
  G1 pre-activation empty creation, malformed/missing identity rejection,
  allocation-bound `FailShardCopy`, active-versus-failed-versus-stale install
  marker classification, UUID/allocation/term/missing-field replica
  rejection, bulk pre-mutation validation, durable fence restart, separate
  source/target state handles, and a real-process in-sync replica disk-loss
  recovery with exact acknowledged documents.
- For file-based peer recovery, cover WAL pins on every truncation path,
  exact snapshot boundary under concurrent writes, marker/strict-open
  semantics, file name/offset/length/hash validation, ordered bounded operation
  apply, session expiry, stale term/session rejection, final barrier admission,
  and live post-admission replication. Process coverage must include added
  replica recovery with concurrent acknowledged writes, recovery disabled via
  per-node config, and stale same-directory replica rejoin before failover.
- Availability regressions must also cover dynamic-mapping reopen with an
  active source session, completion timeout remaining non-destructive, pending
  target restart followed by promotion, and definitive term rejection
  restoring the destructive recovery marker.
- Review regressions also cover cancelled asynchronous Start, cancelled
  PrepareFinalize, settlement-safe idle reaping, queued index/bulk/delete after
  primary change, marker creation during open, live-generation reads after a
  failed manifest publish, and routing-update rejection before node removal.
- Allocation/fencing review regressions cover two-shard red-sibling routing
  isolation, initialized/present-primary admission guards, newer-term and
  changed-primary pending rejection, restart restoration before recovery,
  target/source refusal to reattach a settling session, transient fence I/O
  without routing failure, retry cleanup without a failed-install marker, stale
  identity-temp cleanup, and preservation of existing test-copy identity.
- Round-2 allocation/fencing regressions cover corrupt WAL/Tantivy/marker
  classification, open/fence/marker-I/O count/time escalation, shared request
  and lifecycle open backoff, replica removal with resumed writes,
  promote-only primary failover and no-survivor rejection, idle lifecycle
  activation after primary restart, post-rename pending-state repair, and
  delete/recreate-safe recovery abort.
- Round-3 allocation/fencing regressions cover persistent ENOSPC/read-only
  apply failures on already-open primary and replica copies, transient apply
  failure reset, bounded apply backoff, local-storage versus network recovery
  accounting, structural Tantivy metadata/mapping corruption, exact replica
  removal with resumed writes, leader-selected primary promotion, and
  no-candidate status-only primary unavailability   without routing change.
- Round-4 allocation/fencing regressions cover stable unavailable status across
  repeated write-only faults, same-term clearing after the first repaired
  write, fresh activation after repaired open failure, stale availability CAS
  rejection, throttled definitive quarantine, no Apply-level quarantine or
  runtime WAL replay, readable failed-writer copies, and escalation of a
  force-merge replacement failure through the Apply key.
- Round-5 allocation/fencing regressions cover a one-shot force-merge
  replacement failure rebuilding successfully on the next write, persistent
  rebuild failure remaining reportable under the Apply budget, and successful
  write responses remaining independent of a blocked or slow
  `MarkPrimaryAvailable` report.
- Collision quarantine regressions must assert repeated immediate single and
  bulk replication attempts remain `DATA_LOSS`; a definitive marker must never
  be masked by copy-I/O backoff. Inject collision-marker persistence failure
  and prove the engine remains evicted, the in-memory allocation stays marked,
  the storage error is reportable, and later replication and reads fail closed
  even while the durable identity still has an unset marker. Lifecycle removal
  coverage must create the marker through an actual colliding replica RPC, not
  by editing the identity file, and assert the exact allocation is removed.
- Round-6 storage regressions use a real Tantivy commit failure to prove the
  writer is removed, the persisted checkpoint does not advance, five later
  acknowledged writes survive the next commit and restart, and a failed
  replica retains every acknowledged write after promotion.
- Round-7 storage regressions cover delete-preserving startup and failed-writer
  replay, malformed WAL document envelopes, idempotent replay with deletes,
  replica delete survival through promotion, idle refresh/snapshot healing,
  and full transport recovery after a transient source refresh-commit failure.
- D1 vector recovery regressions must cover a failed post-WAL text apply
  followed by refresh or primary activation before the next write, plus
  restart persistence and successful clearing of `vectors.stale`.
- Vector rebuild coverage must include a test-only small batch size crossing
  multiple batch boundaries with a deleted document, plus an explicit
  greater-than-100,000-document activation regression proving vector count,
  last-document version state, text visibility, and kNN visibility.
- Dynamic-mapping reopen coverage must prove kNN results remain value-identical
  immediately after replacement and after a later refresh. A replacement
  engine with an unloaded or empty vector index is a failure even when text
  documents remain visible.
- Promotion NoOp coverage must include a real source activation, an initial
  failed gRPC fan-out, a later lifecycle activation, and value-level proof that
  the replica durably received the original term/sequence NoOp and closed its
  checkpoint gap. Also hold one shard's retry RPC open on a black-hole replica
  and prove a write to another shard completes within a small bound.
- Keep result-level round-6 coverage that a post-snapshot source apply failure
  stalls catch-up at the physical cursor and is served during finalize, and
  that 1,030 non-contiguous promotion NoOps reach a live replica in exactly two
  bounded bulk RPCs.
- Retry-catch-up coverage must preserve an empty incomplete response at the
  unchanged physical cursor as a return-to-finalize signal, not a
  no-progress failure. Drive that shape through `run_peer_recovery` with a real
  source session, gRPC transport, target install, finalize, and admission;
  assert the same session uses one setup and reaches finalize rather than only
  testing the cursor helper.
- Zero-replica bulk replication coverage must prove target resolution happens
  before sequence-range construction or JSON serialization. Performance
  changes require equivalent release-mode before/after measurements.
- Replica JSON decode coverage must exercise the real single and bulk
  transport handlers and prove one decode per index operation. Keep the
  engine's independent source validation as defense in depth, and use an
  equivalent release-mode workload for performance claims.
- R6 replay regressions cover more than one 1,000-operation replay batch after
  a fence raise, visibility of every pre-promotion document plus a later write,
  clean assigned-copy reopen, and restart from an intermediate commit whose
  term-start maximum is ahead of its committed maximum.
- Round-2 recovery regressions cover lock-free large-generation WAL scans,
  one-shot setup error polling, stale-target replacement, cancelled reopen
  during hashing, Notify lost-wakeup ordering, Tokio-safe cleanup, and primary
  changes during dynamic-mapping Raft work.
- Round-3 recovery regressions cover detached reopen versus delete/recreate at
  both lifecycle and per-shard-lock boundaries, setup panic completion,
  unrelated finalize settlement during long hashing, and torn/oversized WAL
  frames at the captured recovery head.
- Round-4 recovery regressions cover delete during the reopen open-window,
  missing existing Tantivy metadata, same-term index UUID replacement,
  partially visible post-head WAL appends, the 32 MiB frame boundary on every
  WAL write API, and HTTP 503 mapping with attributable bulk failures.
- Round-5 WAL regressions cover the uniform 32 MiB persisted/recovery frame
  ceiling, exact
  final-generation/captured-size handling for in-progress appends, durable
  active-tail truncation before append, and fail-closed middle corruption.
- For CLI parser fixes, add multiline regressions when behavior depends on SQL statement structure (`EXPLAIN`, table extraction, quoted identifiers), not just single-line happy paths.
- For global SQL routing fixes, add both helper-level coverage and a `POST /_sql/stream` regression using a quoted hyphenated index name with keyword-casing variants, including the aliasless `count(*)` fast path.
- For index-engine metadata changes, add unit coverage for create-body parsing and transport/proto roundtrips, plus REST coverage for `PUT /{index}` and `GET /{index}/_settings` so immutable engine selection is exercised end to end.
- For any new Raft control-plane mutation (new `ClusterCommand`, new `ClusterState` config field, new forwarded write RPC — see `control-plane.instructions.md`), add all three layers: (1) unit — `types.rs` serde JSON roundtrip per variant, `state_machine.rs` apply test asserting the map changed AND `version` bumped, `cluster/state.rs` current-shape snapshot roundtrip plus a missing-field fixture that is rejected; (2) transport — a direct gRPC test of each RPC (leader applies, non-leader returns `failed_precondition`); (3) coordinator/multi-node — a follower's API handler forwards the write to the leader and the change is observable on the leader (preserve real `raft_node_id`s, route through a non-master node).
- For the dynamic security control plane specifically, also assert: create→authenticate→revoke→denied, custom-role authz grants only mapped actions, static + dynamic keys coexist, `GET /_security/*` never leaks `hash_sha256`, and a non-admin principal gets 403 on `/_security/*`. Security-enabled REST harnesses must treat HTTP 401 on `GET /` as "server up" during readiness polling (auth rejects the probe).
- For SQL identifier case-sensitivity fixes, add helper-level canonicalization coverage plus REST regressions for both buffered and streamed SQL endpoints using real mixed-case mapping fields, and cover both unquoted source references and quoted exact-identifier preservation on the residual/DataFusion path.
- For `ferris-cli` interactive features, test command parsing and completion token boundaries in pure helpers; keep watch-mode behavior factored so the logic is covered without relying on terminal I/O in tests.
- For `ferris-cli` SQL metadata/footer changes, keep search-stage counts distinct from final SQL row counts, surface the actual `approximate_top_k` state from API metadata when present, and add pure helper coverage for the displayed labels so `matched_hits` is not presented as returned rows.
- For `ferris-cli` grouped timing display changes, add pure helper coverage for nested `timings.grouped_merge` extraction and a `POST /_sql/stream` regression proving the NDJSON `meta` frame preserves timings when the streamed endpoint re-frames buffered grouped-partials results.
- For streamed `ferris-cli` SQL changes, add pure chunk-boundary NDJSON parsing tests and at least one REST integration test covering the global `POST /_sql/stream` route the console uses.
- For feature-gated transport TLS changes, run both `cargo test --lib` and `cargo test --lib --features transport-tls`; enabling TLS without the feature must error instead of silently downgrading to plaintext.
- For transport TLS end-to-end coverage, also run `cargo test --test replication_integration --features transport-tls`.
- For SQL fast-field string changes, add regressions for both `sql_record_batch()` and `sql_streaming_batches()` that assert `_id` and keyword values survive the optimized ordinal path.
- For streamed fast-field scan optimizations, add a value-level regression that integer and date columns survive `sql_streaming_batches()` batch-for-batch against `sql_record_batch()`, not just a schema-equality check.
- For multi-segment streamed fast-field regressions, force multiple segments up front and assert the segment count before executing the streaming path so the test cannot pass accidentally on a single-segment index.
- For local streamed SQL execution changes, add unit coverage that `sql_streaming_batch_handle()` matches `sql_streaming_batches()` batch-for-batch on the same query and that zero-hit handles emit exactly one empty batch before returning `None`.
- For new `SearchRequest` / `QueryClause` variants, add a serde JSON roundtrip regression because search DSL requests cross transport boundaries as serialized JSON.
- For streamed shard SQL transport changes, add a real gRPC integration test that forces multiple Arrow batches from `forward_sql_batch_stream_to_shard()` / `SqlRecordBatchStream`, not just unit tests around IPC decoding.
- For streamed SQL transport metadata changes, add coverage for `total_hits`, `collected_rows`, and actual `streaming_used`, plus at least one `/_sql/stream` regression where the streamed endpoint must keep `streaming_used=false` because the shard falls back to `sql_record_batch()`.
- For JoinCluster or cluster-state transport fixes, add one roundtrip regression
  that proves `raft_node_id`, `unassigned_replicas`, `in_sync_replicas`, index
  `mappings`, index `settings`, and index `uuid` survive proto conversion.
  Reject malformed in-sync membership and unknown field types instead of
  coercing them, and preserve concurrent gRPC regressions for duplicate
  `raft_node_id` rejection and full voter-set preservation across overlapping
  joins.
- For follower heartbeat/rejoin fixes, add transport coverage that `Ping` rejects unregistered source nodes and a lifecycle-level regression whenever the node loop changes how ping rejection triggers `JoinCluster` recovery.
- For `_id` fast-path refactors, add a multi-segment sorted-result regression that proves `_id` stays aligned with projected data columns after segment concatenation and reorder.
- For distributed hit-merge changes, add unit coverage for `merge_sorted_hit_lists()` and a multi-node REST regression where only one shard returns hits but the coordinator still must apply a custom sort.
- For grouped-partials timing changes, add API or unit coverage that `EXPLAIN ANALYZE` exposes the nested grouped-merge breakdown (`partial_merge_ms`, `having_ms`, `top_k_ms`, `row_build_ms`) and bucket counts for grouped SQL queries.
- For grouped column-cache changes, add unit coverage that match-all grouped direct scans populate per-segment grouped cache entries and that filtered grouped readers reuse warm entries without populating cold partial scans, plus a `POST /_sql` regression showing repeated grouped-partials queries keep cache occupancy stable.
- For width-2 grouped-key hot-path changes, keep a direct grouped-pair regression on the packed-key path and add a focused map-key regression when the pair-hasher implementation changes.
- For grouped-key encoding changes, add regressions that distinguish SQL `NULL` buckets from signed integer payloads such as `-1` across single-key, width-2 packed-key, and multi-key grouped execution paths.
- For `_cat/shards` state fixes, add a regression that a live shard copy on one node does not make a different assigned copy on another node appear `STARTED`; display state must be per copy, not per shard ID.
- For `_cat/segments` changes, add a transport regression for `GetSegmentStats` and a multi-node REST regression that compares the returned segment rows against the actual `segment_infos()` reported by all shard copies.
- For node startup/rejoin cleanup changes, add tempdir regressions that prove empty pre-catch-up state does not delete live UUID directories, and that authoritative UUID sets still remove true orphaned directories.

## Integration Test Infrastructure
### Consensus Tests (tests/consensus_integration.rs)
- Spin up real Raft clusters (1-3 nodes) in-process
- Use `create_raft_instance_mem()` for in-memory log store
- Test leader election, failover, log replication, membership changes
- Each test gets isolated temp directories
- No external services needed — everything runs in-process

### Replication Tests (tests/replication_integration.rs)
- Spin up real gRPC servers with isolated shard managers
- Test primary-to-replica replication, bulk replication, recovery
- Test checkpoint tracking, authoritative in-sync targeting, and replica apply
- Uses actual `TransportClient` + `TransportService` over localhost
- Seed `ClusterManager` with node/index/shard metadata before gRPC write, replication, or search calls; transport now rejects unknown shards instead of implicitly creating them from empty metadata

### Restart Regression (tests/restart_regression.rs)
- Spawns real `ferrissearch` processes via `CARGO_BIN_EXE_ferrissearch`
- Builds a real 3-node Raft cluster with isolated tempdirs and log files
- Exercises create -> bulk index -> flush -> restart-all -> verify count and UUID-backed shard directories
- Exercises bounded mixed-role master loss with 3 shards and 2 replicas, then
  verifies per-shard lost-copy accounting, real promotion, exact acknowledged
  values/deletes, and a write routed to the promoted shard.
- Reproduces the flush-truncated-WAL schedule with a later-added out-of-sync
  replica, proves it remains `INITIALIZING` and is not promoted, then restarts
  the original primary and verifies all acknowledged values return.
- This does not prove file recovery, in-sync admission, terms/fencing, or
  restarted-replica gap handling.
- Asserts destructive delete reasons do not appear in logs during the preserved-data workflow

### Request Crash Regression (tests/request_crash_regression.rs)
- Use child processes for abort-reproducing requests and worker probes; an
  abort must fail its parent assertion, not terminate the lib test binary.
- Own every child with a kill/reap guard, including failed startup, and poll
  observable membership/readiness rather than relying on fixed settling sleeps.
- Assert typed errors and exact result rows, then prove subsequent requests
  succeed. Include two-node remote-shard/gRPC classification coverage.
- The ignored worker probe is invoked by parent tests for contained search
  panics, explicit write fail-stop, and bounded SQL streaming allocations.
- `FERRIS_CRASH_TEST_LOG_DIR` optionally retains child-node logs outside the
  per-test tempdirs.

## Test Helper Patterns
- `tokio::time::timeout()` to prevent hung tests
- `tokio::time::sleep()` for Raft election settling
- Assert on cluster state after Raft commands
- Verify shard routing, node membership, index metadata

## Development Workflow
1. **Read first** — understand existing code
2. **Implement** — make code changes
3. **Unit tests** — cover every branch (empty inputs, edge cases, errors)
4. **Integration tests** — if feature involves Raft, gRPC, or multi-component interaction
5. **Live test** — `cargo run` + curl
6. **Fix bugs** — add a test for each bug discovered
7. **Coverage audit** — check every branch has a test
8. **Update README** — examples, roadmap, test counts
9. **Update copilot-instructions.md** — if architecture changed

## Hybrid SQL Test Expectations
- For hybrid SQL planner changes, add unit tests for:
	- pushdown extraction
	- quoted index names
	- grouped analytics planning
	- LIMIT pushdown detection and rewritten SQL preservation
	- residual predicate detection
	- `needs_id` / `needs_score` detection
	- truncation flag logic (explicit LIMIT = not truncated, flat no-LIMIT fast-field queries may truncate at the 100K ceiling, GROUP BY fallback uses the separate scan-limit/error path)
- For direct fast-field execution changes, add tests that assert eligible queries use fast-field readers without requiring `_source` materialization.
- For API-level SQL changes, validate both execution modes where practical:
	- `tantivy_grouped_partials` for eligible grouped SQL queries over matched docs
	- `tantivy_fast_fields` for local non-`SELECT *` queries with columnar access
	- `materialized_hits_fallback` for wildcard projection or distributed compatibility paths
- **LIMIT correctness**: Always assert exact row counts for LIMIT queries — `LIMIT N` must produce exactly N rows, not N × number_of_shards. This was a previous test gap.
- **DataFusion 53 LIMIT regression**: Include pure DataFusion tests that reproduce the projection-reorder LIMIT bug (schema-order SELECT works, reverse-order SELECT without the workaround returns too many rows). These tests document the upstream bug and verify the `project_batch_to_sql_columns` workaround.
- **Projection helper invariant**: `project_batch_to_sql_columns` must use planner-derived dependencies (`required_columns`, `needs_id`, `needs_score`) instead of a second ad-hoc SQL walker. Complex expressions such as `CASE`, `HAVING`, and grouped residual SQL can otherwise register an incomplete MemTable schema and fail at execution time.
- **Distributed grouped-partials key stability**: Add a regression test that round-trips grouped partials through `encode_partial_aggs()` / `decode_partial_aggs()` and verifies integer group keys still merge with local shard keys. Coordinator merge must not split logically identical buckets like `0` and `0.0` across local vs remote shards.
- **Distributed SQL transport coverage**: The sqllogictest harness is single-node and single-shard. Any bug that depends on remote shard fan-out, gRPC transport, or partial-state encode/decode must also have a multi-node integration test (REST or transport-level) in addition to local result-correctness coverage.
- **Schema drift recovery**: Add unit tests for both safe and unsafe remote-schema drift. Cover at least: (1) a drifted-first batch with later typed batches still preserves the later column and canonical type, (2) a missing-first batch fills later-missing values with nulls instead of dropping the column, and (3) an uncastable drifted batch fails loudly instead of degrading values into nulls.
- **Alias shadowing**: Cover cases where a SELECT alias matches a real field name. Real source fields must stay in `required_columns` when projection/GROUP BY expressions or wrapped HAVING aggregate inputs still need them, while pure computed aliases referenced only from ORDER BY/HAVING wrappers like `COALESCE(total, 0)` must still be stripped.
- **Duplicate grouped output aliases**: Cover the end-to-end `/_sql` path where a grouped column alias and metric alias collide, for example `SELECT brand AS total, count(*) AS total ... GROUP BY brand`. Grouped-partials planning must reject the query with an ambiguous-column error instead of silently picking one meaning.
- **Grouped ORDER BY aggregate expressions**: Cover aliasless grouped queries like `SELECT author, SUM(upvotes) ... ORDER BY SUM(upvotes) DESC`. Supported aggregate expressions in ORDER BY must resolve back to the grouped metric and stay on `tantivy_grouped_partials` instead of falling through to the generic fast-fields/DataFusion path.
- **Zero-column SQL batches**: Add regressions for literal-only queries such as `SELECT 1 FROM ... WHERE text_match(...)`. The planner must not fake `needs_score`, and the execution path must still return one output row per hit.
- **Bound-column IR migrations**: When the small planner binder lands, add unit tests that resolve the same identifier name across clauses to different semantic kinds: source field vs output alias vs synthetic `_id` / `_score`. Cover at least WHERE alias residual behavior, HAVING/ORDER BY output-space binding, aggregate-argument source-field binding under alias shadowing, GROUP BY source-only eligibility, and real `score` vs synthetic `_score` separation.
- **Truncation flag**: Assert `truncated=false` for explicit LIMIT queries. Assert `truncated=true` only for flat fast-field queries when `matched_hits` exceeds the internal 100K ceiling without an explicit LIMIT. GROUP BY fallback queries should error via `group_by_scan_limit_exceeded` instead of returning `truncated=true`.
- **GROUP BY scan limit**: Expression GROUP BY and unsupported-aggregate GROUP BY fall to `tantivy_fast_fields` with a raised scan limit (`sql_group_by_scan_limit`, default 1M). Test that: (1) `has_group_by_fallback` is true for expression GROUP BY / unsupported aggs, false for plain GROUP BY and flat queries, (2) the `group_by_scan_limit_exceeded` error fires when a capped fallback path collects fewer rows than it matched (unit test with `sql_group_by_scan_limit: 1` and a text/source-fallback GROUP BY), (3) a fully fast-field-backed local expression GROUP BY can stream past that tiny limit and still succeed, and (4) fallback queries that reference text/source-fallback columns or `_score` stay correct instead of being forced onto the bitset streaming path.
- **Searched CASE bucket grouping**: Add planner coverage for the supported searched-`CASE` bucket shape (`CASE WHEN field >= ... AND field < ... THEN 'bucket' ... END`) staying on `tantivy_grouped_partials`, plus a runtime regression for the derived bucket assignment itself. Also keep neighboring negative tests proving non-literal `ELSE` branches fall back, non-string-backed source fields fall back before execution, and unrelated expression GROUP BY shapes (for example `LOWER(field)`) still fall back.
- **Residual expression tree**: Queries like `ROUND(AVG(x), 2)`, `AVG(x) + AVG(y)`, `SUM(a) / COUNT(*)`, `MAX(x) - MIN(x)` must use `tantivy_grouped_partials` with `residual_expr`. Test that: (1) hidden metrics are extracted for each inner aggregate, (2) the projected metric has `residual_expr: Some(...)`, (3) ROUND/CAST/arithmetic are correctly represented in the tree, (4) `eval_residual_expr` produces correct values including integer preservation for `MAX - MIN` on integer fields, and (5) ORDER BY on residual-expr metrics works correctly.
- **Nested grouped metric expressions**: Cover grouped aggregates whose argument is itself a nested per-doc arithmetic tree, for example `AVG((fare - pay) / fare)`. Add both planner coverage proving the query stays on `tantivy_grouped_partials` and engine coverage proving the batched fast-field evaluator computes the correct merged result.
- Live tests should inspect the `planner`, `execution_mode`, `streaming_used`, and `truncated` fields, not just the returned rows.
- Add regression tests when planner or execution changes accidentally widen the fallback path for queries that should stay search-aware.

## SQL Correctness Testing Strategy

### Industry Standard: sqllogictest
The industry standard for SQL engine correctness testing is [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki), originally from SQLite. It's used by **DataFusion, CockroachDB, DuckDB, RisingWave, Databend, CnosDB**, and many others.

- **Format**: `.slt` files containing `statement ok`, `statement error`, and `query <type> <sort_mode>` records with expected output after `----`.
- **Rust crate**: [`sqllogictest`](https://crates.io/crates/sqllogictest) (v0.29.1) from risinglightdb — parser + runner, 12M+ downloads.
- **DataFusion's approach**: 200+ `.slt` files covering GROUP BY, HAVING, LIMIT, ORDER BY, aggregations, joins, subqueries, window functions. Also runs SQLite's 5M+ test queries and Postgres compatibility tests.
- **Key principle**: Tests assert **result correctness** (actual values), not just plan properties (mode, strategy). This is what caught the GROUP BY + LIMIT bug — our tests only checked `plan.limit_pushed_down` and `plan.grouped_sql.is_some()`, never the actual row values.

### Testing Rules for SQL Changes
1. **Always test result values, not just plan metadata.** A test that asserts `plan.limit_pushed_down == true` but never checks if the returned rows are correct is incomplete.
2. **Always test WHERE/HAVING on aliases.** SELECT aliases like `count(*) AS posts` must NEVER be pushed down to Tantivy — they are computed values, not physical fields. Test that alias-referencing predicates stay as residual.
3. **Test multi-shard correctness.** GROUP BY results must be identical regardless of how data is distributed across shards. Compare single-shard vs multi-shard results, and cross the remote partial-state encode/decode boundary when the query uses distributed grouped partials.
4. **Test boundary conditions for LIMIT.** `LIMIT N` on grouped partials must return exactly N rows after merge+sort, not N rows per shard.
5. **Test HAVING with LIMIT and OFFSET together.** The execution order must be: merge → HAVING → sort → LIMIT/OFFSET.
6. **Every new SQL feature MUST have sqllogictest coverage.** When adding a new SQL capability (new aggregate function, new clause, new pushdown, new execution path), add `.slt` tests in `tests/slt/` that assert the correct output values. This is a BLOCKING requirement — do not merge SQL changes without corresponding `.slt` tests.
7. **Same-index semijoin changes need three layers of coverage.** Add planner/unit coverage for supported and rejected shapes, at least one single-node end-to-end `/_sql` regression with grouped inner `HAVING` or `text_match`, and one multi-node regression where the inner grouped query merges remote shard partials before the outer filter is lowered.

### sqllogictest Scope
- `tests/sql_correctness.rs` builds a single-node, single-shard sample cluster. It is the right place to assert SQL result correctness, but it cannot catch distributed transport or remote partial-state serialization bugs by itself.

### sqllogictest Infrastructure (Implemented)
- **Crate**: `sqllogictest = "0.29"` as a dev dependency
- **Runner**: `tests/sql_correctness.rs` — implements sync `DB` trait via `FerrisDB` adapter that calls `execute_sql_for_testing()` with `block_in_place` + `Handle::current()` bridge
- **Test files**: `tests/slt/*.slt` — automatically discovered and run
- **Dataset**: 10 HN-style docs with known values (5 authors, 5 categories, deterministic upvotes/comments)
- **Coverage**: 179 assertions across 4 `.slt` files covering `count(*)`, `GROUP BY`, `HAVING`, same-index semijoins, case-insensitive unquoted columns, `LIMIT`, `OFFSET`, single and multiple top-level `text_match` predicates, `sum`, `avg`, `min`, `max`, alias non-pushdown, tie-breaking ORDER BY, CASE-based grouped aggregates, and exact per-row arithmetic aggregate semantics (`AVG(x / y)` vs `SUM(x) / SUM(y)`)
- **Float presentation rule**: The sqllogictest adapter compares rounded text output. When a regression needs one-decimal user-visible output, make that rounding explicit in SQL with `ROUND(..., 1)` and keep an exact-value Rust assertion in `tests/sql_correctness.rs` for the unrounded semantics.
- **HAVING coverage rule**: Always test HAVING with **both** alias-based (`HAVING cnt > 1`) and aggregate-expression (`HAVING COUNT(*) > 1`) forms. These take different code paths in the planner — alias goes through `expr_to_field_name`, aggregate expression goes through `resolve_having_name` → `parse_grouped_metric`. Missing one form caused a regression where HAVING with aggregate expressions silently fell to the wrong execution path.
- **Adding tests**: Create new `.slt` files in `tests/slt/` — the runner picks them up automatically
- **Tie-breaking**: Always use secondary sort (e.g., `ORDER BY posts DESC, author ASC`) in `.slt` tests to avoid non-deterministic ordering

## Dev Cluster
```bash
./dev_cluster.sh 1    # HTTP 9200, Transport 9300, Raft ID 1
./dev_cluster.sh 2    # HTTP 9201, Transport 9301, Raft ID 2
./dev_cluster.sh 3    # HTTP 9202, Transport 9302, Raft ID 3
```
