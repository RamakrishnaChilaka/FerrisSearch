# FerrisSearch: The Next 50 Engineering Tasks

> **Status:** All 50 engineering tasks (`FS-001` through `FS-050`) across Roadmap Gates 0 through 4 are **Completed** in `v0.3.0` (`2026-10-17`).
>
> **Strategic authority:** [`architecture-roadmap.md`](architecture-roadmap.md).
>
> **Current-behavior & release evidence:** [`production-v1-and-research-evidence.md`](production-v1-and-research-evidence.md), source, and tests.

This document preserves the stable task IDs (`FS-001` through `FS-050`), dependency graph, and completion evidence for the 50 engineering work packages across Waves 1–5, followed by the post-`v0.3.0` backlog items below the V1 cut.

**Classes**

- **Release blocker:** required for trustworthy production-facing behavior.
- **Scale blocker:** required before the architecture can grow safely.
- **Research opportunity:** validates or differentiates the core systems thesis.

## Wave 1 — Freeze Contracts And Write Identity

### FS-001 — Decide The Write Consistency And Retry Contract

**Status:** Implemented ([ADR 0001](adr/0001-write-consistency-and-retry-contract.md), [`recovery-acceptance-matrix.md`](recovery-acceptance-matrix.md), and `FS-009`–`FS-014`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Outcome:** Accepted ADR 0001 defines operation identity, sequence number, primary epoch, acknowledged durability, retry behavior, refresh visibility, partial replication, and conflict semantics for single and bulk writes.

### FS-007 — Build Deterministic Distributed Failure Injection

**Status:** Implemented (`protocol-trace` feature, `src/failpoints.rs`, `tests/d1_protocol_trace.rs`, `tests/stale_primary_failover.rs`, and `src/storage/fs017_021_tests.rs`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Outcome:** Deterministic failpoint and fault-injection hooks pause, fail, crash, or delay named write, replication, manifest publication, recovery, hydration, and compaction boundaries across in-process and process-backed tests, and emit protocol traces validated against `specs/tla/`.

### FS-002 — Decide The Fenced Manifest Publication Protocol

**Status:** Implemented ([ADR 0002](adr/0002-fenced-manifest-publication-protocol.md) and `FS-015`–`FS-019`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Outcome:** Accepted ADR 0002 defines the single Raft-leased sequencer, writer epoch, parent generation, operation ID, conditional pointer update, backend capability probes, idempotent retry, audit history, and failover state machine.

### FS-003 — Decide Read Snapshots, Retention, And Visibility

**Status:** Implemented ([ADR 0003](adr/0003-read-snapshots-retention-and-visibility.md) and `FS-036`–`FS-042`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001, FS-002

**Outcome:** Accepted ADR 0003 defines request-stable snapshot identity, pinning, read-after-write boundaries, exclusive hot/immutable handoff watermarks, forward-only rollback, and snapshot-safe garbage collection.

### FS-004 — Decide The Unified Lifecycle And Migration Model

**Status:** Implemented ([ADR 0004](adr/0004-unified-lifecycle-and-migration-model.md) and `FS-034`–`FS-043`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001, FS-003

**Outcome:** Accepted ADR 0004 defines `MutableOnly`, `ImmutableOnly`, and `Tiered` index lifecycle modes, primary-owned version/tombstone continuity across hot delta and immutable splits, and online migration/rollback.

### FS-005 — Decide Schema Generations And Durable Format Compatibility

**Status:** Implemented ([ADR 0005](adr/0005-schema-generations-and-durable-format-compatibility.md) and `FS-048`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-003, FS-004

**Outcome:** Accepted ADR 0005 separates Raft-owned logical schema generations from physical component formats and defines additive field evolution, typed-null materialization, rejected type changes, and compaction rewrite rules.

### FS-006 — Publish A Current OpenSearch Compatibility Matrix

**Status:** Implemented ([`opensearch-compatibility.md`](opensearch-compatibility.md), [`opensearch-compatibility.json`](opensearch-compatibility.json), and `scripts/check_opensearch_compatibility.py`) | **Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001

**Outcome:** A machine-verified matrix classifies every routed HTTP endpoint, client-critical semantic difference, and unsupported OpenSearch surface with source and test evidence.

### FS-008 — Establish Reproducible Correctness And Performance Baselines

**Status:** Implemented (`scripts/repro_baseline.py`, `scripts/benchmark_profiles/fs008.json`, and [`reproducible-baselines.md`](reproducible-baselines.md)) | **Class:** Research opportunity | **Gate:** 0 | **Depends on:** none

**Outcome:** A checked-in harness runs an isolated 3-node matrix across `local_shards` and `remote_store`, verifies every response against an exact Python oracle, and emits raw JSON samples plus an automated Markdown summary.

### FS-009 — Plumb Real Operation Metadata End To End

**Status:** Implemented (WAL v3 receipts, `_seq_no`, `_primary_term`, `_version`, realtime GET, and replica/recovery preservation) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001

**Outcome:** WAL allocation, engine mutation, replication RPCs, coordinator forwarding, bulk items, and REST responses carry authoritative sequence number, document version, and primary epoch metadata.

### FS-010 — Implement Optimistic Concurrency Control

**Status:** Implemented (`if_seq_no`/`if_primary_term` preconditions, `op_type=create`, primary-side CAS `_update` with `upsert`/`detect_noop`/`retry_on_conflict`, and `tests/write_correctness_regression.rs`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-009

**Outcome:** Index, update, and delete enforce expected sequence and primary-term preconditions at the primary before durable mutation without allocating sequences on conflict.

## Wave 2 — Make Writes And Publication Trustworthy

### FS-011 — Add Idempotent Client Operation IDs

**Status:** Implemented (`ClientOperationRecord`, `ClientOperationResult`, WAL-persisted 4,096-op deduplication ledger, replica/recovery transfer, and `409 operation_id_conflict`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-009

**Outcome:** Single and bulk writes carry an `operation_id` with bounded durable deduplication across primary retry, coordinator forwarding, replica apply, and failover.

### FS-012 — Fence Stale Primaries And Replica Applies

**Status:** Implemented (Raft allocation IDs, durable replica term fences, per-document `(seq_no, primary_term)` ordering, `tests/stale_primary_failover.rs`, and `large_bulk_and_same_document_writes_survive_replica_wal_replay`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-009, FS-010

**Outcome:** Primary epoch and allocation identity are validated at primary write, replica RPC, recovery, and promotion boundaries; stale epochs cannot mutate data and concurrent writes converge identically across primary and replicas.

### FS-013 — Make Write Acknowledgement Policy Explicit

**Status:** Implemented (`WriteAuthorityGate`, `minimum_durable_copies`, conditional Raft replica exclusion, sticky exclusion debt, and separate `_refresh` visibility reporting) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-011, FS-012

**Outcome:** Request-durable writes certify every captured in-sync copy or observe settled conditional Raft exclusion before acknowledgement, fail-stop on WAL/post-WAL uncertainty, and report refresh visibility separately from data durability.

### FS-014 — Replace Bulk Materialization With A Strict Streaming Pipeline

**Status:** Implemented (`src/api/index/bulk/pipeline.rs`, checksummed anonymous-file disk staging, bounded shard RPC units, streamed NDJSON/pretty responses, and [`bulk-ingestion.md`](bulk-ingestion.md)) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-001, FS-009

**Outcome:** Bulk intake streams and validates NDJSON in bounded disk staging before executing bounded shard batches and streaming ordered per-item results within bounded memory.

### FS-015 — Add Conditional Object-Store Pointer Updates

**Status:** Implemented (`src/storage/conditional.rs`, `create_immutable`, `read_with_token`, `compare_and_swap`, MAC-authenticated `CompareToken`, and startup 5-step backend capability probe) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-002

**Outcome:** `StorageManager` enforces create-if-absent and conditional compare-and-swap across local filesystems (`flock` + two-slot checksummed revision state) and S3-compatible backends (`If-None-Match` / `If-Match`), failing closed on uncertified backends.

### FS-016 — Replicate Manifest Writer Lease And Epoch In Raft

**Status:** Implemented (`ManifestWriterAuthority`, `ClusterCommand::MutateManifestWriterAuthority`, monotonic `last_writer_epoch` / `last_lease_id`, and `MutateManifestWriterAuthority` gRPC forwarding) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-002

**Outcome:** Raft manages one lease/epoch sequencer record per `remote_store` index across acquire, activate, renew, transfer, expiry, and holder removal without storing split inventory in Raft.

### FS-017 — Version Manifests With Parent, Writer Epoch, And Operation ID

**Status:** Implemented (`RemoteStoreManifest` v3 and `RemoteManifestPointer` v3 with `parent_generation`, `writer_epoch`, `operation_id`, `request_fingerprint`, `schema_generation`, and lineage validation) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-005, FS-016

**Outcome:** Manifests and pointers record parent lineage, writer epoch, operation identity, and schema generation, rejecting broken lineage or unsupported versions while supporting idempotent retry.

### FS-018 — Implement The Fenced Single-Sequencer Commit Path

**Status:** Implemented (`src/storage/publication.rs`, `CommitRemoteStorePublication` gRPC forwarding, epoch pointer fencing before activation, and inspectable publication attempt/commit records) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-015, FS-016, FS-017

**Outcome:** Parallel producers stage immutable split bundles while the active Raft-leased sequencer fences `manifest.current.json`, validates parent lineage and operation fingerprints, and conditionally commits each generation.

### FS-019 — Prove Multi-Writer Publication Under Crash And Failover

**Status:** Implemented (`specs/tla/MC_FS018_ManifestPublication.tla`, `src/storage/fs017_021_tests.rs` 4-process OS child crash/race suite, and 3-node REST/gRPC failover tests) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-018

**Outcome:** Bounded TLA+ model checking and process-backed crash/failover suites verify no lost committed split, no stale epoch commit, monotonic lineage, and idempotent retry across every publication phase.

### FS-020 — Build A Split Reachability Inventory

**Status:** Implemented (`StorageManager::build_reachability_inventory`, `ReachabilityInventoryReport`, and `GET/POST /{index}/_remote_store/inventory`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-017

**Outcome:** Non-mutating reachability analysis walks `manifest.current.json`, retained parent manifests, compaction lineage, operation records, and active pins/leases to classify every object-store artifact.

## Wave 3 — Bound Recovery And Distributed Work

### FS-021 — Add A Dry-Run-First Object-Store Janitor

**Status:** Implemented (`StorageManager::run_janitor`, `POST /{index}/_remote_store/janitor`, durable `<index_uuid>/janitor/marks.json`, and pre-delete reachability re-verification) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-003, FS-020

**Outcome:** A rate-limited janitor defaults to dry run, explains every candidate decision, persists sweep marks across restarts, and deletes only unpinned objects outside the retention and lease safety windows.

### FS-022 — Define And Persist A Shard Snapshot Format

**Status:** Implemented (`ShardSnapshotManifest` v1, `ShardSnapshotCompletionMarker`, `<shard_dir>/snapshot.latest`, and `ShardSnapshotVectorMetadata`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-005

**Outcome:** Flushed shards persist a checksum-verified snapshot manifest and completion marker capturing Tantivy segments, WAL checkpoint, retained delete/operation metadata, canonical history, vector state, and schema hash.

### FS-023 — Implement Snapshot Transfer And Atomic Install

**Status:** Implemented (`prepare_peer_recovery_target_blocking` into `<shard_dir>.staging` and atomic swap with `<shard_dir>.backup` rollback in `finalize_peer_recovery_target_blocking`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-022

**Outcome:** Peer recovery streams and verifies snapshots in `<shard_dir>.staging` before atomically swapping into `<shard_dir>`, preserving either the previous valid shard or the new verified snapshot across interruption, disk exhaustion, or restart.

### FS-024 — Stream The WAL Suffix During Recovery

**Status:** Implemented (`apply_ordered_recovery_suffix_chunk`, bounded `MAX_RECOVERY_OPS` / `MAX_RECOVERY_OP_BYTES` streaming, contiguous checkpoint verification, and `CancelPeerRecovery`) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-022, FS-023

**Outcome:** Peer recovery streams bounded, gap-free, ordered WAL suffix chunks with idempotent duplicate handling, contiguous final checkpoint enforcement, and bidirectional session cancellation.

### FS-025 — Introduce An Explicit Replica Recovery And ISR State Machine

**Status:** Implemented (`ReplicaCopyState`, `ReplicaStateRegistry` in `src/shard/replica_state.rs`, and `ferrissearch_replica_copy_state_copies` / `ferrissearch_replica_state_transitions_total` metrics) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-012, FS-013, FS-024

**Outcome:** Replica copies transition through `Unassigned`, `Initializing`, `Snapshotting`, `CatchingUp`, `InSync`, `Stale`, and `Failed` under one validated state machine with transition metrics.

### FS-026 — Make Replica Policy Settings Reactive And Operable

**Status:** Implemented (`ReplicaPolicySettings`, `EffectiveReplicaPolicy`, `WriteAcknowledgementMode`, and follower-reactive `ShardManager::reconcile_applied_settings`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-013, FS-025

**Outcome:** Lag thresholds, recovery timeouts, retry backoff, and supported acknowledgement modes replicate through Raft and apply reactively on every open shard copy across all nodes.

### FS-027 — Make Vector Recovery Complete And Explicit

**Status:** Implemented (`vectors.docids.bin` v2, `VectorRecoveryMode::SnapshotRestored`, unbounded `VectorRecoveryMode::SegmentScanRebuilt`, and `VectorRecoveryProgress`) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-022

**Outcome:** Vector indexes restore directly from verified persisted/snapshot state or rebuild via an unbounded streaming segment scan with explicit progress and fail-closed error handling.

### FS-028 — Add Request-Scoped Cancellation

**Status:** Implemented (`RequestCancellationContext`, `CancellationDropGuard`, and cooperative stage cancellation across search, SQL, hydration, merge, and maintenance) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** none

**Outcome:** HTTP and gRPC requests create a cancellation context propagated across distributed fan-out and worker pools, releasing temporary files, pins, load counters, and byte reservations on disconnect or cancellation.

### FS-029 — Propagate Absolute Deadlines Across gRPC

**Status:** Implemented (`RequestDeadline`, clamped child fan-out budgets, `x-ferris-remaining-budget-ms`, `x-ferris-deadline-epoch-ms`, `grpc-timeout`, and `ferrissearch_deadline_exhausted_total`) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-028

**Outcome:** Coordinators propagate monotonic remaining deadline budgets across nested RPCs, charge admission queue wait time, and reject expired work before execution.

### FS-030 — Add Bounded Admission Queues

**Status:** Implemented (`AdmissionController`, per-`WorkClass` concurrency and FIFO queue caps, deadline-aware dequeue, and `429` / `ResourceExhausted` overload rejection) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-028, FS-029

**Outcome:** Every work class (`Search`, `Sql`, `Write`, `Hydration`, `Recovery`, `Maintenance`) enforces bounded concurrency and queue depth before spawning work or allocating large buffers.

## Wave 4 — Govern Resources And Converge Data

### FS-031 — Reserve Memory, Disk, And In-Flight Bytes

**Status:** Implemented (`ResourceGovernor` and RAII `ResourceReservation` across `Memory`, `TempDisk`, `PersistentCache`, and `ObjectStoreInflight` byte pools) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-030

**Outcome:** Nodes reserve estimated bytes before expensive query, hydration, staging, or compaction work, reconcile actual usage, and release reservations on every completion, error, or cancellation path.

### FS-032 — Classify And Prioritize Background Work

**Status:** Implemented (`WorkPriorityTier`, `WorkClassBudget`, foreground-pressure throttling, starvation-aging promotion, cooperative background yields, I/O byte accounting, and `ferrissearch_work_class_saturation_ratio` / `ferrissearch_background_throttled_total` metrics) | **Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-030, FS-031

**Outcome:** Foreground search/write and background recovery/hydration/maintenance receive explicit concurrency, CPU, I/O, and byte budgets with priority and starvation rules.

### FS-033 — Add End-To-End Distributed Tracing And Stable Error Classes

**Status:** Implemented (`RequestTraceIds`, `StageTimingLedger`, `StableErrorClass`, header/gRPC propagation, secret sanitization, and `ferrissearch_classified_errors_total` / `ferrissearch_stage_execution_duration_seconds` metrics) | **Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-029

**Outcome:** Request, operation, snapshot, assignment, and task IDs propagate through structured spans with bounded attributes and stable retryable/terminal/corruption/overload error classes.

### FS-034 — Implement The Searchable Mutable Delta

**Status:** Implemented (`IndexLifecycleMode::Tiered`, `HotDeltaSealPolicy`, durable replicated hot-delta shards, `enumerate_seal_boundary`, and CRUD/search visibility before sealing) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-004, FS-009, FS-013

**Outcome:** A logical unified index owns a durable replicated hot delta with explicit refresh/search visibility and the same document/version identity used by immutable splits.

### FS-035 — Add Automatic Bounded Sealing Into Immutable Splits

**Status:** Implemented (`seal_hot_delta`, `ActiveSealBarrier`, policy & manual triggers, backpressure rejection, `ResourceGovernor` reservations, and deterministic retryable `operation_id`) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-018, FS-031, FS-034

**Outcome:** Policy-driven sealing selects a stable delta range, builds a split under resource limits, and submits it to the fenced sequencer with retryable operation identity.

### FS-036 — Commit A Publication Visibility Watermark

**Status:** Implemented (`ShardPublishedWatermark`, `SplitSealRangeMetadata`, `RemoteStoreManifest.published_watermarks`, contiguous sequencer validation, and idempotent post-commit `retire_sealed_delta_through`) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-035

**Outcome:** Publication commit records a contiguous logical watermark and keeps the corresponding delta visible until the manifest is safely queryable.

### FS-037 — Query One Snapshot Across Hot And Immutable Layers

**Status:** Implemented (`ReadSnapshot`, `ResolvedIndexSnapshot`, `resolve_index_read_snapshot`, `QueryClause::SnapshotFilter`, and cross-layer version/tombstone exclusion across Search DSL, `_count`, aggregations, and SQL) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-036

**Outcome:** Search DSL and SQL pin one snapshot, query eligible hot and split sources, suppress overlapping versions, and merge hits/partials with type and ordering stability.

### FS-038 — Add Versioned Tombstones And Supersession

**Status:** Implemented (split-level `doc_versions` and `tombstones`, deterministic `(seq_no, primary_term, version, split_order)` winner selection across hot delta and splits, and pinned `snapshot_generation` reads) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-010, FS-037

**Outcome:** Updates create newer logical versions and deletes create tombstones; snapshot filtering selects the newest visible version across delta and splits without relying on wall-clock time.

### FS-039 — Model Split Lineage And Replacement

**Status:** Implemented (`CompactionLineageRecord`, `RemoteSplitState::Compacted`, `parent_split_ids`, `replaced_by_split_ids`, `validate_compaction_lineage`, and `apply_compaction_replacement`) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-017, FS-038

**Outcome:** Manifest metadata records immutable split inputs, replacement outputs, version/tombstone coverage, and lineage under one fenced commit.

### FS-040 — Build A Fenced, Idempotent Split Compactor

**Status:** Implemented (`compact_splits`, `RemoteStoreCompactionOptions`, `deterministic_compaction_operation_id`, `POST /{index}/_remote_store/compact`, `ResourceGovernor` reservations, and fenced sequencer lineage commit) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-031, FS-039

**Outcome:** A resource-governed compactor selects eligible splits, builds compacted outputs, and atomically commits lineage through the manifest sequencer.

## Wave 5 — Complete Lifecycle, Scale, And Evidence

### FS-041 — Implement Snapshot-Safe Mark-And-Sweep GC

**Status:** Implemented (`SnapshotRetentionLease`, `SnapshotLeaseGuard`, compacted-split reachability in `build_reachability_inventory`, pre-delete lease re-verification in `run_janitor`, and forward-only `rollback_manifest`) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-021, FS-039, FS-040

**Outcome:** GC marks from every retained/pinned manifest and sweeps only expired unreachable bundles, manifests, hotcache artifacts, and staging data under bounded request/delete rates.

### FS-042 — Deliver Backup, Restore, Verify, And Repair Workflows

**Status:** Implemented (`ClusterBackupManifest`, `POST /_snapshot/{snapshot_id}`, `GET /_snapshot/{snapshot_id}`, `POST /_snapshot/{snapshot_id}/_verify`, `POST /_snapshot/{snapshot_id}/_restore`, `POST /{index}/_remote_store/repair`, and `restore_documents_with_versions`) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-022, FS-041

**Outcome:** Cluster backup and recovery workflows snapshot Raft metadata and data references, restore to a clean cluster, verify checksums/lineage, and repair or quarantine reconstructible failures.

### FS-043 — Migrate Existing Indices Without Reinterpreting Semantics

**Status:** Implemented (`IndexMigrationPhase`, `IndexMigrationRecord`, `POST /{index}/_lifecycle/migrate`, `POST /{index}/_lifecycle/rollback`, `GET /{index}/_lifecycle/status`, oracle equivalence verification, fast retained local rollback, and post-retirement base reconstruction) | **Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-004, FS-037, FS-042

**Outcome:** Online lifecycle migration seals eligible local-shard data into the unified tiered lifecycle, verifies oracle equivalence, switches at a pinned boundary, and retains rollback state.

### FS-044 — Stream Bundle Hydration And Verification

**Status:** Implemented (`stream_bundle_to_temp_file_with_reservation`, `unpack_bundle_file_into`, `HydrationLimits`, incremental SHA-256 verification, compressed/uncompressed decompression-bomb rejection, and atomic `.done` marker promotion) | **Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-028, FS-031

**Outcome:** Hydration streams object bytes to a temporary artifact, hashes incrementally, enforces compressed/uncompressed limits, and atomically marks completion before extraction/read admission.

### FS-045 — Unify Cache Inventory, Admission, And Eviction

**Status:** Implemented (`NodeCacheInventorySnapshot`, `rebuild_cache_inventory_from_disk`, `enforce_artifact_admission_budget`, `GET /_nodes/cache/inventory`, and unified tier accounting across artifact, reader, column, and staging tiers) | **Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-031, FS-044

**Outcome:** One node cache governor accounts for logical/physical bytes, pinned readers, column entries, artifacts, temporary data, admission cost, and eviction reason without scanning the filesystem per query.

### FS-046 — Maintain A Bounded Leaf Capability And Load Inventory

**Status:** Implemented (`LeafCapabilityInventorySnapshot`, TTL-bounded leaf inventory on `RemoteSplitReaderCache`, piggybacked batch warmth updates, membership convergence, cardinality caps, and conservative fallback) | **Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-033, FS-045

**Outcome:** Roots maintain short-lived, bounded, failure-aware leaf capability/load/cache summaries with freshness and conservative fallback.

### FS-047 — Scale Split Assignment, Retry, And Partial-Failure Semantics

**Status:** Implemented (`assign_remote_store_splits` $O(S \cdot L)$ bounded top-$K$ candidate selection, `RemoteStoreSchedulingStats`, snapshot-pinned `assignment_ids`, `duplicate_responses_suppressed`, `split_error_is_terminal`, `allow_partial_search_results`, and `EXPLAIN ANALYZE` scheduling telemetry) | **Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-028, FS-029, FS-046

**Outcome:** Snapshot-pinned batch assignments use bounded candidate selection, assignment IDs, deduplicated retry, retryable/terminal errors, and explicit partial-result policy.

### FS-048 — Execute Mixed Schema Generations Safely

**Status:** Implemented (`SchemaLineageMetadata`, `GET/PUT/POST /{index}/_mapping`, `validate_manifest_for_metadata`, `QueryClause::Exists`, `StringFastFieldReader::empty`, typed-null materialization across DSL and SQL, incompatible field-type rejection, and compaction schema-generation upgrade) | **Class:** Release blocker | **Gate:** 3 | **Depends on:** FS-005, FS-039

**Outcome:** Splits declare logical schema generation and physical format; readers materialize missing additive fields as typed nulls, reject incompatible types, and compaction upgrades older schema generations.

### FS-049 — Enforce Tenant-Aware Security And Resource Isolation

**Status:** Implemented (`TenantQuotaConfig`, `TenantQuotaLease`, `AuditLogEntry`, `AuditOutcome`, `GET /_security/audit`, `GET/PUT/DELETE /_security/quota*`, `validate_external_object_store_uri`, cross-tenant task/cache/`_cat`/semijoin isolation, and body-routed target-index charging) | **Class:** Release blocker | **Gate:** 3 | **Depends on:** FS-030, FS-031, FS-033

**Outcome:** Principal/index context reaches admission and audit decisions; quotas bound concurrent work/bytes; cross-tenant metadata/task/cache access is denied; and object-store credentials remain external to Raft/manifests/logs.

### FS-050 — Produce The Production V1 And Research Evidence Package

**Status:** Implemented ([`docs/production-v1-and-research-evidence.md`](production-v1-and-research-evidence.md) and `fs047_050_scale_scheduling_mixed_schema_generations_tenant_isolation_and_research_evidence_suite`) | **Class:** Release blocker + research opportunity | **Gate:** 4 | **Depends on:** FS-006, FS-008, FS-019, FS-037, FS-041, FS-042, FS-047, FS-048, FS-049

**Outcome:** The Gate 4 evidence package consolidates reproducible evaluation commands, systems-research ablation studies, formal TLA+ model and fault matrices, operational recovery/migration drills, and Production V1 sign-off/deferrals.

## What Is Intentionally Below This Cut

With `FS-001` through `FS-050` complete in `v0.3.0`, the following capabilities remain explicitly deferred beyond the V1 cut:

- Remote-split k-NN vector execution and hotcache sidecar consumption on `remote_store` splits (currently rejected with `501`).
- True concurrent multi-writer manifest merging without a single index sequencer (single fenced sequencer with parallel split producers is the V1 design).
- Cross-cluster replication (CCR).
- Arbitrary painless/script execution, cross-index joins beyond `IN (SELECT ...)` semijoins, and OpenSearch plugin ABI compatibility.
