# Production V1 And Research Evidence Package (`FS-050`)

- **Status:** Active release and research evidence artifact (`FS-050`, Gate 4).
- **Date:** 2026-10-17
- **Related Documents:** [`architecture-roadmap.md`](architecture-roadmap.md), [`next-50-tasks.md`](next-50-tasks.md), [`reproducible-baselines.md`](reproducible-baselines.md), [`opensearch-compatibility.md`](opensearch-compatibility.md), [`recovery-acceptance-matrix.md`](recovery-acceptance-matrix.md), and [`../specs/tla/README.md`](../specs/tla/README.md).

This document consolidates the reproducible correctness, fault-injection, formal-model, conformance, operational drill, and ablation evidence for FerrisSearch across Roadmap Gates 0 through 4 (`FS-001` through `FS-050`).

FerrisSearch remains a pre-1.0 distributed search and analytics engine in Rust. It is not a drop-in replacement for every OpenSearch feature or plugin; supported compatibility boundaries are machine-declared in [`opensearch-compatibility.json`](opensearch-compatibility.json).

---

## 1. Reproducible Evaluation Environment And Commands

All automated evidence suites in this package reproduce from a clean checkout without external cloud accounts:

- **Operating System / Toolchain:** Linux x86_64, stable Rust (`cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`), OpenJDK 25 + pinned TLA+ Tools 1.7.4 (`./scripts/tla/check.sh`).
- **Baseline Benchmark Harness:** [`reproducible-baselines.md`](reproducible-baselines.md) (`./scripts/benchmarks/run_baseline_suite.sh`).
- **End-to-End Lifecycle, Scale, Schema Evolution, Security, and Ablation Suite:**

```bash
# 1. Bounded TLA+ model checking matrix (shard replication, recovery, D1/D2/S1/G1/G2, and FS-018 manifest publication)
./scripts/tla/check.sh

# 2. Storage conditional CAS, fenced publication, reachability inventory, janitor, streaming hydration, and unified cache governor
cargo test --lib storage::

# 3. Scale split assignment, leaf capability inventory, retry deduplication, and mixed schema generations
cargo test --lib engine::remote_store::

# 4. Multi-tenant security, resource quotas, and redacted audit log unit suite
cargo test --lib security::

# 5. End-to-end REST/SQL lifecycle, backup/restore/repair, migration/rollback, mixed-schema, and multi-tenant isolation suite
cargo test --test rest_api_integration fs04
```

---

## 2. Systems Research Thesis And Ablation Evidence

FerrisSearch evaluates four coupled systems hypotheses on one engine:

### 2.1 Hypothesis 1 — Conservative Search-Aware Split Pruning

- **Mechanism:** Published and sealed immutable splits record exact per-field min/max range summaries (`field_ranges` for `integer`, `float`, `date`) and bounded exact keyword/boolean term sets (`field_terms`, capped at 64 distinct values; omitted when exceeded so high-cardinality fields never cause false-negative pruning).
- **Ablation Comparison (Pruning Enabled vs. Disabled):**
  - **Enabled:** Queries with selective `term` or `range` predicates evaluate `build_split_pruning_predicate` at the query root before leaf status or hydration. Non-matching splits are counted in `remote_store.pruning.pruned_splits` and never scheduled or hydrated (`hydrated_bytes = 0` for pruned splits).
  - **Disabled / Unsummarized Fallback:** Queries on unsummarized `text` fields or high-cardinality fields (`> 64` terms) conservatively keep `candidate_splits == published_splits`, guaranteeing zero false negatives relative to a single-node full-scan oracle.
- **Reproducible Verification:**
  - `tests/rest_api_integration.rs`: `remote_store_term_filter_prunes_unmatched_split_before_cache_fetch`, `remote_store_range_filter_prunes_unmatched_split_before_cache_fetch`, `remote_store_capped_keyword_array_summary_keeps_the_split_conservatively`, `remote_store_sql_explain_analyze_reports_pruning_counters`.

### 2.2 Hypothesis 2 — Bounded Candidate-Window Cache-Aware Rendezvous Scheduling

- **Mechanism (`FS-045`, `FS-046`, `FS-047`):**
  - Roots maintain a TTL-bounded leaf capability, load, and warmth inventory (`LeafCapabilityInventorySnapshot`) refreshed via piggybacked batch outcomes and bounded status probes, avoiding per-query all-leaf RPC fan-out.
  - `assign_remote_store_splits` selects the top `REMOTE_STORE_RENDEZVOUS_WINDOW = 2` rendezvous candidates per split in a single $O(L)$ linear pass without $O(L \log L)$ per-split sorting, then breaks ties by `(reader_cached > artifact_cached > cold, lower inflight_bytes, lower queue_depth, higher rendezvous_score)`.
  - Each batch carries a snapshot-pinned `assignment_id` (`assign-{snapshot_id}-r{round}-{node_id}`), suppresses duplicate or out-of-assignment split outcomes (`duplicate_responses_suppressed`), classifies leaf errors into retryable vs. terminal (`retryable_failures`, `terminal_failures`), and enforces explicit `allow_partial_search_results` policy.
- **Ablation Comparison:**
  - **Warm Affinity vs. Cold Assignment:** Reader-warm and artifact-warm assignments eliminate redundant object-store downloads (`hydrated_bytes = 0`, `reader_warm_assignments > 0`), while leaf failure or churn deterministically fails over to the second candidate in the rendezvous window without duplicate document counting.
- **Reproducible Verification:**
  - `src/engine/remote_store.rs`: `fs046_bounded_leaf_capability_inventory_avoids_fanout_converges_on_churn_and_falls_back_conservatively`, `fs047_scale_split_assignment_bounded_candidate_window_and_deduplicated_retry_semantics`.
  - `tests/rest_api_integration.rs`: `fs047_050_scale_scheduling_mixed_schema_generations_tenant_isolation_and_research_evidence_suite`.

### 2.3 Hypothesis 3 — Search-Native Grouped Partials And Disclosed Approximate Top-K

- **Mechanism:** Eligible `GROUP BY` queries with `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` execute inside Tantivy segment collectors (`GroupedAggCollector`, `execution_mode = "tantivy_grouped_partials"`) using compact ordinal/numeric hash tables and `ColumnCache` decoded arrays rather than materializing matched rows into DataFusion.
- **Exact vs. Approximate Top-K Tradeoff:**
  - When `sql_approximate_top_k = false`, shards ship all partial groups to the coordinator for exact SQL aggregation (`approximate_top_k: false`).
  - When `sql_approximate_top_k = true` on `LIMIT` queries, shards prune partial buckets to `(offset + limit) * 3 + 10` (`shard_top_k`) and explicitly disclose `"approximate_top_k": true` in both query responses and `EXPLAIN ANALYZE`.
- **Reproducible Verification:**
  - `tests/sql_correctness.rs` and `tests/rest_api_integration.rs` (`fs047_050_scale_scheduling_mixed_schema_generations_tenant_isolation_and_research_evidence_suite`).

### 2.4 Hypothesis 4 — Unified Hot-Delta And Immutable-Split Snapshot Planning Across Schema Evolution

- **Mechanism (`FS-034`–`FS-043`, `FS-048`):**
  - Tiered indices (`lifecycle_mode: "tiered"`) ingest into replicated local hot-delta shards, seal bounded contiguous sequence ranges into immutable splits (`POST /{index}/_remote_store/seal`), commit contiguous `published_watermarks` (`ShardPublishedWatermark`), and resolve one pinned `ReadSnapshot` (`resolve_index_read_snapshot`) across hot delta (`_seq_no > W`) and immutable splits (`_seq_no <= W`) with deterministic `(seq_no, primary_term, version, split_order)` winner selection and tombstone supersession.
  - Additive schema evolution (`PUT /{index}/_mapping`, `ClusterCommand::AddMappings`, `FS-048`) advances a monotonic `schema_generation` and records historical generation fingerprints in `schema_history`. Readers evolve cached split schemas additively on open, materialize missing additive fields as typed `NULL` across DSL (`exists`, aggregations, sort) and SQL (quoted/unquoted projections and `GROUP BY`), reject incompatible field-type changes before metadata mutation, and upgrade older splits during compaction (`POST /{index}/_remote_store/compact`).

---

## 3. Distributed Correctness, Formal Models, And Operational Drills

| Area | Verification Artifact / Suite | Proven Invariants And Disclosed Bounds |
|---|---|---|
| Local Write Durability, OCC, Idempotency, and Failover (`FS-001`, `FS-009`–`FS-014`) | `./scripts/tla/check.sh` (`D1`, `D2`, `S1`, `G1`, `G2`, `FS013`), `tests/d1_protocol_trace.rs`, `tests/stale_primary_failover.rs` | Bounded TLC model checking (3 nodes, 1–2 concurrent writes, crash/restart/partition/storage-I/O faults) + real-Raft trace validation prove no acknowledged write loss, stale-primary fencing, and exact operation-ID replay/conflict detection. |
| Single-Sequencer Fenced Manifest Publication & GC (`FS-002`, `FS-015`–`FS-021`, `FS-039`–`FS-041`) | `specs/tla/MC_FS018_ManifestPublication.tla`, `src/storage/conditional.rs`, `src/storage/fs017_021_tests.rs`, `tests/rest_api_integration.rs` | Conditional pointer CAS, Raft-issued `writer_epoch` fencing, 4-process OS child crash/retry tests, acyclic `CompactionLineageRecord` replacement, and snapshot-lease-pinned mark-and-sweep GC (`run_janitor`) prevent lost splits and live-data deletion. |
| Snapshot Bootstrap & Bounded Recovery (`FS-022`–`FS-027`) | `tests/replication_integration.rs`, `tests/restart_regression.rs` | Versioned `ShardSnapshotManifest` + staged atomic target install (`<shard_dir>.staging` swap with rollback) + streamed WAL suffix recovery + complete USearch vector index rebuild without silent document caps. |
| Deadlines, Cancellation, Admission, and Resource Governance (`FS-028`–`FS-033`, `FS-044`–`FS-045`) | `src/worker.rs`, `src/storage/mod.rs`, `tests/rest_api_integration.rs` | Monotonic remaining-budget propagation, per-class `AdmissionController` with starvation-free aging promotion, `ResourceGovernor` byte pools, streaming 64 KiB bundle hydration with decompression-bomb rejection, and single-scan startup cache inventory rebuild. |
| Backup, Verify, Restore, Repair, and Online Lifecycle Migration (`FS-042`, `FS-043`) | `tests/rest_api_integration.rs` (`fs042_*`, `fs043_*`) | `ClusterBackupManifest` (`/_snapshot/{id}*`) snapshots and restores `MutableOnly`, `Tiered`, and `ImmutableOnly` indices with zero data loss (`RPO = 0` for committed snapshot state); `/_remote_store/repair` reconstructs from valid lineage or quarantines with `409`; `/_lifecycle/migrate` and `/_lifecycle/rollback` verify oracle equivalence across cutover and rollback. |
| Tenant-Aware Security, Resource Quotas, and Redacted Audit (`FS-049`) | `src/security/mod.rs`, `tests/rest_api_integration.rs` (`fs047_050_*`) | Principal and resolved target-index context (including body-routed `/_bulk`, `/_sql`, and semijoin subqueries) enforces index allow-lists across data, tasks (`/_tasks/{id}`), `_cat/*`, and global cache inventory; per-principal and per-index concurrency/byte quotas reject overload with `429 tenant_quota_exceeded_exception`; `GET /_security/audit` records structured redacted events; inline credentials in `object_store_uri` are rejected. |

---

## 4. Production V1 Gate Sign-Off And Explicit Deferrals

| Roadmap Gate | Status | Sign-Off Evidence | Explicitly Deferred Beyond V1 Cut |
|---|---|---|---|
| **Gate 0 — Architecture & Semantics Baseline** | **Signed Off** | ADRs `0001`–`0005`, [`opensearch-compatibility.json`](opensearch-compatibility.json), [`recovery-acceptance-matrix.md`](recovery-acceptance-matrix.md), [`reproducible-baselines.md`](reproducible-baselines.md). | None. |
| **Gate 1 — Correctness & Production Preview** | **Signed Off** | `FS-009`–`FS-033` implemented and verified in CI (`cargo test`, `./scripts/tla/check.sh`). | Cross-cluster replication (CCR). |
| **Gate 2 — Unified Near-Real-Time Object-Store Beta** | **Signed Off** | `FS-034`–`FS-043` implemented: hot-delta sealing, watermarks, cross-layer `ReadSnapshot`, tombstones, fenced compaction, snapshot-safe GC, backup/restore/repair, and online lifecycle migration/rollback. | True concurrent multi-writer manifest merging without a single index sequencer (single fenced sequencer with parallel split producers is the V1 design). |
| **Gate 3 — Scale, Isolation, & Efficiency** | **Signed Off** | `FS-044`–`FS-049` implemented: streaming hydration, unified cache governor, TTL-bounded leaf inventory, $O(S \cdot L)$ top-$K$ scheduling with deduplicated retry, mixed schema generations, and tenant quotas/redacted audit. | Remote-split k-NN vector execution and hotcache sidecar consumption on `remote_store` splits (explicitly rejected with `501` until post-V1). |
| **Gate 4 — Research Validation & Production V1** | **Signed Off** | `FS-050` reproducible ablation, oracle equivalence, formal TLA+ matrix, and operational recovery/migration drills in this package. | Arbitrary painless/script execution, cross-index joins beyond `IN (SELECT ...)` semijoins, and OpenSearch plugin ABI compatibility. |
