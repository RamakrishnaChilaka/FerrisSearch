<p align="center">
  <img src="docs/logo.png" alt="FerrisSearch" width="400">
</p>

# FerrisSearch

<p align="center">
  <strong>Search first. Analyze in place.</strong>
</p>

<p align="center">
  A distributed Rust search engine for full-text retrieval, search-aware SQL,
  vector search, and experimental object-store-native execution.
</p>

<p align="center">
  <a href="#five-minute-tour">Five-Minute Tour</a> ·
  <a href="#why-ferrissearch">Why FerrisSearch</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#current-maturity">Maturity</a> ·
  <a href="#roadmap">Roadmap</a> ·
  <a href="#documentation">Docs</a>
</p>

---

FerrisSearch explores a simple systems idea:

> Keep text matching, structured filtering, fast-field reads, and eligible
> partial aggregation close to the search index. Move compact results—not every
> matching row—across the cluster.

It combines [Tantivy](https://github.com/quickwit-oss/tantivy),
[Arrow](https://arrow.apache.org/),
[DataFusion](https://datafusion.apache.org/),
[openraft](https://github.com/datafuselabs/openraft),
[USearch](https://github.com/unum-cloud/usearch), and
[tonic](https://github.com/hyperium/tonic) in one service binary with no JVM.

FerrisSearch is **pre-1.0**. It has substantial working code and broad automated
coverage, but it is not yet a production-safe drop-in replacement for
OpenSearch. The [maturity section](#current-maturity) names the important gaps.

## Why FerrisSearch

### Search-aware SQL

```sql
SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes
FROM "hackernews"
WHERE text_match(title, 'rust')
GROUP BY author
HAVING posts > 5
ORDER BY avg_upvotes DESC
LIMIT 10;
```

Tantivy evaluates `text_match` and pushable filters. Eligible shards aggregate
directly from fast fields. The coordinator merges compact partial states,
applies final relational semantics, and reports how the query ran.

```json
{
  "execution_mode": "tantivy_grouped_partials",
  "streaming_used": false,
  "matched_hits": 12473,
  "truncated": false
}
```

DataFusion is the residual relational engine, not the text-search engine.
FerrisSearch keeps a materialized-hit path for compatibility shapes that cannot
yet stay columnar.

### One coordinator model

Any HTTP node can accept a request. FerrisSearch forwards leader-only metadata
mutations and shard-owned operations internally, so clients do not have to
discover the Raft leader or shard primary.

### Two useful execution paths—and one intended future

| Engine | What exists now | Write path | Query path |
|---|---|---|---|
| `local_shards` | Mutable Tantivy + USearch shards, WAL, routing, replica RPCs | Standard document and bulk APIs | Local/remote shard scatter-gather |
| `remote_store` | Shardless immutable split bundles and generation manifests in local or S3-compatible storage | Dedicated manual split publication API | Root/leaf pruning, assignment, hydration, cache, and merge |

The long-term architecture does not keep these as two independent products. It
turns mutable, near-real-time data into immutable object-store splits under one
version and snapshot model:

```text
durable mutable delta
  -> searchable refresh
  -> bounded split seal
  -> fenced manifest commit
  -> stateless query compute
  -> version-aware compaction
  -> snapshot-safe garbage collection
```

That lifecycle is the central 12-24 month roadmap, not a claim about the current
implementation.

## Five-Minute Tour

### Prerequisites

- A current stable Rust toolchain with Rust 2024 edition support
- `protoc`
- `curl`

### 1. Start a node

```bash
cargo run --release --bin ferrissearch
```

The REST API listens on `localhost:9200`; internal gRPC uses `localhost:9300`.

### 2. Create an index

```bash
curl -sS -X PUT 'http://localhost:9200/movies' \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{
  "engine": "local_shards",
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "title":  {"type": "text"},
      "genre":  {"type": "keyword"},
      "year":   {"type": "integer"},
      "rating": {"type": "float"}
    }
  }
}
JSON
```

Every index also has a built-in `body` text field. It collects the text values
of each document for `?q=` search. You can omit it from `properties`, even in
strict indices; an explicit `body` mapping must be exactly `{"type": "text"}`.
Top-level document keys that name metadata fields, such as `_id`, `_source`,
`_seq_no`, and `_primary_term`, are rejected with `400 mapper_parsing_exception`.

### 3. Index a small batch

```bash
curl -sS -X POST 'http://localhost:9200/movies/_bulk?refresh=true' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- <<'NDJSON'
{"index":{"_id":"1"}}
{"title":"Rust Never Sleeps","genre":"documentary","year":2024,"rating":8.7}
{"index":{"_id":"2"}}
{"title":"Memory Safety","genre":"documentary","year":2025,"rating":9.1}
{"index":{"_id":"3"}}
{"title":"Borrowed Time","genre":"drama","year":2023,"rating":8.2}
NDJSON
```

### 4. Search

```bash
curl -sS -X POST 'http://localhost:9200/movies/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "must": [{"match": {"title": "rust"}}],
        "filter": [{"range": {"rating": {"gte": 8.0}}}]
      }
    }
  }'
```

### 5. Analyze the matched set

```bash
curl -sS -X POST 'http://localhost:9200/movies/_sql?pretty' \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{
  "query": "SELECT genre, count(*) AS films, avg(rating) AS avg_rating FROM movies WHERE text_match(title, 'rust') GROUP BY genre ORDER BY avg_rating DESC"
}
JSON
```

### SQL console

```bash
cargo run --release --bin ferris-cli
cargo run --release --bin ferris-cli -- -c 'SHOW TABLES'
```

The interactive console supports multiline SQL, persistent history, completion,
`\watch`, streamed NDJSON results, and EXPLAIN metadata.

### Three-node development cluster

```bash
./dev_cluster_release.sh --nodes 3
```

Or run the debug nodes in separate terminals:

```bash
./dev_cluster.sh 1
./dev_cluster.sh 2
./dev_cluster.sh 3
```

### Docker

```bash
docker build -t ferrissearch .
docker run --rm -p 9200:9200 -p 9300:9300 ferrissearch
```

## What Works Today

### Search and analytics

- Query DSL: `match`, `term`, `bool`, `range`, `wildcard`, `prefix`, `fuzzy`,
  and `match_all`
- Numeric/date sorting and `search_after` cursor pagination, with documented
  tie limitations
- Terms, stats, min, max, average, sum, value-count, and histogram aggregations;
  numeric terms preserve full signed-integer bucket identity
- Declared keyword fields support nested scalar arrays: values are flattened,
  coerced to text, deduplicated per document, and preserved unchanged in
  `_source`; object elements are rejected
- Search-aware SQL with `text_match`, structured filter pushdown, grouping,
  `HAVING`, sorting, limit/offset, residual expressions, and same-index
  uncorrelated `IN (SELECT key ...)` semijoins
- Local and distributed fast-field Arrow execution
- Compact grouped partial aggregation
- k-NN vector search and Reciprocal Rank Fusion hybrid text/vector queries on
  the local-shard engine

SQL responses expose the current execution path:

| Mode | Meaning |
|---|---|
| `count_star_fast` | Metadata-only unfiltered `count(*)` |
| `tantivy_fast_fields` | Explicit columns read from fast fields |
| `tantivy_grouped_partials` | Search-native shard-local partial aggregation |
| `materialized_hits_fallback` | Compatibility path for unsupported columnar shapes |

Keyword arrays are searchable and aggregate correctly through the Query DSL.
Direct SQL fast-field readers are still scalar-first; FerrisSearch does not yet
claim general SQL ARRAY values or `UNNEST` support.

`sql_approximate_top_k` currently defaults to `true`. Eligible grouped
`ORDER BY metric LIMIT N` queries may prune shard-local buckets approximately;
responses and EXPLAIN metadata disclose activation. Set it to `false` when exact
coordinator-side merge semantics are required.

### Distribution and durability

- Raft-backed cluster membership, leader election, index metadata, mappings,
  settings, and dynamic security metadata
- Primary/replica shard routing over gRPC
- Generation-based binary translog with request or asynchronous durability
- Primary write receipts propagated to REST `_seq_no` responses, including bulk
  ranges and `_primary_term`, with replica WAL operation identity preservation
- Gap-aware processed and persisted checkpoints, with explicit `None` distinct
  from sequence zero and persisted-prefix global checkpoint calculation
- Bounded file-based peer recovery for initial, later-added, and rejoining replicas:
  gap-free committed-boundary installation, pinned physical-order WAL streaming
  that pauses at source-unapplied frames, processed-checkpoint finalization, a
  final replaying write barrier, and allocation-bound conditional in-sync
  admission
- Raft-owned shard-copy allocation IDs, durable local copy identity, and
  replica primary-term fencing before WAL mutation
- Fail-closed copy startup with immediate corruption reporting, bounded
  per-copy I/O retry/backoff, allocation-bound replica removal, and
  promote-only primary failover
- Proactive lifecycle activation of restarted or promoted primaries so idle
  pending recoveries do not depend on a later client write
- UUID-backed shard data directories and process-backed restart regression
- Separate rayon pools for search and write engine work
- Blocking wrappers for filesystem/recovery work on async call paths

### Operations and security

- Index settings, refresh, flush, asynchronous shard-local force merge, and task status
- Cluster health/state and `_cat` node, index, shard, master, and segment views
- Prometheus metrics and `EXPLAIN ANALYZE`
- Optional HTTP API-key authentication with role/index authorization
- Runtime API-key and custom-role management through `/_security/*`
- Optional client HTTP TLS and inter-node transport TLS through Cargo features

Security is disabled by default. FerrisSearch stores API-key hashes, never
plaintext secrets; dynamically generated secrets are returned only once.

On Linux, `column_cache_size_percent` is applied to host physical memory capped
by visible finite cgroup v2 `memory.max` or cgroup v1
`memory.limit_in_bytes` values, including tighter visible ancestors. Startup
exports `ferrissearch_column_cache_effective_memory_bytes` and
`ferrissearch_column_cache_budget_bytes`. This is only the shared column-cache
capacity; it is not a total-process memory limit.

`max_concurrent_peer_recoveries` limits target-side recovery sessions per node
(default `2`, maximum `64`). Set it to `0`, or set
`FERRISSEARCH_MAX_CONCURRENT_PEER_RECOVERIES=0`, to keep assigned replicas
`INITIALIZING` without automatic recovery. A new `local_shards` index therefore
starts with only its primary in the authoritative write set; initial replicas
join through peer recovery. Setting the limit to `0` keeps new indices
single-copy for acknowledgement and promotion purposes until recovery is
re-enabled and completes.

Local shard-storage I/O uses bounded retry/backoff before routing escalation.
`shard_io_failure_escalation_attempts` defaults to `3` and
`shard_io_failure_escalation_window_ms` defaults to `60000`; both thresholds
must be reached. The matching environment overrides are
`FERRISSEARCH_SHARD_IO_FAILURE_ESCALATION_ATTEMPTS` and
`FERRISSEARCH_SHARD_IO_FAILURE_ESCALATION_WINDOW_MS`. Corrupt storage metadata
fails closed immediately, while network/transfer failures do not consume this
local-storage budget.

When a failed primary has no live in-sync replacement, Raft records
`primary_unavailable` without changing its allocation or term. A write-only
failure keeps that red health status until the first later successful local
write clears it conditionally at the same term. A definitive or open-level
failure quarantines the copy after report throttling and clears the status only
after repaired storage completes a fresh primary activation.

This makes FerrisSearch red health intentionally broader than OpenSearch red
health. For example, a write-only fault can leave the assigned primary open and
serving reads while writes are unavailable. OpenSearch red means the affected
primary is unassigned, so that shard serves neither reads nor writes.

Because replica acknowledgement is synchronous, a persistent write fault on an
in-sync replica can fail every write to that shard for at least the default
60-second escalation window before that exact allocation is removed. Recovery
allocation can currently choose the same faulty node again; a
MaxRetryAllocationDecider-style exclusion policy and
`index.allocation.max_retries` setting are deferred.

FerrisSearch pre-1.0 does not migrate data or metadata from earlier builds.
Existing indices, shard directories, WALs, manifests, copy identities, Raft
logs/snapshots, and incompatible peer wire formats fail closed. Recreate
incompatible indices and reindex their source data. For incompatible Raft logs
or snapshots, wipe the node data directories and recreate the cluster. There is
no rolling mixed-version compatibility path.

For `local_shards`, each encoded WAL operation is limited to 32 MiB, including
the frame header and internal `_doc_id` / `_source` wrapper. The maximum usable
JSON document body is therefore slightly smaller and varies with the document
ID and serialized shape. Oversized single or bulk items are rejected before
WAL mutation. Restart, replay, and peer recovery enforce the same 32 MiB frame
limit.

Force merge keeps its asynchronous `202 Accepted` task lifecycle. A valid
`max_num_segments` is at least 1; each shard drains already-scheduled automatic
Tantivy merges, performs the requested merge under shard-local maintenance
coordination, and then restores its automatic merge policy.

### Experimental remote-store reads

`remote_store` indices can:

- publish deterministic Tantivy split bundles;
- store manifests and bundles on local filesystems or S3/S3-compatible storage;
- prune splits conservatively from exact term sets and numeric/date ranges;
- assign batches to data-node leaves with rendezvous affinity and cache/load
  preference;
- hydrate and checksum node-local artifacts;
- reuse open readers; and
- report pruning counters in search and SQL analysis responses.

Standard `_doc`, `_bulk`, `_update`, and `_delete` APIs return `501` for this
engine. Data is currently added through:

```http
POST /{index}/_remote_store/publish
{"docs":[{"_id":"a","title":"..."},{"_id":"b","title":"..."}]}
```

See the [RustFS live runbook](docs/remote-store-rustfs-live-runbook.md) and
`scripts/remote_store_rustfs_smoke.sh` for the S3-compatible path.

## Architecture

```mermaid
flowchart LR
    C[Client] --> H[Any HTTP coordinator]
    H --> R[Raft metadata]
    H --> L[local_shards primaries and replicas]
    H --> Q[remote_store root]
    Q --> M[Manifest generations in object storage]
    Q --> N[Data-node leaves]
    N --> O[Split artifact and reader caches]
    L --> T[Tantivy + USearch + WAL]
    N --> S[Tantivy split readers]
    T --> A[Hits / Arrow / partial aggregates]
    S --> A
    A --> H
```

### Responsibility split

| Layer | Responsibility |
|---|---|
| Raft control plane | Cluster membership, master, index identity, routing, mappings, settings, security records |
| Mutable data plane | WAL, primary mutation, replica apply, refresh, local Tantivy/USearch |
| Immutable data plane | Split bundles, manifests, schema summaries, object storage |
| Query plane | Tantivy matching/fast fields/partials, root/leaf fan-out, Arrow transport, residual DataFusion |
| Maintenance plane | Flush, force merge, cache reaping; compaction and object GC are roadmap work |

Split inventories, query assignments, and cache state do not belong in Raft.

## Current Maturity

FerrisSearch is a strong prototype and research platform. It is **not yet
production ready**. The most important limits are:

- A primary can mutate before replica acknowledgement fails; write retry and
  acknowledgement semantics need a formal contract.
- Synchronous WAL/fsync/engine failures fail the request and enter bounded
  escalation. An operation that reached the WAL and then failed engine apply
  has an unknown outcome. In production this failure means the Tantivy writer
  was killed: the next commit fails, the rebuilt writer replays the operation
  on this copy, and restart replay applies it too. When the failure is on the
  primary, replicas never receive the operation, because replication starts
  only after local success. In-sync copies can therefore diverge on up to one
  refresh interval of client-failed writes, or longer with refresh disabled,
  and peer recovery from this copy can ship the retained entry to a new copy.
  A partial frame followed by later writes can still create middle corruption
  that restart correctly rejects.
- A failed Tantivy commit invalidates the writer without advancing
  `translog.committed` or truncating the WAL. The next write, blocking
  maintenance operation, or peer-recovery snapshot rebuilds the writer and
  replays the retained suffix. Replay deletes each document ID first and adds
  content back only for index operations; malformed operation payloads fail
  closed. Persistent rebuild or replay I/O enters the Apply escalation budget
  only when a write triggers the rebuild. A rebuild triggered by refresh,
  flush, or snapshot preparation logs an error and retries on the next
  maintenance tick without escalating, so an idle copy with a persistent fault
  retries until a write arrives. Replay holds the shard translog lock for the
  entire suffix. Writes to that shard block while it runs and occupy
  write-pool threads, so a long replay can also delay writes to other shards
  on the node. The suffix can be large when refresh is disabled.
- At the `8f17172` main baseline, startup replay resurrected acknowledged
  deletes and one transient Tantivy commit failure could lose later
  acknowledged writes. Both defects are fixed on this branch.
- `_seq_no` and `_primary_term` report the primary-assigned operation identity,
  while `_version` remains a placeholder. Internal replica redelivery is
  sequence/term aware, but client retry tokens, `if_seq_no` /
  `if_primary_term`, and complete optimistic concurrency control are still
  missing.
- Replica bootstrap uses file snapshot plus physical-order WAL streaming, but
  source sessions and retention pins remain process-local and general D10
  rollback/resync is not implemented.
- Remote manifest publication is serialized only inside one process; there is
  no cross-process compare-and-set or writer fencing.
- Remote-store ingest is manual, not near-real-time, and there is no unified
  update/delete, compaction, retention, or object-store GC lifecycle.
- Cold split hydration buffers full bundles, and cache/query/background work do
  not share one complete resource budget.
- End-to-end cancellation, deadline propagation, and admission control are
  incomplete.
- OpenSearch compatibility is a tested subset, not a drop-in contract.
- Security is API-key based and opt-in; there is no SSO/OIDC/LDAP layer.

These are ranked as engineering work, not hidden as caveats:
[The Next 50 Engineering Tasks](docs/next-50-tasks.md).

## API Surface

FerrisSearch intentionally exposes an OpenSearch-style REST API **subset**.

| Area | Representative endpoints |
|---|---|
| Index | `PUT /{index}`, `DELETE /{index}`, `GET/PUT /{index}/_settings` |
| Documents | `POST/PUT /{index}/_doc`, `GET/DELETE /{index}/_doc/{id}`, `POST /{index}/_update/{id}` |
| Bulk | `POST /_bulk`, `POST /{index}/_bulk` |
| Search | `GET/POST /{index}/_search`, `GET/POST /{index}/_count` |
| SQL | `POST /{index}/_sql`, `/_sql`, `/{index}/_sql/stream`, `/_sql/stream`, `/{index}/_sql/explain` |
| Catalog | `/_cat/nodes`, `/_cat/indices`, `/_cat/shards`, `/_cat/segments`, `/_cat/master` |
| Maintenance | `/{index}/_refresh`, `/{index}/_flush`, `/{index}/_forcemerge`, `/_tasks/{id}` |
| Security | `/_security/api_key`, `/_security/role/{name}` |
| Remote store | `/{index}/_remote_store/publish`, `/{index}/_remote_store/verify` |
| Observability | `/_cluster/health`, `/_cluster/state`, `/_metrics` |

Unsupported behavior should fail explicitly rather than return
shape-compatible false success.

## Benchmarks And Evidence

FerrisSearch includes ingestion, search, grouped SQL, NYC Taxi, and remote-store
scripts. Existing results are exploratory single-machine measurements—not
service-level promises or proof of production scale.

- [Benchmark notes and reproduction](docs/benchmarks.md)
- [Terms aggregation optimization benchmark](docs/terms-aggregation-benchmark-2026-09-22.md)
- [NYC Taxi hybrid benchmark](docs/nyc-taxi-hybrid-benchmark.md)
- [Remote-store pruning design and evidence](docs/remote-store-split-pruning.md)
- [EXPLAIN ANALYZE output design](docs/explain-analyze-output.md)

Performance contributions should include raw results, hardware, dataset,
topology, build flags, query text, cache state, correctness checks, and resource
cost. The roadmap treats reproducibility and ablation studies as part of the
systems-research contribution.

## Testing

```bash
./scripts/ci-local.sh

# Or run focused suites:
cargo test --lib
cargo test --bin ferris-cli
cargo test --test consensus_integration
cargo test --test replication_integration
cargo test --test rest_api_integration
cargo test --test restart_regression
cargo test --test sql_correctness
```

The repository includes unit, transport, multi-node REST, process-backed
restart, SQL logic, security, remote-store, and feature-gated TLS coverage.
S3-compatible tests require the documented external RustFS endpoint and report
as skipped when it is absent.

## Roadmap

FerrisSearch is aiming at an object-store-native search analytics system, not
maximum OpenSearch endpoint count.

1. Freeze write, manifest, snapshot, lifecycle, and format semantics.
2. Make writes, publication, recovery, cancellation, and admission trustworthy.
3. Converge mutable ingest and immutable splits.
4. Add versioned updates/deletes, compaction, retention, and GC.
5. Validate scale and the research hypotheses with reproducible evidence.

Read:

- [Strategic Architecture Roadmap](docs/architecture-roadmap.md) — target
  architecture, invariants, gates, research agenda, and V1 definition
- [Next 50 Engineering Tasks](docs/next-50-tasks.md) — ranked, dependency-aware
  work packages and completion evidence

## Documentation

| Document | Purpose |
|---|---|
| [Architecture roadmap](docs/architecture-roadmap.md) | Canonical future direction and release gates |
| [AI agent guide](docs/ai-agent-guide.md) | Context, planning, validation, and handoff rules for GPT/Claude/Copilot |
| [Remote-store live runbook](docs/remote-store-rustfs-live-runbook.md) | Shared S3-compatible storage validation |
| [Dynamic settings](docs/dynamic-settings.md) | Refresh and flush-threshold behavior |
| [Vector search](docs/vector-search.md) | Current vector architecture and examples |
| [Benchmarks](docs/benchmarks.md) | Historical measurements and reproduction notes |
| [Dependency review](docs/dependency-review-2026-09-22.md) | Point-in-time direct dependency assessment and deferred migrations |

`docs/architecture.md` and `docs/remote-store-one-pager.md` are historical
context; they do not override current source or the strategic roadmap.

## Contributing

Start with [AGENTS.md](AGENTS.md) for repository context and
[the next-50 backlog](docs/next-50-tasks.md) for priority. A strong change:

- advances one coherent outcome;
- protects the documented invariants;
- includes result-level and failure-path tests;
- reports focused validation; and
- keeps current capabilities distinct from roadmap targets.

## License

[Apache-2.0](LICENSE)
