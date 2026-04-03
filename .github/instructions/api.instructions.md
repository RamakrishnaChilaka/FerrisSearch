# API Module — src/api/

## Router (src/api/mod.rs)
Axum router with two middleware layers and a Prometheus metrics endpoint.

### Middleware
```rust
async fn pretty_json_middleware(req: Request<Body>, next: Next) -> Response
```
- When `?pretty` query param present, reformats JSON response with indentation
- Only applies to `application/json` responses
- Silently skips if parsing fails

```rust
async fn metrics_middleware(req: Request<Body>, next: Next) -> Response
```
- Records `ferrissearch_http_requests_total` (counter, labels: method/path/status) and `ferrissearch_http_request_duration_seconds` (histogram, labels: method/path) for every HTTP request.
- Path labels are normalized via `normalize_metrics_path()` to prevent high cardinality — index names become `{index}`, doc IDs become `{id}`, unknown paths become `{unknown}`.

### Metrics Endpoint
`GET /_metrics` returns all Prometheus metrics in text exposition format.
- Calls `update_cluster_metrics(state)` on each scrape to refresh cluster/shard/index gauges.
- Calls `initialize_metrics()` to force-register all `LazyLock` statics even before first traffic, so metrics appear in output from the first scrape.
- `INDEX_DOCS` gauge is reset before rebuilding to remove stale series from deleted indices.

### Metrics Placement Rules
- **HTTP-level latency**: `INDEX_LATENCY_SECONDS` and `SEARCH_LATENCY_SECONDS` use RAII `start_timer()` at the top of API handlers. These measure full request wall-clock time including routing, forwarding, and replication — NOT engine-level latency.
- **Engine-level counters**: `DOCS_INDEXED_TOTAL` and `BULK_DOCS_TOTAL` are incremented in the gRPC transport handler (`src/transport/server.rs`) after the engine write succeeds. This counts actual documents written, not API requests.
- **SQL counters**: `SQL_QUERIES_TOTAL` (with `mode` label) must be incremented on ALL exit paths from `execute_sql_query()` — including `count_star_fast` and `tantivy_grouped_partials` early returns. The `_sql_timer` histogram uses RAII drop so it auto-observes on all paths.
- **Bulk requests**: `BULK_REQUESTS_TOTAL` is incremented once per bulk API call in the HTTP handler. `BULK_DOCS_TOTAL` is incremented per-document in the gRPC handler.
- **Search queries**: `SEARCH_QUERIES_TOTAL` is incremented after successful distributed search in both `search_documents()` and `search_documents_dsl()`.

### Error Response Helper
```rust
pub fn error_response(
    status: StatusCode,
    error_type: &str,
    reason: impl std::fmt::Display,
) -> (StatusCode, Json<Value>) {
    (status, Json(json!({
        "error": { "type": error_type, "reason": reason.to_string() },
        "status": status.as_u16()
    })))
}
```

### Standard Error Types
| Error Type | When Used |
|-----------|-----------|
| `invalid_index_name_exception` | Index name validation failed |
| `resource_already_exists_exception` | Index already exists |
| `index_not_found_exception` | Index does not exist |
| `no_data_nodes_exception` | No data nodes available for shard allocation |
| `shard_not_available_exception` | Shard not open on this node |
| `node_not_found_exception` | Referenced node doesn't exist |
| `forward_exception` | gRPC forwarding to another node failed |
| `raft_write_exception` | Raft client_write command failed |
| `master_not_discovered_exception` | No master in cluster state |
| `group_by_scan_limit_exceeded` | GROUP BY fallback query matched more docs than `sql_group_by_scan_limit` |

## API Handlers

### Cluster & Catalog — src/api/cat.rs, src/api/cluster.rs (read-only, serve locally)
| HTTP | Path | Handler | Purpose |
|------|------|---------|---------|
| GET | `/` | `handle_root()` | Node info |
| GET | `/_cluster/health` | `get_health()` | Cluster health (green/yellow/red) |
| GET | `/_cluster/state` | `get_state()` | Full cluster state JSON |
| GET | `/_cat/nodes` | `cat_nodes()` | Tabular node listing (`?v` for headers) |
| GET | `/_cat/shards` | `cat_shards()` | Shard allocation (prirep=p/r, state, docs, node) |
| GET | `/_cat/indices` | `cat_indices()` | Index listing (health, shards, docs) |
| GET | `/_cat/master` | `cat_master()` | Current master node |

### Shard Display State
The `_cat/shards` endpoint shows three possible shard states:
- **`STARTED`**: The shard engine is open and serving docs.
- **`INITIALIZING`**: The shard is assigned to a live node but the shard engine isn't open yet (e.g., the shard is still being reopened from disk during startup). Other shards on the same node may already be `STARTED`.
- **`UNASSIGNED`**: The assigned node doesn't exist in the cluster (node left or shard not yet placed).

State is determined by `shard_display_state()` — a single function used for both primaries and replicas. In distributed mode (default), the state is derived from whether the shard appears in the `collect_shard_doc_counts()` fan-out results. In `?local` mode, it checks whether `shard_manager.get_shard()` returns the engine.

`INITIALIZING` is a runtime observation, NOT a cluster state change. The `ShardState` enum (`Started` / `Unassigned`) represents the Raft-managed allocation intent. The display state is the intersection of allocation intent + shard engine availability.

### Cat Endpoint Doc Count Collection
By default, `_cat/shards` and `_cat/indices` **fan out to all nodes** via gRPC `GetShardStats` to collect real doc counts (mirrors OpenSearch behavior). This ensures every shard row shows accurate doc counts regardless of which node is queried.

- **`?local`**: Falls back to local-only doc counts — shows counts for shards hosted on this node, `-` for remote shards. Useful for debugging or reducing overhead.
- The fan-out uses concurrent `tokio::spawn` for each remote node, with graceful degradation (failed RPCs are silently skipped, showing `0`).
### Index Management — src/api/index.rs (Raft writes → forward to leader)
| HTTP | Path | Handler |
|------|------|---------|
| HEAD | `/{index}` | `index_exists()` — 204 or 404 |
| PUT | `/{index}` | `create_index()` — with settings/mappings |
| DELETE | `/{index}` | `delete_index()` |
| GET | `/{index}/_settings` | `get_index_settings()` — local read |
| PUT | `/{index}/_settings` | `update_index_settings()` — forwarded to leader |
| POST | `/_cluster/transfer_master` | `transfer_master()` — forwarded |

### Document Operations — src/api/index.rs (routed to shard primary)
| HTTP | Path | Handler |
|------|------|---------|
| POST | `/{index}/_doc` | `index_document()` — auto-generate ID |
| PUT | `/{index}/_doc/{id}` | `index_document_with_id()` |
| GET | `/{index}/_doc/{id}` | `get_document()` |
| DELETE | `/{index}/_doc/{id}` | `delete_document()` |
| POST | `/{index}/_update/{id}` | `update_document()` — partial merge |
| POST | `/_bulk` | `bulk_index_global()` |
| POST | `/{index}/_bulk` | `bulk_index()` |

### Search — src/api/search.rs
| HTTP | Path | Handler |
|------|------|---------|
| GET | `/{index}/_search` | `search_documents()` — query-string (q=, size, from) |
| POST | `/{index}/_search` | `search_documents_dsl()` — DSL body (SearchRequest) |
| GET/POST | `/{index}/_count` | `count_documents()` — document count (match_all or query body) |
| POST | `/{index}/_sql` | `search_sql()` — SQL over matched docs with planner metadata and execution mode |
| POST | `/{index}/_sql/explain` | `explain_sql()` | Explain SQL plan; with `"analyze": true`, execute and return plan + per-stage timings + rows |
| POST | `/_sql` | `global_sql()` — global SQL endpoint: SHOW TABLES, DESCRIBE, SHOW CREATE TABLE, and SELECT (index auto-extracted from FROM clause) |

### Global SQL Endpoint
- `POST /_sql` handles SQL commands that don't require an index in the URL path.
- Supported commands:
    - `SHOW TABLES` / `SHOW INDICES` — lists all indices with doc counts, shards, replicas, field count
    - `DESCRIBE <index>` / `DESC <index>` — shows field names and types for an index
    - `SHOW CREATE TABLE <index>` — returns settings + mappings JSON for recreating the index
    - `SELECT ... FROM "index" ...` — auto-extracts index name from the FROM clause and routes to `execute_sql_query()`
- Helper functions: `matches_command()`, `strip_command()`, `unquote_identifier()`, `extract_index_from_sql()`
- All commands are case-insensitive and handle optional trailing semicolons and quoted identifiers

### SQL Endpoint Expectations
- `POST /{index}/_sql` must remain coordinator-safe like other search endpoints.
- `POST /{index}/_sql/explain` returns the query plan without executing it — validates SQL, shows pushdown decisions, execution strategy, rewritten SQL, and the full pipeline stages.
- With `"analyze": true`, `explain_sql` executes the query fully and returns the plan JSON enriched with:
    - `timings` object: `planning_ms`, `search_ms`, `collect_ms`, `merge_ms`, `datafusion_ms`, `total_ms` (fractional milliseconds)
    - `rows` and `row_count`: the actual query results
    - `matched_hits`, `execution_mode`, `truncated`, `_shards`
- The `search_sql` handler internally delegates to `execute_sql_query()` — the same function used by EXPLAIN ANALYZE — so timing instrumentation is in one place.
- Responses include an `execution_mode` field:
    - `count_star_fast` when `SELECT count(*) FROM ...` (no WHERE/GROUP BY) is answered from `doc_count()` metadata without scanning documents
    - `tantivy_grouped_partials` when an eligible `GROUP BY` query executes as shard-local grouped partial aggregation with coordinator merge
    - `tantivy_fast_fields` when the query runs from local shard fast fields without materializing full hits first
    - `materialized_hits_fallback` when SQL runs over gathered hits for compatibility
- Responses include a `planner` object showing the compatibility `text_match` field, the full `text_matches` array, structured filters, grouping columns, required columns, and whether residual predicates remained.
- Responses include a `truncated` boolean flag:
    - `true` only when the flat-query internal 100K collection ceiling silently drops matching documents on the `tantivy_fast_fields` path.
    - `false` when the user specified an explicit `LIMIT` — they got what they asked for, that's not truncation.
    - `false` for GROUP BY fast-field fallbacks — those use `sql_group_by_scan_limit` and return `group_by_scan_limit_exceeded` instead of truncated aggregates when capped.
    - Never `true` for grouped partials (they scan all matched docs via aggregation collectors).
- Future SQL work should preserve these fields so manual testing can confirm whether a query stayed on the intended search-aware path.
- API docs and responses should make it clear that `materialized_hits_fallback` is a compatibility mode, while `tantivy_grouped_partials` and `tantivy_fast_fields` reflect the intended search-aware execution paths.
- `explain_sql(analyze=true)` must treat JSON serialization failures as API errors, not panic paths.
- The `count_star_fast` metadata path should batch remote `GetShardStats` fan-out per node, not per shard, since one RPC already returns all shard stats for that node.

### Maintenance — src/api/index.rs
| HTTP | Path | Handler |
|------|------|---------|
| POST/GET | `/{index}/_refresh` | `refresh_index()` — fans out to all nodes |
| POST/GET | `/{index}/_flush` | `flush_index()` — fans out to all nodes |

### Refresh/Flush Fan-Out
Both `refresh_index()` and `flush_index()` fan out to ALL nodes via `fan_out_maintenance()`:
- **Local shards**: iterated via `ensure_local_index_shards_open()` and executed directly
- **Remote nodes**: concurrent `tokio::spawn` per node via gRPC `RefreshIndex`/`FlushIndex` RPCs
- The gRPC handlers use `run_maintenance_on_assigned_shards()` which checks the routing table — only shards where this node is primary or replica are operated on (orphaned shards are skipped)
- Response: `{"_shards": {"total": N, "successful": M, "failed": F}}`

## RefreshParam
```rust
pub struct RefreshParam { pub refresh: Option<String> }
// ?refresh=true or ?refresh (empty) → forces refresh after write
```
Used by: index, update, delete, bulk endpoints.

## Bulk Index Parsing
`parse_bulk_ndjson(text)` supports:
- **OpenSearch format**: action line `{"index": {"_index": "idx", "_id": "1"}}` + document line
- **Legacy format**: `_id` or `_doc_id` in document body
- **`_source` wrapper**: unwrapped before storage
- **Missing IDs**: UUID auto-generated

## Auto-Create Index (Coordinator Pattern)
Document and bulk handlers auto-create missing indices via `auto_create_index()`. This helper:
- Checks `raft.is_leader()` before writing
- If NOT leader → forwards `CreateIndex` to master via `forward_create_index()` gRPC
- If leader → commits directly via `raft.client_write(CreateIndex)`
- NEVER calls `raft.client_write()` from a follower node

All four auto-create callsites use this shared helper:
- `index_document()` (POST `/{index}/_doc`)
- `index_document_with_id()` (PUT `/{index}/_doc/{id}`)
- `bulk_index_global()` (POST `/_bulk`)
- `bulk_index()` (POST `/{index}/_bulk`)

## Coordinator Pattern (CRITICAL)
**Every node is a coordinator.** See copilot-instructions.md for the full pattern.
NEVER return "not the leader" errors — always forward transparently.
