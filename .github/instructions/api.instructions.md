# API Module — src/api/

## Router (src/api/mod.rs)
Axum router with middleware for `?pretty` JSON formatting.

### Middleware
```rust
async fn pretty_json_middleware(req: Request<Body>, next: Next) -> Response
```
- When `?pretty` query param present, reformats JSON response with indentation
- Only applies to `application/json` responses
- Silently skips if parsing fails

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
| POST | `/{index}/_sql` | `search_sql()` — SQL over matched docs with planner metadata and execution mode |
| POST | `/{index}/_sql/explain` | `explain_sql()` | Explain SQL plan; with `"analyze": true`, execute and return plan + per-stage timings + rows |

### SQL Endpoint Expectations
- `POST /{index}/_sql` must remain coordinator-safe like other search endpoints.
- `POST /{index}/_sql/explain` returns the query plan without executing it — validates SQL, shows pushdown decisions, execution strategy, rewritten SQL, and the full pipeline stages.
- With `"analyze": true`, `explain_sql` executes the query fully and returns the plan JSON enriched with:
    - `timings` object: `planning_ms`, `search_ms`, `collect_ms`, `merge_ms`, `datafusion_ms`, `total_ms` (fractional milliseconds)
    - `rows` and `row_count`: the actual query results
    - `matched_hits`, `execution_mode`, `truncated`, `_shards`
- The `search_sql` handler internally delegates to `execute_sql_query()` — the same function used by EXPLAIN ANALYZE — so timing instrumentation is in one place.
- Responses include an `execution_mode` field:
    - `tantivy_grouped_partials` when an eligible `GROUP BY` query executes as shard-local grouped partial aggregation with coordinator merge
    - `tantivy_fast_fields` when the query runs from local shard fast fields without materializing full hits first
    - `materialized_hits_fallback` when SQL runs over gathered hits for compatibility
- Responses include a `planner` object showing pushed-down text match, structured filters, grouping columns, required columns, and whether residual predicates remained.
- Responses include a `truncated` boolean flag:
    - `true` only when the internal 100K collection ceiling silently drops matching documents.
    - `false` when the user specified an explicit `LIMIT` — they got what they asked for, that's not truncation.
    - Never `true` for grouped partials (they scan all matched docs via aggregation collectors).
- Future SQL work should preserve these fields so manual testing can confirm whether a query stayed on the intended search-aware path.
- API docs and responses should make it clear that `materialized_hits_fallback` is a compatibility mode, while `tantivy_grouped_partials` and `tantivy_fast_fields` reflect the intended search-aware execution paths.

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
