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

### SQL Endpoint Expectations
- `POST /{index}/_sql` must remain coordinator-safe like other search endpoints.
- Responses include an `execution_mode` field:
    - `tantivy_fast_fields` when the query runs from local shard fast fields without materializing full hits first
    - `materialized_hits_fallback` when SQL runs over gathered hits for compatibility
- Responses include a `planner` object showing pushed-down text match, structured filters, grouping columns, required columns, and whether residual predicates remained.
- Future SQL work should preserve these fields so manual testing can confirm whether a query stayed on the intended search-aware path.
- API docs and responses should make it clear that `materialized_hits_fallback` is a compatibility mode, while `tantivy_fast_fields` reflects the intended search-aware local fast-field path.

### Maintenance — src/api/index.rs
| HTTP | Path | Handler |
|------|------|---------|
| POST/GET | `/{index}/_refresh` | `refresh_index()` |
| POST/GET | `/{index}/_flush` | `flush_index()` |

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
