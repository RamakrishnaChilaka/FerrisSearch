# OpenSearch Compatibility Matrix

> - **Status:** Current implemented behavior
> - **Matrix version:** 2026-10-15
> - **Source baseline:** `d1aa226` plus FS-014
> - **Product claim:** OpenSearch-style REST API subset
> - **Canonical inventory:** [`opensearch-compatibility.json`](opensearch-compatibility.json)

FerrisSearch is pre-1.0 and is not a drop-in OpenSearch replacement. This
matrix records the public HTTP routes implemented at the source baseline, the
important semantic differences that affect clients, and high-value APIs that
are not implemented.

The JSON inventory is canonical and machine-checked against
`src/api/mod.rs`. Run:

```bash
python3 scripts/check_opensearch_compatibility.py
```

CI runs the same command. The check fails when a routed method/path pair is
missing from the inventory, an inventory route is no longer routed, an
unsupported route becomes active, a status is invalid, or an evidence path is
missing.

## Classification

| Status | Meaning |
|---|---|
| **Supported** | Compatible for the explicitly documented request and response surface. This does not imply unlisted parameters or adjacent APIs. |
| **Partial** | The route exists, but accepted fields or semantics are narrower or observably different. |
| **Intentionally different** | FerrisSearch-specific API or a deliberate contract that is not OpenSearch compatible. |
| **Unsupported** | No supported route or semantic contract exists. |

## Client-Critical Semantic Differences

### Writes, versions, and conflicts

- Successful single and bulk mutations return the primary WAL-assigned
  `_seq_no`, `_primary_term`, and internal per-document `_version`. Sequence
  zero is valid; document versions start at 1 and increase across
  index/update/delete, including `not_found` deletes. Detected update no-ops
  retain the current metadata. Client-operation IDs provide bounded shard-local
  retry identity; they are not an unbounded exactly-once ledger. External
  version preconditions remain unsupported.
- Index and delete accept paired `if_seq_no` and `if_primary_term`. Supplying
  only one is a 400. Create-only writes use `op_type=create` or
  `PUT/POST /{index}/_create/{id}` and return 409 without allocating a sequence
  on conflict.
- Update supports `doc`, `upsert`, `doc_as_upsert`, `detect_noop`, and
  `retry_on_conflict`. It performs realtime primary GET plus CAS. Scripts are
  rejected.
- Write parameters including `routing`, `pipeline`, `version`,
  `version_type`, `require_alias`, `require_data_stream`, and
  `dynamic_templates` fail with 400 instead of being ignored.
  `wait_for_active_shards` accepts only absent or `1`, not `all`.
- A pre-execution rejection has no receipt. A dispatched write whose outcome
  cannot be proven returns `write_outcome_unknown`; clients must not treat that
  as safe to retry.
- A successful data certificate requires every captured in-sync copy's exact
  proof or observed, settled conditional exclusion and the configured
  `minimum_durable_copies` floor (default one). WAL/post-WAL uncertainty
  immediately stops the copy; restart/promotion acquires fresh canonical
  authority. These bounded guarantees are not full OpenSearch recovery parity.

Evidence:
`src/api/index/mod.rs`,
`src/api/index/bulk.rs`,
`tests/rest_api_integration.rs#writes_regression_conditions_create_upsert_and_noop`,
and
`tests/replication_integration.rs#writes_regression_conditional_rest_updates_and_mixed_bulk_keep_replica_documents_and_sequences_identical`.

### Refresh and visibility

- Document and bulk URLs accept absent/empty refresh, `refresh=true`, and
  `refresh=false`. `refresh=wait_for` is rejected because sequence-covering
  listeners are not implemented.
- `refresh=true` performs a bounded primary-owned visibility round for the
  primary and authoritative in-sync replicas. Bulk performs one round per
  mutated shard after item execution, not one per item.
- Write `_shards` counts describe actual data-copy acknowledgements and
  committed exclusions. They are not overwritten by visibility results.
- A bounded post-ack refresh failure keeps the write acknowledged and reports
  failures in the separate `_refresh` object; it does not turn a committed mutation into a failed
  bulk item. This differs from OpenSearch refresh-failure handling.
- `GET/POST /{index}/_refresh` and `GET/POST /{index}/_flush` fan out across
  assigned copies and return a small shard-count response.

Evidence:
`src/api/index/refresh_tests.rs`,
`src/api/index/maintenance.rs`,
and [ADR 0001 D8](adr/0001-write-consistency-and-retry-contract.md#d8-visibility).

### Bulk parsing and failures

- The NDJSON parser supports `index`, `create`, `update`, and `delete` actions.
  It validates streamed input in protected temporary storage before bounded
  execution and emits ordered results without whole-response buffering.
- Malformed action structure, missing required source lines, and unsupported
  safety metadata reject the whole request before mutation. Invalid document
  sources and mapper errors remain per-item failures, so valid neighboring
  items can execute.
- The index-scoped route supplies only a default index. An action `_index`
  override is still validated, authorized, and honored.
- Delete of a missing document can return item status 404 without setting
  top-level `errors`; `errors` reflects item error objects.
- Infrastructure, replication, and ambiguous outcomes preserve their
  underlying cause instead of becoming generic item failures.
- FerrisSearch-specific admission, input/spool/line limits, and cancellation
  are described in [bounded bulk ingestion](bulk-ingestion.md). Bulk is not
  atomic across shard RPC units. Post-dispatch spool failures remain
  indeterminate; failed response reads abort the body.

Evidence:
`src/api/index/bulk.rs`,
`src/api/index/bulk/pipeline.rs`,
`tests/rest_api/bulk_streaming.rs`,
and `src/api/index/fs014_tests.rs`.

### Search, count, and partial failures

- Query-string search is a Tantivy-backed subset, not full Lucene syntax.
  Standalone `*:*`, field presence, and typed numeric/date conversion have
  explicitly tested FerrisSearch rules.
- POST search supports match, term/terms, range, bool, wildcard, prefix,
  fuzzy, selected aggregations, k-NN, hybrid RRF, sort, and `search_after`.
  The primary sort value must be unique for globally complete deep pagination
  when ties exist; multi-key collection is not yet global-top-K complete.
- Partial shard failures return HTTP 200 with `_shards.failed` and failure
  details. If every shard fails, the request returns
  `search_phase_execution_exception`; client parse failures are 400,
  unavailable targets 503, and other engine failures 500.
  `allow_partial_search_results=false` is not implemented.
- `_count` supports match-all and POST query bodies. `_count?q=...` and
  `_msearch` are unsupported.
- Remote-store search is shardless and can report conservative split-pruning
  counters. A failed split is not silently dropped.

Evidence:
`.github/instructions/search.instructions.md`,
`tests/rest_api_integration.rs#rest_qsearch_partial_remote_failure_stays_200_and_keeps_healthy_hits`,
and
`tests/rest_api_integration.rs#rest_search_after_total_and_aggs_invariant_across_pages`.

### SQL

`/_sql`, `/{index}/_sql`, streaming, and explain routes are FerrisSearch APIs,
not the OpenSearch SQL plugin contract. They expose search-aware execution
metadata including `execution_mode`, `planner`, `matched_hits`, `_shards`,
`truncated`, and optional timing/pruning data. Streaming responses are NDJSON
with one `meta` frame followed by `rows` frames. Supported global commands are
`SHOW TABLES`, `SHOW INDICES`, `DESCRIBE`, `SHOW CREATE TABLE`, and
`SELECT ... FROM`.

Evidence:
`src/api/search/mod.rs`,
`tests/sql_correctness.rs`, and
`tests/rest_api_integration.rs#rest_sql_stream_endpoint_returns_ndjson_frames`.

### Cluster, catalog, security, and maintenance

- FerrisSearch red health includes an assigned primary marked
  storage-unavailable even when that copy can still serve reads. OpenSearch red
  normally denotes an unassigned primary.
- `/_cluster/state` exposes the FerrisSearch Raft state schema, not the
  OpenSearch cluster-state schema. `/_cluster/transfer_master` and `/_metrics`
  are FerrisSearch extensions.
- `_cat` routes implement selected columns. Distributed count collection can
  degrade to missing/zero values when a node cannot be queried.
- `/_security/*` is FerrisSearch's opt-in API-key and custom-role control
  plane. It is not the OpenSearch Security plugin API and does not provide
  SSO, OIDC, LDAP, certificates-as-identities, or the plugin's resource model.
- Force merge is always asynchronous, returns 202 plus a coordinator-local
  task ID, and supports only `max_num_segments`. Task records are not
  Raft-replicated, so status must be queried from the same coordinator.

Evidence:
`src/api/cluster.rs`,
`src/api/cat.rs`,
`src/api/security.rs`,
`src/api/index/maintenance.rs`, and
`tests/rest_api_integration.rs`.

## Routed Endpoint Inventory

The status applies only to the scope described in the canonical JSON entry.

| Methods | Path | Status |
|---|---|---|
| `GET` | `/` | Partial |
| `GET` | `/_cluster/health` | Partial |
| `GET` | `/_cluster/state` | Intentionally different |
| `POST` | `/_cluster/transfer_master` | Intentionally different |
| `GET` | `/_metrics` | Intentionally different |
| `GET` | `/_cat/nodes` | Partial |
| `GET` | `/_cat/shards` | Partial |
| `GET` | `/_cat/indices` | Partial |
| `GET` | `/_cat/master` | Partial |
| `GET` | `/_cat/segments` | Partial |
| `GET` | `/_tasks/{task_id}` | Partial |
| `HEAD` | `/{index}` | Supported |
| `PUT` | `/{index}` | Partial |
| `DELETE` | `/{index}` | Partial |
| `GET` | `/{index}/_settings` | Partial |
| `PUT` | `/{index}/_settings` | Partial |
| `POST` | `/{index}/_remote_store/publish` | Intentionally different |
| `POST` | `/{index}/_remote_store/verify` | Intentionally different |
| `POST` | `/{index}/_doc` | Partial |
| `PUT`, `POST` | `/{index}/_doc/{id}` | Partial |
| `GET` | `/{index}/_doc/{id}` | Partial |
| `DELETE` | `/{index}/_doc/{id}` | Partial |
| `POST` | `/{index}/_update/{id}` | Partial |
| `PUT`, `POST` | `/{index}/_create/{id}` | Partial |
| `POST` | `/_bulk` | Partial |
| `POST` | `/{index}/_bulk` | Partial |
| `GET` | `/{index}/_search` | Partial |
| `POST` | `/{index}/_search` | Partial |
| `GET`, `POST` | `/{index}/_count` | Partial |
| `POST` | `/_sql` | Intentionally different |
| `POST` | `/_sql/stream` | Intentionally different |
| `POST` | `/{index}/_sql` | Intentionally different |
| `POST` | `/{index}/_sql/stream` | Intentionally different |
| `POST` | `/{index}/_sql/explain` | Intentionally different |
| `POST` | `/_security/api_key` | Intentionally different |
| `GET` | `/_security/api_key` | Intentionally different |
| `GET` | `/_security/api_key/{id}` | Intentionally different |
| `DELETE` | `/_security/api_key/{id}` | Intentionally different |
| `GET` | `/_security/role` | Intentionally different |
| `PUT` | `/_security/role/{name}` | Intentionally different |
| `GET` | `/_security/role/{name}` | Intentionally different |
| `DELETE` | `/_security/role/{name}` | Intentionally different |
| `GET`, `POST` | `/{index}/_refresh` | Partial |
| `GET`, `POST` | `/{index}/_flush` | Partial |
| `POST` | `/{index}/_forcemerge` | Intentionally different |

## Important Unsupported Surfaces

| Surface | Examples | Migration impact |
|---|---|---|
| Mapping updates | `PUT /{index}/_mapping` | No public route; current dynamic mapping is additive and internal. |
| Multi-search and count query string | `/_msearch`, `/{index}/_count?q=...` | Submit supported searches individually and use POST count bodies. |
| Aliases | `/_aliases`, `/{index}/_alias/*` | No alias-based atomic cutover or routing. |
| Index templates and data streams | `/_index_template/*`, `/_data_stream/*` | Provision concrete indices explicitly. |
| PIT, scroll, and named snapshots | point-in-time, `/_search/scroll` | Only request-scoped current readers exist; no public stable multi-request snapshot. |
| Snapshot/restore repositories | `/_snapshot/*` | No OpenSearch backup/restore API. |
| Index lifecycle management | `/_plugins/_ism/*` | No ISM/ILM policy engine. |
| OpenSearch Security plugin | `/_plugins/_security/api/*` | FerrisSearch's smaller `/_security/*` model is not wire compatible. |
| Full write parameter surface | routing, pipelines, external versions, aliases, data streams | Safety-relevant unsupported parameters fail explicitly. |
| Full Query DSL and aggregations | scripts, nested/parent-child, composite, top_hits, full Lucene syntax | Rewrite queries to the supported subset and verify result semantics. |

The machine-readable inventory contains the exact representative unsupported
method/path pairs and evidence references.

## Maintenance Contract

Update the JSON and this document in the same change whenever a public route or
listed semantic contract changes. Add or update result-level tests before
raising a classification. A route's existence alone is not evidence for
**Supported** status.

Do not broaden the repository's public claim beyond "OpenSearch-style REST API
subset" without a reviewed matrix version whose evidence covers the stronger
claim.
