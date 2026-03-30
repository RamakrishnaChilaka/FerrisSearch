---
description: "Use when implementing hybrid SQL over search, planner pushdown, fast-field execution, Arrow batch building, DataFusion residual execution, or grouped analytics over matched docs."
applyTo: "src/hybrid/**,src/api/search.rs,src/engine/tantivy.rs"
---

# Hybrid SQL Module — src/hybrid/

## Architecture Boundary
- Tantivy is the search engine. It owns text matching, ranking, term/range pushdown, fast-field reads, and shard-local partial aggregation.
- Arrow is the in-memory columnar interchange layer for matched docs or merged partial results.
- DataFusion is the residual SQL engine. It owns projection, aliases, expression evaluation after pushdown, final sort, and final aggregate shaping.
- DataFusion must not become the primary text-search or full-scan execution engine.
- The target is a true hybrid planner, not a thin SQL layer over already materialized hits.

## Fast-Field First Rule
- Do not invent a new fast-field storage format for hybrid SQL features.
- Read Tantivy fast fields directly from Rust:
  - numeric: `segment_reader.fast_fields().f64(name)` / `.i64(name)`
  - keyword/string: `segment_reader.fast_fields().str(name)` with `term_ords(doc)` and `ord_to_str()`
  - `_id`: `segment_reader.fast_fields().str("_id")` — `_id` is `(STRING | STORED).set_fast(None)`, enabling fast-field columnar access
- Prefer fast fields or stored fields over `_source` materialization whenever the needed SQL columns are available columnarly.

## Stored Fields Optimization (sql_record_batch)
- When all requested SQL columns have fast-field readers (no `SourceFallback`), `sql_record_batch` reads `_id` from its fast-field column and skips `searcher.doc()` entirely.
- This avoids loading the full stored document (which includes the `_source` JSON blob) from disk for every hit in the SQL fast-field execution path.
- When any column requires `SourceFallback` (unmapped or non-fast field), the stored doc is loaded as before and `_id` is read from it.
- The `needs_stored_doc` flag is computed once per query by scanning all field plans for `SourceFallback`.

## _id/score Skip Optimization
- `QueryPlan` has `needs_id: bool` and `needs_score: bool` flags, detected by checking whether the SQL query references `_id` or `score`/`_score` in any projection, filter, GROUP BY, or ORDER BY.
- When `needs_id` is false, the engine skips reading `_id` from fast-field columns entirely and the Arrow bridge emits an empty-string `_id` column.
- When `needs_score` is false, the engine skips collecting scores and the Arrow bridge emits a zero-filled `score` column.
- The DataFusion table schema always includes `_id` and `score` columns for compatibility, but they contain dummy values when not referenced.
- Typical SQL queries like `SELECT category, avg(price) FROM ... GROUP BY category` benefit from this: zero `_id` reads across all matched docs.
- **Safety rule**: When `required_columns` is empty, `_id`/`score` are unreferenced, and the query is not grouped or `SELECT *`, `needs_score` is forced true as a fallback for edge cases like `SELECT 1 FROM ...`.

## Ungrouped Aggregate Pushdown
- SQL queries with only aggregate functions (no GROUP BY) now use the `tantivy_grouped_partials` execution path with zero group-by columns.
- `SELECT count(*), avg(price), sum(price) FROM ...` produces a single global bucket via fast-field collectors — no row materialization needed.
- The planner's `extract_grouped_sql_plan` no longer requires `group_by_columns` to be non-empty.
- With zero group-by columns, the engine produces one bucket key (`"[]"`) per segment, merged correctly by `merge_grouped_metrics_partials`.
- Queries with bare column references alongside aggregates (e.g., `SELECT price, count(*)`) correctly fall back — the bare column is not a valid aggregate.

## SQL LIMIT Pushdown
- When a SQL query has LIMIT/OFFSET and ORDER BY is `_score`-only (or absent), the limit is pushed into Tantivy's `TopDocs` collector.
- When ORDER BY is a single non-score fast-field column (e.g., `ORDER BY price DESC`), both the sort and limit are pushed into Tantivy via `TopDocs::order_by_fast_field()`. This avoids materializing 100K docs and sorting in DataFusion.
- **LIMIT pushdown applies to all query shapes including `SELECT *`.** The `selects_all_columns` flag does NOT block limit pushdown — it only determines whether the fast-field or materialized-fallback execution path is used. For `SELECT * LIMIT 4`, the engine collects only 4 docs (not 100K) and materializes only those 4.
- **LIMIT with GROUP BY does NOT push to Tantivy.** When `grouped_sql.is_some()`, limit is applied after merge+sort in `execute_grouped_partial_sql`. LIMIT and OFFSET are applied post-sort, post-HAVING, to the final merged grouped results.
- `QueryPlan` has `limit: Option<usize>`, `offset: Option<usize>`, `limit_pushed_down: bool`, and `sort_pushdown: Option<(String, bool)>` fields.
- `sort_pushdown` captures the single ORDER BY column name and direction (true = DESC) when eligible for fast-field sort pushdown.
- `to_search_request()` populates `SearchRequest.sort` from `sort_pushdown`, so the engine uses `order_by_fast_field()` in both `search_query()` and `sql_record_batch()`.
- Multi-column ORDER BY cannot be pushed — falls back to DataFusion sorting.
- `parse_limit_expr()` and `parse_offset_expr()` extract numeric values from `sqlparser::ast::LimitClause`.
- When limit is pushed down, the Tantivy search collects only `limit + offset` docs instead of the default 100K.
- The `limit_pushed_down` guard conditions are: `limit.is_some() && !has_residual_predicates && grouped_sql.is_none() && (is_order_by_score_only || sort_pushdown.is_some())`.

## Distributed Fast-Field SQL
- The `tantivy_fast_fields` path now works across multi-node clusters.
- For non-`SELECT *` queries, the coordinator scatters `SqlRecordBatch` gRPC RPCs to remote shards in parallel.
- Remote shards run `sql_record_batch()` locally and return Arrow IPC-serialized `RecordBatch` bytes.
- The coordinator deserializes Arrow IPC, concatenates batches from all shards (local + remote), and runs DataFusion for final SQL execution.
- Arrow IPC helpers: `record_batch_to_ipc()` and `record_batch_from_ipc()` in `arrow_bridge.rs`.
- `SELECT *` queries always use the materialized fallback path (`materialized_hits_fallback`) — the fast-field path requires explicit column projection.

## EXPLAIN ANALYZE
- `POST /{index}/_sql/explain` with `"analyze": true` executes the query and returns plan + per-stage timings + result rows.
- `SqlTimings` struct in `src/hybrid/mod.rs`: `planning_ms`, `search_ms`, `collect_ms`, `merge_ms`, `datafusion_ms`, `total_ms` (fractional milliseconds, `f64`).
- `execute_sql_query()` in `src/api/search.rs` is the shared internal function used by both `search_sql` and `explain_sql(analyze=true)`. Timing instrumentation is in this one place.
- The `search_sql` handler is a thin response formatter over `execute_sql_query()`.
- Without `"analyze": true`, `explain_sql` returns plan-only (no execution, no timings, no rows) — unchanged from the original behavior.

## DataFusion 53 LIMIT Workaround
- DataFusion 53.0.0 has a bug where `LIMIT` fetch-pushdown into `MemTable`'s `TableScan` is silently ignored when the SQL `SELECT` projects columns in a different order than the table schema.
- Example: schema `[base_passenger_fare, hvfhs_license_num]` + SQL `SELECT hvfhs_license_num, base_passenger_fare ... LIMIT 5` → returns all rows instead of 5.
- The same query with columns in schema order (`SELECT base_passenger_fare, hvfhs_license_num`) correctly returns 5 rows.
- **Workaround**: `project_batch_to_sql_columns()` in `datafusion_exec.rs` reorders the `RecordBatch` columns to match the SQL `SELECT` list order before creating the `MemTable`. This ensures DataFusion's projection is identity (no reorder), preventing the buggy optimizer path.
- The workaround uses `sqlparser` to extract the SELECT column order and all referenced column names (including ORDER BY, WHERE) from the rewritten SQL, then physically reorders the Arrow batch accordingly.
- DataFusion still handles LIMIT/OFFSET execution — we do NOT strip LIMIT from the SQL or apply it manually. The workaround only prevents the specific optimizer misfire.
- When DataFusion fixes this upstream, the workaround can be removed — the reordering is a no-op when projection already matches schema order.

## Planning Rules
- Split planning into two stages:
  1. search-aware planning in Tantivy
  2. residual SQL planning in DataFusion
- Push `text_match(...)`, exact filters (`=`), range filters (`>`, `>=`, `<`, `<=`), `BETWEEN`, and `IN` into Tantivy before Arrow/DataFusion execution.
- `BETWEEN a AND b` is pushed as `Range { gte: a, lte: b }` — reuses existing Range query support.
- `IN (a, b, c)` is pushed as `Bool { should: [Term(a), Term(b), Term(c)] }` — reuses existing Bool+Term support.
- `NOT IN` and `NOT BETWEEN` are NOT pushed — they remain as residual predicates for DataFusion.
- `score` and `_id` fields are never pushed (score is computed by Tantivy, _id is a doc identifier).
- **SELECT aliases are never pushed down.** If a WHERE predicate references a SELECT alias (e.g., `WHERE posts > 10` when `posts` is `count(*) AS posts`), it stays as a residual predicate for DataFusion. This prevents pushing filters on nonexistent Tantivy fields, which silently fall back to the `body` catch-all field and match nearly everything.
- Prefer shard-local partial execution before coordinator-side row materialization.
- Treat `materialized_hits_fallback` as a compatibility path, not the target architecture.

## HAVING Support
- `HAVING` clauses are supported in the `tantivy_grouped_partials` path as post-merge filters.
- Simple comparisons (`>`, `>=`, `<`, `<=`, `=`) on output columns (group columns or metric aliases) are converted to `HavingFilter` structs.
- HAVING filters are applied after shard-partial merge but before sorting and LIMIT/OFFSET.
- Multiple HAVING conditions (ANDed) are supported.
- Complex HAVING expressions (OR, subqueries, expressions) cause fallback to the DataFusion path.
- The pipeline order in grouped partials is: merge → HAVING → sort → LIMIT/OFFSET.

## Anti-Pattern To Avoid
- Do not advertise or implement the feature as "SQL over hits".
- Do not normalize coordinator-side row materialization as the primary path.
- Do not move search-native logic into DataFusion just because SQL is involved.

## Grouped Analytics Direction
- For grouped analytics over matched docs, prefer shard-local partial aggregation from fast fields.
- Ship compact partial states to the coordinator and merge there.
- Only fall back to row materialization when a query requires unsupported expressions, wildcard projection, or unavailable columnar data.

## Critical Invariants
- `doc_id` must remain the row index for any columnar representation built from matched docs.
- `score` must be surfaced as a normal Arrow/DataFusion column.
- Avoid extra maps or indirection unless there is a proven need.

## Execution Priorities
1. Tantivy search returns matched docs or shard-local partials.
2. Fast fields provide structured columns directly.
3. Arrow batches represent the matched tabular view.
4. DataFusion executes only the residual SQL semantics that still need a relational engine.

## Preferred Language In Reviews And Docs
- Say: "search-aware planning", "residual SQL execution", and "grouped analytics over matched docs".
- Avoid: "SQL over hits" unless you are explicitly describing the compatibility fallback path.

## Arrow Bridge Type Hints
- `build_record_batch_with_hints(column_store, type_hints)` accepts a `HashMap<String, ColumnKind>` to override data-driven type inference.
- When building Arrow batches from the fast-field path (`sql_record_batch`), callers MUST populate `type_hints` from `SqlFieldReader` variants or the Tantivy schema so that zero-result queries still produce correctly-typed columns (e.g. Float64 for price) instead of defaulting to Utf8.
- `infer_column_kind()` (data-driven inference) is only the fallback for columns without a type hint — it defaults to Utf8 when all values are null or the column is empty.
- The plain `build_record_batch()` (no hints) is used only by the `materialized_hits_fallback` path where schema info is unavailable.

## Testing Expectations
- Add unit tests for pushdown extraction, quoted index handling, grouped analytics planning, LIMIT pushdown, and residual predicate detection.
- Add execution tests for both:
  - `tantivy_grouped_partials`
  - `tantivy_fast_fields`
  - `materialized_hits_fallback`
- Add regression tests for zero-result or all-null columns: verify that schema-derived `type_hints` override `infer_column_kind` to produce correct Arrow DataTypes (e.g. Float64, not Utf8 for numeric columns).
- Add tests verifying that the fast-field path reads `_id` from the fast-field column (not from stored docs) when all columns are fast-field-backed.
- Add tests verifying that mixed fast+fallback column queries still return correct `_id` values.
- Live tests should inspect both returned rows and planner metadata.