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
- When `_id` is requested and no stored docs are needed, the flat fast-field path should project `_id` through the same per-segment Arrow array builder and reorder flow as ordinary string fast fields rather than building a separate top-doc-order `Vec<String>`.
- When any column requires `SourceFallback` (unmapped or non-fast field), the stored doc is loaded as before and `_id` is read from it.
- The `needs_stored_doc` flag is computed once per query by scanning all field plans for `SourceFallback`.

## _id/_score Skip Optimization
- `QueryPlan` has `needs_id: bool` and `needs_score: bool` flags, detected by checking whether the SQL query references `_id` or synthetic `_score` in any projection, filter, GROUP BY, or ORDER BY.
- Planner-side synthetic column classification should go through one internal helper/enum instead of repeated raw string comparisons, so `_id` / `_score` exclusions stay aligned across pushdown guards, required-column extraction, and GROUP BY eligibility checks.
- When `needs_id` is false, the engine skips reading `_id` from fast-field columns entirely and the Arrow bridge emits an empty-string `_id` column.
- When `required_columns` is empty but `_id` and/or `_score` are requested, the engine may still stay on the fast path as long as no stored-doc fallback is required.
- When `needs_score` is false, the engine skips collecting scores and the Arrow bridge emits a zero-filled `_score` column.
- The DataFusion table schema always includes `_id` and `_score` columns for compatibility, but they contain dummy values when not referenced.
- Typical SQL queries like `SELECT category, avg(price) FROM ... GROUP BY category` benefit from this: zero `_id` reads across all matched docs.
- Zero-column queries such as `SELECT 1 FROM ... WHERE text_match(...)` should not fake a `_score` dependency. `sql_record_batch()` must still preserve one output row per hit when `required_columns` is empty and neither `_id` nor `_score` is referenced.

## Ungrouped Aggregate Pushdown
- SQL queries with only aggregate functions (no GROUP BY) now use the `tantivy_grouped_partials` execution path with zero group-by columns.
- Aggregate arguments on the grouped-partials path may be nested per-doc arithmetic trees over numeric fast fields, not just a single `a op b` pair. Shapes like `SUM(a + b)`, `AVG(a / b)`, and `AVG((a - b) / a)` should stay on `tantivy_grouped_partials` when every leaf is a fast-field-backed numeric source column.

## Residual Expression Tree (ResidualExpr)
- SELECT items that wrap or combine aggregates (e.g. `ROUND(AVG(x), 2)`, `AVG(x) + AVG(y)`, `SUM(a) / COUNT(*)`) are pushed down to `tantivy_grouped_partials` by extracting each inner aggregate as a hidden metric and building a `ResidualExpr` tree for post-merge evaluation.
- `ResidualExpr` variants: `MetricRef(name)`, `Literal(f64)`, `Round(expr, precision)`, `CastFloat(expr)`, `CastInt(expr)`, `BinaryOp(left, op, right)`, `Negate(expr)`.
- `ResidualBinOp`: `Add`, `Sub`, `Mul`, `Div`.
- `extract_projected_metric()` is the entry point: tries bare aggregate first, then calls `try_build_residual_expr()` which recursively walks the SQL AST extracting aggregates and building the expression tree.
- Metrics with `residual_expr: Some(...)` are filtered out of the Tantivy `GroupedMetricAgg` parameters — they have no real field to aggregate on.
- `eval_residual_expr()` / `eval_residual_f64()` evaluates the tree against resolved metric values in a merged bucket.
- Bare `MetricRef` aliases for date-backed `MIN` / `MAX` metrics must preserve the canonical metric's ISO 8601 final-row formatting. Do not route duplicate projected aliases back through numeric-only rendering.
- `bucket_metric_f64()` also evaluates residual expressions for ORDER BY and HAVING comparisons.
- Integer type preservation: when the result has no fractional part and the expression tree doesn't contain float-producing operations (ROUND, CastFloat, division, AVG, SUM), the value is returned as `i64` rather than `f64`.
- `has_ungrouped_aggregate_fallback` is a safety net for truly unparseable aggregate expressions that the residual extractor cannot handle. It forces the scan limit to `sql_group_by_scan_limit` and errors on truncation.

## Scan Limits and GROUP BY Correctness
- **Flat `tantivy_fast_fields` queries** (no GROUP BY): `SQL_MATCH_LIMIT = 100_000` caps `TopDocs` collection unless `LIMIT` pushdown applies. Truncation only affects completeness — individual rows are correct. The response includes `truncated: true`. The CLI renders a `⚠ TRUNCATED` warning.
- **GROUP BY on grouped_partials path**: No scan limit. Aggregation collectors process ALL matched docs per shard.
- **GROUP BY on fast-fields fallback**: The planner sets `has_group_by_fallback = true` when GROUP BY exists but `grouped_sql` is `None` (expression GROUP BY, unsupported aggregates, residual predicates). The API layer overrides `SearchRequest.size` from 100K to `sql_group_by_scan_limit` (default: 1M, configurable via `ferrissearch.yml`, 0 = unlimited).
- **Guarded local streaming**: Local GROUP BY fallback queries may use `sql_streaming_batches()` only when they are `_score`-free and every requested column is fast-field-backed on every local segment. If any column needs `SourceFallback`/stored-doc loading or the SQL query references `_score`, stay on `sql_record_batch()` (or the broader fallback path) so results remain correct.
- **Error on overflow**: If a GROUP BY fallback query still collects fewer rows than it matched after guarded local streaming is applied, the API returns HTTP 400 `group_by_scan_limit_exceeded` with an actionable error message instead of silently wrong aggregates. This is fundamentally different from flat-query truncation: GROUP BY over incomplete data produces wrong counts, missing groups, and biased averages.
- **Why not raise the flat-query limit too?** Flat query truncation makes individual rows correct but incomplete. GROUP BY truncation makes aggregates mathematically wrong. These are different severity levels requiring different treatment.
- **ClickHouse comparison**: ClickHouse has no scan limit — it processes all matching rows for GROUP BY. It uses `max_rows_to_group_by` to limit output cardinality and `max_bytes_before_external_group_by` to spill to disk when memory is tight. Our approach is the right trade-off for a search engine that isn't designed for full-table OLAP scans.
- `SELECT count(*), avg(price), sum(price) FROM ...` produces a single global bucket via fast-field collectors — no row materialization needed.
- The planner's `extract_grouped_sql_plan` no longer requires `group_by_columns` to be non-empty.
- With zero group-by columns, the engine produces one bucket key (`"[]"`) per segment, merged correctly by `merge_grouped_metrics_partials`.
- Queries with bare column references alongside aggregates (e.g., `SELECT price, count(*)`) correctly fall back — the bare column is not a valid aggregate.

## SQL LIMIT Pushdown
- When a SQL query has LIMIT/OFFSET and ORDER BY is `_score`-only (or absent), the limit is pushed into Tantivy's `TopDocs` collector.
- When ORDER BY is a single non-score fast-field column (e.g., `ORDER BY price DESC`), both the sort and limit are pushed into Tantivy via `TopDocs::order_by_fast_field()`. This avoids materializing 100K docs and sorting in DataFusion.
- **LIMIT pushdown applies to all query shapes including `SELECT *`.** The `selects_all_columns` flag does NOT block limit pushdown — it only determines whether the fast-field or materialized-fallback execution path is used. For `SELECT * LIMIT 4`, the engine collects only 4 docs (not 100K) and materializes only those 4.
- **LIMIT with GROUP BY does NOT push to Tantivy.** When `grouped_sql.is_some()`, limit is applied after merge in `execute_grouped_partial_sql`. When `grouped_sql.is_none()` but `group_by_columns` is non-empty (computed aggregate expressions that fell through the grouped partials planner), LIMIT is also NOT pushed — DataFusion needs all matching docs to produce correct GROUP BY results. The pipeline order for grouped partials is: merge → HAVING → top-K selection → LIMIT/OFFSET.
- **Top-K selection**: When ORDER BY + LIMIT are both present on grouped partials, uses `select_nth_unstable_by` (O(N) average) instead of full sort (O(N log N)). Only the top-K subset is fully sorted. `GroupedSqlPlan::uses_top_k()` detects eligibility. The EXPLAIN pipeline shows `"top_k_selection": true`.
- `QueryPlan` has `limit: Option<usize>`, `offset: Option<usize>`, `limit_pushed_down: bool`, and `sort_pushdown: Option<(String, bool)>` fields.
- `sort_pushdown` captures the single ORDER BY column name and direction (true = DESC) when eligible for fast-field sort pushdown.
- `to_search_request()` populates `SearchRequest.sort` from `sort_pushdown`, so the engine uses `order_by_fast_field()` in both `search_query()` and `sql_record_batch()`.
- Multi-column ORDER BY cannot be pushed — falls back to DataFusion sorting.
- `parse_limit_expr()` and `parse_offset_expr()` extract numeric values from `sqlparser::ast::LimitClause`.
- When limit is pushed down, the Tantivy search collects only `limit + offset` docs instead of the default 100K.
- The `limit_pushed_down` guard conditions are: `limit.is_some() && !has_residual_predicates && grouped_sql.is_none() && group_by_columns.is_empty() && (is_order_by_score_only || sort_pushdown.is_some())`.

## Distributed Fast-Field SQL
- The `tantivy_fast_fields` path now works across multi-node clusters.
- For buffered non-`SELECT *` queries, the coordinator can still scatter unary `SqlRecordBatch` gRPC RPCs to remote shards in parallel.
- For streamed explicit-column SQL (`POST /_sql/stream` or `POST /{index}/_sql/stream`), the coordinator opens `SqlRecordBatchStream` gRPC streams to remote shards and keeps those shard streams live.
- Local explicit-column fallback queries should also keep shard batches lazy: build a metadata-bearing batch handle up front, then drain it on the search pool as DataFusion pulls from the local `StreamingTable` partition instead of prebuilding a `Vec<RecordBatch>` first.
- The engine-level local streaming contract is `sql_streaming_batch_handle()`: it returns `total_hits`, `collected_rows`, and a lazy `next_batch()` closure. `sql_streaming_batches()` is now the eager compatibility wrapper that drains that handle for tests and older callers.
- `sql_streaming_batch_handle()` should batch doc IDs per emitted Arrow batch and read fast fields with `Column::first_vals()` / `StringFastFieldReader::first_ords_batch()` instead of per-doc `first()` loops. That batched path applies to numeric, date, `_id`, and keyword columns.
- Remote shards now serve `SqlRecordBatchStream` from the same lazy engine batch-handle contract used by the local coordinator path, and still fall back to `sql_record_batch()` when streaming is not eligible.
- The streamed SQL path registers `StreamingTable` partitions in DataFusion, so explicit-column remote shard batches flow through the coordinator without an extra coordinator-side `MemTable` staging step.
- The coordinator should read only the first remote shard response up front to capture `total_hits`, `collected_rows`, and actual `streaming_used`, then pass the remaining live shard stream directly into the `StreamingTable` partition instead of `try_collect()`-ing all remote batches before DataFusion starts.
- Local zero-hit handles should still emit one empty batch with the canonical schema so `StreamingTable` registration and downstream DataFusion planning stay stable.
- Large unbounded `SELECT *` queries are still a compatibility/export limitation today: wildcard projection uses `materialized_hits_fallback`, and export-style wildcard streaming remains the next major gap.
- Future work for that query class is a dedicated export-style path that avoids that coordinator-side pre-DataFusion buffering entirely, instead of full result materialization inside the coordinator.
- **Plan-driven canonical schema**: `execute_sql_batches()` first merges the base schema across ALL incoming batches via `merge_batch_schemas()`, then derives `final_schema` from that merged schema + the plan's SQL column ordering via `project_schema()`. This prevents a broken/drifted first batch from corrupting canonical types or dropping columns that appear later.
- **Streamed canonical schema**: `direct_sql_input_schema()` derives the coordinator input schema from planner dependencies + index mappings before execution starts, and each streamed batch is normalized through `normalize_sql_batch_for_schema()` to keep the registered `StreamingTable` schema stable.
- **Schema-recovery fallback**: If projected batches have inconsistent schemas (e.g., remote Arrow IPC schema drift), each batch is normalized to the plan-derived canonical schema via `normalize_batch_to_schema()`. Missing columns are filled with nulls. Type casts are strict: `cast_array_strict()` rejects any cast that would introduce additional nulls, so bad shard data is surfaced as an error instead of silently degrading into nulls.
- **Per-shard streaming fallback**: `execute_sql_stream_query()` tries `sql_streaming_batch_handle()` on local shards and `SqlRecordBatchStream` on remote shards for eligible explicit-column queries. If a shard returns `None` (score needed or SourceFallback column), that shard transparently falls back to `sql_record_batch()`. This is per-shard, not global — some shards can stream while others use the TopDocs path.
- Arrow IPC helpers: `record_batch_to_ipc()` and `record_batch_from_ipc()` in `arrow_bridge.rs`.
- `SELECT *` queries always use the materialized fallback path (`materialized_hits_fallback`) — the fast-field path requires explicit column projection.

## EXPLAIN ANALYZE
- `POST /{index}/_sql/explain` with `"analyze": true` executes the query and returns plan + per-stage timings + result rows.
- `SqlTimings` struct in `src/hybrid/mod.rs`: `planning_ms`, `search_ms`, `collect_ms`, `merge_ms`, `datafusion_ms`, `total_ms` (fractional milliseconds, `f64`).
- `execute_sql_query()` in `src/api/search.rs` is the shared internal function used by both `search_sql` and `explain_sql(analyze=true)`. Timing instrumentation is in this one place.
- The `search_sql` handler is a thin response formatter over `execute_sql_query()`.
- Without `"analyze": true`, `explain_sql` returns plan-only (no execution, no timings, no rows) — unchanged from the original behavior.

## DataFusion 53 LIMIT Workaround
- DataFusion 53.0.0 has a bug where `LIMIT` fetch-pushdown into the registered input table scan can be silently ignored when the SQL `SELECT` projects columns in a different order than the table schema.
- Example: schema `[base_passenger_fare, hvfhs_license_num]` + SQL `SELECT hvfhs_license_num, base_passenger_fare ... LIMIT 5` → returns all rows instead of 5.
- The same query with columns in schema order (`SELECT base_passenger_fare, hvfhs_license_num`) correctly returns 5 rows.
- **Workaround**: `project_batch_to_sql_columns()` in `datafusion_exec.rs` reorders each `RecordBatch` to match the SQL `SELECT` list order before registering the input table (buffered or streamed). This keeps DataFusion's projection identity-like, preventing the buggy optimizer path.
- The workaround uses `sqlparser` only for SELECT-list order. The set of required columns must come from the planner (`required_columns`, `needs_id`, `needs_score`), not from a second SQL AST walker, so CASE/HAVING/GROUP BY dependencies are preserved.
- DataFusion still handles LIMIT/OFFSET execution — we do NOT strip LIMIT from the SQL or apply it manually. The workaround only prevents the specific optimizer misfire.
- When DataFusion fixes this upstream, the workaround can be removed — the reordering is a no-op when projection already matches schema order.

## Planning Rules
- Split planning into two stages:
  1. search-aware planning in Tantivy
  2. residual SQL planning in DataFusion
- Push `text_match(...)`, exact filters (`=`), range filters (`>`, `>=`, `<`, `<=`), `BETWEEN`, `IN`, and `OR` disjunctions into Tantivy before Arrow/DataFusion execution.
- Multiple top-level `AND`ed `text_match(...)` predicates are supported. Keep them as explicit planner state and lower them into separate `BoolQuery.must` clauses instead of hiding extras as structured filters.
- `BETWEEN a AND b` is pushed as `Range { gte: a, lte: b }` — reuses existing Range query support.
- `IN (a, b, c)` is pushed as `Bool { should: [Term(a), Term(b), Term(c)] }` — reuses existing Bool+Term support.
- `OR` disjunctions (e.g., `author = 'pg' OR author = 'sama'`) are pushed as `Bool { should: [...] }` when ALL branches are individually pushable. Nested ORs are flattened. Parenthesized expressions are unwrapped.
- If any OR branch is not pushable (e.g., contains `text_match`), the planner returns an error. `text_match()` must appear as a top-level AND predicate, not inside OR or complex expressions.
- Roadmap for this limitation: support `text_match()` inside `OR` / more complex boolean trees only with branch-aware boolean lowering when the entire subtree is fully pushable into Tantivy. Never leave residual `text_match()` evaluation to DataFusion.
- The `expr_contains_text_match` guard walks all expression shapes that can survive as residual predicates: `BinaryOp`, `Nested`, `UnaryOp`, `IsNull`, `IsNotNull`, `Cast`, `Case` (with `CaseWhen` arms), `Between`, `InList`, and `Function` arguments (for `COALESCE(text_match(...), false)` etc.).
- `NOT IN` and `NOT BETWEEN` are NOT pushed — they remain as residual predicates for DataFusion.
- Same-index semijoin MVP rules:
  - only top-level `AND` predicates in `WHERE`
  - same index only
  - uncorrelated inner query only
  - exactly one projected inner key column
  - distinct inner keys are deduplicated, exactly `SQL_SEMIJOIN_MAX_KEYS = 50_000` keys are allowed, and the next distinct key must fail before lowering
  - `NULL` inner keys are ignored during lowering
  - lower supported semijoins into the existing concrete outer filter path; do not leave them as DataFusion residual subqueries
- `_score` and `_id` fields are never pushed (`_score` is computed by Tantivy, `_id` is a doc identifier).
- **GROUP BY expressions** (e.g., `GROUP BY LOWER(author)`, `GROUP BY year / 10`) are NOT eligible for the grouped partials path by default. Only plain column references in GROUP BY enable `tantivy_grouped_partials`, with one explicit exception: searched `CASE` bucket expressions over a single source field with literal range bounds and literal labels may be lowered into an internal derived group key and stay on `tantivy_grouped_partials`. General expression-based GROUP BY still falls through to `tantivy_fast_fields` + DataFusion. When the fallback query is `_score`-free and fully fast-field-backed, the runtime may use bitset streaming internally; responses expose this separately as `streaming_used: true`. Mixed GROUP BY (plain columns + unsupported expressions, e.g., `GROUP BY author, LOWER(category)`) still bails.
- The supported searched-`CASE` grouped-partials shape is intentionally narrow: same source field in every `WHEN`, range-style comparisons only, literal bucket labels, and only string-backed fast-field source columns (for example `keyword` / string-backed boolean mappings). If the `ELSE` branch is present it must also be a literal bucket label; non-literal `ELSE` branches and non-string-backed source fields must fall back instead of being approximated.
- **GROUP BY fallback scan limit**: When a GROUP BY query falls to `tantivy_fast_fields` (expression GROUP BY, unsupported aggregates like `STDDEV_POP`, residual predicates), the planner sets `has_group_by_fallback = true`. The API layer overrides `SearchRequest.size` from the default 100K to `sql_group_by_scan_limit` (default: 1M, configurable, 0 = unlimited). If any shard still returns fewer rows than it matched on the capped fallback paths, the query returns a `group_by_scan_limit_exceeded` error instead of silently wrong aggregates. The default 100K `SQL_MATCH_LIMIT` remains for flat (non-GROUP BY) queries where truncation only affects completeness, not correctness.
- `sql_approximate_top_k` currently defaults to `true`. Eligible grouped-partials queries with bare-metric `ORDER BY ... LIMIT` attach `ShardTopK` unless the user disables it. SQL responses and EXPLAIN output must surface this with a top-level `approximate_top_k` boolean so the approximation is never hidden.
- **Ungrouped aggregate fallback**: When a SELECT contains aggregate functions but `grouped_sql` is `None` and there is no GROUP BY, `has_ungrouped_aggregate_fallback = true` applies the same scan limit override and truncation error as GROUP BY fallback. This catches expressions the residual expression extractor cannot handle.
- `collect_expr_columns` handles `Cast`, `Case` (with `CaseWhen` arms), and `Like`/`ILike` expressions to ensure all referenced columns are read from fast fields.
- `GroupedMetricAgg.field_expr` is recursive. Planner-side dependency extraction and Tantivy grouped collectors must walk the full expression tree, batch-read every numeric leaf column, and evaluate the arithmetic tree per doc without dropping to the generic fast-fields/DataFusion fallback just because the aggregate argument is nested.
- **SELECT aliases are never pushed down.** If a WHERE predicate references a SELECT alias (e.g., `WHERE posts > 10` when `posts` is `count(*) AS posts`), it stays as a residual predicate for DataFusion. This prevents pushing filters on nonexistent Tantivy fields, which silently fall back to the `body` catch-all field and match nearly everything.
- **Alias shadowing and required columns**: `collect_required_columns()` must use alias-aware walkers for WHERE/HAVING/ORDER BY so bare alias references do not leak into `required_columns`, while real source fields still survive through wrapped aggregate expressions like `COALESCE(AVG(price), 0)` or `CAST(AVG(price) AS FLOAT)` when a SELECT alias shadows that field name.
- Prefer shard-local partial execution before coordinator-side row materialization.
- Treat `materialized_hits_fallback` as a compatibility path, not the target architecture.

## HAVING Support
- `HAVING` clauses are supported in the `tantivy_grouped_partials` path as post-merge filters.
- Simple comparisons (`>`, `>=`, `<`, `<=`, `=`) on output columns (group columns or metric aliases) are converted to `HavingFilter` structs.
- **Both alias-based and aggregate-expression HAVING are supported**: `HAVING posts > 10` (alias) and `HAVING COUNT(*) > 10` (aggregate expression) both route correctly to `tantivy_grouped_partials`.
- **ORDER BY can also resolve supported aggregate expressions directly**: `ORDER BY posts DESC` and `ORDER BY SUM(upvotes) DESC` should both stay on `tantivy_grouped_partials` when the aggregate itself is eligible. Do not require a SELECT alias just to keep grouped-partials planning.
- Mixed HAVING conditions (e.g., `HAVING posts > 10 AND AVG(upvotes) > 5`) are supported — each condition can independently use an alias or an aggregate expression.
- Flipped comparisons (e.g., `HAVING 20 < COUNT(*)`) are also supported with correct operator inversion.
- `resolve_having_name()` resolves HAVING operands: tries identifier lookup first (alias path), then matches aggregate functions against the parsed metrics list (aggregate expression path).
- HAVING filters are applied after shard-partial merge but before sorting and LIMIT/OFFSET.
- Multiple HAVING conditions (ANDed) are supported.
- Complex HAVING expressions (OR, subqueries, expressions not matching a known metric) cause fallback to the DataFusion path.
- The pipeline order in grouped partials is: merge → HAVING → top-K selection → LIMIT/OFFSET.
- `EXPLAIN ANALYZE` / SQL timings for `tantivy_grouped_partials` should expose the nested coordinator breakdown under `timings.grouped_merge`: `partial_merge_ms`, `having_ms`, `top_k_ms`, `row_build_ms`, plus bucket counts before HAVING and after LIMIT/OFFSET. Keep the top-level `merge_ms` as the overall grouped merge wall time.
- Grouped-partials may register hidden support metrics for aggregate expressions referenced only from HAVING / ORDER BY. These metrics must be collected and merged, but they must not appear in the final SQL columns or rows.
- Ungrouped grouped-partials queries with zero matches must still synthesize the empty global bucket so `count(*)` returns `0` and other aggregates return `NULL`, rather than producing an empty result set.

## GROUP BY Field Type Validation
- `GROUP BY` on `Text` fields returns a `group_by_text_field_exception` error (HTTP 400).
- Text fields are tokenized (split into words) and don't have fast-field columnar storage — grouping by individual tokens is not meaningful.
- Only `Keyword`, `Integer`, `Float`, `Boolean` fields support GROUP BY.
- Validation runs in `execute_sql_query()` by checking field mappings from cluster state before executing the grouped partials path.

## Anti-Pattern To Avoid
- Do not advertise or implement the feature as "SQL over hits".
- Do not normalize coordinator-side row materialization as the primary path.
- Do not move search-native logic into DataFusion just because SQL is involved.

## Grouped Analytics Direction
- For grouped analytics over matched docs, prefer shard-local partial aggregation from fast fields.
- Ship compact partial states to the coordinator and merge there.
- Grouped-partial wire formats must preserve numeric group-key type fidelity across shards. Do not coerce all numeric keys to `f64`, or coordinator merge can split logically identical integer buckets like `0` and `0.0` between local and remote shards.
- Only fall back to row materialization when a query requires unsupported expressions, wildcard projection, or unavailable columnar data.

## Critical Invariants
- `doc_id` must remain the row index for any columnar representation built from matched docs.
- `_score` must be surfaced as a normal Arrow/DataFusion column.
- Avoid extra maps or indirection unless there is a proven need.

## Execution Priorities
1. Tantivy search returns matched docs or shard-local partials.
2. Fast fields provide structured columns directly.
3. Arrow batches represent the matched tabular view.
4. DataFusion executes only the residual SQL semantics that still need a relational engine.

## Longer-Term Planner Direction
- The current planner still relies on raw `sqlparser` expression walking for parts of dependency extraction. The long-term target is a clause-aware semantic binder that resolves each identifier as a base field, SELECT alias, aggregate output, or synthetic field like `_id` / `_score` before capability analysis runs.
- `required_columns`, `needs_id`, and `needs_score` should ultimately be derived from that bound query IR rather than from raw AST walkers plus alias filtering. This is the clean way to eliminate recurring alias-shadowing edge cases in WHERE/HAVING/ORDER BY.
- Keep the separation of responsibilities: semantic binding first, then search-aware capability analysis, then execution planning. Do not fold this into ad-hoc pushdown walkers or a second SQL dependency pass.
- Until the binder carries enough source-identifier semantics through execution, the API SQL execution layer canonicalizes unquoted source field names against index mappings case-insensitively before building Tantivy queries or Arrow schemas. For residual DataFusion execution, rewrite the SQL AST to quoted canonical source identifiers instead of lowercasing Arrow schemas, so mixed-case mapped fields work unquoted without breaking quoted exact identifiers. This is a bridge, not the long-term planner architecture.
- `project_batch_to_sql_columns` and `project_schema` in `datafusion_exec.rs` use case-insensitive matching (`eq_ignore_ascii_case`) when aligning `extract_select_column_order` names from `rewritten_sql` against batch schema fields, because `rewritten_sql` preserves the user's original casing while batch schemas use canonicalized names.

## Small Bound-Column IR Plan
- Near-term step before the full semantic binder: introduce a small clause-aware bound-column layer, not a full bound-query IR.
- Goal: resolve every identifier the planner inspects into a semantic kind so dependency extraction and pushdown rules stop depending on raw string checks plus alias stripping.
- The current `SyntheticColumn` enum is stage 0 of this plan. Keep it and compose it into the next layer instead of reintroducing raw `_id` / `_score` comparisons elsewhere.
- Recommended minimal shape:
  - `BoundColumn::Source(String)` — physical mapped field / fast-field candidate
  - `BoundColumn::Synthetic(SyntheticColumn)` — `_id` / `_score`
  - `BoundColumn::Output(String)` — SELECT alias, group output, or metric output visible in HAVING / ORDER BY output space
  - optional `BoundColumn::Unknown(String)` only at binder boundaries; capability analysis should not silently treat unknown names as source fields
- Keep the first version small:
  - Do NOT build a full bound expression tree yet
  - Reuse `sqlparser` AST for expression structure
  - Bind only the identifier-bearing expression paths the planner already inspects: projection, WHERE, GROUP BY, HAVING, ORDER BY, and aggregate arguments
- Clause rules for the small binder:
  - WHERE: bare identifiers bind only to source fields or synthetic columns; SELECT aliases stay residual and must not leak into pushdown or required-column extraction
  - Projection and aggregate arguments: identifiers inside expressions bind to source fields or synthetic columns, never to SELECT aliases from the same projection list
  - GROUP BY eligibility: only `BoundColumn::Source` counts as a plain grouped-partials key; `Synthetic` or expression-derived outputs must bail to residual SQL execution
  - HAVING and ORDER BY on grouped queries: resolve output-space names first (`BoundColumn::Output`), then aggregate-expression matching, then source fields only when semantically valid
  - `_id` / `_score` must always bind through `SyntheticColumn`, never through ad-hoc string checks
## Semantic Binder (Implemented)
- `bind_select()` resolves SELECT projection + GROUP BY items into a `BindContext` **before** pushdown extraction and capability analysis.
- `BoundSelectItem` enum classifies each SELECT item as `Column(BoundIdentifier)`, `Aggregate`, `Wildcard`, or `Unresolved` (escape hatch for expressions the binder can't fully resolve — passed through to DataFusion).
- `BoundIdentifier` distinguishes `Source(field)` from `Synthetic(_id/_score)`.
- `BoundGroupByItem` classifies GROUP BY items as `Source(column)` or `Expression` (synthetic, alias, or complex expression).
- `BindContext::derive_columns()` replaces the old `collect_required_columns()` — walks bound items + WHERE/HAVING/ORDER BY with alias awareness, produces `(required_columns, needs_id, needs_score)`.
- `BindContext::derive_group_by()` replaces the old `collect_group_by_columns()` — derives `(group_by_columns, has_expression_group_by)` from bound items.
- `collect_expr_columns_alias_aware_into()` is the unified column walker that handles alias filtering + synthetic detection in a single pass (replaces the old separate `collect_expr_columns_alias_aware` + post-hoc `_id`/`_score` string matching).
- `collect_expr_source_columns_with_flags()` walks any expression tree and extracts source columns + synthetic flags (used for `BoundSelectItem::Unresolved` source column extraction).
- Identity aliases such as `SELECT author AS author` are treated as the same underlying source column for WHERE/GROUP BY/required-column extraction; non-identity aliases still block pushdown and grouped-partials eligibility.
- The binder's `alias_set` directly feeds `extract_pushdowns()` — no separate `collect_select_aliases()` call needed.
- The old functions (`projection_has_wildcard`, `collect_required_columns`, `collect_expr_columns_alias_aware`, `collect_group_by_columns`, `collect_group_by_exprs`, `collect_select_item_columns`, `collect_select_aliases`, `collect_expr_columns`) have been removed.
- WHERE clause binding is intentionally deferred — pushdown extraction still uses raw AST walkers. The binder covers SELECT, GROUP BY, ORDER BY, and HAVING.
- The binder does NOT replace capability analysis (pushdown eligibility, grouped partials eligibility). It provides resolved identifiers that the analysis consumes.

## Query Shape Validation (Implemented)
- `validate_query_shape()` runs early in `plan_sql()` before any planning work.
- Rejects unsupported shapes with clear, actionable error messages instead of letting them leak to DataFusion as opaque table-resolution failures.
- Validated shapes: CTEs (`WITH`), UNION/EXCEPT/INTERSECT, subqueries in SELECT/WHERE/HAVING/ORDER BY, subqueries in FROM (derived tables), subqueries as function arguments, JOINs, multi-table FROM.
- `validate_where_clause()` recursively walks the WHERE expression tree to detect subquery nodes at any depth.
- `expr_contains_subquery()` must recurse through nested CASE, LIKE/ILIKE, BETWEEN, IN-list, unary/binary, and function-argument expression trees so SELECT/HAVING/ORDER BY do not leak unsupported subqueries downstream.

## Preferred Language In Reviews And Docs
- Say: "search-aware planning", "residual SQL execution", and "grouped analytics over matched docs".
- Avoid: "SQL over hits" unless you are explicitly describing the compatibility fallback path.

## Arrow Bridge Type Hints
- `build_record_batch_with_hints(column_store, type_hints)` accepts a `HashMap<String, ColumnKind>` to override data-driven type inference.
- When building Arrow batches from the fast-field path (`sql_record_batch`), callers MUST populate `type_hints` from `SqlFieldReader` variants or the Tantivy schema so that zero-result queries still produce correctly-typed columns (e.g. Float64 for price, Timestamp(Millisecond, UTC) for mapped Date fields) instead of defaulting to Utf8.
- Timestamp-backed Date columns must serialize back to UTC ISO 8601 strings in JSON result rows; do not leak raw epoch millis from the Arrow/DataFusion bridge.
- `infer_column_kind()` (data-driven inference) is only the fallback for columns without a type hint — it defaults to Utf8 when all values are null or the column is empty.
- The plain `build_record_batch()` (no hints) is used only by the `materialized_hits_fallback` path where schema info is unavailable.

## Testing Expectations
- Add unit tests for pushdown extraction, quoted index handling, grouped analytics planning, LIMIT pushdown, and residual predicate detection.
- Add execution tests for both:
  - `tantivy_grouped_partials`
  - `tantivy_fast_fields`
  - `materialized_hits_fallback`
- Add a regression where grouped SQL uses an unaliased aggregate expression in `ORDER BY` (for example `ORDER BY SUM(price) DESC`) and assert it still stays on `tantivy_grouped_partials`.
- Add assertions for the separate `streaming_used` runtime flag so streaming and non-streaming fallback queries stay distinguishable without overloading `execution_mode`.
- Add a distributed grouped-partials regression test that round-trips partials through `encode_partial_aggs()` / `decode_partial_aggs()` and verifies integer group keys still merge with local shard keys instead of splitting `0` and `0.0` into separate buckets.
- Add an end-to-end multi-node query test for grouped-partials wire-format changes. A local unit test is not enough when the bug only appears after remote shards serialize partials over gRPC.
- Add regression tests for zero-result or all-null columns: verify that schema-derived `type_hints` override `infer_column_kind` to produce correct Arrow DataTypes (e.g. Float64, not Utf8 for numeric columns).
- Add tests verifying that the fast-field path reads `_id` from the fast-field column (not from stored docs) when all columns are fast-field-backed.
- Add tests verifying that mixed fast+fallback column queries still return correct `_id` values.
- Add semijoin regressions for supported top-level same-index shapes, rejected correlated / OR-wrapped / multi-index / multi-semijoin shapes, zero-key aggregate semantics, exact key-cap boundaries, inner grouped `ORDER BY` aggregate shapes, and EXPLAIN ANALYZE semijoin pipeline visibility.
- Live tests should inspect both returned rows and planner metadata.