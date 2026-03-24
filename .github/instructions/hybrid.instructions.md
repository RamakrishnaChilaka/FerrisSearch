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
- Prefer fast fields or stored fields over `_source` materialization whenever the needed SQL columns are available columnarly.

## Planning Rules
- Split planning into two stages:
  1. search-aware planning in Tantivy
  2. residual SQL planning in DataFusion
- Push `text_match(...)`, exact filters, and range filters into Tantivy before Arrow/DataFusion execution.
- Prefer shard-local partial execution before coordinator-side row materialization.
- Treat `materialized_hits_fallback` as a compatibility path, not the target architecture.

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

## Testing Expectations
- Add unit tests for pushdown extraction, quoted index handling, grouped analytics planning, and residual predicate detection.
- Add execution tests for both:
  - `tantivy_fast_fields`
  - `materialized_hits_fallback`
- Live tests should inspect both returned rows and planner metadata.