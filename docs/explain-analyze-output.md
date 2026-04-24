# Richer Explain And Analyze Output

## Goal

Make `/_sql/explain` and `EXPLAIN ANALYZE` better debugging tools for FerrisSearch's hybrid planner and distributed execution without changing the endpoint's basic contract.

The first change set should explain three things more clearly than it does today:

1. why a particular execution mode was chosen
2. what the planner considered but rejected
3. what actually happened at runtime when `analyze=true`

This is a visibility change, not a planner rewrite.

## Current State

FerrisSearch already has a useful base:

- `QueryPlan::to_explain_json()` in `src/hybrid/planner.rs` emits:
  - `execution_strategy`
  - `strategy_reason`
  - `pushdown_summary`
  - `columns`
  - `rewritten_sql`
  - `pipeline`
  - optional `semijoin`
- `POST /{index}/_sql/explain` in `src/api/search/mod.rs` supports:
  - `analyze=false`: plan-only explain
  - `analyze=true`: execute query, then add:
    - `timings`
    - `execution_mode`
    - `approximate_top_k`
    - `streaming_used`
    - `matched_hits`
    - `row_count`
    - `truncated`
    - `_shards`
    - `columns`
    - `rows`

The CLI renderer in `src/cli.rs` already consumes several of those fields.

## Problems

The current output still leaves important gaps:

### Planner Gaps

- it reports the chosen strategy, but not the rejected strategies and their reasons
- it does not clearly separate static plan facts from runtime facts
- it does not explain scan-limit or fallback eligibility in a structured way
- remote_store-specific execution facts are absent

### Runtime Gaps

- `matched_hits` is present, but `collected_hits` is not
- `_shards` exists, but local-vs-remote and stream-vs-buffer decisions are opaque
- grouped merge timings exist only as nested timing data, not as a first-class analysis block
- remote_store manifest generation, candidate split counts, and pruning effects are not surfaced

### UX Gap

The JSON is already useful for humans and the CLI, so the right move is not replacement. The right move is additive structure that keeps current fields intact while making the output easier to reason about mechanically and visually.

## Decision

Keep the current top-level fields for backward compatibility and add two new additive blocks:

- `mode_candidates`: planner-time eligibility and rejection reasons
- `analysis`: runtime facts, only when `analyze=true`

For `remote_store`, add an optional `remote_store` block in both plan-only and analyze output whenever the selected engine is `remote_store`.

## Response Shape

### Plan-Only Output

Plan-only explain should keep the current top-level fields and add:

```json
{
  "engine": "local_shards",
  "execution_strategy": "tantivy_fast_fields",
  "strategy_reason": "All projected columns can be read from Tantivy fast fields",
  "mode_candidates": {
    "tantivy_grouped_partials": {
      "eligible": false,
      "reason": "group_by_absent"
    },
    "tantivy_fast_fields": {
      "eligible": true,
      "reason": "projected_columns_fast_field_backed"
    },
    "materialized_hits_fallback": {
      "eligible": true,
      "reason": "always_available_compatibility_path"
    }
  }
}
```

### Analyze Output

`analyze=true` should keep the current top-level execution fields and add:

```json
{
  "analysis": {
    "collection": {
      "matched_hits": 12473,
      "collected_hits": 12473,
      "row_count": 10,
      "streaming_used": false,
      "truncated": false
    },
    "shards": {
      "total": 3,
      "successful": 3,
      "failed": 0,
      "local": 1,
      "remote": 2
    },
    "timings": {
      "planning_ms": 0.8,
      "search_ms": 32.1,
      "datafusion_ms": 0.0,
      "total_ms": 47.5,
      "grouped_merge": {
        "partial_merge_ms": 4.1,
        "having_ms": 0.0,
        "top_k_ms": 1.2,
        "row_build_ms": 0.4,
        "merged_buckets": 4715,
        "post_having_buckets": 4715,
        "output_buckets": 10
      }
    }
  }
}
```

### Remote Store Output

When the selected engine is `remote_store`, emit an additional block:

```json
{
  "remote_store": {
    "manifest_generation": 42,
    "published_splits": 24,
    "candidate_splits": 5,
    "pruned_splits": 19,
    "assigned_leaves": 2,
    "warm_reader_splits": 3,
    "warm_artifact_splits": 1,
    "cold_splits": 1
  }
}
```

That block should be present in plan-only mode when known at planning time, and enriched in analyze mode with runtime counters.

## Mode Candidate Matrix

FerrisSearch should explicitly report why each main execution mode is eligible or not.

For the first PR, keep the reason set small and stable.

### `tantivy_grouped_partials`

Examples:

- `eligible: true, reason: "simple_grouped_metrics_query"`
- `eligible: false, reason: "group_by_absent"`
- `eligible: false, reason: "expression_group_by_present"`
- `eligible: false, reason: "unsupported_aggregate_shape"`
- `eligible: false, reason: "having_requires_fallback"`

### `tantivy_fast_fields`

Examples:

- `eligible: true, reason: "projected_columns_fast_field_backed"`
- `eligible: false, reason: "select_star_requires_source_materialization"`
- `eligible: false, reason: "stored_field_required"`
- `eligible: false, reason: "score_or_expression_forces_fallback"`

### `materialized_hits_fallback`

Examples:

- `eligible: true, reason: "always_available_compatibility_path"`
- `eligible: true, reason: "selected_due_to_select_star"`
- `eligible: true, reason: "selected_due_to_non_columnar_expression"`

### Why This Matters

The current `strategy_reason` is a single sentence. That is good for humans, but it is weak as a debugging contract. `mode_candidates` makes planner behavior testable and lets the CLI show both the chosen path and the rejected fast path.

## Analysis Block Details

### Collection Counters

Add `collected_hits` alongside the existing `matched_hits`.

- `matched_hits`: total hits before truncation or SQL row shaping
- `collected_hits`: how many hits or rows were actually gathered into the next stage

This makes scan-limit failures and buffered fallback behavior much clearer than the current `truncated` boolean alone.

### Shard Counters

Keep `_shards` for compatibility and add `analysis.shards` with richer facts.

For the first PR:

- `total`
- `successful`
- `failed`
- `local`
- `remote`

Later we can add `streamed` and `buffered` shard counts without changing the shape.

### Timings

Keep `timings` exactly as it exists today and mirror it under `analysis.timings`.

That gives us a migration path:

- legacy clients keep reading top-level `timings`
- new tooling reads `analysis.timings`

Do not remove the existing top-level field in this change set.

## Remote Store Visibility

Once remote_store split pruning lands, explain output should show the root-side split decisions. The initial block should answer:

- which manifest generation was planned
- how many published splits existed
- how many candidate splits survived pruning
- how many were pruned
- how much of the candidate set was already warm

This is intentionally aligned with [docs/remote-store-split-pruning.md](docs/remote-store-split-pruning.md).

## Backward Compatibility

This change should be additive.

- keep `execution_strategy`, `strategy_reason`, `pipeline`, `timings`, `execution_mode`, `_shards`, and `rows`
- add `engine`, `mode_candidates`, `analysis`, and optional `remote_store`
- keep the CLI working against the legacy fields first
- make any CLI rendering of the new fields opportunistic

The endpoint should not switch to a text explain format in this PR. JSON remains the source of truth.

## Code Seams

- `src/hybrid/planner.rs`
  Extend `QueryPlan::to_explain_json()` to emit `engine` and `mode_candidates`.
- `src/api/search/mod.rs`
  Extend `explain_sql()` to build the new `analysis` block in analyze mode.
- `src/api/search/mod.rs`
  Keep `sql_response_body()` and streaming SQL metadata compact; richer blocks stay explain-only.
- `src/engine/remote_store.rs`
  Feed remote_store candidate/pruned/warm split counts into explain/analyze once available.
- `src/cli.rs`
  Optionally render `mode_candidates`, `analysis.collection`, and `remote_store` when present.

## Test Plan

1. Plan-only explain regression asserting `mode_candidates` for:
   - grouped partials
   - fast-field path
   - `SELECT *` fallback
2. Analyze regression asserting `analysis.collection` and `analysis.shards` are present.
3. Grouped query regression asserting `analysis.timings.grouped_merge` stays populated.
4. CLI rendering test ensuring legacy fields still render if the new nested blocks are absent.
5. Remote_store explain regression asserting `remote_store` appears once split-count bookkeeping exists.

## Rollout

Implement in this order:

1. Add planner-time `engine` and `mode_candidates`.
2. Add `analysis.collection` and `analysis.shards` in analyze mode.
3. Mirror timings under `analysis.timings` while keeping top-level `timings`.
4. Add optional `remote_store` details once the root execution path exposes them.

That keeps the change small, preserves current clients, and makes the output strictly more informative at each step.