# Adaptive Shard Top-K Auto Mode

## Goal

Reduce the latency and coordinator memory cost of high-cardinality grouped SQL queries of the form:

```sql
SELECT group_key, agg(...)
FROM idx
GROUP BY group_key
ORDER BY metric DESC
LIMIT N
```

without forcing every grouped query onto approximate shard pruning.

The current implementation is a single node-level boolean:

```yaml
sql_approximate_top_k: true
```

Today, that boolean defaults to enabled and every eligible grouped-partials query uses shard-level top-K pruning with a fixed oversampling rule. Responses should surface that with `"approximate_top_k": true` whenever a query actually used shard-level pruning. This is still too coarse. We want a mode that enables pruning only when the query shape is safe and the runtime bucket count is high enough that pruning is actually useful.

## Proposed Config Shape

Replace the boolean with an explicit mode and tuning knobs:

```yaml
sql_top_k_mode: off           # off | auto | force
sql_top_k_max_requested_rows: 1000
sql_top_k_oversample_factor: 3
sql_top_k_oversample_base: 10
sql_top_k_auto_min_buckets: 4096
sql_top_k_auto_min_bucket_multiplier: 8
```

### Semantics

- `sql_top_k_mode: off`
  Disables shard-level pruning entirely.
- `sql_top_k_mode: auto`
  Enables pruning only for safe query shapes and only when the local shard bucket count is large enough to justify approximation.
- `sql_top_k_mode: force`
  Enables pruning for every safe query shape regardless of local bucket count. This is the benchmarking and power-user mode.

### Defaults

For the first release of this feature, default to:

```yaml
sql_top_k_mode: off
```

That preserves current semantics for existing deployments while making the rollout path explicit. After validation on the NYC taxi and Hacker News workloads, we can consider flipping the default to `auto`.

## Eligibility Rules

`auto` and `force` both start from the same planner-side safety checks. If any of these checks fail, pruning is disabled.

### Planner-Side Safety Checks

Shard top-K is eligible only when all of the following are true:

1. The query is planned on `tantivy_grouped_partials`.
2. The query has a `GROUP BY`.
3. The query has a `LIMIT`.
4. The query has exactly one `ORDER BY` item.
5. The `ORDER BY` item resolves to a real grouped metric.
   This means it must match a `GroupedMetricAgg.output_name` whose `residual_expr` is `None`.
6. The `ORDER BY` item is not a group column.
7. The query has no `HAVING` clause.
8. `offset + limit` is greater than zero.
9. `offset + limit <= sql_top_k_max_requested_rows`.

These rules preserve the current correctness guardrails and make them explicit.

### Shapes That Must Stay Ineligible

The following query shapes must never use shard top-K pruning:

- Queries with `HAVING`.
- Queries with multiple `ORDER BY` items.
- Queries ordered by a group column.
- Queries ordered by a residual expression such as `ROUND(AVG(x), 2)` or `AVG(x) + AVG(y)`.
- Queries without `LIMIT`.
- Queries that already fall to `tantivy_fast_fields` or `materialized_hits_fallback`.
- Queries where `offset + limit` exceeds `sql_top_k_max_requested_rows`.

## Runtime Activation Rules

The planner-side rules above define whether pruning is allowed at all. In `auto` mode, that is still not sufficient to actually prune.

After a shard finishes its grouped-partials accumulation and knows its local bucket count, activate pruning only when:

```text
local_bucket_count > max(
    sql_top_k_auto_min_buckets,
    shard_limit * sql_top_k_auto_min_bucket_multiplier
)
```

Where:

```text
needed_rows = offset + limit
shard_limit = needed_rows * sql_top_k_oversample_factor + sql_top_k_oversample_base
```

### Example

For:

- `LIMIT 10`
- `OFFSET 0`
- `oversample_factor = 3`
- `oversample_base = 10`

We get:

```text
needed_rows = 10
shard_limit = 10 * 3 + 10 = 40
activation_threshold = max(4096, 40 * 8) = 4096
```

So a shard with 265 local groups keeps all 265 buckets and ships exact results. A shard with 250,000 route buckets prunes to 40 before serialization and merge.

This is the core behavior we want from `auto`: exact on small grouped workloads, approximate only when the bucket set is large enough that the approximation is materially beneficial.

## Why This Split Matters

There are two different questions:

1. Is the query shape safe enough that pruning would be semantically understandable?
2. Is the local bucket cardinality large enough that pruning is worth doing?

The current boolean only answers the first question. `auto` should answer both.

That avoids approximate pruning on small grouped queries such as pickup zones, where the exact result set is already small and the optimization buys little.

## Explain / Response Visibility

Approximation must never be hidden. Extend `EXPLAIN` / `EXPLAIN ANALYZE` metadata with:

```json
{
  "top_k_mode": "auto",
  "top_k_eligible": true,
  "top_k_requested": true,
  "top_k_applied": true,
  "top_k_reason": "local_bucket_count_exceeded_threshold",
  "top_k_needed_rows": 10,
  "top_k_shard_limit": 40,
  "top_k_local_bucket_threshold": 4096
}
```

When the query is safe but auto does not activate, return:

```json
{
  "top_k_mode": "auto",
  "top_k_eligible": true,
  "top_k_requested": true,
  "top_k_applied": false,
  "top_k_reason": "local_bucket_count_below_threshold"
}
```

When the query is ineligible, return the specific reason, for example:

- `having_present`
- `multi_column_order_by`
- `order_by_group_column`
- `order_by_residual_metric`
- `limit_missing`
- `requested_rows_exceeds_threshold`
- `not_grouped_partials`

## Backward Compatibility

For one release, accept the old boolean as a deprecated alias:

- `sql_approximate_top_k: false` maps to `sql_top_k_mode: off`
- `sql_approximate_top_k: true` maps to `sql_top_k_mode: force`

If both are set, `sql_top_k_mode` wins.

This keeps existing configs working while making the new semantics explicit.

## Implementation Notes

### Config Layer

- Add `SqlTopKMode` enum to `AppConfig`.
- Keep the oversampling knobs in config rather than hardcoding `3x + 10`.
- Thread the full top-K settings through `AppState` instead of a boolean.

### Planner Layer

- Replace `to_search_request(approximate_top_k: bool)` with a richer settings object.
- Keep planner-side eligibility pure and deterministic.
- Emit `ShardTopK` only for safe query shapes.
- Extend planner metadata with the eligibility reason when pruning is disabled.

### Engine Layer

- In `force` mode, prune every eligible shard result.
- In `auto` mode, only prune when the runtime bucket count exceeds the threshold.
- Keep the current pruning point before expensive ordinal-to-string resolution.

## Test Plan

Add coverage for:

1. `off` mode never emitting `ShardTopK`.
2. `force` mode emitting `ShardTopK` for every safe shape.
3. `auto` mode emitting `ShardTopK` metadata for safe shapes.
4. `auto` mode not applying pruning when `local_bucket_count` is below threshold.
5. `auto` mode applying pruning when `local_bucket_count` is above threshold.
6. Ineligible shapes: `HAVING`, multi-column `ORDER BY`, group-column ordering, residual-expression ordering, missing `LIMIT`.
7. Backward-compatibility mapping from `sql_approximate_top_k` to the new enum.
8. `EXPLAIN` / `EXPLAIN ANALYZE` exposing the correct top-K reason and applied/not-applied state.

## Recommendation

Implement `off | auto | force` in one change set, but keep the default at `off` initially.

That gives FerrisSearch the same class of usability that engines like Druid and OpenSearch expose:

- explicit approximation mode
- automatic activation for the right shapes
- visible tradeoffs in explain output

without silently changing exactness for existing users.