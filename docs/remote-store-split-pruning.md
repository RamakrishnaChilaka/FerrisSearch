# Remote Store Split Pruning

## Status

The first implementation slice is now in place:

- publish writes exact split summaries into `RemoteSplitManifest.field_ranges` for mapped `integer`, `float`, and `date` fields
- publish writes exact small distinct sets into `RemoteSplitManifest.field_terms` for mapped `keyword` and `boolean` fields
- root-side `remote_store` search prunes published splits against supported `term` and `range` filters before rendezvous assignment
- search and SQL EXPLAIN ANALYZE responses expose `remote_store.pruning` counters for published, candidate, pruned, and assigned split counts
- missing or unsupported metadata still keeps the split, preserving correctness

The remaining work in this document is still relevant as follow-up scope: broader predicate coverage, richer EXPLAIN ANALYZE analysis blocks, and deeper scheduling/reporting hooks.

## Goal

Reduce `remote_store` fan-out by pruning published splits at the root before rendezvous assignment and leaf RPC dispatch.

The first change set should be small, conservative, and directly useful:

- prune only on structured predicates we already understand well
- reuse the existing manifest and root/leaf execution path
- preserve correctness by treating missing metadata as "keep the split"
- avoid changing the client-facing coordinator pattern

This is not a design for full-text split skipping, vector pruning, or a new storage format. It is a targeted reduction in unnecessary split reads for `remote_store` indices.

## Current State

Today the `remote_store` read path does the following:

1. Load `manifest.current.json` and the immutable generation manifest from `src/storage/mod.rs`.
2. Validate `schema_hash` against the live index mappings.
3. Ask leaves for cache and load snapshots.
4. Schedule every published split through rendezvous ranking and cache-aware assignment.
5. Execute the search on all assigned splits and merge the results.

The current manifest already carries some pruning-adjacent metadata on each `RemoteSplitManifest`:

```rust
pub struct RemoteSplitManifest {
    pub split_id: String,
    pub state: RemoteSplitState,
    pub bundle_path: String,
    pub bundle_etag: Option<String>,
    pub hotcache_path: Option<String>,
    pub checksum: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
    pub hotcache_bytes: Option<u64>,
    pub time_range: Option<SplitTimeRange>,
    pub tags: BTreeMap<String, String>,
}
```

That is enough for coarse time/tag pruning if the publisher explicitly supplies those fields, but it is not enough for general query-driven pruning over mapped keyword, boolean, integer, float, and date filters.

The practical effect is that a query such as:

```sql
SELECT count(*)
FROM events
WHERE status = 'error' AND ts >= '2026-04-01T00:00:00Z'
```

still fans out to every published split even when most splits cannot possibly match.

## Scope

This PR-sized slice should support root-side pruning for:

- DSL `term` filters on mapped `keyword` and `boolean` fields
- DSL `range` filters on mapped `integer`, `float`, and `date` fields
- SQL queries whose planner output lowers into those same pushed-down filters
- conjunctions of supported predicates (`bool.filter` and `bool.must` only)

This slice explicitly does not support:

- full-text split pruning for `match`, query-string `q=`, `wildcard`, `prefix`, or `fuzzy`
- `must_not` or `should` pruning
- k-NN pruning
- reordering result correctness around score-based top docs
- new leaf cache types or a new split bundle format

## Decision

Add small, additive split summaries to `RemoteSplitManifest` and perform pruning at the root before leaf assignment.

The root should:

1. Build a conservative `SplitPruningPredicate` from the existing `SearchRequest`.
2. Evaluate that predicate against each published split's manifest summaries.
3. Keep splits whose metadata proves they may match.
4. Fall back to keeping the split whenever metadata is missing, unsupported, or ambiguous.
5. Continue with the existing rendezvous + cache-aware scheduling over only the surviving split set.

This keeps the optimization entirely inside the current root execution role. No new coordinator RPC is needed for the first slice.

## Manifest Fields

### Additive Manifest Extension

Extend `RemoteSplitManifest` with two new optional summaries:

```rust
pub struct RemoteSplitManifest {
    // existing fields
    pub split_id: String,
    pub state: RemoteSplitState,
    pub bundle_path: String,
    pub bundle_etag: Option<String>,
    pub hotcache_path: Option<String>,
    pub checksum: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
    pub hotcache_bytes: Option<u64>,
    pub time_range: Option<SplitTimeRange>,
    pub tags: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_ranges: BTreeMap<String, SplitFieldRange>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_terms: BTreeMap<String, SplitFieldTerms>,
}

pub struct SplitFieldRange {
    pub min: String,
    pub max: String,
}

pub struct SplitFieldTerms {
    pub values: Vec<String>,
}
```

### Encoding Rules

- `field_ranges` is for mapped `integer`, `float`, and `date` fields.
- `field_terms` is for mapped `keyword` and `boolean` fields.
- `field_ranges` values are stored as canonical strings:
  - integer: decimal string
  - float: normalized finite decimal string
  - date: epoch-millis string
- `field_terms` is emitted only when the field's exact distinct set is small enough to keep in the manifest. If the field is too high-cardinality, omit it entirely.

The important invariant is simple: if a summary exists, it must be exact enough for safe pruning. If exactness is not cheap, omit the field and keep the split.

### Existing Fields Kept As-Is

Keep `time_range` and `tags`.

- `time_range` remains useful for human inspection and existing tooling.
- `tags` remains the explicit publisher-controlled hook for routing or coarse bucketing.

The root planner should consult `field_ranges` first for date predicates and fall back to `time_range` only when the queried field matches the manifest's `time_range.field`.

### Backward Compatibility

This should be an additive manifest change.

- existing manifests without `field_ranges` or `field_terms` still load
- old splits remain searchable
- pruning simply does less work until new splits are published with summaries

Because the change is additive and defaults are empty, the first implementation does not need a manifest version bump.

## Publish-Time Summary Construction

The publish path already has the necessary inputs:

- the source documents for the split
- the authoritative mapped field types
- the split metadata being written into `RemoteSplitManifest`

Build summaries during publish, before the final manifest append:

1. For each mapped numeric/date field seen in the split, compute exact min/max.
2. For each mapped keyword/boolean field, collect the distinct set up to a fixed cap.
3. If the distinct set exceeds the cap, drop that field from `field_terms`.
4. Serialize the summaries into the manifest entry.

This work should stay on the publish path; the read path should never need to open split bundles just to derive pruning metadata.

## Root-Side Pruning Rules

Introduce a root helper that lowers supported filters into a manifest-level predicate.

### Supported Lowerings

| Query shape | Manifest field | Rule |
| --- | --- | --- |
| `term(field = value)` on `keyword` / `boolean` | `field_terms[field]` | keep only if `value` is present |
| `IN(field, [...])` lowered from SQL semijoin or DSL terms-style filters | `field_terms[field]` | keep only if any value is present |
| `range(field)` on `integer` / `float` / `date` | `field_ranges[field]` | keep only if split range intersects requested range |
| `date range` on first-class time field | `field_ranges[field]` or `time_range` | same intersection rule |
| conjunction of supported filters | both | all supported predicates must keep the split |

### Unsupported Shapes

If any predicate falls outside the supported lowering set, do not use that predicate to discard splits.

Examples:

- `match(title, 'rust')`
- query-string `q=rust`
- `wildcard`, `prefix`, `fuzzy`
- `must_not`
- `should`
- arbitrary OR expressions

Those predicates still execute correctly inside the split reader. They just do not contribute to manifest pruning in the first PR.

### Correctness Rule

The pruning rule is one-way:

- metadata may prove a split cannot match -> prune it
- metadata may fail to prove anything -> keep it

No manifest summary is allowed to create a false negative.

## Query Flow After The Change

The remote_store read path becomes:

1. Load and validate the current manifest.
2. Collect `published_splits()`.
3. Build a `SplitPruningPredicate` from the `SearchRequest`.
4. Prune the split list using `field_ranges`, `field_terms`, `time_range`, and `tags`.
5. Run the existing leaf warmth + load snapshot collection.
6. Schedule only the surviving splits through rendezvous ranking.
7. Execute the existing batched leaf search RPCs.
8. Merge partials exactly as today.

The leaf execution logic does not need to know whether the split set was pruned or not.

## Why This Is PR-Sized

This design deliberately avoids the tempting second-order work:

- no bloom filters
- no postings metadata
- no text term dictionaries in manifests
- no new transport protocol
- no early-stop search based on `LIMIT`
- no changes to split cache format or bundle layout

The change is contained to:

- publish-time manifest enrichment
- a root-side predicate extractor
- a root-side split filter helper
- optional visibility counters in explain/analyze output

## Explain / Analyze Visibility

SQL `EXPLAIN ANALYZE` now carries the same response-level counters exposed by search responses:

```json
{
  "remote_store": {
    "pruning": {
      "published_splits": 24,
      "candidate_splits": 5,
      "pruned_splits": 19,
      "assigned_splits": 5
    }
  }
}
```

Richer future analysis blocks can add manifest generation and pruning reasons without changing the current response-level block:

```json
{
  "remote_store": {
    "manifest_generation": 42,
    "published_splits": 24,
    "candidate_splits": 5,
    "pruned_splits": 19,
    "pruning_reason": "structured_filter_intersection"
  }
}
```

The richer explain/analyze surface is described separately in [docs/explain-analyze-output.md](docs/explain-analyze-output.md).

## Code Seams

- `src/storage/mod.rs`
  Add `field_ranges` / `field_terms` to `RemoteSplitManifest` and round-trip tests.
- remote_store publish path
  Build summaries while creating `RemoteSplitManifest` entries.
- `src/engine/remote_store.rs`
  Add `SplitPruningPredicate`, manifest evaluation helpers, and split-count bookkeeping before scheduling.
- SQL path entrypoints in `src/api/search/mod.rs`
  Keep using the existing `SearchRequest` lowering so SQL and DSL share the same pruning logic.

## Test Plan

1. Manifest round-trip tests for `field_ranges` and `field_terms`.
2. Unit tests for range intersection and exact-term pruning.
3. Unit tests that unsupported predicates never prune a split.
4. Unit tests that missing metadata keeps the split.
5. Integration test with two published splits where a date or keyword filter prunes one split and preserves the other.
6. Integration test proving old manifests without the new fields still load and search correctly.
7. Explain/analyze regression once the visibility block lands, asserting published/candidate/pruned split counts.

## Rollout

Implement in this order:

1. Add manifest fields and publish-time summary construction.
2. Add root-side pruning helpers and unit coverage.
3. Add one or two focused integration tests with published splits.
4. Expose candidate/pruned split counts in explain/analyze.

That sequence keeps the change debuggable: first the metadata exists, then pruning consumes it, then the explain surface makes the effect observable.