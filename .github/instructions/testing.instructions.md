# Testing Patterns

## Test Suite Summary
- **910 unit tests** (`cargo test --lib`)
- **64 CLI tests** (`cargo test --bin ferris-cli`)
- **33 consensus integration tests** (`cargo test --test consensus_integration`)
- **40 replication integration tests** (`cargo test --test replication_integration`)
- **38 REST API integration tests** (`cargo test --test rest_api_integration`)
- **1 SQL correctness harness** (`cargo test --test sql_correctness`) — sqllogictest `.slt` format, 170 assertions across 4 files
- **1086 total** (`cargo test`)

## Running Tests
```bash
cargo test                                      # All tests
cargo test --lib                                # Unit tests only
cargo test --test consensus_integration         # Raft consensus tests
cargo test --test replication_integration       # Replication tests
cargo test --test replication_integration --features transport-tls  # Replication tests with encrypted gRPC transport
cargo test --test rest_api_integration          # REST API integration tests
cargo test -- test_name                         # Single test by name
```

## Unit Test Conventions
- Tests live in `#[cfg(test)] mod tests` at the bottom of each source file
- Use `#[tokio::test]` for async tests
- Use `tempfile::TempDir` for isolated data directories
- Test every code path: happy path, edge cases, error conditions, empty inputs
- For WAL auto-flush or replay changes, add regressions for disabled thresholds (`flush_threshold_bytes = 0`), zero global checkpoint safety (no auto-truncate), and stale `translog.committed` checkpoints that force a replayed suffix after a prior batch commit.
- For auto-flush concurrency changes, add regressions proving maintenance ticks defer instead of blocking when the text flush path or vector persistence path is already busy.
- For maintenance scheduling changes, add a `#[tokio::test(flavor = "current_thread")]` regression that blocks the Tantivy writer lock from another thread and proves the async runtime still makes progress while the maintenance tick waits.
- For refresh/flush fan-out changes, add one regression that the coordinator still dispatches its local node through the per-node maintenance path, plus a transport regression that maintenance reopens persisted assigned shards but refuses to create missing UUID directories.
- For async scheduling changes around shard open/close, orphan cleanup, translog fsync, redb-backed Raft storage, or other blocking wrappers, add a `#[tokio::test(flavor = "current_thread")]` regression that holds the relevant lock or resource from another thread and proves the runtime still advances while the wrapper waits.
- For index UUID / orphan-cleanup fixes, add a regression that an auto-created index opens its local shard with the same UUID stored in cluster state, plus a restart-path regression that missing expected UUID directories cause cleanup to bail out instead of deleting unknown shard data.
- For CLI parser fixes, add multiline regressions when behavior depends on SQL statement structure (`EXPLAIN`, table extraction, quoted identifiers), not just single-line happy paths.
- For global SQL routing fixes, add both helper-level coverage and a `POST /_sql/stream` regression using a quoted hyphenated index name with keyword-casing variants, including the aliasless `count(*)` fast path.
- For SQL identifier case-sensitivity fixes, add helper-level canonicalization coverage plus REST regressions for both buffered and streamed SQL endpoints using real mixed-case mapping fields, and cover both unquoted source references and quoted exact-identifier preservation on the residual/DataFusion path.
- For `ferris-cli` interactive features, test command parsing and completion token boundaries in pure helpers; keep watch-mode behavior factored so the logic is covered without relying on terminal I/O in tests.
- For `ferris-cli` SQL metadata/footer changes, keep search-stage counts distinct from final SQL row counts and add pure helper coverage for the displayed labels so `matched_hits` is not presented as returned rows.
- For streamed `ferris-cli` SQL changes, add pure chunk-boundary NDJSON parsing tests and at least one REST integration test covering the global `POST /_sql/stream` route the console uses.
- For feature-gated transport TLS changes, run both `cargo test --lib` and `cargo test --lib --features transport-tls`; enabling TLS without the feature must error instead of silently downgrading to plaintext.
- For transport TLS end-to-end coverage, also run `cargo test --test replication_integration --features transport-tls`.
- For SQL fast-field string changes, add regressions for both `sql_record_batch()` and `sql_streaming_batches()` that assert `_id` and keyword values survive the optimized ordinal path.
- For local streamed SQL execution changes, add unit coverage that `sql_streaming_batch_handle()` matches `sql_streaming_batches()` batch-for-batch on the same query and that zero-hit handles emit exactly one empty batch before returning `None`.
- For new `SearchRequest` / `QueryClause` variants, add a serde JSON roundtrip regression because search DSL requests cross transport boundaries as serialized JSON.
- For streamed shard SQL transport changes, add a real gRPC integration test that forces multiple Arrow batches from `forward_sql_batch_stream_to_shard()` / `SqlRecordBatchStream`, not just unit tests around IPC decoding.
- For streamed SQL transport metadata changes, add coverage for `total_hits`, `collected_rows`, and actual `streaming_used`, plus at least one `/_sql/stream` regression where the streamed endpoint must keep `streaming_used=false` because the shard falls back to `sql_record_batch()`.
- For JoinCluster or cluster-state transport fixes, add one roundtrip regression that proves `raft_node_id`, `unassigned_replicas`, index `mappings`, index `settings`, and index `uuid` survive proto conversion, one regression that unknown field types fail snapshot decoding instead of being coerced, plus concurrent gRPC regressions for duplicate `raft_node_id` rejection and full voter-set preservation across overlapping joins.
- For `_id` fast-path refactors, add a multi-segment sorted-result regression that proves `_id` stays aligned with projected data columns after segment concatenation and reorder.
- For distributed hit-merge changes, add unit coverage for `merge_sorted_hit_lists()` and a multi-node REST regression where only one shard returns hits but the coordinator still must apply a custom sort.
- For `_cat/shards` state fixes, add a regression that a live shard copy on one node does not make a different assigned copy on another node appear `STARTED`; display state must be per copy, not per shard ID.
- For node startup/rejoin cleanup changes, add tempdir regressions that prove empty pre-catch-up state does not delete live UUID directories, and that authoritative UUID sets still remove true orphaned directories.

## Integration Test Infrastructure
### Consensus Tests (tests/consensus_integration.rs)
- Spin up real Raft clusters (1-3 nodes) in-process
- Use `create_raft_instance_mem()` for in-memory log store
- Test leader election, failover, log replication, membership changes
- Each test gets isolated temp directories
- No external services needed — everything runs in-process

### Replication Tests (tests/replication_integration.rs)
- Spin up real gRPC servers with isolated shard managers
- Test primary-to-replica replication, bulk replication, recovery
- Test checkpoint tracking, ISR behavior
- Uses actual `TransportClient` + `TransportService` over localhost
- Seed `ClusterManager` with node/index/shard metadata before gRPC write, replication, or search calls; transport now rejects unknown shards instead of implicitly creating them from empty metadata

## Test Helper Patterns
- `tokio::time::timeout()` to prevent hung tests
- `tokio::time::sleep()` for Raft election settling
- Assert on cluster state after Raft commands
- Verify shard routing, node membership, index metadata

## Development Workflow
1. **Read first** — understand existing code
2. **Implement** — make code changes
3. **Unit tests** — cover every branch (empty inputs, edge cases, errors)
4. **Integration tests** — if feature involves Raft, gRPC, or multi-component interaction
5. **Live test** — `cargo run` + curl
6. **Fix bugs** — add a test for each bug discovered
7. **Coverage audit** — check every branch has a test
8. **Update README** — examples, roadmap, test counts
9. **Update copilot-instructions.md** — if architecture changed

## Hybrid SQL Test Expectations
- For hybrid SQL planner changes, add unit tests for:
	- pushdown extraction
	- quoted index names
	- grouped analytics planning
	- LIMIT pushdown detection and rewritten SQL preservation
	- residual predicate detection
	- `needs_id` / `needs_score` detection
	- truncation flag logic (explicit LIMIT = not truncated, flat no-LIMIT fast-field queries may truncate at the 100K ceiling, GROUP BY fallback uses the separate scan-limit/error path)
- For direct fast-field execution changes, add tests that assert eligible queries use fast-field readers without requiring `_source` materialization.
- For API-level SQL changes, validate both execution modes where practical:
	- `tantivy_grouped_partials` for eligible grouped SQL queries over matched docs
	- `tantivy_fast_fields` for local non-`SELECT *` queries with columnar access
	- `materialized_hits_fallback` for wildcard projection or distributed compatibility paths
- **LIMIT correctness**: Always assert exact row counts for LIMIT queries — `LIMIT N` must produce exactly N rows, not N × number_of_shards. This was a previous test gap.
- **DataFusion 53 LIMIT regression**: Include pure DataFusion tests that reproduce the projection-reorder LIMIT bug (schema-order SELECT works, reverse-order SELECT without the workaround returns too many rows). These tests document the upstream bug and verify the `project_batch_to_sql_columns` workaround.
- **Projection helper invariant**: `project_batch_to_sql_columns` must use planner-derived dependencies (`required_columns`, `needs_id`, `needs_score`) instead of a second ad-hoc SQL walker. Complex expressions such as `CASE`, `HAVING`, and grouped residual SQL can otherwise register an incomplete MemTable schema and fail at execution time.
- **Distributed grouped-partials key stability**: Add a regression test that round-trips grouped partials through `encode_partial_aggs()` / `decode_partial_aggs()` and verifies integer group keys still merge with local shard keys. Coordinator merge must not split logically identical buckets like `0` and `0.0` across local vs remote shards.
- **Distributed SQL transport coverage**: The sqllogictest harness is single-node and single-shard. Any bug that depends on remote shard fan-out, gRPC transport, or partial-state encode/decode must also have a multi-node integration test (REST or transport-level) in addition to local result-correctness coverage.
- **Schema drift recovery**: Add unit tests for both safe and unsafe remote-schema drift. Cover at least: (1) a drifted-first batch with later typed batches still preserves the later column and canonical type, (2) a missing-first batch fills later-missing values with nulls instead of dropping the column, and (3) an uncastable drifted batch fails loudly instead of degrading values into nulls.
- **Alias shadowing**: Cover cases where a SELECT alias matches a real field name. Real source fields must stay in `required_columns` when projection/GROUP BY expressions or wrapped HAVING aggregate inputs still need them, while pure computed aliases referenced only from ORDER BY/HAVING wrappers like `COALESCE(total, 0)` must still be stripped.
- **Duplicate grouped output aliases**: Cover the end-to-end `/_sql` path where a grouped column alias and metric alias collide, for example `SELECT brand AS total, count(*) AS total ... GROUP BY brand`. Grouped-partials planning must reject the query with an ambiguous-column error instead of silently picking one meaning.
- **Grouped ORDER BY aggregate expressions**: Cover aliasless grouped queries like `SELECT author, SUM(upvotes) ... ORDER BY SUM(upvotes) DESC`. Supported aggregate expressions in ORDER BY must resolve back to the grouped metric and stay on `tantivy_grouped_partials` instead of falling through to the generic fast-fields/DataFusion path.
- **Zero-column SQL batches**: Add regressions for literal-only queries such as `SELECT 1 FROM ... WHERE text_match(...)`. The planner must not fake `needs_score`, and the execution path must still return one output row per hit.
- **Bound-column IR migrations**: When the small planner binder lands, add unit tests that resolve the same identifier name across clauses to different semantic kinds: source field vs output alias vs synthetic `_id` / `_score`. Cover at least WHERE alias residual behavior, HAVING/ORDER BY output-space binding, aggregate-argument source-field binding under alias shadowing, GROUP BY source-only eligibility, and real `score` vs synthetic `_score` separation.
- **Truncation flag**: Assert `truncated=false` for explicit LIMIT queries. Assert `truncated=true` only for flat fast-field queries when `matched_hits` exceeds the internal 100K ceiling without an explicit LIMIT. GROUP BY fallback queries should error via `group_by_scan_limit_exceeded` instead of returning `truncated=true`.
- **GROUP BY scan limit**: Expression GROUP BY and unsupported-aggregate GROUP BY fall to `tantivy_fast_fields` with a raised scan limit (`sql_group_by_scan_limit`, default 1M). Test that: (1) `has_group_by_fallback` is true for expression GROUP BY / unsupported aggs, false for plain GROUP BY and flat queries, (2) the `group_by_scan_limit_exceeded` error fires when a capped fallback path collects fewer rows than it matched (unit test with `sql_group_by_scan_limit: 1` and a text/source-fallback GROUP BY), (3) a fully fast-field-backed local expression GROUP BY can stream past that tiny limit and still succeed, and (4) fallback queries that reference text/source-fallback columns or `_score` stay correct instead of being forced onto the bitset streaming path.
- **Residual expression tree**: Queries like `ROUND(AVG(x), 2)`, `AVG(x) + AVG(y)`, `SUM(a) / COUNT(*)`, `MAX(x) - MIN(x)` must use `tantivy_grouped_partials` with `residual_expr`. Test that: (1) hidden metrics are extracted for each inner aggregate, (2) the projected metric has `residual_expr: Some(...)`, (3) ROUND/CAST/arithmetic are correctly represented in the tree, (4) `eval_residual_expr` produces correct values including integer preservation for `MAX - MIN` on integer fields, and (5) ORDER BY on residual-expr metrics works correctly.
- Live tests should inspect the `planner`, `execution_mode`, `streaming_used`, and `truncated` fields, not just the returned rows.
- Add regression tests when planner or execution changes accidentally widen the fallback path for queries that should stay search-aware.

## SQL Correctness Testing Strategy

### Industry Standard: sqllogictest
The industry standard for SQL engine correctness testing is [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki), originally from SQLite. It's used by **DataFusion, CockroachDB, DuckDB, RisingWave, Databend, CnosDB**, and many others.

- **Format**: `.slt` files containing `statement ok`, `statement error`, and `query <type> <sort_mode>` records with expected output after `----`.
- **Rust crate**: [`sqllogictest`](https://crates.io/crates/sqllogictest) (v0.29.1) from risinglightdb — parser + runner, 12M+ downloads.
- **DataFusion's approach**: 200+ `.slt` files covering GROUP BY, HAVING, LIMIT, ORDER BY, aggregations, joins, subqueries, window functions. Also runs SQLite's 5M+ test queries and Postgres compatibility tests.
- **Key principle**: Tests assert **result correctness** (actual values), not just plan properties (mode, strategy). This is what caught the GROUP BY + LIMIT bug — our tests only checked `plan.limit_pushed_down` and `plan.grouped_sql.is_some()`, never the actual row values.

### Testing Rules for SQL Changes
1. **Always test result values, not just plan metadata.** A test that asserts `plan.limit_pushed_down == true` but never checks if the returned rows are correct is incomplete.
2. **Always test WHERE/HAVING on aliases.** SELECT aliases like `count(*) AS posts` must NEVER be pushed down to Tantivy — they are computed values, not physical fields. Test that alias-referencing predicates stay as residual.
3. **Test multi-shard correctness.** GROUP BY results must be identical regardless of how data is distributed across shards. Compare single-shard vs multi-shard results, and cross the remote partial-state encode/decode boundary when the query uses distributed grouped partials.
4. **Test boundary conditions for LIMIT.** `LIMIT N` on grouped partials must return exactly N rows after merge+sort, not N rows per shard.
5. **Test HAVING with LIMIT and OFFSET together.** The execution order must be: merge → HAVING → sort → LIMIT/OFFSET.
6. **Every new SQL feature MUST have sqllogictest coverage.** When adding a new SQL capability (new aggregate function, new clause, new pushdown, new execution path), add `.slt` tests in `tests/slt/` that assert the correct output values. This is a BLOCKING requirement — do not merge SQL changes without corresponding `.slt` tests.
7. **Same-index semijoin changes need three layers of coverage.** Add planner/unit coverage for supported and rejected shapes, at least one single-node end-to-end `/_sql` regression with grouped inner `HAVING` or `text_match`, and one multi-node regression where the inner grouped query merges remote shard partials before the outer filter is lowered.

### sqllogictest Scope
- `tests/sql_correctness.rs` builds a single-node, single-shard sample cluster. It is the right place to assert SQL result correctness, but it cannot catch distributed transport or remote partial-state serialization bugs by itself.

### sqllogictest Infrastructure (Implemented)
- **Crate**: `sqllogictest = "0.29"` as a dev dependency
- **Runner**: `tests/sql_correctness.rs` — implements sync `DB` trait via `FerrisDB` adapter that calls `execute_sql_for_testing()` with `block_in_place` + `Handle::current()` bridge
- **Test files**: `tests/slt/*.slt` — automatically discovered and run
- **Dataset**: 10 HN-style docs with known values (5 authors, 5 categories, deterministic upvotes/comments)
- **Coverage**: 170 assertions across 4 `.slt` files covering `count(*)`, `GROUP BY`, `HAVING`, same-index semijoins, case-insensitive unquoted columns, `LIMIT`, `OFFSET`, single and multiple top-level `text_match` predicates, `sum`, `avg`, `min`, `max`, alias non-pushdown, tie-breaking ORDER BY, and CASE-based grouped aggregates
- **HAVING coverage rule**: Always test HAVING with **both** alias-based (`HAVING cnt > 1`) and aggregate-expression (`HAVING COUNT(*) > 1`) forms. These take different code paths in the planner — alias goes through `expr_to_field_name`, aggregate expression goes through `resolve_having_name` → `parse_grouped_metric`. Missing one form caused a regression where HAVING with aggregate expressions silently fell to the wrong execution path.
- **Adding tests**: Create new `.slt` files in `tests/slt/` — the runner picks them up automatically
- **Tie-breaking**: Always use secondary sort (e.g., `ORDER BY posts DESC, author ASC`) in `.slt` tests to avoid non-deterministic ordering

## Dev Cluster
```bash
./dev_cluster.sh 1    # HTTP 9200, Transport 9300, Raft ID 1
./dev_cluster.sh 2    # HTTP 9201, Transport 9301, Raft ID 2
./dev_cluster.sh 3    # HTTP 9202, Transport 9302, Raft ID 3
```
