# Testing Patterns

## Test Suite Summary
- **604 unit tests** (`cargo test --lib`)
- **30 consensus integration tests** (`cargo test --test consensus_integration`)
- **39 replication integration tests** (`cargo test --test replication_integration`)
- **17 REST API integration tests** (`cargo test --test rest_api_integration`)
- **15 SQL correctness tests** (`cargo test --test sql_correctness`) — sqllogictest `.slt` format, 46 assertions across 2 files
- **705 total** (`cargo test`)

## Running Tests
```bash
cargo test                                      # All tests
cargo test --lib                                # Unit tests only
cargo test --test consensus_integration         # Raft consensus tests
cargo test --test replication_integration       # Replication tests
cargo test --test rest_api_integration          # REST API integration tests
cargo test -- test_name                         # Single test by name
```

## Unit Test Conventions
- Tests live in `#[cfg(test)] mod tests` at the bottom of each source file
- Use `#[tokio::test]` for async tests
- Use `tempfile::TempDir` for isolated data directories
- Test every code path: happy path, edge cases, error conditions, empty inputs

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
	- truncation flag logic (explicit LIMIT = not truncated, no LIMIT = may truncate at 100K ceiling)
- For direct fast-field execution changes, add tests that assert eligible queries use fast-field readers without requiring `_source` materialization.
- For API-level SQL changes, validate both execution modes where practical:
	- `tantivy_grouped_partials` for eligible grouped SQL queries over matched docs
	- `tantivy_fast_fields` for local non-`SELECT *` queries with columnar access
	- `materialized_hits_fallback` for wildcard projection or distributed compatibility paths
- **LIMIT correctness**: Always assert exact row counts for LIMIT queries — `LIMIT N` must produce exactly N rows, not N × number_of_shards. This was a previous test gap.
- **DataFusion 53 LIMIT regression**: Include pure DataFusion tests that reproduce the projection-reorder LIMIT bug (schema-order SELECT works, reverse-order SELECT without the workaround returns too many rows). These tests document the upstream bug and verify the `project_batch_to_sql_columns` workaround.
- **Truncation flag**: Assert `truncated=false` for explicit LIMIT queries. Assert `truncated=true` only when matched_hits exceeds the internal 100K ceiling without an explicit LIMIT.
- Live tests should inspect the `planner`, `execution_mode`, and `truncated` fields, not just the returned rows.
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
3. **Test multi-shard correctness.** GROUP BY results must be identical regardless of how data is distributed across shards. Compare single-shard vs multi-shard results.
4. **Test boundary conditions for LIMIT.** `LIMIT N` on grouped partials must return exactly N rows after merge+sort, not N rows per shard.
5. **Test HAVING with LIMIT and OFFSET together.** The execution order must be: merge → HAVING → sort → LIMIT/OFFSET.
6. **Every new SQL feature MUST have sqllogictest coverage.** When adding a new SQL capability (new aggregate function, new clause, new pushdown, new execution path), add `.slt` tests in `tests/slt/` that assert the correct output values. This is a BLOCKING requirement — do not merge SQL changes without corresponding `.slt` tests.

### sqllogictest Infrastructure (Implemented)
- **Crate**: `sqllogictest = "0.29"` as a dev dependency
- **Runner**: `tests/sql_correctness.rs` — implements sync `DB` trait via `FerrisDB` adapter that calls `execute_sql_for_testing()` with `block_in_place` + `Handle::current()` bridge
- **Test files**: `tests/slt/*.slt` — automatically discovered and run
- **Dataset**: 10 HN-style docs with known values (5 authors, 5 categories, deterministic upvotes/comments)
- **Coverage**: 15 assertions covering `count(*)`, `GROUP BY`, `HAVING`, `LIMIT`, `OFFSET`, `text_match`, `sum`, `avg`, `min`, `max`, alias non-pushdown, tie-breaking ORDER BY
- **Adding tests**: Create new `.slt` files in `tests/slt/` — the runner picks them up automatically
- **Tie-breaking**: Always use secondary sort (e.g., `ORDER BY posts DESC, author ASC`) in `.slt` tests to avoid non-deterministic ordering

## Dev Cluster
```bash
./dev_cluster.sh 1    # HTTP 9200, Transport 9300, Raft ID 1
./dev_cluster.sh 2    # HTTP 9201, Transport 9301, Raft ID 2
./dev_cluster.sh 3    # HTTP 9202, Transport 9302, Raft ID 3
```
