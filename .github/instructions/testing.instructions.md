# Testing Patterns

## Test Suite Summary
- **583 unit tests** (`cargo test --lib`)
- **30 consensus integration tests** (`cargo test --test consensus_integration`)
- **39 replication integration tests** (`cargo test --test replication_integration`)
- **17 REST API integration tests** (`cargo test --test rest_api_integration`)
- **669 total** (`cargo test`)

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

## Dev Cluster
```bash
./dev_cluster.sh 1    # HTTP 9200, Transport 9300, Raft ID 1
./dev_cluster.sh 2    # HTTP 9201, Transport 9301, Raft ID 2
./dev_cluster.sh 3    # HTTP 9202, Transport 9302, Raft ID 3
```
