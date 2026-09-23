---
description: "Use for configuration, server TLS, metrics, worker pools, background tasks, CI, development clusters, and operational resource controls."
applyTo: "src/config/**,src/metrics/**,src/tasks.rs,src/worker.rs,config/**,Dockerfile,.github/workflows/**,scripts/ci-local.sh,dev_cluster*.sh"
---

# Operations, Configuration, And Resource Instructions

## Configuration

`AppConfig` loads defaults, `config/ferrissearch.yml`, then
`FERRISSEARCH_*` environment overrides. Keep that order stable and add serde
defaults for new fields so older config files continue to load.

- Validate unsafe combinations at startup rather than silently downgrading.
- Transport TLS requires `transport-tls`; HTTP TLS requires `http-tls`.
  Enabling either without its feature or required files is an error.
- Install the rustls ring provider before constructing TLS state.
- `storage_uri` selects local, `file://`, or S3-compatible object storage.
- `0` values can be meaningful: disabled column cache, unlimited SQL group scan,
  or disabled auto-flush. Do not collapse zero into missing.
- `column_cache_size_percent` is capped at 90 and applies to effective memory:
  host physical memory capped by visible finite cgroup hard limits. It does not
  bound total process memory.
- Never log credentials, API keys, private-key contents, or auth headers.

When adding config, cover default, YAML, environment, invalid, and
feature-disabled behavior. Update `config/ferrissearch.yml` and README only for
operator-facing fields.

## Runtime Scheduling

- Tokio owns asynchronous control-plane work: Raft, HTTP/gRPC futures,
  heartbeats, timers, and coordination.
- Tokio's blocking pool owns bounded lifecycle/file operations that must be
  awaited: shard open/close, orphan cleanup, blocking Raft database work,
  process/procfs metric collection, startup cgroup-memory discovery, and
  translog fsync. Blocking refresh, flush, and force-merge waits also belong
  here because shard-local maintenance exclusion can outlive a steady-state
  write operation.
- `WorkerPools` owns dedicated rayon pools for steady-state search and write
  engine work. Document writes and replica applies stay on the write pool;
  maintenance waits must not consume it. Do not run Raft or network futures on
  rayon.
- Avoid nested rayon use. Grouped segment scans deliberately use scoped OS
  threads where nested pool use could deadlock.
- Background maintenance is not automatically low priority just because it was
  spawned. New compaction, hydration, recovery, export, or GC work needs an
  explicit concurrency and byte budget.

The current search/write pools isolate CPU threads, but FerrisSearch does not
yet provide complete admission control, cancellation propagation, tenant
quotas, or unified memory/I/O accounting. Do not claim those properties.

## Tasks And Maintenance

`TaskManager` tracks force-merge tasks in process memory. Cluster task IDs are
coordinator-local and not Raft-replicated. The client must query the coordinator
that created the cluster task.

- Task transitions must expose queued, running, completed, and failed states.
- Preserve per-node dispatch errors and per-shard failures.
- Retention keeps task memory bounded; do not add unbounded result payloads.
- Acknowledging enqueue is not acknowledging task completion.
- Cancellation, persisted task recovery, and a cluster-global task registry
  are future work unless implemented end to end.

Refresh, flush, and force merge fan out the local node alongside remote nodes.
Do not run the local operation inline before remote dispatch begins.

## Metrics And Tracing

- HTTP metrics use normalized route labels. Never put index names, document IDs,
  split IDs, task IDs, SQL text, or error strings in Prometheus labels.
- Counters should represent actual events: engine write success, document
  count, request count, or task transition—not attempts with ambiguous meaning.
- Histograms measure one documented boundary. Keep API wall time distinct from
  engine execution, object fetch, queue time, and merge time.
- Gauges for deleted resources must be reset or removed so stale series do not
  survive.
- Metrics gathering that reads files or procfs runs off the async worker.
- Startup sets `ferrissearch_column_cache_effective_memory_bytes` and
  `ferrissearch_column_cache_budget_bytes`; zero effective memory denotes an
  explicitly disabled cache, not a total-process memory ceiling.
- Preserve SQL execution-mode and remote pruning visibility when refactoring
  response metadata.

The roadmap requires request/query IDs across HTTP, gRPC, split assignment, and
object-store work. When adding tracing, use bounded attributes and propagate
context rather than encoding IDs into metric labels.

## Deadlines, Cancellation, And Admission

Existing endpoint timeouts are not a full cancellation protocol. New work must
distinguish:

- client deadline;
- coordinator queue and execution budget;
- child RPC deadline;
- cancellation observed by collectors, hydration, and residual execution; and
- cleanup of reservations and temporary files.

Do not add a timeout that merely drops the waiting future while CPU or I/O work
continues unbounded. Admission should reserve the constraining resource
(concurrency, bytes, or both) before expensive allocation or download.

## CI And Local Validation

Canonical checks are:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build
cargo test
```

`scripts/ci-local.sh` mirrors CI. Keep CI and the script synchronized. External
S3-compatible tests remain explicitly gated and must report skip vs pass
accurately. Development cluster scripts must give every node a unique data
directory, HTTP port, transport port, Raft ID, and complete seed-host list.

GitHub Actions installs the moving stable Rust toolchain. When CI reports a
compiler-specific lint failure, reproduce the exact runner version with
`cargo +<version>` for format, Clippy, build, and tests; do not change the
developer's global default toolchain or weaken `-D warnings` to match an older
local compiler.

Operational changes need failure tests: port conflicts, missing files, invalid
URIs, feature mismatches, exhausted queues/budgets, task failures, and shutdown
or restart behavior.
