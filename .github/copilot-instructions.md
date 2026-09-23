# FerrisSearch Repository Instructions

## Mission And Maturity

FerrisSearch is a pre-1.0 distributed search engine in Rust. It combines Tantivy
full-text search, columnar SQL execution, vector search, Raft-managed metadata,
and an experimental object-store-backed read path.

The product direction is an object-store-native search and analytics system
with a defensible systems-research contribution. The current repository is a
serious prototype, not yet a production-safe OpenSearch replacement. Preserve
that distinction in code, documentation, benchmarks, and release claims.

## Sources Of Truth

Use this precedence when information conflicts:

1. Current source code and tests describe implemented behavior.
2. [`docs/architecture-roadmap.md`](../docs/architecture-roadmap.md) describes
   intended 12-24 month direction, gates, and non-negotiable invariants.
3. Path-specific files in [`.github/instructions/`](instructions/) describe
   subsystem conventions and known traps.
4. [`docs/next-50-tasks.md`](../docs/next-50-tasks.md) ranks current execution
   priorities and dependencies; refresh its source evidence before starting.
5. README and other documents are explanatory and may lag.

Do not implement roadmap prose as though it already exists. Do not preserve a
historical behavior solely because a document claims it is current. Verify
important claims against source before changing code.

## Load Context Progressively

Start here, then open only the guidance relevant to the files being changed.
For architecture or multi-subsystem work, read the roadmap first; use the
next-50 backlog for priority and dependency context. For test requirements,
read `testing.instructions.md`. A detailed registry and maintenance policy
lives in [`docs/ai-agent-guide.md`](../docs/ai-agent-guide.md).

| Area | Read before changing |
|---|---|
| HTTP APIs | `api.instructions.md` |
| Cluster metadata and settings | `cluster.instructions.md` |
| Raft | `consensus.instructions.md` |
| New Raft-replicated mutations | `control-plane.instructions.md` |
| Tantivy, vector, SQL engine | `engine.instructions.md`, `tantivy-optimizations.instructions.md` |
| Hybrid/search-aware SQL | `hybrid.instructions.md` |
| Node startup and lifecycle | `node.instructions.md` |
| Replication and recovery | `replication.instructions.md` |
| Query DSL and aggregation | `search.instructions.md` |
| Authentication and authorization | `security.instructions.md` |
| Shard lifecycle | `shard.instructions.md` |
| Object store and remote reads | `storage.instructions.md` |
| gRPC and protobuf | `transport.instructions.md` |
| WAL and sequence ownership | `wal.instructions.md` |
| Config, workers, tasks, metrics, CI | `operations.instructions.md` |
| CLI | `cli.instructions.md` |
| Tests | `testing.instructions.md` |
| Docs and benchmark claims | `documentation.instructions.md` |

## Architecture At A Glance

- `local_shards`: mutable, shard-owned Tantivy + USearch engines with a
  generation-based WAL and synchronous replica RPCs.
- `remote_store`: shardless, immutable split bundles and generation manifests
  in local or S3-compatible object storage. Roots prune and schedule splits;
  data-node leaves hydrate and cache split readers. Standard document CRUD is
  read-only for this engine; publication uses a dedicated endpoint.
- Raft manages cluster membership, master identity, index metadata, mappings,
  settings, and dynamic security metadata. Document data is not in the Raft log.
- Every HTTP node is a coordinator. Leader-only metadata writes and shard-owned
  operations are forwarded internally rather than exposed as topology errors.
- Tantivy performs matching, fast-field access, and eligible shard-local
  partial aggregation. DataFusion performs residual relational work.

The roadmap converges mutable ingest and immutable object-store data into one
lifecycle. Do not create a third independent data model without an approved
architecture decision.

## Global Correctness Invariants

- **Raft is mandatory for production nodes.** `Node.raft` and `AppState.raft`
  are `Arc<RaftInstance>`. Only transport test constructors keep Raft optional.
- **Cluster-state mutations go through Raft.** Followers forward writes to the
  leader. Never mutate follower state as a fallback.
- **Every node coordinates.** Do not return "send to master" or "not the
  leader" for a routable client operation.
- **Shard data paths use index UUIDs**, never index names:
  `<data_dir>/<index_uuid>/shard_<id>`.
- **Primary writes own sequence numbers.** Carry the WAL-assigned sequence or
  batch range with the operation result; never infer it from a later shared
  checkpoint. Replica apply and recovery preserve these values.
- **Synchronous replication failures are request failures.** Do not turn
  partial replication into success-shaped responses.
- **Remote publication is not multi-writer safe yet.** Do not claim otherwise
  or add unfenced read-modify-write manifest updates.
- **Wire and storage decoding fails loudly.** Never replace malformed protocol,
  manifest, snapshot, partial-aggregation, or WAL data with defaults.
- **Blocking disk and engine work stays off Tokio workers.** Use Tokio's
  blocking pool for lifecycle I/O and dedicated worker pools for steady-state
  search/write work. Keep Raft and control-plane futures on Tokio.
- **Tantivy terms match schema types.** Use the existing typed-term and typed
  document helpers. Numeric bucket identity must preserve integer precision;
  do not round integer keys through floating-point representations.
- **SQL is search-aware, not row-first.** Prefer pushdown, fast fields, and
  compact shard-local partials. Keep materialized-hit execution as a
  compatibility fallback.
- **Remote and local reads require explicit snapshot semantics.** Until the
  roadmap protocol exists, avoid claims of cross-engine read-after-write or
  point-in-time consistency.
- **Security metadata never stores plaintext secrets.** Protected system
  indices remain hidden from ordinary APIs and SQL/catalog surfaces.
- **Errors remain diagnosable.** Preserve underlying causes at API, transport,
  task, and bulk-item boundaries; do not add broad catches or silent skips.

## Engineering Workflow

Inquiry, diagnosis, review, and planning are read-only unless changes are
requested. For implementation, keep the approved scope and reuse authorization
already given; ask only when a wrong assumption would materially affect behavior,
compatibility, or verification. Preserve unrelated work and do not publish it.

1. Trace the current call path, callers, tests, and matching scoped instructions.
   Search symbols and headings before reading large files; do not reload
   unchanged guidance or unrelated subsystems.
2. Identify current-behavior work versus roadmap work. Roadmap work must name
   its gate, prerequisites, and success evidence.
3. Reuse existing helpers before introducing another implementation. Avoid
   speculative cleanup, abstractions, or dependency upgrades.
4. Make one coherent change across the necessary domain, persistence, transport,
   coordinator, error, and documentation surfaces.
5. Prove result-level behavior, including boundaries and failures. Concurrent
   writes need operation-identity checks; distributed behavior needs transport
   or multi-node coverage, not only local planner assertions.
6. Choose verification from the final diff and affected boundaries. Profile
   performance hypotheses and compare equivalent workloads before claiming gains.
7. Update the owning instructions when their contract changes. Keep implemented
   behavior, limitations, measured evidence, and future direction separate.

For broad work, use the roadmap's AI-session operating contract: keep scope to
one coherent work package, document assumptions, protect invariants, and leave
the next session a verifiable handoff rather than speculative partial wiring.

## Validation

Choose checks by risk; do not run a full Rust gate for unrelated prose changes.

| Change | Verification |
|---|---|
| Docs or instructions only | `git diff --check` and relevant link/instruction checks |
| Localized Rust behavior | Formatting and the narrowest result-level regression |
| Public traits, persistence, transport, or concurrency | Compile affected consumers/features and add boundary-level regressions |
| Broad cross-module Rust changes | Focused checks first, then the canonical CI checks below |

Do not weaken assertions, ignore failures, or suppress lints to obtain a pass.
Rerun checks affected by subsequent edits, not equivalent successful commands
over an unchanged diff. Review findings need a concrete failure or violated
contract; a missing test is a coverage gap, not proof of a runtime defect.

Canonical CI remains:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build
cargo test
```

`./scripts/ci-local.sh` mirrors CI. Start with the narrowest relevant command,
then widen based on impact. Examples:

```bash
cargo test --lib
cargo test --test consensus_integration
cargo test --test replication_integration
cargo test --test rest_api_integration
cargo test --test restart_regression
cargo test --test sql_correctness
cargo test --test replication_integration --features transport-tls
```

Tests that depend on external S3-compatible storage may be environment-gated;
do not report a skipped external-service suite as exercised.

## Repository Map

| Path | Responsibility |
|---|---|
| `src/api/` | Axum REST API, coordination, SQL endpoints |
| `src/cluster/` | Cluster state, routing metadata, reactive settings |
| `src/consensus/` | openraft types, state machine, persistent Raft store |
| `src/engine/` | Tantivy, vector, hybrid SQL, remote split execution |
| `src/node/` | Startup, bootstrap/join, lifecycle, server wiring |
| `src/replication/` | Primary-to-replica fan-out |
| `src/security/` | HTTP authn/authz and dynamic security reads |
| `src/shard/` | Local engine ownership, UUID paths, ISR tracking |
| `src/storage/` | Object-store abstraction, manifests, bundles, cache |
| `src/transport/`, `proto/` | Internal gRPC protocol and clients |
| `src/wal/` | Generation-based translog and recovery reads |
| `tests/` | Integration, restart, SQL logic, and object-store coverage |
| `docs/` | Architecture, operations, evidence, and roadmap |

`src/indexing/` is currently a placeholder; do not describe it as a completed
ingest subsystem.

## Documentation And Product Claims

Use "OpenSearch-style REST API subset" unless a tested compatibility matrix
supports a narrower claim. Qualify benchmark numbers with hardware, dataset,
query, build profile, and reproduction commands. Never turn roadmap targets
into present-tense features. When behavior or architecture changes, update the
roadmap/backlog only if priorities or gates changed, not to record routine
implementation detail.
