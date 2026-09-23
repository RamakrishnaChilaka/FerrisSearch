# Dependency Review - 2026-09-22

> **Status:** Point-in-time maintenance review, not a security audit or an
> upgrade commit.

All locked direct Rust dependencies were checked against the crates.io API on
September 22, 2026. None of the locked versions was reported as yanked.
`Cargo.toml` and `Cargo.lock` remain unchanged so the write/aggregation fixes and
their before/after benchmark stay attributable to source changes rather than a
dependency migration.

## Separate migration work

| Dependency | Locked | Available | Why it is not bundled here |
|---|---:|---:|---|
| Tantivy | 0.25.0 | 0.26.2 | Search, schema, collector, and storage behavior need dedicated correctness and performance comparison. |
| DataFusion | 53.0.0 | 55.1.0 | Coordinated Arrow/SQL planner migration with residual-query and streaming regressions. |
| openraft | 0.10.0-alpha.17 | 0.10.0-alpha.34 | Stay on the selected 0.10 alpha line; 0.9.25 is a different stable line, not an automatic downgrade target. |
| object_store | 0.13.2 | 0.14.2 | Remote-store API and S3-compatible integration validation are required. |
| tonic / prost / tonic-build / tonic-types | 0.13.x | 0.14.x | Protocol stack must move together and exercise plaintext plus feature-gated TLS transport. |
| redb | 3.1.1 | 4.3.0 | Persistent Raft-store migration and restart compatibility require isolated review. |
| bincode-next | 3.0.0-rc.5 | 3.1.1 | WAL/snapshot durable-format compatibility must be proven before changing the codec. |
| rustyline | 15.0.0 | 18.0.1 | CLI API/behavior migration should be separate from engine correctness work. |

Other major-line candidates also need scoped review rather than blind updates,
including `comfy-table` 8, `getrandom` 0.4, `sha2` 0.11,
`tikv-jemallocator` 0.7, and `zstd` 0.14.

## Patch and minor refresh candidates

There are newer compatible-line releases for packages including `anyhow`,
`axum`, `chrono`, `clap`, `config`, `futures`, `moka`, `rayon`, `reqwest`,
`serde`, `serde_json`, `thiserror`, `tokio`, `tokio-rustls`, `tokio-stream`,
and `uuid`. These should be handled in a dedicated refresh with the canonical
format, clippy, build, test, TLS, restart, and benchmark gates. `usearch` also
has a newer 2.x release, but vector index API and persistence behavior make it a
separate vector-focused validation item.

Several direct dependencies were already current at review time, including
`axum-server`, `colored`, `murmur3`, `prometheus`, `rustls-pemfile`,
`sqllogictest`, `tempfile`, `tower`, `tracing`, `tracing-subscriber`, and
`url`.

`serde_yaml` remains at `0.9.34+deprecated`; that crate has no newer maintained
release. Replacing it requires selecting and validating a different YAML
implementation rather than running a normal version bump.

## Decision

Do not update dependencies in this work package. Follow up with:

1. a low-risk compatible-line refresh;
2. coordinated transport, SQL, search, persistence, and CLI migrations as
   separate reviewable changes; and
3. an explicit durable-format compatibility plan before changing
   `bincode-next` or persistent stores.
