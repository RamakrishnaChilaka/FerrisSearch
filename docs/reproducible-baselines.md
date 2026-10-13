# Reproducible Correctness And Performance Baselines

`scripts/repro_baseline.py` runs one controlled matrix against both current
engine modes:

- mutable, routed, replicated `local_shards`; and
- immutable, shardless `remote_store`.

The harness is correctness-first. It generates deterministic service-event
documents, computes expected query results in Python, rejects partial or
inexact responses, and only retains a completed run when every measured
response matches the oracle.

This is a pre-1.0 evidence harness, not a production sizing tool or an
OpenSearch comparison.

## Reproduce On A New Linux Machine

Install the Rust toolchain and Python 3, clone the repository, and run:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install requests

python3 scripts/repro_baseline.py \
  --config scripts/benchmark_profiles/fs008.json \
  --output-dir benchmark-runs/fs008-$(date -u +%Y%m%dT%H%M%SZ)
```

The harness:

1. builds the release binary with the profile's recorded `RUSTFLAGS` and
   feature set;
2. starts an isolated three-node cluster on HTTP ports 19200-19202 and
   transport ports 19300-19302;
3. uses a shared `file://` object-store root inside the run directory;
4. creates one index per engine with the recorded shard and replica topology;
5. ingests the same deterministic dataset through `_bulk` and the dedicated
   remote split publication API;
6. records the first query for each case as cold preparation, then executes
   configured warmup and measured rounds at the configured concurrency;
7. validates match-all, term, range, terms-aggregation, and stats-aggregation
   results against the exact oracle; and
8. stops the cluster while preserving logs, data, objects, raw results, and
   the generated summary.

The output directory contains:

| Artifact | Purpose |
|---|---|
| `results.json` | Raw configuration, requests, oracle values, every latency sample, ingest throughput, host/build metadata, process snapshots, disk usage, and object-store counter deltas |
| `summary.md` | Automatically generated human-readable result tables and scope qualifications |
| `cluster/logs/` | Per-node server logs |
| `cluster/data/` | Per-node Raft, shard, WAL, and remote-cache state |
| `object-store/` | Shared local object-store objects used by `remote_store` |

The script refuses to reuse a non-empty output directory. It records the Git
commit, dirty-worktree state, harness/config/binary hashes, release profile,
feature flags, OS, CPU, memory, CPU affinity, topology, schema, ingest method,
warmup, repetitions, concurrency, cache state, correctness oracle, latency,
throughput, CPU, RSS, virtual memory, file descriptors, threads, logical and
allocated disk bytes, and object-store request/transfer counters.

## Cache And Measurement Boundaries

The cluster and indexes are new for each run. The first request for each query
case is retained as cold preparation. Measured samples run after that request
and the configured warmup rounds, so the generated report labels them warm.
The local ingest path can populate the kernel page cache before the first
query; the harness does not claim a power-cycle cold-disk measurement.

Client latency covers the HTTP request and JSON decoding. Correctness checking
occurs after timing. CPU and memory values come from each node's `/_metrics`
endpoint. Disk values are filesystem snapshots. Object-store values are
FerrisSearch counter deltas for logical successful payload bytes and GET/PUT
outcomes; they are not provider billing bytes.

## S3-Compatible Run

The default `file://` backend makes the complete matrix reproducible without an
external service. To exercise a configured S3-compatible service, export its
standard AWS credentials and endpoint variables, then pass a shared URI:

```bash
eval "$(scripts/rustfs_dev.sh env)"

python3 scripts/repro_baseline.py \
  --config scripts/benchmark_profiles/fs008.json \
  --storage-uri "s3://${FERRIS_REMOTE_STORE_BUCKET}/fs008-$(date -u +%s)" \
  --output-dir benchmark-runs/fs008-rustfs-$(date -u +%Y%m%dT%H%M%SZ)
```

Start RustFS first with `scripts/rustfs_dev.sh up` when using that local
S3-compatible environment. `results.json` reports the external S3-compatible
check as `pass` only when both profiles complete against an `s3://` URI;
failed attempts report `failed`, and default local-file runs report `skipped`.
Do not report a skipped external-service path as a passing S3-compatible run.

## Smaller Development Run

Copy the checked-in JSON profile and reduce `dataset.documents`,
`execution.warmup_rounds`, and `execution.measured_rounds`. Keep both profiles
in the matrix. The modified profile and its SHA-256 are recorded in the raw
result, so exploratory runs remain distinguishable from the checked-in
baseline profile.

## Harness Tests

The deterministic generator, oracle, percentile calculations, metrics parser,
configuration validation, and Markdown generation have dependency-light unit
coverage:

```bash
python3 -m unittest discover -s scripts -p 'test_repro_baseline.py'
```
