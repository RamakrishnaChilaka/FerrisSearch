# Terms Aggregation Counter Benchmark - 2026-09-22

> **Status:** Focused single-host microbenchmark for one collector change. This
> is not a service-level objective, capacity claim, or general FerrisSearch
> versus OpenSearch comparison.

## Question

Does selecting a bounded dense ordinal counter for small string dictionaries
improve `terms` aggregation latency without regressing the sparse
high-cardinality fallback?

The implementation uses a dense `Vec<u64>` for segment dictionaries with at
most 1024 terms and a direct `HashMap<u64, u64>` collector variant above that
bound. Dense and sparse strategies are selected once per segment.

## Compared binaries

| Binary | Source | Release SHA-256 |
|---|---|---|
| Before | Unchanged release binary from `2d6423ae895f99ab06cd54ca2e54734360bc6872` | `a203d014d85d4d64b5c7a7fd578260f1c57e175e3c6de7229e7ce1a01efdad64` |
| After | Current scoped working tree, default features, `cargo build --locked --release --bin ferrissearch` | `5d8583d885eb3fc4dbddd8191799372d6a1f12b6a7a24e8a11f442e413ee6c45` |

Dependencies were not changed. See the
[dated dependency review](dependency-review-2026-09-22.md).

## Environment and controls

- WSL2 Linux `6.6.87.2-microsoft-standard-WSL2`, x86-64
- AMD EPYC 7763 host, 16 logical CPUs, 33,628,008,448 bytes reported memory
- Rust `1.95.0`; Cargo `1.95.0`
- one FerrisSearch node, one primary shard, zero replicas
- request translog durability; 600-second refresh interval
- column cache disabled
- server pinned to CPUs 0-3; client pinned to CPUs 4-7
- server cgroup: 8 GiB `memory.max`, zero swap
- release profile, default Cargo features
- HTTP request plus JSON decoding is inside the timing boundary; oracle
  validation happens immediately after timing
- 50 warmup requests and 500 measured requests per case per run
- two paired runs in opposite order, yielding 1000 measured samples per binary
  and case

Every request was checked against a deterministic exact-answer oracle. Before
and after requests and result hashes match.

## Dataset and identical storage

The harness generated 50,000 documents with:

- `event_id`: integer;
- `dense_tag`: 64 uniformly repeated keyword values; and
- `sparse_tag`: 4096 one-document values plus 64 repeated hot values, for a
  4160-term dictionary while returning only the 64 dominant buckets.

The canonical bulk payload SHA-256 is
`dbbe96e5b456e5967eff6544823742397a5953337c2c1fee709e35a08943d41a`.

Separate ingestion produced different Tantivy segment boundaries, so those
exploratory runs are retained but are not used for the performance conclusion.
The final comparison cloned one clean prepared data directory for each binary.
All four measured runs used the same four segment IDs and document counts. The
normalized prepared-data archive SHA-256 is
`e35a6752c38c4fd97c41d07fa2b4feb425d8b7a8496dc04f0b0a5a4d5f6749de`.

Force merge was not used. The existing asynchronous force-merge path failed
with the already-deferred Tantivy `SegmentManager` error during preparation;
this work does not change that subsystem.

## Requests

Each field was measured with `match_all` and with:

```json
{"range":{"event_id":{"gte":12500}}}
```

All requests used `size: 0` and a `terms` aggregation with `size: 64`.

## Results

Pooled results across the two paired runs:

| Case | Before p50 | After p50 | p50 reduction | Before p95 | After p95 | p95 reduction |
|---|---:|---:|---:|---:|---:|---:|
| dense, match-all | 3.283 ms | 2.444 ms | 25.6% | 4.816 ms | 3.396 ms | 29.5% |
| dense, filtered | 3.224 ms | 2.509 ms | 22.2% | 4.561 ms | 3.462 ms | 24.1% |
| sparse, match-all | 74.450 ms | 72.733 ms | 2.3% | 88.378 ms | 83.701 ms | 5.3% |
| sparse, filtered | 3.941 ms | 3.371 ms | 14.4% | 5.542 ms | 4.638 ms | 16.3% |

The dense p50 reduction was 21.1-30.1% for match-all and 18.2-24.9% for the
filtered query across the individual paired runs. The sparse match-all control
was effectively flat to modestly faster: p50 improved 1.3-3.9%, while one
paired p95 was 1.7% slower. This evidence supports the dense optimization and
does not show a material sparse-path regression; it does not justify a broader
aggregation or end-to-end throughput claim.

## Reproduction

The harness refuses to overwrite evidence, verifies affinity/cgroup limits,
checks the prepared segment layout, and validates every result:

```bash
taskset -c 4-7 .venv/bin/python scripts/benchmark_terms_aggregation.py \
  --url http://127.0.0.1:29420 \
  --index ferris-terms-control-before-20260922 \
  --label identical-before \
  --binary "$BINARY" \
  --pid "$SERVER_PID" \
  --output "$OUTPUT" \
  --documents 50000 \
  --warmup 50 \
  --repetitions 500 \
  --expected-server-cpus 0-3 \
  --expected-client-cpus 4-7 \
  --expected-memory-max-bytes 8589934592 \
  --expected-segments 4 \
  --reuse-prepared-index \
  --prepared-data-sha256 \
    e35a6752c38c4fd97c41d07fa2b4feb425d8b7a8496dc04f0b0a5a4d5f6749de
```

The server was launched in a transient user unit with `MemoryMax=8G`,
`MemorySwapMax=0`, and `taskset -c 0-3`; only the binary and cloned data
directory differed.

Generate the validated comparison:

```bash
.venv/bin/python scripts/summarize_terms_aggregation.py \
  --before \
    docs/benchmark-results/terms-aggregation-20260922/identical-before.json \
    docs/benchmark-results/terms-aggregation-20260922/identical-before-repeat.json \
  --after \
    docs/benchmark-results/terms-aggregation-20260922/identical-after.json \
    docs/benchmark-results/terms-aggregation-20260922/identical-after-repeat.json \
  --preparation \
    docs/benchmark-results/terms-aggregation-20260922/exploratory/control-before.json \
  --rustc-version 'rustc 1.95.0 (59807616e 2026-04-14)' \
  --cargo-version 'cargo 1.95.0 (f2d3ce0bd 2026-03-21)' \
  --host-logical-cpus 16 \
  --output \
    docs/benchmark-results/terms-aggregation-20260922/summary.json
```

## Evidence

- [Validated summary](benchmark-results/terms-aggregation-20260922/summary.json)
- [Baseline run 1](benchmark-results/terms-aggregation-20260922/identical-before.json)
- [Modified run 1](benchmark-results/terms-aggregation-20260922/identical-after.json)
- [Baseline run 2](benchmark-results/terms-aggregation-20260922/identical-before-repeat.json)
- [Modified run 2](benchmark-results/terms-aggregation-20260922/identical-after-repeat.json)

Repository JSON replaces task-local binary paths with
`<session-evidence>/`; each affected report records that sanitization, and no
numeric sample or result hash was changed.

The `exploratory/` subdirectory preserves earlier runs that exposed the segment
layout and large-response confounders; they are evidence of methodology
iteration, not inputs to the reported speedup.
