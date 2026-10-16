# Bounded Bulk Ingestion

`POST /_bulk` and `POST /{index}/_bulk` accept streamed NDJSON for
`local_shards`. FerrisSearch validates the complete action structure before
creating indices or executing document operations. It stages the request on
the coordinator's data volume, then executes bounded batches and streams the
ordered JSON response. This is disk-backed staging, not immediate execution
as request bytes arrive.

## Request And Result Semantics

- Malformed action/metadata, unsupported safety parameters, invalid UTF-8, and
  missing source lines reject the request before any writes. Errors retain
  their action line and item position where applicable.
- Invalid JSON sources, reserved source fields, and oversized source lines
  remain item errors. Valid neighboring items can execute.
- Delete consumes no source line. Other actions consume one nonempty source
  line. Blank lines and CRLF retain the existing parser behavior.
- Results retain request order, including duplicate IDs, conflicts, no-ops,
  and mixed actions. Updates preserve realtime GET/CAS, incarnation pinning,
  and bounded operation-ID retry semantics.
- Bulk is not a transaction. Shard validation and execution use bounded RPC
  units; a failed unit does not roll back an earlier successful unit.
- `refresh=true` and bare `refresh` perform one primary-owned visibility round
  per mutated shard after all units finish. Successful no-ops on a touched
  shard share its report. `_shards` still describes data certification;
  `_refresh` reports visibility separately.
- `?pretty` formats individual spooled results without buffering the complete
  response in the pretty-JSON middleware.

See [ADR 0001 D12](adr/0001-write-consistency-and-retry-contract.md#d12-bulk-semantics)
for write semantics and the
[compatibility matrix](opensearch-compatibility.md#bulk-parsing-and-failures)
for the supported OpenSearch-style subset.

## Limits And Admission

These startup settings apply per HTTP node. Set them in
`config/ferrissearch.yml` or through the corresponding `FERRISSEARCH_`
environment variable.

| Setting | Default | Meaning |
|---|---:|---|
| `bulk_max_concurrent_requests` | 2 | Active request leases, including owned work, response, and file cleanup; 1–64 |
| `bulk_max_request_bytes` | 8 GiB | Logical HTTP body bytes per request |
| `bulk_max_spool_bytes` | 16 GiB | Combined protected input and result file reservations per request |
| `bulk_idle_timeout_ms` | 30,000 | Idle body-read and response-producer send budget; not a total execution deadline |

All settings must be positive. The combined concurrency/spool limit must fit
the metrics counter range. Admission rejects excess requests with
`429 es_rejected_execution_exception` before polling their bodies; it does not
create an unbounded waiting queue.

Fixed implementation bounds:

| Resource | Bound |
|---|---:|
| Input channel and response channel | Two 64 KiB data chunks each |
| One NDJSON line | 32 MiB |
| Coordinator batch | 256 items or 4 MiB consumed input, plus the final bounded item |
| One shard RPC unit | 64 items |
| Concurrent target RPCs or final refresh RPCs | 8 |
| Distinct valid indices per request | 128 |
| Distinct shard targets for requested refresh | 4,096 |
| One protected result record | 128 MiB |

An oversized action line rejects the request with 413. An oversized source
line produces an attributable item-level 400 and preserves later positions.
The NDJSON limit does not override the separate 32 MiB encoded-WAL-frame
limit: IDs and internal wrappers also consume WAL bytes.

The coordinator creates anonymous temporary files under its node data
directory. Frames bind their length, order, payload checksum, and private file
identity. Replay and response emission verify frames before decoding; corrupt,
substituted, reordered, or truncated files fail loudly. No checksum inventory
or whole-request collection remains in memory. These files are temporary,
not a durable ingest queue or a restart/retry log.

Spool limits account for reservations, not free filesystem capacity. Leave
headroom for the maximum admitted spool reservations **and** shard, WAL, Raft,
and maintenance data. This pipeline does not provide a total-process memory
ceiling, tenant quotas, or unified node resource accounting.

## Cancellation And Failures

Cancelling a handler stops later batches and shard units. Cancelling a
response body stops the bounded producer. Owned I/O and already-dispatched
RPCs retain the request lease until they settle; dropping the caller does not
abandon FS013 write permits or make an issued write safe to retry. Update
cancellation is checked between reads, CAS attempts, and conflict retries.
Client disconnects can still leave an indeterminate outcome.

Input size, parsing, and staging failures occur before document mutation.
Spool exhaustion or I/O failure after dispatch returns
`500 write_outcome_unknown` with the cause and the completed earlier-batch
count. A response failure after headers aborts the body instead of emitting
a shortened success response. Retry ambiguous operations only with their
original operation IDs and within the supported retry-retention window.

## Operational Signals

All labels are bounded; these metrics contain no index or document IDs.

| Metric | Boundary |
|---|---|
| `ferrissearch_bulk_active_requests` | Leases retained through execution, response, or cleanup |
| `ferrissearch_bulk_spool_reserved_bytes` | Physical frame/result reservations until file close |
| `ferrissearch_bulk_admission_rejections_total` | Admission rejections before body polling |
| `ferrissearch_bulk_staged_input_bytes_total` | Logical HTTP input bytes actually staged |
| `ferrissearch_bulk_execution_batches_total` | Bounded coordinator batches, not acknowledged documents |
| `ferrissearch_bulk_cancelled_requests_total` | Leases released without completed response or explicit API failure |

Existing engine write counters still measure actual engine results. HTTP
handler timing does not include draining the streamed response.

## Reproduce Memory And Result Evidence

The Linux probe uses fresh single-node processes, request durability, zero
replicas, and disabled column caches. It generates chunked input without
assembling the logical request in client memory, overwrites one document
with a changing integer and 512 KiB unmapped padding, and verifies every
sequence/version/copy report plus final refreshed contents and count.

```bash
cargo build --bin ferrissearch
python3 scripts/bulk_memory_probe.py \
  --binary target/debug/ferrissearch --profile debug \
  --output-dir target/fs014-memory-run --mib 256 2048
```

Python 3 and the existing baseline harness's `requests` dependency are
required. The output directory must not already exist. The probe preserves
raw RSS/high-water samples, phase counters, node logs/data, build/source/binary
hashes, configuration, hardware, and correctness results. It stops only its
owned child if the live RSS allowance is exceeded and fails on missing
metrics, incomplete responses, leaked reservations, or oracle mismatches.

The default acceptance bounds are at most 384 MiB added child RSS above the
post-index-create baseline and at most 128 MiB growth across the input sizes.
These are probe bounds for the recorded workload, not universal production
sizing limits. RSS excludes client memory and kernel page cache. No caches are
dropped; elapsed times are not a controlled throughput comparison.

### Recorded FS-014 Run

The 2026-10-15 run used the command above on Ubuntu 24.04.3 under WSL2, an
AMD EPYC 7763 host with 16 exposed logical CPUs and 33,629,806,592 bytes of
memory, and Rust 1.99.0. The debug build had no additional features and
contained the uncommitted FS-014 candidate over `d1aa226`. Each case used a
fresh single-node, single-shard process with the workload and cache settings
described above.

| Requested padding | Actual NDJSON bytes | Ordered items | Added peak child RSS |
|---|---:|---:|---:|
| 256 MiB | 268,469,650 | 512 | 145.38 MiB |
| 2 GiB | 2,147,761,066 | 4,096 | 158.24 MiB |

Added peak RSS grew by 12.86 MiB. Every item and final-document oracle passed;
active request leases and spool reservations returned to zero. This is
bounded-memory evidence for that overwrite workload, not a throughput gain
or a bound for arbitrary documents, replica counts, or concurrent traffic.

Raw results, resource samples, node logs, and data remain in the ignored local
artifact directory `target/fs014-validation/memory-framed-256-2048/`.
`results.json` records the complete environment and identifies the measured
source and binary:

```text
source_sha256: cbdb779725bca75d3dd8899349e96c0914938c7318254d5159b9d4117513d992
binary_sha256: 557a152887b13dbe5f8496c80db132ae6c658ddc502ca06499e884161cb97687
```

These measurements describe that hashed candidate, not every later build.

## Boundary Regressions

```bash
cargo test --lib bulk
cargo test --features protocol-trace --lib fs014
cargo test --test rest_api_integration fs014
python3 -m unittest discover -s scripts -p test_bulk_memory_probe.py
```

The real-Raft controls cover multi-batch ordering and final visibility on every
copy, failed-replica range attribution and copy-floor fencing, and cancellation
after primary WAL assignment while the issued RPC still owns admission.
