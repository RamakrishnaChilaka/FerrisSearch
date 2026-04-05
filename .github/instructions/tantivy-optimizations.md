# Tantivy SQL Streaming Architecture

## Goal
Keep explicit-column `tantivy_fast_fields` SQL queries columnar and bounded-memory, while preserving correct planner metadata and residual DataFusion semantics.

## Current Status
- Local GROUP BY-fallback and explicit-column fast-field queries can stream lazily from `HotEngine` via `BitSetCollector` + `SqlStreamingBatchHandle`.
- Local SQL string fast-field reads share the same direct-ordinal helper (`StringFastFieldReader`) across grouped keys, selective/full Arrow array builders, `_id` reads, and bitset streaming.
- Flat `sql_record_batch()` no longer keeps `_id` on a separate top-doc-order decode/clone loop; `_id` now reuses the same per-segment array/take/reorder path as the other string fast fields, and `_id`-only queries can stay on the fast path.
- Streamed explicit-column SQL now keeps remote shard gRPC streams live into DataFusion `StreamingTable` partitions after reading only the first remote response for metadata.
- Local explicit-column streamed SQL no longer prebuilds a local `Vec<RecordBatch>` before registration; the coordinator drains `SqlStreamingBatchHandle` on the search pool as DataFusion pulls batches.
- `SqlRecordBatchStream` now carries shard-level `total_hits`, `collected_rows`, and actual `streaming_used` metadata on every batch so the coordinator can emit accurate NDJSON `meta` frames before draining the rest of the stream.
- Queries that need stored-doc fallback (`SourceFallback`) or `_score` still stay on the existing scored/stored-doc path and still honor `sql_group_by_scan_limit`.

## Current Execution Architecture

### 1. Local explicit-column streaming
- `HotEngine::sql_streaming_batch_handle()` is the primary local streaming API.
- It collects all matching docs via `BitSetCollector`, stores them as per-segment manual bitsets, and returns `total_hits`, `collected_rows`, and a lazy `next_batch()` closure up front.
- `SegmentBitSetCursor` walks one segment's set bits lazily; `StreamingBatchState` and `StreamingSegmentState` open fast-field readers only for the active segment and build Arrow batches incrementally.
- Zero-hit handles must still emit one empty batch with the canonical schema before returning `None`, so local `StreamingTable` partitions remain schema-stable.

### 2. Remote explicit-column streaming
- The coordinator uses `open_sql_batch_stream_to_shard()` to read only the first remote `SqlRecordBatchStream` response eagerly.
- That first response yields the first `RecordBatch` plus stable shard metadata: `total_hits`, `collected_rows`, and `streaming_used`.
- The remaining tonic stream stays live and flows directly into a `StreamingTable` partition through `SqlBatchStream::into_stream()`.
- If any later batch disagrees with the first-batch metadata, decoding fails loudly instead of letting the coordinator emit a misleading NDJSON `meta` frame.

### 3. Coordinator / DataFusion boundary
- The streamed SQL path registers per-shard `StreamingTable` partitions rather than staging all batches in a coordinator `MemTable` first.
- `direct_sql_input_schema()` derives the canonical input schema from planner dependencies plus index mappings before execution starts.
- Every streamed batch is normalized through `normalize_sql_batch_for_schema()` so remote Arrow schema drift cannot silently corrupt the registered `StreamingTable` schema.
- `project_batch_to_sql_columns()` still reorders batch columns into SQL SELECT order to preserve the DataFusion 53 LIMIT workaround.

### 4. Buffered compatibility paths that still exist
- `sql_streaming_batches()` remains the eager compatibility wrapper that drains `SqlStreamingBatchHandle` into `Vec<RecordBatch>` for tests and callers that have not yet been converted.
- The remote gRPC server now serves `SqlRecordBatchStream` by draining `SqlStreamingBatchHandle` batch-by-batch on the search pool, so remote shards no longer need to prebuild all Arrow batches before tonic starts consuming them.
- `SELECT *` and other wildcard/export-shaped queries still use `materialized_hits_fallback`; they are not on the explicit-column streaming path.

## Remaining Work
- Add a dedicated export-style path for large wildcard / `SELECT *` scans so they can stream without full coordinator-side materialization.
- Add stored-doc / `_score` compatible streaming only if it can preserve search-aware execution without collapsing into generic row materialization.

## Review / Verification Focus
- `GROUP BY` fallback queries that are fully fast-field-backed should stream past the old TopDocs-style cap without returning incorrect aggregates.
- Local lazy streaming must match eager `sql_streaming_batches()` results batch-for-batch on the same query.
- Zero-hit lazy handles must emit exactly one empty batch and then terminate.
- Remote streamed metadata (`total_hits`, `collected_rows`, `streaming_used`) must stay stable across all batches from the same shard.
- Mixed local/remote streamed partitions must preserve canonical schema, SQL column order, and residual mixed-case identifier behavior.
