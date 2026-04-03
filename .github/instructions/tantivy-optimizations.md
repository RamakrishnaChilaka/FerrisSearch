# Tantivy Optimization Sprint — Weekend + Next Week

## Goal
Eliminate the GROUP BY scan limit and improve fast-field SQL performance by removing the TopDocs bottleneck and batch-reading string ordinals.

## Priority 1: Bitset Collector (eliminates scan limit)

**Problem:** `TopDocs::with_limit(N)` caps GROUP BY fallback at 1M docs. Beyond that → error.

**Solution:** Write a custom `BitSetCollector` that returns a `Vec<(SegmentOrdinal, BitSet)>` of all matching docs. Bitset is 500KB for 4M docs — trivial.

**Steps:**
1. Implement `Collector` trait → `BitSetCollector` that allocates a `BitSet(max_doc)` per segment
2. `SegmentCollector::collect(doc_id, _score)` → set bit
3. Wire into `sql_record_batch()` as alternative to `TopDocs` when `has_group_by_fallback = true`
4. Iterate matched bits per segment, read fast fields, produce Arrow batches in 8K chunks
5. Remove the `sql_group_by_scan_limit` cap (or make it a soft warning instead of hard error)
6. Update `execute_sql_query()` to use new path

**Key files:** `src/engine/tantivy.rs`, `src/api/search.rs`, `src/hybrid/planner.rs`

**Risk:** Tantivy 0.25's internal `BitSet` may not be publicly constructible. Options: `bitvec` crate, `roaring::RoaringBitmap`, or a simple `Vec<u64>` manual bitset (trivial to implement — `set(n)` = `words[n/64] |= 1 << (n%64)`). Verify Tantivy 0.25 API before choosing.

**Verification:**
- GROUP BY LOWER(author) on 4M HN docs should complete without error
- Memory should stay <10MB for the bitset (not 64MB like TopDocs)
- Benchmark: compare latency vs current TopDocs path on 100K, 500K, 2M match counts

---

## Priority 2: Batch String Ordinal Reads + Shared Helper

**Problem:** In `build_selective_array()`, numeric columns use `col.first(doc_id)` which is already a direct columnar lookup — effectively a batch-friendly tight loop. **The only genuinely slow per-doc path is string columns:** `col.term_ords(doc_id)` allocates an iterator per doc, calls `next()` once, then `ord_to_str()`. The grouped collector already optimized this via `GroupKeyReader::Str` which extracts `Column<u64>` ordinals directly with `ord_col.first_vals()`.

**Note:** Numeric fast-field reads and the grouped partial collector (1024-doc batches) are already efficient — not targets.

**Solution:** Extract the `GroupKeyReader::Str` ordinal pattern into a shared helper and apply it to `build_selective_array()` and `_id` reads.

**Steps:**
1. Extract a shared helper that opens `(StrColumn, Column<u64>)` from a fast-field reader — reuse in both `GroupKeyReader::Str` and `build_selective_array()`
2. For string columns in `build_selective_array()`: use `ord_col.first(doc_id)` instead of `str_col.term_ords(doc_id)` iterator
3. For `_id` reads in `sql_record_batch()`: same pattern — `ord_col.first(doc_id)` → `str_col.ord_to_str(ord)`
4. Batch the ordinal→string resolution: collect unique ordinals first, resolve each once via `ord_to_str`, then map back to build `StringArray`

**Key files:** `src/engine/tantivy.rs`

**Verification:**
- Benchmark: `SELECT author, count(*) FROM hackernews GROUP BY author` with 2M matches
- Measure: compare string column read time with `term_ords()` vs direct `Column<u64>` ordinal path
- No correctness change — same results, less overhead

---

## Priority 3: Streaming Arrow Batches (streaming TableProvider)

**Problem:** Current path collects all matched docs into TopDocs, then reads fast fields per-segment (with column cache or `build_selective_array()`), concatenates into one Arrow RecordBatch, and passes to DataFusion as a MemTable. The per-segment read is already efficient (column cache + `take()`), but the **total memory is O(all matched docs)** because the final concatenated batch holds all rows. For 4M matched docs with 5 columns: ~64MB TopDocs heap + ~230MB Arrow batch.

**Note:** The current path is NOT naive full materialization — it already groups by segment, uses column cache, and uses `take()` for sparse reads. P3 is about making it **streaming** so DataFusion can spill to disk instead of holding everything in memory.

**Solution:** Custom DataFusion `TableProvider` that yields 8K-row RecordBatches from the bitset + fast-field reader.

**Steps:**
1. Implement `TableProvider` → `ExecutionPlan` → `RecordBatchStream`
2. Each `poll_next()` reads the next 8K matched docs from the bitset, reads fast fields, returns a RecordBatch
3. DataFusion's streaming GROUP BY handles the rest (can spill to disk)
4. Wire into `execute_sql_query()` as replacement for the current "collect all → MemTable" path

**This depends on priorities 1-2 being done first.** The bitset collector feeds the streaming reader, batch string reads make each batch fast.

**Key files:** new `src/hybrid/tantivy_table_provider.rs`, `src/api/search.rs`

**Verification:**
- GROUP BY on 4M matches should use <10MB memory (bitset + batch window) vs ~300MB current (64MB TopDocs + ~230MB Arrow batch)
- No scan limit needed — DataFusion handles memory via spill

---

## Not Doing (upstream proposals, post-sprint)
- Arrow-native column export (requires Tantivy API change)
- `DocValuesProvider` trait (requires Tantivy RFC)
- Segment aggregation hooks (our Collector impl is good enough)

---

## Schedule

| Day | Target |
|-----|--------|
| Sat AM | P1: BitSetCollector impl + wire into sql_record_batch |
| Sat PM | P1: Test on 4M HN dataset, benchmark vs TopDocs |
| Sun AM | P2: Batch string ordinal reads in build_selective_array + _id reads |
| Sun PM | P2: Extract shared ordinal helper, benchmark |
| Mon-Tue | P2: Polish, verify correctness on all SQL paths |
| Wed-Thu | P3: Streaming TableProvider (if P1-P2 are solid) |
| Fri | Polish, tests, benchmark report, update docs |

---

## Memory Comparison

| Approach | 4M match memory | 40M match memory |
|----------|-----------------|-------------------|
| TopDocs (current, 1M cap) | 16 MB (TopDocs) + Arrow batch | N/A (errors) |
| TopDocs (uncapped) | 64 MB (TopDocs) + Arrow batch | 640 MB (TopDocs) + Arrow batch |
| Bitset + streaming 8K batches | **0.5 MB (bitset) + 2 MB (batch window)** | **5 MB (bitset) + 2 MB (batch window)** |

Note: Arrow batch memory depends on column count and types. Typical 5-column batch is ~10 bytes/row. At 4M rows that's ~40MB on top of TopDocs. The streaming approach holds only 8K rows (~80KB) at a time.
