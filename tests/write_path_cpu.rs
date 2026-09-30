//! CPU-focused bulk-apply benchmark, excluded from ordinary test runs.
//! Run in release mode with `--ignored --nocapture --test-threads=1`.
//! Input construction, warmup refresh, and result checks are outside measurement.
//! Total CPU includes a final refresh to drain background indexing; apply-only
//! CPU is reported separately so queued worker work cannot masquerade as a gain.
//! Async WAL durability isolates apply CPU cost from per-batch fsync latency;
//! this is not an end-to-end replicated-throughput benchmark.
//! Reproduce: `cargo test --release --locked --test write_path_cpu --
//! --ignored --nocapture --test-threads=1` (one command).

use ferrissearch::engine::column_cache::ColumnCache;
use ferrissearch::engine::{
    ApplyOutcome, CompositeEngine, DocumentMutation, SearchEngine, SequencedOperation,
};
use ferrissearch::wal::TranslogDurability;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BATCH_SIZE: usize = 1_000;
const WARMUP_DOCUMENTS: usize = 2_000;
const MEASURED_DOCUMENTS: usize = 100_000;

fn source(number: usize) -> Value {
    json!({
        "body": "search indexing throughput with ordered primary sequence numbers ".repeat(8),
        "category": format!("category-{}", number % 32),
        "number": number,
        "enabled": number.is_multiple_of(2),
        "metadata": {
            "region": "west",
            "description": "nested document source retained exactly ".repeat(8),
            "labels": ["search", "analytics", "replication"]
        }
    })
}

fn engine(path: &std::path::Path) -> CompositeEngine {
    CompositeEngine::new_with_mappings(
        path,
        Duration::from_secs(3_600),
        &HashMap::new(),
        TranslogDurability::Async {
            sync_interval_ms: 60_000,
        },
        Arc::new(ColumnCache::new(0, 0)),
    )
    .unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn clock_ticks_per_second() -> f64 {
    let output = Command::new("getconf").arg("CLK_TCK").output().unwrap();
    assert!(output.status.success(), "getconf CLK_TCK failed");
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn process_cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let (_, fields) = stat.rsplit_once(')').unwrap();
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    let user = fields[11].parse::<u64>().unwrap();
    let system = fields[12].parse::<u64>().unwrap();
    user + system
}

fn verify(engine: &CompositeEngine) {
    assert_eq!(
        engine.doc_count(),
        (WARMUP_DOCUMENTS + MEASURED_DOCUMENTS) as u64
    );
    assert_eq!(
        engine.sequence_stats().processed_checkpoint,
        Some((WARMUP_DOCUMENTS + MEASURED_DOCUMENTS - 1) as u64)
    );
    for number in [
        0,
        WARMUP_DOCUMENTS,
        WARMUP_DOCUMENTS + MEASURED_DOCUMENTS - 1,
    ] {
        let document = engine
            .get_document(&format!("doc-{number}"))
            .unwrap()
            .unwrap();
        assert_eq!(document, source(number));
    }
}

fn report(
    mode: &str,
    cpu_ticks: u64,
    apply_cpu_ticks: u64,
    ticks_per_second: f64,
    wall: Duration,
    apply_wall: Duration,
) {
    println!(
        "{}",
        json!({
            "benchmark": "bulk_apply_cpu_cost",
            "mode": mode,
            "documents": MEASURED_DOCUMENTS,
            "batch_size": BATCH_SIZE,
            "warmup_documents": WARMUP_DOCUMENTS,
            "durability": "async",
            "cpu_seconds": cpu_ticks as f64 / ticks_per_second,
            "cpu_us_per_document": cpu_ticks as f64 * 1_000_000.0
                / ticks_per_second / MEASURED_DOCUMENTS as f64,
            "apply_cpu_us_per_document": apply_cpu_ticks as f64 * 1_000_000.0
                / ticks_per_second / MEASURED_DOCUMENTS as f64,
            "wall_seconds": wall.as_secs_f64(),
            "apply_wall_seconds": apply_wall.as_secs_f64(),
            "includes_final_refresh": true,
            "verified": true
        })
    );
}

#[test]
#[ignore = "release-mode CPU benchmark; run explicitly on pinned CPUs"]
fn primary_bulk_apply_cpu_cost() {
    let runtime = runtime();
    let _runtime_context = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let engine = engine(directory.path());
    let batches = (0..WARMUP_DOCUMENTS + MEASURED_DOCUMENTS)
        .step_by(BATCH_SIZE)
        .map(|start| {
            (start..start + BATCH_SIZE)
                .map(|number| (format!("doc-{number}"), source(number)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut batches = batches.into_iter();
    for batch in batches.by_ref().take(WARMUP_DOCUMENTS / BATCH_SIZE) {
        engine
            .bulk_add_documents_with_receipt_at_term(batch, 1)
            .unwrap();
    }
    engine.refresh().unwrap();
    let ticks_per_second = clock_ticks_per_second();
    let cpu_before = process_cpu_ticks();
    let wall_before = Instant::now();
    for (batch_number, batch) in batches.enumerate() {
        let receipt = engine
            .bulk_add_documents_with_receipt_at_term(batch, 1)
            .unwrap();
        assert_eq!(receipt.doc_ids.len(), BATCH_SIZE);
        assert_eq!(
            receipt.start_seq_no,
            Some((WARMUP_DOCUMENTS + batch_number * BATCH_SIZE) as u64)
        );
    }
    let apply_wall = wall_before.elapsed();
    let apply_cpu_ticks = process_cpu_ticks() - cpu_before;
    engine.refresh().unwrap();
    let wall = wall_before.elapsed();
    let cpu_ticks = process_cpu_ticks() - cpu_before;
    verify(&engine);
    report(
        "primary",
        cpu_ticks,
        apply_cpu_ticks,
        ticks_per_second,
        wall,
        apply_wall,
    );
}

#[test]
#[ignore = "release-mode CPU benchmark; run explicitly on pinned CPUs"]
fn replica_bulk_apply_cpu_cost() {
    let runtime = runtime();
    let _runtime_context = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let engine = engine(directory.path());
    let batches = (0..WARMUP_DOCUMENTS + MEASURED_DOCUMENTS)
        .step_by(BATCH_SIZE)
        .map(|start| {
            (start..start + BATCH_SIZE)
                .map(|number| SequencedOperation {
                    seq_no: number as u64,
                    primary_term: 1,
                    mutation: DocumentMutation::Index {
                        doc_id: format!("doc-{number}"),
                        source: source(number),
                    },
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut batches = batches.into_iter();
    for batch in batches.by_ref().take(WARMUP_DOCUMENTS / BATCH_SIZE) {
        engine.apply_replica_batch(batch).unwrap();
    }
    engine.refresh().unwrap();
    let ticks_per_second = clock_ticks_per_second();
    let cpu_before = process_cpu_ticks();
    let wall_before = Instant::now();
    for batch in batches {
        let receipt = engine.apply_replica_batch(batch).unwrap();
        assert_eq!(receipt.outcomes.len(), BATCH_SIZE);
        assert!(receipt.all_operations_processed);
        assert!(
            receipt
                .outcomes
                .iter()
                .all(|outcome| *outcome == ApplyOutcome::Applied)
        );
    }
    let apply_wall = wall_before.elapsed();
    let apply_cpu_ticks = process_cpu_ticks() - cpu_before;
    engine.refresh().unwrap();
    let wall = wall_before.elapsed();
    let cpu_ticks = process_cpu_ticks() - cpu_before;
    verify(&engine);
    report(
        "replica",
        cpu_ticks,
        apply_cpu_ticks,
        ticks_per_second,
        wall,
        apply_wall,
    );
}
