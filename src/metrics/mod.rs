//! Prometheus metrics for FerrisSearch.
//!
//! Exposes application-level counters, histograms, and gauges that are
//! not available from the OS — request latencies, query throughput,
//! indexing rates, shard health, and process stats.

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;

/// Global Prometheus registry shared across all modules.
static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

// ── HTTP request metrics ────────────────────────────────────────────

pub static HTTP_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_http_requests_total",
            "Total number of HTTP requests",
        ),
        &["method", "path", "status"],
    )
    .expect("metric: http_requests_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: http_requests_total");
    counter
});

pub static HTTP_REQUEST_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new(
            "ferrissearch_http_request_duration_seconds",
            "HTTP request duration in seconds",
        )
        .buckets(vec![
            0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ]),
        &["method", "path"],
    )
    .expect("metric: http_request_duration_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: http_request_duration_seconds");
    histogram
});

// ── Search metrics ──────────────────────────────────────────────────

pub static SEARCH_QUERIES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_search_queries_total",
        "Total number of search queries executed",
    )
    .expect("metric: search_queries_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: search_queries_total");
    counter
});

pub static SEARCH_LATENCY_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "ferrissearch_search_latency_seconds",
            "Search query latency in seconds",
        )
        .buckets(vec![
            0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
        ]),
    )
    .expect("metric: search_latency_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: search_latency_seconds");
    histogram
});

// ── Indexing metrics ────────────────────────────────────────────────

pub static DOCS_INDEXED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_docs_indexed_total",
        "Total number of documents indexed",
    )
    .expect("metric: docs_indexed_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: docs_indexed_total");
    counter
});

pub static BULK_REQUESTS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_requests_total",
        "Total number of bulk index requests",
    )
    .expect("metric: bulk_requests_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_requests_total");
    counter
});

pub static BULK_DOCS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_docs_total",
        "Total number of documents processed via bulk API",
    )
    .expect("metric: bulk_docs_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_docs_total");
    counter
});

pub static INDEX_LATENCY_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "ferrissearch_index_latency_seconds",
            "Index/bulk API request latency in seconds (includes routing, forwarding, replication)",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
        ]),
    )
    .expect("metric: index_latency_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: index_latency_seconds");
    histogram
});

// ── SQL metrics ─────────────────────────────────────────────────────

pub static SQL_QUERIES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_sql_queries_total",
            "Total number of SQL queries by execution mode",
        ),
        &["mode"],
    )
    .expect("metric: sql_queries_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: sql_queries_total");
    counter
});

pub static SQL_LATENCY_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "ferrissearch_sql_latency_seconds",
            "SQL query latency in seconds",
        )
        .buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
        ]),
    )
    .expect("metric: sql_latency_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: sql_latency_seconds");
    histogram
});

// ── Cluster / shard gauges ──────────────────────────────────────────

pub static CLUSTER_NODES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_cluster_nodes",
        "Number of nodes in the cluster",
    )
    .expect("metric: cluster_nodes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: cluster_nodes");
    gauge
});

pub static CLUSTER_INDICES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_cluster_indices",
        "Number of indices in the cluster",
    )
    .expect("metric: cluster_indices");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: cluster_indices");
    gauge
});

pub static CLUSTER_SHARDS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new("ferrissearch_cluster_shards", "Number of shards by type"),
        &["type"], // "primary" or "replica"
    )
    .expect("metric: cluster_shards");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: cluster_shards");
    gauge
});

pub static INDEX_DOCS: LazyLock<GaugeVec> = LazyLock::new(|| {
    let gauge = GaugeVec::new(
        Opts::new("ferrissearch_index_docs", "Number of documents per index"),
        &["index"],
    )
    .expect("metric: index_docs");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: index_docs");
    gauge
});

pub static IS_MASTER: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_is_master",
        "Whether this node is the cluster master (1 = master, 0 = follower)",
    )
    .expect("metric: is_master");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: is_master");
    gauge
});

// ── Process metrics ─────────────────────────────────────────────────

pub static PROCESS_CPU_SECONDS: LazyLock<Gauge> = LazyLock::new(|| {
    let gauge = Gauge::new(
        "process_cpu_seconds_total",
        "Total user and system CPU time spent in seconds",
    )
    .expect("metric: process_cpu_seconds_total");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_cpu_seconds_total");
    gauge
});

pub static PROCESS_RESIDENT_MEMORY_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "process_resident_memory_bytes",
        "Resident memory size in bytes (RSS)",
    )
    .expect("metric: process_resident_memory_bytes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_resident_memory_bytes");
    gauge
});

pub static PROCESS_VIRTUAL_MEMORY_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "process_virtual_memory_bytes",
        "Virtual memory size in bytes",
    )
    .expect("metric: process_virtual_memory_bytes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_virtual_memory_bytes");
    gauge
});

pub static PROCESS_OPEN_FDS: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new("process_open_fds", "Number of open file descriptors")
        .expect("metric: process_open_fds");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_open_fds");
    gauge
});

pub static PROCESS_THREADS: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge =
        IntGauge::new("process_threads", "Number of OS threads").expect("metric: process_threads");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_threads");
    gauge
});

pub static PROCESS_START_TIME_SECONDS: LazyLock<Gauge> = LazyLock::new(|| {
    let gauge = Gauge::new(
        "process_start_time_seconds",
        "Start time of the process since unix epoch in seconds",
    )
    .expect("metric: process_start_time_seconds");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: process_start_time_seconds");
    gauge
});

// ── Snapshot helpers ────────────────────────────────────────────────

fn initialize_metrics() {
    let _ = &*HTTP_REQUESTS_TOTAL;
    let _ = &*HTTP_REQUEST_DURATION_SECONDS;
    let _ = &*SEARCH_QUERIES_TOTAL;
    let _ = &*SEARCH_LATENCY_SECONDS;
    let _ = &*DOCS_INDEXED_TOTAL;
    let _ = &*BULK_REQUESTS_TOTAL;
    let _ = &*BULK_DOCS_TOTAL;
    let _ = &*INDEX_LATENCY_SECONDS;
    let _ = &*SQL_QUERIES_TOTAL;
    let _ = &*SQL_LATENCY_SECONDS;
    let _ = &*CLUSTER_NODES;
    let _ = &*CLUSTER_INDICES;
    let _ = &*CLUSTER_SHARDS;
    let _ = &*INDEX_DOCS;
    let _ = &*IS_MASTER;
    let _ = &*PROCESS_CPU_SECONDS;
    let _ = &*PROCESS_RESIDENT_MEMORY_BYTES;
    let _ = &*PROCESS_VIRTUAL_MEMORY_BYTES;
    let _ = &*PROCESS_OPEN_FDS;
    let _ = &*PROCESS_THREADS;
    let _ = &*PROCESS_START_TIME_SECONDS;
}

/// Update process-level metrics by reading from /proc/self.
fn update_process_metrics() {
    // CPU time from /proc/self/stat
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        let fields: Vec<&str> = stat.split_whitespace().collect();
        if fields.len() > 22 {
            let ticks_per_sec = 100.0_f64; // sysconf(_SC_CLK_TCK) on Linux
            let utime: f64 = fields[13].parse().unwrap_or(0.0);
            let stime: f64 = fields[14].parse().unwrap_or(0.0);
            PROCESS_CPU_SECONDS.set((utime + stime) / ticks_per_sec);

            // Thread count — field 19
            if let Ok(threads) = fields[19].parse::<i64>() {
                PROCESS_THREADS.set(threads);
            }

            // Start time — field 21 (in clock ticks since boot)
            if let Ok(starttime) = fields[21].parse::<f64>()
                && let Ok(uptime_str) = std::fs::read_to_string("/proc/uptime")
                && let Some(uptime) = uptime_str.split_whitespace().next()
                && let Ok(uptime_secs) = uptime.parse::<f64>()
            {
                let boot_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
                    - uptime_secs;
                PROCESS_START_TIME_SECONDS.set(boot_time + starttime / ticks_per_sec);
            }
        }
    }

    // Memory from /proc/self/status
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(val) = line.strip_prefix("VmRSS:")
                && let Ok(kb) = val.trim().trim_end_matches(" kB").trim().parse::<i64>()
            {
                PROCESS_RESIDENT_MEMORY_BYTES.set(kb * 1024);
            } else if let Some(val) = line.strip_prefix("VmSize:")
                && let Ok(kb) = val.trim().trim_end_matches(" kB").trim().parse::<i64>()
            {
                PROCESS_VIRTUAL_MEMORY_BYTES.set(kb * 1024);
            }
        }
    }

    // Open file descriptors from /proc/self/fd
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        PROCESS_OPEN_FDS.set(entries.count() as i64);
    }
}

/// Update cluster-level gauge metrics from current state.
pub fn update_cluster_metrics(state: &crate::api::AppState) {
    let cs = state.cluster_manager.get_state();
    CLUSTER_NODES.set(cs.nodes.len() as i64);
    CLUSTER_INDICES.set(cs.indices.len() as i64);
    INDEX_DOCS.reset();

    let mut primary_count = 0i64;
    let mut replica_count = 0i64;
    for metadata in cs.indices.values() {
        primary_count += metadata.shard_routing.len() as i64;
        for routing in metadata.shard_routing.values() {
            replica_count += routing.replicas.len() as i64;
        }

        // Per-index doc count from local shards
        let mut total_docs = 0u64;
        for shard_id in metadata.shard_routing.keys() {
            if let Some(engine) = state.shard_manager.get_shard(&metadata.name, *shard_id) {
                total_docs += engine.doc_count();
            }
        }
        INDEX_DOCS
            .with_label_values(&[&metadata.name])
            .set(total_docs as f64);
    }
    CLUSTER_SHARDS
        .with_label_values(&["primary"])
        .set(primary_count);
    CLUSTER_SHARDS
        .with_label_values(&["replica"])
        .set(replica_count);

    // Master status
    let is_master = state.raft.as_ref().is_some_and(|r| r.is_leader());
    IS_MASTER.set(if is_master { 1 } else { 0 });
}

/// Render all registered metrics in Prometheus text exposition format.
pub fn gather() -> String {
    initialize_metrics();
    update_process_metrics();

    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_MUTEX: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

    #[test]
    fn gather_produces_valid_prometheus_output() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        // Touch all metrics to ensure they're registered
        HTTP_REQUESTS_TOTAL
            .with_label_values(&["GET", "/test", "200"])
            .inc();
        HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["GET", "/test"])
            .observe(0.042);
        SEARCH_QUERIES_TOTAL.inc();
        SEARCH_LATENCY_SECONDS.observe(0.005);
        DOCS_INDEXED_TOTAL.inc();
        BULK_REQUESTS_TOTAL.inc();
        BULK_DOCS_TOTAL.inc_by(100);
        INDEX_LATENCY_SECONDS.observe(0.001);
        SQL_QUERIES_TOTAL
            .with_label_values(&["tantivy_grouped_partials"])
            .inc();
        SQL_LATENCY_SECONDS.observe(0.06);
        CLUSTER_NODES.set(3);
        CLUSTER_INDICES.set(2);
        CLUSTER_SHARDS.with_label_values(&["primary"]).set(6);
        CLUSTER_SHARDS.with_label_values(&["replica"]).set(6);
        INDEX_DOCS.with_label_values(&["test-index"]).set(1000.0);
        IS_MASTER.set(1);

        let output = gather();
        assert!(output.contains("ferrissearch_http_requests_total"));
        assert!(output.contains("ferrissearch_http_request_duration_seconds"));
        assert!(output.contains("ferrissearch_search_queries_total"));
        assert!(output.contains("ferrissearch_search_latency_seconds"));
        assert!(output.contains("ferrissearch_docs_indexed_total"));
        assert!(output.contains("ferrissearch_bulk_requests_total"));
        assert!(output.contains("ferrissearch_bulk_docs_total"));
        assert!(output.contains("ferrissearch_index_latency_seconds"));
        assert!(output.contains("ferrissearch_sql_queries_total"));
        assert!(output.contains("ferrissearch_sql_latency_seconds"));
        assert!(output.contains("ferrissearch_cluster_nodes"));
        assert!(output.contains("ferrissearch_cluster_indices"));
        assert!(output.contains("ferrissearch_cluster_shards"));
        assert!(output.contains("ferrissearch_index_docs"));
        assert!(output.contains("ferrissearch_is_master"));
        assert!(output.contains("process_cpu_seconds_total"));
        assert!(output.contains("process_resident_memory_bytes"));
        assert!(output.contains("process_open_fds"));
        assert!(output.contains("process_threads"));
    }

    #[test]
    fn histogram_buckets_are_configured() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["POST", "/_bulk"])
            .observe(0.042);
        let output = gather();
        // Check that custom buckets are present (not just default 0.005..10)
        assert!(output.contains("le=\"0.0005\""));
        assert!(output.contains("le=\"0.025\""));
    }

    #[test]
    fn sql_mode_labels_work() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        SQL_QUERIES_TOTAL
            .with_label_values(&["count_star_fast"])
            .inc();
        SQL_QUERIES_TOTAL
            .with_label_values(&["tantivy_fast_fields"])
            .inc();
        SQL_QUERIES_TOTAL
            .with_label_values(&["tantivy_grouped_partials"])
            .inc_by(5);
        SQL_QUERIES_TOTAL
            .with_label_values(&["materialized_hits_fallback"])
            .inc();
        let output = gather();
        assert!(output.contains("mode=\"count_star_fast\""));
        assert!(output.contains("mode=\"tantivy_grouped_partials\""));
    }

    #[test]
    fn process_metrics_are_populated() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        update_process_metrics();
        // On Linux /proc/self exists, so RSS should be > 0
        if std::path::Path::new("/proc/self/status").exists() {
            assert!(PROCESS_RESIDENT_MEMORY_BYTES.get() > 0);
            assert!(PROCESS_OPEN_FDS.get() > 0);
        }
    }

    #[test]
    fn gather_initializes_metrics_without_traffic() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        let output = gather();
        assert!(output.contains("ferrissearch_search_latency_seconds"));
        assert!(output.contains("ferrissearch_bulk_requests_total"));
        assert!(output.contains("ferrissearch_index_latency_seconds"));
    }

    #[test]
    fn index_docs_reset_removes_stale_series() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        INDEX_DOCS
            .with_label_values(&["stale-index-docs-test"])
            .set(1.0);
        INDEX_DOCS.reset();
        INDEX_DOCS
            .with_label_values(&["live-index-docs-test"])
            .set(2.0);

        let output = gather();
        assert!(!output.contains("stale-index-docs-test"));
        assert!(output.contains("live-index-docs-test"));
    }
}
