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

pub(crate) static BULK_ACTIVE_REQUESTS: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_bulk_active_requests",
        "Bulk request leases retained by intake, execution, response, or cleanup",
    )
    .expect("metric: bulk_active_requests");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: bulk_active_requests");
    gauge
});

pub(crate) static BULK_SPOOL_RESERVED_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_bulk_spool_reserved_bytes",
        "Input/result spool bytes reserved until their anonymous files are closed",
    )
    .expect("metric: bulk_spool_reserved_bytes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: bulk_spool_reserved_bytes");
    gauge
});

pub(crate) static BULK_ADMISSION_REJECTIONS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_admission_rejections_total",
        "Bulk requests rejected before body polling because admission is exhausted",
    )
    .expect("metric: bulk_admission_rejections_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_admission_rejections_total");
    counter
});

pub(crate) static BULK_STAGED_INPUT_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_staged_input_bytes_total",
        "HTTP bulk input bytes staged before execution",
    )
    .expect("metric: bulk_staged_input_bytes_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_staged_input_bytes_total");
    counter
});

pub(crate) static BULK_EXECUTION_BATCHES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_execution_batches_total",
        "Bounded coordinator bulk batches submitted for item execution",
    )
    .expect("metric: bulk_execution_batches_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_execution_batches_total");
    counter
});

pub(crate) static BULK_CANCELLED_REQUESTS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_bulk_cancelled_requests_total",
        "Bulk leases released without a completed response or explicit API failure",
    )
    .expect("metric: bulk_cancelled_requests_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: bulk_cancelled_requests_total");
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

pub(crate) static WRITE_CERTIFICATIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_write_certifications_total",
            "Data-copy certification outcomes before optional visibility and final reply checks",
        ),
        &["outcome"],
    )
    .expect("metric: write_certifications_total");
    for outcome in ["all_copies", "after_exclusion", "failed"] {
        let _ = counter.with_label_values(&[outcome]);
    }
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: write_certifications_total");
    counter
});

pub(crate) static WRITE_EXCLUSION_SETTLEMENTS_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "ferrissearch_write_exclusion_settlements_total",
                "Failed-copy exclusion settlement attempts, including reconciliation",
            ),
            &["outcome"],
        )
        .expect("metric: write_exclusion_settlements_total");
        for outcome in ["settled", "failed"] {
            let _ = counter.with_label_values(&[outcome]);
        }
        REGISTRY
            .register(Box::new(counter.clone()))
            .expect("register: write_exclusion_settlements_total");
        counter
    });

pub(crate) static CANONICAL_ACTIVATIONS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_canonical_activations_total",
        "Committed canonical primary activations whose local write epoch was installed",
    )
    .expect("metric: canonical_activations_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: canonical_activations_total");
    counter
});

static LOCAL_PRIMARY_WRITE_AUTHORITIES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_local_primary_write_authorities",
            "Locally assigned primary authorities by applied Raft phase, not current permit readiness",
        ),
        &["phase"],
    )
    .expect("metric: local_primary_write_authorities");
    for phase in ["inactive", "promoting", "active"] {
        let _ = gauge.with_label_values(&[phase]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: local_primary_write_authorities");
    gauge
});

static LOCAL_WRITE_FAIL_STOPPED_COPIES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_local_write_fail_stopped_copies",
        "Current local allocations with a tracked process-sticky write failure",
    )
    .expect("metric: local_write_fail_stopped_copies");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: local_write_fail_stopped_copies");
    gauge
});

static LOCAL_WRITE_EXCLUSION_JOURNAL_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_local_write_exclusion_journal_pending",
        "Unsettled durable exclusion intents in loaded current local copy identities",
    )
    .expect("metric: local_write_exclusion_journal_pending");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: local_write_exclusion_journal_pending");
    gauge
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

pub static COLUMN_CACHE_EFFECTIVE_MEMORY_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_column_cache_effective_memory_bytes",
        "Host memory after applicable hard cgroup caps used to size the column cache; 0 when the cache is disabled",
    )
    .expect("metric: column_cache_effective_memory_bytes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: column_cache_effective_memory_bytes");
    gauge
});

pub static COLUMN_CACHE_BUDGET_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "ferrissearch_column_cache_budget_bytes",
        "Configured maximum weighted capacity of the shared column cache in bytes",
    )
    .expect("metric: column_cache_budget_bytes");
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: column_cache_budget_bytes");
    gauge
});

pub static OBJECT_STORE_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_object_store_requests_total",
            "Object-store requests by operation and outcome",
        ),
        &["operation", "outcome"],
    )
    .expect("metric: object_store_requests_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: object_store_requests_total");
    counter
});

pub static OBJECT_STORE_TRANSFER_BYTES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_object_store_transfer_bytes_total",
            "Successfully transferred object-store payload bytes by direction",
        ),
        &["direction"],
    )
    .expect("metric: object_store_transfer_bytes_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: object_store_transfer_bytes_total");
    counter
});

pub static REMOTE_STORE_PUBLICATION_ATTEMPTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_remote_store_publication_attempts_total",
            "Remote-store manifest publication attempts by outcome",
        ),
        &["outcome"],
    )
    .expect("metric: remote_store_publication_attempts_total");
    for outcome in [
        "committed",
        "replayed",
        "superseded",
        "conflict",
        "indeterminate",
        "aborted",
    ] {
        let _ = counter.with_label_values(&[outcome]);
    }
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: remote_store_publication_attempts_total");
    counter
});

pub static REMOTE_STORE_POINTER_FENCES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_remote_store_pointer_fences_total",
            "Remote-store manifest pointer fence operations by outcome",
        ),
        &["outcome"],
    )
    .expect("metric: remote_store_pointer_fences_total");
    for outcome in ["fenced", "conflict", "failed"] {
        let _ = counter.with_label_values(&[outcome]);
    }
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: remote_store_pointer_fences_total");
    counter
});

pub static REMOTE_STORE_JANITOR_RUNS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_remote_store_janitor_runs_total",
            "Remote-store object janitor runs by mode and outcome",
        ),
        &["mode", "outcome"],
    )
    .expect("metric: remote_store_janitor_runs_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: remote_store_janitor_runs_total");
    counter
});

pub static REMOTE_STORE_JANITOR_CANDIDATES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_remote_store_janitor_candidates_total",
            "Remote-store janitor candidate evaluations by action",
        ),
        &["action"],
    )
    .expect("metric: remote_store_janitor_candidates_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: remote_store_janitor_candidates_total");
    counter
});

pub static REMOTE_STORE_JANITOR_DELETED_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_remote_store_janitor_deleted_bytes_total",
        "Total bytes reclaimed by the remote-store janitor in destructive mode",
    )
    .expect("metric: remote_store_janitor_deleted_bytes_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: remote_store_janitor_deleted_bytes_total");
    counter
});

pub static REPLICA_COPY_STATE_COPIES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_replica_copy_state_copies",
            "Replica copies across the cluster by explicit FS-025 lifecycle state",
        ),
        &["state"],
    )
    .expect("metric: replica_copy_state_copies");
    for state in [
        "unassigned",
        "initializing",
        "snapshotting",
        "catching_up",
        "in_sync",
        "stale",
        "failed",
    ] {
        let _ = gauge.with_label_values(&[state]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: replica_copy_state_copies");
    gauge
});

pub static REPLICA_STATE_TRANSITIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_replica_state_transitions_total",
            "Replica recovery and ISR state machine transitions by source and target state",
        ),
        &["from", "to"],
    )
    .expect("metric: replica_state_transitions_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: replica_state_transitions_total");
    counter
});

pub static PEER_RECOVERY_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_peer_recovery_bytes_total",
        "Total snapshot bytes transferred during peer recovery",
    )
    .expect("metric: peer_recovery_bytes_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: peer_recovery_bytes_total");
    counter
});

pub static PEER_RECOVERY_OPS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_peer_recovery_ops_total",
        "Total WAL suffix operations applied during peer recovery",
    )
    .expect("metric: peer_recovery_ops_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: peer_recovery_ops_total");
    counter
});

pub static PEER_RECOVERY_CANCELLATIONS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "ferrissearch_peer_recovery_cancellations_total",
        "Total peer recovery sessions cancelled on target or source",
    )
    .expect("metric: peer_recovery_cancellations_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: peer_recovery_cancellations_total");
    counter
});

pub static REQUEST_CANCELLATIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_request_cancellations_total",
            "Request-scoped cancellations observed by execution stage and reason",
        ),
        &["stage", "reason"],
    )
    .expect("metric: request_cancellations_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: request_cancellations_total");
    counter
});

pub static DEADLINE_EXHAUSTED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_deadline_exhausted_total",
            "Requests and child RPCs that exhausted their deadline budget by stage",
        ),
        &["stage"],
    )
    .expect("metric: deadline_exhausted_total");
    for stage in [
        "queue",
        "search",
        "sql",
        "write",
        "hydration",
        "replication",
        "recovery",
        "merge",
        "maintenance",
    ] {
        let _ = counter.with_label_values(&[stage]);
    }
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: deadline_exhausted_total");
    counter
});

pub static ADMISSION_QUEUED_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_admission_queued",
            "Currently queued requests waiting for admission by work class",
        ),
        &["work_class"],
    )
    .expect("metric: admission_queued");
    for class in [
        "search",
        "sql",
        "write",
        "hydration",
        "recovery",
        "maintenance",
    ] {
        let _ = gauge.with_label_values(&[class]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: admission_queued");
    gauge
});

pub static ADMISSION_EXECUTING_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_admission_executing",
            "Currently executing requests holding an admission lease by work class",
        ),
        &["work_class"],
    )
    .expect("metric: admission_executing");
    for class in [
        "search",
        "sql",
        "write",
        "hydration",
        "recovery",
        "maintenance",
    ] {
        let _ = gauge.with_label_values(&[class]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: admission_executing");
    gauge
});

pub static ADMISSION_ACQUIRED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_admission_acquired_total",
            "Total admission leases acquired by work class",
        ),
        &["work_class"],
    )
    .expect("metric: admission_acquired_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: admission_acquired_total");
    counter
});

pub static ADMISSION_REJECTED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_admission_rejected_total",
            "Total admission requests rejected by work class and reason",
        ),
        &["work_class", "reason"],
    )
    .expect("metric: admission_rejected_total");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: admission_rejected_total");
    counter
});

pub static ADMISSION_QUEUE_WAIT_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new(
            "ferrissearch_admission_queue_wait_seconds",
            "Time spent waiting in the bounded admission queue in seconds",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
        ]),
        &["work_class"],
    )
    .expect("metric: admission_queue_wait_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: admission_queue_wait_seconds");
    histogram
});

pub static ADMISSION_EXECUTION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new(
            "ferrissearch_admission_execution_seconds",
            "Time spent executing while holding an admission lease in seconds",
        )
        .buckets(vec![
            0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ]),
        &["work_class"],
    )
    .expect("metric: admission_execution_seconds");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("register: admission_execution_seconds");
    histogram
});

pub static RESOURCE_POOL_CAPACITY_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_resource_pool_capacity_bytes",
            "Configured capacity in bytes for each governed resource pool",
        ),
        &["pool"],
    )
    .expect("metric: resource_pool_capacity_bytes");
    for pool in [
        "memory",
        "temp_disk",
        "persistent_cache",
        "object_store_inflight",
    ] {
        let _ = gauge.with_label_values(&[pool]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: resource_pool_capacity_bytes");
    gauge
});

pub static RESOURCE_POOL_RESERVED_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "ferrissearch_resource_pool_reserved_bytes",
            "Currently reserved bytes in each governed resource pool",
        ),
        &["pool"],
    )
    .expect("metric: resource_pool_reserved_bytes");
    for pool in [
        "memory",
        "temp_disk",
        "persistent_cache",
        "object_store_inflight",
    ] {
        let _ = gauge.with_label_values(&[pool]);
    }
    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("register: resource_pool_reserved_bytes");
    gauge
});

pub static RESOURCE_RESERVATIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "ferrissearch_resource_reservations_total",
            "Resource byte reservation operations by pool and outcome",
        ),
        &["pool", "outcome"],
    )
    .expect("metric: resource_reservations_total");
    for pool in [
        "memory",
        "temp_disk",
        "persistent_cache",
        "object_store_inflight",
    ] {
        for outcome in ["granted", "rejected", "reconciled"] {
            let _ = counter.with_label_values(&[pool, outcome]);
        }
    }
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("register: resource_reservations_total");
    counter
});

fn set_column_cache_budget_gauges(
    effective_memory_gauge: &IntGauge,
    cache_budget_gauge: &IntGauge,
    effective_memory_bytes: Option<u64>,
    cache_budget_bytes: u64,
) {
    let as_gauge = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    effective_memory_gauge.set(as_gauge(effective_memory_bytes.unwrap_or(0)));
    cache_budget_gauge.set(as_gauge(cache_budget_bytes));
}

// ── Snapshot helpers ────────────────────────────────────────────────

fn initialize_metrics() {
    let _ = &*HTTP_REQUESTS_TOTAL;
    let _ = &*HTTP_REQUEST_DURATION_SECONDS;
    let _ = &*SEARCH_QUERIES_TOTAL;
    let _ = &*SEARCH_LATENCY_SECONDS;
    let _ = &*DOCS_INDEXED_TOTAL;
    let _ = &*BULK_REQUESTS_TOTAL;
    let _ = &*BULK_ACTIVE_REQUESTS;
    let _ = &*BULK_SPOOL_RESERVED_BYTES;
    let _ = &*BULK_ADMISSION_REJECTIONS_TOTAL;
    let _ = &*BULK_STAGED_INPUT_BYTES_TOTAL;
    let _ = &*BULK_EXECUTION_BATCHES_TOTAL;
    let _ = &*BULK_CANCELLED_REQUESTS_TOTAL;
    let _ = &*BULK_DOCS_TOTAL;
    let _ = &*INDEX_LATENCY_SECONDS;
    let _ = &*WRITE_CERTIFICATIONS_TOTAL;
    let _ = &*WRITE_EXCLUSION_SETTLEMENTS_TOTAL;
    let _ = &*CANONICAL_ACTIVATIONS_TOTAL;
    let _ = &*LOCAL_PRIMARY_WRITE_AUTHORITIES;
    let _ = &*LOCAL_WRITE_FAIL_STOPPED_COPIES;
    let _ = &*LOCAL_WRITE_EXCLUSION_JOURNAL_PENDING;
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
    let _ = &*COLUMN_CACHE_EFFECTIVE_MEMORY_BYTES;
    let _ = &*COLUMN_CACHE_BUDGET_BYTES;
    let _ = &*OBJECT_STORE_REQUESTS_TOTAL;
    let _ = &*OBJECT_STORE_TRANSFER_BYTES_TOTAL;
    let _ = &*REMOTE_STORE_PUBLICATION_ATTEMPTS_TOTAL;
    let _ = &*REMOTE_STORE_POINTER_FENCES_TOTAL;
    let _ = &*REMOTE_STORE_JANITOR_RUNS_TOTAL;
    let _ = &*REMOTE_STORE_JANITOR_CANDIDATES_TOTAL;
    let _ = &*REMOTE_STORE_JANITOR_DELETED_BYTES_TOTAL;
    let _ = &*REPLICA_COPY_STATE_COPIES;
    let _ = &*REPLICA_STATE_TRANSITIONS_TOTAL;
    let _ = &*PEER_RECOVERY_BYTES_TOTAL;
    let _ = &*PEER_RECOVERY_OPS_TOTAL;
    let _ = &*PEER_RECOVERY_CANCELLATIONS_TOTAL;
    let _ = &*REQUEST_CANCELLATIONS_TOTAL;
    let _ = &*DEADLINE_EXHAUSTED_TOTAL;
    let _ = &*ADMISSION_QUEUED_GAUGE;
    let _ = &*ADMISSION_EXECUTING_GAUGE;
    let _ = &*ADMISSION_ACQUIRED_TOTAL;
    let _ = &*ADMISSION_REJECTED_TOTAL;
    let _ = &*ADMISSION_QUEUE_WAIT_SECONDS;
    let _ = &*ADMISSION_EXECUTION_SECONDS;
    let _ = &*RESOURCE_POOL_CAPACITY_BYTES;
    let _ = &*RESOURCE_POOL_RESERVED_BYTES;
    let _ = &*RESOURCE_RESERVATIONS_TOTAL;
}

pub fn record_replica_state_transition(from: &str, to: &str) {
    REPLICA_STATE_TRANSITIONS_TOTAL
        .with_label_values(&[from, to])
        .inc();
}

pub fn record_peer_recovery_progress(bytes: u64, operations: u64) {
    PEER_RECOVERY_BYTES_TOTAL.inc_by(bytes);
    PEER_RECOVERY_OPS_TOTAL.inc_by(operations);
}

pub fn record_peer_recovery_cancellation() {
    PEER_RECOVERY_CANCELLATIONS_TOTAL.inc();
}

pub fn record_object_store_request(operation: &str, outcome: &str) {
    OBJECT_STORE_REQUESTS_TOTAL
        .with_label_values(&[operation, outcome])
        .inc();
}

pub fn record_object_store_transfer(direction: &str, bytes: usize) {
    OBJECT_STORE_TRANSFER_BYTES_TOTAL
        .with_label_values(&[direction])
        .inc_by(bytes as u64);
}

pub fn record_remote_store_publication_attempt(outcome: &str) {
    REMOTE_STORE_PUBLICATION_ATTEMPTS_TOTAL
        .with_label_values(&[outcome])
        .inc();
}

pub fn record_remote_store_pointer_fence(outcome: &str) {
    REMOTE_STORE_POINTER_FENCES_TOTAL
        .with_label_values(&[outcome])
        .inc();
}

pub fn record_remote_store_janitor_run(mode: &str, outcome: &str) {
    REMOTE_STORE_JANITOR_RUNS_TOTAL
        .with_label_values(&[mode, outcome])
        .inc();
}

pub fn record_remote_store_janitor_candidate(action: &str) {
    REMOTE_STORE_JANITOR_CANDIDATES_TOTAL
        .with_label_values(&[action])
        .inc();
}

pub fn record_remote_store_janitor_deleted_bytes(bytes: u64) {
    REMOTE_STORE_JANITOR_DELETED_BYTES_TOTAL.inc_by(bytes);
}

pub fn set_column_cache_budget_metrics(
    effective_memory_bytes: Option<u64>,
    cache_budget_bytes: u64,
) {
    set_column_cache_budget_gauges(
        &COLUMN_CACHE_EFFECTIVE_MEMORY_BYTES,
        &COLUMN_CACHE_BUDGET_BYTES,
        effective_memory_bytes,
        cache_budget_bytes,
    );
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
    let mut authority_counts = [0i64; 3];
    let mut replica_state_counts = [0i64; 7];
    let mut fail_stopped_copies = 0i64;
    let mut pending_exclusions = 0i64;
    for metadata in cs.indices.values() {
        primary_count += metadata.shard_routing.len() as i64;
        for routing in metadata.shard_routing.values() {
            replica_count += routing.replicas.len() as i64;
            replica_state_counts[0] += routing.unassigned_replicas as i64;
        }
        for (shard, routing) in &metadata.shard_routing {
            for replica_node in &routing.replicas {
                let copy_failed = replica_node == &state.local_node_id
                    && cs
                        .shard_allocation_id(&metadata.name, *shard, replica_node)
                        .is_some_and(|alloc| {
                            state.shard_manager.is_copy_write_fail_stopped(
                                metadata.uuid.as_str(),
                                *shard,
                                alloc,
                            )
                        });
                let copy_state = state.shard_manager.replica_states.evaluate_copy_state(
                    &cs,
                    &metadata.name,
                    *shard,
                    replica_node,
                    copy_failed,
                );
                let idx = match copy_state {
                    crate::shard::ReplicaCopyState::Unassigned => 0,
                    crate::shard::ReplicaCopyState::Initializing => 1,
                    crate::shard::ReplicaCopyState::Snapshotting => 2,
                    crate::shard::ReplicaCopyState::CatchingUp => 3,
                    crate::shard::ReplicaCopyState::InSync => 4,
                    crate::shard::ReplicaCopyState::Stale => 5,
                    crate::shard::ReplicaCopyState::Failed => 6,
                };
                replica_state_counts[idx] += 1;
            }
            let Some(allocation) =
                cs.shard_allocation_id(&metadata.name, *shard, &state.local_node_id)
            else {
                continue;
            };
            if routing.primary == state.local_node_id
                && let Some(ids) = cs.shard_allocation_ids(&metadata.name, *shard)
            {
                use crate::cluster::state::PrimaryAuthorityPhase;
                let phase = match ids.primary_authority.phase {
                    PrimaryAuthorityPhase::Inactive => 0,
                    PrimaryAuthorityPhase::Promoting => 1,
                    PrimaryAuthorityPhase::Active => 2,
                };
                authority_counts[phase] += 1;
            }
            if state.shard_manager.is_copy_write_fail_stopped(
                metadata.uuid.as_str(),
                *shard,
                allocation,
            ) {
                fail_stopped_copies += 1;
            }
            if let Some(identity) = state.shard_manager.copy_identity(&metadata.name, *shard)
                && identity.index_uuid == metadata.uuid.as_str()
                && identity.allocation_id == allocation
            {
                pending_exclusions = pending_exclusions.saturating_add(
                    i64::try_from(identity.pending_exclusions.len()).unwrap_or(i64::MAX),
                );
            }
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
    for (phase, count) in ["inactive", "promoting", "active"]
        .iter()
        .zip(authority_counts)
    {
        LOCAL_PRIMARY_WRITE_AUTHORITIES
            .with_label_values(&[phase])
            .set(count);
    }
    LOCAL_WRITE_FAIL_STOPPED_COPIES.set(fail_stopped_copies);
    LOCAL_WRITE_EXCLUSION_JOURNAL_PENDING.set(pending_exclusions);
    for (state_label, count) in [
        "unassigned",
        "initializing",
        "snapshotting",
        "catching_up",
        "in_sync",
        "stale",
        "failed",
    ]
    .iter()
    .zip(replica_state_counts)
    {
        REPLICA_COPY_STATE_COPIES
            .with_label_values(&[state_label])
            .set(count);
    }

    // Master status
    let is_master = state.raft.is_leader();
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
        record_object_store_request("get", "success");
        record_object_store_transfer("download", 1024);

        let output = gather();
        assert!(output.contains("ferrissearch_http_requests_total"));
        assert!(output.contains("ferrissearch_http_request_duration_seconds"));
        assert!(output.contains("ferrissearch_search_queries_total"));
        assert!(output.contains("ferrissearch_search_latency_seconds"));
        assert!(output.contains("ferrissearch_docs_indexed_total"));
        assert!(output.contains("ferrissearch_bulk_requests_total"));
        assert!(output.contains("ferrissearch_bulk_docs_total"));
        assert!(output.contains("ferrissearch_index_latency_seconds"));
        assert!(output.contains("ferrissearch_write_certifications_total"));
        assert!(output.contains("ferrissearch_write_exclusion_settlements_total"));
        assert!(output.contains("ferrissearch_canonical_activations_total"));
        assert!(output.contains("ferrissearch_local_primary_write_authorities"));
        assert!(output.contains("ferrissearch_local_write_fail_stopped_copies"));
        assert!(output.contains("ferrissearch_local_write_exclusion_journal_pending"));
        assert!(output.contains("ferrissearch_sql_queries_total"));
        assert!(output.contains("ferrissearch_sql_latency_seconds"));
        assert!(output.contains("ferrissearch_cluster_nodes"));
        assert!(output.contains("ferrissearch_cluster_indices"));
        assert!(output.contains("ferrissearch_cluster_shards"));
        assert!(output.contains("ferrissearch_index_docs"));
        assert!(output.contains("ferrissearch_is_master"));
        assert!(output.contains("ferrissearch_object_store_requests_total"));
        assert!(output.contains("ferrissearch_object_store_transfer_bytes_total"));
        assert!(output.contains("process_cpu_seconds_total"));
        assert!(output.contains("process_resident_memory_bytes"));
        assert!(output.contains("process_open_fds"));
        assert!(output.contains("process_threads"));
        assert!(
            output
                .lines()
                .any(|line| line.starts_with("ferrissearch_column_cache_effective_memory_bytes "))
        );
        assert!(
            output
                .lines()
                .any(|line| line.starts_with("ferrissearch_column_cache_budget_bytes "))
        );
    }

    #[test]
    fn write_safety_metrics_have_only_bounded_labels() {
        initialize_metrics();
        for family in REGISTRY.gather() {
            let allowed: &[&str] = match family.name() {
                "ferrissearch_write_certifications_total" => {
                    &["all_copies", "after_exclusion", "failed"]
                }
                "ferrissearch_write_exclusion_settlements_total" => &["settled", "failed"],
                "ferrissearch_local_primary_write_authorities" => {
                    &["inactive", "promoting", "active"]
                }
                _ => continue,
            };
            assert_eq!(family.get_metric().len(), allowed.len());
            for metric in family.get_metric() {
                assert_eq!(metric.get_label().len(), 1);
                let label = &metric.get_label()[0];
                assert!(matches!(label.name(), "outcome" | "phase"));
                assert!(allowed.contains(&label.value()));
            }
        }
    }

    #[test]
    fn column_cache_budget_metrics_encode_values_in_isolated_registry() {
        let registry = Registry::new();
        let effective_memory_gauge = IntGauge::new(
            "ferrissearch_column_cache_effective_memory_bytes",
            "Host memory after applicable hard cgroup caps used to size the column cache; 0 when the cache is disabled",
        )
        .unwrap();
        let cache_budget_gauge = IntGauge::new(
            "ferrissearch_column_cache_budget_bytes",
            "Configured maximum weighted capacity of the shared column cache in bytes",
        )
        .unwrap();
        registry
            .register(Box::new(effective_memory_gauge.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_budget_gauge.clone()))
            .unwrap();

        let encode = || {
            let mut buffer = Vec::new();
            TextEncoder::new()
                .encode(&registry.gather(), &mut buffer)
                .unwrap();
            String::from_utf8(buffer).unwrap()
        };
        let has_sample = |output: &str, expected: &str| output.lines().any(|line| line == expected);

        set_column_cache_budget_gauges(
            &effective_memory_gauge,
            &cache_budget_gauge,
            Some(2 * 1024 * 1024 * 1024),
            214_748_364,
        );
        let enabled_output = encode();
        assert!(has_sample(
            &enabled_output,
            "ferrissearch_column_cache_effective_memory_bytes 2147483648"
        ));
        assert!(has_sample(
            &enabled_output,
            "ferrissearch_column_cache_budget_bytes 214748364"
        ));

        set_column_cache_budget_gauges(&effective_memory_gauge, &cache_budget_gauge, None, 0);
        let disabled_output = encode();
        assert!(has_sample(
            &disabled_output,
            "ferrissearch_column_cache_effective_memory_bytes 0"
        ));
        assert!(has_sample(
            &disabled_output,
            "ferrissearch_column_cache_budget_bytes 0"
        ));
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
