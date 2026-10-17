//! Isolated thread pools for search and write workloads.
//!
//! Mirrors OpenSearch/Quickwit thread pool architecture: search traffic and
//! write traffic run on **dedicated OS-thread pools** so heavy bulk indexing
//! cannot starve search latency and vice versa.
//!
//! Uses dedicated `rayon::ThreadPool` instances (same approach as Quickwit,
//! Meilisearch, DataFusion). Each pool owns a fixed set of OS threads —
//! true physical isolation, not just semaphore-bounded concurrency on a
//! shared pool.

use std::any::Any;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionStage {
    Queue,
    Search,
    Sql,
    Write,
    Hydration,
    Replication,
    Recovery,
    Merge,
    Maintenance,
}

impl ExecutionStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Search => "search",
            Self::Sql => "sql",
            Self::Write => "write",
            Self::Hydration => "hydration",
            Self::Replication => "replication",
            Self::Recovery => "recovery",
            Self::Merge => "merge",
            Self::Maintenance => "maintenance",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "queue" => Some(Self::Queue),
            "search" => Some(Self::Search),
            "sql" => Some(Self::Sql),
            "write" => Some(Self::Write),
            "hydration" => Some(Self::Hydration),
            "replication" => Some(Self::Replication),
            "recovery" => Some(Self::Recovery),
            "merge" => Some(Self::Merge),
            "maintenance" => Some(Self::Maintenance),
            _ => None,
        }
    }
}

impl std::fmt::Display for ExecutionStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationReason {
    ClientDisconnected,
    DeadlineExpired(ExecutionStage),
    ParentCancelled,
    Explicit(String),
}

impl CancellationReason {
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::ClientDisconnected => "client_disconnected",
            Self::DeadlineExpired(_) => "deadline_expired",
            Self::ParentCancelled => "parent_cancelled",
            Self::Explicit(_) => "explicit",
        }
    }
}

impl std::fmt::Display for CancellationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientDisconnected => f.write_str("client disconnected"),
            Self::DeadlineExpired(stage) => write!(f, "deadline expired in stage [{stage}]"),
            Self::ParentCancelled => f.write_str("parent request cancelled"),
            Self::Explicit(message) => f.write_str(message),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("request cancelled during stage [{stage}]: {reason}")]
pub struct RequestCancelledError {
    pub stage: ExecutionStage,
    pub reason: CancellationReason,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("request deadline budget ({budget_ms} ms) exhausted during stage [{stage}]")]
pub struct RequestDeadlineExceeded {
    pub stage: ExecutionStage,
    pub budget_ms: u64,
}

pub fn is_cancellation_or_deadline_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<RequestCancelledError>().is_some()
        || error.downcast_ref::<RequestDeadlineExceeded>().is_some()
}

pub fn classify_deadline_or_cancellation(error: &anyhow::Error) -> Option<(ExecutionStage, bool)> {
    if let Some(deadline) = error.downcast_ref::<RequestDeadlineExceeded>() {
        return Some((deadline.stage, true));
    }
    if let Some(cancelled) = error.downcast_ref::<RequestCancelledError>() {
        if let CancellationReason::DeadlineExpired(stage) = cancelled.reason {
            return Some((stage, true));
        }
        return Some((cancelled.stage, false));
    }
    None
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Monotonic request deadline with cross-node remaining-budget propagation (`FS-029`).
///
/// Clock/skew contract:
/// - Each node enforces deadlines using its own monotonic `Instant` (`expires_at`),
///   initialized from the remaining budget (`x-ferris-remaining-budget-ms` /
///   `grpc-timeout`).
/// - `deadline_epoch_ms` is carried alongside the relative remaining budget for
///   cross-node telemetry and trace correlation, but relative remaining budget is
///   authoritative so inter-node wall-clock skew cannot extend a caller's budget
///   or prematurely expire a valid request.
#[derive(Debug, Clone)]
pub struct RequestDeadline {
    created_at: Instant,
    expires_at: Instant,
    initial_budget_ms: u64,
    deadline_epoch_ms: u64,
    charged_queue_nanos: Arc<AtomicU64>,
    exhausted_stage: Arc<Mutex<Option<ExecutionStage>>>,
}

impl RequestDeadline {
    pub fn with_budget(budget: Duration) -> Self {
        let now = Instant::now();
        let expires_at = now.checked_add(budget).unwrap_or(now);
        let budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
        let deadline_epoch_ms = now_epoch_ms().saturating_add(budget_ms);
        Self {
            created_at: now,
            expires_at,
            initial_budget_ms: budget_ms,
            deadline_epoch_ms,
            charged_queue_nanos: Arc::new(AtomicU64::new(0)),
            exhausted_stage: Arc::new(Mutex::new(None)),
        }
    }

    pub fn from_remaining_and_epoch(remaining: Duration, deadline_epoch_ms: Option<u64>) -> Self {
        let mut deadline = Self::with_budget(remaining);
        if let Some(epoch_ms) = deadline_epoch_ms {
            deadline.deadline_epoch_ms = epoch_ms;
        }
        deadline
    }

    pub fn child_capped(&self, cap: Duration) -> Self {
        let remaining = self.remaining().min(cap);
        let budget_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
        Self {
            created_at: Instant::now(),
            expires_at: Instant::now()
                .checked_add(remaining)
                .unwrap_or_else(Instant::now),
            initial_budget_ms: budget_ms,
            deadline_epoch_ms: self
                .deadline_epoch_ms
                .min(now_epoch_ms().saturating_add(budget_ms)),
            charged_queue_nanos: Arc::new(AtomicU64::new(0)),
            exhausted_stage: self.exhausted_stage.clone(),
        }
    }

    pub fn remaining(&self) -> Duration {
        self.expires_at.saturating_duration_since(Instant::now())
    }

    pub fn remaining_ms(&self) -> u64 {
        u64::try_from(self.remaining().as_millis()).unwrap_or(u64::MAX)
    }

    pub fn initial_budget_ms(&self) -> u64 {
        self.initial_budget_ms
    }

    pub fn deadline_epoch_ms(&self) -> u64 {
        self.deadline_epoch_ms
    }

    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn charge_queue_time(&self, queue_wait: Duration) {
        let nanos = u64::try_from(queue_wait.as_nanos()).unwrap_or(u64::MAX);
        self.charged_queue_nanos.fetch_add(nanos, Ordering::AcqRel);
    }

    pub fn charged_queue_time(&self) -> Duration {
        Duration::from_nanos(self.charged_queue_nanos.load(Ordering::Acquire))
    }

    pub fn record_exhausted_stage(&self, stage: ExecutionStage) -> ExecutionStage {
        let mut guard = self
            .exhausted_stage
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = *guard {
            return existing;
        }
        *guard = Some(stage);
        crate::metrics::DEADLINE_EXHAUSTED_TOTAL
            .with_label_values(&[stage.as_str()])
            .inc();
        stage
    }

    pub fn exhausted_stage(&self) -> Option<ExecutionStage> {
        *self
            .exhausted_stage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

// ── End-To-End Distributed Tracing & Stable Error Classes (FS-033) ──────────

pub const REQUEST_ID_HEADER: &str = "x-ferris-request-id";
pub const OPERATION_ID_HEADER: &str = "x-ferris-operation-id";
pub const SNAPSHOT_ID_HEADER: &str = "x-ferris-snapshot-id";
pub const ASSIGNMENT_ID_HEADER: &str = "x-ferris-assignment-id";
pub const TASK_ID_HEADER: &str = "x-ferris-task-id";
pub const STAGE_TIMINGS_HEADER: &str = "x-ferris-stage-timings";
pub const ERROR_CLASS_HEADER: &str = "x-ferris-error-class";
pub const MAX_TRACE_ID_LEN: usize = 128;

/// Sanitize a correlation/trace identifier so secrets, credentials, control
/// characters, or unbounded strings can never enter spans or headers (`FS-033`).
pub fn sanitize_trace_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_TRACE_ID_LEN {
        return None;
    }
    if trimmed
        .chars()
        .any(|ch| ch.is_control() || matches!(ch, '\n' | '\r' | '\t' | ',' | ';'))
    {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    for forbidden in [
        "bearer ",
        "apikey ",
        "basic ",
        "authorization",
        "password",
        "secret",
        "private_key",
        "aws_secret_access_key",
    ] {
        if lower.contains(forbidden) {
            return None;
        }
    }
    Some(trimmed.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestTraceIds {
    pub request_id: String,
    pub operation_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub assignment_id: Option<String>,
    pub task_id: Option<String>,
}

impl RequestTraceIds {
    pub fn new_for_context_id(context_id: u64) -> Self {
        Self {
            request_id: format!("req-{context_id}"),
            operation_id: None,
            snapshot_id: None,
            assignment_id: None,
            task_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceStage {
    Queue,
    Search,
    Sql,
    Write,
    Hydration,
    Retry,
    PartialMerge,
    Storage,
    Replication,
    Recovery,
    Maintenance,
}

impl TraceStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Search => "search",
            Self::Sql => "sql",
            Self::Write => "write",
            Self::Hydration => "hydration",
            Self::Retry => "retry",
            Self::PartialMerge => "partial_merge",
            Self::Storage => "storage",
            Self::Replication => "replication",
            Self::Recovery => "recovery",
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StageTimingSnapshot {
    pub queue_ns: u64,
    pub search_ns: u64,
    pub sql_ns: u64,
    pub write_ns: u64,
    pub hydration_ns: u64,
    pub retry_ns: u64,
    pub partial_merge_ns: u64,
    pub storage_ns: u64,
    pub replication_ns: u64,
    pub recovery_ns: u64,
    pub maintenance_ns: u64,
}

impl StageTimingSnapshot {
    pub fn encode_compact(&self) -> String {
        format!(
            "queue_ns={},search_ns={},sql_ns={},write_ns={},hydration_ns={},retry_ns={},partial_merge_ns={},storage_ns={},replication_ns={},recovery_ns={},maintenance_ns={}",
            self.queue_ns,
            self.search_ns,
            self.sql_ns,
            self.write_ns,
            self.hydration_ns,
            self.retry_ns,
            self.partial_merge_ns,
            self.storage_ns,
            self.replication_ns,
            self.recovery_ns,
            self.maintenance_ns
        )
    }

    pub fn decode_compact(raw: &str) -> Option<Self> {
        let mut snap = Self::default();
        let mut matched_any = false;
        for part in raw.split(',') {
            let (k, v) = part.trim().split_once('=')?;
            let val = v.trim().parse::<u64>().ok()?;
            matched_any = true;
            match k.trim() {
                "queue_ns" => snap.queue_ns = val,
                "search_ns" => snap.search_ns = val,
                "sql_ns" => snap.sql_ns = val,
                "write_ns" => snap.write_ns = val,
                "hydration_ns" => snap.hydration_ns = val,
                "retry_ns" => snap.retry_ns = val,
                "partial_merge_ns" => snap.partial_merge_ns = val,
                "storage_ns" => snap.storage_ns = val,
                "replication_ns" => snap.replication_ns = val,
                "recovery_ns" => snap.recovery_ns = val,
                "maintenance_ns" => snap.maintenance_ns = val,
                _ => {}
            }
        }
        matched_any.then_some(snap)
    }
}

#[derive(Debug, Default)]
pub struct StageTimingLedger {
    queue_ns: AtomicU64,
    search_ns: AtomicU64,
    sql_ns: AtomicU64,
    write_ns: AtomicU64,
    hydration_ns: AtomicU64,
    retry_ns: AtomicU64,
    partial_merge_ns: AtomicU64,
    storage_ns: AtomicU64,
    replication_ns: AtomicU64,
    recovery_ns: AtomicU64,
    maintenance_ns: AtomicU64,
}

impl StageTimingLedger {
    fn slot(&self, stage: TraceStage) -> &AtomicU64 {
        match stage {
            TraceStage::Queue => &self.queue_ns,
            TraceStage::Search => &self.search_ns,
            TraceStage::Sql => &self.sql_ns,
            TraceStage::Write => &self.write_ns,
            TraceStage::Hydration => &self.hydration_ns,
            TraceStage::Retry => &self.retry_ns,
            TraceStage::PartialMerge => &self.partial_merge_ns,
            TraceStage::Storage => &self.storage_ns,
            TraceStage::Replication => &self.replication_ns,
            TraceStage::Recovery => &self.recovery_ns,
            TraceStage::Maintenance => &self.maintenance_ns,
        }
    }

    pub fn record_stage(&self, stage: TraceStage, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        self.slot(stage).fetch_add(nanos, Ordering::AcqRel);
        crate::metrics::STAGE_EXECUTION_DURATION_SECONDS
            .with_label_values(&[stage.as_str()])
            .observe(duration.as_secs_f64());
    }

    pub fn merge_snapshot(&self, other: &StageTimingSnapshot) {
        if other.queue_ns > 0 {
            self.queue_ns.fetch_add(other.queue_ns, Ordering::AcqRel);
        }
        if other.search_ns > 0 {
            self.search_ns.fetch_add(other.search_ns, Ordering::AcqRel);
        }
        if other.sql_ns > 0 {
            self.sql_ns.fetch_add(other.sql_ns, Ordering::AcqRel);
        }
        if other.write_ns > 0 {
            self.write_ns.fetch_add(other.write_ns, Ordering::AcqRel);
        }
        if other.hydration_ns > 0 {
            self.hydration_ns
                .fetch_add(other.hydration_ns, Ordering::AcqRel);
        }
        if other.retry_ns > 0 {
            self.retry_ns.fetch_add(other.retry_ns, Ordering::AcqRel);
        }
        if other.partial_merge_ns > 0 {
            self.partial_merge_ns
                .fetch_add(other.partial_merge_ns, Ordering::AcqRel);
        }
        if other.storage_ns > 0 {
            self.storage_ns
                .fetch_add(other.storage_ns, Ordering::AcqRel);
        }
        if other.replication_ns > 0 {
            self.replication_ns
                .fetch_add(other.replication_ns, Ordering::AcqRel);
        }
        if other.recovery_ns > 0 {
            self.recovery_ns
                .fetch_add(other.recovery_ns, Ordering::AcqRel);
        }
        if other.maintenance_ns > 0 {
            self.maintenance_ns
                .fetch_add(other.maintenance_ns, Ordering::AcqRel);
        }
    }

    pub fn snapshot(&self) -> StageTimingSnapshot {
        StageTimingSnapshot {
            queue_ns: self.queue_ns.load(Ordering::Acquire),
            search_ns: self.search_ns.load(Ordering::Acquire),
            sql_ns: self.sql_ns.load(Ordering::Acquire),
            write_ns: self.write_ns.load(Ordering::Acquire),
            hydration_ns: self.hydration_ns.load(Ordering::Acquire),
            retry_ns: self.retry_ns.load(Ordering::Acquire),
            partial_merge_ns: self.partial_merge_ns.load(Ordering::Acquire),
            storage_ns: self.storage_ns.load(Ordering::Acquire),
            replication_ns: self.replication_ns.load(Ordering::Acquire),
            recovery_ns: self.recovery_ns.load(Ordering::Acquire),
            maintenance_ns: self.maintenance_ns.load(Ordering::Acquire),
        }
    }
}

pub struct StageTimerGuard {
    ledger: Option<Arc<StageTimingLedger>>,
    stage: TraceStage,
    started_at: Instant,
}

impl Drop for StageTimerGuard {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed();
        if let Some(ref ledger) = self.ledger {
            ledger.record_stage(self.stage, elapsed);
        } else {
            crate::metrics::STAGE_EXECUTION_DURATION_SECONDS
                .with_label_values(&[self.stage.as_str()])
                .observe(elapsed.as_secs_f64());
        }
    }
}

/// Stable, low-cardinality error classification across HTTP, gRPC, workers, and storage (`FS-033`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StableErrorClass {
    Retryable,
    Terminal,
    Corruption,
    Overload,
}

impl StableErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Terminal => "terminal",
            Self::Corruption => "corruption",
            Self::Overload => "overload",
        }
    }

    pub fn from_label(raw: &str) -> Option<Self> {
        match raw.trim() {
            "retryable" => Some(Self::Retryable),
            "terminal" => Some(Self::Terminal),
            "corruption" => Some(Self::Corruption),
            "overload" => Some(Self::Overload),
            _ => None,
        }
    }
}

impl std::fmt::Display for StableErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn classify_tonic_status(status: &tonic::Status) -> StableErrorClass {
    let msg = status.message();
    if msg.contains("checksum mismatch")
        || msg.contains("corrupt")
        || msg.contains("fail-stopped")
        || msg.contains("unsupported")
        || msg.contains("collision")
        || msg.contains("invalid compare token")
    {
        return StableErrorClass::Corruption;
    }
    match status.code() {
        tonic::Code::ResourceExhausted | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
            StableErrorClass::Overload
        }
        tonic::Code::Unavailable | tonic::Code::Aborted => StableErrorClass::Retryable,
        tonic::Code::DataLoss => StableErrorClass::Corruption,
        tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::AlreadyExists
        | tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::Unimplemented
        | tonic::Code::OutOfRange => StableErrorClass::Terminal,
        _ => {
            if crate::transport::state_wait::is_state_wait_timeout(status) {
                StableErrorClass::Retryable
            } else {
                StableErrorClass::Terminal
            }
        }
    }
}

pub fn classify_anyhow_error(error: &anyhow::Error) -> StableErrorClass {
    if error.is::<RequestDeadlineExceeded>()
        || error.is::<RequestCancelledError>()
        || error.is::<ResourceExhaustedError>()
        || error.is::<AdmissionError>()
    {
        return StableErrorClass::Overload;
    }
    if error.is::<crate::wal::WalCorruptionError>()
        || error.is::<crate::wal::WalFailStoppedError>()
        || error.is::<crate::common::UnsupportedIndexFormatError>()
    {
        return StableErrorClass::Corruption;
    }
    if let Some(cond) = error.downcast_ref::<crate::storage::ConditionalStorageError>() {
        return match cond {
            crate::storage::ConditionalStorageError::InvalidToken { .. }
            | crate::storage::ConditionalStorageError::CorruptState { .. } => {
                StableErrorClass::Corruption
            }
            crate::storage::ConditionalStorageError::Conflict { .. }
            | crate::storage::ConditionalStorageError::Indeterminate { .. } => {
                StableErrorClass::Retryable
            }
            crate::storage::ConditionalStorageError::ImmutableConflict { .. }
            | crate::storage::ConditionalStorageError::UnsupportedBackend { .. } => {
                StableErrorClass::Terminal
            }
        };
    }
    if let Some(pub_err) = error.downcast_ref::<crate::storage::PublicationError>() {
        return match pub_err {
            crate::storage::PublicationError::OperationConflict { .. }
            | crate::storage::PublicationError::NotCommitted { .. } => StableErrorClass::Terminal,
            crate::storage::PublicationError::StaleWriterEpoch { .. }
            | crate::storage::PublicationError::UnfencedPointer { .. }
            | crate::storage::PublicationError::PointerConflict { .. }
            | crate::storage::PublicationError::Indeterminate { .. }
            | crate::storage::PublicationError::InjectedCrash { .. } => StableErrorClass::Retryable,
        };
    }
    if let Some(wf) = error.downcast_ref::<crate::transport::write_failure::WriteFailure>() {
        return classify_http_error(wf.status, &wf.error_type, &wf.reason);
    }
    for cause in error.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return classify_tonic_status(status);
        }
        if cause.is::<tonic::transport::Error>() {
            return StableErrorClass::Retryable;
        }
    }
    let text = format!("{error:#}").to_ascii_lowercase();
    if text.contains("checksum mismatch")
        || text.contains("corrupt")
        || text.contains("fail-stopped")
        || text.contains("sequence operation collision")
        || text.contains("unsupported")
            && (text.contains("format") || text.contains("recreate the index"))
    {
        return StableErrorClass::Corruption;
    }
    if text.contains("resource pool")
        || text.contains("admission rejected")
        || text.contains("deadline budget")
        || text.contains("capacity exceeded")
    {
        return StableErrorClass::Overload;
    }
    if text.contains("timed out")
        || text.contains("indeterminate")
        || text.contains("shard_not_available")
        || text.contains("unavailable")
    {
        return StableErrorClass::Retryable;
    }
    StableErrorClass::Terminal
}

pub fn classify_http_error(status: u16, error_type: &str, reason: &str) -> StableErrorClass {
    let lower_reason = reason.to_ascii_lowercase();
    if error_type.contains("corrupt")
        || error_type == "unsupported_index_format_exception"
        || lower_reason.contains("checksum mismatch")
        || lower_reason.contains("corrupt")
        || lower_reason.contains("fail-stopped")
        || lower_reason.contains("sequence operation collision")
        || (lower_reason.contains("unsupported") && lower_reason.contains("format"))
    {
        return StableErrorClass::Corruption;
    }
    if status == 429
        || status == 504
        || matches!(
            error_type,
            "rejected_execution_exception"
                | "es_rejected_execution_exception"
                | "deadline_exceeded_exception"
                | "version_map_capacity_exceeded"
                | "task_cancelled_exception"
                | "hot_delta_backpressure_exception"
        )
    {
        return StableErrorClass::Overload;
    }
    if status == 503
        || matches!(
            error_type,
            "shard_not_available_exception"
                | "master_not_discovered_exception"
                | "remote_store_leaf_unavailable_exception"
                | "remote_store_publish_indeterminate_exception"
                | "write_outcome_unknown"
        )
    {
        return StableErrorClass::Retryable;
    }
    StableErrorClass::Terminal
}

pub fn record_classified_error(class: StableErrorClass, stage: ExecutionStage) {
    crate::metrics::CLASSIFIED_ERRORS_TOTAL
        .with_label_values(&[class.as_str(), stage.as_str()])
        .inc();
}

struct RequestCancellationState {
    id: u64,
    cancelled: AtomicBool,
    reason: Mutex<Option<CancellationReason>>,
    notify: tokio::sync::Notify,
    deadline: Option<RequestDeadline>,
    default_stage: Mutex<ExecutionStage>,
    last_error_class: Mutex<Option<StableErrorClass>>,
    trace_ids: Mutex<RequestTraceIds>,
    principal_context: Arc<Mutex<Option<(String, String)>>>,
    target_indices: Arc<Mutex<Vec<String>>>,
    retained_guards: Arc<Mutex<Vec<Box<dyn Any + Send + Sync>>>>,
    security_denial_status: Arc<AtomicU64>,
    stage_ledger: Arc<StageTimingLedger>,
    children: Mutex<Vec<Weak<RequestCancellationState>>>,
    cleanup_hooks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    metric_recorded: AtomicBool,
}

static NEXT_CANCELLATION_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Request-scoped cancellation, deadline, and distributed trace context (`FS-028`, `FS-029`, `FS-033`).
#[derive(Clone)]
pub struct RequestCancellationContext {
    inner: Arc<RequestCancellationState>,
}

impl Default for RequestCancellationContext {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RequestCancellationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestCancellationContext")
            .field("id", &self.inner.id)
            .field("trace_ids", &self.trace_ids())
            .field("is_cancelled", &self.is_cancelled())
            .field("deadline", &self.inner.deadline)
            .finish()
    }
}

impl RequestCancellationContext {
    pub fn new() -> Self {
        let id = NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::new(RequestCancellationState {
                id,
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: None,
                default_stage: Mutex::new(ExecutionStage::Search),
                last_error_class: Mutex::new(None),
                trace_ids: Mutex::new(RequestTraceIds::new_for_context_id(id)),
                principal_context: Arc::new(Mutex::new(None)),
                target_indices: Arc::new(Mutex::new(Vec::new())),
                retained_guards: Arc::new(Mutex::new(Vec::new())),
                security_denial_status: Arc::new(AtomicU64::new(0)),
                stage_ledger: Arc::new(StageTimingLedger::default()),
                children: Mutex::new(Vec::new()),
                cleanup_hooks: Mutex::new(Vec::new()),
                metric_recorded: AtomicBool::new(false),
            }),
        }
    }

    pub fn with_deadline(deadline: RequestDeadline) -> Self {
        let id = NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::new(RequestCancellationState {
                id,
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: Some(deadline),
                default_stage: Mutex::new(ExecutionStage::Search),
                last_error_class: Mutex::new(None),
                trace_ids: Mutex::new(RequestTraceIds::new_for_context_id(id)),
                principal_context: Arc::new(Mutex::new(None)),
                target_indices: Arc::new(Mutex::new(Vec::new())),
                retained_guards: Arc::new(Mutex::new(Vec::new())),
                security_denial_status: Arc::new(AtomicU64::new(0)),
                stage_ledger: Arc::new(StageTimingLedger::default()),
                children: Mutex::new(Vec::new()),
                cleanup_hooks: Mutex::new(Vec::new()),
                metric_recorded: AtomicBool::new(false),
            }),
        }
    }

    pub fn with_budget(budget: Duration) -> Self {
        Self::with_deadline(RequestDeadline::with_budget(budget))
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn default_stage(&self) -> ExecutionStage {
        *self
            .inner
            .default_stage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub fn set_default_stage(&self, stage: ExecutionStage) {
        *self
            .inner
            .default_stage
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = stage;
    }

    pub fn last_error_class(&self) -> Option<StableErrorClass> {
        *self
            .inner
            .last_error_class
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub fn set_last_error_class(&self, class: StableErrorClass) {
        *self
            .inner
            .last_error_class
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(class);
    }

    pub fn trace_ids(&self) -> RequestTraceIds {
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn set_request_id(&self, raw: &str) -> bool {
        let Some(sanitized) = sanitize_trace_id(raw) else {
            return false;
        };
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request_id = sanitized;
        true
    }

    pub fn set_operation_id(&self, raw: &str) -> bool {
        let Some(sanitized) = sanitize_trace_id(raw) else {
            return false;
        };
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .operation_id = Some(sanitized);
        true
    }

    pub fn set_snapshot_id(&self, raw: &str) -> bool {
        let Some(sanitized) = sanitize_trace_id(raw) else {
            return false;
        };
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot_id = Some(sanitized);
        true
    }

    pub fn set_assignment_id(&self, raw: &str) -> bool {
        let Some(sanitized) = sanitize_trace_id(raw) else {
            return false;
        };
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .assignment_id = Some(sanitized);
        true
    }

    pub fn set_task_id(&self, raw: &str) -> bool {
        let Some(sanitized) = sanitize_trace_id(raw) else {
            return false;
        };
        self.inner
            .trace_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .task_id = Some(sanitized);
        true
    }

    pub fn set_principal(&self, principal_id: &str, principal_name: &str) {
        *self
            .inner
            .principal_context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some((principal_id.to_string(), principal_name.to_string()));
    }

    pub fn principal(&self) -> Option<(String, String)> {
        self.inner
            .principal_context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn add_target_index(&self, index: &str) {
        let trimmed = index.trim();
        if trimmed.is_empty() {
            return;
        }
        let mut indices = self
            .inner
            .target_indices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !indices.iter().any(|existing| existing == trimmed) {
            indices.push(trimmed.to_string());
        }
    }

    pub fn target_indices(&self) -> Vec<String> {
        self.inner
            .target_indices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn retain_guard<G: Any + Send + Sync>(&self, guard: G) {
        self.inner
            .retained_guards
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Box::new(guard));
    }

    pub fn record_security_denial_status(&self, status: u16) {
        self.inner
            .security_denial_status
            .store(u64::from(status), Ordering::Release);
    }

    pub fn security_denial_status(&self) -> Option<u16> {
        let val = self.inner.security_denial_status.load(Ordering::Acquire);
        (val > 0).then_some(val as u16)
    }

    pub fn stage_ledger(&self) -> &Arc<StageTimingLedger> {
        &self.inner.stage_ledger
    }

    pub fn record_stage_duration(&self, stage: TraceStage, duration: Duration) {
        self.inner.stage_ledger.record_stage(stage, duration);
    }

    pub fn stage_snapshot(&self) -> StageTimingSnapshot {
        self.inner.stage_ledger.snapshot()
    }

    pub fn deadline(&self) -> Option<&RequestDeadline> {
        self.inner.deadline.as_ref()
    }

    pub fn remaining_budget(&self) -> Option<Duration> {
        self.inner.deadline.as_ref().map(RequestDeadline::remaining)
    }

    pub fn child(&self) -> Self {
        let child_deadline = self
            .inner
            .deadline
            .as_ref()
            .map(|d| d.child_capped(d.remaining()));
        self.build_child(child_deadline)
    }

    pub fn child_with_budget(&self, cap: Duration) -> Self {
        let child_deadline = match &self.inner.deadline {
            Some(parent) => Some(parent.child_capped(cap)),
            None => Some(RequestDeadline::with_budget(cap)),
        };
        self.build_child(child_deadline)
    }

    fn build_child(&self, child_deadline: Option<RequestDeadline>) -> Self {
        let id = NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed);
        let child = Self {
            inner: Arc::new(RequestCancellationState {
                id,
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: child_deadline,
                default_stage: Mutex::new(self.default_stage()),
                last_error_class: Mutex::new(self.last_error_class()),
                trace_ids: Mutex::new(self.trace_ids()),
                principal_context: Arc::clone(&self.inner.principal_context),
                target_indices: Arc::clone(&self.inner.target_indices),
                retained_guards: Arc::clone(&self.inner.retained_guards),
                security_denial_status: Arc::clone(&self.inner.security_denial_status),
                stage_ledger: Arc::clone(&self.inner.stage_ledger),
                children: Mutex::new(Vec::new()),
                cleanup_hooks: Mutex::new(Vec::new()),
                metric_recorded: AtomicBool::new(false),
            }),
        };
        if let Some(reason) = self.cancellation_reason() {
            let child_reason = match reason {
                CancellationReason::DeadlineExpired(stage) => {
                    CancellationReason::DeadlineExpired(stage)
                }
                _ => CancellationReason::ParentCancelled,
            };
            child.cancel(child_reason);
            return child;
        }
        let mut children = self
            .inner
            .children
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        children.retain(|weak| weak.strong_count() > 0);
        children.push(Arc::downgrade(&child.inner));
        drop(children);
        if self.inner.cancelled.load(Ordering::Acquire) {
            child.cancel(CancellationReason::ParentCancelled);
        }
        child
    }

    pub fn on_cancel(&self, hook: impl FnOnce() + Send + 'static) {
        if self.is_cancelled() {
            hook();
            return;
        }
        let mut hooks = self
            .inner
            .cleanup_hooks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.inner.cancelled.load(Ordering::Acquire) {
            drop(hooks);
            hook();
        } else {
            hooks.push(Box::new(hook));
        }
    }

    pub fn cancel(&self, reason: CancellationReason) {
        Self::cancel_state(&self.inner, reason);
    }

    fn cancel_state(state: &Arc<RequestCancellationState>, reason: CancellationReason) {
        {
            let mut reason_guard = state
                .reason
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if reason_guard.is_none() {
                *reason_guard = Some(reason.clone());
            }
        }
        let was_cancelled = state.cancelled.swap(true, Ordering::AcqRel);
        if !was_cancelled {
            if let CancellationReason::DeadlineExpired(stage) = &reason
                && let Some(ref deadline) = state.deadline
            {
                deadline.record_exhausted_stage(*stage);
            }
            let hooks = std::mem::take(
                &mut *state
                    .cleanup_hooks
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            );
            for hook in hooks {
                hook();
            }
            let children = state
                .children
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            let child_reason = match &reason {
                CancellationReason::DeadlineExpired(stage) => {
                    CancellationReason::DeadlineExpired(*stage)
                }
                _ => CancellationReason::ParentCancelled,
            };
            for weak in children {
                if let Some(child) = weak.upgrade() {
                    Self::cancel_state(&child, child_reason.clone());
                }
            }
            state.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        if self.inner.cancelled.load(Ordering::Acquire) {
            return true;
        }
        if let Some(ref deadline) = self.inner.deadline
            && deadline.is_expired()
        {
            return true;
        }
        false
    }

    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        if let Some(reason) = self
            .inner
            .reason
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Some(reason);
        }
        if let Some(ref deadline) = self.inner.deadline
            && deadline.is_expired()
        {
            let stage = deadline.exhausted_stage().unwrap_or(ExecutionStage::Search);
            return Some(CancellationReason::DeadlineExpired(stage));
        }
        None
    }

    pub fn check_active(&self, stage: ExecutionStage) -> crate::common::Result<()> {
        if self.inner.cancelled.load(Ordering::Acquire) {
            let reason = self
                .inner
                .reason
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
                .unwrap_or(CancellationReason::ClientDisconnected);
            if !self.inner.metric_recorded.swap(true, Ordering::AcqRel) {
                crate::metrics::REQUEST_CANCELLATIONS_TOTAL
                    .with_label_values(&[stage.as_str(), reason.metric_label()])
                    .inc();
            }
            if let CancellationReason::DeadlineExpired(expired_stage) = reason {
                let recorded_stage = match &self.inner.deadline {
                    Some(deadline) => deadline.record_exhausted_stage(expired_stage),
                    None => expired_stage,
                };
                let budget_ms = self
                    .inner
                    .deadline
                    .as_ref()
                    .map_or(0, RequestDeadline::initial_budget_ms);
                return Err(RequestDeadlineExceeded {
                    stage: recorded_stage,
                    budget_ms,
                }
                .into());
            }
            return Err(RequestCancelledError { stage, reason }.into());
        }
        if let Some(ref deadline) = self.inner.deadline
            && deadline.is_expired()
        {
            let recorded_stage = deadline.record_exhausted_stage(stage);
            self.cancel(CancellationReason::DeadlineExpired(recorded_stage));
            if !self.inner.metric_recorded.swap(true, Ordering::AcqRel) {
                crate::metrics::REQUEST_CANCELLATIONS_TOTAL
                    .with_label_values(&[recorded_stage.as_str(), "deadline_expired"])
                    .inc();
            }
            return Err(RequestDeadlineExceeded {
                stage: recorded_stage,
                budget_ms: deadline.initial_budget_ms(),
            }
            .into());
        }
        Ok(())
    }

    pub async fn wait_cancelled(&self, default_stage: ExecutionStage) -> CancellationReason {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.cancelled.load(Ordering::Acquire) {
                return self
                    .cancellation_reason()
                    .unwrap_or(CancellationReason::ClientDisconnected);
            }
            if let Some(ref deadline) = self.inner.deadline {
                let remaining = deadline.remaining();
                if remaining.is_zero() {
                    let stage = deadline.record_exhausted_stage(default_stage);
                    let reason = CancellationReason::DeadlineExpired(stage);
                    self.cancel(reason.clone());
                    return reason;
                }
                tokio::select! {
                    () = notified => {}
                    () = tokio::time::sleep(remaining) => {
                        let stage = deadline.record_exhausted_stage(default_stage);
                        let reason = CancellationReason::DeadlineExpired(stage);
                        self.cancel(reason.clone());
                        return reason;
                    }
                }
            } else {
                notified.await;
            }
        }
    }

    pub async fn run_scoped<F, T>(&self, stage: ExecutionStage, fut: F) -> crate::common::Result<T>
    where
        F: std::future::Future<Output = crate::common::Result<T>>,
    {
        self.check_active(stage)?;
        let ctx = self.clone();
        with_task_request_context(ctx.clone(), async move {
            tokio::select! {
                 biased;
                _ = ctx.wait_cancelled(stage) => {
                    ctx.check_active(stage)?;
                    Err(RequestCancelledError {
                        stage,
                        reason: CancellationReason::ClientDisconnected,
                    }.into())
                }
                result = fut => result,
            }
        })
        .await
    }

    pub fn drop_guard(&self, reason_on_drop: CancellationReason) -> CancellationDropGuard {
        CancellationDropGuard {
            ctx: Some(self.clone()),
            reason_on_drop: Some(reason_on_drop),
        }
    }
}

pub struct CancellationDropGuard {
    ctx: Option<RequestCancellationContext>,
    reason_on_drop: Option<CancellationReason>,
}

impl CancellationDropGuard {
    pub fn disarm(&mut self) {
        self.ctx = None;
        self.reason_on_drop = None;
    }
}

impl Drop for CancellationDropGuard {
    fn drop(&mut self) {
        if let (Some(ctx), Some(reason)) = (self.ctx.take(), self.reason_on_drop.take()) {
            ctx.cancel(reason);
        }
    }
}

tokio::task_local! {
    static TASK_REQUEST_CONTEXT: RequestCancellationContext;
}

std::thread_local! {
    static THREAD_REQUEST_CONTEXT: std::cell::RefCell<Option<RequestCancellationContext>> =
        const { std::cell::RefCell::new(None) };
}

pub async fn with_task_request_context<F>(ctx: RequestCancellationContext, fut: F) -> F::Output
where
    F: std::future::Future,
{
    TASK_REQUEST_CONTEXT.scope(ctx, fut).await
}

pub fn with_thread_request_context<R>(
    ctx: Option<RequestCancellationContext>,
    f: impl FnOnce() -> R,
) -> R {
    struct ThreadContextGuard {
        previous: Option<RequestCancellationContext>,
    }
    impl Drop for ThreadContextGuard {
        fn drop(&mut self) {
            let previous = self.previous.take();
            THREAD_REQUEST_CONTEXT.with(|slot| {
                *slot.borrow_mut() = previous;
            });
        }
    }
    let previous =
        THREAD_REQUEST_CONTEXT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), ctx));
    let _guard = ThreadContextGuard { previous };
    f()
}

pub fn current_request_context() -> Option<RequestCancellationContext> {
    if let Ok(ctx) = TASK_REQUEST_CONTEXT.try_with(Clone::clone) {
        return Some(ctx);
    }
    THREAD_REQUEST_CONTEXT.with(|slot| slot.borrow().clone())
}

pub fn check_current_request_active(stage: ExecutionStage) -> crate::common::Result<()> {
    if let Some(ctx) = current_request_context() {
        ctx.check_active(stage)?;
    }
    Ok(())
}

pub fn record_current_stage_duration(stage: TraceStage, duration: Duration) {
    if let Some(ctx) = current_request_context() {
        ctx.record_stage_duration(stage, duration);
    } else {
        crate::metrics::STAGE_EXECUTION_DURATION_SECONDS
            .with_label_values(&[stage.as_str()])
            .observe(duration.as_secs_f64());
    }
}

pub fn start_current_stage_timer(stage: TraceStage) -> StageTimerGuard {
    let ledger = current_request_context().map(|ctx| Arc::clone(ctx.stage_ledger()));
    StageTimerGuard {
        ledger,
        stage,
        started_at: Instant::now(),
    }
}

pub fn set_current_trace_operation_id(raw: &str) {
    if let Some(ctx) = current_request_context() {
        let _ = ctx.set_operation_id(raw);
    }
}

pub fn set_current_trace_snapshot_id(raw: &str) {
    if let Some(ctx) = current_request_context() {
        let _ = ctx.set_snapshot_id(raw);
    }
}

pub fn set_current_trace_assignment_id(raw: &str) {
    if let Some(ctx) = current_request_context() {
        let _ = ctx.set_assignment_id(raw);
    }
}

pub fn set_current_trace_task_id(raw: &str) {
    if let Some(ctx) = current_request_context() {
        let _ = ctx.set_task_id(raw);
    }
}

pub fn set_current_request_principal(principal_id: &str, principal_name: &str) {
    if let Some(ctx) = current_request_context() {
        ctx.set_principal(principal_id, principal_name);
    }
}

pub fn current_request_principal() -> Option<(String, String)> {
    current_request_context().and_then(|ctx| ctx.principal())
}

pub fn add_current_request_index(index: &str) {
    if let Some(ctx) = current_request_context() {
        ctx.add_target_index(index);
    }
}

pub fn current_request_indices() -> Vec<String> {
    current_request_context()
        .map(|ctx| ctx.target_indices())
        .unwrap_or_default()
}

pub fn retain_current_request_guard<G: Any + Send + Sync>(guard: G) {
    if let Some(ctx) = current_request_context() {
        ctx.retain_guard(guard);
    }
}

pub fn record_current_request_security_denial(status: u16) {
    if let Some(ctx) = current_request_context() {
        ctx.record_security_denial_status(status);
    }
}

pub fn current_request_security_denial() -> Option<u16> {
    current_request_context().and_then(|ctx| ctx.security_denial_status())
}

// ── Bounded Admission Queues & Background Work Prioritization (FS-030, FS-032) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPriorityTier {
    Foreground,
    Background,
}

impl WorkPriorityTier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkClass {
    Search,
    Sql,
    Write,
    Hydration,
    Recovery,
    Maintenance,
}

impl WorkClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Sql => "sql",
            Self::Write => "write",
            Self::Hydration => "hydration",
            Self::Recovery => "recovery",
            Self::Maintenance => "maintenance",
        }
    }

    pub const fn priority_tier(self) -> WorkPriorityTier {
        match self {
            Self::Search | Self::Sql | Self::Write => WorkPriorityTier::Foreground,
            Self::Hydration | Self::Recovery | Self::Maintenance => WorkPriorityTier::Background,
        }
    }

    pub const fn is_foreground(self) -> bool {
        matches!(self.priority_tier(), WorkPriorityTier::Foreground)
    }

    pub const fn is_background(self) -> bool {
        matches!(self.priority_tier(), WorkPriorityTier::Background)
    }

    pub fn execution_stage(self) -> ExecutionStage {
        match self {
            Self::Search => ExecutionStage::Search,
            Self::Sql => ExecutionStage::Sql,
            Self::Write => ExecutionStage::Write,
            Self::Hydration => ExecutionStage::Hydration,
            Self::Recovery => ExecutionStage::Recovery,
            Self::Maintenance => ExecutionStage::Maintenance,
        }
    }

    pub const fn trace_stage(self) -> TraceStage {
        match self {
            Self::Search => TraceStage::Search,
            Self::Sql => TraceStage::Sql,
            Self::Write => TraceStage::Write,
            Self::Hydration => TraceStage::Hydration,
            Self::Recovery => TraceStage::Recovery,
            Self::Maintenance => TraceStage::Maintenance,
        }
    }

    pub const ALL: [Self; 6] = [
        Self::Search,
        Self::Sql,
        Self::Write,
        Self::Hydration,
        Self::Recovery,
        Self::Maintenance,
    ];
}

impl std::fmt::Display for WorkClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkClassLimits {
    pub max_concurrent: usize,
    pub max_queue_depth: usize,
}

/// Explicit per-class concurrency, starvation-floor, queue, I/O, and byte budgets (`FS-032`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkClassBudget {
    pub max_concurrent: usize,
    pub min_guaranteed_concurrent: usize,
    pub max_queue_depth: usize,
    pub max_byte_budget: u64,
    pub io_bandwidth_bytes_per_sec: u64,
    pub starvation_aging_ms: u64,
    pub yield_backoff_ms: u64,
}

impl WorkClassBudget {
    pub fn for_limits(work_class: WorkClass, limits: WorkClassLimits) -> Self {
        let max_concurrent = limits.max_concurrent.max(1);
        let min_guaranteed_concurrent = if work_class.is_foreground() {
            max_concurrent
        } else {
            1.min(max_concurrent)
        };
        Self {
            max_concurrent,
            min_guaranteed_concurrent,
            max_queue_depth: limits.max_queue_depth,
            max_byte_budget: if work_class.is_foreground() {
                1024 * 1024 * 1024
            } else {
                256 * 1024 * 1024
            },
            io_bandwidth_bytes_per_sec: 0,
            starvation_aging_ms: 200,
            yield_backoff_ms: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionClassSnapshot {
    pub work_class: WorkClass,
    pub priority_tier: WorkPriorityTier,
    pub active: usize,
    pub queued: usize,
    pub max_concurrent: usize,
    pub effective_max_concurrent: usize,
    pub min_guaranteed_concurrent: usize,
    pub max_queue_depth: usize,
    pub saturation_ratio: f64,
    pub acquired_total: u64,
    pub rejected_queue_full: u64,
    pub rejected_cancelled: u64,
    pub rejected_deadline: u64,
    pub throttled_total: u64,
    pub starvation_promoted_total: u64,
    pub io_bytes_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error(
        "admission rejected for work class [{work_class}]: active={active}/{max_concurrent}, queued={queued}/{max_queue_depth}"
    )]
    QueueFull {
        work_class: WorkClass,
        active: usize,
        max_concurrent: usize,
        queued: usize,
        max_queue_depth: usize,
    },
    #[error(transparent)]
    Cancelled(#[from] RequestCancelledError),
    #[error(transparent)]
    DeadlineExceeded(#[from] RequestDeadlineExceeded),
}

struct QueuedWaiter {
    id: u64,
    enqueued_at: Instant,
    tx: tokio::sync::oneshot::Sender<()>,
    ctx: Option<RequestCancellationContext>,
}

struct ClassQueueInner {
    budget: WorkClassBudget,
    active: usize,
    next_waiter_id: u64,
    waiters: VecDeque<QueuedWaiter>,
    acquired_total: u64,
    rejected_queue_full: u64,
    rejected_cancelled: u64,
    rejected_deadline: u64,
    throttled_total: u64,
    starvation_promoted_total: u64,
    io_bytes_total: u64,
}

struct ClassQueueState {
    work_class: WorkClass,
    inner: Mutex<ClassQueueInner>,
}

impl ClassQueueState {
    fn new(work_class: WorkClass, limits: WorkClassLimits) -> Self {
        let budget = WorkClassBudget::for_limits(work_class, limits);
        Self {
            work_class,
            inner: Mutex::new(ClassQueueInner {
                budget,
                active: 0,
                next_waiter_id: 1,
                waiters: VecDeque::new(),
                acquired_total: 0,
                rejected_queue_full: 0,
                rejected_cancelled: 0,
                rejected_deadline: 0,
                throttled_total: 0,
                starvation_promoted_total: 0,
                io_bytes_total: 0,
            }),
        }
    }

    fn update_gauges(&self, active: usize, queued: usize, effective_max: usize) {
        let eff = effective_max.max(1);
        let ratio = (active as f64) / (eff as f64);
        crate::metrics::ADMISSION_EXECUTING_GAUGE
            .with_label_values(&[self.work_class.as_str()])
            .set(active as i64);
        crate::metrics::ADMISSION_QUEUED_GAUGE
            .with_label_values(&[self.work_class.as_str()])
            .set(queued as i64);
        crate::metrics::WORK_CLASS_SATURATION_RATIO
            .with_label_values(&[
                self.work_class.as_str(),
                self.work_class.priority_tier().as_str(),
            ])
            .set(ratio);
    }

    fn release_slot_with_ceiling(&self, foreground_pressure: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let base_effective = if self.work_class.is_background() && foreground_pressure {
            inner
                .budget
                .min_guaranteed_concurrent
                .max(1)
                .min(inner.budget.max_concurrent)
        } else {
            inner.budget.max_concurrent.max(1)
        };
        while let Some(front) = inner.waiters.front() {
            if let Some(ref ctx) = front.ctx
                && ctx.is_cancelled()
            {
                inner.waiters.pop_front();
                continue;
            }
            let aged = self.work_class.is_background()
                && foreground_pressure
                && front.enqueued_at.elapsed()
                    >= Duration::from_millis(inner.budget.starvation_aging_ms);
            let allowed_ceiling = if aged {
                inner.budget.max_concurrent.max(1)
            } else {
                base_effective
            };
            // We are releasing 1 active slot; handing it to the next waiter keeps `inner.active` unchanged
            // as long as the resulting active count is within `allowed_ceiling`.
            if inner.active > allowed_ceiling {
                break;
            }
            let waiter = inner.waiters.pop_front().expect("checked front");
            if waiter.tx.send(()).is_ok() {
                if aged {
                    inner.starvation_promoted_total += 1;
                    crate::metrics::BACKGROUND_THROTTLED_TOTAL
                        .with_label_values(&[self.work_class.as_str(), "starvation_promoted"])
                        .inc();
                }
                let active = inner.active;
                let queued = inner.waiters.len();
                drop(inner);
                self.update_gauges(active, queued, allowed_ceiling);
                return;
            }
        }
        inner.active = inner.active.saturating_sub(1);
        let active = inner.active;
        let queued = inner.waiters.len();
        drop(inner);
        self.update_gauges(active, queued, base_effective);
    }

    fn wake_unthrottled_waiters(&self, foreground_pressure: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let effective = if self.work_class.is_background() && foreground_pressure {
            inner
                .budget
                .min_guaranteed_concurrent
                .max(1)
                .min(inner.budget.max_concurrent)
        } else {
            inner.budget.max_concurrent.max(1)
        };
        let mut woke_any = false;
        while inner.active < effective {
            let Some(waiter) = inner.waiters.pop_front() else {
                break;
            };
            if let Some(ref ctx) = waiter.ctx
                && ctx.is_cancelled()
            {
                continue;
            }
            if waiter.tx.send(()).is_ok() {
                inner.active += 1;
                woke_any = true;
            }
        }
        let active = inner.active;
        let queued = inner.waiters.len();
        drop(inner);
        if woke_any || queued == 0 {
            self.update_gauges(active, queued, effective);
        }
    }
}

pub struct AdmissionController {
    search: Arc<ClassQueueState>,
    sql: Arc<ClassQueueState>,
    write: Arc<ClassQueueState>,
    hydration: Arc<ClassQueueState>,
    recovery: Arc<ClassQueueState>,
    maintenance: Arc<ClassQueueState>,
}

impl AdmissionController {
    pub fn new(search_threads: usize, write_threads: usize) -> Self {
        let search_conc = search_threads.max(2) * 4;
        let write_conc = write_threads.max(2) * 4;
        Self {
            search: Arc::new(ClassQueueState::new(
                WorkClass::Search,
                WorkClassLimits {
                    max_concurrent: search_conc,
                    max_queue_depth: search_conc * 8,
                },
            )),
            sql: Arc::new(ClassQueueState::new(
                WorkClass::Sql,
                WorkClassLimits {
                    max_concurrent: search_conc,
                    max_queue_depth: search_conc * 8,
                },
            )),
            write: Arc::new(ClassQueueState::new(
                WorkClass::Write,
                WorkClassLimits {
                    max_concurrent: write_conc,
                    max_queue_depth: write_conc * 8,
                },
            )),
            hydration: Arc::new(ClassQueueState::new(
                WorkClass::Hydration,
                WorkClassLimits {
                    max_concurrent: search_threads.max(2) * 2,
                    max_queue_depth: search_threads.max(2) * 8,
                },
            )),
            recovery: Arc::new(ClassQueueState::new(
                WorkClass::Recovery,
                WorkClassLimits {
                    max_concurrent: 4,
                    max_queue_depth: 16,
                },
            )),
            maintenance: Arc::new(ClassQueueState::new(
                WorkClass::Maintenance,
                WorkClassLimits {
                    max_concurrent: 4,
                    max_queue_depth: 32,
                },
            )),
        }
    }

    fn state_for(&self, work_class: WorkClass) -> &Arc<ClassQueueState> {
        match work_class {
            WorkClass::Search => &self.search,
            WorkClass::Sql => &self.sql,
            WorkClass::Write => &self.write,
            WorkClass::Hydration => &self.hydration,
            WorkClass::Recovery => &self.recovery,
            WorkClass::Maintenance => &self.maintenance,
        }
    }

    /// Returns true when foreground classes (`Search`, `Sql`, `Write`) have queued
    /// waiters or are at/above 50% concurrency utilization (`FS-032`).
    pub fn has_foreground_pressure(&self) -> bool {
        for fg in [WorkClass::Search, WorkClass::Sql, WorkClass::Write] {
            let inner = self
                .state_for(fg)
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !inner.waiters.is_empty()
                || (inner.active > 0 && inner.active * 2 >= inner.budget.max_concurrent.max(1))
            {
                return true;
            }
        }
        false
    }

    pub fn set_limits(&self, work_class: WorkClass, limits: WorkClassLimits) {
        let state = self.state_for(work_class);
        let mut inner = state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        inner.budget.max_concurrent = limits.max_concurrent.max(1);
        inner.budget.max_queue_depth = limits.max_queue_depth;
        if work_class.is_foreground() {
            inner.budget.min_guaranteed_concurrent = inner.budget.max_concurrent;
        } else {
            inner.budget.min_guaranteed_concurrent = inner
                .budget
                .min_guaranteed_concurrent
                .max(1)
                .min(inner.budget.max_concurrent);
        }
    }

    pub fn set_budget(&self, work_class: WorkClass, budget: WorkClassBudget) {
        let state = self.state_for(work_class);
        let mut inner = state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let max_concurrent = budget.max_concurrent.max(1);
        let min_guaranteed_concurrent = budget.min_guaranteed_concurrent.max(1).min(max_concurrent);
        inner.budget = WorkClassBudget {
            max_concurrent,
            min_guaranteed_concurrent,
            max_queue_depth: budget.max_queue_depth,
            max_byte_budget: budget.max_byte_budget,
            io_bandwidth_bytes_per_sec: budget.io_bandwidth_bytes_per_sec,
            starvation_aging_ms: budget.starvation_aging_ms,
            yield_backoff_ms: budget.yield_backoff_ms,
        };
    }

    pub fn budget(&self, work_class: WorkClass) -> WorkClassBudget {
        self.state_for(work_class)
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .budget
    }

    pub fn limits(&self, work_class: WorkClass) -> WorkClassLimits {
        let b = self.budget(work_class);
        WorkClassLimits {
            max_concurrent: b.max_concurrent,
            max_queue_depth: b.max_queue_depth,
        }
    }

    pub fn snapshot(&self, work_class: WorkClass) -> AdmissionClassSnapshot {
        let fg_pressure = self.has_foreground_pressure();
        let inner = self
            .state_for(work_class)
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let effective_max = if work_class.is_background() && fg_pressure {
            inner
                .budget
                .min_guaranteed_concurrent
                .max(1)
                .min(inner.budget.max_concurrent)
        } else {
            inner.budget.max_concurrent.max(1)
        };
        let saturation_ratio = (inner.active as f64) / (effective_max as f64);
        AdmissionClassSnapshot {
            work_class,
            priority_tier: work_class.priority_tier(),
            active: inner.active,
            queued: inner.waiters.len(),
            max_concurrent: inner.budget.max_concurrent,
            effective_max_concurrent: effective_max,
            min_guaranteed_concurrent: inner.budget.min_guaranteed_concurrent,
            max_queue_depth: inner.budget.max_queue_depth,
            saturation_ratio,
            acquired_total: inner.acquired_total,
            rejected_queue_full: inner.rejected_queue_full,
            rejected_cancelled: inner.rejected_cancelled,
            rejected_deadline: inner.rejected_deadline,
            throttled_total: inner.throttled_total,
            starvation_promoted_total: inner.starvation_promoted_total,
            io_bytes_total: inner.io_bytes_total,
        }
    }

    pub fn total_queued(&self) -> usize {
        WorkClass::ALL
            .into_iter()
            .map(|class| self.snapshot(class).queued)
            .sum()
    }

    /// Account I/O bytes for `work_class` and optionally pace background I/O
    /// when `io_bandwidth_bytes_per_sec` is configured or foreground pressure is active (`FS-032`).
    pub async fn account_io_bytes(
        &self,
        work_class: WorkClass,
        bytes: u64,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<(), AdmissionError> {
        Self::check_ctx_for_queue(self.state_for(work_class), ctx)?;
        if bytes == 0 {
            return Ok(());
        }
        crate::metrics::WORK_CLASS_IO_BYTES_TOTAL
            .with_label_values(&[work_class.as_str()])
            .inc_by(bytes);
        let rate_bps = {
            let state = self.state_for(work_class);
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            inner.io_bytes_total = inner.io_bytes_total.saturating_add(bytes);
            inner.budget.io_bandwidth_bytes_per_sec
        };
        if work_class.is_background() && rate_bps > 0 {
            let delay_ms = ((bytes as f64 / rate_bps as f64) * 1000.0)
                .ceil()
                .clamp(1.0, 50.0) as u64;
            crate::metrics::BACKGROUND_THROTTLED_TOTAL
                .with_label_values(&[work_class.as_str(), "io_budget"])
                .inc();
            let sleep_dur = Duration::from_millis(delay_ms);
            if let Some(ctx) = ctx {
                tokio::select! {
                    biased;
                    _ = ctx.wait_cancelled(work_class.execution_stage()) => {
                        Self::check_ctx_for_queue(self.state_for(work_class), Some(ctx))?;
                    }
                    () = tokio::time::sleep(sleep_dur) => {}
                }
            } else {
                tokio::time::sleep(sleep_dur).await;
            }
        }
        Ok(())
    }

    /// Cooperative background yield under foreground load (`FS-032`).
    /// Yields the current Tokio task (and sleeps briefly when foreground
    /// pressure is active) while respecting cancellation.
    pub async fn cooperative_background_yield(
        &self,
        work_class: WorkClass,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<bool, AdmissionError> {
        let state = self.state_for(work_class);
        Self::check_ctx_for_queue(state, ctx)?;
        if !work_class.is_background() || !self.has_foreground_pressure() {
            tokio::task::yield_now().await;
            return Ok(false);
        }
        let backoff_ms = {
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            inner.throttled_total += 1;
            inner.budget.yield_backoff_ms
        };
        crate::metrics::BACKGROUND_THROTTLED_TOTAL
            .with_label_values(&[work_class.as_str(), "foreground_pressure"])
            .inc();
        tokio::task::yield_now().await;
        if backoff_ms > 0 {
            let sleep_dur = Duration::from_millis(backoff_ms);
            if let Some(ctx) = ctx {
                tokio::select! {
                    biased;
                    _ = ctx.wait_cancelled(work_class.execution_stage()) => {
                        Self::check_ctx_for_queue(state, Some(ctx))?;
                    }
                    () = tokio::time::sleep(sleep_dur) => {}
                }
            } else {
                tokio::time::sleep(sleep_dur).await;
            }
        }
        Ok(true)
    }

    fn notify_background_queues_if_unpressured(&self) {
        let fg_pressure = self.has_foreground_pressure();
        if !fg_pressure {
            for bg in [
                WorkClass::Hydration,
                WorkClass::Recovery,
                WorkClass::Maintenance,
            ] {
                self.state_for(bg).wake_unthrottled_waiters(false);
            }
        }
    }

    fn check_ctx_for_queue(
        state: &ClassQueueState,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<(), AdmissionError> {
        let Some(ctx) = ctx else {
            return Ok(());
        };
        if let Err(error) = ctx.check_active(ExecutionStage::Queue) {
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(deadline) = error.downcast_ref::<RequestDeadlineExceeded>() {
                inner.rejected_deadline += 1;
                drop(inner);
                crate::metrics::ADMISSION_REJECTED_TOTAL
                    .with_label_values(&[state.work_class.as_str(), "deadline_expired"])
                    .inc();
                record_classified_error(StableErrorClass::Overload, ExecutionStage::Queue);
                return Err(AdmissionError::DeadlineExceeded(deadline.clone()));
            }
            if let Some(cancelled) = error.downcast_ref::<RequestCancelledError>() {
                inner.rejected_cancelled += 1;
                drop(inner);
                crate::metrics::ADMISSION_REJECTED_TOTAL
                    .with_label_values(&[state.work_class.as_str(), "cancelled"])
                    .inc();
                record_classified_error(StableErrorClass::Overload, ExecutionStage::Queue);
                return Err(AdmissionError::Cancelled(cancelled.clone()));
            }
        }
        Ok(())
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        work_class: WorkClass,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<AdmissionLease, AdmissionError> {
        let state = self.state_for(work_class).clone();
        Self::check_ctx_for_queue(&state, ctx)?;
        let fg_pressure = self.has_foreground_pressure();
        let mut inner = state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let effective_max = if work_class.is_background() && fg_pressure {
            inner
                .budget
                .min_guaranteed_concurrent
                .max(1)
                .min(inner.budget.max_concurrent)
        } else {
            inner.budget.max_concurrent.max(1)
        };
        if inner.active < effective_max && inner.waiters.is_empty() {
            inner.active += 1;
            inner.acquired_total += 1;
            let active = inner.active;
            let queued = inner.waiters.len();
            drop(inner);
            state.update_gauges(active, queued, effective_max);
            crate::metrics::ADMISSION_ACQUIRED_TOTAL
                .with_label_values(&[work_class.as_str()])
                .inc();
            crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                .with_label_values(&[work_class.as_str()])
                .observe(0.0);
            if let Some(ctx) = ctx {
                ctx.record_stage_duration(TraceStage::Queue, Duration::from_nanos(1));
            }
            return Ok(AdmissionLease {
                controller: Arc::clone(self),
                state,
                started_at: Instant::now(),
            });
        }
        inner.rejected_queue_full += 1;
        let err = AdmissionError::QueueFull {
            work_class,
            active: inner.active,
            max_concurrent: inner.budget.max_concurrent,
            queued: inner.waiters.len(),
            max_queue_depth: inner.budget.max_queue_depth,
        };
        drop(inner);
        crate::metrics::ADMISSION_REJECTED_TOTAL
            .with_label_values(&[work_class.as_str(), "queue_full"])
            .inc();
        record_classified_error(StableErrorClass::Overload, ExecutionStage::Queue);
        Err(err)
    }

    pub async fn acquire(
        self: &Arc<Self>,
        work_class: WorkClass,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<AdmissionLease, AdmissionError> {
        let state = self.state_for(work_class).clone();
        Self::check_ctx_for_queue(&state, ctx)?;

        let enqueued_at = Instant::now();
        let fg_pressure = self.has_foreground_pressure();
        let (waiter_id, mut rx, starvation_aging_ms) = {
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let effective_max = if work_class.is_background() && fg_pressure {
                inner
                    .budget
                    .min_guaranteed_concurrent
                    .max(1)
                    .min(inner.budget.max_concurrent)
            } else {
                inner.budget.max_concurrent.max(1)
            };
            if inner.active < effective_max && inner.waiters.is_empty() {
                inner.active += 1;
                inner.acquired_total += 1;
                let active = inner.active;
                let queued = inner.waiters.len();
                drop(inner);
                state.update_gauges(active, queued, effective_max);
                crate::metrics::ADMISSION_ACQUIRED_TOTAL
                    .with_label_values(&[work_class.as_str()])
                    .inc();
                crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                    .with_label_values(&[work_class.as_str()])
                    .observe(0.0);
                if let Some(ctx) = ctx {
                    ctx.record_stage_duration(TraceStage::Queue, Duration::from_nanos(1));
                }
                return Ok(AdmissionLease {
                    controller: Arc::clone(self),
                    state,
                    started_at: Instant::now(),
                });
            }
            if work_class.is_background()
                && fg_pressure
                && inner.active >= effective_max
                && inner.active < inner.budget.max_concurrent
            {
                inner.throttled_total += 1;
                crate::metrics::BACKGROUND_THROTTLED_TOTAL
                    .with_label_values(&[work_class.as_str(), "foreground_pressure"])
                    .inc();
            }
            if inner.waiters.len() >= inner.budget.max_queue_depth {
                inner.rejected_queue_full += 1;
                let err = AdmissionError::QueueFull {
                    work_class,
                    active: inner.active,
                    max_concurrent: inner.budget.max_concurrent,
                    queued: inner.waiters.len(),
                    max_queue_depth: inner.budget.max_queue_depth,
                };
                drop(inner);
                crate::metrics::ADMISSION_REJECTED_TOTAL
                    .with_label_values(&[work_class.as_str(), "queue_full"])
                    .inc();
                record_classified_error(StableErrorClass::Overload, ExecutionStage::Queue);
                return Err(err);
            }
            let waiter_id = inner.next_waiter_id;
            inner.next_waiter_id += 1;
            let aging_ms = inner.budget.starvation_aging_ms;
            let (tx, rx) = tokio::sync::oneshot::channel();
            inner.waiters.push_back(QueuedWaiter {
                id: waiter_id,
                enqueued_at,
                tx,
                ctx: ctx.cloned(),
            });
            let active = inner.active;
            let queued = inner.waiters.len();
            drop(inner);
            state.update_gauges(active, queued, effective_max);
            (waiter_id, rx, aging_ms)
        };

        struct QueuedWaiterGuard<'a> {
            controller: &'a Arc<AdmissionController>,
            state: &'a Arc<ClassQueueState>,
            waiter_id: u64,
            rx: &'a mut tokio::sync::oneshot::Receiver<()>,
            disarmed: bool,
        }

        impl Drop for QueuedWaiterGuard<'_> {
            fn drop(&mut self) {
                if self.disarmed {
                    return;
                }
                let fg_pressure = self.controller.has_foreground_pressure();
                let removed = {
                    let mut inner = self
                        .state
                        .inner
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    let eff = if self.state.work_class.is_background() && fg_pressure {
                        inner
                            .budget
                            .min_guaranteed_concurrent
                            .max(1)
                            .min(inner.budget.max_concurrent)
                    } else {
                        inner.budget.max_concurrent.max(1)
                    };
                    if let Some(pos) = inner.waiters.iter().position(|w| w.id == self.waiter_id) {
                        inner.waiters.remove(pos);
                        let active = inner.active;
                        let queued = inner.waiters.len();
                        drop(inner);
                        self.state.update_gauges(active, queued, eff);
                        true
                    } else {
                        false
                    }
                };
                if !removed && self.rx.try_recv().is_ok() {
                    self.state.release_slot_with_ceiling(fg_pressure);
                    if self.state.work_class.is_foreground() {
                        self.controller.notify_background_queues_if_unpressured();
                    }
                }
            }
        }

        let mut guard = QueuedWaiterGuard {
            controller: self,
            state: &state,
            waiter_id,
            rx: &mut rx,
            disarmed: false,
        };

        let try_starvation_promote = || -> bool {
            if !work_class.is_background() {
                return false;
            }
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if inner.active >= inner.budget.max_concurrent {
                return false;
            }
            if let Some(pos) = inner.waiters.iter().position(|w| w.id == waiter_id) {
                inner.waiters.remove(pos);
                inner.active += 1;
                inner.starvation_promoted_total += 1;
                let active = inner.active;
                let queued = inner.waiters.len();
                let max_conc = inner.budget.max_concurrent.max(1);
                drop(inner);
                crate::metrics::BACKGROUND_THROTTLED_TOTAL
                    .with_label_values(&[work_class.as_str(), "starvation_promoted"])
                    .inc();
                state.update_gauges(active, queued, max_conc);
                return true;
            }
            false
        };

        let granted = if work_class.is_background() && starvation_aging_ms > 0 {
            let aging_sleep = tokio::time::sleep(Duration::from_millis(starvation_aging_ms));
            tokio::pin!(aging_sleep);
            let mut aging_checked = false;
            loop {
                match ctx {
                    Some(ctx) => {
                        tokio::select! {
                            biased;
                            res = &mut *guard.rx => break res.is_ok(),
                            _ = ctx.wait_cancelled(ExecutionStage::Queue) => break false,
                            () = &mut aging_sleep, if !aging_checked => {
                                aging_checked = true;
                                if try_starvation_promote() {
                                    break true;
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            biased;
                            res = &mut *guard.rx => break res.is_ok(),
                            () = &mut aging_sleep, if !aging_checked => {
                                aging_checked = true;
                                if try_starvation_promote() {
                                    break true;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            match ctx {
                Some(ctx) => {
                    tokio::select! {
                        biased;
                        res = &mut *guard.rx => res.is_ok(),
                        _ = ctx.wait_cancelled(ExecutionStage::Queue) => false,
                    }
                }
                None => (&mut *guard.rx).await.is_ok(),
            }
        };

        if !granted {
            drop(guard);
            let queue_wait = enqueued_at.elapsed();
            if let Some(ctx) = ctx {
                ctx.record_stage_duration(TraceStage::Queue, queue_wait);
                if let Some(deadline) = ctx.deadline() {
                    deadline.charge_queue_time(queue_wait);
                }
            }
            crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                .with_label_values(&[work_class.as_str()])
                .observe(queue_wait.as_secs_f64());
            Self::check_ctx_for_queue(&state, ctx)?;
            record_classified_error(StableErrorClass::Overload, ExecutionStage::Queue);
            return Err(AdmissionError::Cancelled(RequestCancelledError {
                stage: ExecutionStage::Queue,
                reason: CancellationReason::ClientDisconnected,
            }));
        }

        guard.disarmed = true;
        drop(guard);

        let queue_wait = enqueued_at.elapsed();
        if let Some(ctx) = ctx {
            ctx.record_stage_duration(TraceStage::Queue, queue_wait);
            if let Some(deadline) = ctx.deadline() {
                deadline.charge_queue_time(queue_wait);
            }
        }
        crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
            .with_label_values(&[work_class.as_str()])
            .observe(queue_wait.as_secs_f64());

        if let Err(err) = Self::check_ctx_for_queue(&state, ctx) {
            state.release_slot_with_ceiling(self.has_foreground_pressure());
            if work_class.is_foreground() {
                self.notify_background_queues_if_unpressured();
            }
            return Err(err);
        }

        {
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            inner.acquired_total += 1;
        }
        crate::metrics::ADMISSION_ACQUIRED_TOTAL
            .with_label_values(&[work_class.as_str()])
            .inc();

        Ok(AdmissionLease {
            controller: Arc::clone(self),
            state,
            started_at: Instant::now(),
        })
    }
}

pub struct AdmissionLease {
    controller: Arc<AdmissionController>,
    state: Arc<ClassQueueState>,
    started_at: Instant,
}

impl AdmissionLease {
    pub fn work_class(&self) -> WorkClass {
        self.state.work_class
    }
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        crate::metrics::ADMISSION_EXECUTION_SECONDS
            .with_label_values(&[self.state.work_class.as_str()])
            .observe(elapsed);
        let fg_pressure = self.controller.has_foreground_pressure();
        self.state.release_slot_with_ceiling(fg_pressure);
        if self.state.work_class.is_foreground() {
            self.controller.notify_background_queues_if_unpressured();
        }
    }
}

// ── Resource Governor & Byte Reservations (FS-031) ───────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourcePoolKind {
    Memory,
    TempDisk,
    PersistentCache,
    ObjectStoreInflight,
}

impl ResourcePoolKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::TempDisk => "temp_disk",
            Self::PersistentCache => "persistent_cache",
            Self::ObjectStoreInflight => "object_store_inflight",
        }
    }

    pub const ALL: [Self; 4] = [
        Self::Memory,
        Self::TempDisk,
        Self::PersistentCache,
        Self::ObjectStoreInflight,
    ];
}

impl std::fmt::Display for ResourcePoolKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "resource pool [{pool}] exhausted: requested {requested_bytes} bytes, available {available_bytes} bytes (capacity {capacity_bytes} bytes)"
)]
pub struct ResourceExhaustedError {
    pub pool: ResourcePoolKind,
    pub requested_bytes: u64,
    pub available_bytes: u64,
    pub capacity_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePoolSnapshot {
    pub pool: ResourcePoolKind,
    pub capacity_bytes: u64,
    pub reserved_bytes: u64,
    pub peak_reserved_bytes: u64,
    pub granted_total: u64,
    pub rejected_total: u64,
    pub reconciled_total: u64,
}

struct ResourcePoolState {
    pool: ResourcePoolKind,
    capacity_bytes: AtomicU64,
    reserved_bytes: AtomicU64,
    peak_reserved_bytes: AtomicU64,
    granted_total: AtomicU64,
    rejected_total: AtomicU64,
    reconciled_total: AtomicU64,
}

impl ResourcePoolState {
    fn new(pool: ResourcePoolKind, capacity_bytes: u64) -> Self {
        crate::metrics::RESOURCE_POOL_CAPACITY_BYTES
            .with_label_values(&[pool.as_str()])
            .set(i64::try_from(capacity_bytes).unwrap_or(i64::MAX));
        crate::metrics::RESOURCE_POOL_RESERVED_BYTES
            .with_label_values(&[pool.as_str()])
            .set(0);
        Self {
            pool,
            capacity_bytes: AtomicU64::new(capacity_bytes),
            reserved_bytes: AtomicU64::new(0),
            peak_reserved_bytes: AtomicU64::new(0),
            granted_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            reconciled_total: AtomicU64::new(0),
        }
    }

    fn update_reserved_gauge(&self, reserved: u64) {
        crate::metrics::RESOURCE_POOL_RESERVED_BYTES
            .with_label_values(&[self.pool.as_str()])
            .set(i64::try_from(reserved).unwrap_or(i64::MAX));
    }

    fn observe_peak(&self, reserved: u64) {
        self.peak_reserved_bytes
            .fetch_max(reserved, Ordering::AcqRel);
    }

    fn try_add(&self, delta: u64) -> Result<u64, ResourceExhaustedError> {
        let capacity = self.capacity_bytes.load(Ordering::Acquire);
        let mut current = self.reserved_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(delta) else {
                self.rejected_total.fetch_add(1, Ordering::AcqRel);
                crate::metrics::RESOURCE_RESERVATIONS_TOTAL
                    .with_label_values(&[self.pool.as_str(), "rejected"])
                    .inc();
                return Err(ResourceExhaustedError {
                    pool: self.pool,
                    requested_bytes: delta,
                    available_bytes: capacity.saturating_sub(current),
                    capacity_bytes: capacity,
                });
            };
            if next > capacity {
                self.rejected_total.fetch_add(1, Ordering::AcqRel);
                crate::metrics::RESOURCE_RESERVATIONS_TOTAL
                    .with_label_values(&[self.pool.as_str(), "rejected"])
                    .inc();
                return Err(ResourceExhaustedError {
                    pool: self.pool,
                    requested_bytes: delta,
                    available_bytes: capacity.saturating_sub(current),
                    capacity_bytes: capacity,
                });
            }
            match self.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.observe_peak(next);
                    self.update_reserved_gauge(next);
                    return Ok(next);
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut current = self.reserved_bytes.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(bytes);
            match self.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.update_reserved_gauge(next);
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }
}

pub struct ResourceGovernor {
    memory: ResourcePoolState,
    temp_disk: ResourcePoolState,
    persistent_cache: ResourcePoolState,
    object_store_inflight: ResourcePoolState,
}

impl Default for ResourceGovernor {
    fn default() -> Self {
        Self::new(
            2 * 1024 * 1024 * 1024,  // 2 GiB memory reservation ceiling
            8 * 1024 * 1024 * 1024,  // 8 GiB temp disk staging ceiling
            16 * 1024 * 1024 * 1024, // 16 GiB persistent cache ceiling
            2 * 1024 * 1024 * 1024,  // 2 GiB object-store inflight ceiling
        )
    }
}

impl ResourceGovernor {
    pub fn new(
        memory_bytes: u64,
        temp_disk_bytes: u64,
        persistent_cache_bytes: u64,
        object_store_inflight_bytes: u64,
    ) -> Self {
        Self {
            memory: ResourcePoolState::new(ResourcePoolKind::Memory, memory_bytes),
            temp_disk: ResourcePoolState::new(ResourcePoolKind::TempDisk, temp_disk_bytes),
            persistent_cache: ResourcePoolState::new(
                ResourcePoolKind::PersistentCache,
                persistent_cache_bytes,
            ),
            object_store_inflight: ResourcePoolState::new(
                ResourcePoolKind::ObjectStoreInflight,
                object_store_inflight_bytes,
            ),
        }
    }

    fn pool_state(&self, pool: ResourcePoolKind) -> &ResourcePoolState {
        match pool {
            ResourcePoolKind::Memory => &self.memory,
            ResourcePoolKind::TempDisk => &self.temp_disk,
            ResourcePoolKind::PersistentCache => &self.persistent_cache,
            ResourcePoolKind::ObjectStoreInflight => &self.object_store_inflight,
        }
    }

    pub fn set_capacity(&self, pool: ResourcePoolKind, capacity_bytes: u64) {
        let state = self.pool_state(pool);
        state
            .capacity_bytes
            .store(capacity_bytes, Ordering::Release);
        crate::metrics::RESOURCE_POOL_CAPACITY_BYTES
            .with_label_values(&[pool.as_str()])
            .set(i64::try_from(capacity_bytes).unwrap_or(i64::MAX));
    }

    pub fn capacity_bytes(&self, pool: ResourcePoolKind) -> u64 {
        self.pool_state(pool).capacity_bytes.load(Ordering::Acquire)
    }

    pub fn reserved_bytes(&self, pool: ResourcePoolKind) -> u64 {
        self.pool_state(pool).reserved_bytes.load(Ordering::Acquire)
    }

    pub fn peak_reserved_bytes(&self, pool: ResourcePoolKind) -> u64 {
        self.pool_state(pool)
            .peak_reserved_bytes
            .load(Ordering::Acquire)
    }

    pub fn reset_peak_reserved_bytes(&self, pool: ResourcePoolKind) {
        let state = self.pool_state(pool);
        let current = state.reserved_bytes.load(Ordering::Acquire);
        state.peak_reserved_bytes.store(current, Ordering::Release);
    }

    pub fn snapshot(&self, pool: ResourcePoolKind) -> ResourcePoolSnapshot {
        let state = self.pool_state(pool);
        ResourcePoolSnapshot {
            pool,
            capacity_bytes: state.capacity_bytes.load(Ordering::Acquire),
            reserved_bytes: state.reserved_bytes.load(Ordering::Acquire),
            peak_reserved_bytes: state.peak_reserved_bytes.load(Ordering::Acquire),
            granted_total: state.granted_total.load(Ordering::Acquire),
            rejected_total: state.rejected_total.load(Ordering::Acquire),
            reconciled_total: state.reconciled_total.load(Ordering::Acquire),
        }
    }

    pub fn try_reserve(
        self: &Arc<Self>,
        pool: ResourcePoolKind,
        estimated_bytes: u64,
    ) -> Result<ResourceReservation, ResourceExhaustedError> {
        let state = self.pool_state(pool);
        state.try_add(estimated_bytes)?;
        state.granted_total.fetch_add(1, Ordering::AcqRel);
        crate::metrics::RESOURCE_RESERVATIONS_TOTAL
            .with_label_values(&[pool.as_str(), "granted"])
            .inc();
        Ok(ResourceReservation {
            governor: Arc::clone(self),
            pool,
            reserved_bytes: estimated_bytes,
        })
    }
}

pub struct ResourceReservation {
    governor: Arc<ResourceGovernor>,
    pool: ResourcePoolKind,
    reserved_bytes: u64,
}

impl std::fmt::Debug for ResourceReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceReservation")
            .field("pool", &self.pool)
            .field("reserved_bytes", &self.reserved_bytes)
            .finish()
    }
}

impl ResourceReservation {
    pub fn pool(&self) -> ResourcePoolKind {
        self.pool
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Reconcile the initial estimated reservation with the actual measured byte usage.
    /// Releases surplus bytes immediately or reserves the additional delta atomically.
    pub fn reconcile(&mut self, actual_bytes: u64) -> Result<(), ResourceExhaustedError> {
        let state = self.governor.pool_state(self.pool);
        if actual_bytes <= self.reserved_bytes {
            let surplus = self.reserved_bytes - actual_bytes;
            state.release(surplus);
            self.reserved_bytes = actual_bytes;
        } else {
            let delta = actual_bytes - self.reserved_bytes;
            state.try_add(delta)?;
            self.reserved_bytes = actual_bytes;
        }
        state.reconciled_total.fetch_add(1, Ordering::AcqRel);
        crate::metrics::RESOURCE_RESERVATIONS_TOTAL
            .with_label_values(&[self.pool.as_str(), "reconciled"])
            .inc();
        Ok(())
    }

    pub fn release(self) {
        drop(self);
    }
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        if self.reserved_bytes > 0 {
            self.governor
                .pool_state(self.pool)
                .release(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("search worker task panicked during {operation}: {message}")]
pub(crate) struct SearchWorkerPanic {
    operation: &'static str,
    message: String,
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else {
        "non-string panic payload"
    }
}

fn abort_write_panic(operation: &str, payload: &(dyn Any + Send)) -> ! {
    let message = panic_message(payload);
    tracing::error!(
        operation,
        panic = message,
        "write worker task panicked; aborting to avoid serving inconsistent shard state"
    );
    eprintln!(
        "write worker task panicked during {operation}: {message}; aborting to avoid serving inconsistent shard state"
    );
    std::process::abort()
}

/// Run blocking engine maintenance on Tokio's blocking pool.
///
/// Refresh, flush, and force merge may wait on shard-local maintenance locks
/// for a long time. Keeping those waits off the fixed Rayon write pool prevents
/// one shard's maintenance from starving unrelated writes and replica applies.
pub async fn spawn_engine_maintenance<F, R>(
    operation: &'static str,
    f: F,
) -> crate::common::Result<R>
where
    F: FnOnce() -> crate::common::Result<R> + Send + 'static,
    R: Send + 'static,
{
    let ctx = current_request_context();
    if let Some(ref ctx) = ctx {
        ctx.check_active(ExecutionStage::Maintenance)?;
    }
    let ctx_for_blocking = ctx.clone();
    let handle = tokio::task::spawn_blocking(move || {
        let _stage_timer = start_current_stage_timer(TraceStage::Maintenance);
        if let Some(ref ctx) = ctx_for_blocking {
            ctx.check_active(ExecutionStage::Maintenance)?;
        }
        with_thread_request_context(ctx_for_blocking, f)
    });
    match ctx {
        Some(ctx) => {
            tokio::select! {
                biased;
                _ = ctx.wait_cancelled(ExecutionStage::Maintenance) => {
                    ctx.check_active(ExecutionStage::Maintenance)?;
                    Err(RequestCancelledError {
                        stage: ExecutionStage::Maintenance,
                        reason: CancellationReason::ClientDisconnected,
                    }.into())
                }
                res = handle => {
                    res.map_err(|error| anyhow::anyhow!("{operation} blocking task failed: {error}"))?
                }
            }
        }
        None => handle
            .await
            .map_err(|error| anyhow::anyhow!("{operation} blocking task failed: {error}"))?,
    }
}

/// Dedicated thread pools for CPU-bound engine operations, paired with
/// bounded per-class admission queues (`FS-030`) and byte resource governance (`FS-031`).
///
/// Search and write operations run on physically separate OS threads.
/// A burst of bulk indexing physically cannot consume search threads.
#[derive(Clone)]
pub struct WorkerPools {
    search_pool: Arc<rayon::ThreadPool>,
    write_pool: Arc<rayon::ThreadPool>,
    search_size: usize,
    write_size: usize,
    admission: Arc<AdmissionController>,
    resource_governor: Arc<ResourceGovernor>,
    #[cfg(test)]
    write_submissions: Arc<std::sync::atomic::AtomicUsize>,
}

impl WorkerPools {
    /// Create worker pools with explicit sizes.
    pub fn new(search_pool_size: usize, write_pool_size: usize) -> Self {
        let search_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(search_pool_size)
            .thread_name(|i| format!("search-{i}"))
            .panic_handler(|payload| {
                tracing::error!(
                    operation = "search",
                    panic = panic_message(payload.as_ref()),
                    "unhandled search worker panic"
                );
            })
            .build()
            .expect("failed to create search thread pool");

        let write_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(write_pool_size)
            .thread_name(|i| format!("write-{i}"))
            .panic_handler(|payload| abort_write_panic("unhandled write job", payload.as_ref()))
            .build()
            .expect("failed to create write thread pool");

        Self {
            search_pool: Arc::new(search_pool),
            write_pool: Arc::new(write_pool),
            search_size: search_pool_size,
            write_size: write_pool_size,
            admission: Arc::new(AdmissionController::new(search_pool_size, write_pool_size)),
            resource_governor: Arc::new(ResourceGovernor::default()),
            #[cfg(test)]
            write_submissions: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn admission(&self) -> &Arc<AdmissionController> {
        &self.admission
    }

    pub fn resource_governor(&self) -> &Arc<ResourceGovernor> {
        &self.resource_governor
    }

    /// Create worker pools with default sizes based on available CPUs.
    /// Search: `cpus * 3 / 2` (matches OpenSearch default).
    /// Write:  `cpus + 1` (matches OpenSearch default).
    pub fn default_for_system() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let search = (cpus * 3 / 2).max(2);
        let write = (cpus + 1).max(2);
        Self::new(search, write)
    }

    /// Run a blocking closure on the **search** thread pool.
    ///
    /// The closure executes on a dedicated OS thread owned by the search pool.
    /// Observes the active `RequestCancellationContext` and `WorkClass::Search`
    /// admission queue before dispatching and on the worker thread.
    /// Panics become operation errors without terminating the pool or process.
    pub async fn spawn_search<F, R>(&self, f: F) -> crate::common::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_search_in_class(WorkClass::Search, f).await
    }

    /// Run a blocking closure on the **search** thread pool under a specific
    /// `WorkClass` admission queue (e.g. `WorkClass::Search` or `WorkClass::Sql`).
    pub async fn spawn_search_in_class<F, R>(
        &self,
        work_class: WorkClass,
        f: F,
    ) -> crate::common::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let stage = work_class.execution_stage();
        let trace_stage = work_class.trace_stage();
        let ctx = current_request_context();
        let lease = self.admission.acquire(work_class, ctx.as_ref()).await?;
        let pool = self.search_pool.clone();
        let operation = std::any::type_name::<F>();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let worker_ctx = ctx.clone();
        pool.spawn(move || {
            let _lease = lease;
            if let Some(ref worker_ctx) = worker_ctx
                && let Err(error) = worker_ctx.check_active(stage)
            {
                let _ = tx.send(Err(error));
                return;
            }
            let started = Instant::now();
            let ctx_for_timing = worker_ctx.clone();
            let result = catch_unwind(AssertUnwindSafe(|| {
                with_thread_request_context(worker_ctx, f)
            }))
            .map_err(|payload| {
                anyhow::Error::new(SearchWorkerPanic {
                    operation,
                    message: panic_message(payload.as_ref()).to_string(),
                })
            });
            let elapsed = started.elapsed();
            if let Some(ref ctx_for_timing) = ctx_for_timing {
                ctx_for_timing.record_stage_duration(trace_stage, elapsed);
            } else {
                crate::metrics::STAGE_EXECUTION_DURATION_SECONDS
                    .with_label_values(&[trace_stage.as_str()])
                    .observe(elapsed.as_secs_f64());
            }
            let _ = tx.send(result);
        });
        match ctx {
            Some(ctx) => {
                tokio::select! {
                    biased;
                    _ = ctx.wait_cancelled(stage) => {
                        ctx.check_active(stage)?;
                        Err(RequestCancelledError {
                            stage,
                            reason: CancellationReason::ClientDisconnected,
                        }
                        .into())
                    }
                    res = &mut rx => {
                        res.map_err(|_| anyhow::anyhow!("search task was cancelled"))?
                    }
                }
            }
            None => rx
                .await
                .map_err(|_| anyhow::anyhow!("search task was cancelled"))?,
        }
    }

    /// Run a blocking closure on the **write** thread pool.
    ///
    /// The closure executes on a dedicated OS thread owned by the write pool.
    /// Acquires a `WorkClass::Write` admission lease before submitting to the
    /// Rayon write pool and retains the lease until the worker thread finishes.
    /// A panic is logged and aborts the process: unwinding may leave shard
    /// mutation state inconsistent, so returning an error alone is not safe.
    pub async fn spawn_write<F, R>(&self, f: F) -> crate::common::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let ctx = current_request_context();
        let lease = self
            .admission
            .acquire(WorkClass::Write, ctx.as_ref())
            .await?;
        let pool = self.write_pool.clone();
        let operation = std::any::type_name::<F>();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let worker_ctx = ctx;
        pool.spawn(move || {
            let _lease = lease;
            let started = Instant::now();
            let ctx_for_timing = worker_ctx.clone();
            let result = match catch_unwind(AssertUnwindSafe(|| {
                with_thread_request_context(worker_ctx, f)
            })) {
                Ok(result) => result,
                Err(payload) => abort_write_panic(operation, payload.as_ref()),
            };
            let elapsed = started.elapsed();
            if let Some(ref ctx_for_timing) = ctx_for_timing {
                ctx_for_timing.record_stage_duration(TraceStage::Write, elapsed);
            } else {
                crate::metrics::STAGE_EXECUTION_DURATION_SECONDS
                    .with_label_values(&[TraceStage::Write.as_str()])
                    .observe(elapsed.as_secs_f64());
            }
            let _ = tx.send(result);
        });
        #[cfg(test)]
        self.write_submissions
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        rx.await
            .map_err(|_| anyhow::anyhow!("write task was cancelled"))
    }

    /// Run blocking maintenance work under `WorkClass::Maintenance` admission
    /// with foreground-pressure throttling and cooperative yielding (`FS-032`).
    pub async fn spawn_maintenance<F, R>(
        &self,
        operation: &'static str,
        f: F,
    ) -> crate::common::Result<R>
    where
        F: FnOnce() -> crate::common::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let ctx = current_request_context();
        self.admission
            .cooperative_background_yield(WorkClass::Maintenance, ctx.as_ref())
            .await?;
        let _lease = self
            .admission
            .acquire(WorkClass::Maintenance, ctx.as_ref())
            .await?;
        spawn_engine_maintenance(operation, f).await
    }

    #[cfg(test)]
    pub(crate) fn write_submissions_for_test(&self) -> usize {
        self.write_submissions
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn search_pool_size(&self) -> usize {
        self.search_size
    }

    pub fn write_pool_size(&self) -> usize {
        self.write_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn search_and_write_run_on_separate_pools() {
        let pools = WorkerPools::new(2, 2);

        let (search_result, write_result) = tokio::join!(
            pools.spawn_search(|| {
                let name = std::thread::current().name().unwrap_or("").to_string();
                assert!(
                    name.starts_with("search-"),
                    "expected search thread, got {name}"
                );
                42
            }),
            pools.spawn_write(|| {
                let name = std::thread::current().name().unwrap_or("").to_string();
                assert!(
                    name.starts_with("write-"),
                    "expected write thread, got {name}"
                );
                "written"
            }),
        );

        assert_eq!(search_result.unwrap(), 42);
        assert_eq!(write_result.unwrap(), "written");
    }

    #[tokio::test]
    async fn search_pool_isolates_from_write_pool() {
        let pools = WorkerPools::new(1, 1);
        let pools2 = pools.clone();

        // Block the single write thread
        let write_handle = tokio::spawn(async move {
            pools2
                .spawn_write(|| {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    "slow_write"
                })
                .await
        });

        // Search should still complete immediately on its own thread
        let search_start = std::time::Instant::now();
        let search_result = pools.spawn_search(|| "fast_search").await;
        let search_elapsed = search_start.elapsed();

        assert_eq!(search_result.unwrap(), "fast_search");
        assert!(
            search_elapsed.as_millis() < 50,
            "search took {}ms — should not be blocked by write",
            search_elapsed.as_millis()
        );

        let write_result = write_handle.await.unwrap();
        assert_eq!(write_result.unwrap(), "slow_write");
    }

    #[tokio::test]
    async fn default_pool_sizes_are_reasonable() {
        let pools = WorkerPools::default_for_system();
        assert!(pools.search_pool_size() >= 2);
        assert!(pools.write_pool_size() >= 2);
    }

    #[tokio::test]
    async fn engine_maintenance_preserves_operation_errors() {
        let error = spawn_engine_maintenance("refresh", || {
            Err::<(), _>(anyhow::anyhow!("refresh failed"))
        })
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "refresh failed");
    }

    #[tokio::test]
    async fn fs028_request_cancellation_propagates_to_children_and_releases_resources() {
        let parent = RequestCancellationContext::new();
        let child_search = parent.child();
        let child_hydration = parent.child();

        let governor = Arc::new(ResourceGovernor::new(1024, 2048, 4096, 1024));
        let mem_res = governor.try_reserve(ResourcePoolKind::Memory, 512).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let staged_file = temp_dir.path().join("hydration.tmp");
        std::fs::write(&staged_file, b"partial-split").unwrap();
        let staged_file_for_hook = staged_file.clone();
        child_hydration.on_cancel(move || {
            let _ = std::fs::remove_file(&staged_file_for_hook);
            drop(mem_res);
        });

        assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 512);
        assert!(staged_file.exists());

        parent.cancel(CancellationReason::ClientDisconnected);

        assert!(parent.is_cancelled());
        assert!(child_search.is_cancelled());
        assert!(child_hydration.is_cancelled());
        assert!(
            !staged_file.exists(),
            "on_cancel hook must remove staged temp file"
        );
        assert_eq!(
            governor.reserved_bytes(ResourcePoolKind::Memory),
            0,
            "on_cancel hook must release byte reservation immediately"
        );
        let err = child_search
            .check_active(ExecutionStage::Search)
            .unwrap_err();
        assert!(err.is::<RequestCancelledError>());
    }

    #[tokio::test]
    async fn fs029_deadline_propagation_clamps_children_charges_queue_and_records_stage() {
        let parent = RequestCancellationContext::with_budget(Duration::from_millis(250));
        let child = parent.child_with_budget(Duration::from_secs(10));
        assert!(
            child.remaining_budget().unwrap() <= Duration::from_millis(250),
            "nested child budget must never outlive parent budget"
        );

        let mut grpc_req = tonic::Request::new(());
        crate::transport::state_wait::attach_context_deadline_metadata(
            &mut grpc_req,
            &child,
            ExecutionStage::Search,
        )
        .unwrap();
        assert!(
            grpc_req
                .metadata()
                .contains_key(crate::transport::state_wait::REMAINING_BUDGET_MS_HEADER)
        );
        assert!(
            grpc_req
                .metadata()
                .contains_key(crate::transport::state_wait::DEADLINE_EPOCH_MS_HEADER)
        );
        assert!(grpc_req.metadata().contains_key("grpc-timeout"));

        let (extracted, mut guard) = crate::transport::state_wait::extract_grpc_request_context(
            &grpc_req,
            ExecutionStage::Search,
        )
        .unwrap();
        assert!(extracted.remaining_budget().unwrap() <= Duration::from_millis(250));
        guard.disarm();

        // Expired budget on wire is rejected immediately and records the exhausted stage
        let before = crate::metrics::DEADLINE_EXHAUSTED_TOTAL
            .with_label_values(&["hydration"])
            .get();
        let mut expired_req = tonic::Request::new(());
        expired_req.metadata_mut().insert(
            crate::transport::state_wait::REMAINING_BUDGET_MS_HEADER,
            "0".parse().unwrap(),
        );
        let status = crate::transport::state_wait::extract_grpc_request_context(
            &expired_req,
            ExecutionStage::Hydration,
        )
        .err()
        .expect("zero remaining budget must fail with DEADLINE_EXCEEDED");
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        let after = crate::metrics::DEADLINE_EXHAUSTED_TOTAL
            .with_label_values(&["hydration"])
            .get();
        assert_eq!(after, before + 1);
    }

    #[tokio::test]
    async fn fs030_bounded_admission_queues_enforce_overload_fifo_cancellation_and_deadlines() {
        let admission = Arc::new(AdmissionController::new(2, 2));
        admission.set_limits(
            WorkClass::Search,
            WorkClassLimits {
                max_concurrent: 2,
                max_queue_depth: 2,
            },
        );

        let lease1 = admission.acquire(WorkClass::Search, None).await.unwrap();
        let lease2 = admission.acquire(WorkClass::Search, None).await.unwrap();
        assert_eq!(admission.snapshot(WorkClass::Search).active, 2);
        assert_eq!(admission.snapshot(WorkClass::Search).queued, 0);

        let order = Arc::new(Mutex::new(Vec::new()));

        // Enqueue waiter 1 (will be cancelled while queued)
        let cancel_ctx = RequestCancellationContext::new();
        let adm_c1 = admission.clone();
        let cancel_ctx_clone = cancel_ctx.clone();
        let w1 = tokio::spawn(async move {
            adm_c1
                .acquire(WorkClass::Search, Some(&cancel_ctx_clone))
                .await
        });

        // Wait until w1 is queued
        while admission.snapshot(WorkClass::Search).queued < 1 {
            tokio::task::yield_now().await;
        }

        // Enqueue waiter 2 (normal waiter, should get first freed slot in FIFO order)
        let adm_c2 = admission.clone();
        let order_c2 = order.clone();
        let w2 = tokio::spawn(async move {
            let lease = adm_c2.acquire(WorkClass::Search, None).await.unwrap();
            order_c2.lock().unwrap().push("w2");
            lease
        });

        while admission.snapshot(WorkClass::Search).queued < 2 {
            tokio::task::yield_now().await;
        }

        // Queue is now full (active=2, queued=2): 5th arrival must be rejected immediately!
        let overload = admission
            .acquire(WorkClass::Search, None)
            .await
            .err()
            .expect("5th request must fail when queue is full");
        assert!(matches!(overload, AdmissionError::QueueFull { .. }));
        let grpc_status = crate::transport::state_wait::status_from_worker_error(
            &anyhow::Error::new(overload),
            ExecutionStage::Search,
        );
        assert_eq!(grpc_status.code(), tonic::Code::ResourceExhausted);

        // Cancel waiter 1 while queued -> must immediately remove from queue!
        cancel_ctx.cancel(CancellationReason::ClientDisconnected);
        let w1_res = w1.await.unwrap();
        assert!(matches!(w1_res, Err(AdmissionError::Cancelled(_))));
        assert_eq!(admission.snapshot(WorkClass::Search).queued, 1);

        // Enqueue waiter 3 with a short deadline that expires while queued
        let short_ctx = RequestCancellationContext::with_budget(Duration::from_millis(20));
        let w3_res = admission.acquire(WorkClass::Search, Some(&short_ctx)).await;
        assert!(matches!(w3_res, Err(AdmissionError::DeadlineExceeded(_))));
        assert!(short_ctx.deadline().unwrap().charged_queue_time() >= Duration::from_millis(15));
        assert_eq!(admission.snapshot(WorkClass::Search).queued, 1);

        // Release lease1 -> w2 must wake up and acquire the slot!
        drop(lease1);
        let lease_w2 = w2.await.unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["w2"]);
        drop(lease_w2);
        drop(lease2);
        assert_eq!(admission.snapshot(WorkClass::Search).active, 0);
        assert_eq!(admission.snapshot(WorkClass::Search).queued, 0);
    }

    #[tokio::test]
    async fn fs031_resource_governor_reserves_reconciles_rejects_oversized_and_survives_stress() {
        let governor = Arc::new(ResourceGovernor::new(1024, 2048, 4096, 512));

        // 1. Oversized reservation is rejected before allocation
        let err = governor
            .try_reserve(ResourcePoolKind::ObjectStoreInflight, 1024)
            .unwrap_err();
        assert_eq!(err.pool, ResourcePoolKind::ObjectStoreInflight);
        assert_eq!(err.requested_bytes, 1024);
        assert_eq!(err.capacity_bytes, 512);
        assert_eq!(
            governor.reserved_bytes(ResourcePoolKind::ObjectStoreInflight),
            0
        );

        // 2. Estimate -> reconcile downward -> reconcile upward -> drop releases all bytes
        {
            let mut res = governor.try_reserve(ResourcePoolKind::Memory, 800).unwrap();
            assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 800);
            res.reconcile(300).unwrap();
            assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 300);
            res.reconcile(900).unwrap();
            assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 900);
            assert!(res.reconcile(1200).is_err());
            assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 900);
        }
        assert_eq!(governor.reserved_bytes(ResourcePoolKind::Memory), 0);

        // 3. Concurrent stress across all 4 pools stays within configured capacities
        let mut tasks = Vec::new();
        for i in 0..32usize {
            let gov = governor.clone();
            tasks.push(tokio::spawn(async move {
                let pool = ResourcePoolKind::ALL[i % 4];
                for step in 0..25u64 {
                    let req = ((i as u64 % 7) + 1) * 64 + (step % 5) * 32;
                    if let Ok(mut guard) = gov.try_reserve(pool, req) {
                        tokio::task::yield_now().await;
                        let actual = req / 2 + (step % 3) * 16;
                        let _ = guard.reconcile(actual);
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        for pool in ResourcePoolKind::ALL {
            assert_eq!(
                governor.reserved_bytes(pool),
                0,
                "pool {pool} leaked bytes after stress"
            );
            assert!(
                governor.peak_reserved_bytes(pool) <= governor.capacity_bytes(pool),
                "pool {pool} peak {} exceeded capacity {}",
                governor.peak_reserved_bytes(pool),
                governor.capacity_bytes(pool)
            );
        }
    }

    #[tokio::test]
    async fn fs032_background_work_prioritization_throttles_under_foreground_load_and_prevents_starvation()
     {
        let admission = Arc::new(AdmissionController::new(2, 2));
        admission.set_limits(
            WorkClass::Search,
            WorkClassLimits {
                max_concurrent: 2,
                max_queue_depth: 8,
            },
        );
        admission.set_budget(
            WorkClass::Maintenance,
            WorkClassBudget {
                max_concurrent: 3,
                min_guaranteed_concurrent: 1,
                max_queue_depth: 8,
                max_byte_budget: 16 * 1024 * 1024,
                io_bandwidth_bytes_per_sec: 1_000_000,
                starvation_aging_ms: 40,
                yield_backoff_ms: 2,
            },
        );

        assert_eq!(
            WorkClass::Search.priority_tier(),
            WorkPriorityTier::Foreground
        );
        assert_eq!(
            WorkClass::Maintenance.priority_tier(),
            WorkPriorityTier::Background
        );
        assert!(!admission.has_foreground_pressure());

        // Acquire 1 foreground Search slot -> utilization is 1/2 (50%), triggering foreground pressure!
        let fg_lease = admission.acquire(WorkClass::Search, None).await.unwrap();
        assert!(admission.has_foreground_pressure());

        // Under foreground pressure, Maintenance effective concurrency is clamped to min_guaranteed_concurrent (1).
        let bg_snap_initial = admission.snapshot(WorkClass::Maintenance);
        assert_eq!(bg_snap_initial.effective_max_concurrent, 1);
        assert_eq!(bg_snap_initial.max_concurrent, 3);

        // First background task acquires the 1 guaranteed slot immediately.
        let bg_lease_1 = admission
            .acquire(WorkClass::Maintenance, None)
            .await
            .unwrap();
        let bg_snap_busy = admission.snapshot(WorkClass::Maintenance);
        assert_eq!(bg_snap_busy.active, 1);
        assert!((bg_snap_busy.saturation_ratio - 1.0).abs() < f64::EPSILON);

        // Second background task arrives while foreground pressure is active:
        // It is throttled by foreground pressure, then promoted after starvation_aging_ms (40ms)
        // even though fg_lease and bg_lease_1 are still held!
        let adm_for_bg2 = admission.clone();
        let bg2_start = Instant::now();
        let bg_lease_2 = tokio::spawn(async move {
            adm_for_bg2
                .acquire(WorkClass::Maintenance, None)
                .await
                .unwrap()
        })
        .await
        .unwrap();
        let bg2_elapsed = bg2_start.elapsed();
        assert!(
            bg2_elapsed >= Duration::from_millis(30),
            "expected background task 2 to be throttled until starvation aging (took {bg2_elapsed:?})"
        );
        let bg_snap_promoted = admission.snapshot(WorkClass::Maintenance);
        assert_eq!(bg_snap_promoted.active, 2);
        assert!(bg_snap_promoted.throttled_total >= 1);
        assert!(bg_snap_promoted.starvation_promoted_total >= 1);

        // Cooperative yield and I/O byte accounting under foreground pressure:
        let yielded = admission
            .cooperative_background_yield(WorkClass::Maintenance, None)
            .await
            .unwrap();
        assert!(
            yielded,
            "background work must yield under foreground pressure"
        );
        admission
            .account_io_bytes(WorkClass::Maintenance, 4096, None)
            .await
            .unwrap();
        assert_eq!(
            admission.snapshot(WorkClass::Maintenance).io_bytes_total,
            4096
        );

        // Release foreground pressure -> effective ceiling returns to max_concurrent (3)
        drop(fg_lease);
        assert!(!admission.has_foreground_pressure());
        assert_eq!(
            admission
                .snapshot(WorkClass::Maintenance)
                .effective_max_concurrent,
            3
        );
        drop(bg_lease_1);
        drop(bg_lease_2);
        assert_eq!(admission.snapshot(WorkClass::Maintenance).active, 0);
    }

    #[tokio::test]
    async fn fs033_distributed_tracing_stage_ledger_and_stable_error_classes() {
        // 1. Secret and malformed trace IDs are rejected
        assert_eq!(
            sanitize_trace_id("req-valid-123"),
            Some("req-valid-123".into())
        );
        assert_eq!(sanitize_trace_id("Bearer secret-jwt"), None);
        assert_eq!(sanitize_trace_id("ApiKey my-secret-key"), None);
        assert_eq!(sanitize_trace_id("has\nnewline"), None);
        assert_eq!(sanitize_trace_id(&"x".repeat(200)), None);

        // 2. Correlation IDs and stage timing ledger propagate across child contexts and gRPC metadata
        let ctx = RequestCancellationContext::with_budget(Duration::from_secs(5));
        assert!(ctx.set_request_id("req-trace-42"));
        assert!(ctx.set_operation_id("op-publish-7"));
        assert!(ctx.set_snapshot_id("snap-uuid-g3"));
        assert!(ctx.set_assignment_id("assign-g3-r0-node1"));
        assert!(ctx.set_task_id("task-fm-9"));

        ctx.record_stage_duration(TraceStage::Queue, Duration::from_micros(200));
        let child = ctx.child();
        child.record_stage_duration(TraceStage::Hydration, Duration::from_micros(800));
        child.record_stage_duration(TraceStage::Storage, Duration::from_micros(500));
        child.record_stage_duration(TraceStage::Search, Duration::from_micros(400));
        child.record_stage_duration(TraceStage::Retry, Duration::from_micros(100));
        child.record_stage_duration(TraceStage::PartialMerge, Duration::from_micros(150));

        let snap = ctx.stage_snapshot();
        assert!(snap.queue_ns >= 200_000);
        assert!(snap.hydration_ns >= 800_000);
        assert!(snap.storage_ns >= 500_000);
        assert!(snap.search_ns >= 400_000);
        assert!(snap.retry_ns >= 100_000);
        assert!(snap.partial_merge_ns >= 150_000);

        let encoded = snap.encode_compact();
        let decoded = StageTimingSnapshot::decode_compact(&encoded).unwrap();
        assert_eq!(decoded, snap);

        let mut grpc_req = tonic::Request::new(());
        crate::transport::state_wait::attach_context_deadline_metadata(
            &mut grpc_req,
            &child,
            ExecutionStage::Search,
        )
        .unwrap();
        let (extracted, mut guard) = crate::transport::state_wait::extract_grpc_request_context(
            &grpc_req,
            ExecutionStage::Search,
        )
        .unwrap();
        guard.disarm();
        let extracted_ids = extracted.trace_ids();
        assert_eq!(extracted_ids.request_id, "req-trace-42");
        assert_eq!(extracted_ids.operation_id.as_deref(), Some("op-publish-7"));
        assert_eq!(extracted_ids.snapshot_id.as_deref(), Some("snap-uuid-g3"));
        assert_eq!(
            extracted_ids.assignment_id.as_deref(),
            Some("assign-g3-r0-node1")
        );
        assert_eq!(extracted_ids.task_id.as_deref(), Some("task-fm-9"));

        // 3. StableErrorClass classifies overload, corruption, retryable, and terminal errors
        assert_eq!(
            classify_http_error(429, "rejected_execution_exception", "queue full"),
            StableErrorClass::Overload
        );
        assert_eq!(
            classify_http_error(503, "shard_not_available_exception", "primary promoting"),
            StableErrorClass::Retryable
        );
        assert_eq!(
            classify_http_error(
                500,
                "remote_store_manifest_exception",
                "bundle checksum mismatch"
            ),
            StableErrorClass::Corruption
        );
        assert_eq!(
            classify_http_error(400, "mapper_parsing_exception", "invalid field"),
            StableErrorClass::Terminal
        );
    }
}
