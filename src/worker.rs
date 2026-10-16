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

struct RequestCancellationState {
    id: u64,
    cancelled: AtomicBool,
    reason: Mutex<Option<CancellationReason>>,
    notify: tokio::sync::Notify,
    deadline: Option<RequestDeadline>,
    children: Mutex<Vec<Weak<RequestCancellationState>>>,
    cleanup_hooks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    metric_recorded: AtomicBool,
}

static NEXT_CANCELLATION_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Request-scoped cancellation and deadline context (`FS-028`, `FS-029`).
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
            .field("is_cancelled", &self.is_cancelled())
            .field("deadline", &self.inner.deadline)
            .finish()
    }
}

impl RequestCancellationContext {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RequestCancellationState {
                id: NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: None,
                children: Mutex::new(Vec::new()),
                cleanup_hooks: Mutex::new(Vec::new()),
                metric_recorded: AtomicBool::new(false),
            }),
        }
    }

    pub fn with_deadline(deadline: RequestDeadline) -> Self {
        Self {
            inner: Arc::new(RequestCancellationState {
                id: NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: Some(deadline),
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
        let child = Self {
            inner: Arc::new(RequestCancellationState {
                id: NEXT_CANCELLATION_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
                notify: tokio::sync::Notify::new(),
                deadline: child_deadline,
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

// ── Bounded Admission Queues (FS-030) ────────────────────────────────────────

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionClassSnapshot {
    pub work_class: WorkClass,
    pub active: usize,
    pub queued: usize,
    pub max_concurrent: usize,
    pub max_queue_depth: usize,
    pub acquired_total: u64,
    pub rejected_queue_full: u64,
    pub rejected_cancelled: u64,
    pub rejected_deadline: u64,
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
    tx: tokio::sync::oneshot::Sender<()>,
    ctx: Option<RequestCancellationContext>,
}

struct ClassQueueInner {
    limits: WorkClassLimits,
    active: usize,
    next_waiter_id: u64,
    waiters: VecDeque<QueuedWaiter>,
    acquired_total: u64,
    rejected_queue_full: u64,
    rejected_cancelled: u64,
    rejected_deadline: u64,
}

struct ClassQueueState {
    work_class: WorkClass,
    inner: Mutex<ClassQueueInner>,
}

impl ClassQueueState {
    fn new(work_class: WorkClass, limits: WorkClassLimits) -> Self {
        Self {
            work_class,
            inner: Mutex::new(ClassQueueInner {
                limits: WorkClassLimits {
                    max_concurrent: limits.max_concurrent.max(1),
                    max_queue_depth: limits.max_queue_depth,
                },
                active: 0,
                next_waiter_id: 1,
                waiters: VecDeque::new(),
                acquired_total: 0,
                rejected_queue_full: 0,
                rejected_cancelled: 0,
                rejected_deadline: 0,
            }),
        }
    }

    fn update_gauges(&self, active: usize, queued: usize) {
        crate::metrics::ADMISSION_EXECUTING_GAUGE
            .with_label_values(&[self.work_class.as_str()])
            .set(active as i64);
        crate::metrics::ADMISSION_QUEUED_GAUGE
            .with_label_values(&[self.work_class.as_str()])
            .set(queued as i64);
    }

    fn release_slot(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        while let Some(waiter) = inner.waiters.pop_front() {
            if let Some(ref ctx) = waiter.ctx
                && ctx.is_cancelled()
            {
                continue;
            }
            if inner.active <= inner.limits.max_concurrent && waiter.tx.send(()).is_ok() {
                let active = inner.active;
                let queued = inner.waiters.len();
                drop(inner);
                self.update_gauges(active, queued);
                return;
            }
        }
        inner.active = inner.active.saturating_sub(1);
        let active = inner.active;
        let queued = inner.waiters.len();
        drop(inner);
        self.update_gauges(active, queued);
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

    pub fn set_limits(&self, work_class: WorkClass, limits: WorkClassLimits) {
        let state = self.state_for(work_class);
        let mut inner = state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        inner.limits = WorkClassLimits {
            max_concurrent: limits.max_concurrent.max(1),
            max_queue_depth: limits.max_queue_depth,
        };
    }

    pub fn limits(&self, work_class: WorkClass) -> WorkClassLimits {
        self.state_for(work_class)
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .limits
    }

    pub fn snapshot(&self, work_class: WorkClass) -> AdmissionClassSnapshot {
        let inner = self
            .state_for(work_class)
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        AdmissionClassSnapshot {
            work_class,
            active: inner.active,
            queued: inner.waiters.len(),
            max_concurrent: inner.limits.max_concurrent,
            max_queue_depth: inner.limits.max_queue_depth,
            acquired_total: inner.acquired_total,
            rejected_queue_full: inner.rejected_queue_full,
            rejected_cancelled: inner.rejected_cancelled,
            rejected_deadline: inner.rejected_deadline,
        }
    }

    pub fn total_queued(&self) -> usize {
        WorkClass::ALL
            .into_iter()
            .map(|class| self.snapshot(class).queued)
            .sum()
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
                return Err(AdmissionError::DeadlineExceeded(deadline.clone()));
            }
            if let Some(cancelled) = error.downcast_ref::<RequestCancelledError>() {
                inner.rejected_cancelled += 1;
                drop(inner);
                crate::metrics::ADMISSION_REJECTED_TOTAL
                    .with_label_values(&[state.work_class.as_str(), "cancelled"])
                    .inc();
                return Err(AdmissionError::Cancelled(cancelled.clone()));
            }
        }
        Ok(())
    }

    pub fn try_acquire(
        &self,
        work_class: WorkClass,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<AdmissionLease, AdmissionError> {
        let state = self.state_for(work_class).clone();
        Self::check_ctx_for_queue(&state, ctx)?;
        let mut inner = state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if inner.active < inner.limits.max_concurrent && inner.waiters.is_empty() {
            inner.active += 1;
            inner.acquired_total += 1;
            let active = inner.active;
            let queued = inner.waiters.len();
            drop(inner);
            state.update_gauges(active, queued);
            crate::metrics::ADMISSION_ACQUIRED_TOTAL
                .with_label_values(&[work_class.as_str()])
                .inc();
            crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                .with_label_values(&[work_class.as_str()])
                .observe(0.0);
            return Ok(AdmissionLease {
                state,
                started_at: Instant::now(),
            });
        }
        inner.rejected_queue_full += 1;
        let err = AdmissionError::QueueFull {
            work_class,
            active: inner.active,
            max_concurrent: inner.limits.max_concurrent,
            queued: inner.waiters.len(),
            max_queue_depth: inner.limits.max_queue_depth,
        };
        drop(inner);
        crate::metrics::ADMISSION_REJECTED_TOTAL
            .with_label_values(&[work_class.as_str(), "queue_full"])
            .inc();
        Err(err)
    }

    pub async fn acquire(
        &self,
        work_class: WorkClass,
        ctx: Option<&RequestCancellationContext>,
    ) -> Result<AdmissionLease, AdmissionError> {
        let state = self.state_for(work_class).clone();
        Self::check_ctx_for_queue(&state, ctx)?;

        let enqueued_at = Instant::now();
        let (waiter_id, mut rx) = {
            let mut inner = state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if inner.active < inner.limits.max_concurrent && inner.waiters.is_empty() {
                inner.active += 1;
                inner.acquired_total += 1;
                let active = inner.active;
                let queued = inner.waiters.len();
                drop(inner);
                state.update_gauges(active, queued);
                crate::metrics::ADMISSION_ACQUIRED_TOTAL
                    .with_label_values(&[work_class.as_str()])
                    .inc();
                crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                    .with_label_values(&[work_class.as_str()])
                    .observe(0.0);
                return Ok(AdmissionLease {
                    state,
                    started_at: Instant::now(),
                });
            }
            if inner.waiters.len() >= inner.limits.max_queue_depth {
                inner.rejected_queue_full += 1;
                let err = AdmissionError::QueueFull {
                    work_class,
                    active: inner.active,
                    max_concurrent: inner.limits.max_concurrent,
                    queued: inner.waiters.len(),
                    max_queue_depth: inner.limits.max_queue_depth,
                };
                drop(inner);
                crate::metrics::ADMISSION_REJECTED_TOTAL
                    .with_label_values(&[work_class.as_str(), "queue_full"])
                    .inc();
                return Err(err);
            }
            let waiter_id = inner.next_waiter_id;
            inner.next_waiter_id += 1;
            let (tx, rx) = tokio::sync::oneshot::channel();
            inner.waiters.push_back(QueuedWaiter {
                id: waiter_id,
                tx,
                ctx: ctx.cloned(),
            });
            let active = inner.active;
            let queued = inner.waiters.len();
            drop(inner);
            state.update_gauges(active, queued);
            (waiter_id, rx)
        };

        struct QueuedWaiterGuard<'a> {
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
                let removed = {
                    let mut inner = self
                        .state
                        .inner
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if let Some(pos) = inner.waiters.iter().position(|w| w.id == self.waiter_id) {
                        inner.waiters.remove(pos);
                        let active = inner.active;
                        let queued = inner.waiters.len();
                        drop(inner);
                        self.state.update_gauges(active, queued);
                        true
                    } else {
                        false
                    }
                };
                if !removed && self.rx.try_recv().is_ok() {
                    self.state.release_slot();
                }
            }
        }

        let mut guard = QueuedWaiterGuard {
            state: &state,
            waiter_id,
            rx: &mut rx,
            disarmed: false,
        };

        let granted = match ctx {
            Some(ctx) => {
                tokio::select! {
                    biased;
                    res = &mut *guard.rx => res.is_ok(),
                    _ = ctx.wait_cancelled(ExecutionStage::Queue) => false,
                }
            }
            None => (&mut *guard.rx).await.is_ok(),
        };

        if !granted {
            drop(guard);
            let queue_wait = enqueued_at.elapsed();
            if let Some(ctx) = ctx
                && let Some(deadline) = ctx.deadline()
            {
                deadline.charge_queue_time(queue_wait);
            }
            crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
                .with_label_values(&[work_class.as_str()])
                .observe(queue_wait.as_secs_f64());
            Self::check_ctx_for_queue(&state, ctx)?;
            return Err(AdmissionError::Cancelled(RequestCancelledError {
                stage: ExecutionStage::Queue,
                reason: CancellationReason::ClientDisconnected,
            }));
        }

        guard.disarmed = true;
        drop(guard);

        let queue_wait = enqueued_at.elapsed();
        if let Some(ctx) = ctx
            && let Some(deadline) = ctx.deadline()
        {
            deadline.charge_queue_time(queue_wait);
        }
        crate::metrics::ADMISSION_QUEUE_WAIT_SECONDS
            .with_label_values(&[work_class.as_str()])
            .observe(queue_wait.as_secs_f64());

        if let Err(err) = Self::check_ctx_for_queue(&state, ctx) {
            state.release_slot();
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
            state,
            started_at: Instant::now(),
        })
    }
}

pub struct AdmissionLease {
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
        self.state.release_slot();
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
            let result = catch_unwind(AssertUnwindSafe(|| {
                with_thread_request_context(worker_ctx, f)
            }))
            .map_err(|payload| {
                anyhow::Error::new(SearchWorkerPanic {
                    operation,
                    message: panic_message(payload.as_ref()).to_string(),
                })
            });
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
            let result = match catch_unwind(AssertUnwindSafe(|| {
                with_thread_request_context(worker_ctx, f)
            })) {
                Ok(result) => result,
                Err(payload) => abort_write_panic(operation, payload.as_ref()),
            };
            let _ = tx.send(result);
        });
        #[cfg(test)]
        self.write_submissions
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        rx.await
            .map_err(|_| anyhow::anyhow!("write task was cancelled"))
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
}
