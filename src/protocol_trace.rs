use crate::cluster::state::ClusterState;
use crate::engine::{ApplyOutcome, DocumentMutation, SequenceStats, SequencedOperation};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

pub const SCHEMA: &str = "ferrissearch.d1.trace/v4";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceNode {
    pub node: String,
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceStartCopy {
    pub node: String,
    pub allocation: u64,
    pub exists: bool,
    pub fence_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceStartShard {
    pub index_uuid: String,
    pub shard: u32,
    pub primary: String,
    pub term: u64,
    pub activated: bool,
    pub in_sync: Vec<String>,
    pub copies: Vec<TraceStartCopy>,
}

#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub output: PathBuf,
    pub run_id: String,
    pub test: String,
    pub durability: &'static str,
    pub nodes: Vec<TraceNode>,
    pub shard_state: TraceStartShard,
    pub mutation: MutationMode,
    pub faults: Vec<FaultRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationMode {
    None,
    ArrivalOrderApply,
    SeqOnlyRedelivery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultAction {
    DelayRequest { millis: u64 },
    HoldRequestUntilApplied { seq_no: u64 },
    DropRequest,
    DropResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultRule {
    pub target: String,
    pub seq_no: u64,
    pub action: FaultAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TraceCopy {
    pub node: String,
    pub index_uuid: String,
    pub shard: u32,
    pub allocation: u64,
}

#[derive(Debug, Clone)]
pub struct TraceCopySnapshot {
    pub copy: TraceCopy,
    pub live_documents: Vec<(String, u64, u64, String)>,
    pub actual_documents: Vec<TraceActualDocument>,
    pub wal_entries: Vec<TraceWalEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceActualDocument {
    pub doc: String,
    pub state: &'static str,
    pub seq_no: Option<u64>,
    pub term: Option<u64>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceWalEntry {
    pub seq_no: u64,
    pub term: u64,
    pub doc: Option<String>,
    pub op: &'static str,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize)]
struct TraceActualCopy {
    copy: TraceCopy,
    documents: Vec<TraceActualDocument>,
    wal: Vec<TraceWalEntry>,
}

#[derive(Debug, Clone)]
pub struct RecoveryTraceContext {
    pub source_node: String,
    pub target_node: String,
    pub index_uuid: String,
    pub shard: u32,
    pub allocation: u64,
    pub session_id: String,
    pub snapshot_next_seq_no: u64,
}

#[derive(Debug, Clone)]
struct RecoverySnapshotState {
    documents: BTreeMap<String, LogicalDocument>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationKey {
    pub index_uuid: String,
    pub shard: u32,
    pub term: u64,
    pub seq_no: u64,
}

impl OperationKey {
    pub fn new(index_uuid: impl Into<String>, shard: u32, term: u64, seq_no: u64) -> Self {
        Self {
            index_uuid: index_uuid.into(),
            shard,
            term,
            seq_no,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestToken {
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceMessage {
    pub message_id: String,
    pub key: OperationKey,
    pub source: String,
    pub source_incarnation: u64,
    pub target: String,
    pub target_incarnation: u64,
    pub target_allocation: u64,
    pub batch_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOrigin {
    Primary,
    LiveReplication,
    Recovery,
    Replay,
    Promotion,
}

#[derive(Debug, Clone)]
struct ApplyScope {
    origin: ApplyOrigin,
    operations: Vec<SequencedOperation>,
}

thread_local! {
    static OPEN_COPY: RefCell<Option<TraceCopy>> = const { RefCell::new(None) };
    static APPLY_SCOPE: RefCell<Option<ApplyScope>> = const { RefCell::new(None) };
    static REQUEST_SCOPE: RefCell<VecDeque<RequestToken>> = const { RefCell::new(VecDeque::new()) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestStatus {
    Routed,
    Replicating,
    Acked,
    Failed,
}

#[derive(Debug, Clone)]
struct RequestState {
    target: String,
    doc: String,
    op: String,
    content_hash: String,
    status: RequestStatus,
    primary: Option<String>,
}

#[derive(Debug, Clone)]
struct OperationState {
    request_id: Option<String>,
    receipt_id: String,
    doc: Option<String>,
    op: String,
    content_hash: String,
    batch_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessagePhase {
    Request,
    Ack,
    Nack,
}

impl MessagePhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Ack => "ack",
            Self::Nack => "nack",
        }
    }
}

#[derive(Debug, Clone)]
struct MessageState {
    message: TraceMessage,
    phase: Option<MessagePhase>,
}

#[derive(Debug, Clone)]
struct ActiveReplay {
    replay_id: String,
    ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogicalDocument {
    state: &'static str,
    seq_no: u64,
    term: u64,
    content_hash: String,
}

#[derive(Debug, Clone)]
struct FaultState {
    rule: FaultRule,
    used: bool,
}

struct TraceState {
    output: PathBuf,
    run_id: String,
    next_step: u64,
    records: Vec<Value>,
    mutation: MutationMode,
    next_request: u64,
    next_message: u64,
    next_batch: u64,
    next_replay: u64,
    next_commit: u64,
    nodes: HashMap<String, u64>,
    restart_pending: HashSet<String>,
    requests: HashMap<String, RequestState>,
    request_order: Vec<String>,
    operations: HashMap<OperationKey, OperationState>,
    messages: HashMap<String, MessageState>,
    message_by_copy_operation: HashMap<(OperationKey, String), String>,
    faults: Vec<FaultState>,
    replays: HashMap<TraceCopy, ActiveReplay>,
    pending_commits: HashMap<TraceCopy, String>,
    recovery_snapshots: HashMap<String, RecoverySnapshotState>,
    known_documents: BTreeSet<String>,
    copy_documents: HashMap<TraceCopy, BTreeMap<String, LogicalDocument>>,
    actual_copies: HashMap<String, TraceActualCopy>,
    applied_operations: BTreeSet<(String, u64)>,
    applied_notify: std::sync::Arc<tokio::sync::Notify>,
    omit_wal_append_node: Option<String>,
    wal_append_omitted: bool,
}

static TRACE_STATE: OnceLock<Mutex<Option<TraceState>>> = OnceLock::new();

fn trace_state() -> &'static Mutex<Option<TraceState>> {
    TRACE_STATE.get_or_init(|| Mutex::new(None))
}

fn lock_trace_state() -> MutexGuard<'static, Option<TraceState>> {
    trace_state()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

pub struct TraceSession {
    run_id: String,
    finished: bool,
}

impl TraceSession {
    pub fn finish(mut self, quiescent: bool) -> Result<PathBuf> {
        let mut guard = lock_trace_state();
        let mut state = guard
            .take()
            .context("protocol trace session is not active")?;
        if state.run_id != self.run_id {
            anyhow::bail!("protocol trace session changed before finish");
        }
        let records_before_end = state.next_step;
        state.records.push(json!({
            "schema": SCHEMA,
            "run_id": state.run_id,
            "step": state.next_step,
            "event": "trace_end",
            "outcome": "completed",
            "quiescent": quiescent,
            "records_before_end": records_before_end,
        }));
        if state.omit_wal_append_node.is_some() && !state.wal_append_omitted {
            anyhow::bail!("requested WAL-append trace omission did not occur");
        }
        write_trace_records(&state.output, &state.records)?;
        let mut copies = state.actual_copies.into_values().collect::<Vec<_>>();
        copies.sort_by(|left, right| left.copy.node.cmp(&right.copy.node));
        let actual_path = state.output.with_extension("actual.json");
        write_json_file(
            &actual_path,
            &json!({
                "schema": "ferrissearch.d1.trace.actual/v1",
                "run_id": state.run_id,
                "copies": copies,
            }),
        )?;
        self.finished = true;
        Ok(state.output)
    }
}

fn write_trace_records(path: &PathBuf, records: &[Value]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("jsonl.tmp");
    let file = File::create(&temp)
        .with_context(|| format!("create protocol trace temporary file {temp:?}"))?;
    let mut writer = BufWriter::new(file);
    for record in records {
        serde_json::to_writer(&mut writer, record)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    std::fs::rename(&temp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn write_json_file(path: &PathBuf, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    let file = File::create(&temp)
        .with_context(|| format!("create protocol trace actual file {temp:?}"))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    std::fs::rename(&temp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

impl Drop for TraceSession {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut guard = lock_trace_state();
        if guard
            .as_ref()
            .is_some_and(|state| state.run_id == self.run_id)
        {
            *guard = None;
        }
    }
}

pub fn start(config: TraceConfig) -> Result<TraceSession> {
    let TraceConfig {
        output,
        run_id,
        test,
        durability,
        nodes: start_nodes,
        shard_state,
        mutation,
        faults,
    } = config;
    if durability != "request" && durability != "async" {
        anyhow::bail!("protocol trace durability must be request or async");
    }
    let mut guard = lock_trace_state();
    if guard.is_some() {
        anyhow::bail!("a protocol trace session is already active");
    }
    let nodes = start_nodes
        .iter()
        .map(|node| (node.node.clone(), node.incarnation))
        .collect::<HashMap<_, _>>();
    let start = json!({
        "schema": SCHEMA,
        "run_id": run_id,
        "step": 0,
        "event": "trace_start",
        "test": test,
        "durability": durability,
        "nodes": start_nodes,
        "shard_state": shard_state,
    });
    *guard = Some(TraceState {
        output,
        run_id: start["run_id"].as_str().unwrap().to_string(),
        next_step: 1,
        records: vec![start],
        mutation,
        next_request: 0,
        next_message: 0,
        next_batch: 0,
        next_replay: 0,
        next_commit: 0,
        nodes,
        restart_pending: HashSet::new(),
        requests: HashMap::new(),
        request_order: Vec::new(),
        operations: HashMap::new(),
        messages: HashMap::new(),
        message_by_copy_operation: HashMap::new(),
        faults: faults
            .into_iter()
            .map(|rule| FaultState { rule, used: false })
            .collect(),
        replays: HashMap::new(),
        pending_commits: HashMap::new(),
        recovery_snapshots: HashMap::new(),
        known_documents: BTreeSet::new(),
        copy_documents: HashMap::new(),
        actual_copies: HashMap::new(),
        applied_operations: BTreeSet::new(),
        applied_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        omit_wal_append_node: std::env::var("D1_TRACE_OMIT_WAL_APPEND_ONCE")
            .ok()
            .map(|node| {
                if node.is_empty() {
                    "*".to_string()
                } else {
                    node
                }
            }),
        wal_append_omitted: false,
    });
    Ok(TraceSession {
        run_id: guard.as_ref().unwrap().run_id.clone(),
        finished: false,
    })
}

pub fn is_active() -> bool {
    lock_trace_state().is_some()
}

fn with_state<T>(f: impl FnOnce(&mut TraceState) -> T) -> Option<T> {
    let mut guard = lock_trace_state();
    guard.as_mut().map(f)
}

fn push_event(state: &mut TraceState, event: &str, fields: Value) {
    let mut record = match fields {
        Value::Object(fields) => fields,
        _ => panic!("protocol trace event fields must be an object"),
    };
    record.insert("schema".to_string(), Value::String(SCHEMA.to_string()));
    record.insert("run_id".to_string(), Value::String(state.run_id.clone()));
    record.insert("step".to_string(), Value::from(state.next_step));
    record.insert("event".to_string(), Value::String(event.to_string()));
    state.next_step += 1;
    state.records.push(Value::Object(record));
}

pub fn mutation_enabled(mode: MutationMode) -> bool {
    lock_trace_state()
        .as_ref()
        .is_some_and(|state| state.mutation == mode)
}

pub fn with_open_copy<T>(copy: TraceCopy, operation: impl FnOnce() -> T) -> T {
    OPEN_COPY.with(|slot| {
        let previous = slot.replace(Some(copy));
        let result = operation();
        slot.replace(previous);
        result
    })
}

pub fn current_open_copy() -> Option<TraceCopy> {
    OPEN_COPY.with(|slot| slot.borrow().clone())
}

pub fn with_apply_scope<T>(
    origin: ApplyOrigin,
    operations: Vec<SequencedOperation>,
    operation: impl FnOnce() -> T,
) -> T {
    APPLY_SCOPE.with(|slot| {
        let previous = slot.replace(Some(ApplyScope { origin, operations }));
        let result = operation();
        slot.replace(previous);
        result
    })
}

pub fn current_apply_origin() -> Option<ApplyOrigin> {
    APPLY_SCOPE.with(|slot| slot.borrow().as_ref().map(|scope| scope.origin))
}

pub fn current_apply_operations() -> Vec<OperationKey> {
    APPLY_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|scope| {
                let copy = current_open_copy();
                scope
                    .operations
                    .iter()
                    .filter_map(|operation| {
                        copy.as_ref().map(|copy| operation_key(copy, operation))
                    })
                    .collect()
            })
            .unwrap_or_default()
    })
}

pub fn current_apply_operation_values() -> Vec<SequencedOperation> {
    APPLY_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|scope| scope.operations.clone())
            .unwrap_or_default()
    })
}

pub fn with_request_tokens<T>(tokens: Vec<RequestToken>, operation: impl FnOnce() -> T) -> T {
    REQUEST_SCOPE.with(|slot| {
        let previous = slot.replace(tokens.into());
        let result = operation();
        slot.replace(previous);
        result
    })
}

fn take_request_token() -> Option<RequestToken> {
    REQUEST_SCOPE.with(|slot| slot.borrow_mut().pop_front())
}

pub fn content_hash(mutation: &DocumentMutation) -> String {
    let value = match mutation {
        DocumentMutation::Index { source, .. } => canonical_json(source),
        DocumentMutation::Delete { doc_id } => json!({
            "op": "delete",
            "doc": doc_id,
        }),
        DocumentMutation::NoOp { reason } => json!({
            "op": "noop",
            "reason": reason,
        }),
    };
    let bytes = serde_json::to_vec(&value).expect("trace hash value is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        Value::Object(fields) => {
            let sorted = fields
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        value => value.clone(),
    }
}

pub(crate) fn operation_parts(
    operation: &SequencedOperation,
) -> (Option<String>, &'static str, String) {
    match &operation.mutation {
        DocumentMutation::Index { doc_id, .. } => (
            Some(doc_id.clone()),
            "index",
            content_hash(&operation.mutation),
        ),
        DocumentMutation::Delete { doc_id } => (
            Some(doc_id.clone()),
            "delete",
            content_hash(&operation.mutation),
        ),
        DocumentMutation::NoOp { .. } => (None, "noop", content_hash(&operation.mutation)),
    }
}

fn checkpoints_value(stats: SequenceStats) -> Value {
    json!({
        "processed": stats.processed_checkpoint,
        "persisted": stats.persisted_checkpoint,
        "max_seq_no": stats.max_seq_no,
    })
}

pub fn route_client_write(
    node: &str,
    index_uuid: &str,
    shard: u32,
    target_node: &str,
    doc: &str,
    mutation: &DocumentMutation,
) -> Option<RequestToken> {
    let (_, op, hash) = operation_parts(&SequencedOperation {
        seq_no: 0,
        primary_term: 1,
        mutation: mutation.clone(),
    });
    with_state(|state| {
        let request_id = format!("req-{}", state.next_request);
        state.next_request += 1;
        state.known_documents.insert(doc.to_string());
        state.requests.insert(
            request_id.clone(),
            RequestState {
                target: target_node.to_string(),
                doc: doc.to_string(),
                op: op.to_string(),
                content_hash: hash.clone(),
                status: RequestStatus::Routed,
                primary: None,
            },
        );
        state.request_order.push(request_id.clone());
        push_event(
            state,
            "client_write_routed",
            json!({
                "node": node,
                "index_uuid": index_uuid,
                "shard": shard,
                "request_id": request_id,
                "target_node": target_node,
                "doc": doc,
                "op": op,
                "content_hash": hash,
            }),
        );
        RequestToken { request_id }
    })
}

fn find_pending_request(
    state: &TraceState,
    copy: &TraceCopy,
    doc: &str,
    op: &str,
    content_hash: &str,
) -> Option<String> {
    state.request_order.iter().find_map(|request_id| {
        let request = state.requests.get(request_id)?;
        (request.status == RequestStatus::Routed
            && request.target == copy.node
            && request.doc == doc
            && request.op == op
            && request.content_hash == content_hash)
            .then(|| request_id.clone())
    })
}

fn operation_key(copy: &TraceCopy, operation: &SequencedOperation) -> OperationKey {
    OperationKey::new(
        copy.index_uuid.clone(),
        copy.shard,
        operation.primary_term,
        operation.seq_no,
    )
}

fn operation_for_apply(
    state: &mut TraceState,
    copy: &TraceCopy,
    operation: &SequencedOperation,
    origin: ApplyOrigin,
) -> Result<OperationState> {
    let key = operation_key(copy, operation);
    let (doc, op, content_hash) = operation_parts(operation);
    if let Some(existing) = state.operations.get(&key) {
        let same_identity =
            existing.doc == doc && existing.op == op && existing.content_hash == content_hash;
        if origin != ApplyOrigin::Primary || same_identity {
            let mut observed = existing.clone();
            observed.doc = doc;
            observed.op = op.to_string();
            observed.content_hash = content_hash;
            return Ok(observed);
        }
    }
    let request_id = if origin == ApplyOrigin::Primary {
        let request_id = take_request_token()
            .map(|token| token.request_id)
            .or_else(|| {
                doc.as_deref()
                    .and_then(|doc| find_pending_request(state, copy, doc, op, &content_hash))
            })
            .context("primary operation has no routed trace request")?;
        Some(request_id)
    } else {
        None
    };
    let receipt_id = match (&request_id, origin) {
        (Some(request_id), _) => format!("receipt-{request_id}"),
        (None, ApplyOrigin::Promotion) => format!(
            "noop-receipt-{}-{}-{}",
            copy.node, operation.primary_term, operation.seq_no
        ),
        (None, _) => format!(
            "receipt-{}-{}-{}-{}",
            copy.node, copy.shard, operation.primary_term, operation.seq_no
        ),
    };
    let operation_state = OperationState {
        request_id,
        receipt_id,
        doc,
        op: op.to_string(),
        content_hash,
        batch_id: None,
    };
    state.operations.insert(key, operation_state.clone());
    Ok(operation_state)
}

fn apply_origin_for_effect(effect: &str) -> Result<Option<ApplyOrigin>> {
    if let Some(origin) = current_apply_origin() {
        return Ok(Some(origin));
    }
    if is_active() {
        anyhow::bail!("protocol trace {effect} occurred outside an apply scope");
    }
    Ok(None)
}

fn apply_origin_name(origin: ApplyOrigin) -> &'static str {
    match origin {
        ApplyOrigin::Primary => "primary",
        ApplyOrigin::LiveReplication => "live_replication",
        ApplyOrigin::Recovery => "recovery",
        ApplyOrigin::Replay => "replay",
        ApplyOrigin::Promotion => "promotion",
    }
}

pub fn record_wal_appended(
    copy: &TraceCopy,
    operation: &SequencedOperation,
    durable: bool,
) -> Result<()> {
    let Some(origin) = apply_origin_for_effect("WAL append")? else {
        return Ok(());
    };
    match with_state(|state| -> Result<()> {
        let metadata = operation_for_apply(state, copy, operation, origin)?;
        if state
            .omit_wal_append_node
            .as_deref()
            .is_some_and(|node| node == "*" || node == copy.node)
            && !state.wal_append_omitted
        {
            state.wal_append_omitted = true;
            return Ok(());
        }
        push_event(
            state,
            "wal_appended",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "request_id": metadata.request_id,
                "receipt_id": metadata.receipt_id,
                "term": operation.primary_term,
                "seq_no": operation.seq_no,
                "doc": metadata.doc,
                "op": metadata.op,
                "content_hash": metadata.content_hash,
                "origin": apply_origin_name(origin),
                "durable": durable,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

fn outcome_name(outcome: ApplyOutcome) -> &'static str {
    match outcome {
        ApplyOutcome::Applied => "applied_newer",
        ApplyOutcome::Stale => "stale",
        ApplyOutcome::Redelivery => "redelivery",
        ApplyOutcome::NoOp => "noop",
    }
}

pub fn record_operation_processed(
    copy: &TraceCopy,
    operation: &SequencedOperation,
    outcome: ApplyOutcome,
    stats: SequenceStats,
) -> Result<()> {
    let Some(origin) = apply_origin_for_effect("operation processing")? else {
        return Ok(());
    };
    if origin == ApplyOrigin::Replay {
        return record_replay_entry(copy, operation, outcome, stats);
    }
    match with_state(|state| -> Result<()> {
        let key = operation_key(copy, operation);
        let metadata = operation_for_apply(state, copy, operation, origin)?;
        if let Some(request_id) = &metadata.request_id
            && let Some(request) = state.requests.get_mut(request_id)
            && origin == ApplyOrigin::Primary
        {
            request.status = RequestStatus::Replicating;
            request.primary = Some(copy.node.clone());
        }
        if origin == ApplyOrigin::LiveReplication
            && let Some(message_id) = state
                .message_by_copy_operation
                .get(&(key.clone(), copy.node.clone()))
                .cloned()
            && let Some(message) = state.messages.get_mut(&message_id)
        {
            message.phase = Some(MessagePhase::Ack);
        }
        update_logical_copy(state, copy, operation, outcome);
        push_event(
            state,
            "operation_processed",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "request_id": metadata.request_id,
                "receipt_id": metadata.receipt_id,
                "term": operation.primary_term,
                "seq_no": operation.seq_no,
                "doc": metadata.doc,
                "op": metadata.op,
                "content_hash": metadata.content_hash,
                "origin": apply_origin_name(origin),
                "outcome": outcome_name(outcome),
                "checkpoints": checkpoints_value(stats),
                "batch_max_seq_no": stats.max_seq_no,
            }),
        );
        if outcome == ApplyOutcome::Applied {
            state
                .applied_operations
                .insert((copy.node.clone(), operation.seq_no));
            state.applied_notify.notify_waiters();
        }
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_operation_collision(
    copy: &TraceCopy,
    operation: &SequencedOperation,
    stats: SequenceStats,
) -> Result<()> {
    let Some(origin) = apply_origin_for_effect("operation collision")? else {
        return Ok(());
    };
    match with_state(|state| -> Result<()> {
        let key = operation_key(copy, operation);
        let metadata = operation_for_apply(state, copy, operation, origin)?;
        if let Some(message_id) = state
            .message_by_copy_operation
            .get(&(key, copy.node.clone()))
            .cloned()
            && let Some(message) = state.messages.get_mut(&message_id)
        {
            message.phase = Some(MessagePhase::Nack);
        }
        push_event(
            state,
            "operation_processed",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "request_id": metadata.request_id,
                "receipt_id": metadata.receipt_id,
                "term": operation.primary_term,
                "seq_no": operation.seq_no,
                "doc": metadata.doc,
                "op": metadata.op,
                "content_hash": metadata.content_hash,
                "origin": apply_origin_name(origin),
                "outcome": "collision",
                "checkpoints": checkpoints_value(stats),
                "batch_max_seq_no": stats.max_seq_no,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

fn update_logical_copy(
    state: &mut TraceState,
    copy: &TraceCopy,
    operation: &SequencedOperation,
    outcome: ApplyOutcome,
) {
    if outcome != ApplyOutcome::Applied {
        return;
    }
    let documents = state.copy_documents.entry(copy.clone()).or_default();
    match &operation.mutation {
        DocumentMutation::Index { doc_id, .. } => {
            state.known_documents.insert(doc_id.clone());
            documents.insert(
                doc_id.clone(),
                LogicalDocument {
                    state: "live",
                    seq_no: operation.seq_no,
                    term: operation.primary_term,
                    content_hash: content_hash(&operation.mutation),
                },
            );
        }
        DocumentMutation::Delete { doc_id } => {
            state.known_documents.insert(doc_id.clone());
            documents.insert(
                doc_id.clone(),
                LogicalDocument {
                    state: "deleted",
                    seq_no: operation.seq_no,
                    term: operation.primary_term,
                    content_hash: content_hash(&operation.mutation),
                },
            );
        }
        DocumentMutation::NoOp { .. } => {}
    }
}

pub fn operation_keys(copy: &TraceCopy, operations: &[SequencedOperation]) -> Vec<OperationKey> {
    operations
        .iter()
        .map(|operation| operation_key(copy, operation))
        .collect()
}

pub fn start_replication(
    source_node: &str,
    cluster_state: &ClusterState,
    index_name: &str,
    shard: u32,
    operations: &[SequencedOperation],
    noop: bool,
) -> Result<Vec<TraceMessage>> {
    match with_state(|state| -> Result<Vec<TraceMessage>> {
        let metadata = cluster_state
            .indices
            .get(index_name)
            .context("trace replication index is missing")?;
        metadata
            .shard_routing
            .get(&shard)
            .context("trace replication shard is missing")?;
        let source_allocation = cluster_state
            .shard_allocation_id(index_name, shard, source_node)
            .context("trace replication source allocation is missing")?;
        let source_incarnation = *state
            .nodes
            .get(source_node)
            .context("trace replication source node is missing")?;
        let mut targets = metadata
            .in_sync_replica_nodes(shard)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        targets.sort();
        let mut created = Vec::new();
        for operation in operations {
            let key = OperationKey::new(
                metadata.uuid.to_string(),
                shard,
                operation.primary_term,
                operation.seq_no,
            );
            let operation_state = state
                .operations
                .get(&key)
                .cloned()
                .context("trace replication operation is unknown")?;
            let mut required = Vec::new();
            for target in &targets {
                let target_allocation = cluster_state
                    .shard_allocation_id(index_name, shard, target)
                    .context("trace replication target allocation is missing")?;
                let target_incarnation = *state
                    .nodes
                    .get(target)
                    .context("trace replication target node is missing")?;
                let message_id = format!("msg-{}", state.next_message);
                state.next_message += 1;
                let message = TraceMessage {
                    message_id: message_id.clone(),
                    key: key.clone(),
                    source: source_node.to_string(),
                    source_incarnation,
                    target: target.clone(),
                    target_incarnation,
                    target_allocation,
                    batch_id: operation_state.batch_id.clone(),
                };
                state.messages.insert(
                    message_id.clone(),
                    MessageState {
                        message: message.clone(),
                        phase: Some(MessagePhase::Request),
                    },
                );
                state
                    .message_by_copy_operation
                    .insert((key.clone(), target.clone()), message_id.clone());
                if noop {
                    push_event(
                        state,
                        "promotion_noop_replication_started",
                        json!({
                            "node": source_node,
                            "index_uuid": metadata.uuid.as_str(),
                            "shard": shard,
                            "allocation": source_allocation,
                            "source_incarnation": source_incarnation,
                            "batch_id": operation_state.batch_id,
                            "receipt_id": operation_state.receipt_id,
                            "message_id": message_id,
                            "term": operation.primary_term,
                            "seq_no": operation.seq_no,
                            "content_hash": operation_state.content_hash,
                            "replica": target,
                            "replica_allocation": target_allocation,
                            "replica_incarnation": target_incarnation,
                        }),
                    );
                } else {
                    required.push(json!({
                        "node": target,
                        "allocation": target_allocation,
                        "incarnation": target_incarnation,
                        "message_id": message_id,
                    }));
                }
                created.push(message);
            }
            if !noop {
                push_event(
                    state,
                    "primary_replication_started",
                    json!({
                        "node": source_node,
                        "index_uuid": metadata.uuid.as_str(),
                        "shard": shard,
                        "allocation": source_allocation,
                        "source_incarnation": source_incarnation,
                        "request_id": operation_state.request_id,
                        "receipt_id": operation_state.receipt_id,
                        "term": operation.primary_term,
                        "seq_no": operation.seq_no,
                        "required_replicas": required,
                        "routing_version": cluster_state.version,
                    }),
                );
            }
        }
        Ok(created)
    }) {
        Some(result) => result,
        None => Ok(Vec::new()),
    }
}

pub fn message_for(key: &OperationKey, target: &str) -> Option<TraceMessage> {
    with_state(|state| {
        let message_id = state
            .message_by_copy_operation
            .get(&(key.clone(), target.to_string()))?;
        state
            .messages
            .get(message_id)
            .map(|state| state.message.clone())
    })
    .flatten()
}

fn take_fault(
    message_id: &str,
    accepts: impl Fn(FaultAction) -> bool,
) -> Option<(String, FaultAction)> {
    with_state(|state| {
        let message = state.messages.get(message_id)?;
        let rule = state.faults.iter_mut().find(|fault| {
            !fault.used
                && fault.rule.target == message.message.target
                && fault.rule.seq_no == message.message.key.seq_no
                && accepts(fault.rule.action)
        })?;
        rule.used = true;
        Some((message.message.target.clone(), rule.rule.action))
    })
    .flatten()
}

async fn wait_until_applied(node: &str, seq_no: u64) {
    loop {
        let notified = {
            let guard = lock_trace_state();
            let state = guard
                .as_ref()
                .expect("protocol trace causal hold requires an active trace");
            if state
                .applied_operations
                .contains(&(node.to_string(), seq_no))
            {
                return;
            }
            state.applied_notify.clone().notified_owned()
        };
        notified.await;
    }
}

pub async fn apply_request_fault(message_id: &str) -> bool {
    match take_fault(message_id, |action| {
        matches!(
            action,
            FaultAction::DelayRequest { .. }
                | FaultAction::HoldRequestUntilApplied { .. }
                | FaultAction::DropRequest
        )
    }) {
        Some((_, FaultAction::DelayRequest { millis })) => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            false
        }
        Some((target, FaultAction::HoldRequestUntilApplied { seq_no })) => {
            tokio::time::timeout(
                Duration::from_secs(10),
                wait_until_applied(&target, seq_no),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "protocol trace causal hold timed out waiting for {target} to apply seq {seq_no}"
                )
            });
            false
        }
        Some((_, FaultAction::DropRequest)) => true,
        Some((_, FaultAction::DropResponse)) | None => false,
    }
}

pub fn should_drop_response(message_id: &str) -> bool {
    matches!(
        take_fault(message_id, |action| action == FaultAction::DropResponse),
        Some((_, FaultAction::DropResponse))
    )
}

pub fn record_replica_received(copy: &TraceCopy, operation: &SequencedOperation) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let key = operation_key(copy, operation);
        let message_id = state
            .message_by_copy_operation
            .get(&(key, copy.node.clone()))
            .cloned()
            .context("replica receipt has no trace message")?;
        let message = state
            .messages
            .get(&message_id)
            .context("replica receipt trace message disappeared")?;
        let operation_state = state
            .operations
            .get(&message.message.key)
            .cloned()
            .context("replica receipt operation is unknown")?;
        let (doc, op, content_hash) = operation_parts(operation);
        let event = if op == "noop" {
            "promotion_noop_received"
        } else {
            "replica_received"
        };
        let mut fields = json!({
            "node": copy.node,
            "index_uuid": copy.index_uuid,
            "shard": copy.shard,
            "allocation": copy.allocation,
            "source_node": message.message.source,
            "source_incarnation": message.message.source_incarnation,
            "message_id": message_id,
            "receipt_id": operation_state.receipt_id,
            "term": operation.primary_term,
            "seq_no": operation.seq_no,
            "content_hash": content_hash,
        });
        if event == "replica_received" {
            let fields = fields.as_object_mut().unwrap();
            fields.insert("doc".to_string(), json!(doc));
            fields.insert("op".to_string(), json!(op));
        } else {
            fields
                .as_object_mut()
                .unwrap()
                .insert("batch_id".to_string(), json!(operation_state.batch_id));
        }
        push_event(state, event, fields);
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_replica_rejected(
    copy: &TraceCopy,
    operations: &[OperationKey],
    reason: &str,
) -> Result<()> {
    if !matches!(
        reason,
        "quarantined"
            | "term_fence"
            | "identity_mismatch"
            | "recovery_gate"
            | "copy_unavailable"
            | "batch_rejected"
            | "apply_failure"
    ) {
        anyhow::bail!("unknown protocol trace replica rejection reason [{reason}]");
    }
    match with_state(|state| -> Result<()> {
        for key in operations {
            let message_id = state
                .message_by_copy_operation
                .get(&(key.clone(), copy.node.clone()))
                .cloned()
                .context("replica rejection has no trace message")?;
            let message = state
                .messages
                .get(&message_id)
                .cloned()
                .context("replica rejection trace message disappeared")?;
            if message.phase == Some(MessagePhase::Nack) {
                continue;
            }
            if message.phase != Some(MessagePhase::Request) {
                anyhow::bail!("replica rejection message [{message_id}] is not in request phase");
            }
            let operation = state
                .operations
                .get(&message.message.key)
                .cloned()
                .context("replica rejection operation is unknown")?;
            push_event(
                state,
                "replica_rejected",
                json!({
                    "node": copy.node,
                    "index_uuid": copy.index_uuid,
                    "shard": copy.shard,
                    "allocation": copy.allocation,
                    "message_id": message_id,
                    "receipt_id": operation.receipt_id,
                    "term": key.term,
                    "seq_no": key.seq_no,
                    "reason": reason,
                }),
            );
            state
                .messages
                .get_mut(&message.message.message_id)
                .expect("replica rejection message remains registered")
                .phase = Some(MessagePhase::Nack);
        }
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn message_phase(message_id: &str) -> Option<&'static str> {
    with_state(|state| {
        state
            .messages
            .get(message_id)
            .and_then(|message| message.phase.map(MessagePhase::as_str))
    })
    .flatten()
}

pub fn record_replica_result(
    source_copy: &TraceCopy,
    message: &TraceMessage,
    outcome: &str,
    persisted_checkpoint: Option<u64>,
) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let message_state = state
            .messages
            .get(&message.message_id)
            .cloned()
            .context("replica result trace message is unknown")?;
        let operation = state
            .operations
            .get(&message.key)
            .cloned()
            .context("replica result operation is unknown")?;
        let phase = message_state
            .phase
            .map(MessagePhase::as_str)
            .unwrap_or("none");
        let event = if operation.op == "noop" {
            "promotion_noop_result"
        } else {
            "replica_result"
        };
        let mut fields = if event == "replica_result" {
            json!({
                "node": source_copy.node,
                "index_uuid": source_copy.index_uuid,
                "shard": source_copy.shard,
                "request_id": operation.request_id,
                "receipt_id": operation.receipt_id,
                "message_id": message.message_id,
                "replica": message.target,
                "replica_incarnation": message.target_incarnation,
                "outcome": outcome,
                "message_phase": phase,
                "persisted_checkpoint": persisted_checkpoint,
            })
        } else {
            json!({
                "node": source_copy.node,
                "index_uuid": source_copy.index_uuid,
                "shard": source_copy.shard,
                "allocation": source_copy.allocation,
                "batch_id": operation.batch_id,
                "receipt_id": operation.receipt_id,
                "message_id": message.message_id,
                "term": message.key.term,
                "seq_no": message.key.seq_no,
                "replica": message.target,
                "replica_incarnation": message.target_incarnation,
                "outcome": outcome,
                "message_phase": phase,
                "persisted_checkpoint": persisted_checkpoint,
            })
        };
        push_event(state, event, std::mem::take(&mut fields));
        if let Some(message) = state.messages.get_mut(&message.message_id) {
            message.phase = None;
        }
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_client_result(
    token: &RequestToken,
    node: &str,
    index_uuid: &str,
    shard: u32,
    outcome: &str,
    failure_stage: Option<&str>,
) {
    let _ = with_state(|state| {
        if let Some(request) = state.requests.get_mut(&token.request_id) {
            request.status = if outcome == "acknowledged" {
                RequestStatus::Acked
            } else {
                RequestStatus::Failed
            };
        }
        push_event(
            state,
            "client_result",
            json!({
                "node": node,
                "index_uuid": index_uuid,
                "shard": shard,
                "request_id": token.request_id,
                "outcome": outcome,
                "failure_stage": failure_stage,
            }),
        );
    });
}

pub fn record_fence(copy: &TraceCopy, term: u64, fence_max_seq_no: Option<u64>, reason: &str) {
    let _ = with_state(|state| {
        push_event(
            state,
            "fence_persisted",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "term": term,
                "fence_max_seq_no": fence_max_seq_no,
                "reason": reason,
            }),
        );
    });
}

pub fn record_promotion_noop_fill(
    copy: &TraceCopy,
    term: u64,
    operations: &[SequencedOperation],
    stats: SequenceStats,
) -> Result<()> {
    if operations.is_empty() {
        return Ok(());
    }
    match with_state(|state| -> Result<()> {
        let batch_id = format!("noop-batch-{}", state.next_batch);
        state.next_batch += 1;
        let mut noops = Vec::with_capacity(operations.len());
        for operation in operations {
            let key = operation_key(copy, operation);
            let (_, op, content_hash) = operation_parts(operation);
            let operation_state = state
                .operations
                .get_mut(&key)
                .context("promotion NoOp fill has no emitted apply identity")?;
            if operation_state.request_id.is_some()
                || operation_state.doc.is_some()
                || operation_state.op != op
                || operation_state.content_hash != content_hash
            {
                anyhow::bail!("promotion NoOp fill identity differs from its emitted apply");
            }
            operation_state.batch_id = Some(batch_id.clone());
            noops.push(json!({
                "receipt_id": operation_state.receipt_id,
                "seq_no": operation.seq_no,
                "content_hash": content_hash,
            }));
        }
        push_event(
            state,
            "promotion_noop_fill",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "batch_id": batch_id,
                "term": term,
                "noops": noops,
                "checkpoints": checkpoints_value(stats),
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_primary_activated(copy: &TraceCopy, term: u64) {
    let _ = with_state(|state| {
        push_event(
            state,
            "primary_activated",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "term": term,
            }),
        );
    });
}

pub fn record_routing_view(
    node: &str,
    state_view: &ClusterState,
    index_name: &str,
    shard: u32,
) -> Result<()> {
    let Some(metadata) = state_view.indices.get(index_name) else {
        return Ok(());
    };
    let routing = metadata
        .shard_routing
        .get(&shard)
        .context("trace routing view shard is missing")?;
    let mut in_sync = routing.in_sync_replicas.clone();
    in_sync.sort();
    let allocation_state = state_view
        .shard_allocations
        .get(index_name)
        .and_then(|shards| shards.get(&shard))
        .context("trace routing allocations are missing")?;
    let mut allocations = state_view
        .nodes
        .keys()
        .map(|node| {
            let allocation = if node == &routing.primary {
                allocation_state.primary.unwrap_or(0)
            } else {
                allocation_state.replicas.get(node).copied().unwrap_or(0)
            };
            (node.clone(), allocation)
        })
        .collect::<Vec<_>>();
    allocations.sort_by(|left, right| left.0.cmp(&right.0));
    let _ = with_state(|state| {
        push_event(
            state,
            "routing_view",
            json!({
                "node": node,
                "index_uuid": metadata.uuid.as_str(),
                "shard": shard,
                "primary": routing.primary,
                "term": routing.primary_term,
                "in_sync": in_sync,
                "allocations": allocations.into_iter().map(|(node, allocation)| json!({
                    "node": node,
                    "allocation": allocation,
                })).collect::<Vec<_>>(),
                "initialized": state_view.primary_initialized(index_name, shard),
            }),
        );
    });
    Ok(())
}

pub fn record_routing_promoted(
    emitter: &str,
    index_uuid: &str,
    shard: u32,
    new_primary: &str,
    term: u64,
    in_sync: &[String],
) {
    let mut in_sync = in_sync.to_vec();
    in_sync.sort();
    let _ = with_state(|state| {
        push_event(
            state,
            "routing_promoted",
            json!({
                "emitter": emitter,
                "index_uuid": index_uuid,
                "shard": shard,
                "new_primary": new_primary,
                "term": term,
                "in_sync": in_sync,
            }),
        );
    });
}

pub fn record_in_sync_removed(
    emitter: &str,
    index_uuid: &str,
    shard: u32,
    removed_node: &str,
    removed_allocation: u64,
    in_sync: &[String],
) {
    let mut in_sync = in_sync.to_vec();
    in_sync.sort();
    let _ = with_state(|state| {
        push_event(
            state,
            "in_sync_removed",
            json!({
                "emitter": emitter,
                "index_uuid": index_uuid,
                "shard": shard,
                "removed_node": removed_node,
                "removed_allocation": removed_allocation,
                "in_sync": in_sync,
            }),
        );
    });
}

pub fn record_node_crashed(node: &str, outcome: &str) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let incarnation = *state
            .nodes
            .get(node)
            .context("crashed trace node is unknown")?;
        let mut failed_request_ids = state
            .requests
            .iter()
            .filter(|(_, request)| {
                request.status == RequestStatus::Replicating
                    && request.primary.as_deref() == Some(node)
            })
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        failed_request_ids.sort();
        let mut dropped_messages = state
            .messages
            .values()
            .filter_map(|message| {
                let phase = message.phase?;
                let destination = match phase {
                    MessagePhase::Request => &message.message.target,
                    MessagePhase::Ack | MessagePhase::Nack => &message.message.source,
                };
                (destination == node).then(|| {
                    json!({
                        "message_id": message.message.message_id,
                        "message_phase": phase.as_str(),
                    })
                })
            })
            .collect::<Vec<_>>();
        dropped_messages.sort_by(|left, right| {
            (
                left["message_id"].as_str().unwrap(),
                left["message_phase"].as_str().unwrap(),
            )
                .cmp(&(
                    right["message_id"].as_str().unwrap(),
                    right["message_phase"].as_str().unwrap(),
                ))
        });
        let dropped_ids = dropped_messages
            .iter()
            .map(|message| message["message_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        for message_id in dropped_ids {
            if let Some(message) = state.messages.get_mut(&message_id) {
                message.phase = None;
            }
        }
        for request_id in &failed_request_ids {
            if let Some(request) = state.requests.get_mut(request_id) {
                request.status = RequestStatus::Failed;
            }
        }
        push_event(
            state,
            "node_crashed",
            json!({
                "node": node,
                "incarnation": incarnation,
                "outcome": outcome,
                "failed_request_ids": failed_request_ids,
                "dropped_messages": dropped_messages,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn prepare_node_restart(node: &str) -> Result<u64> {
    with_state(|state| -> Result<u64> {
        let incarnation = state
            .nodes
            .get_mut(node)
            .context("restarted trace node is unknown")?;
        *incarnation += 1;
        state.restart_pending.insert(node.to_string());
        Ok(*incarnation)
    })
    .transpose()?
    .context("protocol trace session is not active")
}

pub fn restart_pending(node: &str) -> bool {
    lock_trace_state()
        .as_ref()
        .is_some_and(|state| state.restart_pending.contains(node))
}

pub fn record_node_restarted(copy: &TraceCopy, stats: SequenceStats) {
    let _ = with_state(|state| {
        if !state.restart_pending.remove(&copy.node) {
            return;
        }
        let incarnation = state.nodes[&copy.node];
        push_event(
            state,
            "node_restarted",
            json!({
                "node": copy.node,
                "incarnation": incarnation,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "checkpoints": checkpoints_value(stats),
            }),
        );
    });
}

pub fn record_replay_started(copy: &TraceCopy, stats: SequenceStats) -> Result<Option<String>> {
    match with_state(|state| -> Result<String> {
        if state.replays.contains_key(copy) {
            anyhow::bail!("protocol trace replay started while another replay is active");
        }
        let replay_id = format!("replay-{}", state.next_replay);
        state.next_replay += 1;
        state.replays.insert(
            copy.clone(),
            ActiveReplay {
                replay_id: replay_id.clone(),
                ordinal: 0,
            },
        );
        push_event(
            state,
            "replay_started",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "replay_id": replay_id,
                "checkpoints": checkpoints_value(stats),
            }),
        );
        Ok(replay_id)
    }) {
        Some(result) => result.map(Some),
        None => Ok(None),
    }
}

fn record_replay_entry(
    copy: &TraceCopy,
    operation: &SequencedOperation,
    outcome: ApplyOutcome,
    stats: SequenceStats,
) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let key = operation_key(copy, operation);
        let metadata = state
            .operations
            .get(&key)
            .cloned()
            .context("replay operation is unknown")?;
        let (doc, op, content_hash) = operation_parts(operation);
        let replay = state
            .replays
            .get_mut(copy)
            .context("replay entry has no active replay")?;
        let replay_id = replay.replay_id.clone();
        let ordinal = replay.ordinal;
        replay.ordinal += 1;
        update_logical_copy(state, copy, operation, outcome);
        push_event(
            state,
            "replay_entry",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "replay_id": replay_id,
                "ordinal": ordinal,
                "receipt_id": metadata.receipt_id,
                "term": operation.primary_term,
                "seq_no": operation.seq_no,
                "doc": doc,
                "op": op,
                "content_hash": content_hash,
                "outcome": outcome_name(outcome),
                "checkpoints": checkpoints_value(stats),
                "batch_max_seq_no": stats.max_seq_no,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_replay_skip(
    copy: &TraceCopy,
    operation: &SequencedOperation,
    stats: SequenceStats,
) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let key = operation_key(copy, operation);
        let metadata = state
            .operations
            .get(&key)
            .cloned()
            .context("skipped replay operation is unknown")?;
        let (doc, op, content_hash) = operation_parts(operation);
        let replay = state
            .replays
            .get_mut(copy)
            .context("skipped replay entry has no active replay")?;
        let replay_id = replay.replay_id.clone();
        let ordinal = replay.ordinal;
        replay.ordinal += 1;
        push_event(
            state,
            "replay_entry",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "replay_id": replay_id,
                "ordinal": ordinal,
                "receipt_id": metadata.receipt_id,
                "term": operation.primary_term,
                "seq_no": operation.seq_no,
                "doc": doc,
                "op": op,
                "content_hash": content_hash,
                "outcome": "skip_committed",
                "checkpoints": checkpoints_value(stats),
                "batch_max_seq_no": stats.max_seq_no,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_replay_finished(copy: &TraceCopy, outcome: &str) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let replay = state
            .replays
            .remove(copy)
            .context("protocol trace replay finished without an active replay")?;
        push_event(
            state,
            "replay_finished",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "replay_id": replay.replay_id,
                "outcome": outcome,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_commit_captured(
    copy: &TraceCopy,
    stats: SequenceStats,
    current_term: u64,
    max_seq_no_at_term_start: Option<u64>,
    processed_ranges: Vec<(u64, u64)>,
) -> Option<String> {
    with_state(|state| {
        let commit_id = format!("commit-{}", state.next_commit);
        state.next_commit += 1;
        state
            .pending_commits
            .insert(copy.clone(), commit_id.clone());
        push_event(
            state,
            "commit_captured",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "commit_id": commit_id,
                "checkpoints": checkpoints_value(stats),
                "term_state": {
                    "current_term": current_term,
                    "max_seq_no_at_term_start": max_seq_no_at_term_start,
                    "processed_in_current_term_below_start_max": processed_ranges.into_iter().map(|(start, end)| json!({
                        "start": start,
                        "end": end,
                    })).collect::<Vec<_>>(),
                },
            }),
        );
        commit_id
    })
}

pub fn record_commit_persisted(copy: &TraceCopy) -> Result<()> {
    match with_state(|state| -> Result<()> {
        let commit_id = state
            .pending_commits
            .get(copy)
            .cloned()
            .context("protocol trace commit persistence has no matching capture")?;
        push_event(
            state,
            "commit_persisted",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "commit_id": commit_id,
            }),
        );
        Ok(())
    }) {
        Some(result) => result,
        None => Ok(()),
    }
}

pub fn record_wal_truncated(copy: &TraceCopy, truncate_through: u64) {
    let _ = with_state(|state| {
        push_event(
            state,
            "wal_truncated",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "truncate_through": truncate_through,
            }),
        );
    });
}

#[allow(clippy::too_many_arguments)]
pub fn record_recovery_snapshot(
    source_node: &str,
    target_node: &str,
    index_uuid: &str,
    shard: u32,
    session_id: &str,
    snapshot_next_seq_no: u64,
    mut processed_seqs: Vec<u64>,
    mut documents: Vec<(String, serde_json::Value, u64, u64)>,
) {
    processed_seqs.sort_unstable();
    processed_seqs.dedup();
    documents.sort_by(|left, right| left.0.cmp(&right.0));
    let _ = with_state(|state| {
        let mut snapshot_documents = BTreeMap::new();
        let documents = documents
            .into_iter()
            .map(|(doc, source, seq_no, term)| {
                let content_hash = content_hash(&DocumentMutation::Index {
                    doc_id: doc.clone(),
                    source,
                });
                state.known_documents.insert(doc.clone());
                snapshot_documents.insert(
                    doc.clone(),
                    LogicalDocument {
                        state: "live",
                        seq_no,
                        term,
                        content_hash: content_hash.clone(),
                    },
                );
                json!({
                    "doc": doc,
                    "state": "live",
                    "seq_no": seq_no,
                    "term": term,
                    "content_hash": content_hash,
                })
            })
            .collect::<Vec<_>>();
        state.recovery_snapshots.insert(
            session_id.to_string(),
            RecoverySnapshotState {
                documents: snapshot_documents,
            },
        );
        push_event(
            state,
            "recovery_snapshot",
            json!({
                "source_node": source_node,
                "target_node": target_node,
                "index_uuid": index_uuid,
                "shard": shard,
                "session_id": session_id,
                "snapshot_next_seq_no": snapshot_next_seq_no,
                "processed_seqs": processed_seqs,
                "documents": documents,
            }),
        );
    });
}

#[allow(clippy::too_many_arguments)]
pub fn record_recovery_started(
    source_node: &str,
    target_node: &str,
    index_uuid: &str,
    shard: u32,
    allocation: u64,
    session_id: &str,
) {
    let _ = with_state(|state| {
        push_event(
            state,
            "recovery_started",
            json!({
                "source_node": source_node,
                "target_node": target_node,
                "index_uuid": index_uuid,
                "shard": shard,
                "allocation": allocation,
                "session_id": session_id,
            }),
        );
    });
}

#[allow(clippy::too_many_arguments)]
pub fn record_recovery_installed(
    source_node: &str,
    target_node: &str,
    index_uuid: &str,
    shard: u32,
    allocation: u64,
    session_id: &str,
    snapshot_next_seq_no: u64,
) {
    let _ = with_state(|state| {
        if let Some(snapshot) = state.recovery_snapshots.get(session_id) {
            state.copy_documents.insert(
                TraceCopy {
                    node: target_node.to_string(),
                    index_uuid: index_uuid.to_string(),
                    shard,
                    allocation,
                },
                snapshot.documents.clone(),
            );
        }
        push_event(
            state,
            "recovery_installed",
            json!({
                "source_node": source_node,
                "target_node": target_node,
                "index_uuid": index_uuid,
                "shard": shard,
                "allocation": allocation,
                "session_id": session_id,
                "snapshot_next_seq_no": snapshot_next_seq_no,
            }),
        );
    });
}

#[allow(clippy::too_many_arguments)]
pub fn record_recovery_barrier(
    source_node: &str,
    target_node: &str,
    index_uuid: &str,
    shard: u32,
    allocation: u64,
    session_id: &str,
    barrier_next_seq_no: u64,
    mut processed_seqs: Vec<u64>,
) {
    processed_seqs.sort_unstable();
    processed_seqs.dedup();
    let _ = with_state(|state| {
        push_event(
            state,
            "recovery_barrier",
            json!({
                "source_node": source_node,
                "target_node": target_node,
                "index_uuid": index_uuid,
                "shard": shard,
                "allocation": allocation,
                "session_id": session_id,
                "barrier_next_seq_no": barrier_next_seq_no,
                "processed_seqs": processed_seqs,
            }),
        );
    });
}

#[allow(clippy::too_many_arguments)]
pub fn record_recovery_membership(
    source_node: &str,
    target_node: &str,
    index_uuid: &str,
    shard: u32,
    allocation: u64,
    session_id: &str,
    outcome: &str,
) {
    let _ = with_state(|state| {
        push_event(
            state,
            "recovery_membership",
            json!({
                "source_node": source_node,
                "target_node": target_node,
                "index_uuid": index_uuid,
                "shard": shard,
                "allocation": allocation,
                "session_id": session_id,
                "outcome": outcome,
            }),
        );
    });
}

pub fn record_copy_state(copy: &TraceCopy, reason: &str, live: Vec<(String, u64, u64, String)>) {
    let _ = with_state(|state| {
        let live = live
            .into_iter()
            .map(|(doc, seq_no, term, content_hash)| {
                (
                    doc,
                    LogicalDocument {
                        state: "live",
                        seq_no,
                        term,
                        content_hash,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let tracked = state.copy_documents.get(copy).cloned().unwrap_or_default();
        let mut observed_documents = state.known_documents.clone();
        observed_documents.extend(live.keys().cloned());
        let documents = observed_documents
            .iter()
            .map(|doc| {
                if let Some(document) = live.get(doc) {
                    json!({
                        "doc": doc,
                        "state": "live",
                        "seq_no": document.seq_no,
                        "term": document.term,
                        "content_hash": document.content_hash,
                    })
                } else if let Some(document) = tracked.get(doc)
                    && document.state == "deleted"
                {
                    json!({
                        "doc": doc,
                        "state": "deleted",
                        "seq_no": document.seq_no,
                        "term": document.term,
                        "content_hash": document.content_hash,
                    })
                } else {
                    json!({
                        "doc": doc,
                        "state": "absent",
                        "seq_no": null,
                        "term": null,
                        "content_hash": null,
                    })
                }
            })
            .collect::<Vec<_>>();
        push_event(
            state,
            "copy_state",
            json!({
                "node": copy.node,
                "index_uuid": copy.index_uuid,
                "shard": copy.shard,
                "allocation": copy.allocation,
                "reason": reason,
                "documents": documents,
            }),
        );
    });
}

pub fn record_copy_snapshot(snapshot: TraceCopySnapshot, reason: &str) -> Result<()> {
    let TraceCopySnapshot {
        copy,
        live_documents,
        mut actual_documents,
        wal_entries,
    } = snapshot;
    actual_documents.sort_by(|left, right| left.doc.cmp(&right.doc));
    match with_state(|state| -> Result<()> {
        state.actual_copies.insert(
            copy.node.clone(),
            TraceActualCopy {
                copy: copy.clone(),
                documents: actual_documents,
                wal: wal_entries,
            },
        );
        Ok(())
    }) {
        Some(result) => result?,
        None => return Ok(()),
    }
    record_copy_state(&copy, reason, live_documents);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    static TEST_TRACE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn test_trace_guard() -> MutexGuard<'static, ()> {
        TEST_TRACE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn config(path: &std::path::Path) -> TraceConfig {
        TraceConfig {
            output: path.to_path_buf(),
            run_id: "run".into(),
            test: "unit".into(),
            durability: "request",
            nodes: vec![
                TraceNode {
                    node: "p".into(),
                    incarnation: 0,
                },
                TraceNode {
                    node: "r".into(),
                    incarnation: 0,
                },
            ],
            shard_state: TraceStartShard {
                index_uuid: "idx".into(),
                shard: 0,
                primary: "p".into(),
                term: 1,
                activated: true,
                in_sync: vec!["r".into()],
                copies: vec![
                    TraceStartCopy {
                        node: "p".into(),
                        allocation: 1,
                        exists: true,
                        fence_term: 1,
                    },
                    TraceStartCopy {
                        node: "r".into(),
                        allocation: 2,
                        exists: true,
                        fence_term: 1,
                    },
                ],
            },
            mutation: MutationMode::None,
            faults: Vec::new(),
        }
    }

    #[test]
    fn trace_session_writes_consecutive_jsonl() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let session = start(config(&path)).unwrap();
        let mutation = DocumentMutation::Index {
            doc_id: "d".into(),
            source: json!({"b": 2, "a": 1}),
        };
        let token = route_client_write("p", "idx", 0, "p", "d", &mutation).unwrap();
        record_client_result(&token, "p", "idx", 0, "failed", Some("test"));
        session.finish(false).unwrap();

        let records = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 4);
        for (step, record) in records.iter().enumerate() {
            assert_eq!(record["step"], step as u64);
            assert_eq!(record["schema"], SCHEMA);
        }
    }

    #[test]
    fn content_hash_is_object_order_independent() {
        let left = DocumentMutation::Index {
            doc_id: "d".into(),
            source: json!({"a": 1, "b": {"c": 2, "d": 3}}),
        };
        let right = DocumentMutation::Index {
            doc_id: "d".into(),
            source: json!({"b": {"d": 3, "c": 2}, "a": 1}),
        };
        assert_eq!(content_hash(&left), content_hash(&right));
    }

    #[test]
    fn traced_apply_effect_without_scope_is_an_error() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing-scope.jsonl");
        let session = start(config(&path)).unwrap();
        let error = record_wal_appended(
            &TraceCopy {
                node: "p".into(),
                index_uuid: "idx".into(),
                shard: 0,
                allocation: 1,
            },
            &SequencedOperation {
                seq_no: 0,
                primary_term: 1,
                mutation: DocumentMutation::Index {
                    doc_id: "d".into(),
                    source: json!({"value": 1}),
                },
            },
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside an apply scope"));
        session.finish(false).unwrap();
    }

    #[test]
    fn repeated_wal_effects_are_not_deduplicated() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("duplicate-wal.jsonl");
        let session = start(config(&path)).unwrap();
        let copy = TraceCopy {
            node: "p".into(),
            index_uuid: "idx".into(),
            shard: 0,
            allocation: 1,
        };
        let operation = SequencedOperation {
            seq_no: 0,
            primary_term: 1,
            mutation: DocumentMutation::Index {
                doc_id: "d".into(),
                source: json!({"value": 1}),
            },
        };
        let token = route_client_write("p", "idx", 0, "p", "d", &operation.mutation).unwrap();
        with_request_tokens(vec![token], || {
            with_apply_scope(ApplyOrigin::Primary, vec![operation.clone()], || {
                record_wal_appended(&copy, &operation, true).unwrap();
                record_wal_appended(&copy, &operation, true).unwrap();
            })
        });
        session.finish(false).unwrap();
        let appended = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|record| record["event"] == "wal_appended")
            .count();
        assert_eq!(appended, 2);
    }

    #[test]
    fn replica_events_use_the_applied_operation_identity() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("applied-identity.jsonl");
        let session = start(config(&path)).unwrap();
        let primary = TraceCopy {
            node: "p".into(),
            index_uuid: "idx".into(),
            shard: 0,
            allocation: 1,
        };
        let replica = TraceCopy {
            node: "r".into(),
            index_uuid: "idx".into(),
            shard: 0,
            allocation: 2,
        };
        let primary_operation = SequencedOperation {
            seq_no: 0,
            primary_term: 1,
            mutation: DocumentMutation::Index {
                doc_id: "x".into(),
                source: json!({"value": "primary"}),
            },
        };
        let replica_operation = SequencedOperation {
            seq_no: 0,
            primary_term: 1,
            mutation: DocumentMutation::Index {
                doc_id: "y".into(),
                source: json!({"value": "replica"}),
            },
        };
        let token =
            route_client_write("p", "idx", 0, "p", "x", &primary_operation.mutation).unwrap();
        with_request_tokens(vec![token], || {
            with_apply_scope(
                ApplyOrigin::Primary,
                vec![primary_operation.clone()],
                || {
                    record_wal_appended(&primary, &primary_operation, true).unwrap();
                    record_operation_processed(
                        &primary,
                        &primary_operation,
                        ApplyOutcome::Applied,
                        SequenceStats {
                            processed_checkpoint: Some(0),
                            persisted_checkpoint: Some(0),
                            max_seq_no: Some(0),
                        },
                    )
                    .unwrap();
                },
            )
        });
        with_apply_scope(
            ApplyOrigin::LiveReplication,
            vec![replica_operation.clone()],
            || {
                record_wal_appended(&replica, &replica_operation, true).unwrap();
                record_operation_processed(
                    &replica,
                    &replica_operation,
                    ApplyOutcome::Applied,
                    SequenceStats {
                        processed_checkpoint: Some(0),
                        persisted_checkpoint: Some(0),
                        max_seq_no: Some(0),
                    },
                )
                .unwrap();
            },
        );
        session.finish(false).unwrap();

        let records = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let replica_events = records
            .iter()
            .filter(|record| {
                record["node"] == "r"
                    && matches!(
                        record["event"].as_str(),
                        Some("wal_appended" | "operation_processed")
                    )
            })
            .collect::<Vec<_>>();
        assert_eq!(replica_events.len(), 2);
        for event in replica_events {
            assert_eq!(event["doc"], "y");
            assert_eq!(event["op"], "index");
            assert_eq!(
                event["content_hash"],
                content_hash(&replica_operation.mutation)
            );
        }
    }

    #[test]
    fn repeated_primary_effects_keep_the_same_concurrent_request() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("concurrent-identical-primary.jsonl");
        let session = start(config(&path)).unwrap();
        let copy = TraceCopy {
            node: "p".into(),
            index_uuid: "idx".into(),
            shard: 0,
            allocation: 1,
        };
        let operation = SequencedOperation {
            seq_no: 0,
            primary_term: 1,
            mutation: DocumentMutation::Delete { doc_id: "d".into() },
        };
        let older = route_client_write("p", "idx", 0, "p", "d", &operation.mutation).unwrap();
        let current = route_client_write("p", "idx", 0, "p", "d", &operation.mutation).unwrap();
        with_request_tokens(vec![current], || {
            with_apply_scope(ApplyOrigin::Primary, vec![operation.clone()], || {
                record_wal_appended(&copy, &operation, true).unwrap();
                record_operation_processed(
                    &copy,
                    &operation,
                    ApplyOutcome::Applied,
                    SequenceStats {
                        processed_checkpoint: Some(0),
                        persisted_checkpoint: Some(0),
                        max_seq_no: Some(0),
                    },
                )
                .unwrap();
            })
        });
        record_client_result(&older, "p", "idx", 0, "failed", Some("test"));
        session.finish(false).unwrap();

        let records = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let effect_requests = records
            .iter()
            .filter(|record| {
                matches!(
                    record["event"].as_str(),
                    Some("wal_appended" | "operation_processed")
                )
            })
            .map(|record| record["request_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(effect_requests, ["req-1", "req-1"]);
    }

    #[test]
    fn copy_state_includes_live_documents_unknown_to_trace_history() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("unknown-live-document.jsonl");
        let session = start(config(&path)).unwrap();
        record_copy_state(
            &TraceCopy {
                node: "p".into(),
                index_uuid: "idx".into(),
                shard: 0,
                allocation: 1,
            },
            "trace_end",
            vec![(
                "untraced".into(),
                7,
                2,
                content_hash(&DocumentMutation::Index {
                    doc_id: "untraced".into(),
                    source: json!({"value": 7}),
                }),
            )],
        );
        session.finish(false).unwrap();

        let records = std::fs::read_to_string(path).unwrap();
        assert!(records.contains("\"doc\":\"untraced\""));
    }

    #[test]
    fn every_replay_session_is_emitted() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("replay.jsonl");
        let session = start(config(&path)).unwrap();
        let copy = TraceCopy {
            node: "p".into(),
            index_uuid: "idx".into(),
            shard: 0,
            allocation: 1,
        };
        assert!(
            record_replay_started(
                &copy,
                SequenceStats {
                    processed_checkpoint: None,
                    persisted_checkpoint: None,
                    max_seq_no: None,
                },
            )
            .unwrap()
            .is_some()
        );
        record_replay_finished(&copy, "completed").unwrap();
        session.finish(false).unwrap();
        let events = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()["event"].clone())
            .collect::<Vec<_>>();
        assert!(events.contains(&json!("replay_started")));
        assert!(events.contains(&json!("replay_finished")));
    }

    #[test]
    fn recovery_events_preserve_sorted_snapshot_evidence() {
        let _guard = test_trace_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("recovery.jsonl");
        let session = start(config(&path)).unwrap();
        record_recovery_snapshot(
            "p",
            "r",
            "idx",
            0,
            "session",
            2,
            vec![1, 0, 1],
            vec![
                ("b".into(), json!({"value": 2}), 1, 1),
                ("a".into(), json!({"value": 1}), 0, 1),
            ],
        );
        record_recovery_started("p", "r", "idx", 0, 2, "session");
        record_recovery_installed("p", "r", "idx", 0, 2, "session", 2);
        record_recovery_barrier("p", "r", "idx", 0, 2, "session", 2, vec![1, 0]);
        record_recovery_membership("p", "r", "idx", 0, 2, "session", "admitted");
        record_copy_state(
            &TraceCopy {
                node: "r".into(),
                index_uuid: "idx".into(),
                shard: 0,
                allocation: 2,
            },
            "admission",
            vec![
                (
                    "a".into(),
                    0,
                    1,
                    content_hash(&DocumentMutation::Index {
                        doc_id: "a".into(),
                        source: json!({"value": 1}),
                    }),
                ),
                (
                    "b".into(),
                    1,
                    1,
                    content_hash(&DocumentMutation::Index {
                        doc_id: "b".into(),
                        source: json!({"value": 2}),
                    }),
                ),
            ],
        );
        session.finish(false).unwrap();

        let records = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .map(|record| record["event"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "trace_start",
                "recovery_snapshot",
                "recovery_started",
                "recovery_installed",
                "recovery_barrier",
                "recovery_membership",
                "copy_state",
                "trace_end",
            ]
        );
        assert_eq!(records[1]["processed_seqs"], json!([0, 1]));
        assert_eq!(records[1]["documents"][0]["doc"], "a");
        assert_eq!(records[1]["documents"][1]["doc"], "b");
    }
}
