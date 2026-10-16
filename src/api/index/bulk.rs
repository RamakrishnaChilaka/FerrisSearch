//! Strict NDJSON action parsing and ordered per-shard bulk execution.

use super::*;
use crate::engine::WriteCondition;
use crate::transport::proto::{ShardBulkItemResponse, ShardBulkOpKind, ShardBulkOperation};
use axum::body::Body;
use axum::extract::RawQuery;
use axum::response::{IntoResponse, Response};
use pipeline::{BulkError, StoredResult};

mod parser;
mod pipeline;
pub use pipeline::BulkRuntime;

#[derive(Debug)]
pub(super) struct BulkDoc {
    pub action: String,
    pub doc_id: String,
    pub index: Option<String>,
    pub payload: Value,
    pub condition: WriteCondition,
    pub retry_on_conflict: u32,
    pub operation_id: Option<String>,
    pub source_error: Option<String>,
}

#[derive(Debug)]
pub(super) struct RoutedBulkDoc {
    pub position: usize,
    pub index_name: String,
    pub doc_id: String,
    pub payload: Value,
    pub shard_id: u32,
    pub node_id: String,
    pub action: String,
    pub condition: WriteCondition,
    pub retry_on_conflict: u32,
    pub operation_id: Option<String>,
}

pub(super) type BulkTargetKey = (String, String, u32);

#[derive(Debug, Clone)]
pub(super) struct BulkTargetFailure {
    pub status: StatusCode,
    pub error_type: String,
    pub reason: String,
    pub failure: Option<Box<crate::transport::write_failure::WriteFailure>>,
}

impl BulkTargetFailure {
    #[cfg(test)]
    pub(super) fn internal(reason: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "shard_failure".into(),
            reason,
            failure: None,
        }
    }

    pub(super) fn from_forward_error(error: anyhow::Error) -> Self {
        let (status, error_type) = forwarded_write_error_classification(&error);
        Self {
            status,
            error_type: error_type.into(),
            reason: error.to_string(),
            failure: error
                .downcast_ref::<crate::transport::write_failure::WriteFailure>()
                .cloned()
                .map(Box::new),
        }
    }

    fn from_api_response((status, Json(body)): (StatusCode, Json<Value>)) -> Self {
        Self {
            status,
            error_type: body["error"]["type"]
                .as_str()
                .unwrap_or("bulk_item_exception")
                .to_string(),
            reason: body["error"]["reason"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| body.to_string()),
            failure: None,
        }
    }

    fn item(&self, action: &str, index: &str, doc_id: &str, offset: usize, count: usize) -> Value {
        let mut item = bulk_error_item(
            action,
            Some(index),
            doc_id,
            self.status,
            &self.error_type,
            &self.reason,
        );
        if let Some(failure) = &self.failure {
            match failure.item_receipt(offset, count) {
                Ok(seq_no) => {
                    if let Some(seq_no) = seq_no {
                        item[action]["_seq_no"] = serde_json::json!(seq_no);
                    }
                    if let Some(term) = failure.primary_term {
                        item[action]["_primary_term"] = serde_json::json!(term);
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "Invalid failed bulk operation range");
                    item[action]["error"]["reason"] = serde_json::json!(format!(
                        "{}; invalid failed operation identity: {error:#}",
                        self.reason
                    ));
                }
            }
        }
        item
    }
}

type BulkTargetResults = HashMap<BulkTargetKey, Result<Vec<Value>, BulkTargetFailure>>;

fn bulk_error_item(
    action: &str,
    index_name: Option<&str>,
    doc_id: &str,
    status: StatusCode,
    error_type: &str,
    reason: impl std::fmt::Display,
) -> Value {
    let reason_str = reason.to_string();
    let error_class = crate::worker::classify_http_error(status.as_u16(), error_type, &reason_str);
    crate::worker::record_classified_error(error_class, crate::worker::ExecutionStage::Write);
    let mut result = serde_json::json!({
        "_id": doc_id, "status": status.as_u16(),
        "error": {
            "type": error_type,
            "reason": reason_str,
            "error_class": error_class.as_str(),
        }
    });
    if let Some(index_name) = index_name {
        result["_index"] = serde_json::json!(index_name);
    }
    serde_json::json!({(action): result})
}

fn wire_item_json(index: &str, shard: u32, item: ShardBulkItemResponse) -> Value {
    let mut result =
        serde_json::json!({"_index": index, "_id": item.doc_id, "status": item.status});
    if item.error.is_empty() {
        result["result"] = serde_json::json!(item.result);
        crate::transport::write_acknowledgement::add_write_acknowledgement_to_response(
            &mut result,
            index,
            shard,
            item.write_acknowledgement
                .as_ref()
                .expect("successful bulk items have validated data-copy acknowledgement reports"),
        );
    } else {
        let error_class =
            crate::worker::classify_http_error(item.status as u16, &item.error_type, &item.error);
        crate::worker::record_classified_error(error_class, crate::worker::ExecutionStage::Write);
        result["error"] = serde_json::json!({
            "type": item.error_type,
            "reason": item.error,
            "error_class": error_class.as_str(),
        });
    }
    if let Some(seq_no) = item.seq_no {
        result["_seq_no"] = serde_json::json!(seq_no);
    }
    if let Some(term) = item.primary_term {
        result["_primary_term"] = serde_json::json!(term);
    }
    if let Some(version) = item.version {
        result["_version"] = serde_json::json!(version);
    }
    if let Some(refresh) = &item.write_refresh {
        crate::transport::write_refresh::add_write_refresh_to_response(
            &mut result,
            index,
            shard,
            refresh,
        );
    }
    result
}

fn parse_bulk_action(action_line: &str, line: u64, item: usize) -> Result<BulkDoc, String> {
    let malformed = |reason: String| format!("Malformed action/metadata line [{line}]: {reason}");
    let value: Value =
        serde_json::from_str(action_line).map_err(|error| malformed(error.to_string()))?;
    let object = value
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| {
            malformed("expected exactly one action [index, create, update, delete]".into())
        })?;
    let (action, metadata) = object.iter().next().expect("one action");
    if !matches!(action.as_str(), "index" | "create" | "update" | "delete") {
        return Err(malformed(format!("unknown action [{action}]")));
    }
    let metadata = metadata
        .as_object()
        .ok_or_else(|| malformed(format!("action [{action}] must contain an object")))?;
    for (name, value) in metadata {
        if matches!(name.as_str(), "if_seq_no" | "if_primary_term")
            || (name == "retry_on_conflict" && action == "update")
            || name == "_operation_id"
        {
            continue;
        }
        let scalar = value
            .as_str()
            .or_else(|| (value.as_u64() == Some(1)).then_some("1"));
        validate_write_parameter(name, scalar)
            .map_err(|reason| malformed(format!("bulk item [{item}]: {reason}")))?;
    }
    let string = |key: &str| -> Result<Option<String>, String> {
        metadata
            .get(key)
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| malformed(format!("[{key}] must be a string")))
            })
            .transpose()
    };
    let number = |key: &str| -> Result<Option<u64>, String> {
        metadata
            .get(key)
            .map(|value| {
                value
                    .as_u64()
                    .ok_or_else(|| malformed(format!("[{key}] must be a non-negative integer")))
            })
            .transpose()
    };
    let condition =
        WriteCondition::from_optional_values(number("if_seq_no")?, number("if_primary_term")?)
            .map_err(|error| malformed(error.to_string()))?;
    if action == "create" && condition != WriteCondition::Unconditional {
        return Err(malformed(
            "create operations cannot use if_seq_no or if_primary_term".into(),
        ));
    }
    let retries = number("retry_on_conflict")?.unwrap_or(0);
    let retry_on_conflict = u32::try_from(retries)
        .map_err(|_| malformed("retry_on_conflict exceeds the supported range".into()))?;
    let operation_id = string("_operation_id")?;
    if let Some(operation_id) = operation_id.as_deref() {
        crate::engine::ClientOperationIntent::validate_id(operation_id)
            .map_err(|error| malformed(error.to_string()))?;
    }
    let doc_id = match string("_id")? {
        Some(doc_id) => doc_id,
        None if matches!(action.as_str(), "index" | "create") => match operation_id.as_deref() {
            Some(operation_id) => {
                crate::engine::ClientOperationIntent::stable_document_id(operation_id)
                    .map_err(|error| malformed(error.to_string()))?
            }
            None => uuid::Uuid::new_v4().to_string(),
        },
        None => String::new(),
    };
    let index = string("_index")?;
    Ok(BulkDoc {
        action: action.clone(),
        doc_id,
        index,
        payload: Value::Null,
        condition,
        retry_on_conflict,
        operation_id,
        source_error: None,
    })
}

#[cfg(test)]
pub(super) fn parse_bulk_ndjson(text: &str) -> Result<Vec<BulkDoc>, String> {
    let mut decoder = parser::BulkDecoder::new(std::io::Cursor::new(text));
    let mut documents = Vec::new();
    while let Some(document) = decoder.next_document().map_err(|error| error.to_string())? {
        documents.push(document);
    }
    Ok(documents)
}

fn validate_bulk_document(document: &BulkDoc) -> Result<(), (StatusCode, Json<Value>)> {
    if let Some(error) = &document.source_error {
        return Err(mapper_parsing_error_response(error));
    }
    if document.doc_id.is_empty() {
        return Err(illegal_argument(format!(
            "bulk [{}] requires an _id",
            document.action
        )));
    }
    match document.action.as_str() {
        "delete" => Ok(()),
        "update" => validate_update_body(&document.payload),
        _ => validate_document_source_for_api(&document.payload),
    }
}

pub(super) fn route_bulk_doc(
    position: usize,
    index_name: String,
    doc_id: String,
    payload: Value,
    metadata: &IndexMetadata,
    cluster_state: &crate::cluster::state::ClusterState,
) -> Result<RoutedBulkDoc, Value> {
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let node_id = metadata
        .primary_node(shard_id)
        .ok_or_else(|| {
            bulk_error_item(
                "index",
                Some(&index_name),
                &doc_id,
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                "Shard has no assigned primary",
            )
        })?
        .clone();
    if !cluster_state.nodes.contains_key(&node_id) {
        return Err(bulk_error_item(
            "index",
            Some(&index_name),
            &doc_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!("Primary node [{node_id}] not found in cluster state"),
        ));
    }
    Ok(RoutedBulkDoc {
        position,
        index_name,
        doc_id,
        payload,
        shard_id,
        node_id,
        action: "index".into(),
        condition: WriteCondition::Unconditional,
        retry_on_conflict: 0,
        operation_id: None,
    })
}

async fn forward_bulk_batches(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    routed_docs: &mut [RoutedBulkDoc],
    lease: &pipeline::RequestLease,
) -> BulkTargetResults {
    let mut batches: HashMap<BulkTargetKey, Vec<RoutedBulkDoc>> = HashMap::new();
    for document in routed_docs {
        batches
            .entry((
                document.index_name.clone(),
                document.node_id.clone(),
                document.shard_id,
            ))
            .or_default()
            .push(RoutedBulkDoc {
                position: document.position,
                index_name: document.index_name.clone(),
                doc_id: document.doc_id.clone(),
                payload: document.payload.take(),
                shard_id: document.shard_id,
                node_id: document.node_id.clone(),
                action: document.action.clone(),
                condition: document.condition,
                retry_on_conflict: document.retry_on_conflict,
                operation_id: document.operation_id.clone(),
            });
    }
    stream::iter(batches.into_iter().map(|(key, mut batch)| async move {
        let Some(node) = cluster_state.nodes.get(&key.1) else {
            return (
                key,
                Err(BulkTargetFailure::from_forward_error(
                    crate::transport::write_failure::WriteFailure::not_executed(
                        "Primary node missing from cluster state",
                    )
                    .into(),
                )),
            );
        };
        let mut results = Vec::with_capacity(batch.len());
        let mut cursor = 0;
        while cursor < batch.len() {
            if lease.is_cancelled() {
                let failure = BulkTargetFailure::from_forward_error(
                    crate::transport::write_failure::WriteFailure::not_executed(
                        "Bulk request cancelled before this shard unit was dispatched",
                    )
                    .into(),
                );
                for document in &batch[cursor..] {
                    let item = failure.item(
                        &document.action,
                        &document.index_name,
                        &document.doc_id,
                        0,
                        1,
                    );
                    results.push(item[&document.action].clone());
                }
                break;
            }
            let document = &mut batch[cursor];
            if document.action == "update" {
                let (if_seq_no, if_primary_term) = document.condition.expected_version();
                let (status, Json(mut response)) = execute_update_cancellable(
                    state,
                    &document.index_name,
                    &document.doc_id,
                    document.payload.take(),
                    &UpdateParams {
                        if_seq_no,
                        if_primary_term,
                        retry_on_conflict: document.retry_on_conflict,
                        refresh: None,
                        operation_id: document.operation_id.clone(),
                        ..UpdateParams::default()
                    },
                    Some(lease.cancellation_flag()),
                )
                .await;
                response["status"] = serde_json::json!(status.as_u16());
                response["_index"] = serde_json::json!(document.index_name);
                response["_id"] = serde_json::json!(document.doc_id);
                results.push(response);
                cursor += 1;
                continue;
            }
            let start = cursor;
            while cursor < batch.len()
                && batch[cursor].action != "update"
                && cursor - start < pipeline::RPC_ITEMS
            {
                cursor += 1;
            }
            let run = &mut batch[start..cursor];
            let index_uuid = cluster_state.indices[&key.0].uuid.to_string();
            let operations = run
                .iter_mut()
                .map(|document| {
                    let kind = match document.action.as_str() {
                        "index" => ShardBulkOpKind::Index,
                        "create" => ShardBulkOpKind::Create,
                        "delete" => ShardBulkOpKind::Delete,
                        _ => unreachable!("parser validated the action"),
                    };
                    let (if_seq_no, if_primary_term) = document.condition.expected_version();
                    let client_operation = client_operation_intent(
                        document.operation_id.as_deref(),
                        serde_json::json!({
                            "kind": document.action,
                            "index_uuid": index_uuid,
                            "doc_id": document.doc_id,
                            "source": document.payload,
                            "condition": write_condition_identity(document.condition)
                        }),
                    )
                    .expect("bulk operation ID was validated before routing");
                    (
                        document.doc_id.clone(),
                        document.payload.take(),
                        ShardBulkOperation {
                            kind: kind as i32,
                            if_seq_no,
                            if_primary_term,
                            client_operation: client_operation
                                .as_ref()
                                .map(crate::transport::client_operation::intent_to_proto),
                        },
                    )
                })
                .collect::<Vec<_>>();
            match state
                .transport_client
                .forward_bulk_operations_to_shard(node, &key.0, key.2, &operations, false)
                .await
            {
                Ok(items) => results.extend(
                    items
                        .into_iter()
                        .map(|item| wire_item_json(&key.0, key.2, item)),
                ),
                Err(error) => {
                    let failure = BulkTargetFailure::from_forward_error(error);
                    let count = run.len();
                    for (offset, document) in run.iter().enumerate() {
                        let item = failure.item(
                            &document.action,
                            &document.index_name,
                            &document.doc_id,
                            offset,
                            count,
                        );
                        results.push(item[&document.action].clone());
                    }
                }
            }
        }
        (key, Ok(results))
    }))
    .buffer_unordered(8)
    .collect()
    .await
}

fn is_acknowledged_mutation(item: &Value) -> bool {
    item.get("error").is_none()
        && item["result"] != "noop"
        && item["_seq_no"].is_u64()
        && item["_primary_term"].as_u64().is_some_and(|term| term > 0)
}

async fn refresh_bulk_target(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    key: &BulkTargetKey,
    primary_term: u64,
) -> crate::transport::proto::ShardWriteRefreshResult {
    let allocation = cluster_state
        .shard_allocation_id(&key.0, key.2, &key.1)
        .filter(|allocation| *allocation > 0);
    let result = async {
        let metadata = cluster_state
            .indices
            .get(&key.0)
            .ok_or_else(|| anyhow::anyhow!("bulk refresh index metadata is missing"))?;
        let node = cluster_state
            .nodes
            .get(&key.1)
            .ok_or_else(|| anyhow::anyhow!("bulk refresh primary node [{}] is missing", key.1))?;
        let allocation = allocation
            .ok_or_else(|| anyhow::anyhow!("bulk refresh primary allocation is missing"))?;
        state
            .transport_client
            .refresh_shard_writes(
                node,
                crate::transport::proto::ShardCopyRefreshRequest {
                    index_name: key.0.clone(),
                    index_uuid: metadata.uuid.to_string(),
                    shard_id: key.2,
                    primary_node_id: key.1.clone(),
                    primary_term: Some(primary_term),
                    target_allocation_id: Some(allocation),
                },
            )
            .await
    }
    .await;
    match result {
        Ok(report) => report,
        Err(error) => {
            tracing::error!(
                index = key.0, shard = key.2, node = key.1,
                error = %format!("{error:#}"),
                "Primary-owned bulk refresh failed after writes acknowledged"
            );
            crate::transport::proto::ShardWriteRefreshResult {
                copies: vec![crate::transport::write_refresh::copy_refresh_failure(
                    &key.1,
                    allocation,
                    true,
                    format!("primary-owned bulk refresh failed: {error:#}"),
                )],
            }
        }
    }
}

pub(super) fn finalize_bulk_items(
    mut items: Vec<Option<Value>>,
    routed_docs: Vec<RoutedBulkDoc>,
    outcomes: &BulkTargetResults,
) -> Vec<Value> {
    let mut counts: HashMap<BulkTargetKey, usize> = HashMap::new();
    for document in &routed_docs {
        let key = (
            document.index_name.clone(),
            document.node_id.clone(),
            document.shard_id,
        );
        *counts.entry(key).or_default() += 1;
    }
    let mut offsets: HashMap<BulkTargetKey, usize> = HashMap::new();
    for document in routed_docs {
        let key = (
            document.index_name.clone(),
            document.node_id.clone(),
            document.shard_id,
        );
        let offset = offsets.entry(key.clone()).or_default();
        let result = match outcomes.get(&key) {
            Some(Ok(results)) => results
                .get(*offset)
                .cloned()
                .map(|result| serde_json::json!({(document.action.clone()): result}))
                .unwrap_or_else(|| {
                    bulk_error_item(
                        &document.action,
                        Some(&document.index_name),
                        &document.doc_id,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "shard_failure",
                        "missing primary bulk item result",
                    )
                }),
            Some(Err(failure)) => failure.item(
                &document.action,
                &document.index_name,
                &document.doc_id,
                *offset,
                counts[&key],
            ),
            None => bulk_error_item(
                &document.action,
                Some(&document.index_name),
                &document.doc_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_failure",
                "missing primary bulk write receipt",
            ),
        };
        *offset += 1;
        items[document.position] = Some(result);
    }
    items
        .into_iter()
        .enumerate()
        .map(|(position, item)| {
            item.unwrap_or_else(|| {
                bulk_error_item(
                    "index",
                    None,
                    "",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "bulk_item_exception",
                    format!("bulk item {position} was dropped unexpectedly"),
                )
            })
        })
        .collect()
}

fn authorize_bulk_index(
    state: &AppState,
    principal: Option<&crate::security::Principal>,
    index: &str,
) -> Result<(), BulkTargetFailure> {
    if crate::security::is_protected_system_index(index) {
        return Err(BulkTargetFailure {
            status: StatusCode::FORBIDDEN,
            error_type: "security_exception".into(),
            reason: format!(
                "index [{index}] is a protected system index and cannot be accessed through ordinary index APIs"
            ),
            failure: None,
        });
    }
    crate::common::IndexName::new(index.to_string()).map_err(|reason| BulkTargetFailure {
        status: StatusCode::BAD_REQUEST,
        error_type: "invalid_index_name_exception".into(),
        reason: reason.to_string(),
        failure: None,
    })?;
    state
        .security_manager
        .authorize_or_error(
            principal,
            &crate::security::ClassifiedRequest {
                action: crate::security::SecurityAction::IndexWrite,
                index: Some(index.to_string()),
            },
        )
        .map_err(|error| BulkTargetFailure {
            status: error.status,
            error_type: error.error_type.into(),
            reason: error.reason.to_string(),
            failure: None,
        })
}

async fn bulk_metadata(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    principal: Option<&crate::security::Principal>,
    index: &str,
) -> Result<IndexMetadata, BulkTargetFailure> {
    authorize_bulk_index(state, principal, index)?;
    let current;
    let cluster_state = if cluster_state.indices.contains_key(index) {
        cluster_state
    } else {
        current = super::index_cluster_state(state, index)
            .await
            .map_err(BulkTargetFailure::from_api_response)?;
        &current
    };
    let metadata = match cluster_state.indices.get(index) {
        Some(metadata) => metadata.clone(),
        None => auto_create_index(state, index, cluster_state)
            .await
            .map_err(BulkTargetFailure::from_api_response)?,
    };
    if let Some(response) = crate::api::reject_write_if_engine_read_only(&metadata) {
        return Err(BulkTargetFailure::from_api_response(response));
    }
    Ok(metadata)
}

struct BulkExecution {
    state: AppState,
    principal: Option<crate::security::Principal>,
    default_index: Option<String>,
    refresh: bool,
    cluster_state: crate::cluster::state::ClusterState,
    metadata: HashMap<String, Result<IndexMetadata, BulkTargetFailure>>,
    targets: HashMap<BulkTargetKey, Option<u64>>,
    dispatched: bool,
    lease: Arc<pipeline::RequestLease>,
}

impl BulkExecution {
    async fn owned_batch(
        mut self,
        documents: Vec<BulkDoc>,
    ) -> Result<(Self, Vec<StoredResult>), BulkError> {
        Ok(tokio::spawn(async move {
            let results = self.batch(documents).await;
            (self, results)
        })
        .await?)
    }

    async fn batch(&mut self, documents: Vec<BulkDoc>) -> Vec<StoredResult> {
        let mut items = vec![None; documents.len()];
        let mut routed = Vec::new();
        for (position, document) in documents.into_iter().enumerate() {
            if self.lease.is_cancelled() {
                items[position] = Some(bulk_error_item(
                    &document.action,
                    document.index.as_deref().or(self.default_index.as_deref()),
                    &document.doc_id,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "task_cancelled_exception",
                    "Bulk request cancelled before metadata lookup or mutation",
                ));
                continue;
            }
            let index = document
                .index
                .as_deref()
                .map(str::to_owned)
                .or_else(|| self.default_index.clone());
            if let Err(response) = validate_bulk_document(&document) {
                let failure = BulkTargetFailure::from_api_response(response);
                items[position] = Some(bulk_error_item(
                    &document.action,
                    index.as_deref(),
                    &document.doc_id,
                    failure.status,
                    &failure.error_type,
                    &failure.reason,
                ));
                continue;
            }
            let Some(index) = index else {
                items[position] = Some(bulk_error_item(
                    &document.action,
                    None,
                    &document.doc_id,
                    StatusCode::BAD_REQUEST,
                    "action_request_validation_exception",
                    "bulk action metadata must include _index",
                ));
                continue;
            };
            if let Err(failure) = authorize_bulk_index(&self.state, self.principal.as_ref(), &index)
            {
                items[position] = Some(bulk_error_item(
                    &document.action,
                    Some(&index),
                    &document.doc_id,
                    failure.status,
                    &failure.error_type,
                    &failure.reason,
                ));
                continue;
            }
            if !self.metadata.contains_key(&index) {
                self.metadata.insert(
                    index.clone(),
                    bulk_metadata(
                        &self.state,
                        &self.cluster_state,
                        self.principal.as_ref(),
                        &index,
                    )
                    .await,
                );
                self.cluster_state = self.state.cluster_manager.get_state();
            }
            let index_metadata = match &self.metadata[&index] {
                Ok(metadata) => metadata,
                Err(failure) => {
                    items[position] = Some(bulk_error_item(
                        &document.action,
                        Some(&index),
                        &document.doc_id,
                        failure.status,
                        &failure.error_type,
                        &failure.reason,
                    ));
                    continue;
                }
            };
            match route_bulk_doc(
                position,
                index,
                document.doc_id,
                document.payload,
                index_metadata,
                &self.cluster_state,
            ) {
                Ok(mut target) => {
                    if self.refresh {
                        let key = (
                            target.index_name.clone(),
                            target.node_id.clone(),
                            target.shard_id,
                        );
                        if !self.targets.contains_key(&key)
                            && self.targets.len() == pipeline::MAX_TARGETS
                        {
                            items[position] = Some(bulk_error_item(
                                &document.action,
                                Some(&target.index_name),
                                &target.doc_id,
                                StatusCode::TOO_MANY_REQUESTS,
                                "es_rejected_execution_exception",
                                format!(
                                    "Bulk refresh exceeds {} distinct shard targets",
                                    pipeline::MAX_TARGETS
                                ),
                            ));
                            continue;
                        }
                        self.targets.entry(key).or_insert(None);
                    }
                    target.action = document.action;
                    target.condition = document.condition;
                    target.retry_on_conflict = document.retry_on_conflict;
                    target.operation_id = document.operation_id;
                    routed.push(target);
                }
                Err(item) => {
                    let result = item["index"].clone();
                    items[position] = Some(serde_json::json!({(document.action): result}));
                }
            }
        }
        let positions = routed
            .iter()
            .map(|document| {
                (
                    document.position,
                    (
                        document.index_name.clone(),
                        document.node_id.clone(),
                        document.shard_id,
                    ),
                )
            })
            .collect::<Vec<_>>();
        if !routed.is_empty() {
            self.dispatched = true;
        }
        let outcomes =
            forward_bulk_batches(&self.state, &self.cluster_state, &mut routed, &self.lease).await;
        if self.refresh {
            for (key, outcome) in &outcomes {
                if let Ok(results) = outcome
                    && let Some(term) = results
                        .iter()
                        .rev()
                        .find(|item| is_acknowledged_mutation(item))
                        .and_then(|item| item["_primary_term"].as_u64())
                {
                    *self.targets.get_mut(key).expect("tracked bulk target") = Some(term);
                }
            }
        }
        let mut results = finalize_bulk_items(items, routed, &outcomes)
            .into_iter()
            .map(|item| StoredResult { item, target: None })
            .collect::<Vec<_>>();
        for (position, key) in positions {
            results[position].target = Some(key);
        }
        results
    }

    async fn refresh_reports(
        self,
    ) -> Result<HashMap<BulkTargetKey, crate::transport::proto::ShardWriteRefreshResult>, BulkError>
    {
        Ok(tokio::spawn(async move {
            let targets = self
                .targets
                .iter()
                .filter_map(|(key, term)| term.map(|term| (key.clone(), term)))
                .collect::<Vec<_>>();
            let state = &self.state;
            let cluster_state = &self.cluster_state;
            let lease = &self.lease;
            stream::iter(targets)
                .map(|(key, term)| async move {
                    let report = if lease.is_cancelled() {
                        crate::transport::proto::ShardWriteRefreshResult {
                            copies: vec![crate::transport::write_refresh::copy_refresh_failure(
                                &key.1,
                                cluster_state.shard_allocation_id(&key.0, key.2, &key.1),
                                true,
                                "Bulk request cancelled before final visibility dispatch"
                                    .to_string(),
                            )],
                        }
                    } else {
                        refresh_bulk_target(state, cluster_state, &key, term).await
                    };
                    (key, report)
                })
                .buffer_unordered(8)
                .collect()
                .await
        })
        .await?)
    }
}

async fn execute_bulk(
    state: &AppState,
    principal: Option<&crate::security::Principal>,
    default_index: Option<&str>,
    refresh: &RefreshParam,
    pretty: bool,
    body: Body,
) -> Response {
    if let Err(response) = refresh.validate() {
        return response.into_response();
    }
    let refresh = refresh.should_refresh();
    let lease = match state.bulk_runtime.acquire() {
        Ok(lease) => lease,
        Err(error) => return error.response(false, 0),
    };
    let mut scope = pipeline::RequestScope::new(lease.clone());
    let mut dispatched = false;
    let mut completed_items = 0;
    let result: Result<Response, BulkError> = async {
        let staged = pipeline::stage(
            body,
            state.shard_manager.data_dir().to_path_buf(),
            default_index.map(str::to_string),
            &state.bulk_runtime,
            lease.clone(),
        )
        .await?;
        if staged.item_count == 0 {
            lease.complete();
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"took": 0, "errors": false, "items": []})),
            )
                .into_response());
        }
        let cluster_state = state.cluster_manager.get_state();
        if let Some(index) = default_index
            && staged.only_default_index
            && let Some(metadata) = cluster_state.indices.get(index)
            && let Some(response) = crate::api::reject_write_if_engine_read_only(metadata)
        {
            lease.fail();
            return Ok(response.into_response());
        }
        let mut decoder = staged.decoder;
        let mut spool =
            pipeline::result_spool(state.shard_manager.data_dir().to_path_buf(), lease.clone())
                .await?;
        let mut execution = BulkExecution {
            state: state.clone(),
            principal: principal.cloned(),
            default_index: default_index.map(str::to_string),
            refresh,
            cluster_state,
            metadata: HashMap::new(),
            targets: HashMap::new(),
            dispatched: false,
            lease: lease.clone(),
        };
        let mut errors = false;
        loop {
            let (next, documents) = pipeline::next_batch(decoder).await?;
            decoder = next;
            if documents.is_empty() {
                break;
            }
            crate::metrics::BULK_EXECUTION_BATCHES_TOTAL.inc();
            dispatched = true;
            let (next_execution, results) = execution.owned_batch(documents).await?;
            execution = next_execution;
            dispatched = execution.dispatched;
            errors |= results.iter().any(|result| {
                result
                    .item
                    .as_object()
                    .is_some_and(|object| object.values().any(|item| item.get("error").is_some()))
            });
            let count = results.len() as u64;
            spool = pipeline::store_results(spool, results).await?;
            completed_items += count;
        }
        drop(decoder);
        if completed_items != staged.item_count {
            return Err(BulkError::Io(std::io::Error::other(format!(
                "Bulk spool item count changed: expected {}, completed {completed_items}",
                staged.item_count
            ))));
        }
        let reports = execution.refresh_reports().await?;
        let spool = pipeline::finish_results(spool).await?;
        Ok(pipeline::response(
            spool,
            reports,
            errors,
            pretty,
            &state.bulk_runtime,
            lease.clone(),
        ))
    }
    .await;
    match result {
        Ok(response) => {
            scope.handoff();
            response
        }
        Err(error) => {
            lease.fail();
            tracing::error!(error = %error, dispatched, completed_items, "Bulk pipeline failed");
            error.response(dispatched, completed_items)
        }
    }
}

pub async fn bulk_index_global(
    State(state): State<AppState>,
    principal: Option<axum::extract::Extension<crate::security::Principal>>,
    Query(refresh): Query<RefreshParam>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Response {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();
    execute_bulk(
        &state,
        principal.as_ref().map(|principal| &principal.0),
        None,
        &refresh,
        crate::api::wants_pretty(query.as_deref()),
        body,
    )
    .await
}

pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index): Path<crate::common::IndexName>,
    principal: Option<axum::extract::Extension<crate::security::Principal>>,
    Query(refresh): Query<RefreshParam>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Response {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();
    execute_bulk(
        &state,
        principal.as_ref().map(|principal| &principal.0),
        Some(&index),
        &refresh,
        crate::api::wants_pretty(query.as_deref()),
        body,
    )
    .await
}
