//! Strict NDJSON action parsing and ordered per-shard bulk execution.

use super::*;
use crate::engine::WriteCondition;
use crate::transport::proto::{ShardBulkItemResponse, ShardBulkOpKind, ShardBulkOperation};

#[derive(Debug)]
pub(super) struct BulkDoc {
    pub action: String,
    pub doc_id: String,
    pub index: Option<String>,
    pub payload: Value,
    pub condition: WriteCondition,
    pub retry_on_conflict: u32,
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
}

pub(super) type BulkTargetKey = (String, String, u32);

#[derive(Debug, Clone)]
pub(super) struct BulkTargetFailure {
    pub status: StatusCode,
    pub error_type: String,
    pub reason: String,
}

impl BulkTargetFailure {
    pub(super) fn internal(reason: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "shard_failure".into(),
            reason,
        }
    }

    pub(super) fn from_forward_error(error: anyhow::Error) -> Self {
        let (status, error_type) = forwarded_write_error_classification(&error);
        Self {
            status,
            error_type: error_type.into(),
            reason: error.to_string(),
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
        }
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
    let mut result = serde_json::json!({
        "_id": doc_id, "status": status.as_u16(),
        "error": {"type": error_type, "reason": reason.to_string()}
    });
    if let Some(index_name) = index_name {
        result["_index"] = serde_json::json!(index_name);
    }
    serde_json::json!({(action): result})
}

fn wire_item_json(index: &str, item: ShardBulkItemResponse) -> Value {
    let mut result =
        serde_json::json!({"_index": index, "_id": item.doc_id, "status": item.status});
    if item.error.is_empty() {
        result["result"] = serde_json::json!(item.result);
        result["_shards"] = serde_json::json!({"total": 1, "successful": 1, "failed": 0});
    } else {
        result["error"] = serde_json::json!({"type": item.error_type, "reason": item.error});
    }
    if let Some(seq_no) = item.seq_no {
        result["_seq_no"] = serde_json::json!(seq_no);
    }
    if let Some(term) = item.primary_term {
        result["_primary_term"] = serde_json::json!(term);
    }
    result
}

pub(super) fn parse_bulk_ndjson(text: &str) -> Result<Vec<BulkDoc>, String> {
    let mut documents = Vec::new();
    let mut lines = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty());
    while let Some((line, action_line)) = lines.next() {
        let line = line + 1;
        let malformed =
            |reason: String| format!("Malformed action/metadata line [{line}]: {reason}");
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
        if retries > 0 && action != "update" {
            return Err(malformed(
                "retry_on_conflict is only supported for update".into(),
            ));
        }
        let retry_on_conflict = u32::try_from(retries)
            .map_err(|_| malformed("retry_on_conflict exceeds the supported range".into()))?;
        let doc_id = string("_id")?.unwrap_or_else(|| {
            if matches!(action.as_str(), "index" | "create") {
                uuid::Uuid::new_v4().to_string()
            } else {
                String::new()
            }
        });
        let index = string("_index")?;
        let (payload, source_error) = if action == "delete" {
            (Value::Null, None)
        } else {
            let (source_line, source) = lines
                .next()
                .ok_or_else(|| malformed(format!("action [{action}] requires a source line")))?;
            match serde_json::from_str(source) {
                Ok(payload) => (payload, None),
                Err(error) => (
                    Value::Null,
                    Some(format!(
                        "Malformed source line [{}]: {error}",
                        source_line + 1
                    )),
                ),
            }
        };
        documents.push(BulkDoc {
            action: action.clone(),
            doc_id,
            index,
            payload,
            condition,
            retry_on_conflict,
            source_error,
        });
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
                StatusCode::INTERNAL_SERVER_ERROR,
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
            StatusCode::INTERNAL_SERVER_ERROR,
            "node_not_found_exception",
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
    })
}

async fn forward_bulk_batches(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    routed_docs: &mut [RoutedBulkDoc],
) -> BulkTargetResults {
    let mut batches: HashMap<BulkTargetKey, Vec<&mut RoutedBulkDoc>> = HashMap::new();
    for document in routed_docs {
        batches
            .entry((
                document.index_name.clone(),
                document.node_id.clone(),
                document.shard_id,
            ))
            .or_default()
            .push(document);
    }
    join_all(batches.into_iter().map(|(key, mut batch)| async move {
        let Some(node) = cluster_state.nodes.get(&key.1) else {
            return (
                key,
                Err(BulkTargetFailure::internal(
                    "Primary node missing from cluster state".into(),
                )),
            );
        };
        let mut results = Vec::with_capacity(batch.len());
        let mut cursor = 0;
        while cursor < batch.len() {
            let document = &mut *batch[cursor];
            if document.action == "update" {
                let (if_seq_no, if_primary_term) = document.condition.expected_version();
                let (status, Json(mut response)) = execute_update(
                    state,
                    &document.index_name,
                    &document.doc_id,
                    document.payload.take(),
                    &UpdateParams {
                        if_seq_no,
                        if_primary_term,
                        retry_on_conflict: document.retry_on_conflict,
                        refresh: None,
                    },
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
            while cursor < batch.len() && batch[cursor].action != "update" {
                cursor += 1;
            }
            let run = &mut batch[start..cursor];
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
                    (
                        document.doc_id.clone(),
                        document.payload.take(),
                        ShardBulkOperation {
                            kind: kind as i32,
                            if_seq_no,
                            if_primary_term,
                        },
                    )
                })
                .collect::<Vec<_>>();
            match state
                .transport_client
                .forward_bulk_operations_to_shard(node, &key.0, key.2, &operations)
                .await
            {
                Ok(items) => {
                    results.extend(items.into_iter().map(|item| wire_item_json(&key.0, item)))
                }
                Err(error) => {
                    let failure = BulkTargetFailure::from_forward_error(error);
                    for document in run {
                        let item = bulk_error_item(
                            &document.action,
                            Some(&document.index_name),
                            &document.doc_id,
                            failure.status,
                            &failure.error_type,
                            &failure.reason,
                        );
                        results.push(item[&document.action].clone());
                    }
                }
            }
        }
        (key, Ok(results))
    }))
    .await
    .into_iter()
    .collect()
}

pub(super) fn finalize_bulk_items(
    mut items: Vec<Option<Value>>,
    routed_docs: Vec<RoutedBulkDoc>,
    outcomes: &BulkTargetResults,
) -> Vec<Value> {
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
            Some(Err(failure)) => bulk_error_item(
                &document.action,
                Some(&document.index_name),
                &document.doc_id,
                failure.status,
                &failure.error_type,
                &failure.reason,
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

async fn bulk_metadata(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    principal: Option<&crate::security::Principal>,
    index: &str,
) -> Result<IndexMetadata, BulkTargetFailure> {
    if crate::security::is_protected_system_index(index) {
        return Err(BulkTargetFailure {
            status: StatusCode::FORBIDDEN,
            error_type: "security_exception".into(),
            reason: format!(
                "index [{index}] is a protected system index and cannot be accessed through ordinary index APIs"
            ),
        });
    }
    crate::common::IndexName::new(index.to_string()).map_err(|reason| BulkTargetFailure {
        status: StatusCode::BAD_REQUEST,
        error_type: "invalid_index_name_exception".into(),
        reason: reason.to_string(),
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
        })?;
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

async fn execute_bulk(
    state: &AppState,
    principal: Option<&crate::security::Principal>,
    default_index: Option<&str>,
    refresh: &RefreshParam,
    body: &[u8],
) -> (StatusCode, Json<Value>) {
    let text = match std::str::from_utf8(body) {
        Ok(text) => text,
        Err(error) => return illegal_argument(format!("Invalid UTF-8 bulk body: {error}")),
    };
    let documents = match parse_bulk_ndjson(text) {
        Ok(documents) => documents,
        Err(error) => return illegal_argument(error),
    };
    if documents.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({"took": 0, "errors": false, "items": []})),
        );
    }
    let cluster_state = state.cluster_manager.get_state();
    if let Some(index) = default_index
        && let Some(metadata) = cluster_state.indices.get(index)
        && documents
            .iter()
            .all(|document| document.index.as_deref().unwrap_or(index) == index)
        && let Some(response) = crate::api::reject_write_if_engine_read_only(metadata)
    {
        return response;
    }
    let mut items = vec![None; documents.len()];
    let mut metadata: HashMap<String, Result<IndexMetadata, BulkTargetFailure>> = HashMap::new();
    let mut routed = Vec::new();
    for (position, document) in documents.into_iter().enumerate() {
        let index = document
            .index
            .as_deref()
            .map(str::to_owned)
            .or_else(|| default_index.map(str::to_string));
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
        if !metadata.contains_key(&index) {
            metadata.insert(
                index.clone(),
                bulk_metadata(state, &cluster_state, principal, &index).await,
            );
        }
        let index_metadata = match &metadata[&index] {
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
            &cluster_state,
        ) {
            Ok(mut target) => {
                target.action = document.action;
                target.condition = document.condition;
                target.retry_on_conflict = document.retry_on_conflict;
                routed.push(target);
            }
            Err(item) => {
                let result = item["index"].clone();
                items[position] = Some(serde_json::json!({(document.action): result}));
            }
        }
    }
    let outcomes = forward_bulk_batches(state, &cluster_state, &mut routed).await;
    if refresh.should_refresh() {
        let indices = routed
            .iter()
            .map(|document| &document.index_name)
            .collect::<std::collections::HashSet<_>>();
        for index in indices {
            for (shard_id, engine) in state.shard_manager.get_index_shards(index) {
                if let Err(error) = refresh_engine_after_write(engine).await {
                    tracing::error!("Post-bulk refresh failed for {index}/{shard_id}: {error}");
                }
            }
        }
    }
    let items = finalize_bulk_items(items, routed, &outcomes);
    let errors = items.iter().any(|item| {
        item.as_object()
            .is_some_and(|object| object.values().any(|result| result.get("error").is_some()))
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({"took": 0, "errors": errors, "items": items})),
    )
}

pub async fn bulk_index_global(
    State(state): State<AppState>,
    principal: Option<axum::extract::Extension<crate::security::Principal>>,
    Query(refresh): Query<RefreshParam>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();
    execute_bulk(
        &state,
        principal.as_ref().map(|principal| &principal.0),
        None,
        &refresh,
        &body,
    )
    .await
}

pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index): Path<crate::common::IndexName>,
    principal: Option<axum::extract::Extension<crate::security::Principal>>,
    Query(refresh): Query<RefreshParam>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();
    execute_bulk(
        &state,
        principal.as_ref().map(|principal| &principal.0),
        Some(&index),
        &refresh,
        &body,
    )
    .await
}
