use crate::api::AppState;
use crate::cluster::state::{CreateIndexMetadataError, IndexMetadata};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::api::search::failures::{ShardFailure, all_shards_failed_response, shard_stats};
use crate::api::{raft_write, resolve_leader_or_master};

fn is_document_validation_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
        .is_some_and(|status| status.code() == tonic::Code::InvalidArgument)
}

fn forwarded_write_error_classification(error: &anyhow::Error) -> (StatusCode, &'static str) {
    if is_document_validation_error(error) {
        return (StatusCode::BAD_REQUEST, "mapper_parsing_exception");
    }
    let status = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>());
    match status.map(tonic::Status::code) {
        Some(tonic::Code::AlreadyExists) => {
            (StatusCode::CONFLICT, "version_conflict_engine_exception")
        }
        Some(tonic::Code::NotFound) => (StatusCode::NOT_FOUND, "index_not_found_exception"),
        Some(tonic::Code::ResourceExhausted)
            if status.is_some_and(|status| {
                status
                    .message()
                    .starts_with(crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX)
            }) =>
        {
            (
                StatusCode::TOO_MANY_REQUESTS,
                "version_map_capacity_exceeded",
            )
        }
        Some(tonic::Code::Aborted) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
        ),
        Some(tonic::Code::Unavailable)
            if status.is_some_and(crate::transport::state_wait::is_state_wait_timeout) =>
        {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
            )
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "forward_exception"),
    }
}

pub(crate) fn retryable_forward_error_response(
    error: &anyhow::Error,
) -> Option<(StatusCode, Json<Value>)> {
    let (status, error_type) = forwarded_write_error_classification(error);
    (status == StatusCode::SERVICE_UNAVAILABLE)
        .then(|| crate::api::error_response(status, error_type, format!("{error:#}")))
}

fn document_write_error_response(
    operation: &str,
    error: anyhow::Error,
) -> (StatusCode, Json<Value>) {
    let (status, error_type) = forwarded_write_error_classification(&error);
    if matches!(status, StatusCode::CONFLICT | StatusCode::NOT_FOUND)
        && let Some(error) = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<tonic::Status>())
    {
        return crate::api::error_response(status, error_type, error.message());
    }
    crate::api::error_response(status, error_type, format!("{operation} failed: {error:#}"))
}

fn mapper_parsing_error_response(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    crate::api::error_response(StatusCode::BAD_REQUEST, "mapper_parsing_exception", error)
}

fn validate_document_source_for_api(source: &Value) -> Result<(), (StatusCode, Json<Value>)> {
    crate::common::validate_document_source(source).map_err(mapper_parsing_error_response)
}

mod bulk;
#[cfg(test)]
mod forwarding_tests;
mod maintenance;

pub use bulk::{bulk_index, bulk_index_global};
pub use maintenance::{flush_index, force_merge_index, refresh_index};

#[cfg(test)]
use crate::transport::server::MaintenanceDispatchOp;
#[cfg(test)]
use bulk::{RoutedBulkDoc, finalize_bulk_items, parse_bulk_ndjson, route_bulk_doc};
#[cfg(test)]
use maintenance::{
    enqueue_force_merge_tasks, fan_out_maintenance, maintenance_fanout_concurrency,
    spawn_maintenance_job,
};

pub(crate) struct DistributedDslSearchResult {
    pub all_hits: Vec<Value>,
    pub total_hits: usize,
    pub successful_shards: u32,
    pub failed_shards: u32,
    pub shard_failures: Vec<ShardFailure>,
    pub aggregations: HashMap<String, Value>,
    pub partial_aggs: Vec<HashMap<String, crate::search::PartialAggResult>>,
    pub remote_store_stats: Option<RemoteStoreSearchStats>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RemoteStoreSearchStats {
    pub published_splits: usize,
    pub candidate_splits: usize,
    pub pruned_splits: usize,
    pub assigned_splits: usize,
}

impl RemoteStoreSearchStats {
    pub(crate) fn to_response_json(&self) -> Value {
        serde_json::json!({
            "pruning": {
                "published_splits": self.published_splits,
                "candidate_splits": self.candidate_splits,
                "pruned_splits": self.pruned_splits,
                "assigned_splits": self.assigned_splits,
            }
        })
    }
}

pub(crate) async fn ensure_local_index_shards_open(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    context: &str,
) -> Vec<(u32, Arc<dyn crate::engine::SearchEngine>)> {
    let cluster_state = state.cluster_manager.get_state();
    for (shard_id, routing) in &metadata.shard_routing {
        let authoritative_here = routing.primary == state.local_node_id
            || routing.is_replica_in_sync(&state.local_node_id);
        if !authoritative_here {
            continue;
        }
        let Some(allocation_id) =
            cluster_state.shard_allocation_id(index_name, *shard_id, &state.local_node_id)
        else {
            tracing::error!(
                "{}: refusing to serve {}/{} because the local assignment has no allocation ID",
                context,
                index_name,
                shard_id
            );
            continue;
        };
        if state
            .shard_manager
            .get_shard(index_name, *shard_id)
            .is_some()
        {
            if let Err(error) = state.shard_manager.validate_open_copy_identity(
                index_name,
                *shard_id,
                metadata.uuid.as_str(),
                allocation_id,
            ) {
                tracing::error!(
                    "{}: refusing to serve {}/{} because the local copy identity is invalid: {}",
                    context,
                    index_name,
                    shard_id,
                    error
                );
            }
            continue;
        }

        let expected_dir = state
            .shard_manager
            .data_dir()
            .join(&metadata.uuid)
            .join(format!("shard_{shard_id}"));
        if !expected_dir.exists() {
            tracing::error!(
                "{}: refusing to create fresh shard data for {}/{} on a read/maintenance path because {:?} is missing",
                context,
                index_name,
                shard_id,
                expected_dir
            );
            continue;
        }

        let assignment = crate::shard::AssignedShardOpen {
            allocation_id,
            primary_term: routing.primary_term,
            allow_empty_creation: false,
        };
        let open_result = if routing.primary == state.local_node_id {
            state
                .shard_manager
                .open_primary_assigned_shard_with_settings_blocking(
                    index_name.to_string(),
                    *shard_id,
                    metadata.mappings.clone(),
                    metadata.settings.clone(),
                    metadata.uuid.clone(),
                    assignment,
                )
                .await
        } else {
            state
                .shard_manager
                .open_assigned_shard_with_settings_blocking(
                    index_name.to_string(),
                    *shard_id,
                    metadata.mappings.clone(),
                    metadata.settings.clone(),
                    metadata.uuid.clone(),
                    assignment,
                )
                .await
        };
        if let Err(e) = open_result {
            tracing::error!(
                "{}: failed to open shard {}/{}: {}",
                context,
                index_name,
                shard_id,
                e
            );
        }
    }

    metadata
        .shard_routing
        .iter()
        .filter_map(|(shard_id, routing)| {
            let authoritative_here = routing.primary == state.local_node_id
                || routing.is_replica_in_sync(&state.local_node_id);
            let allocation_id =
                cluster_state.shard_allocation_id(index_name, *shard_id, &state.local_node_id)?;
            if !authoritative_here
                || state
                    .shard_manager
                    .validate_open_copy_identity(
                        index_name,
                        *shard_id,
                        metadata.uuid.as_str(),
                        allocation_id,
                    )
                    .is_err()
            {
                return None;
            }
            state
                .shard_manager
                .get_shard(index_name, *shard_id)
                .map(|engine| (*shard_id, engine))
        })
        .collect()
}

pub(crate) async fn index_cluster_state(
    state: &AppState,
    index_name: &str,
) -> Result<crate::cluster::state::ClusterState, (StatusCode, Json<Value>)> {
    state
        .cluster_manager
        .wait_for_version(state.transport_client.required_state_version())
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                error,
            )
        })?;
    let current = state.cluster_manager.get_state();
    if current.indices.contains_key(index_name) || state.raft.is_leader() {
        return Ok(current);
    }
    let Some(master) = resolve_leader_or_master(state, "index metadata lookup")? else {
        return Ok(current);
    };
    let catch_up = async {
        let version = state
            .transport_client
            .get_cluster_state_version(&master, &state.local_node_id)
            .await?;
        state.cluster_manager.wait_for_version(version).await?;
        Ok::<_, anyhow::Error>(state.cluster_manager.get_state())
    };
    match tokio::time::timeout(state.cluster_manager.forwarding_wait_timeout(), catch_up).await {
        Ok(Ok(current)) => Ok(current),
        Ok(Err(error)) => Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!("index [{index_name}] cluster state catch-up failed: {error:#}"),
        )),
        Err(error) => Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!(
                "timed out waiting for index [{index_name}] cluster state; local version {}: {error}",
                state.cluster_manager.version()
            ),
        )),
    }
}

async fn wait_for_index_metadata(state: &AppState, index_name: &str) -> Option<IndexMetadata> {
    let deadline = tokio::time::Instant::now() + state.cluster_manager.forwarding_wait_timeout();
    loop {
        let cluster_state = state.cluster_manager.get_state();
        if let Some(metadata) = cluster_state.indices.get(index_name) {
            return Some(metadata.clone());
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return None;
        }
        tokio::time::sleep_until(deadline.min(now + std::time::Duration::from_millis(25))).await;
    }
}

/// Auto-create an index with 1 shard, respecting the coordinator pattern.
/// If this node is NOT the Raft leader, forwards to the master.
async fn auto_create_index(
    state: &AppState,
    index_name: &str,
    _cluster_state: &crate::cluster::state::ClusterState,
) -> Result<IndexMetadata, (StatusCode, Json<Value>)> {
    tracing::warn!(
        "Index '{}' not found, auto-creating with 1 shard",
        index_name
    );
    let current = state.cluster_manager.get_state();
    let data_nodes = current
        .nodes
        .values()
        .filter(|node| node.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    let body = serde_json::json!({
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
    });
    let m = IndexMetadata::from_create_request_body(index_name, &body, &data_nodes)
        .map_err(create_index_error_response)?;
    let created_metadata = if let Some(master) =
        resolve_leader_or_master(state, "auto-create index")?
    {
        // Forward auto-create to the leader via gRPC
        let body_bytes = serde_json::to_vec(&body).map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "serialization_exception",
                error,
            )
        })?;
        match state
            .transport_client
            .forward_create_index(&master, index_name, &body_bytes)
            .await
        {
            Ok(_) => match wait_for_index_metadata(state, index_name).await {
                Some(metadata) => metadata,
                None => {
                    return Err(crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        format!(
                            "Index [{index_name}] was created by the leader but the local cluster state has not caught up yet"
                        ),
                    ));
                }
            },
            Err(e) => {
                if let Some(response) = forwarded_create_index_error_response(&e) {
                    return Err(response);
                }
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Auto-create index forward to master failed: {e}"),
                ));
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::CreateIndex {
            metadata: m.clone(),
        };
        raft_write(state, cmd).await?;
        wait_for_index_metadata(state, index_name).await.ok_or_else(|| crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE, "shard_not_available_exception",
            format!("index [{index_name}] is not present in local cluster state at version {} after creation", state.cluster_manager.version()),
        ))?
    };

    let committed_state = state.cluster_manager.get_state();
    if let Some(routing) = created_metadata.shard_routing.get(&0)
        && routing.primary == state.local_node_id
        && let Some(allocation_id) =
            committed_state.shard_allocation_id(index_name, 0, &state.local_node_id)
        && let Err(e) = state
            .shard_manager
            .open_primary_assigned_shard_with_settings_blocking(
                created_metadata.name.clone(),
                0,
                created_metadata.mappings.clone(),
                created_metadata.settings.clone(),
                created_metadata.uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: routing.primary_term,
                    allow_empty_creation: committed_state.may_create_initial_empty_copy(
                        index_name,
                        0,
                        &state.local_node_id,
                    ),
                },
            )
            .await
    {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!("Failed to open auto-created shard [{index_name}][0]: {e:#}"),
        ));
    }

    state
        .transport_client
        .clone()
        .with_cluster_manager(state.cluster_manager.clone())
        .open_remote_index_primaries(&committed_state, index_name, &state.local_node_id)
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                format!("Auto-created index [{index_name}] primary opening failed: {error:#}"),
            )
        })?;

    Ok(created_metadata)
}

#[derive(serde::Deserialize, Default)]
pub struct UnsupportedWriteParams {
    pub wait_for_active_shards: Option<String>,
    #[serde(flatten)]
    pub additional: HashMap<String, String>,
}

fn validate_write_parameter(name: &str, value: Option<&str>) -> Result<(), String> {
    if matches!(
        name,
        "routing"
            | "_routing"
            | "pipeline"
            | "version"
            | "_version"
            | "version_type"
            | "_version_type"
            | "require_alias"
            | "require_data_stream"
            | "dynamic_templates"
            | "if_seq_no"
            | "if_primary_term"
            | "op_type"
            | "retry_on_conflict"
            | "refresh"
    ) || (name == "wait_for_active_shards" && value != Some("1"))
    {
        return Err(format!("request parameter [{name}] is not supported"));
    }
    Ok(())
}

impl UnsupportedWriteParams {
    fn validate(&self) -> Result<(), (StatusCode, Json<Value>)> {
        if let Some(value) = self.wait_for_active_shards.as_deref() {
            validate_write_parameter("wait_for_active_shards", Some(value))
                .map_err(illegal_argument)?;
        }
        for (name, value) in &self.additional {
            validate_write_parameter(name, Some(value)).map_err(illegal_argument)?;
        }
        Ok(())
    }
}

fn validate_refresh_parameter(refresh: Option<&str>) -> Result<(), (StatusCode, Json<Value>)> {
    match refresh {
        None | Some("true" | "false" | "") => Ok(()),
        Some(value) => Err(illegal_argument(format!(
            "request parameter [refresh] with value [{value}] is not supported"
        ))),
    }
}

/// Query parameters for bulk writes, including `?refresh=true|false`.
#[derive(serde::Deserialize, Default)]
pub struct RefreshParam {
    pub refresh: Option<String>,
    #[serde(flatten)]
    pub unsupported: UnsupportedWriteParams,
}

impl RefreshParam {
    fn validate(&self) -> Result<(), (StatusCode, Json<Value>)> {
        self.unsupported.validate()?;
        validate_refresh_parameter(self.as_deref())
    }

    /// Returns true when the caller explicitly requested an immediate refresh.
    /// OpenSearch treats `?refresh`, `?refresh=true`, and `?refresh=""` as "refresh now".
    fn should_refresh(&self) -> bool {
        matches!(self.as_deref(), Some("true") | Some(""))
    }

    fn as_deref(&self) -> Option<&str> {
        self.refresh.as_deref()
    }
}

#[derive(Default, serde::Deserialize)]
pub struct WriteParams {
    pub refresh: Option<String>,
    pub if_seq_no: Option<u64>,
    pub if_primary_term: Option<u64>,
    pub op_type: Option<String>,
    #[serde(flatten)]
    pub unsupported: UnsupportedWriteParams,
}

fn illegal_argument(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    crate::api::error_response(StatusCode::BAD_REQUEST, "illegal_argument_exception", error)
}

impl WriteParams {
    fn condition(&self) -> Result<crate::engine::WriteCondition, (StatusCode, Json<Value>)> {
        self.unsupported.validate()?;
        validate_refresh_parameter(self.refresh.as_deref())?;
        let condition = crate::engine::WriteCondition::from_optional_values(
            self.if_seq_no,
            self.if_primary_term,
        )
        .map_err(illegal_argument)?;
        match self.op_type.as_deref() {
            None | Some("index") => Ok(condition),
            Some("create") if condition == crate::engine::WriteCondition::Unconditional => {
                Ok(crate::engine::WriteCondition::Create)
            }
            Some("create") => Err(illegal_argument(
                "create operations cannot use if_seq_no or if_primary_term",
            )),
            Some(value) => Err(illegal_argument(format!("invalid op_type [{value}]"))),
        }
    }

    fn should_refresh(&self) -> bool {
        matches!(self.refresh.as_deref(), Some("true") | Some(""))
    }
}

#[derive(Default, serde::Deserialize)]
pub struct GetParams {
    pub realtime: Option<bool>,
}

#[derive(Default, serde::Deserialize)]
pub struct UpdateParams {
    pub refresh: Option<String>,
    pub if_seq_no: Option<u64>,
    pub if_primary_term: Option<u64>,
    #[serde(default)]
    pub retry_on_conflict: u32,
    #[serde(flatten)]
    pub unsupported: UnsupportedWriteParams,
}

impl UpdateParams {
    fn validate(&self) -> Result<(), (StatusCode, Json<Value>)> {
        self.unsupported.validate()?;
        validate_refresh_parameter(self.refresh.as_deref())
    }
}

async fn refresh_engine_after_write(
    engine: Arc<dyn crate::engine::SearchEngine>,
) -> crate::common::Result<()> {
    crate::worker::spawn_engine_maintenance("post-write refresh", move || engine.refresh()).await
}

/// HEAD /{index} — Check if an index exists.
pub async fn index_exists(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> StatusCode {
    let cluster_state = state.cluster_manager.get_state();
    if cluster_state.indices.contains_key(index_name.as_str()) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

fn create_index_error_response(error: CreateIndexMetadataError) -> (StatusCode, Json<Value>) {
    match error {
        CreateIndexMetadataError::NoDataNodes => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "no_data_nodes_exception",
            CreateIndexMetadataError::NoDataNodes,
        ),
        CreateIndexMetadataError::InvalidArgument(message) => crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            message,
        ),
        CreateIndexMetadataError::MapperParsing(message) => mapper_parsing_error_response(message),
        CreateIndexMetadataError::UnimplementedEngine(engine) => crate::api::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "illegal_argument_exception",
            CreateIndexMetadataError::UnimplementedEngine(engine),
        ),
    }
}

fn forwarded_create_index_error_response(
    error: &anyhow::Error,
) -> Option<(StatusCode, Json<Value>)> {
    let status = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())?;

    match status.code() {
        tonic::Code::InvalidArgument => {
            let error_type = if crate::common::is_mapping_parsing_error_message(status.message()) {
                "mapper_parsing_exception"
            } else {
                "illegal_argument_exception"
            };
            Some(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                error_type,
                status.message(),
            ))
        }
        tonic::Code::Unimplemented => Some(crate::api::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "illegal_argument_exception",
            status.message(),
        )),
        tonic::Code::Unavailable => Some(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!("{error:#}"),
        )),
        tonic::Code::Internal
            if status.message() == CreateIndexMetadataError::NoDataNodes.to_string() =>
        {
            Some(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "no_data_nodes_exception",
                status.message(),
            ))
        }
        _ => None,
    }
}

/// PUT /{index} — Create an index.
/// Body: `{ "engine": "local_shards", "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }`
pub async fn create_index(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Query(params): Query<UnsupportedWriteParams>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = params.validate() {
        return response;
    }
    // IndexName is validated at extraction time

    let settings: Value = serde_json::from_slice(&body).unwrap_or(serde_json::json!({}));

    let cluster_state = state.cluster_manager.get_state();

    if cluster_state.indices.contains_key(index_name.as_str()) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "resource_already_exists_exception",
            format!("index [{index_name}] already exists"),
        );
    }

    // Build shard assignment: distribute shards round-robin across Data nodes
    let data_nodes: Vec<String> = cluster_state
        .nodes
        .values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| n.id.clone())
        .collect();

    let metadata =
        match IndexMetadata::from_create_request_body(&index_name, &settings, &data_nodes) {
            Ok(metadata) => metadata,
            Err(error) => return create_index_error_response(error),
        };

    let index_settings = metadata.settings.clone();
    let replica_count = metadata.number_of_replicas;

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "index creation") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_create_index(&master, &index_name, &body)
            .await
        {
            Ok(resp) => return (StatusCode::OK, Json(resp)),
            Err(e) => {
                if let Some(response) = forwarded_create_index_error_response(&e) {
                    return response;
                }
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward index creation to master: {e}"),
                );
            }
        }
    }
    let cmd = crate::consensus::types::ClusterCommand::CreateIndex { metadata };
    if let Err(e) = raft_write(&state, cmd).await {
        return e;
    }

    let committed_state = state.cluster_manager.get_state();
    let Some(committed_metadata) = committed_state.indices.get(index_name.as_str()) else {
        return crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "master_not_discovered_exception",
            format!("Index [{index_name}] was committed but is not visible locally"),
        );
    };

    // Only the initial primary may create an empty local copy. Initial replicas
    // remain out of sync and are populated by peer recovery.
    for (shard_id, routing) in &committed_metadata.shard_routing {
        if routing.primary == state.local_node_id
            && let Some(allocation_id) = committed_state.shard_allocation_id(
                index_name.as_str(),
                *shard_id,
                &state.local_node_id,
            )
            && let Err(e) = state
                .shard_manager
                .open_primary_assigned_shard_with_settings_blocking(
                    index_name.to_string(),
                    *shard_id,
                    committed_metadata.mappings.clone(),
                    committed_metadata.settings.clone(),
                    committed_metadata.uuid.clone(),
                    crate::shard::AssignedShardOpen {
                        allocation_id,
                        primary_term: routing.primary_term,
                        allow_empty_creation: committed_state.may_create_initial_empty_copy(
                            index_name.as_str(),
                            *shard_id,
                            &state.local_node_id,
                        ),
                    },
                )
                .await
        {
            return crate::api::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                format!(
                    "Failed to open primary shard [{index_name}][{shard_id}] after creation: {e:#}"
                ),
            );
        }
    }

    if let Err(error) = state
        .transport_client
        .clone()
        .with_cluster_manager(state.cluster_manager.clone())
        .open_remote_index_primaries(&committed_state, &index_name, &state.local_node_id)
        .await
    {
        return crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!("Index [{index_name}] was created but primary shard opening failed: {error:#}"),
        );
    }

    tracing::info!(
        "Created index '{}' with engine {}, {} shards, {} replicas",
        index_name,
        index_settings.engine,
        committed_metadata.shard_routing.len(),
        replica_count
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index_name
        })),
    )
}

/// POST /{index}/_doc — Index a single document with shard routing.
pub async fn index_document(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Query(refresh_param): Query<WriteParams>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    let condition = match refresh_param.condition() {
        Ok(condition) => condition,
        Err(response) => return response,
    };

    // IndexName is validated at extraction time

    if let Err(response) = validate_document_source_for_api(&payload) {
        return response;
    }
    let doc_id = uuid::Uuid::new_v4().to_string();

    let mut cluster_state = match index_cluster_state(&state, &index_name).await {
        Ok(current) => current,
        Err(error) => return error,
    };

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
        m.clone()
    } else {
        match auto_create_index(&state, &index_name, &cluster_state).await {
            Ok(m) => m,
            Err(err_resp) => return err_resp,
        }
    };

    if let Some(resp) = crate::api::reject_write_if_engine_read_only(&metadata) {
        return resp;
    }
    cluster_state = state.cluster_manager.get_state();

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_index_with_condition_to_shard(
            &target_node,
            &index_name,
            shard_id,
            &doc_id,
            &payload,
            condition,
        )
        .await
    {
        Ok(res) => {
            if refresh_param.should_refresh()
                && let Some(engine) = state.shard_manager.get_shard(&index_name, shard_id)
                && let Err(error) = refresh_engine_after_write(engine).await
            {
                tracing::error!(
                    "Post-write refresh failed for {}/{}: {}",
                    index_name,
                    shard_id,
                    error
                );
            }
            (
                if res["result"] == "created" {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                Json(res),
            )
        }
        Err(e) => document_write_error_response("Forward", e),
    }
}

/// PUT /{index}/_doc/{id} — Index a single document with an explicit ID.
pub async fn index_document_with_id(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Query(refresh_param): Query<WriteParams>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    let condition = match refresh_param.condition() {
        Ok(condition) => condition,
        Err(response) => return response,
    };

    // IndexName is validated at extraction time

    if let Err(response) = validate_document_source_for_api(&payload) {
        return response;
    }

    let mut cluster_state = match index_cluster_state(&state, &index_name).await {
        Ok(current) => current,
        Err(error) => return error,
    };

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
        m.clone()
    } else {
        match auto_create_index(&state, &index_name, &cluster_state).await {
            Ok(m) => m,
            Err(err_resp) => return err_resp,
        }
    };

    if let Some(resp) = crate::api::reject_write_if_engine_read_only(&metadata) {
        return resp;
    }
    cluster_state = state.cluster_manager.get_state();

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_index_with_condition_to_shard(
            &target_node,
            &index_name,
            shard_id,
            &doc_id,
            &payload,
            condition,
        )
        .await
    {
        Ok(res) => {
            if refresh_param.should_refresh()
                && let Some(engine) = state.shard_manager.get_shard(&index_name, shard_id)
                && let Err(error) = refresh_engine_after_write(engine).await
            {
                tracing::error!(
                    "Post-write refresh failed for {}/{}: {}",
                    index_name,
                    shard_id,
                    error
                );
            }
            (
                if res["result"] == "created" {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                Json(res),
            )
        }
        Err(e) => document_write_error_response("Forward", e),
    }
}

pub async fn create_document(
    state: State<AppState>,
    path: Path<(crate::common::IndexName, String)>,
    Query(mut params): Query<WriteParams>,
    body: Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Some(value) = params.op_type.as_deref()
        && value != "create"
    {
        return illegal_argument(format!(
            "request parameter [op_type] with value [{value}] is not supported for create operations"
        ));
    }
    params.op_type = Some("create".to_string());
    index_document_with_id(state, path, Query(params), body).await
}

pub(crate) async fn execute_distributed_dsl_search(
    state: &AppState,
    index_name: &str,
    search_req: &crate::search::SearchRequest,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    // IndexName is validated at extraction time

    if search_req.from.checked_add(search_req.size).is_none() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "from [{}] plus size [{}] exceeds the supported pagination range",
                search_req.from, search_req.size
            ),
        ));
    }

    let cluster_state = index_cluster_state(state, index_name).await?;
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            ));
        }
    };

    // remote_store engine: shardless, splits are read from object storage.
    if matches!(
        metadata.settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    ) {
        return crate::engine::remote_store::search(state, index_name, &metadata, search_req).await;
    }

    let mut shard_hit_lists: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut knn_hits = Vec::new();
    let mut successful = 0u32;
    let mut failed = 0u32;
    let mut shard_failures = Vec::new();
    let mut total_hits: usize = 0;
    let is_hybrid = search_req.knn.is_some();

    let mut all_partial_aggs: Vec<HashMap<String, crate::search::PartialAggResult>> = Vec::new();

    let local_shards =
        ensure_local_index_shards_open(state, index_name, &metadata, "DSL search").await;

    // Dispatch all local shard searches in parallel
    let search_futures: Vec<_> = local_shards
        .iter()
        .map(|(shard_id, engine)| {
            let engine = engine.clone();
            let search_req = search_req.clone();
            let shard_id = *shard_id;
            let pools = state.worker_pools.clone();
            async move {
                let result = pools
                    .spawn_search(move || engine.search_query(&search_req))
                    .await;
                (shard_id, result)
            }
        })
        .collect();
    let search_results = futures::future::join_all(search_futures).await;

    for (shard_id, search_result) in search_results {
        match search_result {
            Ok(Ok((hits, shard_total, partial_aggs))) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                let mut shard_list = Vec::with_capacity(hits.len());
                for hit in hits {
                    let mut enriched = serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    });
                    if let Some(sort_vals) = hit.get("sort") {
                        enriched["sort"] = sort_vals.clone();
                    }
                    shard_list.push(enriched);
                }
                shard_hit_lists.push(shard_list);
            }
            Ok(Err(e)) | Err(e) => {
                tracing::error!("Shard {}/{} search failed: {:#}", index_name, shard_id, e);
                shard_failures.push(ShardFailure::from_error(
                    index_name,
                    shard_id,
                    &state.local_node_id,
                    &e,
                ));
                failed += 1;
            }
        }
    }

    if let Some(ref knn) = search_req.knn
        && let Some((field_name, params)) = knn.fields.iter().next()
    {
        for (shard_id, engine) in &local_shards {
            let engine = engine.clone();
            let field_name = field_name.clone();
            let vector = params.vector.clone();
            let k = params.k;
            let filter = params.filter.clone();
            let shard_id = *shard_id;
            let knn_result = state
                .worker_pools
                .spawn_search(move || {
                    engine.search_knn_filtered(&field_name, &vector, k, filter.as_ref())
                })
                .await;
            match knn_result {
                Ok(Ok(hits)) => {
                    for hit in hits {
                        knn_hits.push(serde_json::json!({
                            "_index": index_name,
                            "_shard": shard_id,
                            "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                            "_score": hit.get("_score"),
                            "_source": hit.get("_source"),
                            "_knn_field": hit.get("_knn_field"),
                            "_knn_distance": hit.get("_knn_distance"),
                        }));
                    }
                }
                Ok(Err(e)) | Err(e) => {
                    tracing::error!(
                        "Vector search on {}/shard_{} failed: {}",
                        index_name,
                        shard_id,
                        e
                    );
                    shard_failures.push(ShardFailure::from_error(
                        index_name,
                        shard_id,
                        &state.local_node_id,
                        &e,
                    ));
                    failed += 1;
                }
            }
        }
    }

    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    let mut remote_futures = Vec::new();
    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }
        if let Some(node_info) = cluster_state.nodes.get(&routing.primary) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let index = index_name.to_string();
            let sid = *shard_id;
            let req_clone = search_req.clone();
            let node_id = node_info.id.clone();
            let handle = tokio::spawn(async move {
                client
                    .forward_search_dsl_to_shard(&node_info, &index, sid, &req_clone)
                    .await
            });
            remote_futures.push(async move { (sid, node_id, handle.await) });
        } else {
            failed += 1;
            shard_failures.push(ShardFailure::new(
                index_name,
                *shard_id,
                &routing.primary,
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                format!(
                    "primary node [{}] is not available for shard [{index_name}][{shard_id}]",
                    routing.primary
                ),
            ));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for (shard_id, node_id, result) in remote_results {
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        match result {
            Ok((hits, shard_total, partial_aggs)) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                let mut shard_text = Vec::new();
                for hit in hits {
                    let mut enriched = serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit),
                        "_knn_field": hit.get("_knn_field"),
                        "_knn_distance": hit.get("_knn_distance"),
                    });
                    if let Some(sort_vals) = hit.get("sort") {
                        enriched["sort"] = sort_vals.clone();
                    }
                    if hit.get("_knn_field").is_some() {
                        knn_hits.push(enriched);
                    } else {
                        shard_text.push(enriched);
                    }
                }
                if !shard_text.is_empty() {
                    shard_hit_lists.push(shard_text);
                }
            }
            Err(e) => {
                if let Some(response) = retryable_forward_error_response(&e) {
                    return Err(response);
                }
                tracing::error!(
                    "Remote shard {}/{} search failed: {:#}",
                    index_name,
                    shard_id,
                    e
                );
                shard_failures.push(ShardFailure::from_error(index_name, shard_id, &node_id, &e));
                failed += 1;
            }
        }
    }

    if let Some(error) = all_shards_failed_response(successful, &shard_failures) {
        return Err(error);
    }

    let mut all_hits = if is_hybrid {
        // Hybrid search: flatten shard lists, merge with kNN via RRF
        let text_hits: Vec<serde_json::Value> = shard_hit_lists.into_iter().flatten().collect();
        crate::search::merge_hybrid_hits(text_hits, knn_hits)
    } else {
        // K-way merge of pre-sorted shard hit lists (O(N log K) vs O(N log N))
        crate::search::merge_sorted_hit_lists(shard_hit_lists, &search_req.sort)
    };
    if is_hybrid {
        crate::search::sort_hits(&mut all_hits, &search_req.sort);
    }

    let merged_aggs = if !all_partial_aggs.is_empty() {
        crate::search::merge_aggregations(all_partial_aggs.clone(), &search_req.aggs)
    } else {
        HashMap::new()
    };

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits,
        successful_shards: successful,
        failed_shards: failed,
        shard_failures,
        aggregations: merged_aggs,
        partial_aggs: all_partial_aggs,
        remote_store_stats: None,
    })
}

pub async fn search_documents_dsl(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::SEARCH_LATENCY_SECONDS.start_timer();

    let search_req: crate::search::SearchRequest = match serde_json::from_value(req) {
        Ok(r) => r,
        Err(e) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                format!("Invalid query DSL: {e}"),
            );
        }
    };

    // Validate search_after request shape before dispatching to shards.
    if let Some(cursor) = &search_req.search_after {
        if search_req.sort.is_empty() {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "search_after requires a non-empty `sort` clause",
            );
        }
        if cursor.len() != search_req.sort.len() {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!(
                    "search_after length ({}) must match sort length ({})",
                    cursor.len(),
                    search_req.sort.len()
                ),
            );
        }
        if search_req.from != 0 {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "search_after is incompatible with `from`; set from=0 and paginate via search_after",
            );
        }
        if search_req.knn.is_some() {
            // k-NN merges similarity-ranked hits into the response separately
            // from the BM25/text leg, and the cursor filter is only applied to
            // the text leg. Combining the two would let page 2 re-return page 1
            // kNN hits. Reject the combo until we have a proper kNN cursor.
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "search_after is not supported with k-NN queries",
            );
        }
        for clause in &search_req.sort {
            if let Some((name, _)) = crate::search::sort_clause_name_direction(clause)
                && name == "_score"
            {
                return crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    "search_after does not support sorting by _score",
                );
            }
        }
    }

    let result = match execute_distributed_dsl_search(&state, &index_name, &search_req).await {
        Ok(result) => result,
        Err(err) => return err,
    };

    crate::metrics::SEARCH_QUERIES_TOTAL.inc();

    let mut paginated: Vec<_> = result
        .all_hits
        .into_iter()
        .skip(search_req.from)
        .take(search_req.size)
        .collect();

    // OpenSearch parity:
    //   - When `sort` is present, hits ignore relevance, so each hit's `_score`
    //     is reported as null and the top-level `max_score` is null.
    //   - When `sort` is empty, `_score` is the BM25 score and `max_score` is
    //     the max across returned hits (null if there are no hits).
    let sorted_response = !search_req.sort.is_empty();
    let max_score: serde_json::Value = if sorted_response {
        for hit in paginated.iter_mut() {
            if let Some(obj) = hit.as_object_mut() {
                obj.insert("_score".to_string(), serde_json::Value::Null);
            }
        }
        serde_json::Value::Null
    } else {
        crate::api::search::max_score_from_hits(&paginated)
    };

    let mut response = serde_json::json!({
        "_shards": shard_stats(result.successful_shards, result.failed_shards, &result.shard_failures),
        "hits": {
            "total": { "value": result.total_hits, "relation": "eq" },
            "max_score": max_score,
            "hits": paginated
        }
    });
    if !result.aggregations.is_empty() {
        response["aggregations"] = serde_json::json!(result.aggregations);
    }
    if let Some(stats) = result.remote_store_stats {
        response["remote_store"] = stats.to_response_json();
    }

    (StatusCode::OK, Json(response))
}

/// GET /{index}/_doc/{id} — Retrieve a document by its ID.
pub async fn get_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Query(params): Query<GetParams>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let cluster_state = match index_cluster_state(&state, &index_name).await {
        Ok(current) => current,
        Err(error) => return error,
    };
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            );
        }
    };

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_get_with_index_uuid_to_shard(
            &target_node,
            &index_name,
            shard_id,
            &doc_id,
            params.realtime.unwrap_or(true),
        )
        .await
    {
        Ok(read) => match read.document {
            Some(document) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "_index": index_name, "_index_uuid": read.index_uuid,
                    "_id": doc_id, "_shard": shard_id, "found": true,
                    "_source": document.source, "_seq_no": document.seq_no,
                    "_primary_term": document.primary_term
                })),
            ),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "_index": index_name, "_index_uuid": read.index_uuid,
                    "_id": doc_id, "found": false
                })),
            ),
        },
        Err(error)
            if error
                .chain()
                .find_map(|cause| cause.downcast_ref::<tonic::Status>())
                .is_some_and(|status| {
                    matches!(status.code(), tonic::Code::NotFound | tonic::Code::Aborted)
                        || crate::transport::state_wait::is_state_wait_timeout(status)
                }) =>
        {
            document_write_error_response("Get", error)
        }
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "search_exception",
            format!("{e}"),
        ),
    }
}

/// POST /{index}/_update/{id} — Partial update a document by merging fields.
/// Body: `{ "doc": { "field": "new_value" } }`
/// Fetches the existing document, merges the provided fields, and re-indexes.
pub async fn update_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Query(params): Query<UpdateParams>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    execute_update(&state, &index_name, &doc_id, body, &params).await
}

fn validate_update_body(body: &Value) -> Result<(), (StatusCode, Json<Value>)> {
    let Some(object) = body.as_object() else {
        return Err(illegal_argument("update body must be an object"));
    };
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "doc" | "upsert" | "doc_as_upsert" | "detect_noop"
        ) {
            return Err(illegal_argument(format!(
                "unsupported update field [{key}]"
            )));
        }
    }
    for key in ["doc", "upsert"] {
        if let Some(source) = body.get(key) {
            if !source.is_object() {
                return Err(illegal_argument(format!(
                    "update [{key}] must be an object"
                )));
            }
            validate_document_source_for_api(source)?;
        }
    }
    for key in ["doc_as_upsert", "detect_noop"] {
        if body.get(key).is_some_and(|value| !value.is_boolean()) {
            return Err(illegal_argument(format!(
                "update [{key}] must be a boolean"
            )));
        }
    }
    if body.get("doc").is_none() && (body.get("upsert").is_none() || body["doc_as_upsert"] == true)
    {
        return Err(illegal_argument("update requires a 'doc' object"));
    }
    Ok(())
}

fn merge_update_source(target: &mut Value, partial: Value) -> bool {
    match partial {
        Value::Object(partial) if target.is_object() => {
            let target = target.as_object_mut().expect("checked object");
            let mut changed = false;
            for (key, value) in partial {
                if let Some(existing) = target.get_mut(&key) {
                    changed |= merge_update_source(existing, value);
                } else {
                    target.insert(key, value);
                    changed = true;
                }
            }
            changed
        }
        partial if *target != partial => {
            *target = partial;
            true
        }
        _ => false,
    }
}

async fn resolve_document_primary(
    state: &AppState,
    index_name: &str,
    doc_id: &str,
) -> Result<(IndexMetadata, u32, crate::cluster::state::NodeInfo), (StatusCode, Json<Value>)> {
    let cluster_state = index_cluster_state(state, index_name).await?;
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            ));
        }
    };

    if let Some(resp) = crate::api::reject_write_if_engine_read_only(&metadata) {
        return Err(resp);
    }

    let shard_id = crate::engine::routing::calculate_shard(doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            ));
        }
    };
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            ));
        }
    };

    Ok((metadata, shard_id, target_node))
}

async fn execute_update(
    state: &AppState,
    index_name: &str,
    doc_id: &str,
    mut body: Value,
    params: &UpdateParams,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = params.validate() {
        return response;
    }
    if let Err(response) = validate_update_body(&body) {
        return response;
    }
    let requested = match crate::engine::WriteCondition::from_optional_values(
        params.if_seq_no,
        params.if_primary_term,
    ) {
        Ok(condition) => condition,
        Err(error) => return illegal_argument(error),
    };
    let mut retries = params.retry_on_conflict;
    let mut index_uuid: Option<String> = None;
    loop {
        let (_, shard_id, target_node) =
            match resolve_document_primary(state, index_name, doc_id).await {
                Ok(target) => target,
                Err(response) => return response,
            };
        let read = match state
            .transport_client
            .forward_get_with_index_uuid_to_shard(&target_node, index_name, shard_id, doc_id, true)
            .await
        {
            Ok(document) => document,
            Err(error)
                if error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<tonic::Status>())
                    .is_some_and(|status| {
                        matches!(status.code(), tonic::Code::NotFound | tonic::Code::Aborted)
                            || crate::transport::state_wait::is_state_wait_timeout(status)
                    }) =>
            {
                return document_write_error_response("Get", error);
            }
            Err(error) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "get_exception",
                    error,
                );
            }
        };
        if index_uuid
            .as_ref()
            .is_some_and(|expected| expected != &read.index_uuid)
        {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!(
                    "no such index [{index_name}] for UUID [{}]",
                    index_uuid.as_deref().expect("checked pinned UUID")
                ),
            );
        }
        let index_uuid = index_uuid.get_or_insert(read.index_uuid);
        let existing = read.document;
        if let Err(error) = requested.check(
            doc_id,
            existing
                .as_ref()
                .map(|document| (document.seq_no, document.primary_term)),
        ) {
            return crate::api::error_response(
                StatusCode::CONFLICT,
                "version_conflict_engine_exception",
                error,
            );
        }
        let (source, condition) = if let Some(existing) = existing {
            let mut merged = existing.source;
            let changed = body.get_mut("doc").is_some_and(|partial| {
                let partial = if retries > 0 {
                    partial.clone()
                } else {
                    partial.take()
                };
                merge_update_source(&mut merged, partial)
            });
            if body
                .get("detect_noop")
                .and_then(Value::as_bool)
                .unwrap_or(true)
                && !changed
            {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "_index": index_name, "_id": doc_id,
                        "_seq_no": existing.seq_no, "_primary_term": existing.primary_term,
                        "result": "noop",
                        "_shards": {"total": 0, "successful": 0, "failed": 0}
                    })),
                );
            }
            (
                merged,
                crate::engine::WriteCondition::IfMatch {
                    seq_no: existing.seq_no,
                    primary_term: existing.primary_term,
                },
            )
        } else {
            let key = if body["doc_as_upsert"] == true {
                "doc"
            } else {
                "upsert"
            };
            let source = body.get_mut(key);
            let Some(source) = source else {
                return crate::api::error_response(
                    StatusCode::NOT_FOUND,
                    "document_missing_exception",
                    format!("[{doc_id}]: document missing"),
                );
            };
            (
                if retries > 0 {
                    source.clone()
                } else {
                    source.take()
                },
                crate::engine::WriteCondition::Create,
            )
        };
        if let Err(response) = validate_document_source_for_api(&source) {
            return response;
        }
        let (if_seq_no, if_primary_term) = condition.expected_version();
        let request = crate::transport::proto::ShardDocRequest {
            index_name: index_name.to_owned(),
            shard_id,
            doc_id: doc_id.to_owned(),
            payload_json: match serde_json::to_vec(&source) {
                Ok(payload) => payload,
                Err(error) => {
                    return crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "serialization_exception",
                        error,
                    );
                }
            },
            if_seq_no,
            if_primary_term,
            create_only: condition == crate::engine::WriteCondition::Create,
            index_uuid: Some(index_uuid.clone()),
        };
        match state
            .transport_client
            .forward_index_request_to_shard(&target_node, request)
            .await
        {
            Ok(response) => {
                if matches!(params.refresh.as_deref(), Some("true") | Some(""))
                    && let Some(engine) = state.shard_manager.get_shard(index_name, shard_id)
                    && let Err(error) = refresh_engine_after_write(engine).await
                {
                    tracing::error!(
                        "Post-update refresh failed for {index_name}/{shard_id}: {error}"
                    );
                }
                return (
                    if response["result"] == "created" {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    },
                    Json(response),
                );
            }
            Err(error)
                if retries > 0
                    && error
                        .downcast_ref::<tonic::Status>()
                        .is_some_and(|status| status.code() == tonic::Code::AlreadyExists) =>
            {
                retries -= 1;
            }
            Err(error) => return document_write_error_response("Update", error),
        }
    }
}

/// DELETE /{index}/_doc/{id} — Delete a document by its ID.
pub async fn delete_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Query(params): Query<WriteParams>,
) -> (StatusCode, Json<Value>) {
    let condition = match params.condition() {
        Ok(condition) if params.op_type.is_none() => condition,
        Ok(_) => {
            return illegal_argument("request parameter [op_type] is not supported for delete");
        }
        Err(response) => return response,
    };
    // IndexName is validated at extraction time

    let cluster_state = match index_cluster_state(&state, &index_name).await {
        Ok(current) => current,
        Err(error) => return error,
    };
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            );
        }
    };

    if let Some(resp) = crate::api::reject_write_if_engine_read_only(&metadata) {
        return resp;
    }

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_delete_with_condition_to_shard(
            &target_node,
            &index_name,
            shard_id,
            &doc_id,
            condition,
        )
        .await
    {
        Ok(res) => {
            if params.should_refresh()
                && let Some(engine) = state.shard_manager.get_shard(&index_name, shard_id)
                && let Err(error) = refresh_engine_after_write(engine).await
            {
                tracing::error!(
                    "Post-delete refresh failed for {}/{}: {}",
                    index_name,
                    shard_id,
                    error
                );
            }
            (
                if res["result"] == "not_found" {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::OK
                },
                Json(res),
            )
        }
        Err(e) => document_write_error_response("Delete", e),
    }
}

/// DELETE /{index} — Delete an entire index (remove from cluster state, close shards, delete data).
/// GET /{index}/_settings — Get the current index settings.
pub async fn get_index_settings(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m,
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            );
        }
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            index_name.to_string(): {
                "settings": {
                    "index": {
                        "number_of_shards": metadata.number_of_shards,
                        "number_of_replicas": metadata.number_of_replicas,
                        "engine": metadata.settings.engine.to_string(),
                        "refresh_interval_ms": metadata.settings.refresh_interval_ms,
                        "flush_threshold_bytes": metadata.settings.flush_threshold_bytes,
                        "dynamic": metadata.dynamic.to_string(),
                    }
                }
            }
        })),
    )
}

/// POST /{index}/_remote_store/publish — Build a split from the supplied
/// documents and publish it as a new manifest generation.
///
/// Body format:
/// ```json
/// { "docs": [ {"_id": "a", "title": "..."}, {"title": "..."} ] }
/// ```
///
/// This endpoint is only valid for indices created with `engine: remote_store`.
/// Any other engine is rejected with 400. The publish is executed locally on
/// the node that receives the request. Multi-node readers therefore require
/// every node to use the same shared object-store root; local filesystem roots
/// are suitable only when all work stays on that node, while S3-compatible
/// storage provides a shared backend.
pub async fn publish_remote_store_documents(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name.as_str()),
            );
        }
    };

    if !matches!(
        metadata.settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    ) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] uses engine [{}] which does not support remote_store publish",
                index_name.as_str(),
                metadata.settings.engine
            ),
        );
    }

    let docs_value = match body.get("docs") {
        Some(v) => v,
        None => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "publish body must contain a 'docs' array",
            );
        }
    };
    let docs_array = match docs_value.as_array() {
        Some(arr) => arr,
        None => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "'docs' must be a JSON array",
            );
        }
    };
    let docs: Vec<Value> = docs_array.to_vec();

    match crate::engine::remote_store::publish_docs(&state, index_name.as_str(), &metadata, docs)
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)),
        Err(err_resp) => err_resp,
    }
}

/// POST /{index}/_remote_store/verify — Recompute the sha256 checksum of
/// every published split bundle on this node and compare against the value
/// stored in the current manifest.
///
/// Valid only for `engine: remote_store` indices. Other engines return 400.
/// Missing index returns 404. Per-split outcomes ("ok" / "mismatch" /
/// "missing" / "unsupported") are reported in the response body; the HTTP
/// status stays 200 as long as the request itself is valid, so operators
/// can see full diagnostics even when some splits are corrupt.
///
/// Response shape:
/// ```json
/// {
///   "index": "...",
///   "generation": 6,
///   "splits": [
///     { "split_id": "...", "status": "ok" },
///     { "split_id": "...", "status": "mismatch", "expected": "sha256:...", "actual": "sha256:..." }
///   ],
///   "ok_count": 5,
///   "mismatch_count": 1,
///   "missing_count": 0,
///   "unsupported_count": 0
/// }
/// ```
pub async fn verify_remote_store_splits(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name.as_str()),
            );
        }
    };

    match crate::engine::remote_store::verify_splits(&state, index_name.as_str(), &metadata).await {
        Ok(response) => (StatusCode::OK, Json(response)),
        Err(err_resp) => err_resp,
    }
}

/// PUT /{index}/_settings — Update dynamic index settings.
///
/// Supported dynamic settings:
/// - `index.number_of_replicas` (u32) — adjusts replica count
/// - `index.refresh_interval_ms` (u64 | null) — per-index refresh interval
/// - `index.flush_threshold_bytes` (u64 | null) — auto-flush threshold for the WAL
///
/// Immutable settings (rejected with 400):
/// - `index.number_of_shards`
/// - `index.engine`
///
/// Body format (OpenSearch-compatible):
/// ```json
/// {
///   "index": {
///     "number_of_replicas": 2,
///     "refresh_interval_ms": 10000,
///     "flush_threshold_bytes": 536870912
///   }
/// }
/// ```
pub async fn update_index_settings(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    // Reject static settings
    if body.pointer("/index/number_of_shards").is_some() {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "index.number_of_shards is immutable and cannot be changed after index creation",
        );
    }
    if body.pointer("/index/engine").is_some() {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "index.engine is immutable and cannot be changed after index creation",
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            );
        }
    };

    let mut changed = false;

    // Update number_of_replicas
    if let Some(new_replicas) = body
        .pointer("/index/number_of_replicas")
        .and_then(|v| v.as_u64())
    {
        let new_replicas = new_replicas as u32;
        if new_replicas != metadata.number_of_replicas {
            metadata.update_number_of_replicas(new_replicas);
            changed = true;
        }
    }

    // Update refresh_interval_ms
    if let Some(val) = body.pointer("/index/refresh_interval_ms") {
        if val.is_null() {
            if metadata.settings.refresh_interval_ms.is_some() {
                metadata.settings.refresh_interval_ms = None;
                changed = true;
            }
        } else if let Some(ms) = val.as_u64()
            && metadata.settings.refresh_interval_ms != Some(ms)
        {
            metadata.settings.refresh_interval_ms = Some(ms);
            changed = true;
        }
    }

    // Update flush_threshold_bytes
    if let Some(val) = body.pointer("/index/flush_threshold_bytes") {
        if val.is_null() {
            if metadata.settings.flush_threshold_bytes.is_some() {
                metadata.settings.flush_threshold_bytes = None;
                changed = true;
            }
        } else if let Some(bytes) = val.as_u64()
            && metadata.settings.flush_threshold_bytes != Some(bytes)
        {
            metadata.settings.flush_threshold_bytes = Some(bytes);
            changed = true;
        }
    }

    if !changed {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "acknowledged": true })),
        );
    }

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "settings update") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_update_settings(&master, &index_name, &body)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                if let Some(response) = retryable_forward_error_response(&e) {
                    return response;
                }
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward settings update to master: {e:#}"),
                );
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::UpdateIndex {
            metadata: metadata.clone(),
        };
        if let Err(e) = raft_write(&state, cmd).await {
            return e;
        }
    }

    // Apply settings to live engines on this node via watch channels
    state
        .shard_manager
        .apply_settings(&index_name, &metadata.settings);

    tracing::info!("Updated settings for index '{}'", index_name);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "acknowledged": true })),
    )
}

pub async fn delete_index(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let cluster_state = state.cluster_manager.get_state();

    let Some(index_metadata) = cluster_state.indices.get(index_name.as_str()) else {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{index_name}]"),
        );
    };
    if let Err(error) = state
        .shard_manager
        .abort_source_recoveries_for_index(&index_metadata.uuid)
        .await
    {
        return crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "peer_recovery_cleanup_exception",
            format!("Failed to stop peer recovery before deleting [{index_name}]: {error}"),
        );
    }

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "index deletion") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_delete_index(&master, &index_name)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward index deletion to master: {e}"),
                );
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::DeleteIndex {
            index_name: index_name.to_string(),
        };
        if let Err(e) = raft_write(&state, cmd).await {
            return e;
        }
    }

    // Close local shard engines and delete data
    if let Err(e) = state
        .shard_manager
        .close_index_shards_blocking_with_reason(
            index_name.to_string(),
            crate::shard::SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX,
        )
        .await
    {
        tracing::error!("Failed to close shards for index '{}': {}", index_name, e);
    }

    tracing::info!("Deleted index '{}'", index_name);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "acknowledged": true })),
    )
}

#[cfg(test)]
mod tests;
