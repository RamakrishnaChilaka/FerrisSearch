//! Primary-replica replication logic.
//!
//! After a primary shard writes to its local WAL + engine, it replicates
//! the operation to all authoritative in-sync replica shards via gRPC. Replication is synchronous
//! (write is only acknowledged after all in-sync replicas confirm).

use crate::cluster::state::ClusterState;
use crate::shard::ReplicaCheckpointUpdate;
use crate::transport::TransportClient;
use crate::transport::proto::{ReplicateBulkRequest, ReplicateDocRequest};
use crate::wal::TranslogDurability;
use std::sync::Arc;
use tracing::error;

#[cfg(feature = "protocol-trace")]
fn trace_replication_failure(
    node_id: impl Into<String>,
    allocation_id: Option<u64>,
    error: impl std::fmt::Display,
) -> Vec<ReplicaReplicationFailure> {
    vec![ReplicaReplicationFailure::message(
        node_id,
        allocation_id,
        format!("protocol trace replication error: {error}"),
    )]
}

#[cfg(feature = "protocol-trace")]
fn trace_source_copy(
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    source_node: &str,
    index_uuid: &str,
) -> Result<crate::protocol_trace::TraceCopy, Vec<ReplicaReplicationFailure>> {
    let allocation = cluster_state
        .shard_allocation_id(index_name, shard_id, source_node)
        .ok_or_else(|| {
            trace_replication_failure(
                source_node,
                None,
                "source allocation is missing from the captured routing view",
            )
        })?;
    Ok(crate::protocol_trace::TraceCopy {
        node: source_node.to_string(),
        index_uuid: index_uuid.to_string(),
        shard: shard_id,
        allocation,
    })
}

#[cfg(feature = "protocol-trace")]
fn trace_result_outcome(message_id: &str) -> &'static str {
    match crate::protocol_trace::message_phase(message_id) {
        Some("nack") => "failed",
        _ => "timeout",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaReplicationFailure {
    pub node_id: String,
    pub allocation_id: Option<u64>,
    pub message: String,
    pub definitive: bool,
}

impl ReplicaReplicationFailure {
    fn message(node_id: impl Into<String>, allocation_id: Option<u64>, message: String) -> Self {
        Self {
            node_id: node_id.into(),
            allocation_id,
            message,
            definitive: false,
        }
    }

    fn from_error(node_id: String, allocation_id: u64, error: anyhow::Error) -> Self {
        let definitive = error
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::DataLoss);
        Self {
            message: format!("{node_id}: {error}"),
            node_id,
            allocation_id: Some(allocation_id),
            definitive,
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        self.message.contains(value)
    }
}

impl std::fmt::Display for ReplicaReplicationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[derive(Debug, Clone)]
struct ReplicaWireOperation {
    seq_no: u64,
    op: String,
    doc_id: String,
    payload_json: Vec<u8>,
}

struct ReplicaBatchRoute {
    index_uuid: String,
    #[cfg(feature = "protocol-trace")]
    primary_node: String,
    replica_node_ids: Vec<String>,
}

fn resolve_replica_batch_route(
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    primary_term: u64,
    operation_label: &'static str,
) -> Result<Option<ReplicaBatchRoute>, Vec<ReplicaReplicationFailure>> {
    let metadata = match cluster_state.indices.get(index_name) {
        Some(metadata) => metadata,
        None => return Ok(None),
    };
    let Some(routing) = metadata.shard_routing.get(&shard_id) else {
        return Ok(None);
    };
    if routing.primary_term != primary_term {
        return Err(vec![ReplicaReplicationFailure::message(
            "<routing>",
            None,
            format!(
                "{operation_label} replication term {primary_term} does not match captured routing term {}",
                routing.primary_term
            ),
        )]);
    }
    let replica_node_ids = metadata
        .in_sync_replica_nodes(shard_id)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    Ok(Some(ReplicaBatchRoute {
        index_uuid: metadata.uuid.to_string(),
        #[cfg(feature = "protocol-trace")]
        primary_node: routing.primary.clone(),
        replica_node_ids,
    }))
}

/// Replicate a single document write to all in-sync replica nodes for a shard.
/// Returns Ok(replica_checkpoints) if all in-sync replicas acknowledged, Err otherwise.
/// Replication is performed concurrently (fan-out) — latency = max(replica RTTs).
#[allow(clippy::too_many_arguments)]
pub async fn replicate_write(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    payload: &serde_json::Value,
    op: &str,
    seq_no: u64,
    primary_term: u64,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    replicate_write_with_durability(
        transport_client,
        cluster_state,
        index_name,
        shard_id,
        doc_id,
        payload,
        op,
        seq_no,
        primary_term,
        TranslogDurability::Request,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn replicate_write_with_durability(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    payload: &serde_json::Value,
    op: &str,
    seq_no: u64,
    primary_term: u64,
    durability: TranslogDurability,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m,
        None => return Ok(vec![]), // no index metadata, nothing to replicate
    };
    let Some(routing) = metadata.shard_routing.get(&shard_id) else {
        return Ok(vec![]);
    };
    let index_uuid = metadata.uuid.to_string();
    if routing.primary_term != primary_term {
        return Err(vec![ReplicaReplicationFailure::message(
            "<routing>",
            None,
            format!(
                "replication term {primary_term} does not match captured routing term {}",
                routing.primary_term
            ),
        )]);
    }

    let replica_node_ids = metadata.in_sync_replica_nodes(shard_id);
    #[cfg(feature = "protocol-trace")]
    let trace_operation = crate::engine::SequencedOperation {
        seq_no,
        primary_term,
        mutation: match op {
            "index" => crate::engine::DocumentMutation::Index {
                doc_id: doc_id.to_string(),
                source: payload.clone(),
            },
            "delete" => crate::engine::DocumentMutation::Delete {
                doc_id: doc_id.to_string(),
            },
            other => {
                return Err(trace_replication_failure(
                    routing.primary.clone(),
                    None,
                    format!("unsupported traced replication operation '{other}'"),
                ));
            }
        },
    };
    #[cfg(feature = "protocol-trace")]
    let trace_messages = crate::protocol_trace::start_replication(
        &routing.primary,
        cluster_state,
        index_name,
        shard_id,
        std::slice::from_ref(&trace_operation),
        false,
    )
    .map_err(|error| trace_replication_failure(routing.primary.clone(), None, error))?;
    #[cfg(feature = "protocol-trace")]
    let trace_source = trace_source_copy(
        cluster_state,
        index_name,
        shard_id,
        &routing.primary,
        &index_uuid,
    )?;
    if replica_node_ids.is_empty() {
        return Ok(vec![]);
    }

    // Build futures for concurrent replication to all in-sync replicas
    let mut futures = Vec::with_capacity(replica_node_ids.len());
    let mut serialized_payload = None;

    for replica_node_id in &replica_node_ids {
        let node_info = match cluster_state.nodes.get(*replica_node_id) {
            Some(n) => n.clone(),
            None => {
                // Immediately record error for missing nodes — no future to spawn
                let rid = replica_node_id.to_string();
                futures.push(tokio::spawn(async move {
                    (
                        rid.clone(),
                        0,
                        Err(ReplicaReplicationFailure::message(
                            rid.clone(),
                            None,
                            format!("Replica node {rid} not in cluster state"),
                        )),
                    )
                }));
                continue;
            }
        };

        let client = transport_client.clone();
        let idx = index_name.to_string();
        let did = doc_id.to_string();
        let operation = op.to_string();
        let rid = replica_node_id.to_string();
        let Some(target_allocation_id) =
            cluster_state.shard_allocation_id(index_name, shard_id, replica_node_id)
        else {
            futures.push(tokio::spawn(async move {
                (
                    rid.clone(),
                    0,
                    Err(ReplicaReplicationFailure::message(
                        rid.clone(),
                        None,
                        format!("Replica node {rid} has no allocation ID in cluster state"),
                    )),
                )
            }));
            continue;
        };
        let payload_json = Arc::clone(serialized_payload.get_or_insert_with(|| {
            Arc::new(serde_json::to_vec(payload).map_err(|error| error.to_string()))
        }));
        let uuid = index_uuid.clone();
        #[cfg(feature = "protocol-trace")]
        let trace_message = trace_messages
            .iter()
            .find(|message| message.target == rid)
            .cloned();
        #[cfg(feature = "protocol-trace")]
        let trace_source = trace_source.clone();

        futures.push(tokio::spawn(async move {
            #[cfg(feature = "protocol-trace")]
            if let Some(message) = trace_message.as_ref()
                && crate::protocol_trace::apply_request_fault(&message.message_id).await
            {
                let _ = crate::protocol_trace::record_replica_result(
                    &trace_source,
                    message,
                    "dropped",
                    None,
                );
                return (
                    rid.clone(),
                    target_allocation_id,
                    Err(ReplicaReplicationFailure::message(
                        rid.clone(),
                        Some(target_allocation_id),
                        format!("{rid}: injected trace request drop"),
                    )),
                );
            }
            let payload_json = match Arc::unwrap_or_clone(payload_json) {
                Ok(payload_json) => payload_json,
                Err(error) => {
                    return (
                        rid.clone(),
                        target_allocation_id,
                        Err(ReplicaReplicationFailure::message(
                            rid.clone(),
                            Some(target_allocation_id),
                            format!("{rid}: serialize replica payload: {error}"),
                        )),
                    );
                }
            };
            match client
                .replicate_to_shard(
                    &node_info,
                    ReplicateDocRequest {
                        index_name: idx,
                        shard_id,
                        doc_id: did,
                        payload_json,
                        op: operation,
                        seq_no,
                        index_uuid: uuid,
                        primary_term: Some(primary_term),
                        target_allocation_id: Some(target_allocation_id),
                    },
                    matches!(durability, TranslogDurability::Request),
                )
                .await
            {
                Ok(checkpoint) => {
                    #[cfg(feature = "protocol-trace")]
                    if let Some(message) = trace_message.as_ref() {
                        if crate::protocol_trace::should_drop_response(&message.message_id) {
                            let _ = crate::protocol_trace::record_replica_result(
                                &trace_source,
                                message,
                                "dropped",
                                checkpoint.persisted_checkpoint,
                            );
                            return (
                                rid.clone(),
                                target_allocation_id,
                                Err(ReplicaReplicationFailure::message(
                                    rid.clone(),
                                    Some(target_allocation_id),
                                    format!("{rid}: injected trace response drop"),
                                )),
                            );
                        }
                        let _ = crate::protocol_trace::record_replica_result(
                            &trace_source,
                            message,
                            "acknowledged",
                            checkpoint.persisted_checkpoint,
                        );
                    }
                    (rid, target_allocation_id, Ok(checkpoint))
                }
                Err(error) => {
                    #[cfg(feature = "protocol-trace")]
                    if let Some(message) = trace_message.as_ref() {
                        let outcome = trace_result_outcome(&message.message_id);
                        let _ = crate::protocol_trace::record_replica_result(
                            &trace_source,
                            message,
                            outcome,
                            None,
                        );
                    }
                    let failure = ReplicaReplicationFailure::from_error(
                        rid.clone(),
                        target_allocation_id,
                        error,
                    );
                    (rid, target_allocation_id, Err(failure))
                }
            }
        }));
    }
    drop(serialized_payload);

    let results = futures::future::join_all(futures).await;
    let mut errors = Vec::new();
    let mut checkpoints = Vec::new();

    for result in results {
        match result {
            Ok((rid, allocation_id, Ok(checkpoint))) => {
                checkpoints.push(ReplicaCheckpointUpdate {
                    node_id: rid,
                    allocation_id,
                    processed_checkpoint: checkpoint.processed_checkpoint,
                    persisted_checkpoint: checkpoint.persisted_checkpoint,
                });
            }
            Ok((rid, _, Err(e))) => {
                error!(
                    "Replication to {} for {}/shard_{} failed: {}",
                    rid, index_name, shard_id, e
                );
                errors.push(e);
            }
            Err(e) => {
                error!("Replication task panicked: {}", e);
                errors.push(ReplicaReplicationFailure::message(
                    "<task>",
                    None,
                    format!("task panicked: {e}"),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(checkpoints)
    } else {
        Err(errors)
    }
}

/// Replicate a bulk set of writes to all in-sync replica nodes for a shard.
/// Replication is performed concurrently (fan-out) — latency = max(replica RTTs).
pub async fn replicate_bulk(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    docs: &[(String, serde_json::Value)],
    start_seq_no: u64,
    primary_term: u64,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    replicate_bulk_with_durability(
        transport_client,
        cluster_state,
        index_name,
        shard_id,
        docs,
        start_seq_no,
        primary_term,
        TranslogDurability::Request,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn replicate_bulk_with_durability(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    docs: &[(String, serde_json::Value)],
    start_seq_no: u64,
    primary_term: u64,
    durability: TranslogDurability,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    let Some(route) =
        resolve_replica_batch_route(cluster_state, index_name, shard_id, primary_term, "bulk")?
    else {
        return Ok(Vec::new());
    };
    if route.replica_node_ids.is_empty() {
        #[cfg(feature = "protocol-trace")]
        {
            let trace_operations = docs
                .iter()
                .enumerate()
                .map(|(offset, (doc_id, payload))| {
                    let seq_no = start_seq_no.checked_add(offset as u64).ok_or_else(|| {
                        trace_replication_failure(
                            route.primary_node.clone(),
                            None,
                            "bulk replication sequence range overflows".to_string(),
                        )
                    })?;
                    Ok(crate::engine::SequencedOperation {
                        seq_no,
                        primary_term,
                        mutation: crate::engine::DocumentMutation::Index {
                            doc_id: doc_id.clone(),
                            source: payload.clone(),
                        },
                    })
                })
                .collect::<Result<Vec<_>, Vec<ReplicaReplicationFailure>>>()?;
            crate::protocol_trace::start_replication(
                &route.primary_node,
                cluster_state,
                index_name,
                shard_id,
                &trace_operations,
                false,
            )
            .map_err(|error| trace_replication_failure(route.primary_node.clone(), None, error))?;
        }
        return Ok(Vec::new());
    }
    let operations = docs
        .iter()
        .enumerate()
        .map(|(offset, (doc_id, payload))| {
            let seq_no = start_seq_no.checked_add(offset as u64).ok_or_else(|| {
                ReplicaReplicationFailure::message(
                    "<bulk>",
                    None,
                    "bulk replication sequence range overflows".to_string(),
                )
            })?;
            let payload_json = serde_json::to_vec(payload).map_err(|error| {
                ReplicaReplicationFailure::message(
                    "<bulk>",
                    None,
                    format!("serialize replica payload: {error}"),
                )
            })?;
            Ok(ReplicaWireOperation {
                seq_no,
                op: "index".to_string(),
                doc_id: doc_id.clone(),
                payload_json,
            })
        })
        .collect::<Result<Vec<_>, ReplicaReplicationFailure>>()
        .map_err(|error| vec![error])?;
    replicate_explicit_batch_with_durability(
        transport_client,
        cluster_state,
        route,
        index_name,
        shard_id,
        Arc::from(operations),
        primary_term,
        durability,
        "bulk",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn replicate_noop_batch_with_durability(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    operations: &[crate::engine::SequencedOperation],
    primary_term: u64,
    durability: TranslogDurability,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    let mut previous_seq_no = None;
    let operations = operations
        .iter()
        .map(|operation| {
            if operation.primary_term != primary_term {
                return Err(ReplicaReplicationFailure::message(
                    "<promotion>",
                    None,
                    format!(
                        "promotion NoOp term {} does not match activated term {primary_term}",
                        operation.primary_term
                    ),
                ));
            }
            if previous_seq_no.is_some_and(|previous| operation.seq_no <= previous) {
                return Err(ReplicaReplicationFailure::message(
                    "<promotion>",
                    None,
                    "promotion NoOp sequences must be strictly increasing".to_string(),
                ));
            }
            previous_seq_no = Some(operation.seq_no);
            let crate::engine::DocumentMutation::NoOp { reason } = &operation.mutation else {
                return Err(ReplicaReplicationFailure::message(
                    "<promotion>",
                    None,
                    "promotion replication batch contains a non-NoOp operation".to_string(),
                ));
            };
            let payload_json = serde_json::to_vec(&serde_json::json!({ "_reason": reason }))
                .map_err(|error| {
                    ReplicaReplicationFailure::message(
                        "<promotion>",
                        None,
                        format!("serialize promotion NoOp: {error}"),
                    )
                })?;
            Ok(ReplicaWireOperation {
                seq_no: operation.seq_no,
                op: "noop".to_string(),
                doc_id: String::new(),
                payload_json,
            })
        })
        .collect::<Result<Vec<_>, ReplicaReplicationFailure>>()
        .map_err(|error| vec![error])?;
    let Some(route) = resolve_replica_batch_route(
        cluster_state,
        index_name,
        shard_id,
        primary_term,
        "promotion NoOp",
    )?
    else {
        return Ok(Vec::new());
    };
    if route.replica_node_ids.is_empty() {
        return Ok(Vec::new());
    }
    replicate_explicit_batch_with_durability(
        transport_client,
        cluster_state,
        route,
        index_name,
        shard_id,
        Arc::from(operations),
        primary_term,
        durability,
        "promotion NoOp",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn replicate_explicit_batch_with_durability(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    route: ReplicaBatchRoute,
    index_name: &str,
    shard_id: u32,
    operations: Arc<[ReplicaWireOperation]>,
    primary_term: u64,
    durability: TranslogDurability,
    operation_label: &'static str,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>> {
    #[cfg(feature = "protocol-trace")]
    let trace_operations = operations
        .iter()
        .map(|operation| {
            let mutation = match operation.op.as_str() {
                "index" => crate::engine::DocumentMutation::Index {
                    doc_id: operation.doc_id.clone(),
                    source: serde_json::from_slice(&operation.payload_json).map_err(|error| {
                        trace_replication_failure(
                            route.primary_node.clone(),
                            None,
                            format!("decode traced replica payload: {error}"),
                        )
                    })?,
                },
                "delete" => crate::engine::DocumentMutation::Delete {
                    doc_id: operation.doc_id.clone(),
                },
                "noop" => {
                    let payload: serde_json::Value =
                        serde_json::from_slice(&operation.payload_json).map_err(|error| {
                            trace_replication_failure(
                                route.primary_node.clone(),
                                None,
                                format!("decode traced promotion NoOp: {error}"),
                            )
                        })?;
                    let reason = payload
                        .get("_reason")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            trace_replication_failure(
                                route.primary_node.clone(),
                                None,
                                "traced promotion NoOp has no _reason",
                            )
                        })?;
                    crate::engine::DocumentMutation::NoOp {
                        reason: reason.to_string(),
                    }
                }
                other => {
                    return Err(trace_replication_failure(
                        route.primary_node.clone(),
                        None,
                        format!("unsupported traced batch operation '{other}'"),
                    ));
                }
            };
            Ok(crate::engine::SequencedOperation {
                seq_no: operation.seq_no,
                primary_term,
                mutation,
            })
        })
        .collect::<Result<Vec<_>, Vec<ReplicaReplicationFailure>>>()?;
    #[cfg(feature = "protocol-trace")]
    let trace_messages = crate::protocol_trace::start_replication(
        &route.primary_node,
        cluster_state,
        index_name,
        shard_id,
        &trace_operations,
        operation_label == "promotion NoOp",
    )
    .map_err(|error| trace_replication_failure(route.primary_node.clone(), None, error))?;
    #[cfg(feature = "protocol-trace")]
    let trace_source = trace_source_copy(
        cluster_state,
        index_name,
        shard_id,
        &route.primary_node,
        &route.index_uuid,
    )?;
    // Build futures for concurrent replication to all in-sync replicas
    let mut futures = Vec::with_capacity(route.replica_node_ids.len());

    for replica_node_id in &route.replica_node_ids {
        let node_info = match cluster_state.nodes.get(replica_node_id) {
            Some(n) => n.clone(),
            None => {
                let rid = replica_node_id.to_string();
                futures.push(tokio::spawn(async move {
                    (
                        rid.clone(),
                        0,
                        Err(ReplicaReplicationFailure::message(
                            rid.clone(),
                            None,
                            format!("Replica node {rid} not in cluster state"),
                        )),
                    )
                }));
                continue;
            }
        };

        let client = transport_client.clone();
        let idx = index_name.to_string();
        let rid = replica_node_id.to_string();
        let operations = Arc::clone(&operations);
        let Some(target_allocation_id) =
            cluster_state.shard_allocation_id(index_name, shard_id, replica_node_id)
        else {
            futures.push(tokio::spawn(async move {
                (
                    rid.clone(),
                    0,
                    Err(ReplicaReplicationFailure::message(
                        rid.clone(),
                        None,
                        format!("Replica node {rid} has no allocation ID in cluster state"),
                    )),
                )
            }));
            continue;
        };
        let uuid = route.index_uuid.clone();
        #[cfg(feature = "protocol-trace")]
        let trace_target_messages = trace_messages
            .iter()
            .filter(|message| message.target == rid)
            .cloned()
            .collect::<Vec<_>>();
        #[cfg(feature = "protocol-trace")]
        let trace_source = trace_source.clone();

        futures.push(tokio::spawn(async move {
            #[cfg(feature = "protocol-trace")]
            for message in &trace_target_messages {
                if crate::protocol_trace::apply_request_fault(&message.message_id).await {
                    for result_message in &trace_target_messages {
                        let _ = crate::protocol_trace::record_replica_result(
                            &trace_source,
                            result_message,
                            "dropped",
                            None,
                        );
                    }
                    return (
                        rid.clone(),
                        target_allocation_id,
                        Err(ReplicaReplicationFailure::message(
                            rid.clone(),
                            Some(target_allocation_id),
                            format!("{rid}: injected trace request drop"),
                        )),
                    );
                }
            }
            let ops = operations
                .iter()
                .map(|operation| ReplicateDocRequest {
                    index_name: idx.clone(),
                    shard_id,
                    doc_id: operation.doc_id.clone(),
                    payload_json: operation.payload_json.clone(),
                    op: operation.op.clone(),
                    seq_no: operation.seq_no,
                    index_uuid: uuid.clone(),
                    primary_term: Some(primary_term),
                    target_allocation_id: Some(target_allocation_id),
                })
                .collect();
            match client
                .replicate_bulk_to_shard(
                    &node_info,
                    ReplicateBulkRequest {
                        index_name: idx,
                        shard_id,
                        ops,
                        index_uuid: uuid,
                        primary_term: Some(primary_term),
                        target_allocation_id: Some(target_allocation_id),
                    },
                    matches!(durability, TranslogDurability::Request),
                )
                .await
            {
                Ok(checkpoint) => {
                    #[cfg(feature = "protocol-trace")]
                    {
                        let drop_response = trace_target_messages.iter().any(|message| {
                            crate::protocol_trace::should_drop_response(&message.message_id)
                        });
                        let outcome = if drop_response {
                            "dropped"
                        } else {
                            "acknowledged"
                        };
                        for message in &trace_target_messages {
                            let _ = crate::protocol_trace::record_replica_result(
                                &trace_source,
                                message,
                                outcome,
                                checkpoint.persisted_checkpoint,
                            );
                        }
                        if drop_response {
                            return (
                                rid.clone(),
                                target_allocation_id,
                                Err(ReplicaReplicationFailure::message(
                                    rid.clone(),
                                    Some(target_allocation_id),
                                    format!("{rid}: injected trace response drop"),
                                )),
                            );
                        }
                    }
                    (rid, target_allocation_id, Ok(checkpoint))
                }
                Err(error) => {
                    #[cfg(feature = "protocol-trace")]
                    for message in &trace_target_messages {
                        let outcome = trace_result_outcome(&message.message_id);
                        let _ = crate::protocol_trace::record_replica_result(
                            &trace_source,
                            message,
                            outcome,
                            None,
                        );
                    }
                    let failure = ReplicaReplicationFailure::from_error(
                        rid.clone(),
                        target_allocation_id,
                        error,
                    );
                    (rid, target_allocation_id, Err(failure))
                }
            }
        }));
    }

    let results = futures::future::join_all(futures).await;
    let mut errors = Vec::new();
    let mut checkpoints = Vec::new();

    for result in results {
        match result {
            Ok((rid, allocation_id, Ok(checkpoint))) => {
                checkpoints.push(ReplicaCheckpointUpdate {
                    node_id: rid,
                    allocation_id,
                    processed_checkpoint: checkpoint.processed_checkpoint,
                    persisted_checkpoint: checkpoint.persisted_checkpoint,
                });
            }
            Ok((rid, _, Err(e))) => {
                error!(
                    "{} replication to {} for {}/shard_{} failed: {}",
                    operation_label, rid, index_name, shard_id, e
                );
                errors.push(e);
            }
            Err(e) => {
                error!("Bulk replication task panicked: {}", e);
                errors.push(ReplicaReplicationFailure::message(
                    "<task>",
                    None,
                    format!("task panicked: {e}"),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(checkpoints)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::*;
    use std::collections::HashMap;

    fn make_cluster_state_with_nodes() -> ClusterState {
        let mut cs = ClusterState::new("test-cluster".into());
        cs.add_node(NodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 19300,
            http_port: 19200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        cs.add_node(NodeInfo {
            id: "node-2".into(),
            name: "node-2".into(),
            host: "127.0.0.1".into(),
            transport_port: 19301,
            http_port: 19201,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        cs
    }

    fn add_index_with_routing(cs: &mut ClusterState, name: &str, replicas: Vec<String>) {
        let in_sync_replicas = replicas.clone();
        add_index_with_membership(cs, name, replicas, in_sync_replicas);
    }

    fn add_index_with_membership(
        cs: &mut ClusterState,
        name: &str,
        replicas: Vec<String>,
        in_sync_replicas: Vec<String>,
    ) {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas,
                in_sync_replicas,
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: name.into(),
            uuid: IndexUuid::new("test-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            dynamic: Default::default(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
    }

    // ── replicate_write ─────────────────────────────────────────────────

    #[tokio::test]
    async fn write_noop_when_index_missing() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let result = replicate_write(
            &client,
            &cs,
            "nonexistent",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_ignores_unreachable_out_of_sync_replica() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_membership(&mut cs, "test-idx", vec!["node-2".into()], vec![]);
        let checkpoints = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await
        .unwrap();
        assert!(checkpoints.is_empty());
    }

    #[tokio::test]
    async fn write_noop_when_no_replicas() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec![]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_noop_when_shard_not_in_routing() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        // Shard 99 doesn't exist in routing table → no replicas → Ok
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            99,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_errors_when_replica_node_not_in_cluster_state() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["ghost-node".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("ghost-node"));
    }

    #[tokio::test]
    async fn write_errors_when_replica_node_unreachable() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        // node-2 is in cluster state but no gRPC server running → connection refused
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("node-2"));
    }

    #[tokio::test]
    async fn write_collects_multiple_errors() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        // Both replicas will fail: one not in state, one unreachable
        add_index_with_routing(&mut cs, "test-idx", vec!["ghost".into(), "node-2".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
            1,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 2);
    }

    // ── replicate_bulk ──────────────────────────────────────────────────

    #[tokio::test]
    async fn bulk_noop_when_index_missing() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let result = replicate_bulk(&client, &cs, "nonexistent", 0, &docs, 0, 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bulk_noop_when_no_replicas() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec![]);
        let docs = vec![
            (
                "d1".into(),
                serde_json::json!({"payload": "x".repeat(4096)}),
            ),
            (
                "d2".into(),
                serde_json::json!({"payload": "y".repeat(4096)}),
            ),
        ];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, u64::MAX, 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bulk_ignores_unreachable_out_of_sync_replica() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_membership(&mut cs, "test-idx", vec!["node-2".into()], vec![]);
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let checkpoints = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0, 1)
            .await
            .unwrap();
        assert!(checkpoints.is_empty());
    }

    #[tokio::test]
    async fn bulk_errors_when_replica_node_not_in_cluster_state() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["phantom".into()]);
        let docs = vec![
            ("d1".into(), serde_json::json!({"a": 1})),
            ("d2".into(), serde_json::json!({"b": 2})),
        ];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0, 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("phantom"));
    }

    #[tokio::test]
    async fn bulk_errors_when_replica_unreachable() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0, 1).await;
        assert!(result.is_err());
    }

    // ── Return type: checkpoints ────────────────────────────────────────

    #[tokio::test]
    async fn write_noop_returns_empty_checkpoints() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let checkpoints = replicate_write(
            &client,
            &cs,
            "nonexistent",
            0,
            "doc1",
            &serde_json::json!({"f": 1}),
            "index",
            0,
            1,
        )
        .await
        .unwrap();
        assert!(checkpoints.is_empty(), "no replicas → empty checkpoints");
    }

    #[tokio::test]
    async fn bulk_noop_returns_empty_checkpoints() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let checkpoints = replicate_bulk(&client, &cs, "nonexistent", 0, &docs, 0, 1)
            .await
            .unwrap();
        assert!(checkpoints.is_empty());
    }
}
