//! Proto ↔ domain type conversion helpers for the transport layer.

use crate::transport::proto::*;
use tonic::Status;

pub(super) fn node_info_to_proto(n: &crate::cluster::state::NodeInfo) -> NodeInfo {
    NodeInfo {
        id: n.id.clone(),
        name: n.name.clone(),
        host: n.host.clone(),
        transport_port: n.transport_port as u32,
        http_port: n.http_port as u32,
        roles: n
            .roles
            .iter()
            .map(|r| match r {
                crate::cluster::state::NodeRole::Master => "master".into(),
                crate::cluster::state::NodeRole::Data => "data".into(),
                crate::cluster::state::NodeRole::Client => "client".into(),
            })
            .collect(),
        raft_node_id: n.raft_node_id,
    }
}

pub(super) fn proto_to_node_info(p: &NodeInfo) -> crate::cluster::state::NodeInfo {
    crate::cluster::state::NodeInfo {
        id: p.id.clone(),
        name: p.name.clone(),
        host: p.host.clone(),
        transport_port: p.transport_port as u16,
        http_port: p.http_port as u16,
        roles: p
            .roles
            .iter()
            .map(|r| match r.as_str() {
                "master" => crate::cluster::state::NodeRole::Master,
                "data" => crate::cluster::state::NodeRole::Data,
                "client" => crate::cluster::state::NodeRole::Client,
                _ => crate::cluster::state::NodeRole::Data,
            })
            .collect(),
        raft_node_id: p.raft_node_id,
    }
}

pub(super) fn field_type_to_proto(field_type: &crate::cluster::state::FieldType) -> String {
    match field_type {
        crate::cluster::state::FieldType::Text => "text",
        crate::cluster::state::FieldType::Keyword => "keyword",
        crate::cluster::state::FieldType::Integer => "integer",
        crate::cluster::state::FieldType::Float => "float",
        crate::cluster::state::FieldType::Boolean => "boolean",
        crate::cluster::state::FieldType::Date => "date",
        crate::cluster::state::FieldType::KnnVector => "knn_vector",
    }
    .to_string()
}

#[allow(clippy::result_large_err)]
pub(super) fn proto_to_field_type(
    field_type: &str,
) -> Result<crate::cluster::state::FieldType, Status> {
    match field_type {
        "text" => Ok(crate::cluster::state::FieldType::Text),
        "keyword" => Ok(crate::cluster::state::FieldType::Keyword),
        "integer" | "long" => Ok(crate::cluster::state::FieldType::Integer),
        "float" | "double" => Ok(crate::cluster::state::FieldType::Float),
        "boolean" => Ok(crate::cluster::state::FieldType::Boolean),
        "date" | "datetime" | "timestamp" => Ok(crate::cluster::state::FieldType::Date),
        "knn_vector" => Ok(crate::cluster::state::FieldType::KnnVector),
        other => Err(Status::invalid_argument(format!(
            "unknown field type in cluster state snapshot: {}",
            other
        ))),
    }
}

pub(super) fn index_settings_to_proto(
    settings: &crate::cluster::state::IndexSettings,
) -> IndexSettings {
    IndexSettings {
        refresh_interval_ms: settings.refresh_interval_ms,
        flush_threshold_bytes: settings.flush_threshold_bytes,
    }
}

pub(super) fn proto_to_index_settings(
    settings: Option<&IndexSettings>,
) -> crate::cluster::state::IndexSettings {
    let Some(settings) = settings else {
        return crate::cluster::state::IndexSettings::default();
    };

    crate::cluster::state::IndexSettings {
        refresh_interval_ms: settings.refresh_interval_ms,
        flush_threshold_bytes: settings.flush_threshold_bytes,
    }
}

#[allow(clippy::result_large_err)]
pub(super) fn validate_join_identity(
    state: &crate::cluster::state::ClusterState,
    node_id: &str,
    raft_node_id: u64,
) -> Result<(), Status> {
    if let Some(existing) = state.nodes.get(node_id)
        && existing.raft_node_id > 0
        && existing.raft_node_id != raft_node_id
    {
        return Err(Status::already_exists(format!(
            "node [{}] is already registered with raft_node_id {}",
            node_id, existing.raft_node_id
        )));
    }

    if raft_node_id > 0
        && let Some(existing) = state
            .nodes
            .values()
            .find(|existing| existing.raft_node_id == raft_node_id && existing.id != node_id)
    {
        return Err(Status::already_exists(format!(
            "raft_node_id {} is already registered to node [{}]",
            raft_node_id, existing.id
        )));
    }

    Ok(())
}

pub fn cluster_state_to_proto(s: &crate::cluster::state::ClusterState) -> ClusterState {
    ClusterState {
        cluster_name: s.cluster_name.clone(),
        version: s.version,
        master_node: s.master_node.clone(),
        nodes: s.nodes.values().map(node_info_to_proto).collect(),
        indices: s
            .indices
            .values()
            .map(|idx| IndexMetadata {
                name: idx.name.clone(),
                uuid: idx.uuid.to_string(),
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shards: idx
                    .shard_routing
                    .iter()
                    .map(|(sid, routing)| ShardAssignment {
                        shard_id: *sid,
                        node_id: routing.primary.clone(),
                        replica_node_ids: routing.replicas.clone(),
                        unassigned_replicas: routing.unassigned_replicas,
                    })
                    .collect(),
                mappings: idx
                    .mappings
                    .iter()
                    .map(|(name, mapping)| FieldMappingEntry {
                        name: name.clone(),
                        field_type: field_type_to_proto(&mapping.field_type),
                        dimension: mapping.dimension.map(|dimension| dimension as u32),
                    })
                    .collect(),
                settings: Some(index_settings_to_proto(&idx.settings)),
            })
            .collect(),
    }
}

#[allow(clippy::result_large_err)]
pub fn proto_to_cluster_state(
    p: &ClusterState,
) -> Result<crate::cluster::state::ClusterState, Status> {
    let mut state = crate::cluster::state::ClusterState::new(p.cluster_name.clone());
    state.version = p.version;
    state.master_node = p.master_node.clone();
    for node in &p.nodes {
        let ni = proto_to_node_info(node);
        state.nodes.insert(ni.id.clone(), ni);
    }
    for idx in &p.indices {
        let mut shard_routing = std::collections::HashMap::new();
        for sa in &idx.shards {
            shard_routing.insert(
                sa.shard_id,
                crate::cluster::state::ShardRoutingEntry {
                    primary: sa.node_id.clone(),
                    replicas: sa.replica_node_ids.clone(),
                    unassigned_replicas: sa.unassigned_replicas,
                },
            );
        }
        let mut mappings = std::collections::HashMap::new();
        for mapping in &idx.mappings {
            let field_type = proto_to_field_type(&mapping.field_type).map_err(|_| {
                Status::invalid_argument(format!(
                    "unknown field type '{}' for field '{}' in index '{}'",
                    mapping.field_type, mapping.name, idx.name
                ))
            })?;
            mappings.insert(
                mapping.name.clone(),
                crate::cluster::state::FieldMapping {
                    field_type,
                    dimension: mapping.dimension.map(|dimension| dimension as usize),
                },
            );
        }
        let index_uuid = if idx.uuid.is_empty() {
            return Err(Status::invalid_argument(format!(
                "Index '{}' has no UUID in cluster state snapshot",
                idx.name
            )));
        } else {
            crate::cluster::state::IndexUuid::new(idx.uuid.clone())
        };
        state.indices.insert(
            idx.name.clone(),
            crate::cluster::state::IndexMetadata {
                name: idx.name.clone(),
                uuid: index_uuid,
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shard_routing,
                mappings,
                settings: proto_to_index_settings(idx.settings.as_ref()),
            },
        );
    }
    Ok(state)
}
