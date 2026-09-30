//! Raft state machine — applies committed log entries to the ClusterState.

use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};

use crate::cluster::state::ClusterState;
use crate::consensus::types::{
    ClusterCommand, ClusterResponse, Entry, LogId, Snapshot, SnapshotMeta, StoredMembership,
    TypeConfig,
};

/// The Raft state machine that wraps ClusterState.
/// All cluster state mutations go through Raft log → apply().
pub struct ClusterStateMachine {
    /// The authoritative cluster state, updated only via Raft apply().
    state: Arc<RwLock<ClusterState>>,
    /// Last applied log id.
    last_applied: Option<LogId>,
    /// Last applied membership.
    last_membership: StoredMembership,
}

impl ClusterStateMachine {
    pub fn new(cluster_name: String) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new(cluster_name))),
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    /// Get a read-only handle to the cluster state (for API queries).
    pub fn state_handle(&self) -> Arc<RwLock<ClusterState>> {
        self.state.clone()
    }

    #[cfg(feature = "protocol-trace")]
    pub fn from_state_for_protocol_trace_test(state: ClusterState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    #[cfg(feature = "protocol-trace")]
    pub fn apply_command_for_protocol_trace_test(
        &self,
        command: &ClusterCommand,
        raft_log_index: u64,
    ) -> ClusterResponse {
        self.apply_command_at(command, raft_log_index)
    }

    fn apply_command_at(&self, cmd: &ClusterCommand, raft_log_index: u64) -> ClusterResponse {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        match cmd {
            ClusterCommand::AddNode { node } => {
                state.add_node(node.clone());
                ClusterResponse::Ok
            }
            ClusterCommand::RemoveNode { node_id } => {
                state.remove_node(node_id);
                ClusterResponse::Ok
            }
            ClusterCommand::CreateIndex { metadata } => {
                if metadata.shard_routing.len() != metadata.number_of_shards as usize {
                    return ClusterResponse::Error(format!(
                        "index '{}' has {} shard routing entries but declares {} shards",
                        metadata.name,
                        metadata.shard_routing.len(),
                        metadata.number_of_shards
                    ));
                }
                for shard_id in 0..metadata.number_of_shards {
                    let Some(routing) = metadata.shard_routing.get(&shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{}' is missing routing for shard {}",
                            metadata.name, shard_id
                        ));
                    };
                    if routing.primary_term == 0 {
                        return ClusterResponse::Error(format!(
                            "index '{}' shard {} must start with primary term at least 1",
                            metadata.name, shard_id
                        ));
                    }
                    if let Err(reason) = routing.validate_membership() {
                        return ClusterResponse::Error(format!(
                            "invalid routing for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }
                }
                match state.add_index_with_allocation_id(metadata.clone(), raft_log_index) {
                    Ok(()) => ClusterResponse::Ok,
                    Err(error) => ClusterResponse::Error(format!(
                        "invalid initial allocation metadata for index '{}': {error}",
                        metadata.name
                    )),
                }
            }
            ClusterCommand::DeleteIndex { index_name } => {
                state.indices.remove(index_name);
                state.shard_allocations.remove(index_name);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::SetMaster { node_id } => {
                state.master_node = Some(node_id.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateIndex { metadata } => {
                let Some(current) = state.indices.get(&metadata.name).cloned() else {
                    return ClusterResponse::Error(format!(
                        "index '{}' does not exist",
                        metadata.name
                    ));
                };
                if current.uuid != metadata.uuid {
                    return ClusterResponse::Error(format!(
                        "index '{}' UUID mismatch: expected {}, got {}",
                        metadata.name, current.uuid, metadata.uuid
                    ));
                }
                if current.number_of_shards != metadata.number_of_shards
                    || current.shard_routing.len() != metadata.shard_routing.len()
                {
                    return ClusterResponse::Error(format!(
                        "index '{}' shard topology cannot be replaced by UpdateIndex",
                        metadata.name
                    ));
                }

                let Some(current_allocations) =
                    state.shard_allocations.get(&metadata.name).cloned()
                else {
                    return ClusterResponse::Error(format!(
                        "index '{}' has no allocation identity metadata",
                        metadata.name
                    ));
                };

                let mut updated = metadata.clone();
                let mut updated_allocations = HashMap::new();
                for (shard_id, current_routing) in &current.shard_routing {
                    let Some(next_routing) = updated.shard_routing.get_mut(shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{}' update is missing shard {}",
                            metadata.name, shard_id
                        ));
                    };
                    let Some(current_ids) = current_allocations.get(shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{}' shard {} has no allocation identity metadata",
                            metadata.name, shard_id
                        ));
                    };
                    if let Err(reason) = current_ids.validate_for_routing(current_routing) {
                        return ClusterResponse::Error(format!(
                            "invalid current allocation metadata for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }

                    next_routing.in_sync_replicas = current_routing
                        .in_sync_replicas
                        .iter()
                        .filter(|replica| next_routing.replicas.contains(replica))
                        .cloned()
                        .collect();

                    let primary_changed = next_routing.primary != current_routing.primary;
                    let next_primary_allocation = if primary_changed {
                        if !current_routing.is_replica_in_sync(&next_routing.primary) {
                            return ClusterResponse::Error(format!(
                                "cannot promote out-of-sync replica '{}' for index '{}' shard {}",
                                next_routing.primary, metadata.name, shard_id
                            ));
                        }
                        let Some(candidate_allocation) =
                            current_ids.replicas.get(&next_routing.primary).copied()
                        else {
                            return ClusterResponse::Error(format!(
                                "cannot promote replica '{}' without an allocation ID for index '{}' shard {}",
                                next_routing.primary, metadata.name, shard_id
                            ));
                        };
                        let Some(next_term) = current_routing.primary_term.checked_add(1) else {
                            return ClusterResponse::Error(format!(
                                "primary term exhausted for index '{}' shard {}",
                                metadata.name, shard_id
                            ));
                        };
                        next_routing.primary_term = next_term;
                        next_routing
                            .in_sync_replicas
                            .retain(|replica| replica != &next_routing.primary);
                        Some(candidate_allocation)
                    } else {
                        next_routing.primary_term = current_routing.primary_term;
                        current_ids.primary
                    };

                    let mut next_replica_allocations = HashMap::new();
                    for replica in &next_routing.replicas {
                        let existing = if replica == &current_routing.primary {
                            current_ids.primary
                        } else {
                            current_ids.replicas.get(replica).copied()
                        };
                        next_replica_allocations
                            .insert(replica.clone(), existing.unwrap_or(raft_log_index));
                    }
                    let next_ids = crate::cluster::state::ShardAllocationIds {
                        primary: next_primary_allocation,
                        replicas: next_replica_allocations,
                        initial_allocation_id: current_ids.initial_allocation_id,
                        primary_initialized: current_ids.primary_initialized,
                        primary_unavailable: if primary_changed {
                            false
                        } else {
                            current_ids.primary_unavailable
                        },
                    };

                    if let Err(reason) = next_routing.validate_membership() {
                        return ClusterResponse::Error(format!(
                            "invalid routing update for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }
                    if let Err(reason) = next_ids.validate_for_routing(next_routing) {
                        return ClusterResponse::Error(format!(
                            "invalid allocation update for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }
                    updated_allocations.insert(*shard_id, next_ids);
                }

                state.indices.insert(metadata.name.clone(), updated);
                state
                    .shard_allocations
                    .insert(metadata.name.clone(), updated_allocations);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::MarkReplicaInSync {
                index_name,
                index_uuid,
                shard_id,
                replica,
                allocation_id,
                primary,
                primary_term,
            } => {
                if *allocation_id == 0 {
                    return ClusterResponse::Error(format!(
                        "replica allocation ID must be greater than zero for index '{index_name}' shard {shard_id}"
                    ));
                }
                let Some(allocation_state) = state.shard_allocation_ids(index_name, *shard_id)
                else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' shard {shard_id} has no allocation identity metadata"
                    ));
                };
                if !allocation_state.primary_initialized {
                    return ClusterResponse::Error(format!(
                        "cannot admit a replica before the primary is initialized for index '{index_name}' shard {shard_id}"
                    ));
                }
                if allocation_state.primary.is_none() {
                    return ClusterResponse::Error(format!(
                        "cannot admit a replica without an allocated primary for index '{index_name}' shard {shard_id}"
                    ));
                }
                if state.shard_allocation_id(index_name, *shard_id, replica) != Some(*allocation_id)
                {
                    return ClusterResponse::Error(format!(
                        "replica allocation mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                let Some(metadata) = state.indices.get_mut(index_name) else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(routing) = metadata.shard_routing.get_mut(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if &routing.primary != primary {
                    return ClusterResponse::Error(format!(
                        "primary mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.primary_term != *primary_term {
                    return ClusterResponse::Error(format!(
                        "primary term mismatch for index '{index_name}' shard {shard_id}: expected {}, got {}",
                        routing.primary_term, primary_term
                    ));
                }
                if !routing.replicas.contains(replica) {
                    return ClusterResponse::Error(format!(
                        "replica '{replica}' is not assigned to index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.is_replica_in_sync(replica) {
                    return ClusterResponse::Error(format!(
                        "replica '{replica}' is already in sync for index '{index_name}' shard {shard_id}"
                    ));
                }
                routing.in_sync_replicas.push(replica.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::ActivatePrimary {
                index_name,
                index_uuid,
                shard_id,
                primary,
                allocation_id,
                expected_term,
            } => {
                if *allocation_id == 0 {
                    return ClusterResponse::Error(format!(
                        "primary allocation ID must be greater than zero for index '{index_name}' shard {shard_id}"
                    ));
                }
                if state.shard_allocation_id(index_name, *shard_id, primary) != Some(*allocation_id)
                {
                    return ClusterResponse::Error(format!(
                        "primary allocation mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                {
                    let Some(metadata) = state.indices.get_mut(index_name) else {
                        return ClusterResponse::Error(format!(
                            "index '{index_name}' does not exist"
                        ));
                    };
                    if metadata.uuid.as_str() != index_uuid {
                        return ClusterResponse::Error(format!(
                            "index '{index_name}' UUID mismatch"
                        ));
                    }
                    let Some(routing) = metadata.shard_routing.get_mut(shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{index_name}' has no shard {shard_id}"
                        ));
                    };
                    if &routing.primary != primary {
                        return ClusterResponse::Error(format!(
                            "primary mismatch for index '{index_name}' shard {shard_id}"
                        ));
                    }
                    if routing.primary_term != *expected_term {
                        return ClusterResponse::Error(format!(
                            "primary term mismatch for index '{index_name}' shard {shard_id}: expected {}, got {}",
                            routing.primary_term, expected_term
                        ));
                    }
                    let Some(next_term) = routing.primary_term.checked_add(1) else {
                        return ClusterResponse::Error(format!(
                            "primary term exhausted for index '{index_name}' shard {shard_id}"
                        ));
                    };
                    routing.primary_term = next_term;
                }
                let Some(allocations) = state
                    .shard_allocations
                    .get_mut(index_name)
                    .and_then(|shards| shards.get_mut(shard_id))
                else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' shard {shard_id} has no allocation identity metadata"
                    ));
                };
                allocations.primary_initialized = true;
                allocations.primary_unavailable = false;
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::MarkPrimaryUnavailable {
                index_name,
                index_uuid,
                shard_id,
                primary,
                allocation_id,
            } => {
                let Some(metadata) = state.indices.get(index_name) else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(routing) = metadata.shard_routing.get(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if &routing.primary != primary {
                    return ClusterResponse::Error(format!(
                        "primary mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                let Some(allocations) = state
                    .shard_allocations
                    .get_mut(index_name)
                    .and_then(|shards| shards.get_mut(shard_id))
                else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' shard {shard_id} has no allocation identity metadata"
                    ));
                };
                if allocations.primary != Some(*allocation_id) {
                    return ClusterResponse::Error(format!(
                        "primary allocation mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if !allocations.primary_initialized {
                    return ClusterResponse::Error(format!(
                        "cannot mark an uninitialized primary unavailable for index '{index_name}' shard {shard_id}"
                    ));
                }
                if allocations.primary_unavailable {
                    return ClusterResponse::Error(format!(
                        "primary is already marked unavailable for index '{index_name}' shard {shard_id}"
                    ));
                }
                allocations.primary_unavailable = true;
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::MarkPrimaryAvailable {
                index_name,
                index_uuid,
                shard_id,
                primary,
                allocation_id,
                primary_term,
            } => {
                let Some(metadata) = state.indices.get(index_name) else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(routing) = metadata.shard_routing.get(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if &routing.primary != primary {
                    return ClusterResponse::Error(format!(
                        "primary mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.primary_term != *primary_term {
                    return ClusterResponse::Error(format!(
                        "primary term mismatch for index '{index_name}' shard {shard_id}: expected {}, got {}",
                        routing.primary_term, primary_term
                    ));
                }
                let Some(allocations) = state
                    .shard_allocations
                    .get_mut(index_name)
                    .and_then(|shards| shards.get_mut(shard_id))
                else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' shard {shard_id} has no allocation identity metadata"
                    ));
                };
                if allocations.primary != Some(*allocation_id) {
                    return ClusterResponse::Error(format!(
                        "primary allocation mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if !allocations.primary_unavailable {
                    return ClusterResponse::Error(format!(
                        "primary is not marked unavailable for index '{index_name}' shard {shard_id}"
                    ));
                }
                allocations.primary_unavailable = false;
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::FailShardCopy {
                index_name,
                index_uuid,
                shard_id,
                node,
                allocation_id,
                expected_primary_term,
                promote_only,
                promotion_candidate,
            } => {
                if *allocation_id == 0 {
                    return ClusterResponse::Error(format!(
                        "failed-copy allocation ID must be greater than zero for index '{index_name}' shard {shard_id}"
                    ));
                }
                let Some(current_metadata) = state.indices.get(index_name).cloned() else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if current_metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(current_allocations) = state
                    .shard_allocations
                    .get(index_name)
                    .and_then(|shards| shards.get(shard_id))
                    .cloned()
                else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' shard {shard_id} has no allocation identity metadata"
                    ));
                };
                let Some(current_routing) = current_metadata.shard_routing.get(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if *expected_primary_term == 0 {
                    return ClusterResponse::Error(format!(
                        "failed-copy primary term must be greater than zero for index '{index_name}' shard {shard_id}"
                    ));
                }
                if current_routing.primary_term != *expected_primary_term {
                    return ClusterResponse::Error(format!(
                        "primary term mismatch for failed copy of index '{index_name}' shard {shard_id}: expected {}, got {}",
                        current_routing.primary_term, expected_primary_term
                    ));
                }
                if !current_allocations.primary_initialized {
                    return ClusterResponse::Error(format!(
                        "cannot fail shard copy for uninitialized index '{index_name}' shard {shard_id}"
                    ));
                }
                if current_allocations.allocation_for(current_routing, node) != Some(*allocation_id)
                {
                    return ClusterResponse::Error(format!(
                        "failed-copy allocation mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }

                let mut metadata = current_metadata;
                let mut allocations = current_allocations;
                let is_primary = metadata.shard_routing[shard_id].primary == *node;
                if is_primary {
                    if !*promote_only {
                        return ClusterResponse::Error(format!(
                            "primary shard failure must be promote-only for index '{index_name}' shard {shard_id}"
                        ));
                    }
                    if let Some(candidate) = promotion_candidate.clone() {
                        if !metadata.shard_routing[shard_id].is_replica_in_sync(&candidate) {
                            return ClusterResponse::Error(format!(
                                "promotion candidate '{candidate}' is not in sync for index '{index_name}' shard {shard_id}"
                            ));
                        }
                        let Some(candidate_allocation) =
                            allocations.replicas.get(&candidate).copied()
                        else {
                            return ClusterResponse::Error(format!(
                                "cannot promote replica '{candidate}' without an allocation ID for index '{index_name}' shard {shard_id}"
                            ));
                        };
                        let current_term = metadata.shard_routing[shard_id].primary_term;
                        let Some(next_term) = current_term.checked_add(1) else {
                            return ClusterResponse::Error(format!(
                                "primary term exhausted for index '{index_name}' shard {shard_id}"
                            ));
                        };
                        if !metadata.promote_replica_to(*shard_id, &candidate) {
                            return ClusterResponse::Error(format!(
                                "failed to promote in-sync replica '{candidate}' for index '{index_name}' shard {shard_id}"
                            ));
                        }
                        allocations.replicas.remove(&candidate);
                        allocations.primary = Some(candidate_allocation);
                        allocations.primary_unavailable = false;
                        let routing = metadata
                            .shard_routing
                            .get_mut(shard_id)
                            .expect("routing exists after promotion candidate selection");
                        routing.primary_term = next_term;
                        let Some(next_unassigned) = routing.unassigned_replicas.checked_add(1)
                        else {
                            return ClusterResponse::Error(format!(
                                "unassigned copy count exhausted for index '{index_name}' shard {shard_id}"
                            ));
                        };
                        routing.unassigned_replicas = next_unassigned;
                    } else {
                        return ClusterResponse::Error(format!(
                            "cannot apply promote-only primary failure for index '{index_name}' shard {shard_id} without an in-sync replica"
                        ));
                    }
                } else {
                    if *promote_only {
                        return ClusterResponse::Error(format!(
                            "promote-only shard failure requires the current primary for index '{index_name}' shard {shard_id}"
                        ));
                    }
                    if promotion_candidate.is_some() {
                        return ClusterResponse::Error(format!(
                            "replica shard failure cannot carry a promotion candidate for index '{index_name}' shard {shard_id}"
                        ));
                    }
                    let Some(routing) = metadata.shard_routing.get_mut(shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{index_name}' has no shard {shard_id}"
                        ));
                    };
                    let replicas_before = routing.replicas.len();
                    routing.replicas.retain(|replica| replica != node);
                    if routing.replicas.len() == replicas_before {
                        return ClusterResponse::Error(format!(
                            "node '{node}' is not assigned to index '{index_name}' shard {shard_id}"
                        ));
                    }
                    routing.in_sync_replicas.retain(|replica| replica != node);
                    let Some(next_unassigned) = routing.unassigned_replicas.checked_add(1) else {
                        return ClusterResponse::Error(format!(
                            "unassigned copy count exhausted for index '{index_name}' shard {shard_id}"
                        ));
                    };
                    routing.unassigned_replicas = next_unassigned;
                    allocations.replicas.remove(node);
                }

                let routing = &metadata.shard_routing[shard_id];
                if let Err(reason) = routing.validate_membership() {
                    return ClusterResponse::Error(format!(
                        "invalid routing after failing copy for index '{index_name}' shard {shard_id}: {reason}"
                    ));
                }
                if let Err(reason) = allocations.validate_for_routing(routing) {
                    return ClusterResponse::Error(format!(
                        "invalid allocation metadata after failing copy for index '{index_name}' shard {shard_id}: {reason}"
                    ));
                }
                #[cfg(feature = "protocol-trace")]
                let promoted_trace = is_primary.then(|| {
                    (
                        routing.primary.clone(),
                        routing.primary_term,
                        routing.in_sync_replicas.clone(),
                    )
                });
                #[cfg(feature = "protocol-trace")]
                let removed_trace = (!is_primary)
                    .then(|| (routing.primary.clone(), routing.in_sync_replicas.clone()));
                state.indices.insert(index_name.clone(), metadata);
                state
                    .shard_allocations
                    .get_mut(index_name)
                    .expect("validated allocation map exists")
                    .insert(*shard_id, allocations);
                state.version += 1;
                #[cfg(feature = "protocol-trace")]
                if let Some((new_primary, term, in_sync)) = promoted_trace {
                    crate::protocol_trace::record_routing_promoted(
                        &new_primary,
                        index_uuid,
                        *shard_id,
                        &new_primary,
                        term,
                        &in_sync,
                    );
                }
                #[cfg(feature = "protocol-trace")]
                if let Some((emitter, in_sync)) = removed_trace {
                    crate::protocol_trace::record_in_sync_removed(
                        &emitter,
                        index_uuid,
                        *shard_id,
                        node,
                        *allocation_id,
                        &in_sync,
                    );
                }
                ClusterResponse::Ok
            }
            ClusterCommand::AddMappings {
                index_name,
                new_fields,
                dynamic,
            } => {
                if let Some(existing) = state.indices.get_mut(index_name) {
                    for (name, mapping) in new_fields {
                        existing
                            .mappings
                            .entry(name.clone())
                            .or_insert(mapping.clone());
                    }
                    existing.dynamic = dynamic.clone();
                    state.version += 1;
                }
                ClusterResponse::Ok
            }
            ClusterCommand::PutApiKey { record } => {
                state.api_keys.insert(record.id.clone(), record.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::DeleteApiKey { key_id } => {
                state.api_keys.remove(key_id);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::PutRole { role } => {
                state.roles.insert(role.name.clone(), role.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::DeleteRole { name } => {
                state.roles.remove(name);
                state.version += 1;
                ClusterResponse::Ok
            }
        }
    }

    #[cfg(test)]
    fn apply_command(&self, cmd: &ClusterCommand) -> ClusterResponse {
        let raft_log_index = self
            .state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .version
            .saturating_add(1)
            .max(1);
        self.apply_command_at(cmd, raft_log_index)
    }
}

impl RaftStateMachine<TypeConfig> for ClusterStateMachine {
    type SnapshotBuilder = ClusterSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<
                    (Entry, Option<openraft::storage::ApplyResponder<TypeConfig>>),
                    io::Error,
                >,
            > + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        futures::pin_mut!(entries);

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;

            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ClusterResponse::Ok,
                EntryPayload::Normal(cmd) => self.apply_command_at(&cmd, entry.log_id.index),
                EntryPayload::Membership(ref mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    ClusterResponse::Ok
                }
            };

            if let Some(tx) = responder {
                tx.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner()).clone();
        ClusterSnapshotBuilder {
            state,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let new_state: ClusterState = serde_json::from_slice(&data).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                crate::consensus::UnsupportedRaftFormatError::new(
                    "Raft cluster-state snapshot",
                    format!("cannot decode current snapshot format: {error}"),
                ),
            )
        })?;
        if new_state.format_version != crate::cluster::state::CLUSTER_STATE_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                crate::consensus::UnsupportedRaftFormatError::new(
                    "Raft cluster-state snapshot",
                    format!(
                        "version {} is not supported; expected {}",
                        new_state.format_version,
                        crate::cluster::state::CLUSTER_STATE_FORMAT_VERSION
                    ),
                ),
            ));
        }

        {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            *state = new_state;
        }

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner()).clone();
        let data = serde_json::to_vec(&state).map_err(io::Error::other)?;

        let snapshot_id = format!("snap-{}", self.last_applied.map(|l| l.index).unwrap_or(0));

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}

// ─── Snapshot Builder ───────────────────────────────────────────────────────

pub struct ClusterSnapshotBuilder {
    state: ClusterState,
    last_applied: Option<LogId>,
    last_membership: StoredMembership,
}

impl RaftSnapshotBuilder<TypeConfig> for ClusterSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        let data = serde_json::to_vec(&self.state).map_err(io::Error::other)?;

        let snapshot_id = format!("snap-{}", self.last_applied.map(|l| l.index).unwrap_or(0));

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{IndexMetadata, NodeInfo, NodeRole, ShardRoutingEntry};
    use std::collections::HashMap;

    fn make_node(id: &str) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: 0,
        }
    }

    fn make_index(name: &str) -> IndexMetadata {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        IndexMetadata {
            name: name.into(),
            uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            dynamic: Default::default(),
            settings: crate::cluster::state::IndexSettings::default(),
        }
    }

    #[test]
    fn new_state_machine_has_empty_state() {
        let sm = ClusterStateMachine::new("test-cluster".into());
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.cluster_name, "test-cluster");
        assert!(state.nodes.is_empty());
        assert!(state.indices.is_empty());
        assert_eq!(state.version, 0);
    }

    #[test]
    fn state_handle_returns_shared_ref() {
        let sm = ClusterStateMachine::new("test".into());
        let h1 = sm.state_handle();
        let h2 = sm.state_handle();
        // Both handles point to the same underlying RwLock
        assert!(std::sync::Arc::ptr_eq(&h1, &h2));
    }

    #[test]
    fn apply_add_node_command() {
        let sm = ClusterStateMachine::new("test".into());
        let node = make_node("n1");
        sm.apply_command(&ClusterCommand::AddNode { node: node.clone() });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.nodes.contains_key("n1"));
        assert_eq!(state.nodes["n1"].name, "n1");
        assert_eq!(state.version, 1);
    }

    #[test]
    fn apply_remove_node_command() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::AddNode {
            node: make_node("n1"),
        });
        sm.apply_command(&ClusterCommand::RemoveNode {
            node_id: "n1".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.nodes.is_empty());
    }

    #[test]
    fn apply_create_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        let idx = make_index("my-index");
        let response = sm.apply_command(&ClusterCommand::CreateIndex { metadata: idx });
        assert_eq!(response, ClusterResponse::Ok);

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.indices.contains_key("my-index"));
        assert_eq!(state.indices["my-index"].number_of_shards, 1);
    }

    #[test]
    fn apply_delete_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx"),
        });
        sm.apply_command(&ClusterCommand::DeleteIndex {
            index_name: "idx".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(!state.indices.contains_key("idx"));
    }

    #[test]
    fn apply_update_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        // Create index with unassigned replicas
        let mut idx = IndexMetadata::build_shard_routing("products", 1, 2, &["node-1".into()]);
        assert_eq!(idx.unassigned_replica_count(), 2);
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: idx.clone(),
        });

        // Simulate allocator: assign replicas
        idx.allocate_unassigned_replicas(&["node-1".into(), "node-2".into(), "node-3".into()]);
        assert_eq!(idx.unassigned_replica_count(), 0);

        // Apply UpdateIndex
        let response = sm.apply_command(&ClusterCommand::UpdateIndex { metadata: idx });
        assert_eq!(response, ClusterResponse::Ok);

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let updated = &state.indices["products"];
        assert_eq!(updated.unassigned_replica_count(), 0);
        assert_eq!(updated.shard_routing[&0].replicas.len(), 2);
    }

    #[test]
    fn create_index_rejects_zero_primary_term() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("legacy");
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 0;

        let response = sm.apply_command(&ClusterCommand::CreateIndex { metadata });

        assert!(matches!(
            response,
            ClusterResponse::Error(error) if error.contains("primary term at least 1")
        ));
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(!state.indices.contains_key("legacy"));
        assert_eq!(state.version, 0);
    }

    #[test]
    fn update_index_cannot_add_in_sync_members() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        metadata.shard_routing.get_mut(&0).unwrap().replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        metadata.shard_routing.get_mut(&0).unwrap().in_sync_replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::UpdateIndex { metadata }),
            ClusterResponse::Ok
        );

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(
            state.indices["idx"].shard_routing[&0]
                .in_sync_replicas
                .is_empty()
        );
    }

    #[test]
    fn update_index_rejects_out_of_sync_primary_without_partial_apply() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        metadata.shard_routing.get_mut(&0).unwrap().replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        let version_before = sm.state_handle().read().unwrap().version;
        let routing = metadata.shard_routing.get_mut(&0).unwrap();
        routing.primary = "node-2".into();
        routing.replicas.clear();
        routing.primary_term = 99;
        metadata.settings.refresh_interval_ms = Some(1234);

        let response = sm.apply_command(&ClusterCommand::UpdateIndex { metadata });

        assert!(matches!(
            response,
            ClusterResponse::Error(error) if error.contains("out-of-sync replica")
        ));
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let current = &state.indices["idx"];
        assert_eq!(current.shard_routing[&0].primary, "node-1");
        assert_eq!(current.shard_routing[&0].primary_term, 1);
        assert_eq!(current.settings.refresh_interval_ms, None);
        assert_eq!(state.version, version_before);
    }

    #[test]
    fn update_index_promotes_in_sync_primary_and_computes_next_term() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 2;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.primary_term = 7;
            routing.replicas = vec!["node-2".into(), "node-3".into()];
            routing.in_sync_replicas = routing.replicas.clone();
        }
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        assert!(metadata.promote_replica_to(0, "node-2"));
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 999;
        assert_eq!(
            sm.apply_command(&ClusterCommand::UpdateIndex { metadata }),
            ClusterResponse::Ok
        );

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let routing = &state.indices["idx"].shard_routing[&0];
        assert_eq!(routing.primary, "node-2");
        assert_eq!(routing.primary_term, 8);
        assert_eq!(routing.replicas, ["node-3"]);
        assert_eq!(routing.in_sync_replicas, ["node-3"]);
    }

    #[test]
    fn mark_replica_in_sync_enforces_all_compare_and_set_fields() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.primary_term = 3;
            routing.replicas = vec!["node-2".into()];
        }
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex { metadata }),
            ClusterResponse::Ok
        );
        let allocation_id = sm
            .state_handle()
            .read()
            .unwrap()
            .shard_allocation_id("idx", 0, "node-2")
            .unwrap();
        let pre_activation = ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: index_uuid.clone(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id,
            primary: "node-1".into(),
            primary_term: 3,
        };
        assert!(matches!(
            sm.apply_command(&pre_activation),
            ClusterResponse::Error(error) if error.contains("before the primary is initialized")
        ));
        let primary_allocation_id = sm
            .state_handle()
            .read()
            .unwrap()
            .primary_allocation_id("idx", 0)
            .unwrap();
        assert_eq!(
            sm.apply_command(&ClusterCommand::ActivatePrimary {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                primary: "node-1".into(),
                allocation_id: primary_allocation_id,
                expected_term: 3,
            }),
            ClusterResponse::Ok
        );
        let version_before = sm.state_handle().read().unwrap().version;

        for command in [
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: "wrong".into(),
                shard_id: 0,
                replica: "node-2".into(),
                allocation_id,
                primary: "node-1".into(),
                primary_term: 4,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-2".into(),
                allocation_id,
                primary: "wrong".into(),
                primary_term: 4,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-2".into(),
                allocation_id,
                primary: "node-1".into(),
                primary_term: 2,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-3".into(),
                allocation_id,
                primary: "node-1".into(),
                primary_term: 4,
            },
        ] {
            assert!(matches!(
                sm.apply_command(&command),
                ClusterResponse::Error(_)
            ));
        }
        assert_eq!(sm.state_handle().read().unwrap().version, version_before);

        let command = ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid,
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id,
            primary: "node-1".into(),
            primary_term: 4,
        };
        assert_eq!(sm.apply_command(&command), ClusterResponse::Ok);
        assert_eq!(
            sm.state_handle().read().unwrap().indices["idx"].shard_routing[&0].in_sync_replicas,
            ["node-2"]
        );
        assert!(matches!(
            sm.apply_command(&command),
            ClusterResponse::Error(error) if error.contains("already in sync")
        ));
    }

    #[test]
    fn mark_replica_in_sync_rejects_a_red_shard_without_primary_allocation() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("red-admission");
        metadata.number_of_replicas = 1;
        metadata.shard_routing.get_mut(&0).unwrap().replicas = vec!["node-2".into()];
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "red-admission".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        sm.state_handle()
            .write()
            .unwrap()
            .shard_allocations
            .get_mut("red-admission")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary = None;
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::MarkReplicaInSync {
                    index_name: "red-admission".into(),
                    index_uuid,
                    shard_id: 0,
                    replica: "node-2".into(),
                    allocation_id: 10,
                    primary: "node-1".into(),
                    primary_term: 2,
                },
                13,
            ),
            ClusterResponse::Error(error)
                if error.contains("without an allocated primary")
        ));
    }

    #[test]
    fn activate_primary_enforces_compare_and_set_and_advances_once() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 4;
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex { metadata }),
            ClusterResponse::Ok
        );
        let allocation_id = sm
            .state_handle()
            .read()
            .unwrap()
            .shard_allocation_id("idx", 0, "node-1")
            .unwrap();

        let stale = ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: index_uuid.clone(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id,
            expected_term: 3,
        };
        assert!(matches!(
            sm.apply_command(&stale),
            ClusterResponse::Error(error) if error.contains("primary term mismatch")
        ));

        let command = ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid,
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id,
            expected_term: 4,
        };
        assert_eq!(sm.apply_command(&command), ClusterResponse::Ok);
        assert_eq!(
            sm.state_handle().read().unwrap().indices["idx"].shard_routing[&0].primary_term,
            5
        );
        assert!(matches!(
            sm.apply_command(&command),
            ClusterResponse::Error(error) if error.contains("primary term mismatch")
        ));
    }

    #[test]
    fn allocation_ids_follow_assignment_log_positions_and_clear_on_removal() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.replicas = vec!["node-2".into()];
            routing.in_sync_replicas.clear();
        }
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::CreateIndex {
                    metadata: metadata.clone(),
                },
                10,
            ),
            ClusterResponse::Ok
        );
        {
            let state = sm.state_handle();
            let state = state.read().unwrap();
            assert_eq!(state.primary_allocation_id("idx", 0), Some(10));
            assert_eq!(state.shard_allocation_id("idx", 0, "node-2"), Some(10));
            assert!(!state.primary_initialized("idx", 0));
        }

        let mut removed = metadata.clone();
        {
            let routing = removed.shard_routing.get_mut(&0).unwrap();
            routing.replicas.clear();
            routing.in_sync_replicas.clear();
            routing.unassigned_replicas = 1;
        }
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::UpdateIndex { metadata: removed }, 20),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.state_handle()
                .read()
                .unwrap()
                .shard_allocation_id("idx", 0, "node-2"),
            None
        );

        let mut reallocated = sm.state_handle().read().unwrap().indices["idx"].clone();
        assert!(reallocated.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::UpdateIndex {
                    metadata: reallocated,
                },
                30,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.state_handle()
                .read()
                .unwrap()
                .shard_allocation_id("idx", 0, "node-2"),
            Some(30)
        );
    }

    #[test]
    fn activate_primary_marks_initialized_only_for_exact_allocation() {
        let sm = ClusterStateMachine::new("test".into());
        let metadata = make_index("idx");
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "idx".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 9,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Error(error) if error.contains("allocation mismatch")
        ));
        assert!(
            !sm.state_handle()
                .read()
                .unwrap()
                .primary_initialized("idx", 0)
        );

        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "idx".into(),
                    index_uuid,
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                12,
            ),
            ClusterResponse::Ok
        );
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert!(state.primary_initialized("idx", 0));
        assert_eq!(state.indices["idx"].shard_routing[&0].primary_term, 2);
    }

    #[test]
    fn fail_shard_copy_removes_replica_and_rejects_stale_identity() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.replicas = vec!["node-2".into()];
            routing.in_sync_replicas = vec!["node-2".into()];
        }

        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "idx".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );

        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "idx".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    node: "node-2".into(),
                    allocation_id: 9,
                    expected_primary_term: 2,
                    promote_only: false,
                    promotion_candidate: None,
                },
                12,
            ),
            ClusterResponse::Error(error) if error.contains("allocation mismatch")
        ));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "idx".into(),
                    index_uuid,
                    shard_id: 0,
                    node: "node-2".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: false,
                    promotion_candidate: None,
                },
                13,
            ),
            ClusterResponse::Ok
        );
        let state = sm.state_handle();
        let state = state.read().unwrap();
        let routing = &state.indices["idx"].shard_routing[&0];
        assert!(routing.replicas.is_empty());
        assert!(routing.in_sync_replicas.is_empty());
        assert_eq!(routing.unassigned_replicas, 1);
        assert_eq!(state.shard_allocation_id("idx", 0, "node-2"), None);
        drop(state);

        let mut replacement = sm.state_handle().read().unwrap().indices["idx"].clone();
        assert!(replacement.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::UpdateIndex {
                    metadata: replacement,
                },
                14,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.state_handle()
                .read()
                .unwrap()
                .shard_allocation_id("idx", 0, "node-2"),
            Some(14)
        );
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "idx".into(),
                    index_uuid: "test-uuid".into(),
                    shard_id: 0,
                    node: "node-2".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: false,
                    promotion_candidate: None,
                },
                15,
            ),
            ClusterResponse::Error(error) if error.contains("allocation mismatch")
        ));
        assert_eq!(
            sm.state_handle()
                .read()
                .unwrap()
                .shard_allocation_id("idx", 0, "node-2"),
            Some(14)
        );
    }

    #[test]
    fn delayed_old_term_failure_cannot_remove_replica() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.replicas = vec!["node-2".into()];
            routing.in_sync_replicas = vec!["node-2".into()];
        }
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "idx".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "idx".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 2,
                },
                12,
            ),
            ClusterResponse::Ok
        );
        let version_before = sm.state_handle().read().unwrap().version;

        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "idx".into(),
                    index_uuid,
                    shard_id: 0,
                    node: "node-2".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: false,
                    promotion_candidate: None,
                },
                13,
            ),
            ClusterResponse::Error(error) if error.contains("primary term mismatch")
        ));
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert_eq!(state.version, version_before);
        assert_eq!(state.shard_allocation_id("idx", 0, "node-2"), Some(10));
    }

    #[test]
    fn fail_shard_copy_is_rejected_before_first_primary_activation() {
        let sm = ClusterStateMachine::new("test".into());
        let metadata = make_index("idx");
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        let version_before = sm.state_handle().read().unwrap().version;

        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "idx".into(),
                    index_uuid,
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 1,
                    promote_only: false,
                    promotion_candidate: None,
                },
                11,
            ),
            ClusterResponse::Error(error) if error.contains("uninitialized")
        ));
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert_eq!(state.version, version_before);
        assert_eq!(state.primary_allocation_id("idx", 0), Some(10));
        assert!(!state.primary_initialized("idx", 0));
    }

    #[test]
    fn fail_primary_promotes_in_sync_copy() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("promote");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.replicas = vec!["node-2".into()];
            routing.in_sync_replicas = vec!["node-2".into()];
        }
        let promote_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "promote".into(),
                    index_uuid: promote_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "promote".into(),
                    index_uuid: promote_uuid,
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: true,
                    promotion_candidate: Some("node-2".into()),
                },
                13,
            ),
            ClusterResponse::Ok
        );
        {
            let state = sm.state_handle();
            let state = state.read().unwrap();
            let routing = &state.indices["promote"].shard_routing[&0];
            assert_eq!(routing.primary, "node-2");
            assert_eq!(routing.primary_term, 3);
            assert_eq!(routing.unassigned_replicas, 1);
            assert_eq!(state.primary_allocation_id("promote", 0), Some(10));
        }
    }

    #[test]
    fn promote_only_primary_failure_never_clears_the_last_primary_allocation() {
        let sm = ClusterStateMachine::new("test".into());
        let metadata = make_index("single-copy");
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "single-copy".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        let version_before = sm.state_handle().read().unwrap().version;

        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "single-copy".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: false,
                    promotion_candidate: None,
                },
                12,
            ),
            ClusterResponse::Error(error) if error.contains("must be promote-only")
        ));
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "single-copy".into(),
                    index_uuid,
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: true,
                    promotion_candidate: None,
                },
                13,
            ),
            ClusterResponse::Error(error) if error.contains("without an in-sync replica")
        ));
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert_eq!(state.version, version_before);
        assert_eq!(state.primary_allocation_id("single-copy", 0), Some(10));
        assert_eq!(
            state.indices["single-copy"].shard_routing[&0].unassigned_replicas,
            0
        );
    }

    #[test]
    fn primary_availability_is_conditional_and_never_changes_term() {
        let sm = ClusterStateMachine::new("test".into());
        let metadata = make_index("unavailable");
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryUnavailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 9,
                },
                12,
            ),
            ClusterResponse::Error(
                "primary allocation mismatch for index 'unavailable' shard 0".into()
            )
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryUnavailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                },
                13,
            ),
            ClusterResponse::Ok
        );
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryUnavailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                },
                14,
            ),
            ClusterResponse::Error(error) if error.contains("already marked unavailable")
        ));
        {
            let state = sm.state_handle();
            let state = state.read().unwrap();
            assert!(state.primary_unavailable("unavailable", 0));
            assert_eq!(state.primary_allocation_id("unavailable", 0), Some(10));
            assert_eq!(
                state.indices["unavailable"].shard_routing[&0].primary,
                "node-1"
            );
            assert_eq!(
                state.indices["unavailable"].shard_routing[&0].primary_term,
                2
            );
        }
        let unavailable_version = sm.state_handle().read().unwrap().version;
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryAvailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 9,
                    primary_term: 2,
                },
                15,
            ),
            ClusterResponse::Error(error) if error.contains("allocation mismatch")
        ));
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryAvailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    primary_term: 3,
                },
                16,
            ),
            ClusterResponse::Error(error) if error.contains("term mismatch")
        ));
        assert_eq!(
            sm.state_handle().read().unwrap().version,
            unavailable_version
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryAvailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    primary_term: 2,
                },
                17,
            ),
            ClusterResponse::Ok
        );
        {
            let state = sm.state_handle();
            let state = state.read().unwrap();
            assert!(!state.primary_unavailable("unavailable", 0));
            assert_eq!(
                state.indices["unavailable"].shard_routing[&0].primary_term,
                2
            );
            assert_eq!(state.version, unavailable_version + 1);
        }
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryAvailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    primary_term: 2,
                },
                18,
            ),
            ClusterResponse::Error(error) if error.contains("not marked unavailable")
        ));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkPrimaryUnavailable {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                },
                19,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "unavailable".into(),
                    index_uuid,
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 2,
                },
                20,
            ),
            ClusterResponse::Ok
        );
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert!(!state.primary_unavailable("unavailable", 0));
        assert_eq!(
            state.indices["unavailable"].shard_routing[&0].primary_term,
            3
        );
    }

    #[test]
    fn promote_only_failure_validates_the_leader_selected_candidate() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("candidate");
        metadata.number_of_replicas = 2;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.replicas = vec!["node-2".into(), "node-3".into()];
            routing.in_sync_replicas = vec!["node-2".into()];
        }
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "candidate".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 1,
                },
                11,
            ),
            ClusterResponse::Ok
        );
        assert!(matches!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "candidate".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: true,
                    promotion_candidate: Some("node-3".into()),
                },
                12,
            ),
            ClusterResponse::Error(error) if error.contains("not in sync")
        ));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::FailShardCopy {
                    index_name: "candidate".into(),
                    index_uuid,
                    shard_id: 0,
                    node: "node-1".into(),
                    allocation_id: 10,
                    expected_primary_term: 2,
                    promote_only: true,
                    promotion_candidate: Some("node-2".into()),
                },
                13,
            ),
            ClusterResponse::Ok
        );
        assert_eq!(
            sm.state_handle().read().unwrap().indices["candidate"].shard_routing[&0].primary,
            "node-2"
        );
    }

    #[test]
    fn red_sibling_shard_does_not_block_update_index() {
        let sm = ClusterStateMachine::new("test".into());
        for node_id in ["node-1", "node-2", "node-3"] {
            assert_eq!(
                sm.apply_command(&ClusterCommand::AddNode {
                    node: make_node(node_id),
                }),
                ClusterResponse::Ok
            );
        }
        let mut metadata = make_index("idx");
        metadata.number_of_shards = 2;
        metadata.number_of_replicas = 1;
        metadata.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: vec!["node-2".into()],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        metadata.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-2".into(),
                primary_term: 1,
                replicas: vec!["node-1".into()],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        let uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 10),
            ClusterResponse::Ok
        );
        for (shard_id, primary, log_index) in [(0u32, "node-1", 11u64), (1, "node-2", 12)] {
            assert_eq!(
                sm.apply_command_at(
                    &ClusterCommand::ActivatePrimary {
                        index_name: "idx".into(),
                        index_uuid: uuid.clone(),
                        shard_id,
                        primary: primary.into(),
                        allocation_id: 10,
                        expected_term: 1,
                    },
                    log_index,
                ),
                ClusterResponse::Ok
            );
        }
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkReplicaInSync {
                    index_name: "idx".into(),
                    index_uuid: uuid.clone(),
                    shard_id: 1,
                    replica: "node-1".into(),
                    allocation_id: 10,
                    primary: "node-2".into(),
                    primary_term: 2,
                },
                13,
            ),
            ClusterResponse::Ok
        );
        {
            let state = sm.state_handle();
            let mut state = state.write().unwrap();
            state
                .shard_allocations
                .get_mut("idx")
                .unwrap()
                .get_mut(&0)
                .unwrap()
                .primary = None;
            state
                .indices
                .get_mut("idx")
                .unwrap()
                .shard_routing
                .get_mut(&0)
                .unwrap()
                .unassigned_replicas = 1;
        }
        assert_eq!(
            sm.state_handle()
                .read()
                .unwrap()
                .primary_allocation_id("idx", 0),
            None
        );

        let mut updated = sm.state_handle().read().unwrap().indices["idx"].clone();
        let orphaned = updated.remove_node(&"node-2".to_string());
        assert_eq!(orphaned, vec![1]);
        assert!(updated.promote_replica_to(1, "node-1"));
        updated
            .shard_routing
            .get_mut(&1)
            .unwrap()
            .unassigned_replicas += 1;
        updated.settings.refresh_interval_ms = Some(1234);
        let response = sm.apply_command_at(&ClusterCommand::UpdateIndex { metadata: updated }, 15);

        assert_eq!(
            response,
            ClusterResponse::Ok,
            "healthy shard failover must not be blocked by a red sibling"
        );
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert_eq!(state.indices["idx"].shard_routing[&1].primary, "node-1");
        assert_eq!(
            state.indices["idx"].settings.refresh_interval_ms,
            Some(1234)
        );
        drop(state);

        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::RemoveNode {
                    node_id: "node-2".into(),
                },
                16,
            ),
            ClusterResponse::Ok
        );
        let mut allocated = sm.state_handle().read().unwrap().indices["idx"].clone();
        assert!(allocated.allocate_unassigned_replicas_for_shards(
            &["node-1".into(), "node-3".into()],
            &std::collections::HashSet::from([1]),
        ));
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::UpdateIndex {
                    metadata: allocated,
                },
                17,
            ),
            ClusterResponse::Ok,
            "replica allocation for a healthy shard must not be blocked by a red sibling"
        );
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert!(!state.nodes.contains_key("node-2"));
        assert!(state.indices["idx"].shard_routing[&0].replicas.is_empty());
        assert_eq!(state.indices["idx"].shard_routing[&1].replicas, ["node-3"]);
        assert_eq!(state.shard_allocation_id("idx", 1, "node-3"), Some(17));
    }

    #[test]
    fn update_index_preserves_other_indices() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx-a"),
        });
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx-b"),
        });

        // Update only idx-a
        let mut updated_a = make_index("idx-a");
        updated_a.number_of_replicas = 99;
        sm.apply_command(&ClusterCommand::UpdateIndex {
            metadata: updated_a,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.indices["idx-a"].number_of_replicas, 99);
        assert_eq!(
            state.indices["idx-b"].number_of_replicas, 0,
            "idx-b should be untouched"
        );
    }

    #[test]
    fn applied_state_returns_none_initially() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("test".into());
            let (log_id, membership) = sm.applied_state().await.unwrap();
            assert!(log_id.is_none());
            // Default membership has no log id
            assert!(membership.log_id().is_none());
        });
    }

    #[test]
    fn snapshot_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("snap-test".into());

            // Add some state
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("n1"),
            });
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: make_index("idx1"),
            });

            // Get snapshot
            let snap = sm.get_current_snapshot().await.unwrap().unwrap();
            assert_eq!(snap.meta.snapshot_id, "snap-0"); // last_applied is None

            // Deserialize snapshot data to verify state
            let data = snap.snapshot.into_inner();
            let restored: ClusterState = serde_json::from_slice(&data).unwrap();
            assert_eq!(restored.cluster_name, "snap-test");
            assert!(restored.nodes.contains_key("n1"));
            assert!(restored.indices.contains_key("idx1"));
        });
    }

    #[test]
    fn install_snapshot_replaces_state() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("original".into());
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("old"),
            });

            // Build snapshot data from a different state
            let mut new_state = ClusterState::new("replaced".into());
            new_state.add_node(make_node("new-node"));
            let snap_data = serde_json::to_vec(&new_state).unwrap();

            let meta = SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-install".into(),
            };

            sm.install_snapshot(&meta, Cursor::new(snap_data))
                .await
                .unwrap();

            let handle = sm.state_handle();
            let state = handle.read().unwrap();
            assert_eq!(state.cluster_name, "replaced");
            assert!(state.nodes.contains_key("new-node"));
            assert!(!state.nodes.contains_key("old"));
        });
    }

    #[test]
    fn no_compat_old_raft_snapshot_requires_recreate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("original".into());
            let mut old_state = ClusterState::new("old".into());
            old_state.format_version = 0;
            let snap_data = serde_json::to_vec(&old_state).unwrap();
            let meta = SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "old-format".into(),
            };

            let error = sm
                .install_snapshot(&meta, Cursor::new(snap_data))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(
                error
                    .to_string()
                    .contains("wipe the node data directories and recreate the cluster")
            );
            assert!(!error.to_string().contains("recreate the index"));
        });
    }

    #[test]
    fn malformed_raft_snapshot_requires_cluster_recreation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("original".into());
            let meta = SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "malformed".into(),
            };

            let error = sm
                .install_snapshot(&meta, Cursor::new(b"{not-json".to_vec()))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(
                error
                    .to_string()
                    .contains("wipe the node data directories and recreate the cluster")
            );
            assert!(!error.to_string().contains("recreate the index"));
        });
    }

    #[test]
    fn snapshot_builder_builds_correct_snapshot() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("builder-test".into());
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("b1"),
            });

            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.unwrap();

            let data = snap.snapshot.into_inner();
            let restored: ClusterState = serde_json::from_slice(&data).unwrap();
            assert!(restored.nodes.contains_key("b1"));
        });
    }

    // ── AddMappings command tests ──────────────────────────────────────

    #[test]
    fn apply_add_mappings_merges_new_fields() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let meta = make_index("idx");
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: meta.clone(),
        });

        let new_fields = std::collections::HashMap::from([
            (
                "count".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "active".to_string(),
                FieldMapping {
                    field_type: FieldType::Boolean,
                    dimension: None,
                },
            ),
        ]);

        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "idx".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let idx = &state.indices["idx"];
        assert_eq!(idx.mappings.len(), 2);
        assert_eq!(idx.mappings["count"].field_type, FieldType::Integer);
        assert_eq!(idx.mappings["active"].field_type, FieldType::Boolean);
        assert_eq!(idx.dynamic, DynamicMapping::True);
    }

    #[test]
    fn apply_add_mappings_preserves_existing_fields() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let mut meta = make_index("idx");
        meta.mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: meta.clone(),
        });

        // Try to add "title" with a different type — existing should win.
        let new_fields = std::collections::HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "idx".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        // Existing mapping wins — still Text, not Keyword.
        assert_eq!(
            state.indices["idx"].mappings["title"].field_type,
            FieldType::Text
        );
    }

    #[test]
    fn apply_add_mappings_noop_for_nonexistent_index() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let new_fields = std::collections::HashMap::from([(
            "f".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "nonexistent".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        // Version should not have been bumped.
        assert_eq!(state.version, 0);
        assert!(!state.indices.contains_key("nonexistent"));
    }

    fn make_api_key(id: &str) -> crate::cluster::state::SecurityApiKeyRecord {
        crate::cluster::state::SecurityApiKeyRecord {
            id: id.into(),
            name: format!("{id}-name"),
            hash_sha256: "d".repeat(64),
            roles: vec!["read".into()],
            indices: vec![],
            created_at_millis: 7,
        }
    }

    fn make_role(name: &str) -> crate::cluster::state::SecurityRoleDefinition {
        crate::cluster::state::SecurityRoleDefinition {
            name: name.into(),
            cluster: vec!["monitor".into()],
            indices: vec!["logs-*".into()],
            index_privileges: vec!["read".into()],
        }
    }

    #[test]
    fn apply_put_api_key_inserts_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.api_keys["key-1"].name, "key-1-name");
    }

    #[test]
    fn apply_put_api_key_replaces_existing() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });
        let mut updated = make_api_key("key-1");
        updated.name = "renamed".into();
        sm.apply_command(&ClusterCommand::PutApiKey { record: updated });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert_eq!(state.api_keys.len(), 1);
        assert_eq!(state.api_keys["key-1"].name, "renamed");
    }

    #[test]
    fn apply_delete_api_key_removes_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });
        sm.apply_command(&ClusterCommand::DeleteApiKey {
            key_id: "key-1".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert!(state.api_keys.is_empty());
    }

    #[test]
    fn apply_delete_missing_api_key_still_bumps_version() {
        // Delete is idempotent like RemoveNode/DeleteIndex — it always bumps version.
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::DeleteApiKey {
            key_id: "ghost".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert!(state.api_keys.is_empty());
    }

    #[test]
    fn apply_put_role_inserts_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutRole {
            role: make_role("analyst"),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.roles["analyst"].cluster, vec!["monitor".to_string()]);
    }

    #[test]
    fn apply_delete_role_removes_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutRole {
            role: make_role("analyst"),
        });
        sm.apply_command(&ClusterCommand::DeleteRole {
            name: "analyst".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert!(state.roles.is_empty());
    }
}
