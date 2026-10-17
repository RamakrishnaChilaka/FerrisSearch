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
    #[cfg(test)]
    apply_gate: Option<Arc<TestApplyGate>>,
}

#[cfg(test)]
pub(crate) struct TestApplyGate {
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pending: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    entered: tokio::sync::watch::Sender<bool>,
}

#[cfg(test)]
impl Default for TestApplyGate {
    fn default() -> Self {
        Self {
            release: std::sync::Mutex::new(None),
            pending: std::sync::Mutex::new(None),
            entered: tokio::sync::watch::channel(false).0,
        }
    }
}

#[cfg(test)]
impl TestApplyGate {
    pub(crate) fn pause(&self) {
        let mut release = self.release.lock().unwrap();
        assert!(release.is_none(), "previous apply gate was not released");
        self.entered.send_replace(false);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.pending.lock().unwrap() = Some(receiver);
        *release = Some(sender);
    }

    pub(crate) fn resume(&self) {
        if let Some(release) = self.release.lock().unwrap().take() {
            let _ = release.send(());
        }
    }

    pub(crate) async fn wait_until_entered(&self) {
        self.entered
            .subscribe()
            .wait_for(|entered| *entered)
            .await
            .expect("apply gate remains alive");
    }

    async fn before_apply(&self) {
        let pending = self.pending.lock().unwrap().take();
        if let Some(pending) = pending {
            self.entered.send_replace(true);
            pending
                .await
                .expect("apply gate must be explicitly released");
        }
    }
}

impl ClusterStateMachine {
    pub fn new(cluster_name: String) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new(cluster_name))),
            last_applied: None,
            last_membership: StoredMembership::default(),
            #[cfg(test)]
            apply_gate: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_apply_gate(&mut self, gate: Arc<TestApplyGate>) {
        self.apply_gate = Some(gate);
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
            #[cfg(test)]
            apply_gate: None,
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
                if state.indices.contains_key(&metadata.name) {
                    return ClusterResponse::IndexAlreadyExists {
                        index_name: metadata.name.clone(),
                    };
                }
                if let Err(error) = metadata.validate_write_policy() {
                    return ClusterResponse::Error(error);
                }
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
                state.manifest_writer_authorities.remove(index_name);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::SetMaster { node_id } => {
                state.master_node = Some(node_id.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateIndex { metadata } => {
                if let Err(error) = metadata.validate_write_policy() {
                    return ClusterResponse::Error(error);
                }
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
                        primary_authority: {
                            let mut authority = current_ids.primary_authority.clone();
                            if primary_changed {
                                authority.invalidate();
                            }
                            authority
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

                let needs_manifest_authority = updated.uses_manifest_writer_authority();
                let uses_remote = updated.uses_remote_store();
                let updated_uuid = updated.uuid.to_string();
                state.indices.insert(metadata.name.clone(), updated);
                state
                    .shard_allocations
                    .insert(metadata.name.clone(), updated_allocations);
                if needs_manifest_authority {
                    let auth = state
                        .manifest_writer_authorities
                        .entry(metadata.name.clone())
                        .or_insert_with(|| {
                            crate::cluster::state::ManifestWriterAuthority::unassigned(
                                metadata.name.clone(),
                                updated_uuid,
                            )
                        });
                    if !uses_remote {
                        // When rolled back to MutableOnly, revoke active lease while preserving last_writer_epoch high-water mark.
                        auth.phase = crate::cluster::state::ManifestWriterPhase::Unassigned;
                        auth.lease_expires_at_millis = 0;
                    }
                } else {
                    state.manifest_writer_authorities.remove(&metadata.name);
                }
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
                if allocation_state.primary_authority.phase
                    != crate::cluster::state::PrimaryAuthorityPhase::Active
                {
                    return ClusterResponse::Error(
                        "replica admission requires canonically active primary authority".into(),
                    );
                }
                if allocation_state.primary_unavailable {
                    return ClusterResponse::Error(
                        "replica admission requires an available canonical primary".into(),
                    );
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
                allocations.primary_authority.reserve(raft_log_index);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::FinishPrimaryActivation {
                index_name,
                activation,
            } => {
                if let Err(reason) = activation.validate() {
                    return ClusterResponse::Error(reason);
                }
                if let Err(reason) = state.validate_write_authority(
                    index_name,
                    &activation.authority,
                    crate::cluster::state::PrimaryAuthorityPhase::Promoting,
                ) {
                    return ClusterResponse::WriteAuthorityRejected { reason };
                }
                let shard_id = activation.authority.shard_id;
                let mut metadata = state.indices[index_name].clone();
                let mut allocations = state.shard_allocations[index_name][&shard_id].clone();
                let routing = metadata
                    .shard_routing
                    .get_mut(&shard_id)
                    .expect("reserved authority validated its shard routing");
                for copy in &activation.eligible_replicas {
                    if !routing.is_replica_in_sync(&copy.node_id)
                        || allocations.replicas.get(&copy.node_id) != Some(&copy.allocation_id)
                    {
                        return ClusterResponse::Error(format!(
                            "activation cannot retain unproven allocation {} on node '{}'",
                            copy.allocation_id, copy.node_id,
                        ));
                    }
                }
                let eligible: std::collections::HashSet<_> = activation
                    .eligible_replicas
                    .iter()
                    .map(|copy| copy.node_id.as_str())
                    .collect();
                let excluded: std::collections::HashSet<_> = routing
                    .in_sync_replicas
                    .iter()
                    .filter(|node| !eligible.contains(node.as_str()))
                    .cloned()
                    .collect();
                let removed_count = match u32::try_from(excluded.len()) {
                    Ok(count) => count,
                    Err(_) => {
                        return ClusterResponse::Error(
                            "activation excluded-copy count overflows".into(),
                        );
                    }
                };
                let Some(unassigned) = routing.unassigned_replicas.checked_add(removed_count)
                else {
                    return ClusterResponse::Error(
                        "activation unassigned-copy count overflows".into(),
                    );
                };
                routing.unassigned_replicas = unassigned;
                routing.replicas.retain(|node| !excluded.contains(node));
                routing
                    .in_sync_replicas
                    .retain(|node| eligible.contains(node.as_str()));
                allocations
                    .replicas
                    .retain(|node, _| !excluded.contains(node));
                allocations
                    .primary_authority
                    .finish(activation.history.clone());
                allocations.primary_unavailable = false;
                if let Err(reason) = routing
                    .validate_membership()
                    .and_then(|()| allocations.validate_for_routing(routing))
                {
                    return ClusterResponse::Error(reason);
                }
                state.indices.insert(index_name.clone(), metadata);
                state
                    .shard_allocations
                    .get_mut(index_name)
                    .expect("reserved authority validated its allocation map")
                    .insert(shard_id, allocations);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::ExcludeFailedShardCopies {
                index_name,
                exclusion,
            } => {
                if let Err(reason) = exclusion.validate() {
                    return ClusterResponse::Error(reason);
                }
                if state.version < exclusion.captured_state_version {
                    return ClusterResponse::Error(
                        "failed-copy exclusion is ahead of applied metadata".into(),
                    );
                }
                if let Err(reason) = state.validate_write_authority(
                    index_name,
                    &exclusion.authority,
                    crate::cluster::state::PrimaryAuthorityPhase::Active,
                ) {
                    return ClusterResponse::WriteAuthorityRejected { reason };
                }
                let shard_id = exclusion.authority.shard_id;
                let mut metadata = state.indices[index_name].clone();
                let mut allocations = state.shard_allocations[index_name][&shard_id].clone();
                let routing = metadata
                    .shard_routing
                    .get_mut(&shard_id)
                    .expect("active authority validated its shard routing");
                let removed: std::collections::HashSet<_> = exclusion
                    .failed_copies
                    .iter()
                    .filter(|copy| {
                        allocations.replicas.get(&copy.node_id) == Some(&copy.allocation_id)
                    })
                    .map(|copy| copy.node_id.clone())
                    .collect();
                let removed_count = match u32::try_from(removed.len()) {
                    Ok(count) => count,
                    Err(_) => {
                        return ClusterResponse::Error(
                            "failed-copy exclusion count overflows".into(),
                        );
                    }
                };
                let Some(unassigned) = routing.unassigned_replicas.checked_add(removed_count)
                else {
                    return ClusterResponse::Error("failed-copy unassigned count overflows".into());
                };
                routing.unassigned_replicas = unassigned;
                routing.replicas.retain(|node| !removed.contains(node));
                routing
                    .in_sync_replicas
                    .retain(|node| !removed.contains(node));
                allocations
                    .replicas
                    .retain(|node, _| !removed.contains(node));
                if let Err(reason) = routing
                    .validate_membership()
                    .and_then(|()| allocations.validate_for_routing(routing))
                {
                    return ClusterResponse::Error(reason);
                }
                state.indices.insert(index_name.clone(), metadata);
                state
                    .shard_allocations
                    .get_mut(index_name)
                    .expect("active authority validated its allocation map")
                    .insert(shard_id, allocations);
                state.version += 1;
                ClusterResponse::ExclusionApplied {
                    transition_id: exclusion.transition_id.clone(),
                }
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
                        allocations.primary_authority.invalidate();
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
                    let prev_gen = existing.effective_schema_generation();
                    let prev_hash = crate::storage::compute_schema_hash(&existing.mappings);
                    let mut added_any = false;
                    for (name, mapping) in new_fields {
                        if !existing.mappings.contains_key(name) {
                            existing.mappings.insert(name.clone(), mapping.clone());
                            added_any = true;
                        }
                    }
                    let dynamic_changed = existing.dynamic != *dynamic;
                    existing.dynamic = dynamic.clone();
                    if added_any || dynamic_changed {
                        existing.record_schema_generation_advance(prev_gen, prev_hash);
                    }
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
            ClusterCommand::MutateManifestWriterAuthority {
                index_name,
                mutation,
            } => Self::apply_manifest_writer_mutation(
                &mut state,
                index_name,
                mutation,
                raft_log_index,
            ),
        }
    }

    fn apply_manifest_writer_mutation(
        state: &mut ClusterState,
        index_name: &str,
        mutation: &crate::cluster::state::ManifestWriterMutation,
        raft_log_index: u64,
    ) -> ClusterResponse {
        use crate::cluster::state::{
            ManifestLeaseAcquireReason, ManifestWriterMutation, ManifestWriterPhase,
        };

        if let Err(reason) = mutation.validate() {
            return ClusterResponse::Error(reason);
        }
        let Some(metadata) = state.indices.get(index_name) else {
            return ClusterResponse::ManifestWriterAuthorityRejected {
                reason: format!("index '{index_name}' does not exist"),
            };
        };
        if !metadata.uses_manifest_writer_authority() {
            return ClusterResponse::ManifestWriterAuthorityRejected {
                reason: format!(
                    "index '{index_name}' uses engine '{}', not remote_store",
                    metadata.settings.engine
                ),
            };
        }
        let metadata_uuid = metadata.uuid.to_string();
        if metadata_uuid != mutation.index_uuid() {
            return ClusterResponse::ManifestWriterAuthorityRejected {
                reason: format!(
                    "index '{index_name}' UUID mismatch: expected {}, got {}",
                    metadata_uuid,
                    mutation.index_uuid()
                ),
            };
        }
        let current = state
            .manifest_writer_authorities
            .get(index_name)
            .cloned()
            .unwrap_or_else(|| {
                crate::cluster::state::ManifestWriterAuthority::unassigned(
                    index_name.to_string(),
                    metadata_uuid.clone(),
                )
            });
        if current.index_uuid != mutation.index_uuid() {
            return ClusterResponse::ManifestWriterAuthorityRejected {
                reason: format!("manifest writer authority UUID mismatch for '{index_name}'"),
            };
        }

        let mut updated = current.clone();
        match mutation {
            ManifestWriterMutation::Acquire {
                expected,
                candidate_node,
                reason,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                if !current.matches_expected(expected) {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer authority for '{index_name}' changed concurrently (expected epoch={}, lease_id={}, phase={:?}; found epoch={}, lease_id={}, phase={:?})",
                            expected.last_writer_epoch,
                            expected.last_lease_id,
                            expected.phase,
                            current.last_writer_epoch,
                            current.last_lease_id,
                            current.phase,
                        ),
                    };
                }
                if !state.nodes.contains_key(candidate_node) {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "candidate node '{candidate_node}' is not a registered cluster member"
                        ),
                    };
                }
                if *leader_term < current.leader_term {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer acquire leader_term {leader_term} is older than authority leader_term {}",
                            current.leader_term
                        ),
                    };
                }
                match reason {
                    ManifestLeaseAcquireReason::Unassigned => {
                        if !current.phase.is_unassigned() {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "index '{index_name}' already has an assigned manifest writer"
                                ),
                            };
                        }
                    }
                    ManifestLeaseAcquireReason::Expired => {
                        if current.phase.is_unassigned() {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "index '{index_name}' manifest writer is unassigned, not expired"
                                ),
                            };
                        }
                        if *now_millis < current.lease_expires_at_millis {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "manifest writer lease for '{index_name}' has not expired (now_millis={now_millis} < lease_expires_at_millis={})",
                                    current.lease_expires_at_millis
                                ),
                            };
                        }
                    }
                    ManifestLeaseAcquireReason::HolderUnavailable => {
                        let Some(holder) = current.phase.holder_node() else {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "index '{index_name}' has no assigned holder to replace"
                                ),
                            };
                        };
                        if state.nodes.contains_key(holder) {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "manifest writer holder '{holder}' is still a registered cluster member"
                                ),
                            };
                        }
                    }
                    ManifestLeaseAcquireReason::Transfer { from_holder } => {
                        if current.phase.holder_node() != Some(from_holder.as_str()) {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "manifest writer transfer expected holder '{from_holder}', found {:?}",
                                    current.phase.holder_node()
                                ),
                            };
                        }
                    }
                    ManifestLeaseAcquireReason::RevokeAndReplace => {
                        if current.phase.is_unassigned() {
                            return ClusterResponse::ManifestWriterAuthorityRejected {
                                reason: format!(
                                    "index '{index_name}' has no assigned manifest writer to revoke"
                                ),
                            };
                        }
                    }
                }
                let Some(next_epoch) = current.last_writer_epoch.checked_add(1) else {
                    return ClusterResponse::Error(format!(
                        "manifest writer epoch exhausted for index '{index_name}'"
                    ));
                };
                let next_lease_id = raft_log_index.max(current.last_lease_id.saturating_add(1));
                let Some(expires_at) = now_millis.checked_add(*lease_duration_ms) else {
                    return ClusterResponse::Error(format!(
                        "manifest writer lease expiration overflowed for index '{index_name}'"
                    ));
                };
                updated.last_writer_epoch = next_epoch;
                updated.last_lease_id = next_lease_id;
                updated.renewal_seq = 0;
                updated.leader_term = *leader_term;
                updated.lease_expires_at_millis = expires_at;
                updated.phase = ManifestWriterPhase::Fencing {
                    holder_node: candidate_node.clone(),
                    writer_epoch: next_epoch,
                    lease_id: next_lease_id,
                };
            }
            ManifestWriterMutation::Activate {
                identity,
                fenced_pointer_revision,
                fenced_pointer_token,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                if !state.nodes.contains_key(&identity.holder_node) {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer holder '{}' is no longer a registered cluster member",
                            identity.holder_node
                        ),
                    };
                }
                let expected_phase = ManifestWriterPhase::Fencing {
                    holder_node: identity.holder_node.clone(),
                    writer_epoch: identity.writer_epoch,
                    lease_id: identity.lease_id,
                };
                if current.last_writer_epoch != identity.writer_epoch
                    || current.phase != expected_phase
                {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer activation for '{index_name}' does not match current fencing authority"
                        ),
                    };
                }
                if *now_millis >= current.lease_expires_at_millis {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "fencing manifest writer lease for '{index_name}' expired at {} before activation (now={now_millis})",
                            current.lease_expires_at_millis
                        ),
                    };
                }
                if *leader_term < current.leader_term {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer activation leader_term {leader_term} is older than authority leader_term {}",
                            current.leader_term
                        ),
                    };
                }
                let Some(expires_at) = now_millis.checked_add(*lease_duration_ms) else {
                    return ClusterResponse::Error(format!(
                        "manifest writer lease expiration overflowed for index '{index_name}'"
                    ));
                };
                updated.leader_term = *leader_term;
                updated.lease_expires_at_millis = expires_at;
                updated.phase = ManifestWriterPhase::Active {
                    holder_node: identity.holder_node.clone(),
                    writer_epoch: identity.writer_epoch,
                    lease_id: identity.lease_id,
                    fenced_pointer_revision: *fenced_pointer_revision,
                    fenced_pointer_token: fenced_pointer_token.clone(),
                };
            }
            ManifestWriterMutation::Renew {
                identity,
                expected_fenced_pointer_revision,
                expected_fenced_pointer_token,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                if !state.nodes.contains_key(&identity.holder_node) {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer holder '{}' is no longer a registered cluster member",
                            identity.holder_node
                        ),
                    };
                }
                let expected_phase = ManifestWriterPhase::Active {
                    holder_node: identity.holder_node.clone(),
                    writer_epoch: identity.writer_epoch,
                    lease_id: identity.lease_id,
                    fenced_pointer_revision: *expected_fenced_pointer_revision,
                    fenced_pointer_token: expected_fenced_pointer_token.clone(),
                };
                if current.last_writer_epoch != identity.writer_epoch
                    || current.phase != expected_phase
                {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer renewal for '{index_name}' does not match current active authority and pointer fence"
                        ),
                    };
                }
                if *now_millis >= current.lease_expires_at_millis {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer lease for '{index_name}' expired at {} and cannot be renewed in-place (now={now_millis})",
                            current.lease_expires_at_millis
                        ),
                    };
                }
                if *leader_term < current.leader_term {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer renewal leader_term {leader_term} is older than authority leader_term {}",
                            current.leader_term
                        ),
                    };
                }
                let Some(next_renewal_seq) = current.renewal_seq.checked_add(1) else {
                    return ClusterResponse::Error(format!(
                        "manifest writer renewal_seq exhausted for index '{index_name}'"
                    ));
                };
                let Some(expires_at) = now_millis.checked_add(*lease_duration_ms) else {
                    return ClusterResponse::Error(format!(
                        "manifest writer lease expiration overflowed for index '{index_name}'"
                    ));
                };
                updated.renewal_seq = next_renewal_seq;
                updated.leader_term = *leader_term;
                updated.lease_expires_at_millis = expires_at;
            }
            ManifestWriterMutation::Release { identity } => {
                if current.last_writer_epoch != identity.writer_epoch
                    || !current.phase.matches_identity(identity)
                {
                    return ClusterResponse::ManifestWriterAuthorityRejected {
                        reason: format!(
                            "manifest writer release for '{index_name}' does not match current authority"
                        ),
                    };
                }
                updated.revoke_to_unassigned();
            }
        }

        if let Err(reason) = updated.validate() {
            return ClusterResponse::Error(reason);
        }
        state
            .manifest_writer_authorities
            .insert(index_name.to_string(), updated.clone());
        state.version += 1;
        ClusterResponse::ManifestWriterAuthorityApplied { authority: updated }
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

            #[cfg(test)]
            if matches!(&entry.payload, EntryPayload::Normal(_))
                && let Some(gate) = &self.apply_gate
            {
                gate.before_apply().await;
            }

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
        if let Err(reason) = new_state.validate_manifest_writer_authorities() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                crate::consensus::UnsupportedRaftFormatError::new(
                    "Raft cluster-state snapshot",
                    format!("invalid manifest writer authority state: {reason}"),
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

    fn finish_empty_primary(sm: &ClusterStateMachine, index: &str, shard: u32) {
        use crate::cluster::state::{
            CanonicalHistory, PrimaryActivationFinish, WriteAuthorityIdentity,
        };
        let state = sm.state_handle().read().unwrap().clone();
        let metadata = &state.indices[index];
        let routing = &metadata.shard_routing[&shard];
        let ids = state.shard_allocation_ids(index, shard).unwrap();
        let activation_id = ids.primary_authority.activation_id.unwrap();
        assert_eq!(
            sm.apply_command(&ClusterCommand::FinishPrimaryActivation {
                index_name: index.into(),
                activation: PrimaryActivationFinish {
                    authority: WriteAuthorityIdentity {
                        index_uuid: metadata.uuid.to_string(),
                        shard_id: shard,
                        primary_node_id: routing.primary.clone(),
                        primary_allocation_id: ids.primary.unwrap(),
                        primary_term: routing.primary_term,
                        activation_id,
                    },
                    history: CanonicalHistory {
                        activation_id,
                        primary_term: routing.primary_term,
                        max_seq_no: None,
                        digest: [1; 32],
                    },
                    eligible_replicas: Vec::new(),
                },
            }),
            ClusterResponse::Ok
        );
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
    fn minimum_durable_copies_raft_rejects_invalid_create_and_capacity_decrease_atomically() {
        let sm = ClusterStateMachine::new("minimum-copies".into());
        let mut metadata = make_index("idx");
        metadata.settings.minimum_durable_copies = 2;
        assert!(matches!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone()
            }),
            ClusterResponse::Error(_)
        ));
        assert_eq!(sm.state_handle().read().unwrap().version, 0);
        assert!(sm.state_handle().read().unwrap().indices.is_empty());
        metadata.number_of_replicas = 1;
        metadata
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .unassigned_replicas = 1;
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone()
            }),
            ClusterResponse::Ok
        );
        let before = serde_json::to_value(&*sm.state_handle().read().unwrap()).unwrap();
        metadata.number_of_replicas = 0;
        metadata
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .unassigned_replicas = 0;
        assert!(matches!(
            sm.apply_command(&ClusterCommand::UpdateIndex { metadata }),
            ClusterResponse::Error(_)
        ));
        assert_eq!(
            serde_json::to_value(&*sm.state_handle().read().unwrap()).unwrap(),
            before
        );
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
    fn atomic_create_index_rejects_duplicate_without_mutation() {
        let machines = (0..3)
            .map(|_| ClusterStateMachine::new("atomic-create".into()))
            .collect::<Vec<_>>();
        let first = IndexMetadata::build_shard_routing(
            "atomic-index",
            2,
            1,
            &["node-1".into(), "node-2".into()],
        );
        let duplicate =
            IndexMetadata::build_shard_routing("atomic-index", 1, 0, &["node-3".into()]);
        assert_ne!(first.uuid, duplicate.uuid);
        let mut responses = Vec::new();
        for machine in &machines {
            assert_eq!(
                machine.apply_command_at(
                    &ClusterCommand::CreateIndex {
                        metadata: first.clone(),
                    },
                    10,
                ),
                ClusterResponse::Ok,
            );
            let before = machine.state_handle().read().unwrap().clone();
            let response = machine.apply_command_at(
                &ClusterCommand::CreateIndex {
                    metadata: duplicate.clone(),
                },
                20,
            );
            let after = machine.state_handle().read().unwrap().clone();
            println!(
                "ATOMIC_APPLY response={response:?} before_uuid={} after_uuid={} before_version={} after_version={}",
                before.indices["atomic-index"].uuid,
                after.indices["atomic-index"].uuid,
                before.version,
                after.version,
            );
            assert!(
                response.clone().into_result().is_err(),
                "duplicate CreateIndex must reject, not replace the first incarnation",
            );
            assert_eq!(after.version, before.version);
            assert_eq!(after.indices["atomic-index"].uuid, first.uuid);
            assert_eq!(
                serde_json::to_value(&after.indices).unwrap(),
                serde_json::to_value(&before.indices).unwrap(),
            );
            assert_eq!(after.shard_allocations, before.shard_allocations);
            responses.push(response);
        }
        assert!(responses.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn atomic_create_index_allows_recreate_after_applied_delete() {
        let machine = ClusterStateMachine::new("atomic-create".into());
        let first = make_index("atomic-recreate");
        let mut second = first.clone();
        second.uuid = crate::cluster::state::IndexUuid::new_random();
        assert_eq!(
            machine.apply_command_at(&ClusterCommand::CreateIndex { metadata: first }, 10),
            ClusterResponse::Ok,
        );
        assert_eq!(
            machine.apply_command_at(
                &ClusterCommand::DeleteIndex {
                    index_name: "atomic-recreate".into(),
                },
                20,
            ),
            ClusterResponse::Ok,
        );
        assert_eq!(
            machine.apply_command_at(
                &ClusterCommand::CreateIndex {
                    metadata: second.clone(),
                },
                30,
            ),
            ClusterResponse::Ok,
        );
        let state = machine.state_handle().read().unwrap().clone();
        assert_eq!(state.indices["atomic-recreate"].uuid, second.uuid);
        assert_eq!(state.primary_allocation_id("atomic-recreate", 0), Some(30));
        assert!(!state.primary_initialized("atomic-recreate", 0));
        assert_eq!(state.version, 3);
    }

    #[test]
    fn atomic_duplicate_create_rejects_same_uuid_and_malformed_candidate() {
        let machine = ClusterStateMachine::new("atomic-create".into());
        let first = make_index("atomic-existing");
        assert_eq!(
            machine.apply_command_at(
                &ClusterCommand::CreateIndex {
                    metadata: first.clone(),
                },
                10,
            ),
            ClusterResponse::Ok,
        );
        let before = machine.state_handle().read().unwrap().clone();
        let mut same_uuid = first.clone();
        same_uuid.settings.refresh_interval_ms = Some(999);
        let mut malformed = first;
        malformed.uuid = crate::cluster::state::IndexUuid::new_random();
        malformed.shard_routing.clear();
        for metadata in [same_uuid, malformed] {
            assert_eq!(
                machine.apply_command_at(&ClusterCommand::CreateIndex { metadata }, 20),
                ClusterResponse::IndexAlreadyExists {
                    index_name: "atomic-existing".into(),
                },
            );
            let after = machine.state_handle().read().unwrap().clone();
            assert_eq!(
                serde_json::to_value(&after).unwrap(),
                serde_json::to_value(&before).unwrap(),
            );
        }
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
        finish_empty_primary(&sm, "idx", 0);
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
        sm.state_handle()
            .write()
            .unwrap()
            .shard_allocations
            .get_mut("red-admission")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_authority
            .invalidate();
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
            routing.in_sync_replicas = Vec::new();
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
        finish_empty_primary(&sm, "promote", 0);
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::MarkReplicaInSync {
                    index_name: "promote".into(),
                    index_uuid: promote_uuid.clone(),
                    shard_id: 0,
                    replica: "node-2".into(),
                    allocation_id: 10,
                    primary: "node-1".into(),
                    primary_term: 2,
                },
                12,
            ),
            ClusterResponse::Ok
        );
        let previous_history = sm
            .state_handle()
            .read()
            .unwrap()
            .shard_allocation_ids("promote", 0)
            .unwrap()
            .primary_authority
            .history
            .clone();
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
            let authority = &state
                .shard_allocation_ids("promote", 0)
                .unwrap()
                .primary_authority;
            assert_eq!(
                authority.phase,
                crate::cluster::state::PrimaryAuthorityPhase::Inactive
            );
            assert_eq!(authority.activation_id, None);
            assert_eq!(authority.history, previous_history);
            assert!(authority.validate(3, true).is_ok());
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
    fn primary_unavailability_requires_fresh_canonical_finish() {
        use crate::cluster::state::{
            CanonicalHistory, PrimaryActivationFinish, PrimaryAuthorityPhase,
            WriteAuthorityIdentity,
        };

        let sm = ClusterStateMachine::new("test".into());
        let metadata = make_index("unavailable");
        let index_uuid = metadata.uuid.to_string();
        let finish = |primary_term, activation_id| ClusterCommand::FinishPrimaryActivation {
            index_name: "unavailable".into(),
            activation: PrimaryActivationFinish {
                authority: WriteAuthorityIdentity {
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary_node_id: "node-1".into(),
                    primary_allocation_id: 10,
                    primary_term,
                    activation_id,
                },
                history: CanonicalHistory {
                    activation_id,
                    primary_term,
                    max_seq_no: None,
                    digest: [1; 32],
                },
                eligible_replicas: Vec::new(),
            },
        };
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
        assert_eq!(sm.apply_command_at(&finish(2, 11), 12), ClusterResponse::Ok);
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
            sm.apply_command_at(&finish(2, 11), 15),
            ClusterResponse::WriteAuthorityRejected { .. }
        ));
        assert_eq!(
            sm.state_handle().read().unwrap().version,
            unavailable_version
        );
        assert_eq!(
            sm.apply_command_at(
                &ClusterCommand::ActivatePrimary {
                    index_name: "unavailable".into(),
                    index_uuid: index_uuid.clone(),
                    shard_id: 0,
                    primary: "node-1".into(),
                    allocation_id: 10,
                    expected_term: 2,
                },
                20,
            ),
            ClusterResponse::Ok
        );
        {
            let handle = sm.state_handle();
            let state = handle.read().unwrap();
            assert!(state.primary_unavailable("unavailable", 0));
            assert_eq!(
                state
                    .shard_allocation_ids("unavailable", 0)
                    .unwrap()
                    .primary_authority
                    .phase,
                PrimaryAuthorityPhase::Promoting
            );
            assert_eq!(state.version, unavailable_version + 1);
        }
        assert!(matches!(
            sm.apply_command_at(&finish(2, 11), 21),
            ClusterResponse::WriteAuthorityRejected { .. }
        ));
        assert_eq!(sm.apply_command_at(&finish(3, 20), 22), ClusterResponse::Ok);
        let state = sm.state_handle();
        let state = state.read().unwrap();
        assert!(!state.primary_unavailable("unavailable", 0));
        assert_eq!(state.version, unavailable_version + 2);
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
            finish_empty_primary(&sm, "idx", shard_id);
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
                .shard_allocations
                .get_mut("idx")
                .unwrap()
                .get_mut(&0)
                .unwrap()
                .primary_authority
                .invalidate();
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

    #[test]
    fn manifest_writer_authority_lifecycle_acquire_activate_renew_transfer_expire_and_reject_stale()
    {
        use crate::cluster::state::{
            IndexEngine, IndexSettings, IndexUuid, ManifestLeaseAcquireReason,
            ManifestWriterIdentity, ManifestWriterMutation, ManifestWriterPhase,
        };

        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::AddNode {
            node: make_node("node-1"),
        });
        sm.apply_command(&ClusterCommand::AddNode {
            node: make_node("node-2"),
        });

        let remote_meta = IndexMetadata {
            name: "events".into(),
            uuid: IndexUuid::new("events-uuid"),
            number_of_shards: 0,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings {
                engine: IndexEngine::RemoteStore,
                ..Default::default()
            },
        };
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: remote_meta,
            }),
            ClusterResponse::Ok
        );

        let initial_expected = sm
            .state_handle()
            .read()
            .unwrap()
            .manifest_writer_authority("events")
            .unwrap()
            .expected_snapshot();
        assert_eq!(initial_expected.last_writer_epoch, 0);
        assert!(initial_expected.phase.is_unassigned());

        // 1. Acquire on node-1 -> Fencing(epoch=1).
        let version_before = sm.state_handle().read().unwrap().version;
        let acquire_1 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: initial_expected.clone(),
                candidate_node: "node-1".into(),
                reason: ManifestLeaseAcquireReason::Unassigned,
                now_millis: 1_000,
                lease_duration_ms: 10_000,
                leader_term: 1,
            },
        });
        let fencing_1 = match acquire_1 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected ManifestWriterAuthorityApplied, got {other:?}"),
        };
        assert_eq!(fencing_1.last_writer_epoch, 1);
        assert_eq!(
            fencing_1.phase,
            ManifestWriterPhase::Fencing {
                holder_node: "node-1".into(),
                writer_epoch: 1,
                lease_id: fencing_1.last_lease_id,
            }
        );
        assert_eq!(
            sm.state_handle().read().unwrap().version,
            version_before + 1
        );

        // Duplicate acquire with stale initial_expected must be rejected without bumping version.
        let version_after_acquire = sm.state_handle().read().unwrap().version;
        let dup_acquire = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: initial_expected,
                candidate_node: "node-2".into(),
                reason: ManifestLeaseAcquireReason::Unassigned,
                now_millis: 1_100,
                lease_duration_ms: 10_000,
                leader_term: 1,
            },
        });
        assert!(matches!(
            dup_acquire,
            ClusterResponse::ManifestWriterAuthorityRejected { .. }
        ));
        assert_eq!(
            sm.state_handle().read().unwrap().version,
            version_after_acquire
        );

        // 2. Activate epoch 1 on node-1 after pointer fence.
        let identity_1 = fencing_1.current_identity().unwrap();
        let activate_1 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Activate {
                identity: identity_1.clone(),
                fenced_pointer_revision: 1,
                fenced_pointer_token: "tok-rev-1".into(),
                now_millis: 2_000,
                lease_duration_ms: 10_000,
                leader_term: 1,
            },
        });
        let active_1 = match activate_1 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected active authority, got {other:?}"),
        };
        assert_eq!(active_1.lease_expires_at_millis, 12_000);
        assert!(active_1.phase.is_active());

        // 3. Renew epoch 1 on node-1 preserves epoch=1 and lease_id while extending deadline and bumping renewal_seq.
        let renew_1 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Renew {
                identity: identity_1.clone(),
                expected_fenced_pointer_revision: 1,
                expected_fenced_pointer_token: "tok-rev-1".into(),
                now_millis: 8_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        let renewed_1 = match renew_1 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected renewed authority, got {other:?}"),
        };
        assert_eq!(renewed_1.last_writer_epoch, 1);
        assert_eq!(renewed_1.renewal_seq, 1);
        assert_eq!(renewed_1.leader_term, 2);
        assert_eq!(renewed_1.lease_expires_at_millis, 18_000);

        // Premature Expired replacement before 18_000 must be rejected.
        let premature_expire = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: renewed_1.expected_snapshot(),
                candidate_node: "node-2".into(),
                reason: ManifestLeaseAcquireReason::Expired,
                now_millis: 17_999,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        assert!(matches!(
            premature_expire,
            ClusterResponse::ManifestWriterAuthorityRejected { .. }
        ));

        // In-place Renew at or after expiry (18_000) must be rejected (requires re-fencing a new epoch).
        let expired_renew = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Renew {
                identity: identity_1.clone(),
                expected_fenced_pointer_revision: 1,
                expected_fenced_pointer_token: "tok-rev-1".into(),
                now_millis: 18_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        assert!(matches!(
            expired_renew,
            ClusterResponse::ManifestWriterAuthorityRejected { .. }
        ));

        // 4. Expired replacement at 18_000 (even by the same node-1!) allocates epoch 2 and enters Fencing.
        let acquire_2 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: renewed_1.expected_snapshot(),
                candidate_node: "node-1".into(),
                reason: ManifestLeaseAcquireReason::Expired,
                now_millis: 18_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        let fencing_2 = match acquire_2 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected fencing epoch 2, got {other:?}"),
        };
        assert_eq!(fencing_2.last_writer_epoch, 2);
        assert!(fencing_2.last_lease_id > renewed_1.last_lease_id);

        // Stale epoch 1 identity is now rejected across Activate, Renew, and Release.
        for stale_mutation in [
            ManifestWriterMutation::Activate {
                identity: identity_1.clone(),
                fenced_pointer_revision: 2,
                fenced_pointer_token: "tok-rev-2".into(),
                now_millis: 19_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
            ManifestWriterMutation::Renew {
                identity: identity_1.clone(),
                expected_fenced_pointer_revision: 1,
                expected_fenced_pointer_token: "tok-rev-1".into(),
                now_millis: 19_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
            ManifestWriterMutation::Release {
                identity: identity_1,
            },
        ] {
            let v_before = sm.state_handle().read().unwrap().version;
            let res = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
                index_name: "events".into(),
                mutation: stale_mutation,
            });
            assert!(matches!(
                res,
                ClusterResponse::ManifestWriterAuthorityRejected { .. }
            ));
            assert_eq!(sm.state_handle().read().unwrap().version, v_before);
        }

        // 5. Transfer from node-1 (epoch 2) to node-2 allocates epoch 3.
        let transfer_3 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: fencing_2.expected_snapshot(),
                candidate_node: "node-2".into(),
                reason: ManifestLeaseAcquireReason::Transfer {
                    from_holder: "node-1".into(),
                },
                now_millis: 20_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        let fencing_3 = match transfer_3 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected transfer to epoch 3, got {other:?}"),
        };
        assert_eq!(fencing_3.last_writer_epoch, 3);
        assert_eq!(fencing_3.phase.holder_node(), Some("node-2"));

        // 6. Removing node-2 revokes the lease to Unassigned while preserving last_writer_epoch=3;
        // re-acquiring on node-1 allocates epoch 4.
        assert_eq!(
            sm.apply_command(&ClusterCommand::RemoveNode {
                node_id: "node-2".into(),
            }),
            ClusterResponse::Ok
        );
        let after_remove = sm
            .state_handle()
            .read()
            .unwrap()
            .manifest_writer_authority("events")
            .unwrap()
            .clone();
        assert!(after_remove.phase.is_unassigned());
        assert_eq!(after_remove.last_writer_epoch, 3);

        let acquire_4 = sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
            index_name: "events".into(),
            mutation: ManifestWriterMutation::Acquire {
                expected: after_remove.expected_snapshot(),
                candidate_node: "node-1".into(),
                reason: ManifestLeaseAcquireReason::Unassigned,
                now_millis: 21_000,
                lease_duration_ms: 10_000,
                leader_term: 2,
            },
        });
        let fencing_4 = match acquire_4 {
            ClusterResponse::ManifestWriterAuthorityApplied { authority } => authority,
            other => panic!("expected epoch 4, got {other:?}"),
        };
        assert_eq!(fencing_4.last_writer_epoch, 4);

        // Stale writer with forged higher epoch is rejected.
        let forged = ManifestWriterIdentity {
            index_uuid: "events-uuid".into(),
            holder_node: "node-1".into(),
            writer_epoch: 99,
            lease_id: fencing_4.last_lease_id,
        };
        assert!(matches!(
            sm.apply_command(&ClusterCommand::MutateManifestWriterAuthority {
                index_name: "events".into(),
                mutation: ManifestWriterMutation::Release { identity: forged },
            }),
            ClusterResponse::ManifestWriterAuthorityRejected { .. }
        ));

        // DeleteIndex removes the manifest writer authority record.
        assert_eq!(
            sm.apply_command(&ClusterCommand::DeleteIndex {
                index_name: "events".into(),
            }),
            ClusterResponse::Ok
        );
        assert!(
            sm.state_handle()
                .read()
                .unwrap()
                .manifest_writer_authority("events")
                .is_none()
        );
    }
}
