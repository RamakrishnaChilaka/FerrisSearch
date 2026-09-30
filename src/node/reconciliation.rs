//! Shard reconciliation: open local assigned shards, orphan cleanup, UUID directory management.

use crate::shard::ShardManager;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct ShardCopyFailure {
    pub index_name: String,
    pub index_uuid: String,
    pub shard_id: u32,
    pub node_id: String,
    pub allocation_id: u64,
    pub primary_term: u64,
    pub promote_only: bool,
    pub quarantine: bool,
    pub reason: String,
}

pub(super) fn snapshot_uuid_dirs(data_dir: &std::path::Path) -> std::collections::HashSet<String> {
    let mut dirs = std::collections::HashSet::new();
    match std::fs::read_dir(data_dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                if entry.path().is_dir()
                    && let Some(name) = entry.file_name().to_str()
                {
                    dirs.insert(name.to_string());
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to snapshot pre-existing UUID directories under {:?}: {}",
                data_dir,
                e
            );
        }
    }
    dirs
}

pub(super) async fn cleanup_orphaned_data_if_authoritative_blocking(
    state: Option<crate::cluster::state::ClusterState>,
    local_node_id: String,
    shard_manager: Arc<ShardManager>,
    allow_empty_indices: bool,
    pre_existing_uuid_dirs: std::collections::HashSet<String>,
) -> bool {
    let Some(state) = state else {
        return false;
    };

    if state.indices.is_empty() && !allow_empty_indices {
        tracing::info!(
            "Skipping orphaned data cleanup until authoritative cluster index UUIDs are available"
        );
        return false;
    }

    for (index_name, metadata) in &state.indices {
        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == local_node_id
                || routing
                    .replicas
                    .iter()
                    .any(|node_id| node_id == &local_node_id);
            if !assigned_here {
                continue;
            }

            // Check that the UUID directory existed on disk BEFORE we
            // opened any shards in this startup.  A directory that was just
            // created by `open_local_assigned_shards` is empty and must not
            // be treated as proof that the authoritative data is present.
            if !pre_existing_uuid_dirs.contains(metadata.uuid.as_str()) {
                tracing::warn!(
                    "Skipping orphaned data cleanup because UUID directory for {}/{} was not present before shard opening — it was freshly created and deleting unknown UUID directories could discard live data",
                    index_name,
                    shard_id
                );
                return false;
            }

            let shard_dir = shard_manager
                .data_dir()
                .join(&metadata.uuid)
                .join(format!("shard_{shard_id}"));
            if !shard_dir.exists() {
                tracing::warn!(
                    "Skipping orphaned data cleanup because locally assigned shard {}/{} expects data at {:?}, and deleting unknown UUID directories could discard live data",
                    index_name,
                    shard_id,
                    shard_dir
                );
                return false;
            }
        }
    }

    let known_uuids: std::collections::HashSet<String> = state
        .indices
        .values()
        .map(|metadata| metadata.uuid.to_string())
        .collect();
    if let Err(e) = shard_manager
        .cleanup_orphaned_data_blocking(known_uuids)
        .await
    {
        tracing::warn!("Failed to clean orphaned shard data: {}", e);
    }
    true
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn cleanup_orphaned_data_if_authoritative(
    state: Option<&crate::cluster::state::ClusterState>,
    local_node_id: &str,
    shard_manager: &ShardManager,
    allow_empty_indices: bool,
    pre_existing_uuid_dirs: &std::collections::HashSet<String>,
) -> bool {
    let Some(state) = state else {
        return false;
    };

    if state.indices.is_empty() && !allow_empty_indices {
        tracing::info!(
            "Skipping orphaned data cleanup until authoritative cluster index UUIDs are available"
        );
        return false;
    }

    for (index_name, metadata) in &state.indices {
        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == local_node_id
                || routing
                    .replicas
                    .iter()
                    .any(|node_id| node_id == local_node_id);
            if !assigned_here {
                continue;
            }

            if !pre_existing_uuid_dirs.contains(metadata.uuid.as_str()) {
                tracing::warn!(
                    "Skipping orphaned data cleanup because UUID directory for {}/{} was not present before shard opening — it was freshly created and deleting unknown UUID directories could discard live data",
                    index_name,
                    shard_id
                );
                return false;
            }

            let shard_dir = shard_manager
                .data_dir()
                .join(&metadata.uuid)
                .join(format!("shard_{shard_id}"));
            if !shard_dir.exists() {
                tracing::warn!(
                    "Skipping orphaned data cleanup because locally assigned shard {}/{} expects data at {:?}, and deleting unknown UUID directories could discard live data",
                    index_name,
                    shard_id,
                    shard_dir
                );
                return false;
            }
        }
    }

    let known_uuids: std::collections::HashSet<String> =
        state.indices.values().map(|m| m.uuid.to_string()).collect();
    shard_manager.cleanup_orphaned_data(&known_uuids);
    true
}

pub(super) fn should_retry_cluster_join(
    state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
) -> bool {
    !state.nodes.contains_key(local_node_id)
}

pub(super) type GuardedStartupShards =
    std::sync::Arc<std::sync::Mutex<std::collections::HashSet<(String, u32, String)>>>;

pub(super) fn build_guarded_startup_shards(
    recovered_state: Option<&crate::cluster::state::ClusterState>,
    local_node_id: &str,
) -> GuardedStartupShards {
    let set = recovered_state
        .map(|state| collect_guarded_startup_shards(state, local_node_id))
        .unwrap_or_default();
    std::sync::Arc::new(std::sync::Mutex::new(set))
}

pub(super) fn collect_guarded_startup_shards(
    state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
) -> std::collections::HashSet<(String, u32, String)> {
    let mut guarded = std::collections::HashSet::new();
    for (index_name, metadata) in &state.indices {
        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == local_node_id
                || routing
                    .replicas
                    .iter()
                    .any(|node_id| node_id == local_node_id);
            if assigned_here {
                guarded.insert((index_name.clone(), *shard_id, metadata.uuid.to_string()));
            }
        }
    }
    guarded
}

pub(super) async fn open_local_assigned_shards_blocking(
    state: crate::cluster::state::ClusterState,
    local_node_id: String,
    shard_manager: Arc<ShardManager>,
    guarded_missing_startup_shards: GuardedStartupShards,
) -> Vec<ShardCopyFailure> {
    match tokio::task::spawn_blocking(move || {
        open_local_assigned_shards(
            &state,
            &local_node_id,
            shard_manager.as_ref(),
            guarded_missing_startup_shards.as_ref(),
        )
    })
    .await
    {
        Ok(failures) => failures,
        Err(error) => {
            tracing::warn!(
                "Lifecycle shard-open reconciliation task failed to join: {}",
                error
            );
            Vec::new()
        }
    }
}

pub(super) fn open_local_assigned_shards(
    state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
    shard_manager: &ShardManager,
    guarded_missing_startup_shards: &std::sync::Mutex<
        std::collections::HashSet<(String, u32, String)>,
    >,
) -> Vec<ShardCopyFailure> {
    let mut failures = Vec::new();
    let guard_set = guarded_missing_startup_shards
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    for (index_name, metadata) in &state.indices {
        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == local_node_id
                || routing
                    .replicas
                    .iter()
                    .any(|node_id| node_id == local_node_id);
            if !assigned_here {
                if shard_manager.get_shard(index_name, *shard_id).is_some() {
                    shard_manager.quarantine_shard_copy(index_name, *shard_id);
                }
                continue;
            }
            let authoritative_here =
                routing.primary == local_node_id || routing.is_replica_in_sync(local_node_id);
            let Some(allocation_id) =
                state.shard_allocation_id(index_name, *shard_id, local_node_id)
            else {
                if authoritative_here {
                    shard_manager.quarantine_shard_copy(index_name, *shard_id);
                    let red_primary = routing.primary == local_node_id
                        && state.primary_initialized(index_name, *shard_id)
                        && state.primary_allocation_id(index_name, *shard_id).is_none();
                    if red_primary {
                        tracing::debug!(
                            "Skipping red primary shard {}/{} because no surviving primary allocation is assigned",
                            index_name,
                            shard_id
                        );
                    } else {
                        tracing::error!(
                            "Refusing to open authoritative shard {}/{} because allocation identity metadata is missing; pre-1.0 copies must be recreated or reindexed",
                            index_name,
                            shard_id
                        );
                    }
                }
                continue;
            };
            if !authoritative_here {
                match shard_manager.restore_peer_recovery_awaiting_membership(
                    index_name,
                    *shard_id,
                    &metadata.mappings,
                    &metadata.settings,
                    metadata.uuid.as_str(),
                    crate::shard::AssignedShardOpen {
                        allocation_id,
                        primary_term: routing.primary_term,
                        allow_empty_creation: false,
                    },
                ) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(
                            "Unable to restore finalized peer recovery target for {}/{} allocation {}: {}",
                            index_name,
                            shard_id,
                            allocation_id,
                            error
                        );
                        if state.primary_initialized(index_name, *shard_id)
                            && ShardManager::should_report_copy_failure(&error)
                        {
                            failures.push(ShardCopyFailure {
                                index_name: index_name.clone(),
                                index_uuid: metadata.uuid.to_string(),
                                shard_id: *shard_id,
                                node_id: local_node_id.to_string(),
                                allocation_id,
                                primary_term: routing.primary_term,
                                promote_only: false,
                                quarantine: ShardManager::should_quarantine_copy_failure(&error),
                                reason: error.to_string(),
                            });
                        }
                        continue;
                    }
                }
                if state.primary_initialized(index_name, *shard_id) {
                    match shard_manager.failed_peer_recovery_install_matches(
                        index_name,
                        *shard_id,
                        metadata.uuid.as_str(),
                        allocation_id,
                    ) {
                        Ok(true) => failures.push(ShardCopyFailure {
                            index_name: index_name.clone(),
                            index_uuid: metadata.uuid.to_string(),
                            shard_id: *shard_id,
                            node_id: local_node_id.to_string(),
                            allocation_id,
                            primary_term: routing.primary_term,
                            promote_only: false,
                            quarantine: true,
                            reason: "peer recovery install marker remains after target failure"
                                .to_string(),
                        }),
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(
                                "Unable to classify peer recovery install marker for {}/{} allocation {}: {}",
                                index_name,
                                shard_id,
                                allocation_id,
                                error
                            );
                            if ShardManager::should_report_copy_failure(&error) {
                                failures.push(ShardCopyFailure {
                                    index_name: index_name.clone(),
                                    index_uuid: metadata.uuid.to_string(),
                                    shard_id: *shard_id,
                                    node_id: local_node_id.to_string(),
                                    allocation_id,
                                    primary_term: routing.primary_term,
                                    promote_only: false,
                                    quarantine: ShardManager::should_quarantine_copy_failure(
                                        &error,
                                    ),
                                    reason: error.to_string(),
                                });
                            }
                        }
                    }
                }
                continue;
            }
            if shard_manager.get_shard(index_name, *shard_id).is_some() {
                if let Err(error) = shard_manager.validate_open_copy_identity(
                    index_name,
                    *shard_id,
                    metadata.uuid.as_str(),
                    allocation_id,
                ) {
                    if ShardManager::should_report_copy_failure(&error) {
                        failures.push(ShardCopyFailure {
                            index_name: index_name.clone(),
                            index_uuid: metadata.uuid.to_string(),
                            shard_id: *shard_id,
                            node_id: local_node_id.to_string(),
                            allocation_id,
                            primary_term: routing.primary_term,
                            promote_only: routing.primary == local_node_id,
                            quarantine: ShardManager::should_quarantine_copy_failure(&error),
                            reason: error.to_string(),
                        });
                    } else {
                        tracing::warn!(
                            "Retryable local shard identity validation failed for {}/{}: {}",
                            index_name,
                            shard_id,
                            error
                        );
                    }
                }
                continue;
            }

            let shard_dir = shard_manager
                .data_dir()
                .join(&metadata.uuid)
                .join(format!("shard_{shard_id}"));
            let allow_empty_creation =
                state.may_create_initial_empty_copy(index_name, *shard_id, local_node_id);
            let shard_dir_missing = shard_dir.try_exists().is_ok_and(|exists| !exists);
            if shard_dir_missing
                && guard_set.contains(&(index_name.clone(), *shard_id, metadata.uuid.to_string()))
                && !allow_empty_creation
            {
                tracing::warn!(
                    "Skipping lifecycle reopen for {}/{} because {:?} is missing on a recovered node; refusing to create a fresh shard directory for a startup assignment",
                    index_name,
                    shard_id,
                    shard_dir
                );
                if state.primary_initialized(index_name, *shard_id) {
                    failures.push(ShardCopyFailure {
                        index_name: index_name.clone(),
                        index_uuid: metadata.uuid.to_string(),
                        shard_id: *shard_id,
                        node_id: local_node_id.to_string(),
                        allocation_id,
                        primary_term: routing.primary_term,
                        promote_only: routing.primary == local_node_id,
                        quarantine: true,
                        reason: format!("expected shard directory {shard_dir:?} is missing"),
                    });
                }
                continue;
            }

            let assignment = crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: routing.primary_term,
                allow_empty_creation,
            };
            let open_result = if routing.primary == local_node_id {
                shard_manager.open_primary_assigned_shard_with_settings(
                    index_name,
                    *shard_id,
                    &metadata.mappings,
                    &metadata.settings,
                    &metadata.uuid,
                    assignment,
                )
            } else {
                shard_manager.open_assigned_shard_with_settings(
                    index_name,
                    *shard_id,
                    &metadata.mappings,
                    &metadata.settings,
                    &metadata.uuid,
                    assignment,
                )
            };
            if let Err(error) = open_result {
                tracing::warn!(
                    "Failed to reopen local shard {}/{} during lifecycle reconciliation: {}",
                    index_name,
                    shard_id,
                    error
                );
                if ShardManager::should_report_copy_failure(&error) {
                    failures.push(ShardCopyFailure {
                        index_name: index_name.clone(),
                        index_uuid: metadata.uuid.to_string(),
                        shard_id: *shard_id,
                        node_id: local_node_id.to_string(),
                        allocation_id,
                        primary_term: routing.primary_term,
                        promote_only: routing.primary == local_node_id,
                        quarantine: ShardManager::should_quarantine_copy_failure(&error),
                        reason: error.to_string(),
                    });
                }
            }
        }
    }
    failures
}
