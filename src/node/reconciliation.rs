//! Shard reconciliation: open local assigned shards, orphan cleanup, UUID directory management.

use crate::shard::ShardManager;
use std::sync::Arc;

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
                .join(format!("shard_{}", shard_id));
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
                .join(format!("shard_{}", shard_id));
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
) {
    if let Err(e) = tokio::task::spawn_blocking(move || {
        open_local_assigned_shards(
            &state,
            &local_node_id,
            shard_manager.as_ref(),
            guarded_missing_startup_shards.as_ref(),
        )
    })
    .await
    {
        tracing::warn!(
            "Lifecycle shard-open reconciliation task failed to join: {}",
            e
        );
    }
}

pub(super) fn open_local_assigned_shards(
    state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
    shard_manager: &ShardManager,
    guarded_missing_startup_shards: &std::sync::Mutex<
        std::collections::HashSet<(String, u32, String)>,
    >,
) {
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
            if !assigned_here || shard_manager.get_shard(index_name, *shard_id).is_some() {
                continue;
            }

            let shard_dir = shard_manager
                .data_dir()
                .join(&metadata.uuid)
                .join(format!("shard_{}", shard_id));
            if !shard_dir.exists()
                && guard_set.contains(&(index_name.clone(), *shard_id, metadata.uuid.to_string()))
            {
                tracing::warn!(
                    "Skipping lifecycle reopen for {}/{} because {:?} is missing on a recovered node; refusing to create a fresh shard directory for a startup assignment",
                    index_name,
                    shard_id,
                    shard_dir
                );
                continue;
            }

            if let Err(e) = shard_manager.open_shard_with_settings(
                index_name,
                *shard_id,
                &metadata.mappings,
                &metadata.settings,
                &metadata.uuid,
            ) {
                tracing::warn!(
                    "Failed to reopen local shard {}/{} during lifecycle reconciliation: {}",
                    index_name,
                    shard_id,
                    e
                );
            }
        }
    }
}
