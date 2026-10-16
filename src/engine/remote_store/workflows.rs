use super::*;
use crate::cluster::state::{
    HotDeltaSealPolicy, IndexEngine, IndexMigrationPhase, IndexMigrationRecord, ShardRoutingEntry,
};
use crate::engine::{DeletedDocumentVersionTuple, LiveDocumentVersionTuple, SealWatermarkState};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CLUSTER_BACKUP_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardBackupPayload {
    pub format_version: u32,
    pub index_uuid: String,
    pub shard_id: u32,
    pub live_docs: Vec<LiveDocumentVersionTuple>,
    pub tombstones: Vec<DeletedDocumentVersionTuple>,
    pub seal_watermark_state: SealWatermarkState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardBackupDescriptor {
    pub shard_id: u32,
    pub payload_path: String,
    pub payload_checksum: String,
    pub live_doc_count: usize,
    pub tombstone_count: usize,
    pub sealed_through_seq_no: Option<u64>,
    pub retired_through_seq_no: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBackupEntry {
    pub metadata: IndexMetadata,
    pub lifecycle_mode: String,
    pub schema_hash: String,
    pub remote_manifest_path: Option<String>,
    pub remote_manifest_checksum: Option<String>,
    pub remote_manifest_generation: Option<u64>,
    pub split_checksums: BTreeMap<String, String>,
    pub shards: BTreeMap<u32, ShardBackupDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterBackupManifest {
    pub format_version: u32,
    pub snapshot_id: String,
    pub cluster_name: String,
    pub cluster_state_version: u64,
    pub created_at: String,
    pub created_at_epoch_ms: u64,
    pub duration_ms: u64,
    pub indices: BTreeMap<String, IndexBackupEntry>,
    pub integrity_sha256: String,
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), (StatusCode, Json<Value>)> {
    let trimmed = snapshot_id.trim();
    if trimmed.is_empty() || trimmed.len() > 120 {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "snapshot_id must be between 1 and 120 characters",
        ));
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!("snapshot_id [{trimmed}] contains invalid characters"),
        ));
    }
    Ok(())
}

fn snapshot_manifest_key(snapshot_id: &str) -> String {
    format!("_snapshots/{snapshot_id}/manifest.json")
}

fn snapshot_index_manifest_key(snapshot_id: &str, index_uuid: &str) -> String {
    format!("_snapshots/{snapshot_id}/indices/{index_uuid}/manifest.json")
}

fn snapshot_shard_payload_key(snapshot_id: &str, index_uuid: &str, shard_id: u32) -> String {
    format!("_snapshots/{snapshot_id}/indices/{index_uuid}/shards/shard_{shard_id}.json")
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn compute_backup_integrity(manifest: &ClusterBackupManifest) -> Result<String, anyhow::Error> {
    let payload = serde_json::json!({
        "format_version": manifest.format_version,
        "snapshot_id": manifest.snapshot_id,
        "cluster_name": manifest.cluster_name,
        "cluster_state_version": manifest.cluster_state_version,
        "created_at_epoch_ms": manifest.created_at_epoch_ms,
        "indices": manifest.indices,
    });
    let raw = crate::engine::ClientOperationIntent::fingerprint_json(&payload)?;
    Ok(if raw.starts_with("sha256:") {
        raw
    } else {
        format!("sha256:{raw}")
    })
}

async fn commit_hot_delta_policy_update(
    state: &AppState,
    index_name: &str,
    expected_uuid: &str,
    expected_migration_id: Option<&str>,
    hot_delta: Option<HotDeltaSealPolicy>,
) -> Result<(), (StatusCode, Json<Value>)> {
    let mut index_obj = serde_json::Map::new();
    index_obj.insert(
        "_expected_uuid".to_string(),
        Value::String(expected_uuid.to_string()),
    );
    if let Some(mig_id) = expected_migration_id {
        index_obj.insert(
            "_expected_migration_id".to_string(),
            Value::String(mig_id.to_string()),
        );
    }
    index_obj.insert(
        "_internal_lifecycle_hot_delta".to_string(),
        serde_json::to_value(hot_delta).unwrap_or(Value::Null),
    );
    let settings_body = Value::Object(
        [("index".to_string(), Value::Object(index_obj))]
            .into_iter()
            .collect(),
    );
    if let Some(master) = match crate::api::resolve_leader_or_master(state, "lifecycle update") {
        Ok(m) => m,
        Err(e) => return Err(e),
    } {
        state
            .transport_client
            .forward_update_settings(&master, index_name, &settings_body)
            .await
            .map_err(|e| {
                crate::api::error_response(
                    StatusCode::CONFLICT,
                    "migration_conflict_exception",
                    format!("failed to forward lifecycle settings update: {e:#}"),
                )
            })?;
    } else {
        let mut metadata = state
            .cluster_manager
            .get_state()
            .indices
            .get(index_name)
            .cloned()
            .ok_or_else(|| {
                crate::api::error_response(
                    StatusCode::NOT_FOUND,
                    "index_not_found_exception",
                    format!("no such index [{index_name}]"),
                )
            })?;
        metadata
            .apply_settings_update(&settings_body)
            .map_err(|e| {
                crate::api::error_response(StatusCode::CONFLICT, "migration_conflict_exception", e)
            })?;
        crate::api::raft_write(
            state,
            crate::consensus::types::ClusterCommand::UpdateIndex { metadata },
        )
        .await?;
    }
    let target_ver = state.cluster_manager.version();
    let _ = state.cluster_manager.wait_for_version(target_ver).await;
    Ok(())
}

/// POST /_snapshot/{snapshot_id} — Capture a cluster-wide or filtered index
/// backup manifest including Raft metadata, remote_store manifest lineages and
/// split checksums, and local shard document/tombstone/watermark state (`FS-042`).
pub(crate) async fn create_cluster_snapshot(
    state: &AppState,
    snapshot_id: &str,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    validate_snapshot_id(snapshot_id)?;
    let started = std::time::Instant::now();
    let now_ms = now_epoch_ms();

    let manifest_key = snapshot_manifest_key(snapshot_id);
    let manifest_obj_path = state
        .storage_manager
        .object_path(&manifest_key)
        .map_err(|e| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "snapshot_exception",
                format!("invalid snapshot path: {e:#}"),
            )
        })?;
    if state
        .storage_manager
        .get_bytes(&manifest_obj_path)
        .await
        .is_ok()
    {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "snapshot_already_exists_exception",
            format!("snapshot [{snapshot_id}] already exists"),
        ));
    }

    let requested_indices: Option<HashSet<String>> = body
        .as_ref()
        .and_then(|v| v.get("indices"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        });

    let cluster_state = state.cluster_manager.get_state();
    if let Some(ref req_set) = requested_indices {
        for name in req_set {
            if !cluster_state.indices.contains_key(name) {
                return Err(crate::api::error_response(
                    StatusCode::NOT_FOUND,
                    "index_not_found_exception",
                    format!("cannot snapshot unknown index [{name}]"),
                ));
            }
        }
    }

    let mut index_entries = BTreeMap::new();
    let mut total_splits = 0usize;
    let mut total_shards = 0usize;
    let mut total_live_docs = 0usize;
    let mut total_tombstones = 0usize;

    for (index_name, metadata) in &cluster_state.indices {
        if crate::security::is_protected_system_index(index_name) {
            continue;
        }
        if let Some(ref req_set) = requested_indices
            && !req_set.contains(index_name)
        {
            continue;
        }

        let index_uuid = metadata.uuid.to_string();
        let schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
        let mut remote_manifest_path = None;
        let mut remote_manifest_checksum = None;
        let mut remote_manifest_generation = None;
        let mut split_checksums = BTreeMap::new();

        if metadata.uses_remote_store()
            && let Some(manifest) = state
                .storage_manager
                .load_current_manifest(&index_uuid, Some(&schema_hash))
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_exception",
                        format!("failed to load manifest for index [{index_name}]: {e:#}"),
                    )
                })?
        {
            state
                .storage_manager
                .verify_manifest_lineage_chain(&index_uuid, &manifest, Some(&schema_hash))
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::CONFLICT,
                        "snapshot_corruption_exception",
                        format!(
                            "manifest lineage verification failed for index [{index_name}]: {e:#}"
                        ),
                    )
                })?;
            let mut pinned_split_ids = HashSet::new();
            for split in manifest.published_splits() {
                let actual_hash = state
                    .storage_manager
                    .hash_split_bundle(&index_uuid, split)
                    .await
                    .map_err(|e| {
                        crate::api::error_response(
                            StatusCode::CONFLICT,
                            "snapshot_corruption_exception",
                            format!(
                                "split [{}] bundle missing or unreadable during snapshot of [{index_name}]: {e:#}",
                                split.split_id
                            ),
                        )
                    })?;
                if actual_hash != split.checksum {
                    return Err(crate::api::error_response(
                        StatusCode::CONFLICT,
                        "snapshot_corruption_exception",
                        format!(
                            "split [{}] checksum mismatch during snapshot of [{index_name}]: expected {}, got {}",
                            split.split_id, split.checksum, actual_hash
                        ),
                    ));
                }
                split_checksums.insert(split.split_id.clone(), split.checksum.clone());
                pinned_split_ids.insert(split.split_id.clone());
                total_splits += 1;
            }

            let snap_manifest_key = snapshot_index_manifest_key(snapshot_id, &index_uuid);
            let snap_manifest_obj = state
                .storage_manager
                .object_path(&snap_manifest_key)
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_exception",
                        e,
                    )
                })?;
            let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "snapshot_exception",
                    e,
                )
            })?;
            let manifest_csum = sha256_prefixed(&manifest_bytes);
            state
                .storage_manager
                .create_immutable(&snap_manifest_obj, manifest_bytes)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_exception",
                        format!("failed to write snapshot manifest copy: {e:#}"),
                    )
                })?;

            state.storage_manager.register_named_snapshot_lease(
                crate::storage::SnapshotRetentionLease {
                    snapshot_id: format!("backup-{snapshot_id}-{index_uuid}"),
                    index_uuid: index_uuid.clone(),
                    manifest_generation: manifest.generation,
                    pinned_split_ids,
                    acquired_at_epoch_ms: now_ms,
                    expires_at_epoch_ms: now_ms.saturating_add(86_400_000 * 30),
                },
            );

            remote_manifest_path = Some(snap_manifest_key);
            remote_manifest_checksum = Some(manifest_csum);
            remote_manifest_generation = Some(manifest.generation);
        }

        let mut shard_descriptors = BTreeMap::new();
        if metadata.uses_local_shards() {
            let local_shards = crate::api::index::ensure_local_index_shards_open(
                state,
                index_name,
                metadata,
                "cluster snapshot",
            )
            .await;
            if local_shards.len() != metadata.number_of_shards as usize {
                return Err(crate::api::error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_not_available_exception",
                    format!(
                        "cannot capture complete snapshot for index [{index_name}]: expected {} open local shards, found {}",
                        metadata.number_of_shards,
                        local_shards.len()
                    ),
                ));
            }
            for (shard_id, engine) in local_shards {
                let engine_for_snap = engine.clone();
                let (live_docs, tombstones, wm_state) = state
                    .worker_pools
                    .spawn_maintenance("snapshot_local_shard", move || -> anyhow::Result<_> {
                        let _ = engine_for_snap.refresh();
                        let live = engine_for_snap.live_documents_with_versions()?;
                        let del = engine_for_snap.deleted_documents_with_versions()?;
                        let wm = engine_for_snap.seal_watermark_state()?;
                        Ok((live, del, wm))
                    })
                    .await
                    .map_err(|e| {
                        crate::api::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "snapshot_exception",
                            format!(
                                "failed to capture local shard [{index_name}][{shard_id}]: {e:#}"
                            ),
                        )
                    })?;

                let payload = ShardBackupPayload {
                    format_version: CLUSTER_BACKUP_FORMAT_VERSION,
                    index_uuid: index_uuid.clone(),
                    shard_id,
                    live_docs,
                    tombstones,
                    seal_watermark_state: wm_state.clone(),
                };
                let payload_bytes = serde_json::to_vec_pretty(&payload).map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_exception",
                        e,
                    )
                })?;
                let payload_checksum = sha256_prefixed(&payload_bytes);
                let payload_key = snapshot_shard_payload_key(snapshot_id, &index_uuid, shard_id);
                let payload_obj = state
                    .storage_manager
                    .object_path(&payload_key)
                    .map_err(|e| {
                        crate::api::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "snapshot_exception",
                            e,
                        )
                    })?;
                state
                    .storage_manager
                    .create_immutable(&payload_obj, payload_bytes)
                    .await
                    .map_err(|e| {
                        crate::api::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "snapshot_exception",
                            format!("failed to write shard snapshot payload: {e:#}"),
                        )
                    })?;

                total_shards += 1;
                total_live_docs += payload.live_docs.len();
                total_tombstones += payload.tombstones.len();
                shard_descriptors.insert(
                    shard_id,
                    ShardBackupDescriptor {
                        shard_id,
                        payload_path: payload_key,
                        payload_checksum,
                        live_doc_count: payload.live_docs.len(),
                        tombstone_count: payload.tombstones.len(),
                        sealed_through_seq_no: wm_state.sealed_through_seq_no,
                        retired_through_seq_no: wm_state.retired_through_seq_no,
                    },
                );
            }
        }

        index_entries.insert(
            index_name.clone(),
            IndexBackupEntry {
                metadata: metadata.clone(),
                lifecycle_mode: metadata.lifecycle_mode().as_str().to_string(),
                schema_hash,
                remote_manifest_path,
                remote_manifest_checksum,
                remote_manifest_generation,
                split_checksums,
                shards: shard_descriptors,
            },
        );
    }

    let duration_ms = started.elapsed().as_millis() as u64;
    let mut backup_manifest = ClusterBackupManifest {
        format_version: CLUSTER_BACKUP_FORMAT_VERSION,
        snapshot_id: snapshot_id.to_string(),
        cluster_name: cluster_state.cluster_name.clone(),
        cluster_state_version: cluster_state.version,
        created_at: chrono::Utc::now().to_rfc3339(),
        created_at_epoch_ms: now_ms,
        duration_ms,
        indices: index_entries,
        integrity_sha256: String::new(),
    };
    backup_manifest.integrity_sha256 = compute_backup_integrity(&backup_manifest).map_err(|e| {
        crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "snapshot_exception",
            format!("failed to compute snapshot integrity hash: {e:#}"),
        )
    })?;

    let manifest_bytes = serde_json::to_vec_pretty(&backup_manifest).map_err(|e| {
        crate::api::error_response(StatusCode::INTERNAL_SERVER_ERROR, "snapshot_exception", e)
    })?;
    let manifest_checksum = sha256_prefixed(&manifest_bytes);
    state
        .storage_manager
        .create_immutable(&manifest_obj_path, manifest_bytes)
        .await
        .map_err(|e| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "snapshot_exception",
                format!("failed to persist cluster backup manifest: {e:#}"),
            )
        })?;

    let index_names: Vec<String> = backup_manifest.indices.keys().cloned().collect();
    Ok(serde_json::json!({
        "snapshot_id": snapshot_id,
        "status": "committed",
        "cluster_name": backup_manifest.cluster_name,
        "cluster_state_version": backup_manifest.cluster_state_version,
        "indices": index_names,
        "total_indices": backup_manifest.indices.len(),
        "total_splits": total_splits,
        "total_shards": total_shards,
        "total_live_docs": total_live_docs,
        "total_tombstones": total_tombstones,
        "duration_ms": duration_ms,
        "manifest_path": manifest_key,
        "manifest_checksum": manifest_checksum,
        "integrity_sha256": backup_manifest.integrity_sha256,
    }))
}

async fn load_cluster_backup_manifest(
    state: &AppState,
    snapshot_id: &str,
) -> Result<ClusterBackupManifest, (StatusCode, Json<Value>)> {
    validate_snapshot_id(snapshot_id)?;
    let manifest_key = snapshot_manifest_key(snapshot_id);
    let obj_path = state
        .storage_manager
        .object_path(&manifest_key)
        .map_err(|e| {
            crate::api::error_response(StatusCode::INTERNAL_SERVER_ERROR, "snapshot_exception", e)
        })?;
    let bytes = state
        .storage_manager
        .get_bytes(&obj_path)
        .await
        .map_err(|_| {
            crate::api::error_response(
                StatusCode::NOT_FOUND,
                "snapshot_not_found_exception",
                format!("snapshot [{snapshot_id}] does not exist"),
            )
        })?;
    let manifest: ClusterBackupManifest = serde_json::from_slice(&bytes).map_err(|e| {
        crate::api::error_response(
            StatusCode::CONFLICT,
            "snapshot_corruption_exception",
            format!("snapshot [{snapshot_id}] manifest JSON is corrupt: {e}"),
        )
    })?;
    if manifest.format_version != CLUSTER_BACKUP_FORMAT_VERSION {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "snapshot_corruption_exception",
            format!(
                "unsupported snapshot format_version {}; expected {}",
                manifest.format_version, CLUSTER_BACKUP_FORMAT_VERSION
            ),
        ));
    }
    Ok(manifest)
}

/// GET /_snapshot/{snapshot_id} — Inspect a stored cluster backup manifest (`FS-042`).
pub(crate) async fn get_cluster_snapshot(
    state: &AppState,
    snapshot_id: &str,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let manifest = load_cluster_backup_manifest(state, snapshot_id).await?;
    let expected_integrity = compute_backup_integrity(&manifest).map_err(|e| {
        crate::api::error_response(StatusCode::INTERNAL_SERVER_ERROR, "snapshot_exception", e)
    })?;
    if manifest.integrity_sha256 != expected_integrity {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "snapshot_corruption_exception",
            format!(
                "snapshot [{snapshot_id}] integrity hash mismatch: expected {}, got {}",
                manifest.integrity_sha256, expected_integrity
            ),
        ));
    }
    serde_json::to_value(&manifest).map_err(|e| {
        crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_exception",
            e,
        )
    })
}

/// POST /_snapshot/{snapshot_id}/_verify — Verify backup manifest integrity,
/// remote_store manifest lineage and bundle checksums, and local-shard payload
/// checksums (`FS-042`).
pub(crate) async fn verify_cluster_snapshot(
    state: &AppState,
    snapshot_id: &str,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let manifest = load_cluster_backup_manifest(state, snapshot_id).await?;
    let mut failures = Vec::new();
    let mut corrupt_count = 0usize;
    let mut missing_count = 0usize;
    let mut verified_indices = 0usize;
    let mut verified_splits = 0usize;
    let mut verified_shards = 0usize;

    match compute_backup_integrity(&manifest) {
        Ok(actual) if actual == manifest.integrity_sha256 => {}
        Ok(actual) => {
            corrupt_count += 1;
            failures.push(serde_json::json!({
                "component": "backup_manifest",
                "status": "corrupt",
                "reason": format!(
                    "backup manifest integrity hash mismatch: expected {}, got {}",
                    manifest.integrity_sha256, actual
                )
            }));
        }
        Err(e) => {
            corrupt_count += 1;
            failures.push(serde_json::json!({
                "component": "backup_manifest",
                "status": "corrupt",
                "reason": format!("failed to compute backup integrity: {e:#}")
            }));
        }
    }

    for (index_name, entry) in &manifest.indices {
        let index_uuid = entry.metadata.uuid.to_string();
        let actual_schema_hash = crate::storage::compute_schema_hash(&entry.metadata.mappings);
        if actual_schema_hash != entry.schema_hash {
            corrupt_count += 1;
            failures.push(serde_json::json!({
                "index": index_name,
                "component": "schema_hash",
                "status": "corrupt",
                "reason": format!(
                    "schema_hash mismatch: expected {}, got {}",
                    entry.schema_hash, actual_schema_hash
                )
            }));
            continue;
        }

        if let Some(ref remote_manifest_path) = entry.remote_manifest_path {
            let Ok(obj_path) = state.storage_manager.object_path(remote_manifest_path) else {
                corrupt_count += 1;
                continue;
            };
            match state.storage_manager.get_bytes(&obj_path).await {
                Ok(bytes) => {
                    let actual_csum = sha256_prefixed(&bytes);
                    if let Some(ref expected_csum) = entry.remote_manifest_checksum
                        && &actual_csum != expected_csum
                    {
                        corrupt_count += 1;
                        failures.push(serde_json::json!({
                            "index": index_name,
                            "component": "remote_manifest",
                            "status": "corrupt",
                            "reason": format!(
                                "remote manifest checksum mismatch: expected {expected_csum}, got {actual_csum}"
                            )
                        }));
                    } else if let Ok(remote_manifest) =
                        serde_json::from_slice::<crate::storage::RemoteStoreManifest>(&bytes)
                    {
                        for split in remote_manifest.published_splits() {
                            match state
                                .storage_manager
                                .hash_split_bundle(&index_uuid, split)
                                .await
                            {
                                Ok(actual_split_hash) if actual_split_hash == split.checksum => {
                                    verified_splits += 1;
                                }
                                Ok(actual_split_hash) => {
                                    corrupt_count += 1;
                                    failures.push(serde_json::json!({
                                        "index": index_name,
                                        "split_id": split.split_id,
                                        "component": "split_bundle",
                                        "status": "corrupt",
                                        "expected": split.checksum,
                                        "actual": actual_split_hash,
                                    }));
                                }
                                Err(e) => {
                                    missing_count += 1;
                                    failures.push(serde_json::json!({
                                        "index": index_name,
                                        "split_id": split.split_id,
                                        "component": "split_bundle",
                                        "status": "missing",
                                        "reason": format!("{e:#}"),
                                    }));
                                }
                            }
                        }
                    } else {
                        corrupt_count += 1;
                        failures.push(serde_json::json!({
                            "index": index_name,
                            "component": "remote_manifest",
                            "status": "corrupt",
                            "reason": "cannot decode backed-up RemoteStoreManifest JSON"
                        }));
                    }
                }
                Err(e) => {
                    missing_count += 1;
                    failures.push(serde_json::json!({
                        "index": index_name,
                        "component": "remote_manifest",
                        "status": "missing",
                        "reason": format!("{e:#}")
                    }));
                }
            }
        }

        for expected_shard_id in 0..entry.metadata.number_of_shards {
            if !entry.shards.contains_key(&expected_shard_id) {
                missing_count += 1;
                failures.push(serde_json::json!({
                    "index": index_name,
                    "shard_id": expected_shard_id,
                    "component": "shard_snapshot",
                    "status": "missing",
                    "reason": format!("missing shard_snapshot descriptor for shard {expected_shard_id}")
                }));
            }
        }
        for (shard_id, shard_desc) in &entry.shards {
            let Ok(obj_path) = state.storage_manager.object_path(&shard_desc.payload_path) else {
                corrupt_count += 1;
                continue;
            };
            match state.storage_manager.get_bytes(&obj_path).await {
                Ok(bytes) => {
                    let actual_csum = sha256_prefixed(&bytes);
                    if actual_csum != shard_desc.payload_checksum {
                        corrupt_count += 1;
                        failures.push(serde_json::json!({
                            "index": index_name,
                            "shard_id": shard_id,
                            "component": "shard_snapshot",
                            "status": "corrupt",
                            "expected": shard_desc.payload_checksum,
                            "actual": actual_csum,
                        }));
                    } else if serde_json::from_slice::<ShardBackupPayload>(&bytes).is_err() {
                        corrupt_count += 1;
                        failures.push(serde_json::json!({
                            "index": index_name,
                            "shard_id": shard_id,
                            "component": "shard_snapshot",
                            "status": "corrupt",
                            "reason": "cannot decode ShardBackupPayload JSON"
                        }));
                    } else {
                        verified_shards += 1;
                    }
                }
                Err(e) => {
                    missing_count += 1;
                    failures.push(serde_json::json!({
                        "index": index_name,
                        "shard_id": shard_id,
                        "component": "shard_snapshot",
                        "status": "missing",
                        "reason": format!("{e:#}")
                    }));
                }
            }
        }

        verified_indices += 1;
    }

    let status = if corrupt_count == 0 && missing_count == 0 {
        "ok"
    } else {
        "corrupt"
    };

    Ok(serde_json::json!({
        "snapshot_id": snapshot_id,
        "status": status,
        "verified_indices": verified_indices,
        "verified_splits": verified_splits,
        "verified_shards": verified_shards,
        "corrupt_count": corrupt_count,
        "missing_count": missing_count,
        "failures": failures,
    }))
}

/// POST /_snapshot/{snapshot_id}/_restore — Verify and restore a cluster
/// backup snapshot to the cluster, reconstructing control-plane metadata,
/// remote_store manifests/pointers, and local shard documents, tombstones, and
/// seal watermarks while reporting RPO/RTO (`FS-042`).
pub(crate) async fn restore_cluster_snapshot(
    state: &AppState,
    snapshot_id: &str,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let restore_started = std::time::Instant::now();
    let verification = verify_cluster_snapshot(state, snapshot_id).await?;
    if verification.get("status").and_then(Value::as_str) != Some("ok") {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": {
                    "type": "snapshot_corruption_exception",
                    "reason": format!(
                        "cannot restore snapshot [{snapshot_id}] because verification failed (corrupt_count={}, missing_count={})",
                        verification["corrupt_count"],
                        verification["missing_count"]
                    ),
                    "error_class": "corruption",
                    "verification": verification,
                },
                "status": StatusCode::CONFLICT.as_u16(),
            })),
        ));
    }

    let manifest = load_cluster_backup_manifest(state, snapshot_id).await?;
    let overwrite_existing = body
        .as_ref()
        .and_then(|v| v.get("overwrite_existing"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let requested_indices: Option<HashSet<String>> = body
        .as_ref()
        .and_then(|v| v.get("indices"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        });

    let cluster_state = state.cluster_manager.get_state();
    for index_name in manifest.indices.keys() {
        if let Some(ref req_set) = requested_indices
            && !req_set.contains(index_name)
        {
            continue;
        }
        if cluster_state.indices.contains_key(index_name) && !overwrite_existing {
            return Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "resource_already_exists_exception",
                format!("index [{index_name}] already exists and overwrite_existing is false"),
            ));
        }
    }

    let mut restored_indices = Vec::new();
    let mut restored_splits = 0usize;
    let mut restored_shards = 0usize;
    let mut restored_live_docs = 0usize;
    let mut restored_tombstones = 0usize;

    for (index_name, entry) in &manifest.indices {
        if let Some(ref req_set) = requested_indices
            && !req_set.contains(index_name)
        {
            continue;
        }

        if let Some(existing_meta) = state
            .cluster_manager
            .get_state()
            .indices
            .get(index_name)
            .cloned()
        {
            let _ = state
                .shard_manager
                .close_index_shards_for_uuid_blocking_with_reason(
                    index_name.clone(),
                    existing_meta.uuid.to_string(),
                    crate::shard::SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX,
                )
                .await;
            if let Some(master) =
                match crate::api::resolve_leader_or_master(state, "delete before restore") {
                    Ok(m) => m,
                    Err(e) => return Err(e),
                }
            {
                state
                    .transport_client
                    .forward_delete_index(&master, index_name)
                    .await
                    .map_err(|e| {
                        crate::api::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "snapshot_restore_exception",
                            format!("failed to delete existing index [{index_name}]: {e:#}"),
                        )
                    })?;
            } else {
                crate::api::raft_write(
                    state,
                    crate::consensus::types::ClusterCommand::DeleteIndex {
                        index_name: index_name.clone(),
                    },
                )
                .await?;
            }
            let ver = state.cluster_manager.version();
            let _ = state.cluster_manager.wait_for_version(ver).await;
        }

        // Restore remote_store manifest and pointer if present in the backup.
        if let Some(ref remote_manifest_path) = entry.remote_manifest_path {
            let obj_path = state
                .storage_manager
                .object_path(remote_manifest_path)
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_restore_exception",
                        e,
                    )
                })?;
            let bytes = state
                .storage_manager
                .get_bytes(&obj_path)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_restore_exception",
                        format!("failed to read backed-up remote manifest: {e:#}"),
                    )
                })?;
            let remote_manifest: crate::storage::RemoteStoreManifest =
                serde_json::from_slice(&bytes).map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_restore_exception",
                        e,
                    )
                })?;
            restored_splits += remote_manifest.published_splits().count();
            state
                .storage_manager
                .publish_manifest(&remote_manifest)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_restore_exception",
                        format!("failed to restore remote manifest for [{index_name}]: {e:#}"),
                    )
                })?;
        }

        // Adapt shard routing to the current cluster's data nodes if needed while preserving UUID, mappings, and settings.
        let mut metadata_to_create = entry.metadata.clone();
        if metadata_to_create.number_of_shards > 0 {
            let current_cs = state.cluster_manager.get_state();
            let mut data_nodes: Vec<String> = current_cs
                .nodes
                .values()
                .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
                .map(|n| n.id.clone())
                .collect();
            data_nodes.sort();
            if data_nodes.is_empty() {
                data_nodes.push(state.local_node_id.clone());
            }
            let all_nodes_present = metadata_to_create.shard_routing.values().all(|r| {
                data_nodes.contains(&r.primary)
                    && r.replicas.iter().all(|rep| data_nodes.contains(rep))
            });
            if !all_nodes_present {
                let mut rebuilt_routing = HashMap::new();
                for shard_id in 0..metadata_to_create.number_of_shards {
                    let primary = data_nodes[(shard_id as usize) % data_nodes.len()].clone();
                    let mut replicas = Vec::new();
                    for r_idx in 0..(metadata_to_create.number_of_replicas as usize) {
                        let cand = &data_nodes[(shard_id as usize + r_idx + 1) % data_nodes.len()];
                        if cand != &primary && !replicas.contains(cand) {
                            replicas.push(cand.clone());
                        }
                    }
                    let unassigned_replicas = metadata_to_create
                        .number_of_replicas
                        .saturating_sub(replicas.len() as u32);
                    rebuilt_routing.insert(
                        shard_id,
                        ShardRoutingEntry {
                            primary,
                            primary_term: 1,
                            replicas,
                            in_sync_replicas: Vec::new(),
                            unassigned_replicas,
                        },
                    );
                }
                metadata_to_create.shard_routing = rebuilt_routing;
            } else {
                for routing in metadata_to_create.shard_routing.values_mut() {
                    routing.primary_term = routing.primary_term.max(1);
                    routing.in_sync_replicas.clear();
                }
            }
        }

        let index_uuid_str = entry.metadata.uuid.to_string();
        let stale_index_dir = state.shard_manager.data_dir().join(&index_uuid_str);
        if stale_index_dir.exists() {
            let _ = std::fs::remove_dir_all(&stale_index_dir);
        }

        if let Some(master) =
            match crate::api::resolve_leader_or_master(state, "create index for restore") {
                Ok(m) => m,
                Err(e) => return Err(e),
            }
        {
            let create_body = serde_json::json!({
                "_restore_uuid": metadata_to_create.uuid.as_str(),
                "engine": metadata_to_create.settings.engine.to_string(),
                "settings": {
                    "number_of_shards": metadata_to_create.number_of_shards,
                    "number_of_replicas": metadata_to_create.number_of_replicas,
                    "refresh_interval_ms": metadata_to_create.settings.refresh_interval_ms,
                    "flush_threshold_bytes": metadata_to_create.settings.flush_threshold_bytes,
                    "remote_store": metadata_to_create.settings.remote_store,
                },
                "mappings": {
                    "dynamic": metadata_to_create.dynamic.to_string(),
                    "properties": metadata_to_create.mappings,
                }
            });
            let body_bytes = serde_json::to_vec(&create_body).unwrap_or_default();
            state
                .transport_client
                .forward_create_index(&master, index_name, &body_bytes)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "snapshot_restore_exception",
                        format!("failed to forward index creation during restore: {e:#}"),
                    )
                })?;
        } else {
            crate::api::raft_write(
                state,
                crate::consensus::types::ClusterCommand::CreateIndex {
                    metadata: metadata_to_create.clone(),
                },
            )
            .await?;
        }
        let ver = state.cluster_manager.version();
        let _ = state.cluster_manager.wait_for_version(ver).await;

        if metadata_to_create.uses_local_shards() {
            let committed_state = state.cluster_manager.get_state();
            let committed_meta = committed_state
                .indices
                .get(index_name)
                .cloned()
                .unwrap_or_else(|| metadata_to_create.clone());
            for (shard_id, routing) in &committed_meta.shard_routing {
                if routing.primary == state.local_node_id
                    && let Some(allocation_id) = committed_state.shard_allocation_id(
                        index_name,
                        *shard_id,
                        &state.local_node_id,
                    )
                {
                    let _ = state
                        .shard_manager
                        .open_primary_assigned_shard_with_settings_blocking(
                            committed_meta.name.clone(),
                            *shard_id,
                            committed_meta.mappings.clone(),
                            committed_meta.settings.clone(),
                            committed_meta.uuid.clone(),
                            crate::shard::AssignedShardOpen {
                                allocation_id,
                                primary_term: routing.primary_term,
                                allow_empty_creation: committed_state
                                    .may_create_initial_empty_copy(
                                        index_name,
                                        *shard_id,
                                        &state.local_node_id,
                                    ),
                            },
                        )
                        .await;
                }
            }
            let local_shards = crate::api::index::ensure_local_index_shards_open(
                state,
                index_name,
                &committed_meta,
                "restore index shards",
            )
            .await;
            if local_shards.len() != committed_meta.number_of_shards as usize {
                return Err(crate::api::error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_not_available_exception",
                    format!(
                        "cannot complete restore for index [{index_name}]: expected {} open local shards, found {}",
                        committed_meta.number_of_shards,
                        local_shards.len()
                    ),
                ));
            }
            for (shard_id, engine) in local_shards {
                if let Some(shard_desc) = entry.shards.get(&shard_id) {
                    let obj_path = state
                        .storage_manager
                        .object_path(&shard_desc.payload_path)
                        .map_err(|e| {
                            crate::api::error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "snapshot_restore_exception",
                                e,
                            )
                        })?;
                    let bytes = state
                        .storage_manager
                        .get_bytes(&obj_path)
                        .await
                        .map_err(|e| {
                            crate::api::error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "snapshot_restore_exception",
                                format!("failed to read shard backup payload: {e:#}"),
                            )
                        })?;
                    let payload: ShardBackupPayload =
                        serde_json::from_slice(&bytes).map_err(|e| {
                            crate::api::error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "snapshot_restore_exception",
                                e,
                            )
                        })?;
                    let live_len = payload.live_docs.len();
                    let tomb_len = payload.tombstones.len();
                    let engine_for_restore = engine.clone();
                    state
                        .worker_pools
                        .spawn_maintenance("restore_shard_documents", move || {
                            engine_for_restore.restore_documents_with_versions(
                                &payload.live_docs,
                                &payload.tombstones,
                                Some(payload.seal_watermark_state),
                            )
                        })
                        .await
                        .map_err(|e| {
                            crate::api::error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "snapshot_restore_exception",
                                format!(
                                    "failed to restore documents on shard [{index_name}][{shard_id}]: {e:#}"
                                ),
                            )
                        })?;
                    restored_shards += 1;
                    restored_live_docs += live_len;
                    restored_tombstones += tomb_len;
                }
            }
        }

        restored_indices.push(index_name.clone());
    }

    let rto_ms = restore_started.elapsed().as_millis() as u64;
    Ok(serde_json::json!({
        "snapshot_id": snapshot_id,
        "status": "restored",
        "restored_indices": restored_indices,
        "restored_splits": restored_splits,
        "restored_shards": restored_shards,
        "restored_live_docs": restored_live_docs,
        "restored_tombstones": restored_tombstones,
        "rto_ms": rto_ms,
        "rpo_unsealed_ops": 0,
    }))
}

/// POST /{index}/_remote_store/repair — Verify, reconstruct, or quarantine
/// corrupt remote_store pointers, local cache artifacts, and split bundles (`FS-042`).
pub(crate) async fn repair_remote_store_index(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !metadata.uses_remote_store() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] has lifecycle_mode [{}] which does not support remote_store repair",
                index_name,
                metadata.lifecycle_mode()
            ),
        ));
    }

    let quarantine_corrupt = body
        .as_ref()
        .and_then(|v| v.get("quarantine_corrupt_splits"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let rebuild_local_cache = body
        .as_ref()
        .and_then(|v| v.get("rebuild_local_cache"))
        .and_then(Value::as_bool)
        .unwrap_or(true);

    let index_uuid = metadata.uuid.to_string();
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let lock_arc = state.storage_manager.sequencer_lock(&index_uuid).await;
    let _sequencer_guard = lock_arc.lock().await;
    let _ = state.storage_manager.rebuild_cache_inventory_online();

    // 1. Check or repair manifest.current.json pointer from valid committed manifest generations.
    let mut pointer_repaired = false;
    let (current_manifest, current_manifest_path) = match state
        .storage_manager
        .load_manifest_for_snapshot(&index_uuid, None, Some(&expected_schema_hash))
        .await
    {
        Ok(Some((m, path, _, _))) => (m, path),
        Ok(None) | Err(_) => {
            let manifests_prefix = format!("{index_uuid}/manifests/");
            let prefix_path = state
                .storage_manager
                .object_path(&format!("{index_uuid}/manifests"))
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_repair_exception",
                        e,
                    )
                })?;
            let mut stream = state
                .storage_manager
                .object_store()
                .list(Some(&prefix_path));
            let mut candidate_generations: Vec<(u64, String)> = Vec::new();
            while let Some(item) = stream.next().await {
                let Ok(meta) = item else { continue };
                let loc = meta.location.to_string();
                if loc.contains(".ferris.") && loc.ends_with("#0") {
                    continue;
                }
                let Some(rest) = loc.strip_prefix(&manifests_prefix) else {
                    continue;
                };
                if let Some(stem) = rest.strip_suffix(".json")
                    && !stem.contains('/')
                    && let Ok(gen_num) = stem.parse::<u64>()
                    && gen_num > 0
                {
                    candidate_generations.push((gen_num, loc));
                } else if let Some((gen_str, file_part)) = rest.split_once('/')
                    && file_part.ends_with(".json")
                    && !file_part.contains('/')
                    && let Ok(gen_num) = gen_str.parse::<u64>()
                    && gen_num > 0
                {
                    candidate_generations.push((gen_num, loc));
                }
            }
            candidate_generations.sort_by_key(|a| std::cmp::Reverse(a.0));
            let mut recovered = None;
            for (gen_num, manifest_loc) in candidate_generations {
                if let Ok(m) = state
                    .storage_manager
                    .load_manifest(&index_uuid, gen_num, Some(&expected_schema_hash))
                    .await
                    && state
                        .storage_manager
                        .verify_manifest_lineage_chain(&index_uuid, &m, Some(&expected_schema_hash))
                        .await
                        .is_ok()
                {
                    let is_committed = m.operation_id.is_empty()
                        || m.operation_id.starts_with("repair-")
                        || m.operation_id.starts_with("rollback-")
                        || state
                            .storage_manager
                            .load_publication_committed(&index_uuid, &m.operation_id)
                            .await
                            .ok()
                            .flatten()
                            .is_some();
                    if is_committed {
                        recovered = Some((m, manifest_loc));
                        break;
                    }
                }
            }
            let Some((valid_manifest, valid_manifest_path)) = recovered else {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "remote_store_irreparable_corruption_exception",
                    format!(
                        "index [{index_name}] has a corrupt or missing manifest pointer and no valid manifest generation could be recovered"
                    ),
                ));
            };
            let pointer_key = format!("{index_uuid}/manifest.current.json");
            let pointer_obj = state
                .storage_manager
                .object_path(&pointer_key)
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_repair_exception",
                        e,
                    )
                })?;
            let manifest_obj = state
                .storage_manager
                .object_path(&valid_manifest_path)
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_repair_exception",
                        e,
                    )
                })?;
            let gen_bytes = state
                .storage_manager
                .get_bytes(&manifest_obj)
                .await
                .unwrap_or_else(|_| serde_json::to_vec_pretty(&valid_manifest).unwrap_or_default());
            let repaired_ptr = crate::storage::RemoteManifestPointer {
                version: crate::storage::REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
                index_uuid: index_uuid.clone(),
                pointer_revision: valid_manifest.generation.max(1),
                writer_epoch: valid_manifest.writer_epoch.max(1),
                current_generation: valid_manifest.generation,
                manifest_path: valid_manifest_path.clone(),
                manifest_checksum: Some(sha256_prefixed(&gen_bytes)),
                last_operation_id: (!valid_manifest.operation_id.is_empty())
                    .then(|| valid_manifest.operation_id.clone()),
                manifest_etag: None,
            };
            let ptr_bytes = serde_json::to_vec_pretty(&repaired_ptr).map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_repair_exception",
                    e,
                )
            })?;
            state
                .storage_manager
                .put(&pointer_obj, ptr_bytes)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_repair_exception",
                        format!("failed to write repaired manifest pointer: {e:#}"),
                    )
                })?;
            pointer_repaired = true;
            (valid_manifest, valid_manifest_path)
        }
    };

    let mut healthy_splits = Vec::new();
    let mut repaired_cache_splits = Vec::new();
    let mut repaired_bundles = Vec::new();
    let mut quarantined_splits = Vec::new();
    let mut irreparable_splits = Vec::new();
    let mut parent_restorations: Vec<(String, Vec<String>)> = Vec::new();

    for split in current_manifest.published_splits() {
        let bundle_ok = state
            .storage_manager
            .hash_split_bundle(&index_uuid, split)
            .await
            .is_ok_and(|actual| actual == split.checksum);

        if bundle_ok {
            let cache_status = state.storage_manager.cached_split_status(
                &index_uuid,
                &split.split_id,
                &split.checksum,
            );
            if rebuild_local_cache && !cache_status.artifact_cached {
                let _ = state.storage_manager.evict_split_from_cache(
                    &index_uuid,
                    &split.split_id,
                    "corrupt_or_repaired",
                );
                if state
                    .storage_manager
                    .fetch_split_into_cache(&index_uuid, split)
                    .await
                    .is_ok()
                {
                    repaired_cache_splits.push(split.split_id.clone());
                }
            }
            healthy_splits.push(split.split_id.clone());
            continue;
        }

        // Bundle in object storage is corrupt or missing. Try reconstruction!
        let _ = state.storage_manager.evict_split_from_cache(
            &index_uuid,
            &split.split_id,
            "corrupt_or_repaired",
        );

        // Attempt 1: Reconstruct from unretired local shard history if this is a sealed split.
        let mut reconstructed = false;
        if metadata.is_tiered()
            && let Ok(Some(seal_range)) = split.seal_range()
        {
            let local_shards = crate::api::index::ensure_local_index_shards_open(
                state,
                index_name,
                metadata,
                "repair sealed split from local shard",
            )
            .await;
            if let Some((_, engine)) = local_shards
                .iter()
                .find(|(sid, _)| *sid == seal_range.shard_id)
                && let Ok(wm) = engine.seal_watermark_state()
                && wm
                    .retired_through_seq_no
                    .is_none_or(|ret| ret < seal_range.start_seq_no)
                && let Ok(all_live) = engine.live_documents_with_versions()
            {
                let docs_in_range: Vec<(String, Value)> = all_live
                    .into_iter()
                    .filter(|(_, _, seq, _, _)| {
                        *seq >= seal_range.start_seq_no && *seq <= seal_range.end_seq_no
                    })
                    .map(|(id, src, _, _, _)| (id, src))
                    .collect();
                if docs_in_range.len() as u64 == split.doc_count {
                    let staging_dir = state
                        .storage_manager
                        .staging_dir(&index_uuid, &split.split_id);
                    let mappings_arc = Arc::new(metadata.mappings.clone());
                    let staging_clone = staging_dir.clone();
                    let seal_meta_clone = seal_range.clone();
                    if state
                        .worker_pools
                        .spawn_maintenance("repair_rebuild_split", move || -> anyhow::Result<()> {
                            if staging_clone.exists() {
                                std::fs::remove_dir_all(&staging_clone)?;
                            }
                            std::fs::create_dir_all(&staging_clone)?;
                            let col_cache =
                                Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                            let split_engine =
                                crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                                    &staging_clone,
                                    Duration::from_secs(60),
                                    mappings_arc.as_ref(),
                                    col_cache,
                                )?;
                            if !docs_in_range.is_empty() {
                                split_engine.bulk_add_documents(docs_in_range)?;
                            }
                            split_engine.flush()?;
                            strip_split_wal(&staging_clone)?;
                            let versions_bytes = serde_json::to_vec_pretty(&seal_meta_clone)?;
                            std::fs::write(
                                staging_clone.join("seal.versions.json"),
                                versions_bytes,
                            )?;
                            Ok(())
                        })
                        .await
                        .is_ok()
                        && let Ok((rebuilt_bytes, rebuilt_csum)) =
                            crate::storage::pack_bundle_from_dir(&staging_dir)
                    {
                        let _ = std::fs::remove_dir_all(&staging_dir);
                        if rebuilt_csum == split.checksum
                            && let Ok(obj_path) =
                                state.storage_manager.object_path(&split.bundle_path)
                            && state
                                .storage_manager
                                .put(&obj_path, rebuilt_bytes)
                                .await
                                .is_ok()
                        {
                            let _ = state
                                .storage_manager
                                .fetch_split_into_cache(&index_uuid, split)
                                .await;
                            repaired_bundles.push(split.split_id.clone());
                            reconstructed = true;
                        }
                    }
                }
            }
        }

        // Attempt 2: Reconstruct from intact compaction parent splits in manifest.
        if !reconstructed
            && let Ok(parents) = split.parent_split_ids()
            && !parents.is_empty()
        {
            let mut all_parents_ok = true;
            for pid in &parents {
                let Some(parent_split) =
                    current_manifest.splits.iter().find(|s| &s.split_id == pid)
                else {
                    all_parents_ok = false;
                    break;
                };
                let ok = state
                    .storage_manager
                    .hash_split_bundle(&index_uuid, parent_split)
                    .await
                    .is_ok_and(|actual| actual == parent_split.checksum);
                if !ok {
                    all_parents_ok = false;
                    break;
                }
            }
            if all_parents_ok {
                parent_restorations.push((split.split_id.clone(), parents));
                repaired_bundles.push(split.split_id.clone());
                reconstructed = true;
            }
        }

        if !reconstructed {
            if quarantine_corrupt {
                let quarantine_key = format!("{index_uuid}/quarantine/{}.json", split.split_id);
                if let Ok(q_obj) = state.storage_manager.object_path(&quarantine_key) {
                    let q_record = serde_json::json!({
                        "index": index_name,
                        "index_uuid": index_uuid,
                        "split_id": split.split_id,
                        "bundle_path": split.bundle_path,
                        "expected_checksum": split.checksum,
                        "quarantined_at": chrono::Utc::now().to_rfc3339(),
                    });
                    let _ = state
                        .storage_manager
                        .put(
                            &q_obj,
                            serde_json::to_vec_pretty(&q_record).unwrap_or_default(),
                        )
                        .await;
                }
                quarantined_splits.push(split.split_id.clone());
            } else {
                irreparable_splits.push(split.split_id.clone());
            }
        }
    }

    if !irreparable_splits.is_empty() {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": {
                    "type": "remote_store_irreparable_corruption_exception",
                    "reason": format!(
                        "index [{index_name}] has {} irreparable split bundle(s) {:?}; restore from a cluster _snapshot or re-invoke with quarantine_corrupt_splits=true",
                        irreparable_splits.len(),
                        irreparable_splits
                    ),
                    "error_class": "corruption",
                    "irreparable_splits": irreparable_splits,
                },
                "status": StatusCode::CONFLICT.as_u16(),
            })),
        ));
    }

    let mut new_generation = current_manifest.generation;
    if !parent_restorations.is_empty() || !quarantined_splits.is_empty() {
        let restored_parent_ids: HashSet<String> = parent_restorations
            .iter()
            .flat_map(|(_, p)| p.iter().cloned())
            .collect();
        let removed_compacted_ids: HashSet<String> = parent_restorations
            .iter()
            .map(|(cid, _)| cid.clone())
            .collect();
        let quarantined_set: HashSet<String> = quarantined_splits.iter().cloned().collect();

        let mut updated_splits = Vec::new();
        for mut s in current_manifest.splits.clone() {
            if removed_compacted_ids.contains(&s.split_id) || quarantined_set.contains(&s.split_id)
            {
                continue;
            }
            if restored_parent_ids.contains(&s.split_id) {
                s.state = crate::storage::RemoteSplitState::Published;
                s.tags.remove("replaced_by_split_ids_json");
                s.tags.remove("replaced_by_split_ids");
                s.tags.remove("replaced_by_operation_id");
                s.tags.remove("replaced_at_generation");
            }
            updated_splits.push(s);
        }
        let next_gen = current_manifest.generation + 1;
        let op_id = format!("repair-{index_uuid}-g{next_gen}");
        let repaired_manifest = crate::storage::RemoteStoreManifest {
            version: crate::storage::REMOTE_MANIFEST_FORMAT_VERSION,
            engine: IndexEngine::RemoteStore,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            schema_generation: current_manifest.schema_generation,
            schema_hash: expected_schema_hash,
            generation: next_gen,
            parent_generation: current_manifest.generation,
            parent_manifest_path: Some(current_manifest_path),
            writer_epoch: current_manifest.writer_epoch.max(1),
            operation_id: op_id,
            request_fingerprint: sha256_prefixed(format!("repair-{next_gen}").as_bytes()),
            published_at: chrono::Utc::now().to_rfc3339(),
            settings: current_manifest.settings.clone(),
            published_watermarks: current_manifest.published_watermarks.clone(),
            compaction_lineage: Vec::new(),
            splits: updated_splits,
        };
        let ptr = state
            .storage_manager
            .publish_manifest(&repaired_manifest)
            .await
            .map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_repair_exception",
                    format!("failed to publish repaired manifest generation: {e:#}"),
                )
            })?;
        new_generation = ptr.current_generation;
    }

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": index_uuid,
        "status": if !quarantined_splits.is_empty() { "quarantined" } else if pointer_repaired || !repaired_bundles.is_empty() || !repaired_cache_splits.is_empty() { "repaired" } else { "healthy" },
        "generation": new_generation,
        "pointer_repaired": pointer_repaired,
        "healthy_splits": healthy_splits,
        "repaired_cache_splits": repaired_cache_splits,
        "repaired_bundles": repaired_bundles,
        "quarantined_splits": quarantined_splits,
    }))
}

/// Compare the `MutableOnly` local-shard oracle against the candidate `Tiered`
/// read snapshot (published splits through `W` + hot delta after `W`) (`FS-043`, ADR 0004 section 3.12).
async fn verify_tiered_vs_mutable_equivalence(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
) -> Result<usize, (StatusCode, Json<Value>)> {
    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        metadata,
        "verify lifecycle equivalence",
    )
    .await;

    // 1. Collect authoritative MutableOnly live winners from local shards.
    let mut mutable_winners: BTreeMap<String, (Value, u64, u64, u64)> = BTreeMap::new();
    for (_shard_id, engine) in &local_shards {
        let eng = engine.clone();
        let live_docs = state
            .worker_pools
            .spawn_search(move || {
                let _ = eng.refresh();
                eng.live_documents_with_versions()
            })
            .await
            .map_err(query_error_response)?
            .map_err(query_error_response)?;
        for (doc_id, source, seq_no, term, ver) in live_docs {
            let replace = mutable_winners
                .get(&doc_id)
                .is_none_or(|(_, s, t, v)| (seq_no, term, ver) >= (*s, *t, *v));
            if replace {
                mutable_winners.insert(doc_id, (source, seq_no, term, ver));
            }
        }
    }

    // 2. Collect candidate Tiered winners across published splits (<= W) and hot delta (> W).
    let mut tiered_meta = metadata.clone();
    let mut rs = tiered_meta
        .settings
        .remote_store
        .clone()
        .unwrap_or_default();
    let mut hd = rs.hot_delta.clone().unwrap_or_default();
    hd.enabled = true;
    rs.hot_delta = Some(hd);
    tiered_meta.settings.remote_store = Some(rs);

    let resolved =
        resolve_index_read_snapshot(state, index_name, &tiered_meta, None, &local_shards).await?;
    let mut tiered_winners: BTreeMap<String, (u64, u64, u64, bool)> = BTreeMap::new();

    if let Some(ref manifest) = resolved.manifest {
        for split in manifest.published_splits() {
            let actual_hash = state
                .storage_manager
                .hash_split_bundle(metadata.uuid.as_str(), split)
                .await
                .map_err(|e| {
                    crate::api::error_response(
                        StatusCode::CONFLICT,
                        "migration_equivalence_exception",
                        format!(
                            "published split [{}] failed verification: {e:#}",
                            split.split_id
                        ),
                    )
                })?;
            if actual_hash != split.checksum {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "migration_equivalence_exception",
                    format!("published split [{}] checksum mismatch", split.split_id),
                ));
            }
            if let Ok(dvs) = split.doc_versions() {
                for dv in dvs {
                    let cand = (dv.seq_no, dv.primary_term, dv.version, false);
                    if tiered_winners
                        .get(&dv.doc_id)
                        .is_none_or(|(s, t, v, _)| (cand.0, cand.1, cand.2) >= (*s, *t, *v))
                    {
                        tiered_winners.insert(dv.doc_id, cand);
                    }
                }
            }
            if let Ok(tss) = split.tombstones() {
                for ts in tss {
                    let cand = (ts.seq_no, ts.primary_term, ts.version, true);
                    if tiered_winners
                        .get(&ts.doc_id)
                        .is_none_or(|(s, t, v, _)| (cand.0, cand.1, cand.2) >= (*s, *t, *v))
                    {
                        tiered_winners.insert(ts.doc_id, cand);
                    }
                }
            }
        }
    }

    for (shard_id, engine) in &local_shards {
        let wm_seq = resolved
            .snapshot
            .published_watermarks
            .get(shard_id)
            .map(|w| w.sealed_through_seq_no);
        let eng = engine.clone();
        let (live_res, del_res) = state
            .worker_pools
            .spawn_search(move || {
                (
                    eng.live_documents_with_versions(),
                    eng.deleted_documents_with_versions(),
                )
            })
            .await
            .map_err(query_error_response)?;
        if let Ok(live_docs) = live_res {
            for (doc_id, _src, seq_no, term, ver) in live_docs {
                if wm_seq.is_none_or(|w| seq_no > w) {
                    let cand = (seq_no, term, ver, false);
                    if tiered_winners
                        .get(&doc_id)
                        .is_none_or(|(s, t, v, _)| (cand.0, cand.1, cand.2) >= (*s, *t, *v))
                    {
                        tiered_winners.insert(doc_id, cand);
                    }
                }
            }
        }
        if let Ok(del_docs) = del_res {
            for (doc_id, seq_no, term, ver) in del_docs {
                if wm_seq.is_none_or(|w| seq_no > w) {
                    let cand = (seq_no, term, ver, true);
                    if tiered_winners
                        .get(&doc_id)
                        .is_none_or(|(s, t, v, _)| (cand.0, cand.1, cand.2) >= (*s, *t, *v))
                    {
                        tiered_winners.insert(doc_id, cand);
                    }
                }
            }
        }
    }

    let tiered_live: BTreeMap<String, (u64, u64, u64)> = tiered_winners
        .into_iter()
        .filter_map(|(doc_id, (s, t, v, is_del))| (!is_del).then_some((doc_id, (s, t, v))))
        .collect();

    if mutable_winners.len() != tiered_live.len() {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "migration_equivalence_exception",
            format!(
                "lifecycle equivalence check failed for [{index_name}]: mutable live doc count ({}) != tiered live doc count ({})",
                mutable_winners.len(),
                tiered_live.len()
            ),
        ));
    }

    for (doc_id, (_src, m_seq, m_term, m_ver)) in &mutable_winners {
        let Some((t_seq, t_term, t_ver)) = tiered_live.get(doc_id) else {
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "migration_equivalence_exception",
                format!(
                    "lifecycle equivalence check failed for [{index_name}]: doc [{doc_id}] missing from tiered snapshot"
                ),
            ));
        };
        if (m_seq, m_term, m_ver) != (t_seq, t_term, t_ver) {
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "migration_equivalence_exception",
                format!(
                    "lifecycle equivalence check failed for [{index_name}] doc [{doc_id}]: mutable version ({m_seq},{m_term},{m_ver}) != tiered version ({t_seq},{t_term},{t_ver})"
                ),
            ));
        }
    }

    Ok(mutable_winners.len())
}

/// POST /{index}/_lifecycle/migrate — Online migration of a `MutableOnly`
/// (`local_shards`) index into the unified `Tiered` lifecycle with bootstrap
/// sealing, equivalence verification, conditional Raft cutover, and fast-rollback
/// retention (`FS-043`, ADR 0004 sections 3.10–3.14).
pub(crate) async fn migrate_index_lifecycle(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    // Preflight 1: Reject source-only ImmutableOnly remote_store indices.
    if !metadata.uses_local_shards() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{index_name}] has lifecycle_mode [{}] without mutable shard history; source-only immutable_only indices must be rewritten into versioned splits before enabling tiered lifecycle (ADR 0004 section 3.14)",
                metadata.lifecycle_mode()
            ),
        ));
    }

    // Preflight 2: Reject unsupported knn_vector mappings.
    let vector_fields: Vec<String> = metadata
        .mappings
        .iter()
        .filter(|(_, m)| matches!(m.field_type, FieldType::KnnVector))
        .map(|(k, _)| k.clone())
        .collect();
    if !vector_fields.is_empty() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "cannot migrate index [{index_name}] to tiered lifecycle: knn_vector fields {vector_fields:?} are not supported by remote_store split readers"
            ),
        ));
    }

    // Preflight 3: Ensure object-store backend supports fenced conditional publication.
    if let Err(error) = state.storage_manager.ensure_publication_supported().await {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "remote_store_publish_exception",
            error,
        ));
    }

    let index_uuid = metadata.uuid.to_string();
    let requested_migration_id = body
        .as_ref()
        .and_then(|v| v.get("migration_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("mig-{index_uuid}"));
    let retire_sealed_base = body
        .as_ref()
        .and_then(|v| v.get("retire_sealed_base"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let fail_stage = body
        .as_ref()
        .and_then(|v| v.get("fail_stage"))
        .and_then(Value::as_str)
        .map(ToString::to_string);

    if let Some(existing) = metadata.migration_record() {
        if existing.phase != IndexMigrationPhase::MutableOnly
            && existing.migration_id != requested_migration_id
        {
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "migration_conflict_exception",
                format!(
                    "index [{index_name}] already has an active or committed migration_id [{}] in phase [{}]; cannot start conflicting migration_id [{requested_migration_id}]",
                    existing.migration_id, existing.phase
                ),
            ));
        }
        if metadata.is_tiered() && existing.phase == IndexMigrationPhase::Tiered {
            return Ok(serde_json::json!({
                "index": index_name,
                "index_uuid": index_uuid,
                "status": "replayed",
                "replayed": true,
                "migration_id": existing.migration_id,
                "lifecycle_mode": "tiered",
                "phase": existing.phase.as_str(),
                "base_manifest_generation": existing.base_manifest_generation,
                "bootstrap_watermarks": existing.bootstrap_watermarks,
                "equivalence_verified": existing.equivalence_verified,
                "fast_rollback_retained": existing.fast_rollback_retained,
            }));
        }
    }

    let base_policy = metadata
        .settings
        .remote_store
        .as_ref()
        .and_then(|rs| rs.hot_delta.clone())
        .unwrap_or_else(|| HotDeltaSealPolicy {
            enabled: false,
            ..HotDeltaSealPolicy::default()
        });

    // Phase 1: TieringBootstrap (enabled remains false so authoritative reads stay MutableOnly).
    let bootstrap_record = IndexMigrationRecord {
        migration_id: requested_migration_id.clone(),
        phase: IndexMigrationPhase::TieringBootstrap,
        source_mode: "mutable_only".to_string(),
        target_mode: "tiered".to_string(),
        base_manifest_generation: None,
        bootstrap_watermarks: BTreeMap::new(),
        equivalence_verified: false,
        fast_rollback_retained: true,
        updated_at_epoch_ms: now_epoch_ms(),
    };
    let mut bootstrap_policy = base_policy.clone();
    bootstrap_policy.enabled = false;
    bootstrap_policy.migration = Some(bootstrap_record);
    commit_hot_delta_policy_update(state, index_name, &index_uuid, None, Some(bootstrap_policy))
        .await?;

    let refreshed_meta = state
        .cluster_manager
        .get_state()
        .indices
        .get(index_name)
        .cloned()
        .ok_or_else(|| {
            crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{index_name}]"),
            )
        })?;

    // Seal contiguous base range without retiring local shard data so MutableOnly remains complete.
    let _seal_resp = seal_hot_delta_with_mode(
        state,
        index_name,
        &refreshed_meta,
        Some(serde_json::json!({
            "force": true,
            "retire_after_commit": false,
            "max_docs": 1_000_000,
            "max_bytes": 1_073_741_824u64
        })),
        true,
    )
    .await?;

    let expected_schema_hash = crate::storage::compute_schema_hash(&refreshed_meta.mappings);
    let mut current_manifest = state
        .storage_manager
        .load_current_manifest(&index_uuid, Some(&expected_schema_hash))
        .await
        .map_err(|e| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load bootstrap manifest: {e:#}"),
            )
        })?;

    // Fallback for shards whose older WAL generations were legitimately truncated after Tantivy flushes:
    // if a shard has committed live documents in its reader that are not yet covered by published_watermarks,
    // seal a base split directly from the committed reader + WAL tombstones!
    let local_shards_for_bootstrap = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        &refreshed_meta,
        "bootstrap reader fallback",
    )
    .await;
    if local_shards_for_bootstrap.len() != refreshed_meta.number_of_shards as usize {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!(
                "cannot bootstrap migration for index [{index_name}]: expected {} open local shards, found {}",
                refreshed_meta.number_of_shards,
                local_shards_for_bootstrap.len()
            ),
        ));
    }
    for (shard_id, engine) in &local_shards_for_bootstrap {
        let already_sealed = current_manifest
            .as_ref()
            .is_some_and(|m| m.published_watermarks.contains_key(shard_id));
        if already_sealed {
            continue;
        }
        let eng = engine.clone();
        let (live_res, del_res) = state
            .worker_pools
            .spawn_search(move || {
                let _ = eng.refresh();
                (
                    eng.live_documents_with_versions(),
                    eng.deleted_documents_with_versions(),
                )
            })
            .await
            .map_err(query_error_response)?;
        let live_docs_ver = live_res.unwrap_or_default();
        let del_docs_ver = del_res.unwrap_or_default();
        if live_docs_ver.is_empty() && del_docs_ver.is_empty() {
            continue;
        }
        let max_seq = live_docs_ver
            .iter()
            .map(|(_, _, s, _, _)| *s)
            .chain(del_docs_ver.iter().map(|(_, s, _, _)| *s))
            .max()
            .unwrap_or(0);
        let max_term = live_docs_ver
            .iter()
            .map(|(_, _, _, t, _)| *t)
            .chain(del_docs_ver.iter().map(|(_, _, t, _)| *t))
            .max()
            .unwrap_or(1)
            .max(1);
        let seal_range_meta = crate::storage::SplitSealRangeMetadata {
            shard_id: *shard_id,
            start_seq_no: 0,
            end_seq_no: max_seq,
            primary_term: max_term,
            doc_versions: live_docs_ver
                .iter()
                .map(|(id, _, s, t, v)| crate::storage::SplitDocVersionRecord {
                    doc_id: id.clone(),
                    seq_no: *s,
                    primary_term: *t,
                    version: *v,
                })
                .collect(),
            tombstones: del_docs_ver
                .iter()
                .map(|(id, s, t, v)| crate::storage::SplitTombstoneRecord {
                    doc_id: id.clone(),
                    seq_no: *s,
                    primary_term: *t,
                    version: *v,
                })
                .collect(),
        };
        let operation_id =
            deterministic_hot_delta_seal_operation_id(&index_uuid, *shard_id, 0, max_seq);
        let split_id =
            crate::storage::deterministic_publication_split_id(&index_uuid, &operation_id);
        let staging_dir = state.storage_manager.staging_dir(&index_uuid, &split_id);
        let docs_for_build: Vec<(String, Value)> = live_docs_ver
            .iter()
            .map(|(id, src, _, _, _)| (id.clone(), src.clone()))
            .collect();
        let raw_sources: Vec<Value> = live_docs_ver
            .iter()
            .map(|(_, src, _, _, _)| src.clone())
            .collect();
        let (field_ranges, field_terms) =
            build_split_field_summaries(&raw_sources, &refreshed_meta.mappings).map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    e,
                )
            })?;
        let mappings_arc = Arc::new(refreshed_meta.mappings.clone());
        let staging_clone = staging_dir.clone();
        let seal_meta_clone = seal_range_meta.clone();
        state
            .worker_pools
            .spawn_maintenance(
                "build_bootstrap_reader_split",
                move || -> anyhow::Result<()> {
                    if staging_clone.exists() {
                        std::fs::remove_dir_all(&staging_clone)?;
                    }
                    std::fs::create_dir_all(&staging_clone)?;
                    let col_cache = Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let split_engine =
                        crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                            &staging_clone,
                            Duration::from_secs(60),
                            mappings_arc.as_ref(),
                            col_cache,
                        )?;
                    if !docs_for_build.is_empty() {
                        split_engine.bulk_add_documents(docs_for_build)?;
                    }
                    split_engine.flush()?;
                    strip_split_wal(&staging_clone)?;
                    let versions_bytes = serde_json::to_vec_pretty(&seal_meta_clone)?;
                    std::fs::write(staging_clone.join("seal.versions.json"), versions_bytes)?;
                    Ok(())
                },
            )
            .await
            .map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    format!("failed to build bootstrap split from reader: {e:#}"),
                )
            })?;
        let (bundle_key, checksum, size_bytes) = state
            .storage_manager
            .upload_split_bundle(&index_uuid, &split_id, &staging_dir)
            .await
            .map_err(|e| {
                let _ = std::fs::remove_dir_all(&staging_dir);
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    format!("failed to upload bootstrap split: {e:#}"),
                )
            })?;
        let _ = std::fs::remove_dir_all(&staging_dir);
        let mut tags = BTreeMap::new();
        tags.insert("operation_id".to_string(), operation_id.clone());
        let mut new_split = crate::storage::RemoteSplitManifest {
            split_id: split_id.clone(),
            state: crate::storage::RemoteSplitState::Published,
            bundle_path: bundle_key.clone(),
            bundle_etag: None,
            hotcache_path: None,
            checksum,
            doc_count: live_docs_ver.len() as u64,
            size_bytes,
            uncompressed_bytes: size_bytes,
            hotcache_bytes: None,
            time_range: None,
            tags,
            field_ranges,
            field_terms,
        };
        new_split.set_seal_range(&seal_range_meta);
        let fp = sha256_prefixed(format!("bootstrap-{index_uuid}-{operation_id}").as_bytes());
        let now_ms = now_epoch_ms();
        let intent = crate::storage::PublicationIntentRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: fp.clone(),
            schema_generation: 1,
            schema_hash: expected_schema_hash.clone(),
            split_id: split_id.clone(),
            planned_bundle_path: bundle_key,
            doc_count: (live_docs_ver.len() + del_docs_ver.len()).max(1) as u64,
            producer_node_id: state.local_node_id.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            created_at_epoch_ms: now_ms,
        };
        let _ = state
            .storage_manager
            .stage_publication_intent(&intent)
            .await;
        let prep = crate::storage::PublicationPreparedRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: fp.clone(),
            schema_generation: 1,
            schema_hash: expected_schema_hash.clone(),
            producer_node_id: state.local_node_id.clone(),
            prepared_at: chrono::Utc::now().to_rfc3339(),
            prepared_at_epoch_ms: now_ms,
            split: new_split,
        };
        let _ = state
            .storage_manager
            .write_publication_prepared(&prep)
            .await;
        let seq_req = ForwardedSequencerCommitRequest {
            operation_id,
            request_fingerprint: fp,
            schema_generation: 1,
            schema_hash: expected_schema_hash.clone(),
            settings: refreshed_meta.settings.remote_store.clone(),
            known_authority: None,
        };
        let _ = coordinate_sequencer_commit(state, index_name, &index_uuid, &seq_req).await;
    }

    current_manifest = state
        .storage_manager
        .load_current_manifest(&index_uuid, Some(&expected_schema_hash))
        .await
        .map_err(|e| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load bootstrap manifest: {e:#}"),
            )
        })?;
    let base_generation = current_manifest.as_ref().map(|m| m.generation).unwrap_or(0);
    let bootstrap_watermarks: BTreeMap<u32, u64> = current_manifest
        .as_ref()
        .map(|m| {
            m.published_watermarks
                .iter()
                .map(|(sid, wm)| (*sid, wm.sealed_through_seq_no))
                .collect()
        })
        .unwrap_or_default();

    if fail_stage.as_deref() == Some("after_bootstrap_before_validate") {
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "migration_interrupted_exception",
            format!(
                "injected interruption after TieringBootstrap for migration_id [{requested_migration_id}]"
            ),
        ));
    }

    // Phase 2: TieringValidate — verify oracle equivalence while authoritative reads stay MutableOnly.
    let validate_record = IndexMigrationRecord {
        migration_id: requested_migration_id.clone(),
        phase: IndexMigrationPhase::TieringValidate,
        source_mode: "mutable_only".to_string(),
        target_mode: "tiered".to_string(),
        base_manifest_generation: (base_generation > 0).then_some(base_generation),
        bootstrap_watermarks: bootstrap_watermarks.clone(),
        equivalence_verified: false,
        fast_rollback_retained: true,
        updated_at_epoch_ms: now_epoch_ms(),
    };
    let mut validate_policy = base_policy.clone();
    validate_policy.enabled = false;
    validate_policy.migration = Some(validate_record);
    commit_hot_delta_policy_update(
        state,
        index_name,
        &index_uuid,
        Some(&requested_migration_id),
        Some(validate_policy),
    )
    .await?;

    let verified_docs =
        verify_tiered_vs_mutable_equivalence(state, index_name, &refreshed_meta).await?;

    if fail_stage.as_deref() == Some("after_validate_before_cutover") {
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "migration_interrupted_exception",
            format!(
                "injected interruption after TieringValidate for migration_id [{requested_migration_id}]"
            ),
        ));
    }

    // Phase 3: Conditional cutover to Tiered.
    let tiered_record = IndexMigrationRecord {
        migration_id: requested_migration_id.clone(),
        phase: IndexMigrationPhase::Tiered,
        source_mode: "mutable_only".to_string(),
        target_mode: "tiered".to_string(),
        base_manifest_generation: (base_generation > 0).then_some(base_generation),
        bootstrap_watermarks: bootstrap_watermarks.clone(),
        equivalence_verified: true,
        fast_rollback_retained: !retire_sealed_base,
        updated_at_epoch_ms: now_epoch_ms(),
    };
    let mut tiered_policy = base_policy;
    tiered_policy.enabled = true;
    tiered_policy.migration = Some(tiered_record);
    commit_hot_delta_policy_update(
        state,
        index_name,
        &index_uuid,
        Some(&requested_migration_id),
        Some(tiered_policy),
    )
    .await?;

    if retire_sealed_base
        && let Some(post_cutover_meta) = state
            .cluster_manager
            .get_state()
            .indices
            .get(index_name)
            .cloned()
    {
        let _ =
            reconcile_published_watermark_retirement(state, index_name, &post_cutover_meta).await;
    }

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": index_uuid,
        "status": "committed",
        "replayed": false,
        "migration_id": requested_migration_id,
        "previous_lifecycle_mode": "mutable_only",
        "lifecycle_mode": "tiered",
        "phase": "tiered",
        "base_manifest_generation": (base_generation > 0).then_some(base_generation),
        "bootstrap_watermarks": bootstrap_watermarks,
        "verified_live_docs": verified_docs,
        "equivalence_verified": true,
        "fast_rollback_retained": !retire_sealed_base,
    }))
}

/// POST /{index}/_lifecycle/rollback — Roll back an in-progress or completed
/// lifecycle migration to `MutableOnly`, supporting pre-cutover cancellation,
/// fast retained local rollback, and post-retirement base reconstruction (`FS-043`, ADR 0004 section 3.13).
pub(crate) async fn rollback_index_lifecycle(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !metadata.uses_local_shards() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!("index [{index_name}] does not have mutable local shards"),
        ));
    }

    let allow_reconstruction = body
        .as_ref()
        .and_then(|v| v.get("allow_reconstruction"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let expected_migration_id = body
        .as_ref()
        .and_then(|v| v.get("migration_id"))
        .and_then(Value::as_str);

    if let Some(expected_id) = expected_migration_id
        && metadata
            .migration_record()
            .is_none_or(|m| m.migration_id != expected_id)
    {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "migration_conflict_exception",
            format!("expected migration_id [{expected_id}] does not match current migration state"),
        ));
    }

    let index_uuid = metadata.uuid.to_string();
    let migration_id = metadata
        .migration_record()
        .map(|m| m.migration_id.clone())
        .unwrap_or_else(|| format!("mig-{index_uuid}"));

    let rolled_back_policy = {
        let mut p = metadata
            .settings
            .remote_store
            .as_ref()
            .and_then(|rs| rs.hot_delta.clone())
            .unwrap_or_default();
        p.enabled = false;
        p.migration = Some(IndexMigrationRecord {
            migration_id: migration_id.clone(),
            phase: IndexMigrationPhase::MutableOnly,
            source_mode: "tiered".to_string(),
            target_mode: "mutable_only".to_string(),
            base_manifest_generation: metadata
                .migration_record()
                .and_then(|m| m.base_manifest_generation),
            bootstrap_watermarks: metadata
                .migration_record()
                .map(|m| m.bootstrap_watermarks.clone())
                .unwrap_or_default(),
            equivalence_verified: true,
            fast_rollback_retained: false,
            updated_at_epoch_ms: now_epoch_ms(),
        });
        p
    };

    // Case 1: Pre-cutover cancellation (still in TieringBootstrap or TieringValidate).
    if !metadata.is_tiered() {
        commit_hot_delta_policy_update(
            state,
            index_name,
            &index_uuid,
            expected_migration_id,
            Some(rolled_back_policy),
        )
        .await?;
        return Ok(serde_json::json!({
            "index": index_name,
            "index_uuid": index_uuid,
            "status": "committed",
            "migration_id": migration_id,
            "rollback_kind": "cancelled_before_cutover",
            "lifecycle_mode": "mutable_only",
            "phase": "mutable_only",
            "reconstructed_docs": 0,
        }));
    }

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        metadata,
        "rollback index lifecycle",
    )
    .await;
    if local_shards.len() != metadata.number_of_shards as usize {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception",
            format!(
                "cannot roll back lifecycle for index [{index_name}]: expected {} open local shards, found {}",
                metadata.number_of_shards,
                local_shards.len()
            ),
        ));
    }

    let mut any_retired = false;
    for (_sid, engine) in &local_shards {
        if let Ok(wm) = engine.seal_watermark_state()
            && wm.retired_through_seq_no.is_some()
        {
            any_retired = true;
            break;
        }
    }

    // Case 2: Fast rollback window (local shards still retain all unretired base documents).
    if !any_retired {
        let verified_docs =
            verify_tiered_vs_mutable_equivalence(state, index_name, metadata).await?;
        for (_sid, engine) in &local_shards {
            let eng = engine.clone();
            let _ = state
                .worker_pools
                .spawn_maintenance("reset_fast_rollback_watermarks", move || {
                    eng.restore_documents_with_versions(
                        &[],
                        &[],
                        Some(SealWatermarkState::default()),
                    )
                })
                .await;
        }
        commit_hot_delta_policy_update(
            state,
            index_name,
            &index_uuid,
            expected_migration_id,
            Some(rolled_back_policy),
        )
        .await?;
        return Ok(serde_json::json!({
            "index": index_name,
            "index_uuid": index_uuid,
            "status": "committed",
            "migration_id": migration_id,
            "rollback_kind": "fast_retained_local",
            "lifecycle_mode": "mutable_only",
            "phase": "mutable_only",
            "verified_live_docs": verified_docs,
            "reconstructed_docs": 0,
        }));
    }

    // Case 3: Post-retirement rollback — an immediate metadata-only flip is forbidden (ADR 0004 section 3.13).
    if !allow_reconstruction {
        return Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "illegal_state_exception",
            format!(
                "cannot perform immediate metadata-only rollback for index [{index_name}] after local base retirement; set allow_reconstruction=true to rebuild local mutable state before switching to mutable_only"
            ),
        ));
    }

    let mut prep_policy = metadata
        .settings
        .remote_store
        .as_ref()
        .and_then(|rs| rs.hot_delta.clone())
        .unwrap_or_default();
    prep_policy.enabled = true;
    prep_policy.migration = Some(IndexMigrationRecord {
        migration_id: migration_id.clone(),
        phase: IndexMigrationPhase::MutableRollbackPreparing,
        source_mode: "tiered".to_string(),
        target_mode: "mutable_only".to_string(),
        base_manifest_generation: metadata
            .migration_record()
            .and_then(|m| m.base_manifest_generation),
        bootstrap_watermarks: metadata
            .migration_record()
            .map(|m| m.bootstrap_watermarks.clone())
            .unwrap_or_default(),
        equivalence_verified: false,
        fast_rollback_retained: false,
        updated_at_epoch_ms: now_epoch_ms(),
    });
    commit_hot_delta_policy_update(
        state,
        index_name,
        &index_uuid,
        expected_migration_id,
        Some(prep_policy),
    )
    .await?;

    // Reconstruct retired base documents from published splits + current hot delta.
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let current_manifest = state
        .storage_manager
        .load_current_manifest(&index_uuid, Some(&expected_schema_hash))
        .await
        .map_err(|e| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load manifest for rollback reconstruction: {e:#}"),
            )
        })?
        .ok_or_else(|| {
            crate::api::error_response(
                StatusCode::CONFLICT,
                "remote_store_manifest_exception",
                format!(
                    "index [{index_name}] has retired local base data but no committed manifest"
                ),
            )
        })?;

    // Hydrate every published split and extract its stored documents + versions/tombstones.
    let mut split_live_by_shard: HashMap<u32, Vec<LiveDocumentVersionTuple>> = HashMap::new();
    let mut split_tomb_by_shard: HashMap<u32, Vec<DeletedDocumentVersionTuple>> = HashMap::new();
    let num_shards = metadata.number_of_shards.max(1);

    for split in current_manifest.published_splits() {
        let split_dir = state
            .storage_manager
            .fetch_split_into_cache(&index_uuid, split)
            .await
            .map_err(|e| {
                crate::api::error_response(
                    StatusCode::CONFLICT,
                    "remote_store_manifest_exception",
                    format!(
                        "failed to hydrate split [{}] during rollback reconstruction: {e:#}",
                        split.split_id
                    ),
                )
            })?;
        let target_shard = split
            .seal_range()
            .ok()
            .flatten()
            .map(|r| r.shard_id)
            .unwrap_or(0)
            % num_shards;
        let dv_map: HashMap<String, (u64, u64, u64)> = split
            .doc_versions()
            .unwrap_or_default()
            .into_iter()
            .map(|dv| (dv.doc_id, (dv.seq_no, dv.primary_term, dv.version)))
            .collect();
        let tombstones: Vec<DeletedDocumentVersionTuple> = split
            .tombstones()
            .unwrap_or_default()
            .into_iter()
            .map(|ts| (ts.doc_id, ts.seq_no, ts.primary_term, ts.version))
            .collect();

        let mappings_clone = metadata.mappings.clone();
        let split_docs = state
            .worker_pools
            .spawn_search(move || -> anyhow::Result<Vec<(String, Value)>> {
                let col_cache = Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                let split_engine =
                    crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                        &split_dir,
                        Duration::from_secs(60),
                        &mappings_clone,
                        col_cache,
                    )?;
                let docs = split_engine.live_documents_with_versions()?;
                Ok(docs
                    .into_iter()
                    .map(|(id, src, _, _, _)| (id, src))
                    .collect())
            })
            .await
            .map_err(query_error_response)?
            .map_err(query_error_response)?;

        let shard_live = split_live_by_shard.entry(target_shard).or_default();
        for (doc_id, src) in split_docs {
            let (seq_no, term, ver) = dv_map.get(&doc_id).copied().unwrap_or((0, 1, 1));
            shard_live.push((doc_id, src, seq_no, term, ver));
        }
        split_tomb_by_shard
            .entry(target_shard)
            .or_default()
            .extend(tombstones);
    }

    let mut total_reconstructed_docs = 0usize;
    for (shard_id, engine) in &local_shards {
        let eng = engine.clone();
        let (hot_live, hot_del) = state
            .worker_pools
            .spawn_search(move || {
                (
                    eng.live_documents_with_versions(),
                    eng.deleted_documents_with_versions(),
                )
            })
            .await
            .map_err(query_error_response)?;

        let mut normalized_live: BTreeMap<String, (Value, u64, u64, u64)> = BTreeMap::new();
        let mut winners_del: BTreeMap<String, DeletedDocumentVersionTuple> = BTreeMap::new();

        for (doc_id, src, seq, term, ver) in
            split_live_by_shard.remove(shard_id).unwrap_or_default()
        {
            let update = normalized_live
                .get(&doc_id)
                .is_none_or(|(_, s, t, v)| (seq, term, ver) >= (*s, *t, *v));
            if update {
                normalized_live.insert(doc_id, (src, seq, term, ver));
            }
        }
        for (doc_id, seq, term, ver) in split_tomb_by_shard.remove(shard_id).unwrap_or_default() {
            if let Some((_, l_seq, l_term, l_ver)) = normalized_live.get(&doc_id)
                && (*l_seq, *l_term, *l_ver) > (seq, term, ver)
            {
                continue;
            }
            normalized_live.remove(&doc_id);
            let update = winners_del
                .get(&doc_id)
                .is_none_or(|(_, s, t, v)| (seq, term, ver) >= (*s, *t, *v));
            if update {
                winners_del.insert(doc_id.clone(), (doc_id, seq, term, ver));
            }
        }
        if let Ok(hl) = hot_live {
            for (doc_id, src, seq, term, ver) in hl {
                if let Some((_, d_seq, d_term, d_ver)) = winners_del.get(&doc_id)
                    && (*d_seq, *d_term, *d_ver) >= (seq, term, ver)
                {
                    continue;
                }
                winners_del.remove(&doc_id);
                let update = normalized_live
                    .get(&doc_id)
                    .is_none_or(|(_, s, t, v)| (seq, term, ver) >= (*s, *t, *v));
                if update {
                    normalized_live.insert(doc_id, (src, seq, term, ver));
                }
            }
        }
        if let Ok(hd) = hot_del {
            for (doc_id, seq, term, ver) in hd {
                if let Some((_, l_seq, l_term, l_ver)) = normalized_live.get(&doc_id)
                    && (*l_seq, *l_term, *l_ver) > (seq, term, ver)
                {
                    continue;
                }
                normalized_live.remove(&doc_id);
                let update = winners_del
                    .get(&doc_id)
                    .is_none_or(|(_, s, t, v)| (seq, term, ver) >= (*s, *t, *v));
                if update {
                    winners_del.insert(doc_id.clone(), (doc_id, seq, term, ver));
                }
            }
        }

        let restore_live: Vec<LiveDocumentVersionTuple> = normalized_live
            .into_iter()
            .map(|(doc_id, (src, seq, term, ver))| (doc_id, src, seq, term, ver))
            .collect();
        let restore_del: Vec<DeletedDocumentVersionTuple> = winners_del.into_values().collect();
        let eng_restore = engine.clone();
        let count = state
            .worker_pools
            .spawn_maintenance("rollback_reconstruct_shard", move || {
                eng_restore.restore_documents_with_versions(
                    &restore_live,
                    &restore_del,
                    Some(SealWatermarkState::default()),
                )
            })
            .await
            .map_err(|e| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "lifecycle_rollback_exception",
                    format!("failed to reconstruct local shard [{index_name}][{shard_id}]: {e:#}"),
                )
            })?;
        total_reconstructed_docs += count;
    }

    // Verify equivalence between reconstructed local shards and Tiered snapshot before switching metadata!
    let verified_docs = verify_tiered_vs_mutable_equivalence(state, index_name, metadata).await?;

    commit_hot_delta_policy_update(
        state,
        index_name,
        &index_uuid,
        Some(&migration_id),
        Some(rolled_back_policy),
    )
    .await?;

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": index_uuid,
        "status": "committed",
        "migration_id": migration_id,
        "rollback_kind": "reconstructed_from_splits",
        "lifecycle_mode": "mutable_only",
        "phase": "mutable_only",
        "verified_live_docs": verified_docs,
        "reconstructed_docs": total_reconstructed_docs,
    }))
}

/// GET /{index}/_lifecycle/status — Inspect the index's lifecycle mode,
/// migration authority record, per-shard seal watermarks, and manifest state (`FS-043`).
pub(crate) async fn get_index_lifecycle_status(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let index_uuid = metadata.uuid.to_string();
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let current_manifest = state
        .storage_manager
        .load_current_manifest(&index_uuid, Some(&expected_schema_hash))
        .await
        .ok()
        .flatten();

    let mut shard_watermarks = BTreeMap::new();
    if metadata.uses_local_shards() {
        let local_shards = crate::api::index::ensure_local_index_shards_open(
            state,
            index_name,
            metadata,
            "inspect lifecycle status",
        )
        .await;
        for (shard_id, engine) in local_shards {
            if let Ok(wm) = engine.seal_watermark_state() {
                shard_watermarks.insert(
                    shard_id,
                    serde_json::json!({
                        "sealed_through_seq_no": wm.sealed_through_seq_no,
                        "retired_through_seq_no": wm.retired_through_seq_no,
                        "last_sealed_generation": wm.last_sealed_generation,
                    }),
                );
            }
        }
    }

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": index_uuid,
        "engine": metadata.settings.engine.to_string(),
        "lifecycle_mode": metadata.lifecycle_mode().as_str(),
        "migration": metadata.migration_record(),
        "manifest_generation": current_manifest.as_ref().map(|m| m.generation).unwrap_or(0),
        "published_splits": current_manifest.as_ref().map(|m| m.published_splits().count()).unwrap_or(0),
        "shard_watermarks": shard_watermarks,
    }))
}
