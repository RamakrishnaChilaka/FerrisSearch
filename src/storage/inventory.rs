//! Non-mutating object-store reachability inventory for `remote_store` indices
//! (ADR 0002 section 3.5/3.9, ADR 0003 section 3.9, FS-020).

use anyhow::Result;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::publication::{
    MAX_LINEAGE_WALK_GENERATIONS, PublicationAttemptOutcome, PublicationAttemptRecord,
    PublicationCommittedRecord, PublicationIntentRecord, PublicationPreparedRecord,
};
use super::{RemoteStoreManifest, StorageManager, manifest_pointer_key, now_epoch_ms};

pub const DEFAULT_INVENTORY_RETENTION_GENERATIONS: usize = 16;
pub const DEFAULT_INVENTORY_RETENTION_HORIZON_MS: u64 = 300_000;
pub const DEFAULT_INVENTORY_ACTIVE_GRACE_MS: u64 = 60_000;
pub const DEFAULT_INVENTORY_MAX_REPORTED_OBJECTS: usize = 10_000;

/// Classification of an object in the remote-store reachability inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectReachabilityClass {
    Committed,
    Retained,
    Pinned,
    Staging,
    Prepared,
    Unresolved,
    Superseded,
    Abandoned,
    Unknown,
    Corrupt,
}

/// Kind of object discovered under `<index_uuid>/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryObjectKind {
    ManifestPointer,
    ManifestGeneration,
    SplitBundle,
    PublicationIntent,
    PublicationPrepared,
    PublicationCommitted,
    PublicationAttempt,
    JanitorState,
    Unknown,
}

/// Options controlling reachability classification and retention windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReachabilityInventoryOptions {
    pub index_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    pub retention_generations: usize,
    pub retention_horizon_ms: u64,
    pub active_grace_ms: u64,
    pub now_epoch_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer_authority: Option<crate::cluster::state::ManifestWriterAuthority>,
    #[serde(default)]
    pub pinned_split_ids: HashSet<String>,
    #[serde(default)]
    pub pinned_generations: HashSet<u64>,
    pub max_reported_objects: usize,
    #[serde(default)]
    pub force_partial_listing_for_test: bool,
}

impl ReachabilityInventoryOptions {
    pub fn for_index(index_uuid: impl Into<String>, index_name: Option<String>) -> Self {
        Self {
            index_uuid: index_uuid.into(),
            index_name,
            retention_generations: DEFAULT_INVENTORY_RETENTION_GENERATIONS,
            retention_horizon_ms: DEFAULT_INVENTORY_RETENTION_HORIZON_MS,
            active_grace_ms: DEFAULT_INVENTORY_ACTIVE_GRACE_MS,
            now_epoch_ms: now_epoch_ms(),
            active_writer_authority: None,
            pinned_split_ids: HashSet::new(),
            pinned_generations: HashSet::new(),
            max_reported_objects: DEFAULT_INVENTORY_MAX_REPORTED_OBJECTS,
            force_partial_listing_for_test: false,
        }
    }
}

/// Machine-readable classification entry for one object in object storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryObjectEntry {
    pub path: String,
    pub kind: InventoryObjectKind,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_epoch_ms: Option<u64>,
    pub classification: ObjectReachabilityClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub pinned: bool,
    pub reclaimable: bool,
    pub reason: String,
}

/// Aggregate object and byte counts across the scanned index namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventorySummaryCounts {
    pub total_objects: usize,
    pub total_bytes: u64,
    pub reclaimable_objects: usize,
    pub reclaimable_bytes: u64,
    pub by_classification: BTreeMap<String, usize>,
}

/// Complete bounded reachability report for one `remote_store` index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachabilityInventoryReport {
    pub index_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    pub scanned_at_epoch_ms: u64,
    pub current_generation: u64,
    pub pointer_revision: u64,
    pub pointer_writer_epoch: u64,
    pub listing_complete: bool,
    pub truncated: bool,
    pub lineage_healthy: bool,
    pub has_unresolved_publications: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lineage_errors: Vec<String>,
    pub summary: InventorySummaryCounts,
    pub objects: Vec<InventoryObjectEntry>,
}

#[derive(Debug, Clone)]
struct DiscoveredObject {
    path: String,
    size_bytes: u64,
    last_modified_epoch_ms: Option<u64>,
}

fn parse_rfc3339_epoch_ms(ts: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp_millis()).ok())
}

fn classify_key_shape(
    index_uuid: &str,
    path: &str,
) -> (
    InventoryObjectKind,
    Option<String>,
    Option<String>,
    Option<u64>,
) {
    if path == manifest_pointer_key(index_uuid) {
        return (InventoryObjectKind::ManifestPointer, None, None, None);
    }
    let janitor_prefix = format!("{index_uuid}/janitor/");
    if path.starts_with(&janitor_prefix) {
        return (InventoryObjectKind::JanitorState, None, None, None);
    }
    let manifests_prefix = format!("{index_uuid}/manifests/");
    if let Some(rest) = path.strip_prefix(&manifests_prefix) {
        if let Some(gen_str) = rest.strip_suffix(".json")
            && !gen_str.contains('/')
            && let Ok(generation) = gen_str.parse::<u64>()
        {
            return (
                InventoryObjectKind::ManifestGeneration,
                None,
                None,
                Some(generation),
            );
        }
        if let Some((gen_str, file_part)) = rest.split_once('/')
            && let Some(stem) = file_part.strip_suffix(".json")
            && !stem.is_empty()
            && !stem.contains('/')
            && let Ok(generation) = gen_str.parse::<u64>()
        {
            let op_id = match stem.rsplit_once("--epoch-") {
                Some((op, epoch_str))
                    if !op.is_empty()
                        && epoch_str.len() == 12
                        && epoch_str.chars().all(|c| c.is_ascii_digit()) =>
                {
                    op
                }
                _ => stem,
            };
            return (
                InventoryObjectKind::ManifestGeneration,
                Some(op_id.to_string()),
                None,
                Some(generation),
            );
        }
    }
    let splits_prefix = format!("{index_uuid}/splits/");
    if let Some(rest) = path.strip_prefix(&splits_prefix) {
        let split_id_opt = rest
            .strip_suffix("/bundle")
            .or_else(|| rest.strip_suffix("/hotcache"))
            .or_else(|| rest.strip_suffix("/hotcache.bin"));
        if let Some(split_id) = split_id_opt
            && !split_id.is_empty()
            && !split_id.contains('/')
        {
            return (
                InventoryObjectKind::SplitBundle,
                None,
                Some(split_id.to_string()),
                None,
            );
        }
    }
    let staged_prefix = format!("{index_uuid}/staged/");
    if let Some(rest) = path.strip_prefix(&staged_prefix)
        && let Some(op_id) = rest.strip_suffix("/bundle")
        && !op_id.is_empty()
        && !op_id.contains('/')
    {
        return (
            InventoryObjectKind::SplitBundle,
            Some(op_id.to_string()),
            Some(op_id.to_string()),
            None,
        );
    }
    let pubs_prefix = format!("{index_uuid}/publications/");
    if let Some(rest) = path.strip_prefix(&pubs_prefix)
        && let Some((op_id, file_name)) = rest.split_once('/')
        && !op_id.is_empty()
        && !file_name.contains('/')
    {
        match file_name {
            "intent.json" => {
                return (
                    InventoryObjectKind::PublicationIntent,
                    Some(op_id.to_string()),
                    None,
                    None,
                );
            }
            "prepared.json" => {
                return (
                    InventoryObjectKind::PublicationPrepared,
                    Some(op_id.to_string()),
                    None,
                    None,
                );
            }
            "committed.json" => {
                return (
                    InventoryObjectKind::PublicationCommitted,
                    Some(op_id.to_string()),
                    None,
                    None,
                );
            }
            _ => {}
        }
    }
    let attempts_prefix = format!("{index_uuid}/publication-attempts/");
    if let Some(rest) = path.strip_prefix(&attempts_prefix)
        && let Some((op_id, file_name)) = rest.split_once('/')
        && !op_id.is_empty()
        && file_name.ends_with(".json")
        && !file_name.contains('/')
    {
        return (
            InventoryObjectKind::PublicationAttempt,
            Some(op_id.to_string()),
            None,
            None,
        );
    }
    (InventoryObjectKind::Unknown, None, None, None)
}

impl StorageManager {
    pub(crate) fn locally_pinned_splits_for_index(&self, index_uuid: &str) -> HashSet<String> {
        let prefix = format!("{index_uuid}/");
        self.pin_counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, count)| **count > 0)
            .filter_map(|(key, _)| key.strip_prefix(&prefix).map(ToString::to_string))
            .collect()
    }

    /// Enumerate object-store state for `options.index_uuid`, walk the current
    /// pointer and retained manifest parent chain, inspect operation/attempt
    /// records and active pins/leases, and emit a bounded non-mutating
    /// [`ReachabilityInventoryReport`].
    pub async fn build_reachability_inventory(
        &self,
        options: &ReachabilityInventoryOptions,
    ) -> Result<ReachabilityInventoryReport> {
        let index_uuid = options.index_uuid.as_str();
        if index_uuid.is_empty() {
            anyhow::bail!("reachability inventory requires non-empty index_uuid");
        }
        let prefix_path = self.object_path(index_uuid)?;
        let mut stream = self.object_store().list(Some(&prefix_path));
        let mut discovered = Vec::new();
        let mut list_error: Option<String> = None;

        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    let path_str = meta.location.to_string();
                    // Ignore internal local-FS conditional companion files if any surface.
                    if path_str.contains(".ferris.") && path_str.ends_with("#0") {
                        continue;
                    }
                    let last_modified_epoch_ms =
                        u64::try_from(meta.last_modified.timestamp_millis()).ok();
                    discovered.push(DiscoveredObject {
                        path: path_str,
                        size_bytes: meta.size,
                        last_modified_epoch_ms,
                    });
                }
                Err(error) => {
                    list_error = Some(error.to_string());
                    break;
                }
            }
        }
        crate::metrics::record_object_store_request(
            "list",
            if list_error.is_none() {
                "success"
            } else {
                "error"
            },
        );
        discovered.sort_by(|a, b| a.path.cmp(&b.path));

        let mut lineage_healthy = list_error.is_none() && !options.force_partial_listing_for_test;
        let mut lineage_errors = Vec::new();
        if let Some(err) = list_error {
            lineage_errors.push(format!("object-store list failed partway: {err}"));
        }
        if options.force_partial_listing_for_test {
            lineage_errors.push("forced partial object listing for test".into());
        }

        let mut pinned_splits = options.pinned_split_ids.clone();
        pinned_splits.extend(self.locally_pinned_splits_for_index(index_uuid));
        let mut effective_pinned_generations = options.pinned_generations.clone();
        for lease in self.active_snapshot_leases_for_index(index_uuid, options.now_epoch_ms) {
            if lease.manifest_generation > 0 {
                effective_pinned_generations.insert(lease.manifest_generation);
            }
            pinned_splits.extend(lease.pinned_split_ids);
        }
        if let Ok(snapshots_prefix) = self.object_path("_snapshots") {
            let mut snap_stream = self.object_store().list(Some(&snapshots_prefix));
            let suffix = format!("/indices/{index_uuid}/manifest.json");
            while let Some(Ok(meta)) = snap_stream.next().await {
                let loc = meta.location.to_string();
                if loc.ends_with(&suffix)
                    && let Ok(bytes) = self.get_bytes(&meta.location).await
                    && let Ok(snap_manifest) = serde_json::from_slice::<RemoteStoreManifest>(&bytes)
                {
                    if snap_manifest.generation > 0 {
                        effective_pinned_generations.insert(snap_manifest.generation);
                    }
                    for s in snap_manifest.published_splits() {
                        pinned_splits.insert(s.split_id.clone());
                    }
                }
            }
        }

        // 1. Load and validate current pointer + walk ancestor manifest lineage.
        let pointer_key = manifest_pointer_key(index_uuid);
        let mut current_generation = 0u64;
        let mut pointer_revision = 0u64;
        let mut pointer_writer_epoch = 0u64;
        let mut current_manifest_path: Option<String> = None;
        let mut retained_manifest_paths: HashMap<String, u64> = HashMap::new();
        let mut expired_ancestor_manifest_paths: HashMap<String, u64> = HashMap::new();
        let mut committed_split_paths: HashSet<String> = HashSet::new();
        let mut retained_split_paths: HashSet<String> = HashSet::new();
        let mut compacted_split_paths: HashSet<String> = HashSet::new();
        let mut pinned_generation_split_paths: HashSet<String> = HashSet::new();
        let mut committed_operation_ids: HashSet<String> = HashSet::new();
        let mut retained_operation_ids: HashSet<String> = HashSet::new();
        let mut corrupt_paths: HashMap<String, String> = HashMap::new();

        if discovered.iter().any(|obj| obj.path == pointer_key) {
            match self.load_manifest_pointer(index_uuid).await {
                Ok(Some(pointer)) => {
                    current_generation = pointer.current_generation;
                    pointer_revision = pointer.pointer_revision;
                    pointer_writer_epoch = pointer.writer_epoch;
                    if pointer.current_generation > 0 {
                        current_manifest_path = Some(pointer.manifest_path.clone());
                        let mut next_path = Some(pointer.manifest_path.clone());
                        let mut expected_gen = pointer.current_generation;
                        let mut depth = 0usize;

                        while let Some(m_path) = next_path.take() {
                            depth += 1;
                            if depth > MAX_LINEAGE_WALK_GENERATIONS {
                                lineage_healthy = false;
                                lineage_errors.push(format!(
                                    "manifest lineage exceeded {MAX_LINEAGE_WALK_GENERATIONS} generations"
                                ));
                                break;
                            }
                            match self
                                .load_manifest_at_path(index_uuid, &m_path, expected_gen, None)
                                .await
                            {
                                Ok((manifest, checksum)) => {
                                    if depth == 1
                                        && let Err(err) = pointer
                                            .validate_manifest_agreement(&manifest, &checksum)
                                    {
                                        lineage_healthy = false;
                                        lineage_errors.push(err.to_string());
                                        corrupt_paths.insert(pointer_key.clone(), err.to_string());
                                    }
                                    let published_ms =
                                        parse_rfc3339_epoch_ms(&manifest.published_at)
                                            .unwrap_or(options.now_epoch_ms);
                                    let age_ms = options.now_epoch_ms.saturating_sub(published_ms);
                                    let within_retention = depth == 1
                                        || depth <= options.retention_generations.saturating_add(1)
                                        || age_ms <= options.retention_horizon_ms
                                        || effective_pinned_generations
                                            .contains(&manifest.generation);

                                    if depth == 1 {
                                        if !manifest.operation_id.is_empty() {
                                            committed_operation_ids
                                                .insert(manifest.operation_id.clone());
                                        }
                                        for split in &manifest.splits {
                                            if matches!(
                                                split.state,
                                                super::RemoteSplitState::Published
                                            ) {
                                                committed_split_paths
                                                    .insert(split.bundle_path.clone());
                                                if let Some(ref hc) = split.hotcache_path {
                                                    committed_split_paths.insert(hc.clone());
                                                }
                                                if let Some(op_id) = split.tags.get("operation_id")
                                                {
                                                    committed_operation_ids.insert(op_id.clone());
                                                }
                                            } else {
                                                compacted_split_paths
                                                    .insert(split.bundle_path.clone());
                                                if let Some(ref hc) = split.hotcache_path {
                                                    compacted_split_paths.insert(hc.clone());
                                                }
                                                if let Some(op_id) = split.tags.get("operation_id")
                                                {
                                                    retained_operation_ids.insert(op_id.clone());
                                                }
                                            }
                                        }
                                        for lineage_rec in &manifest.compaction_lineage {
                                            retained_operation_ids
                                                .insert(lineage_rec.operation_id.clone());
                                        }
                                    } else if within_retention {
                                        retained_manifest_paths
                                            .insert(m_path.clone(), manifest.generation);
                                        if !manifest.operation_id.is_empty() {
                                            retained_operation_ids
                                                .insert(manifest.operation_id.clone());
                                        }
                                        for split in &manifest.splits {
                                            if matches!(
                                                split.state,
                                                super::RemoteSplitState::Published
                                            ) {
                                                retained_split_paths
                                                    .insert(split.bundle_path.clone());
                                                if let Some(ref hc) = split.hotcache_path {
                                                    retained_split_paths.insert(hc.clone());
                                                }
                                                if effective_pinned_generations
                                                    .contains(&manifest.generation)
                                                {
                                                    pinned_generation_split_paths
                                                        .insert(split.bundle_path.clone());
                                                    if let Some(ref hc) = split.hotcache_path {
                                                        pinned_generation_split_paths
                                                            .insert(hc.clone());
                                                    }
                                                }
                                            } else {
                                                compacted_split_paths
                                                    .insert(split.bundle_path.clone());
                                                if let Some(ref hc) = split.hotcache_path {
                                                    compacted_split_paths.insert(hc.clone());
                                                }
                                            }
                                            if let Some(op_id) = split.tags.get("operation_id") {
                                                retained_operation_ids.insert(op_id.clone());
                                            }
                                        }
                                        for lineage_rec in &manifest.compaction_lineage {
                                            retained_operation_ids
                                                .insert(lineage_rec.operation_id.clone());
                                        }
                                    } else {
                                        expired_ancestor_manifest_paths
                                            .insert(m_path.clone(), manifest.generation);
                                        if !manifest.operation_id.is_empty() {
                                            retained_operation_ids
                                                .insert(manifest.operation_id.clone());
                                        }
                                        for split in &manifest.splits {
                                            compacted_split_paths.insert(split.bundle_path.clone());
                                            if let Some(ref hc) = split.hotcache_path {
                                                compacted_split_paths.insert(hc.clone());
                                            }
                                            if let Some(op_id) = split.tags.get("operation_id") {
                                                retained_operation_ids.insert(op_id.clone());
                                            }
                                        }
                                        for lineage_rec in &manifest.compaction_lineage {
                                            retained_operation_ids
                                                .insert(lineage_rec.operation_id.clone());
                                        }
                                    }

                                    if manifest.parent_generation == 0 {
                                        break;
                                    }
                                    expected_gen = manifest.parent_generation;
                                    next_path = manifest.parent_manifest_path.clone();
                                    if next_path.is_none() {
                                        lineage_healthy = false;
                                        lineage_errors.push(format!(
                                            "manifest generation {} is missing parent_manifest_path for parent_generation {}",
                                            manifest.generation, manifest.parent_generation
                                        ));
                                    }
                                }
                                Err(err) => {
                                    lineage_healthy = false;
                                    let msg = format!(
                                        "failed to load ancestor manifest '{m_path}' at generation {expected_gen}: {err}"
                                    );
                                    lineage_errors.push(msg.clone());
                                    corrupt_paths.insert(m_path, msg);
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    lineage_healthy = false;
                    let msg = format!("corrupt or unsupported manifest pointer: {err}");
                    lineage_errors.push(msg.clone());
                    corrupt_paths.insert(pointer_key.clone(), msg);
                }
            }
        }

        // 2. Decode operation records and non-chain candidate manifests to build operation context.
        let mut intents_by_op: HashMap<String, PublicationIntentRecord> = HashMap::new();
        let mut prepared_by_op: HashMap<String, PublicationPreparedRecord> = HashMap::new();
        let mut committed_records_by_op: HashMap<String, PublicationCommittedRecord> =
            HashMap::new();
        let mut attempts_by_op: HashMap<String, Vec<PublicationAttemptRecord>> = HashMap::new();
        let mut candidate_manifests_by_path: HashMap<String, RemoteStoreManifest> = HashMap::new();

        for obj in &discovered {
            if corrupt_paths.contains_key(&obj.path) {
                continue;
            }
            let (kind, op_id_opt, _, gen_opt) = classify_key_shape(index_uuid, &obj.path);
            match kind {
                InventoryObjectKind::PublicationIntent => {
                    let Ok(loc) = self.object_path(&obj.path) else {
                        continue;
                    };
                    match self.get_bytes(&loc).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PublicationIntentRecord>(&bytes) {
                                Ok(rec) if rec.validate(index_uuid).is_ok() => {
                                    intents_by_op.insert(rec.operation_id.clone(), rec);
                                }
                                Ok(_) | Err(_) => {
                                    corrupt_paths.insert(
                                        obj.path.clone(),
                                        "malformed or invalid publication intent.json".into(),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            corrupt_paths.insert(obj.path.clone(), err.to_string());
                        }
                    }
                }
                InventoryObjectKind::PublicationPrepared => {
                    let Ok(loc) = self.object_path(&obj.path) else {
                        continue;
                    };
                    match self.get_bytes(&loc).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PublicationPreparedRecord>(&bytes) {
                                Ok(rec) if rec.validate(index_uuid).is_ok() => {
                                    prepared_by_op.insert(rec.operation_id.clone(), rec);
                                }
                                Ok(_) | Err(_) => {
                                    corrupt_paths.insert(
                                        obj.path.clone(),
                                        "malformed or invalid publication prepared.json".into(),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            corrupt_paths.insert(obj.path.clone(), err.to_string());
                        }
                    }
                }
                InventoryObjectKind::PublicationCommitted => {
                    let Ok(loc) = self.object_path(&obj.path) else {
                        continue;
                    };
                    match self.get_bytes(&loc).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PublicationCommittedRecord>(&bytes) {
                                Ok(rec) if rec.validate(index_uuid).is_ok() => {
                                    committed_records_by_op.insert(rec.operation_id.clone(), rec);
                                }
                                Ok(_) | Err(_) => {
                                    corrupt_paths.insert(
                                        obj.path.clone(),
                                        "malformed or invalid publication committed.json".into(),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            corrupt_paths.insert(obj.path.clone(), err.to_string());
                        }
                    }
                }
                InventoryObjectKind::PublicationAttempt => {
                    let Ok(loc) = self.object_path(&obj.path) else {
                        continue;
                    };
                    match self.get_bytes(&loc).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PublicationAttemptRecord>(&bytes) {
                                Ok(rec)
                                    if rec.index_uuid == index_uuid
                                        && op_id_opt.as_deref()
                                            == Some(rec.operation_id.as_str()) =>
                                {
                                    attempts_by_op
                                        .entry(rec.operation_id.clone())
                                        .or_default()
                                        .push(rec);
                                }
                                Ok(_) | Err(_) => {
                                    corrupt_paths.insert(
                                        obj.path.clone(),
                                        "malformed or invalid publication attempt record".into(),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            corrupt_paths.insert(obj.path.clone(), err.to_string());
                        }
                    }
                }
                InventoryObjectKind::ManifestGeneration => {
                    if current_manifest_path.as_deref() == Some(obj.path.as_str())
                        || retained_manifest_paths.contains_key(&obj.path)
                        || expired_ancestor_manifest_paths.contains_key(&obj.path)
                    {
                        continue;
                    }
                    let Some(expected_gen) = gen_opt else {
                        continue;
                    };
                    match self
                        .load_manifest_at_path(index_uuid, &obj.path, expected_gen, None)
                        .await
                    {
                        Ok((manifest, _)) => {
                            candidate_manifests_by_path.insert(obj.path.clone(), manifest);
                        }
                        Err(err) => {
                            corrupt_paths.insert(obj.path.clone(), err.to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        let active_epoch = options.active_writer_authority.as_ref().and_then(|auth| {
            if options.now_epoch_ms < auth.lease_expires_at_millis {
                auth.phase.writer_epoch()
            } else {
                None
            }
        });

        let mut unresolved_ops: HashSet<String> = HashSet::new();
        for (op_id, attempts) in &attempts_by_op {
            if committed_operation_ids.contains(op_id) || retained_operation_ids.contains(op_id) {
                continue;
            }
            let max_terminal_epoch = attempts
                .iter()
                .filter(|a| {
                    matches!(
                        a.outcome,
                        PublicationAttemptOutcome::Committed
                            | PublicationAttemptOutcome::Replayed
                            | PublicationAttemptOutcome::Superseded
                            | PublicationAttemptOutcome::Aborted
                    )
                })
                .map(|a| a.writer_epoch)
                .max()
                .unwrap_or(0);
            let still_unresolved = attempts.iter().any(|attempt| {
                attempt.outcome == PublicationAttemptOutcome::Indeterminate
                    && attempt.writer_epoch > max_terminal_epoch
                    && pointer_writer_epoch <= attempt.writer_epoch
                    && current_generation < attempt.candidate_generation
            });
            if still_unresolved {
                unresolved_ops.insert(op_id.clone());
            }
        }
        let has_unresolved_publications = !unresolved_ops.is_empty();

        // Map split bundle paths to their owning operation_id from intent, prepared, and candidate manifest records.
        let mut bundle_to_op: HashMap<String, String> = HashMap::new();
        for (op_id, intent) in &intents_by_op {
            bundle_to_op.insert(intent.planned_bundle_path.clone(), op_id.clone());
        }
        for (op_id, prep) in &prepared_by_op {
            bundle_to_op.insert(prep.split.bundle_path.clone(), op_id.clone());
        }
        for cand in candidate_manifests_by_path.values() {
            for split in &cand.splits {
                if let Some(op_id) = split.tags.get("operation_id") {
                    bundle_to_op
                        .entry(split.bundle_path.clone())
                        .or_insert_with(|| op_id.clone());
                } else if !cand.operation_id.is_empty() {
                    bundle_to_op
                        .entry(split.bundle_path.clone())
                        .or_insert_with(|| cand.operation_id.clone());
                }
            }
        }

        // Compute operation-level liveness across all artifacts of each operation_id
        // so an older intent.json or split bundle is never reclaimed while a later stage
        // (prepared.json, candidate manifest, or active-epoch attempt) of the same operation is active.
        let mut op_min_age_ms: HashMap<String, u64> = HashMap::new();
        let mut op_in_active_epoch: HashSet<String> = HashSet::new();
        for cand in candidate_manifests_by_path.values() {
            if active_epoch.is_some_and(|e| e == cand.writer_epoch) && !cand.operation_id.is_empty()
            {
                op_in_active_epoch.insert(cand.operation_id.clone());
            }
        }
        for (op_id, attempts) in &attempts_by_op {
            if attempts
                .iter()
                .any(|a| active_epoch.is_some_and(|e| e == a.writer_epoch))
            {
                op_in_active_epoch.insert(op_id.clone());
            }
        }
        for obj in &discovered {
            let (_, mut op_id, _, _) = classify_key_shape(index_uuid, &obj.path);
            if op_id.is_none()
                && let Some(mapped_op) = bundle_to_op.get(&obj.path)
            {
                op_id = Some(mapped_op.clone());
            }
            if let Some(op) = op_id {
                let age_ms = obj
                    .last_modified_epoch_ms
                    .map(|modified| options.now_epoch_ms.saturating_sub(modified))
                    .unwrap_or(0);
                op_min_age_ms
                    .entry(op)
                    .and_modify(|min_age| *min_age = (*min_age).min(age_ms))
                    .or_insert(age_ms);
            }
        }

        let mut summary = InventorySummaryCounts::default();
        let mut entries = Vec::new();
        let mut truncated = false;

        let index_safe_for_reclaim = lineage_healthy
            && !options.force_partial_listing_for_test
            && !has_unresolved_publications;

        for obj in discovered {
            let (kind, mut op_id, split_id, generation) = classify_key_shape(index_uuid, &obj.path);
            if op_id.is_none()
                && let Some(mapped_op) = bundle_to_op.get(&obj.path)
            {
                op_id = Some(mapped_op.clone());
            }
            let raw_obj_age_ms = obj
                .last_modified_epoch_ms
                .map(|modified| options.now_epoch_ms.saturating_sub(modified))
                .unwrap_or(0);
            let obj_age_ms = op_id
                .as_ref()
                .and_then(|op| op_min_age_ms.get(op).copied())
                .unwrap_or(raw_obj_age_ms);
            let belongs_to_active_epoch = op_id
                .as_ref()
                .is_some_and(|op| op_in_active_epoch.contains(op));

            let is_split_pinned = split_id
                .as_ref()
                .is_some_and(|sid| pinned_splits.contains(sid))
                || pinned_generation_split_paths.contains(&obj.path);
            let is_gen_pinned =
                generation.is_some_and(|g| effective_pinned_generations.contains(&g));
            let pinned = is_split_pinned || is_gen_pinned;

            let (classification, reclaimable, reason) = if let Some(corrupt_reason) =
                corrupt_paths.get(&obj.path)
            {
                (
                    ObjectReachabilityClass::Corrupt,
                    false,
                    format!("corrupt or unsupported object metadata: {corrupt_reason}"),
                )
            } else {
                match kind {
                    InventoryObjectKind::Unknown => (
                        ObjectReachabilityClass::Unknown,
                        false,
                        "unrecognized object path under index namespace; preserved conservatively"
                            .to_string(),
                    ),
                    InventoryObjectKind::JanitorState => (
                        ObjectReachabilityClass::Retained,
                        false,
                        "durable janitor mark/audit state".to_string(),
                    ),
                    InventoryObjectKind::ManifestPointer => (
                        ObjectReachabilityClass::Committed,
                        false,
                        format!(
                            "active manifest pointer at generation {current_generation}, revision {pointer_revision}"
                        ),
                    ),
                    InventoryObjectKind::ManifestGeneration => {
                        if current_manifest_path.as_deref() == Some(obj.path.as_str()) {
                            (
                                ObjectReachabilityClass::Committed,
                                false,
                                format!(
                                    "current committed manifest generation {current_generation}"
                                ),
                            )
                        } else if let Some(manifest_gen) = retained_manifest_paths.get(&obj.path) {
                            if is_gen_pinned {
                                (
                                    ObjectReachabilityClass::Pinned,
                                    false,
                                    format!(
                                        "ancestor manifest generation {manifest_gen} is pinned by an active reader/snapshot"
                                    ),
                                )
                            } else {
                                (
                                    ObjectReachabilityClass::Retained,
                                    false,
                                    format!(
                                        "ancestor manifest generation {manifest_gen} within retention window"
                                    ),
                                )
                            }
                        } else if let Some(manifest_gen) =
                            expired_ancestor_manifest_paths.get(&obj.path)
                        {
                            (
                                ObjectReachabilityClass::Retained,
                                false,
                                format!(
                                    "historical ancestor manifest generation {manifest_gen} preserved for lineage integrity"
                                ),
                            )
                        } else if let Some(cand) = candidate_manifests_by_path.get(&obj.path) {
                            if unresolved_ops.contains(&cand.operation_id) {
                                (
                                    ObjectReachabilityClass::Unresolved,
                                    false,
                                    format!(
                                        "candidate manifest for operation '{}' has an unresolved indeterminate pointer CAS",
                                        cand.operation_id
                                    ),
                                )
                            } else if active_epoch.is_some_and(|e| e == cand.writer_epoch)
                                || obj_age_ms <= options.active_grace_ms
                            {
                                (
                                    ObjectReachabilityClass::Prepared,
                                    false,
                                    format!(
                                        "in-flight candidate manifest at generation {} for epoch {} within active grace window",
                                        cand.generation, cand.writer_epoch
                                    ),
                                )
                            } else {
                                let beyond_horizon = obj_age_ms > options.retention_horizon_ms;
                                let can_reclaim =
                                    index_safe_for_reclaim && beyond_horizon && !pinned;
                                (
                                    if pinned {
                                        ObjectReachabilityClass::Pinned
                                    } else {
                                        ObjectReachabilityClass::Superseded
                                    },
                                    can_reclaim,
                                    if can_reclaim {
                                        format!(
                                            "unreachable superseded candidate manifest for operation '{}' (epoch {}) outside retention and active-lease windows",
                                            cand.operation_id, cand.writer_epoch
                                        )
                                    } else {
                                        format!(
                                            "superseded candidate manifest for operation '{}' retained until retention horizon expires",
                                            cand.operation_id
                                        )
                                    },
                                )
                            }
                        } else {
                            (
                                ObjectReachabilityClass::Unknown,
                                false,
                                "unclassified manifest object".into(),
                            )
                        }
                    }
                    InventoryObjectKind::SplitBundle => {
                        if committed_split_paths.contains(&obj.path) {
                            (
                                ObjectReachabilityClass::Committed,
                                false,
                                "split bundle referenced by the current committed manifest"
                                    .to_string(),
                            )
                        } else if retained_split_paths.contains(&obj.path) {
                            if pinned {
                                (
                                    ObjectReachabilityClass::Pinned,
                                    false,
                                    "split bundle referenced by a retained manifest and pinned by an active reader"
                                        .to_string(),
                                )
                            } else {
                                (
                                    ObjectReachabilityClass::Retained,
                                    false,
                                    "split bundle referenced by a retained ancestor manifest"
                                        .to_string(),
                                )
                            }
                        } else if pinned {
                            (
                                ObjectReachabilityClass::Pinned,
                                false,
                                "split bundle is actively pinned by a local reader or snapshot lease"
                                    .to_string(),
                            )
                        } else if compacted_split_paths.contains(&obj.path) {
                            let beyond_horizon = obj_age_ms > options.retention_horizon_ms;
                            let can_reclaim = index_safe_for_reclaim && beyond_horizon;
                            (
                                ObjectReachabilityClass::Superseded,
                                can_reclaim,
                                if can_reclaim {
                                    "compacted split bundle no longer referenced by any retained or pinned manifest generation"
                                        .to_string()
                                } else {
                                    "compacted split bundle retained until retention horizon expires"
                                        .to_string()
                                },
                            )
                        } else if let Some(ref op) = op_id
                            && unresolved_ops.contains(op)
                        {
                            (
                                ObjectReachabilityClass::Unresolved,
                                false,
                                format!(
                                    "split bundle for operation '{op}' has an unresolved pointer CAS outcome"
                                ),
                            )
                        } else if let Some(ref op) = op_id
                            && (belongs_to_active_epoch
                                || (prepared_by_op.contains_key(op)
                                    && obj_age_ms <= options.active_grace_ms))
                        {
                            (
                                ObjectReachabilityClass::Prepared,
                                false,
                                format!(
                                    "prepared split bundle for operation '{op}' within active sequencer grace window"
                                ),
                            )
                        } else if obj_age_ms <= options.active_grace_ms {
                            (
                                ObjectReachabilityClass::Staging,
                                false,
                                "staged split bundle within active producer grace window"
                                    .to_string(),
                            )
                        } else {
                            let beyond_horizon = obj_age_ms > options.retention_horizon_ms;
                            let can_reclaim = index_safe_for_reclaim && beyond_horizon;
                            (
                                ObjectReachabilityClass::Abandoned,
                                can_reclaim,
                                if can_reclaim {
                                    "unreferenced split bundle outside active producer/sequencer and retention windows"
                                        .to_string()
                                } else {
                                    "unreferenced abandoned split bundle waiting for retention horizon before reclamation"
                                        .to_string()
                                },
                            )
                        }
                    }
                    InventoryObjectKind::PublicationIntent
                    | InventoryObjectKind::PublicationPrepared
                    | InventoryObjectKind::PublicationCommitted
                    | InventoryObjectKind::PublicationAttempt => {
                        let op = op_id.clone().unwrap_or_default();
                        if committed_operation_ids.contains(&op) {
                            (
                                ObjectReachabilityClass::Committed,
                                false,
                                format!("audit record for committed operation '{op}'"),
                            )
                        } else if retained_operation_ids.contains(&op) {
                            (
                                ObjectReachabilityClass::Retained,
                                false,
                                format!("audit record for retained operation '{op}'"),
                            )
                        } else if unresolved_ops.contains(&op) {
                            (
                                ObjectReachabilityClass::Unresolved,
                                false,
                                format!(
                                    "operation '{op}' has an unresolved indeterminate pointer CAS attempt"
                                ),
                            )
                        } else if committed_records_by_op.contains_key(&op) {
                            // A committed.json exists for an operation not reachable from current pointer!
                            lineage_healthy = false;
                            let err_msg = format!(
                                "operation '{op}' has committed.json but is not reachable from manifest.current.json"
                            );
                            if !lineage_errors.contains(&err_msg) {
                                lineage_errors.push(err_msg.clone());
                            }
                            (ObjectReachabilityClass::Corrupt, false, err_msg)
                        } else if belongs_to_active_epoch || obj_age_ms <= options.active_grace_ms {
                            if prepared_by_op.contains_key(&op) {
                                (
                                    ObjectReachabilityClass::Prepared,
                                    false,
                                    format!(
                                        "operation '{op}' is prepared and within active grace window"
                                    ),
                                )
                            } else {
                                (
                                    ObjectReachabilityClass::Staging,
                                    false,
                                    format!(
                                        "operation '{op}' intent is within active producer grace window"
                                    ),
                                )
                            }
                        } else {
                            let is_superseded = attempts_by_op.get(&op).is_some_and(|list| {
                                list.iter()
                                    .any(|a| a.outcome == PublicationAttemptOutcome::Superseded)
                            });
                            let beyond_horizon = obj_age_ms > options.retention_horizon_ms;
                            let can_reclaim = index_safe_for_reclaim && beyond_horizon && !pinned;
                            let class = if is_superseded {
                                ObjectReachabilityClass::Superseded
                            } else {
                                ObjectReachabilityClass::Abandoned
                            };
                            (
                                class,
                                can_reclaim,
                                if can_reclaim {
                                    format!(
                                        "unreachable publication record for operation '{op}' outside active and retention windows"
                                    )
                                } else {
                                    format!(
                                        "unreachable publication record for operation '{op}' retained until retention horizon expires"
                                    )
                                },
                            )
                        }
                    }
                }
            };

            summary.total_objects += 1;
            summary.total_bytes += obj.size_bytes;
            if reclaimable {
                summary.reclaimable_objects += 1;
                summary.reclaimable_bytes += obj.size_bytes;
            }
            let class_name = serde_json::to_value(classification)
                .ok()
                .and_then(|v| v.as_str().map(ToString::to_string))
                .unwrap_or_else(|| format!("{classification:?}").to_lowercase());
            *summary.by_classification.entry(class_name).or_insert(0) += 1;

            if entries.len() < options.max_reported_objects {
                entries.push(InventoryObjectEntry {
                    path: obj.path,
                    kind,
                    size_bytes: obj.size_bytes,
                    last_modified_epoch_ms: obj.last_modified_epoch_ms,
                    classification,
                    operation_id: op_id,
                    split_id,
                    generation,
                    pinned,
                    reclaimable,
                    reason,
                });
            } else {
                truncated = true;
            }
        }

        let listing_complete =
            lineage_errors.is_empty() && !truncated && !options.force_partial_listing_for_test;
        if !listing_complete || !lineage_healthy || has_unresolved_publications {
            for entry in &mut entries {
                if entry.reclaimable {
                    entry.reclaimable = false;
                    entry.reason = format!(
                        "{}; reclamation disabled because inventory listing or lineage is incomplete",
                        entry.reason
                    );
                }
            }
            summary.reclaimable_objects = 0;
            summary.reclaimable_bytes = 0;
        }

        Ok(ReachabilityInventoryReport {
            index_uuid: options.index_uuid.clone(),
            index_name: options.index_name.clone(),
            scanned_at_epoch_ms: options.now_epoch_ms,
            current_generation,
            pointer_revision,
            pointer_writer_epoch,
            listing_complete,
            truncated,
            lineage_healthy,
            has_unresolved_publications,
            lineage_errors,
            summary,
            objects: entries,
        })
    }
}
