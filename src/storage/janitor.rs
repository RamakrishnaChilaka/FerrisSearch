//! Conservative, dry-run-first object-store janitor consuming the FS-020
//! reachability inventory (ADR 0002 section 3.9, ADR 0003 section 3.9, FS-021).

use anyhow::{Context, Result};
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::inventory::{
    InventoryObjectEntry, ObjectReachabilityClass, ReachabilityInventoryOptions,
    ReachabilityInventoryReport,
};
use super::{CompareAndSwapOutcome, StorageManager};

pub const JANITOR_MARKS_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_JANITOR_MAX_DELETES_PER_RUN: usize = 64;
pub const DEFAULT_JANITOR_MAX_DELETE_BYTES_PER_RUN: u64 = 256 * 1024 * 1024;
pub const DEFAULT_JANITOR_SWEEP_GRACE_MS: u64 = 0;

const fn default_true() -> bool {
    true
}

/// Action taken (or planned in `dry_run` mode) for an inventoried object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JanitorCandidateAction {
    Keep,
    DryRunWouldDelete,
    MarkedForSweep,
    Deleted,
    FalsePositivePrevented,
    RateLimited,
    DeleteFailed,
    Blocked,
}

impl JanitorCandidateAction {
    pub const fn as_metric_label(&self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::DryRunWouldDelete => "dry_run_would_delete",
            Self::MarkedForSweep => "marked",
            Self::Deleted => "deleted",
            Self::FalsePositivePrevented => "false_positive_prevented",
            Self::RateLimited => "rate_limited",
            Self::DeleteFailed => "delete_failed",
            Self::Blocked => "blocked",
        }
    }
}

/// Durable mark entry stored in `<index_uuid>/janitor/marks.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JanitorMarkEntry {
    pub path: String,
    pub classification: ObjectReachabilityClass,
    pub size_bytes: u64,
    pub marked_at_epoch_ms: u64,
    pub last_verified_at_epoch_ms: u64,
    pub delete_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Durable mark table persisted across process restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JanitorMarksDocument {
    pub format_version: u32,
    pub index_uuid: String,
    pub updated_at_epoch_ms: u64,
    pub marks: BTreeMap<String, JanitorMarkEntry>,
}

impl JanitorMarksDocument {
    pub fn empty(index_uuid: &str, now_epoch_ms: u64) -> Self {
        Self {
            format_version: JANITOR_MARKS_FORMAT_VERSION,
            index_uuid: index_uuid.to_string(),
            updated_at_epoch_ms: now_epoch_ms,
            marks: BTreeMap::new(),
        }
    }
}

/// Configuration for a janitor run. Defaults to `dry_run = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorOptions {
    #[serde(default = "default_true")]
    pub dry_run: bool,
    pub inventory_options: ReachabilityInventoryOptions,
    pub max_deletes_per_run: usize,
    pub max_delete_bytes_per_run: u64,
    pub sweep_grace_ms: u64,
}

impl JanitorOptions {
    pub fn dry_run_for_index(index_uuid: impl Into<String>, index_name: Option<String>) -> Self {
        Self {
            dry_run: true,
            inventory_options: ReachabilityInventoryOptions::for_index(index_uuid, index_name),
            max_deletes_per_run: DEFAULT_JANITOR_MAX_DELETES_PER_RUN,
            max_delete_bytes_per_run: DEFAULT_JANITOR_MAX_DELETE_BYTES_PER_RUN,
            sweep_grace_ms: DEFAULT_JANITOR_SWEEP_GRACE_MS,
        }
    }
}

/// Per-object audit explanation emitted by a janitor run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JanitorCandidateDecision {
    pub path: String,
    pub classification: ObjectReachabilityClass,
    pub size_bytes: u64,
    pub action: JanitorCandidateAction,
    pub reason: String,
}

/// Complete machine-readable audit report from one janitor execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JanitorRunReport {
    pub index_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    pub dry_run: bool,
    pub executed_at_epoch_ms: u64,
    pub blocked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub would_delete_objects: usize,
    pub would_delete_bytes: u64,
    pub marked_objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
    pub false_positives_prevented: usize,
    pub rate_limited_objects: usize,
    pub delete_failures: usize,
    pub decisions: Vec<JanitorCandidateDecision>,
    pub inventory: ReachabilityInventoryReport,
}

pub fn janitor_marks_key(index_uuid: &str) -> String {
    format!("{index_uuid}/janitor/marks.json")
}

impl StorageManager {
    pub fn fail_janitor_delete_path_for_test(&self, path: impl Into<String>) {
        self.janitor_fail_delete_paths
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.into());
    }

    pub fn clear_janitor_delete_failures_for_test(&self) {
        self.janitor_fail_delete_paths
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    pub async fn load_janitor_marks(
        &self,
        index_uuid: &str,
    ) -> Result<Option<(JanitorMarksDocument, super::CompareToken)>> {
        let key = janitor_marks_key(index_uuid);
        let path = self.object_path(&key)?;
        let Some(read) = self.read_with_token(&path).await? else {
            return Ok(None);
        };
        let doc: JanitorMarksDocument = serde_json::from_slice(&read.bytes).map_err(|error| {
            crate::common::unsupported_index_format(
                "remote store janitor marks",
                format!("cannot decode janitor marks at {path}: {error}"),
            )
        })?;
        if doc.format_version != JANITOR_MARKS_FORMAT_VERSION || doc.index_uuid != index_uuid {
            return Err(crate::common::unsupported_index_format(
                "remote store janitor marks",
                format!(
                    "invalid format_version {} or index_uuid '{}' at {}",
                    doc.format_version, doc.index_uuid, path
                ),
            ));
        }
        Ok(Some((doc, read.token)))
    }

    async fn save_janitor_marks(
        &self,
        index_uuid: &str,
        expected_token: Option<&super::CompareToken>,
        doc: &JanitorMarksDocument,
    ) -> Result<()> {
        let key = janitor_marks_key(index_uuid);
        let path = self.object_path(&key)?;
        let bytes =
            serde_json::to_vec_pretty(doc).context("failed to serialize janitor marks document")?;
        match self
            .compare_and_swap_optional(&path, expected_token, bytes)
            .await?
        {
            CompareAndSwapOutcome::Swapped { .. } => Ok(()),
            CompareAndSwapOutcome::Conflict => {
                anyhow::bail!("janitor marks at {path} were modified concurrently")
            }
        }
    }

    async fn delete_reclaimable_object(&self, path_str: &str) -> Result<()> {
        let should_fail = self
            .janitor_fail_delete_paths
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(path_str);
        if should_fail {
            crate::metrics::record_object_store_request("delete", "error");
            anyhow::bail!("injected transient delete failure for {path_str}");
        }
        let path = self.object_path(path_str)?;
        match self.object_store().delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                crate::metrics::record_object_store_request("delete", "success");
                Ok(())
            }
            Err(error) => {
                crate::metrics::record_object_store_request("delete", "error");
                Err(anyhow::Error::from(error))
            }
        }
    }

    /// Execute one conservative janitor pass over `options.inventory_options.index_uuid`.
    ///
    /// - Defaults to `dry_run = true`.
    /// - Refuses destructive deletion if the reachability inventory listing is
    ///   incomplete, lineage is unhealthy, or any publication has an unresolved
    ///   indeterminate pointer CAS.
    /// - Records durable marks in `<index_uuid>/janitor/marks.json`, enforces
    ///   `sweep_grace_ms`, recomputes reachability immediately before deletion
    ///   to prevent false positives from delayed readers/pins or concurrent
    ///   commits, and rate-limits deletions by object count and bytes.
    pub async fn run_janitor(&self, options: &JanitorOptions) -> Result<JanitorRunReport> {
        let mode_label = if options.dry_run {
            "dry_run"
        } else {
            "destructive"
        };
        let index_uuid = options.inventory_options.index_uuid.as_str();
        let now_ms = options.inventory_options.now_epoch_ms;
        let inventory = self
            .build_reachability_inventory(&options.inventory_options)
            .await?;

        let blocked_reason = if !inventory.listing_complete {
            Some("reachability inventory listing was incomplete or truncated".to_string())
        } else if !inventory.lineage_healthy {
            Some(format!(
                "manifest or operation lineage is unhealthy: {}",
                inventory.lineage_errors.join("; ")
            ))
        } else if inventory.has_unresolved_publications {
            Some(
                "index has unresolved indeterminate publication attempts; reclamation is paused"
                    .to_string(),
            )
        } else {
            None
        };
        let blocked = blocked_reason.is_some();

        let existing_marks_with_token = self.load_janitor_marks(index_uuid).await?;
        let (mut marks_doc, expected_marks_token) = match existing_marks_with_token {
            Some((doc, token)) => (doc, Some(token)),
            None => (JanitorMarksDocument::empty(index_uuid, now_ms), None),
        };
        let mut marks_changed = false;

        // First, check if any previously marked object is now reachable or pinned!
        let mut false_positives_prevented = 0usize;
        let current_by_path: BTreeMap<&str, &InventoryObjectEntry> = inventory
            .objects
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        let stale_marked_paths: Vec<String> = marks_doc
            .marks
            .keys()
            .filter(|marked_path| {
                current_by_path
                    .get(marked_path.as_str())
                    .is_some_and(|entry| {
                        entry.pinned
                            || matches!(
                                entry.classification,
                                ObjectReachabilityClass::Committed
                                    | ObjectReachabilityClass::Retained
                                    | ObjectReachabilityClass::Pinned
                                    | ObjectReachabilityClass::Staging
                                    | ObjectReachabilityClass::Prepared
                            )
                            || (!blocked && !entry.reclaimable)
                    })
            })
            .cloned()
            .collect();
        for path in stale_marked_paths {
            marks_doc.marks.remove(&path);
            marks_changed = true;
            false_positives_prevented += 1;
            crate::metrics::record_remote_store_janitor_candidate("false_positive_prevented");
        }
        if inventory.listing_complete {
            let missing_marked_paths: Vec<String> = marks_doc
                .marks
                .keys()
                .filter(|marked_path| !current_by_path.contains_key(marked_path.as_str()))
                .cloned()
                .collect();
            for path in missing_marked_paths {
                marks_doc.marks.remove(&path);
                marks_changed = true;
            }
        }

        // Order candidates so split bundles and candidate manifests are reclaimed before
        // their operation's intent.json / prepared.json metadata when rate-limited.
        let mut ordered_objects = inventory.objects.clone();
        ordered_objects.sort_by(|a, b| {
            let priority = |kind: super::InventoryObjectKind| match kind {
                super::InventoryObjectKind::SplitBundle => 0u8,
                super::InventoryObjectKind::ManifestGeneration => 1,
                super::InventoryObjectKind::PublicationAttempt => 2,
                super::InventoryObjectKind::PublicationPrepared => 3,
                super::InventoryObjectKind::PublicationIntent => 4,
                _ => 5,
            };
            priority(a.kind)
                .cmp(&priority(b.kind))
                .then_with(|| a.path.cmp(&b.path))
        });

        // Reverify snapshot immediately before destructive sweep if any mark is eligible.
        let reverified_inventory = if !options.dry_run && !blocked {
            Some(
                self.build_reachability_inventory(&options.inventory_options)
                    .await?,
            )
        } else {
            None
        };
        let reverified_by_path: BTreeMap<&str, &InventoryObjectEntry> = reverified_inventory
            .as_ref()
            .map(|rep| {
                rep.objects
                    .iter()
                    .map(|entry| (entry.path.as_str(), entry))
                    .collect()
            })
            .unwrap_or_default();

        let mut decisions = Vec::with_capacity(inventory.objects.len());
        let mut would_delete_objects = 0usize;
        let mut would_delete_bytes = 0u64;
        let mut marked_objects = 0usize;
        let mut deleted_objects = 0usize;
        let mut deleted_bytes = 0u64;
        let mut rate_limited_objects = 0usize;
        let mut delete_failures = 0usize;

        for entry in &ordered_objects {
            if blocked {
                let action = if matches!(
                    entry.classification,
                    ObjectReachabilityClass::Abandoned | ObjectReachabilityClass::Superseded
                ) {
                    JanitorCandidateAction::Blocked
                } else {
                    JanitorCandidateAction::Keep
                };
                crate::metrics::record_remote_store_janitor_candidate(action.as_metric_label());
                decisions.push(JanitorCandidateDecision {
                    path: entry.path.clone(),
                    classification: entry.classification,
                    size_bytes: entry.size_bytes,
                    action,
                    reason: blocked_reason
                        .clone()
                        .unwrap_or_else(|| entry.reason.clone()),
                });
                continue;
            }

            if !entry.reclaimable {
                crate::metrics::record_remote_store_janitor_candidate("keep");
                decisions.push(JanitorCandidateDecision {
                    path: entry.path.clone(),
                    classification: entry.classification,
                    size_bytes: entry.size_bytes,
                    action: JanitorCandidateAction::Keep,
                    reason: entry.reason.clone(),
                });
                continue;
            }

            if options.dry_run {
                let within_count = would_delete_objects < options.max_deletes_per_run;
                let within_bytes = would_delete_bytes.saturating_add(entry.size_bytes)
                    <= options.max_delete_bytes_per_run;
                if within_count && within_bytes {
                    would_delete_objects += 1;
                    would_delete_bytes += entry.size_bytes;
                    crate::metrics::record_remote_store_janitor_candidate("dry_run_would_delete");
                    decisions.push(JanitorCandidateDecision {
                        path: entry.path.clone(),
                        classification: entry.classification,
                        size_bytes: entry.size_bytes,
                        action: JanitorCandidateAction::DryRunWouldDelete,
                        reason: format!("dry_run: would reclaim ({})", entry.reason),
                    });
                } else {
                    rate_limited_objects += 1;
                    crate::metrics::record_remote_store_janitor_candidate("rate_limited");
                    decisions.push(JanitorCandidateDecision {
                        path: entry.path.clone(),
                        classification: entry.classification,
                        size_bytes: entry.size_bytes,
                        action: JanitorCandidateAction::RateLimited,
                        reason: format!(
                            "dry_run: candidate exceeds per-run delete rate limit (max_deletes={}, max_bytes={})",
                            options.max_deletes_per_run, options.max_delete_bytes_per_run
                        ),
                    });
                }
                continue;
            }

            // Destructive mode: mark, wait sweep_grace_ms, prove via reverified_by_path, then sweep.
            let was_already_marked = marks_doc.marks.contains_key(&entry.path);
            let mark = marks_doc
                .marks
                .entry(entry.path.clone())
                .or_insert_with(|| {
                    marks_changed = true;
                    JanitorMarkEntry {
                        path: entry.path.clone(),
                        classification: entry.classification,
                        size_bytes: entry.size_bytes,
                        marked_at_epoch_ms: now_ms,
                        last_verified_at_epoch_ms: now_ms,
                        delete_attempts: 0,
                        last_error: None,
                    }
                });
            mark.last_verified_at_epoch_ms = now_ms;

            let mark_age_ms = now_ms.saturating_sub(mark.marked_at_epoch_ms);
            if options.sweep_grace_ms > 0
                && (!was_already_marked || mark_age_ms < options.sweep_grace_ms)
            {
                marked_objects += 1;
                crate::metrics::record_remote_store_janitor_candidate("marked");
                decisions.push(JanitorCandidateDecision {
                    path: entry.path.clone(),
                    classification: entry.classification,
                    size_bytes: entry.size_bytes,
                    action: JanitorCandidateAction::MarkedForSweep,
                    reason: format!(
                        "marked durably at {} ms; waiting for sweep_grace_ms ({} ms remaining)",
                        mark.marked_at_epoch_ms,
                        options.sweep_grace_ms.saturating_sub(mark_age_ms)
                    ),
                });
                continue;
            }

            // Reverify reachability immediately before deletion.
            let still_reclaimable = reverified_by_path
                .get(entry.path.as_str())
                .is_some_and(|reverified| reverified.reclaimable && !reverified.pinned)
                && !self
                    .locally_pinned_splits_for_index(index_uuid)
                    .iter()
                    .any(|pinned_split| entry.split_id.as_deref() == Some(pinned_split.as_str()));

            if !still_reclaimable {
                marks_doc.marks.remove(&entry.path);
                marks_changed = true;
                false_positives_prevented += 1;
                crate::metrics::record_remote_store_janitor_candidate("false_positive_prevented");
                decisions.push(JanitorCandidateDecision {
                    path: entry.path.clone(),
                    classification: entry.classification,
                    size_bytes: entry.size_bytes,
                    action: JanitorCandidateAction::FalsePositivePrevented,
                    reason: "object became pinned or reachable during pre-delete verification; mark cleared"
                        .to_string(),
                });
                continue;
            }

            let within_count = deleted_objects < options.max_deletes_per_run;
            let within_bytes =
                deleted_bytes.saturating_add(entry.size_bytes) <= options.max_delete_bytes_per_run;
            if !within_count || !within_bytes {
                rate_limited_objects += 1;
                crate::metrics::record_remote_store_janitor_candidate("rate_limited");
                decisions.push(JanitorCandidateDecision {
                    path: entry.path.clone(),
                    classification: entry.classification,
                    size_bytes: entry.size_bytes,
                    action: JanitorCandidateAction::RateLimited,
                    reason: format!(
                        "candidate deferred by per-run rate limit (max_deletes={}, max_bytes={})",
                        options.max_deletes_per_run, options.max_delete_bytes_per_run
                    ),
                });
                continue;
            }

            match self.delete_reclaimable_object(&entry.path).await {
                Ok(()) => {
                    marks_doc.marks.remove(&entry.path);
                    marks_changed = true;
                    deleted_objects += 1;
                    deleted_bytes += entry.size_bytes;
                    crate::metrics::record_remote_store_janitor_candidate("deleted");
                    crate::metrics::record_remote_store_janitor_deleted_bytes(entry.size_bytes);
                    decisions.push(JanitorCandidateDecision {
                        path: entry.path.clone(),
                        classification: entry.classification,
                        size_bytes: entry.size_bytes,
                        action: JanitorCandidateAction::Deleted,
                        reason: format!("deleted reclaimable object ({})", entry.reason),
                    });
                }
                Err(error) => {
                    if let Some(m) = marks_doc.marks.get_mut(&entry.path) {
                        m.delete_attempts = m.delete_attempts.saturating_add(1);
                        m.last_error = Some(error.to_string());
                        marks_changed = true;
                    }
                    delete_failures += 1;
                    crate::metrics::record_remote_store_janitor_candidate("delete_failed");
                    decisions.push(JanitorCandidateDecision {
                        path: entry.path.clone(),
                        classification: entry.classification,
                        size_bytes: entry.size_bytes,
                        action: JanitorCandidateAction::DeleteFailed,
                        reason: format!(
                            "transient delete failure (mark retained for retry): {error}"
                        ),
                    });
                }
            }
        }

        if !options.dry_run && (marks_changed || expected_marks_token.is_some()) {
            marks_doc.updated_at_epoch_ms = now_ms;
            self.save_janitor_marks(index_uuid, expected_marks_token.as_ref(), &marks_doc)
                .await?;
        }

        crate::metrics::record_remote_store_janitor_run(
            mode_label,
            if blocked { "blocked" } else { "completed" },
        );

        Ok(JanitorRunReport {
            index_uuid: options.inventory_options.index_uuid.clone(),
            index_name: options.inventory_options.index_name.clone(),
            dry_run: options.dry_run,
            executed_at_epoch_ms: now_ms,
            blocked,
            blocked_reason,
            would_delete_objects,
            would_delete_bytes,
            marked_objects,
            deleted_objects,
            deleted_bytes,
            false_positives_prevented,
            rate_limited_objects,
            delete_failures,
            decisions,
            inventory,
        })
    }
}
