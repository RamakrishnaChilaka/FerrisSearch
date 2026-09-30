//! Composite search engine — combines Tantivy (text) + USearch (vector).
//!
//! This is the default engine backing each shard. It delegates text operations
//! to HotEngine and vector operations to VectorIndex. Future engine
//! implementations (e.g. shardless/split-based) can implement the SearchEngine
//! trait directly without using this composite.

use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::SearchEngine;
use super::tantivy::HotEngine;
use super::vector::VectorIndex;
use crate::wal::TranslogDurability;

const VECTOR_INDEX_FILE: &str = "vectors.usearch";
const VECTOR_DOC_IDS_FILE: &str = "vectors.docids.bin";
const VECTORS_STALE_FILE: &str = "vectors.stale";
const VECTORS_STALE_TEMP_FILE: &str = "vectors.stale.tmp";

/// A composite engine that owns both a text index (Tantivy) and an optional
/// vector index (USearch). All document operations go through here — vector
/// fields are auto-detected and indexed into USearch transparently.
pub struct CompositeEngine {
    text: HotEngine,
    vector: RwLock<Option<VectorIndex>>,
    data_dir: std::path::PathBuf,
    /// Monotonic replicated persisted checkpoint (primary only).
    global_cp: Mutex<Option<u64>>,
    /// Monotonic local persisted prefix learned through replica apply.
    replica_persisted_cp: Mutex<Option<u64>>,
    /// Serializes text recovery with vector rebuild and stale-marker updates.
    vector_recovery: Mutex<()>,
    has_vector_mappings: bool,
    /// Shared column cache for fast-field Arrow arrays.
    #[allow(dead_code)]
    column_cache: Arc<super::column_cache::ColumnCache>,
}

#[derive(Clone)]
enum PreparedVectorMutation {
    None,
    SkipShapeMismatch {
        field: String,
        expected: usize,
        actual: usize,
    },
    Index {
        vector: Vec<f32>,
    },
    Delete,
}

impl CompositeEngine {
    /// Create a new composite engine at the given data directory.
    pub fn new(data_dir: impl AsRef<Path>, refresh_interval: Duration) -> Result<Self> {
        Self::new_with_mappings(
            data_dir,
            refresh_interval,
            &std::collections::HashMap::new(),
            TranslogDurability::Request,
            Arc::new(super::column_cache::ColumnCache::new(0, 0)),
        )
    }

    /// Create a new composite engine with explicit field mappings.
    pub fn new_with_mappings(
        data_dir: impl AsRef<Path>,
        refresh_interval: Duration,
        mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let text = HotEngine::new_with_mappings(
            &data_dir,
            refresh_interval,
            mappings,
            durability,
            column_cache.clone(),
        )?;
        let has_vector_mappings = mappings.values().any(|mapping| {
            matches!(
                mapping.field_type,
                crate::cluster::state::FieldType::KnnVector
            )
        });

        // Load existing vector index if present.
        let vector_path = data_dir.join(VECTOR_INDEX_FILE);
        let vector = if vector_path.exists() {
            // We don't know the dimensions yet — we'll discover on first vector field.
            // For now, skip loading; rebuild_vectors will handle it.
            None
        } else {
            None
        };

        Ok(Self {
            text,
            vector: RwLock::new(vector),
            data_dir,
            global_cp: Mutex::new(None),
            replica_persisted_cp: Mutex::new(None),
            vector_recovery: Mutex::new(()),
            has_vector_mappings,
            column_cache,
        })
    }

    pub(crate) fn open_existing_with_mappings(
        data_dir: impl AsRef<Path>,
        refresh_interval: Duration,
        mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let text = HotEngine::open_existing_with_mappings(
            &data_dir,
            refresh_interval,
            mappings,
            durability,
            column_cache.clone(),
        )?;
        let has_vector_mappings = mappings.values().any(|mapping| {
            matches!(
                mapping.field_type,
                crate::cluster::state::FieldType::KnnVector
            )
        });

        Ok(Self {
            text,
            vector: RwLock::new(None),
            data_dir,
            global_cp: Mutex::new(None),
            replica_persisted_cp: Mutex::new(None),
            vector_recovery: Mutex::new(()),
            has_vector_mappings,
            column_cache,
        })
    }

    /// Get a reference to the underlying HotEngine (for refresh loop).
    pub fn text_engine(&self) -> &HotEngine {
        &self.text
    }

    /// Start the background refresh loop for the text engine.
    /// Holds only a Weak reference so dropping the shard from the manager lets
    /// the old engine shut down cleanly before a reopen.
    pub fn start_refresh_loop(engine: Arc<Self>) {
        let interval = engine.text.refresh_interval;
        let weak_engine = Arc::downgrade(&engine);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(engine) = weak_engine.upgrade() else {
                    tracing::info!("Shard refresh loop stopping because engine was dropped");
                    break;
                };
                if let Err(e) = Self::run_background_maintenance(engine, None).await {
                    tracing::error!("Background shard maintenance task failed: {}", e);
                }
            }
        });
    }

    async fn run_background_maintenance(
        engine: Arc<Self>,
        flush_threshold: Option<u64>,
    ) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            // Commit/truncate/save work is blocking I/O and must stay off the async
            // scheduler so Raft heartbeats and transport RPCs keep making progress.
            if let Err(e) = engine.refresh() {
                tracing::error!("Background refresh failed: {}", e);
            }
            if let Some(flush_threshold) = flush_threshold
                && let Err(e) = engine.maybe_auto_flush(flush_threshold)
            {
                tracing::error!("Auto-flush failed: {}", e);
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("background shard maintenance task failed: {e}"))?;
        Ok(())
    }

    /// Start a reactive refresh loop that adjusts its interval when the
    /// setting changes via the provided `watch::Receiver`.
    ///
    /// Uses `tokio::select!` to either:
    /// - Sleep for the current interval, then refresh
    /// - Wake up immediately when the interval setting changes
    ///
    /// After each refresh tick, checks the translog size and triggers an
    /// automatic flush+truncate if it exceeds the configured threshold.
    pub fn start_refresh_loop_reactive(
        engine: Arc<Self>,
        mut refresh_rx: tokio::sync::watch::Receiver<std::time::Duration>,
        mut flush_threshold_rx: tokio::sync::watch::Receiver<u64>,
    ) {
        let weak_engine = Arc::downgrade(&engine);
        tokio::spawn(async move {
            const MIN_REFRESH: std::time::Duration = std::time::Duration::from_secs(1);
            let mut interval = (*refresh_rx.borrow_and_update()).max(MIN_REFRESH);
            let mut flush_threshold = *flush_threshold_rx.borrow_and_update();
            loop {
                tokio::select! {
                    () = tokio::time::sleep(interval) => {
                        let Some(engine) = weak_engine.upgrade() else {
                            tracing::info!("Reactive shard refresh loop stopping because engine was dropped");
                            break;
                        };
                        if let Err(e) = Self::run_background_maintenance(
                            engine,
                            Some(flush_threshold),
                        ).await {
                            tracing::error!("Background shard maintenance task failed: {}", e);
                        }
                    }
                    result = refresh_rx.changed() => {
                        match result {
                            Ok(()) => {
                                if weak_engine.upgrade().is_none() {
                                    tracing::info!("Reactive refresh loop stopping because engine was dropped");
                                    break;
                                }
                                interval = (*refresh_rx.borrow_and_update()).max(MIN_REFRESH);
                                tracing::info!("Refresh interval updated to {:?}", interval);
                            }
                            Err(_) => {
                                // Sender dropped — settings manager is gone (index deleted)
                                tracing::info!("Settings channel closed, stopping refresh loop");
                                break;
                            }
                        }
                    }
                    result = flush_threshold_rx.changed() => {
                        match result {
                            Ok(()) => {
                                if weak_engine.upgrade().is_none() {
                                    tracing::info!("Reactive refresh loop stopping because engine was dropped");
                                    break;
                                }
                                flush_threshold = *flush_threshold_rx.borrow_and_update();
                                tracing::info!("Flush threshold updated to {} bytes", flush_threshold);
                            }
                            Err(_) => {
                                tracing::info!("Flush threshold channel closed, stopping refresh loop");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn maybe_auto_flush(&self, flush_threshold: u64) -> Result<bool> {
        if flush_threshold == 0 {
            return Ok(false);
        }

        let tl_size = self.text.translog_size_bytes();
        if tl_size < flush_threshold {
            return Ok(false);
        }

        let Some(truncation_checkpoint) = self.safe_truncation_checkpoint() else {
            tracing::debug!(
                "Skipping auto-flush with translog size {} bytes because no safe truncation checkpoint is available",
                tl_size
            );
            return Ok(false);
        };
        let _vector_recovery = match self.vector_recovery.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;

        tracing::info!(
            "Translog size ({} bytes) exceeds threshold ({} bytes), auto-flushing",
            tl_size,
            flush_threshold
        );
        if !self
            .text
            .try_flush_with_global_checkpoint(truncation_checkpoint)?
        {
            tracing::debug!(
                "Skipping auto-flush with translog size {} bytes because the shard is busy ingesting or committing",
                tl_size
            );
            return Ok(false);
        }
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
        } else if !self.try_save_vectors()? {
            tracing::debug!(
                "Deferred vector index save during auto-flush because the vector index is busy"
            );
        }
        Ok(true)
    }

    fn record_replica_persisted_checkpoint(&self, checkpoint: Option<u64>) {
        let Some(checkpoint) = checkpoint else {
            return;
        };
        let mut current = self
            .replica_persisted_cp
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *current = Some(current.map_or(checkpoint, |value| value.max(checkpoint)));
    }

    fn safe_truncation_checkpoint(&self) -> Option<u64> {
        self.global_checkpoint().or_else(|| {
            *self
                .replica_persisted_cp
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        })
    }

    fn vector_index_path(&self) -> std::path::PathBuf {
        self.data_dir.join(VECTOR_INDEX_FILE)
    }

    fn vector_doc_ids_path(&self) -> std::path::PathBuf {
        self.data_dir.join(VECTOR_DOC_IDS_FILE)
    }

    fn vectors_stale_path(&self) -> std::path::PathBuf {
        self.data_dir.join(VECTORS_STALE_FILE)
    }

    fn vectors_stale_temp_path(&self) -> std::path::PathBuf {
        self.data_dir.join(VECTORS_STALE_TEMP_FILE)
    }

    fn vectors_are_stale(&self) -> Result<bool> {
        Ok(self.vectors_stale_path().try_exists()?
            || self.vectors_stale_temp_path().try_exists()?)
    }

    fn vector_state_is_relevant(&self) -> Result<bool> {
        if self.has_vector_mappings
            || self
                .vector
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .is_some()
        {
            return Ok(true);
        }
        Ok(self.vector_index_path().try_exists()?
            || self.vector_doc_ids_path().try_exists()?
            || self.vectors_are_stale()?)
    }

    fn mark_vectors_stale(&self) -> Result<()> {
        let marker_path = self.vectors_stale_path();
        if marker_path.try_exists()? {
            return Ok(());
        }

        let temporary_path = self.vectors_stale_temp_path();
        let mut marker = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)
            .with_context(|| {
                format!("failed to create temporary vectors-stale marker {temporary_path:?}")
            })?;
        marker.write_all(b"stale\n")?;
        marker.sync_all()?;
        drop(marker);
        std::fs::rename(&temporary_path, &marker_path).with_context(|| {
            format!(
                "failed to atomically install vectors-stale marker {marker_path:?} from {temporary_path:?}"
            )
        })?;
        File::open(&self.data_dir)?.sync_all()?;
        Ok(())
    }

    fn clear_vectors_stale(&self) -> Result<()> {
        let mut removed = false;
        for path in [self.vectors_stale_path(), self.vectors_stale_temp_path()] {
            match std::fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to remove vectors-stale marker {path:?}")
                    });
                }
            }
        }
        if removed {
            File::open(&self.data_dir)?.sync_all()?;
        }
        Ok(())
    }

    fn prepare_vector_rebuild(&self, force_text_replay: bool) -> Result<bool> {
        if !self.vector_state_is_relevant()? {
            return Ok(false);
        }
        let stale = self.vectors_are_stale()?;
        let writer_requires_rebuild = self.text.writer_requires_rebuild();
        if force_text_replay || writer_requires_rebuild {
            self.mark_vectors_stale()?;
        }
        Ok(stale || force_text_replay || writer_requires_rebuild)
    }

    fn record_vector_staleness_after_text_failure(
        &self,
        context: &str,
        error: anyhow::Error,
    ) -> anyhow::Error {
        if !self.text.writer_requires_rebuild() {
            return error;
        }
        let marker_result = self.vector_state_is_relevant().and_then(|relevant| {
            if relevant {
                self.mark_vectors_stale()
            } else {
                Ok(())
            }
        });
        match marker_result {
            Ok(()) => error,
            Err(marker_error) => marker_error.context(format!(
                "failed to persist vectors-stale state after {context} failed: {error:#}"
            )),
        }
    }

    /// Ensure a vector index exists with the given dimensions.
    /// Creates one if it doesn't exist, or returns the existing one.
    fn ensure_vector_index(&self, dimensions: usize) -> Result<()> {
        {
            let vi = self.vector.read().unwrap_or_else(|e| e.into_inner());
            if vi.is_some() {
                return Ok(());
            }
        }

        let vector_path = self.vector_index_path();
        let vi = VectorIndex::open(&vector_path, dimensions, usearch::ffi::MetricKind::Cos)?;

        let mut guard = self.vector.write().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(vi);
        }
        Ok(())
    }

    fn detect_vector_mutation(
        &self,
        payload: &serde_json::Value,
        expected_dimensions: Option<usize>,
    ) -> Result<PreparedVectorMutation> {
        if let Some(obj) = payload.as_object() {
            for (field, value) in obj {
                if let Some(arr) = value.as_array()
                    && !self.text.is_keyword_field(field)
                {
                    let floats: Option<Vec<f32>> =
                        arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
                    if let Some(vector) = floats
                        && !vector.is_empty()
                    {
                        if let Some(expected) = expected_dimensions
                            && expected != vector.len()
                        {
                            return Ok(PreparedVectorMutation::SkipShapeMismatch {
                                field: field.clone(),
                                expected,
                                actual: vector.len(),
                            });
                        }
                        return Ok(PreparedVectorMutation::Index { vector });
                    }
                }
            }
        }
        Ok(PreparedVectorMutation::None)
    }

    fn prepare_vector_mutation(
        &self,
        payload: &serde_json::Value,
    ) -> Result<PreparedVectorMutation> {
        let expected_dimensions = self
            .vector
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(VectorIndex::dimensions);
        let prepared = self.detect_vector_mutation(payload, expected_dimensions)?;
        if let PreparedVectorMutation::Index { vector } = &prepared {
            self.ensure_vector_index(vector.len())?;
        }
        Ok(prepared)
    }

    fn apply_prepared_vector_mutation_to_index(
        index: Option<&VectorIndex>,
        operation: &super::SequencedOperation,
        prepared: &PreparedVectorMutation,
    ) -> Result<()> {
        match (prepared, &operation.mutation) {
            (
                PreparedVectorMutation::Index { vector },
                super::DocumentMutation::Index { doc_id, .. },
            ) => {
                let index =
                    index.ok_or_else(|| anyhow::anyhow!("prepared vector index disappeared"))?;
                index.apply_index(doc_id, vector, operation.seq_no, operation.primary_term)?;
            }
            (PreparedVectorMutation::Delete, super::DocumentMutation::Delete { doc_id }) => {
                if let Some(index) = index {
                    index.apply_delete(doc_id, operation.seq_no, operation.primary_term)?;
                }
            }
            (
                PreparedVectorMutation::SkipShapeMismatch {
                    field,
                    expected,
                    actual,
                },
                super::DocumentMutation::Index { doc_id, .. },
            ) => {
                tracing::warn!(
                    document_id = doc_id,
                    field,
                    expected,
                    actual,
                    "Skipping vector mutation because the numeric array dimension does not match"
                );
            }
            (PreparedVectorMutation::None, _)
            | (PreparedVectorMutation::Delete, _)
            | (PreparedVectorMutation::Index { .. }, _)
            | (PreparedVectorMutation::SkipShapeMismatch { .. }, _) => {}
        }
        Ok(())
    }

    fn apply_prepared_vector_mutation(
        &self,
        operation: &super::SequencedOperation,
        prepared: &PreparedVectorMutation,
    ) -> Result<()> {
        if let PreparedVectorMutation::Index { vector } = prepared {
            self.ensure_vector_index(vector.len())?;
        }
        let guard = self
            .vector
            .read()
            .unwrap_or_else(|error| error.into_inner());
        Self::apply_prepared_vector_mutation_to_index(guard.as_ref(), operation, prepared)
    }

    fn apply_vector_mutation_after_rebuild(
        &self,
        operation: &super::SequencedOperation,
        prepared: &PreparedVectorMutation,
    ) -> Result<()> {
        match self.apply_prepared_vector_mutation(operation, prepared) {
            Ok(()) => Ok(()),
            Err(error) => match self.mark_vectors_stale() {
                Ok(()) => Err(error),
                Err(marker_error) => Err(marker_error.context(format!(
                    "failed to persist vectors-stale state after post-rebuild vector apply failed: {error:#}"
                ))),
            },
        }
    }

    fn persist_vector_index(&self, index: &VectorIndex) -> Result<()> {
        let vector_path = self.vector_index_path();
        index.save(&vector_path)?;
        File::open(&vector_path)?.sync_all()?;
        File::open(self.vector_doc_ids_path())?.sync_all()?;
        File::open(&self.data_dir)?.sync_all()?;
        Ok(())
    }

    fn remove_persisted_vector_index(&self) -> Result<()> {
        let mut removed = false;
        for path in [self.vector_index_path(), self.vector_doc_ids_path()] {
            match std::fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to remove vector index file {path:?}"));
                }
            }
        }
        if removed {
            File::open(&self.data_dir)?.sync_all()?;
        }
        Ok(())
    }

    /// Save the vector index to disk (called during flush).
    fn save_vectors(&self) -> Result<()> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref index) = *guard {
            self.persist_vector_index(index)?;
        }
        Ok(())
    }

    /// Best-effort vector persistence used by background auto-flush.
    /// Returns `Ok(false)` when the vector lock is poisoned-and-unrecoverable.
    fn try_save_vectors(&self) -> Result<bool> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref index) = *guard {
            self.persist_vector_index(index)?;
        }
        Ok(true)
    }

    fn rebuild_vectors_locked(&self) -> Result<()> {
        let mut rebuilt = None;
        let mut vector_count = 0;
        self.text.for_each_vector_rebuild_batch(|documents| {
            for (doc_id, source, seq_no, primary_term) in documents {
                let expected_dimensions = rebuilt.as_ref().map(VectorIndex::dimensions);
                let prepared = self.detect_vector_mutation(&source, expected_dimensions)?;
                if let PreparedVectorMutation::Index { vector } = &prepared
                    && rebuilt.is_none()
                {
                    rebuilt = Some(VectorIndex::new(
                        vector.len(),
                        usearch::ffi::MetricKind::Cos,
                    )?);
                }
                let operation = super::SequencedOperation {
                    seq_no,
                    primary_term,
                    mutation: super::DocumentMutation::Index { doc_id, source },
                };
                Self::apply_prepared_vector_mutation_to_index(
                    rebuilt.as_ref(),
                    &operation,
                    &prepared,
                )?;
            }
            Ok(())
        })?;

        if let Some(index) = rebuilt.as_ref() {
            vector_count = index.len();
            self.persist_vector_index(index)?;
        } else {
            self.remove_persisted_vector_index()?;
        }
        *self
            .vector
            .write()
            .unwrap_or_else(|error| error.into_inner()) = rebuilt;
        self.clear_vectors_stale()?;

        if vector_count > 0 {
            tracing::info!("Rebuilt vector index: {} vectors recovered", vector_count);
        }
        Ok(())
    }

    /// Rebuild the vector index from the authoritative Tantivy document view.
    /// The rebuild is persisted before durable stale state is cleared.
    pub fn rebuild_vectors(&self) -> Result<()> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.rebuild_vectors_locked()
    }
}

impl SearchEngine for CompositeEngine {
    #[cfg(test)]
    fn inject_wal_write_failures_for_test(&self, raw_os_error: i32, attempts: usize) {
        self.text
            .inject_wal_write_failures_for_test(raw_os_error, attempts);
    }

    #[cfg(test)]
    fn inject_writer_replacement_failures_for_test(&self, raw_os_error: i32, attempts: usize) {
        self.text
            .inject_writer_replacement_failures_for_test(raw_os_error, attempts);
    }

    #[cfg(test)]
    fn inject_engine_apply_failures_for_test(&self, raw_os_error: i32, attempts: usize) {
        self.text
            .inject_engine_apply_failures_for_test(raw_os_error, attempts);
    }

    #[cfg(test)]
    fn writer_is_failed_for_test(&self) -> bool {
        self.text.writer_is_failed_for_test()
    }

    fn add_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
    ) -> Result<super::IndexWriteReceipt> {
        self.add_document_with_condition_at_term(
            doc_id,
            payload,
            primary_term,
            super::WriteCondition::Unconditional,
        )
    }

    fn add_document_with_condition_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        condition: super::WriteCondition,
    ) -> Result<super::IndexWriteReceipt> {
        crate::common::validate_document_source(&payload)?;
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prepared = self.prepare_vector_mutation(&payload)?;
        let source_for_rebuild = payload.clone();
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let receipt = match self.text.add_primary_index_with_condition_and_side_effect(
            doc_id,
            payload,
            primary_term,
            condition,
            |operation| self.apply_prepared_vector_mutation(operation, &prepared),
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(self.record_vector_staleness_after_text_failure(
                    "primary document indexing",
                    error,
                ));
            }
        };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
            self.apply_vector_mutation_after_rebuild(
                &super::SequencedOperation {
                    seq_no: receipt.seq_no,
                    primary_term: receipt.primary_term,
                    mutation: super::DocumentMutation::Index {
                        doc_id: receipt.doc_id.clone(),
                        source: source_for_rebuild,
                    },
                },
                &prepared,
            )?;
        }
        self.update_local_checkpoint(receipt.seq_no);
        Ok(receipt)
    }

    fn bulk_add_documents_with_receipt_at_term(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        primary_term: u64,
    ) -> Result<super::BulkWriteReceipt> {
        for (_, payload) in &docs {
            crate::common::validate_document_source(payload)?;
        }
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prepared = docs
            .iter()
            .map(|(_, payload)| self.prepare_vector_mutation(payload))
            .collect::<Result<Vec<_>>>()?;
        let docs_for_rebuild = docs.clone();
        let prepared_for_rebuild = prepared.clone();
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;

        let mut prepared = prepared.into_iter();
        let receipt =
            match self
                .text
                .add_primary_bulk_with_side_effect(docs, primary_term, |operation| {
                    let prepared = prepared
                        .next()
                        .expect("primary bulk vector preparation matches operation order");
                    self.apply_prepared_vector_mutation(operation, &prepared)
                }) {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Err(self.record_vector_staleness_after_text_failure(
                        "primary bulk indexing",
                        error,
                    ));
                }
            };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
            if let Some(start_seq_no) = receipt.start_seq_no {
                for (offset, ((doc_id, source), prepared)) in docs_for_rebuild
                    .into_iter()
                    .zip(prepared_for_rebuild.iter())
                    .enumerate()
                {
                    self.apply_vector_mutation_after_rebuild(
                        &super::SequencedOperation {
                            seq_no: start_seq_no + offset as u64,
                            primary_term: receipt.primary_term,
                            mutation: super::DocumentMutation::Index { doc_id, source },
                        },
                        prepared,
                    )?;
                }
            }
        }
        if let Some(last_seq_no) = receipt.last_seq_no()? {
            self.update_local_checkpoint(last_seq_no);
        }
        Ok(receipt)
    }

    fn delete_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
    ) -> Result<super::DeleteWriteReceipt> {
        self.delete_document_with_condition_at_term(
            doc_id,
            primary_term,
            super::WriteCondition::Unconditional,
        )
    }

    fn delete_document_with_condition_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
        condition: super::WriteCondition,
    ) -> Result<super::DeleteWriteReceipt> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let receipt = match self.text.delete_primary_with_condition_and_side_effect(
            doc_id,
            primary_term,
            condition,
            |operation| {
                self.apply_prepared_vector_mutation(operation, &PreparedVectorMutation::Delete)
            },
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(self.record_vector_staleness_after_text_failure(
                    "primary document deletion",
                    error,
                ));
            }
        };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
            self.apply_vector_mutation_after_rebuild(
                &super::SequencedOperation {
                    seq_no: receipt.seq_no,
                    primary_term: receipt.primary_term,
                    mutation: super::DocumentMutation::Delete {
                        doc_id: doc_id.to_string(),
                    },
                },
                &PreparedVectorMutation::Delete,
            )?;
        }
        self.update_local_checkpoint(receipt.seq_no);
        Ok(receipt)
    }

    fn apply_replica_operation(
        &self,
        operation: super::SequencedOperation,
    ) -> Result<super::ReplicaApplyReceipt> {
        if let super::DocumentMutation::Index { source, .. } = &operation.mutation {
            crate::common::validate_document_source(source)?;
        }
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prepared = match &operation.mutation {
            super::DocumentMutation::Index { source, .. } => {
                self.prepare_vector_mutation(source)?
            }
            super::DocumentMutation::Delete { .. } => PreparedVectorMutation::Delete,
            super::DocumentMutation::NoOp { .. } => PreparedVectorMutation::None,
        };
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let operation_for_rebuild = operation.clone();
        let receipt = match self
            .text
            .apply_sequenced_operation_with_side_effect(operation, |operation| {
                self.apply_prepared_vector_mutation(operation, &prepared)
            }) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(self
                    .record_vector_staleness_after_text_failure("replica operation apply", error));
            }
        };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
            if receipt.outcome == super::ApplyOutcome::Applied {
                self.apply_vector_mutation_after_rebuild(&operation_for_rebuild, &prepared)?;
            }
        }
        self.record_replica_persisted_checkpoint(receipt.sequence.persisted_checkpoint);
        Ok(receipt)
    }

    fn apply_replica_batch(
        &self,
        operations: Vec<super::SequencedOperation>,
    ) -> Result<super::ReplicaBulkApplyReceipt> {
        for operation in &operations {
            if let super::DocumentMutation::Index { source, .. } = &operation.mutation {
                crate::common::validate_document_source(source)?;
            }
        }
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prepared_by_identity = operations
            .iter()
            .map(|operation| {
                let prepared = match &operation.mutation {
                    super::DocumentMutation::Index { source, .. } => {
                        self.prepare_vector_mutation(source)?
                    }
                    super::DocumentMutation::Delete { .. } => PreparedVectorMutation::Delete,
                    super::DocumentMutation::NoOp { .. } => PreparedVectorMutation::None,
                };
                Ok(((operation.primary_term, operation.seq_no), prepared))
            })
            .collect::<Result<std::collections::HashMap<_, _>>>()?;
        let mut apply_prepared = prepared_by_identity.clone();
        let operations_for_rebuild = operations.clone();
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let receipt =
            match self
                .text
                .apply_sequenced_batch_with_side_effect(operations, true, |operation| {
                    let prepared = apply_prepared
                        .remove(&(operation.primary_term, operation.seq_no))
                        .expect("prepared vector mutation must match the operation");
                    self.apply_prepared_vector_mutation(operation, &prepared)
                }) {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Err(self
                        .record_vector_staleness_after_text_failure("replica batch apply", error));
                }
            };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
            for (operation, outcome) in operations_for_rebuild.iter().zip(&receipt.outcomes) {
                if *outcome == super::ApplyOutcome::Applied {
                    let prepared = prepared_by_identity
                        .get(&(operation.primary_term, operation.seq_no))
                        .expect("prepared vector mutation must match the operation");
                    self.apply_vector_mutation_after_rebuild(operation, prepared)?;
                }
            }
        }
        self.record_replica_persisted_checkpoint(receipt.sequence.persisted_checkpoint);
        Ok(receipt)
    }

    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>> {
        self.text.get_document(doc_id)
    }

    fn get_document_with_metadata(
        &self,
        doc_id: &str,
        realtime: bool,
    ) -> Result<Option<super::DocumentRead>> {
        self.text.get_document_with_metadata(doc_id, realtime)
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_documents(&self) -> Result<Vec<(String, serde_json::Value, u64, u64)>> {
        self.text.protocol_trace_documents_snapshot()
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_processed_sequences(&self) -> Result<Vec<u64>> {
        self.text.protocol_trace_processed_sequences()
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_copy_evidence(&self) -> Result<super::ProtocolTraceCopyEvidence> {
        self.text.protocol_trace_copy_evidence()
    }

    fn refresh(&self) -> Result<()> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let pruned = match self.text.refresh_with_pruned_tombstones() {
            Ok(pruned) => pruned,
            Err(error) => {
                return Err(self.record_vector_staleness_after_text_failure("refresh", error));
            }
        };
        if rebuild_vectors {
            return self.rebuild_vectors_locked();
        }
        if let Some(index) = self
            .vector
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            for tombstone in pruned {
                index.prune_tombstone(tombstone.key, tombstone.seq_no, tombstone.primary_term);
            }
        }
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        if let Err(error) = self.text.flush() {
            return Err(self.record_vector_staleness_after_text_failure("flush", error));
        }
        if rebuild_vectors {
            self.rebuild_vectors_locked()
        } else {
            self.save_vectors()
        }
    }

    fn flush_with_global_checkpoint(&self) -> Result<()> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let result = if let Some(truncation_checkpoint) = self.safe_truncation_checkpoint() {
            self.text
                .flush_with_global_checkpoint(truncation_checkpoint)
        } else {
            self.text.flush_without_truncation()
        };
        if let Err(error) = result {
            return Err(
                self.record_vector_staleness_after_text_failure("checkpoint-aware flush", error)
            );
        }
        if rebuild_vectors {
            self.rebuild_vectors_locked()
        } else {
            self.save_vectors()
        }
    }

    fn force_merge(&self, max_num_segments: usize) -> Result<()> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        if let Err(error) = self.text.force_merge(max_num_segments) {
            return Err(self.record_vector_staleness_after_text_failure("force merge", error));
        }
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
        }
        Ok(())
    }

    fn segment_infos(&self) -> Vec<super::SegmentInfo> {
        self.text.segment_infos()
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        self.text.search(query_str)
    }

    fn search_query(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<(
        Vec<serde_json::Value>,
        usize,
        std::collections::HashMap<String, crate::search::PartialAggResult>,
    )> {
        self.text.search_query(req)
    }

    fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<Option<super::SqlBatchResult>> {
        Ok(Some(self.text.sql_record_batch(
            req,
            columns,
            needs_id,
            needs_score,
        )?))
    }

    fn sql_streaming_batch_handle(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<Option<super::SqlStreamingBatchHandle>> {
        <HotEngine as super::SearchEngine>::sql_streaming_batch_handle(
            &self.text,
            req,
            columns,
            needs_id,
            needs_score,
            batch_size,
        )
    }

    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<serde_json::Value>> {
        self.search_knn_filtered(field, vector, k, None)
    }

    fn search_knn_filtered(
        &self,
        field: &str,
        vector: &[f32],
        k: usize,
        filter: Option<&crate::search::QueryClause>,
    ) -> Result<Vec<serde_json::Value>> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        let vi = match *guard {
            Some(ref vi) => vi,
            None => return Ok(vec![]),
        };

        // When a filter is present, oversample to get enough candidates that
        // pass the filter. We fetch k * OVERSAMPLE_FACTOR candidates from the
        // vector index, then post-filter against the Tantivy query.
        const OVERSAMPLE_FACTOR: usize = 10;
        let fetch_k = if filter.is_some() {
            std::cmp::min(k * OVERSAMPLE_FACTOR, vi.len())
        } else {
            k
        };

        let (keys, distances) = vi.search(vector, fetch_k)?;

        // Build the allowed doc_id set if a filter is present
        let allowed_ids = match filter {
            Some(clause) => Some(self.text.matching_doc_ids(clause)?),
            None => None,
        };

        let mut hits = Vec::with_capacity(k);
        for (key, distance) in keys.iter().zip(distances.iter()) {
            if hits.len() >= k {
                break;
            }
            let doc_id = vi.doc_id_for_key(*key).unwrap_or_else(|| key.to_string());

            // Skip docs that don't pass the filter
            if let Some(ref allowed) = allowed_ids
                && !allowed.contains(&doc_id)
            {
                continue;
            }

            let source = self.text.get_document(&doc_id).ok().flatten();
            hits.push(serde_json::json!({
                "_id": doc_id,
                "_score": 1.0 / (1.0 + distance),
                "_source": source,
                "_knn_field": field,
                "_knn_distance": distance,
            }));
        }

        Ok(hits)
    }

    fn doc_count(&self) -> u64 {
        self.text.doc_count()
    }

    fn local_checkpoint(&self) -> Option<u64> {
        self.text.sequence_stats().processed_checkpoint
    }

    fn update_local_checkpoint(&self, seq_no: u64) {
        self.text.update_local_checkpoint_compat(seq_no);
    }

    fn sequence_stats(&self) -> super::SequenceStats {
        self.text.sequence_stats()
    }

    fn wal_max_seq_no(&self) -> Option<u64> {
        self.text.wal_max_seq_no()
    }

    fn reconcile_term_sequence_state(
        &self,
        identity_fence: u64,
        identity_fence_max_seq_no: Option<u64>,
    ) -> Result<()> {
        self.text
            .reconcile_term_sequence_state(identity_fence, identity_fence_max_seq_no)
    }

    fn current_primary_term(&self) -> u64 {
        self.text.current_primary_term()
    }

    fn global_checkpoint(&self) -> Option<u64> {
        *self
            .global_cp
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn update_global_checkpoint(&self, checkpoint: u64) {
        let mut global = self
            .global_cp
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *global = Some(global.map_or(checkpoint, |current| current.max(checkpoint)));
    }

    fn create_peer_recovery_snapshot(
        &self,
        snapshot_dir: &std::path::Path,
    ) -> Result<super::PeerRecoverySnapshot> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let snapshot = match self.text.create_peer_recovery_snapshot(snapshot_dir) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(self.record_vector_staleness_after_text_failure(
                    "peer recovery snapshot creation",
                    error,
                ));
            }
        };
        if rebuild_vectors && let Err(error) = self.rebuild_vectors_locked() {
            let release_result = self
                .text
                .release_peer_recovery_pin(snapshot.retention_pin_id);
            let _ = std::fs::remove_dir_all(snapshot_dir);
            if let Err(release_error) = release_result {
                return Err(release_error.context(format!(
                    "failed to release peer recovery pin after vector rebuild failed: {error:#}"
                )));
            }
            return Err(error);
        }
        Ok(snapshot)
    }

    fn prepare_peer_recovery_snapshot(
        &self,
        snapshot_dir: &std::path::Path,
    ) -> Result<super::PeerRecoverySnapshotPreparation> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let preparation = match self.text.prepare_peer_recovery_snapshot(snapshot_dir) {
            Ok(preparation) => preparation,
            Err(error) => {
                return Err(self.record_vector_staleness_after_text_failure(
                    "peer recovery snapshot preparation",
                    error,
                ));
            }
        };
        if rebuild_vectors && let Err(error) = self.rebuild_vectors_locked() {
            drop(preparation);
            let _ = std::fs::remove_dir_all(snapshot_dir);
            return Err(error);
        }
        Ok(preparation)
    }

    fn release_peer_recovery_pin(&self, pin_id: u64) -> Result<()> {
        self.text.release_peer_recovery_pin(pin_id)
    }

    fn peer_recovery_ops(
        &self,
        cursor: crate::wal::WalCursor,
        end_cursor: Option<crate::wal::WalCursor>,
        max_ops: usize,
        max_bytes: usize,
    ) -> Result<super::PeerRecoveryOpsBatch> {
        self.text
            .peer_recovery_ops(cursor, end_cursor, max_ops, max_bytes)
    }

    fn retained_recovery_ops(
        &self,
        min_seq_no: u64,
        max_ops: usize,
        max_bytes: usize,
    ) -> Result<super::PeerRecoveryOpsBatch> {
        self.text
            .retained_recovery_ops(min_seq_no, max_ops, max_bytes)
    }

    fn peer_recovery_barrier(&self) -> Result<super::PeerRecoveryBarrier> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(false)?;
        let barrier = match self.text.peer_recovery_barrier() {
            Ok(barrier) => barrier,
            Err(error) => {
                return Err(
                    self.record_vector_staleness_after_text_failure("peer recovery barrier", error)
                );
            }
        };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
        }
        Ok(barrier)
    }

    fn prepare_primary_activation(
        &self,
        primary_term: u64,
    ) -> Result<Vec<super::SequencedOperation>> {
        let _vector_recovery = self
            .vector_recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rebuild_vectors = self.prepare_vector_rebuild(true)?;
        *self
            .replica_persisted_cp
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        let operations = match self.text.prepare_primary_activation(primary_term) {
            Ok(operations) => operations,
            Err(error) => {
                return Err(
                    self.record_vector_staleness_after_text_failure("primary activation", error)
                );
            }
        };
        if rebuild_vectors {
            self.rebuild_vectors_locked()?;
        }
        Ok(operations)
    }

    fn peer_recovery_commit_files(&self) -> Result<Vec<String>> {
        self.text.peer_recovery_commit_files()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_engine() -> (tempfile::TempDir, CompositeEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        (dir, engine)
    }

    // ── Basic operations ────────────────────────────────────────────────

    #[test]
    fn new_engine_is_empty() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.doc_count(), 0);
    }

    #[test]
    fn add_and_get_text_document() {
        let (_dir, engine) = create_engine();
        let id = engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
        assert_eq!(id, "d1");
        engine.refresh().unwrap();

        let doc = engine.get_document("d1").unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["title"], "hello world");
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn delete_document_removes_it() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "test"})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        engine.delete_document("d1").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 0);
    }

    #[test]
    fn text_search_works() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine.refresh().unwrap();

        let results = engine.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn dsl_search_works() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Vector operations ───────────────────────────────────────────────

    #[test]
    fn add_document_with_vector_auto_indexes() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python", "embedding": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Vector index should have been created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_some(), "vector index should be auto-created");
        assert_eq!(guard.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn search_knn_returns_nearest_neighbors() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python", "embedding": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document("d3", json!({"title": "go", "embedding": [0.9, 0.1, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "d1", "exact match should be first");
        assert_eq!(hits[1]["_id"], "d3", "close vector should be second");
        assert!(
            hits[0]["_knn_distance"].as_f64().unwrap() < hits[1]["_knn_distance"].as_f64().unwrap()
        );
    }

    #[test]
    fn search_knn_returns_source() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);
        let source = hits[0].get("_source").unwrap();
        assert_eq!(source["title"], "rust");
    }

    #[test]
    fn search_knn_no_vector_index_returns_empty() {
        let (_dir, engine) = create_engine();
        // Only text, no vector fields
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 5).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn bulk_add_with_vectors() {
        let (_dir, engine) = create_engine();
        let docs = vec![
            ("d1".into(), json!({"title": "a", "vec": [1.0, 0.0]})),
            ("d2".into(), json!({"title": "b", "vec": [0.0, 1.0]})),
        ];
        let ids = engine.bulk_add_documents(docs).unwrap();
        assert_eq!(ids.len(), 2);
        engine.refresh().unwrap();

        let guard = engine.vector.read().unwrap();
        assert!(guard.is_some());
        assert_eq!(guard.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn delete_removes_from_vector_index() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "a", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "b", "embedding": [0.0, 1.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        engine.delete_document("d1").unwrap();

        // knn search should not find d1 anymore
        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 5).unwrap();
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(
            !ids.contains(&"d1"),
            "deleted doc should not appear in knn results"
        );
    }

    // ── Flush & persistence ─────────────────────────────────────────────

    #[test]
    fn flush_saves_vector_index_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let vector_path = dir.path().join("vectors.usearch");

        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        assert!(
            vector_path.exists(),
            "flush should save vectors.usearch to disk"
        );
    }

    #[test]
    fn flush_saves_doc_id_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar_path = dir.path().join("vectors.docids.bin");

        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("doc_one", json!({"emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("doc_two", json!({"emb": [0.0, 1.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        assert!(
            sidecar_path.exists(),
            "flush should save doc_id sidecar alongside vector index"
        );
    }

    #[test]
    fn flush_reopen_knn_preserves_doc_ids() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: add docs with vectors, flush everything
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("alpha", json!({"title": "a", "emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("beta", json!({"title": "b", "emb": [0.0, 1.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: reopen, rebuild vectors, knn should return correct doc_ids
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            assert_eq!(engine.doc_count(), 2);
            engine.rebuild_vectors().unwrap();

            let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
            assert_eq!(hits.len(), 2);
            assert_eq!(hits[0]["_id"], "alpha");
            assert_eq!(hits[1]["_id"], "beta");
        }
    }

    #[test]
    fn rebuild_vectors_recovers_from_tantivy() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: add docs with vectors, flush text only (not vectors)
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"title": "a", "emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("d2", json!({"title": "b", "emb": [0.0, 1.0, 0.0]}))
                .unwrap();
            // Only flush text — don't save vectors
            engine.text.flush().unwrap();
        }

        // Phase 2: reopen engine — vectors are gone but text is persisted
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            assert_eq!(engine.doc_count(), 2, "text docs should survive restart");

            // Before rebuild, no vector index
            assert!(engine.vector.read().unwrap().is_none());

            // Rebuild vectors from Tantivy's _source
            engine.rebuild_vectors().unwrap();

            // Now knn should work
            let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
            assert_eq!(hits.len(), 2, "rebuild should restore vector search");
            assert_eq!(hits[0]["_id"], "d1");
        }
    }

    fn vector_engine(dir: &std::path::Path) -> CompositeEngine {
        use crate::cluster::state::{FieldMapping, FieldType};
        CompositeEngine::new_with_mappings(
            dir,
            Duration::from_secs(60),
            &std::collections::HashMap::from([(
                "emb".into(),
                FieldMapping {
                    field_type: FieldType::KnnVector,
                    dimension: Some(3),
                },
            )]),
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap()
    }

    fn vector_state_after_failed_write(
        between: impl FnOnce(&CompositeEngine),
    ) -> (Option<u64>, serde_json::Value) {
        let dir = tempfile::tempdir().unwrap();
        let engine = vector_engine(dir.path());
        engine
            .add_document_with_receipt_at_term("doc", json!({"emb": [1.0, 0.0, 0.0]}), 1)
            .unwrap();
        engine.refresh().unwrap();
        engine.inject_engine_apply_failures_for_test(5, 1);
        assert!(
            engine
                .add_document_with_receipt_at_term("doc", json!({"emb": [0.0, 1.0, 0.0]}), 1)
                .is_err()
        );
        between(&engine);
        engine
            .add_document_with_receipt_at_term("trigger", json!({"emb": [0.0, 0.0, 1.0]}), 1)
            .unwrap();

        let vector_version = engine
            .vector
            .read()
            .unwrap()
            .as_ref()
            .and_then(|index| index.version_for_test("doc"))
            .map(|version| version.seq_no);
        let source = engine.get_document("doc").unwrap().unwrap()["emb"].clone();
        (vector_version, source)
    }

    #[test]
    fn refresh_rebuild_recovers_vector_state_before_next_write() {
        let (vector_version, source) =
            vector_state_after_failed_write(|engine| engine.refresh().unwrap());

        assert_eq!(vector_version, Some(1));
        assert_eq!(source, json!([0.0, 1.0, 0.0]));
    }

    #[test]
    fn primary_activation_replay_recovers_vector_state_before_next_write() {
        let (vector_version, source) = vector_state_after_failed_write(|engine| {
            engine.prepare_primary_activation(1).unwrap();
        });

        assert_eq!(vector_version, Some(1));
        assert_eq!(source, json!([0.0, 1.0, 0.0]));
    }

    #[test]
    fn vectors_stale_marker_survives_restart_until_rebuild_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("vectors.stale");
        {
            let engine = vector_engine(dir.path());
            engine
                .add_document_with_receipt_at_term("doc", json!({"emb": [1.0, 0.0, 0.0]}), 1)
                .unwrap();
            engine.refresh().unwrap();
            engine.inject_engine_apply_failures_for_test(5, 1);
            assert!(
                engine
                    .add_document_with_receipt_at_term("doc", json!({"emb": [0.0, 1.0, 0.0]}), 1,)
                    .is_err()
            );
            assert!(marker.exists(), "failed text apply must mark vectors stale");
        }

        let engine = vector_engine(dir.path());
        assert!(marker.exists(), "vectors-stale state must survive restart");
        engine.rebuild_vectors().unwrap();
        assert!(
            !marker.exists(),
            "successful full vector rebuild must clear stale state"
        );
        let vector_version = engine
            .vector
            .read()
            .unwrap()
            .as_ref()
            .and_then(|index| index.version_for_test("doc"));
        assert_eq!(vector_version.map(|version| version.seq_no), Some(1));
    }

    #[test]
    fn vector_rebuild_crosses_small_batches_and_skips_deleted_documents() {
        let dir = tempfile::tempdir().unwrap();
        let engine = vector_engine(dir.path());
        let _batch_size = super::super::tantivy::override_vector_rebuild_batch_size_for_test(7);
        let docs = (0..17)
            .map(|index| {
                (
                    format!("d{index}"),
                    json!({"emb": [1.0, index as f32, 0.0]}),
                )
            })
            .collect();
        engine.bulk_add_documents_with_receipt(docs).unwrap();
        engine.delete_document_with_receipt("d5").unwrap();
        engine.refresh().unwrap();

        engine.prepare_primary_activation(1).unwrap();

        let vectors = engine.vector.read().unwrap();
        let vectors = vectors.as_ref().expect("vector index should be rebuilt");
        assert_eq!(vectors.len(), 16);
        assert!(vectors.version_for_test("d5").is_none());
        assert!(vectors.version_for_test("d16").is_some());
    }

    #[test]
    #[ignore = "large >100k vector rebuild regression"]
    fn review_r8_activation_rebuild_keeps_all_vectors_above_100k() {
        let dir = tempfile::tempdir().unwrap();
        let engine = vector_engine(dir.path());
        let total = 100_010usize;
        for start in (0..total).step_by(10_000) {
            let end = (start + 10_000).min(total);
            let docs = (start..end)
                .map(|index| {
                    let x = (index % 997) as f32 + 1.0;
                    (
                        format!("d{index}"),
                        json!({"emb": [x, 1.0, (index % 13) as f32]}),
                    )
                })
                .collect();
            engine.bulk_add_documents_with_receipt(docs).unwrap();
        }
        engine.refresh().unwrap();
        assert_eq!(
            engine.vector.read().unwrap().as_ref().map(VectorIndex::len),
            Some(total)
        );

        let last_id = format!("d{}", total - 1);
        engine.prepare_primary_activation(1).unwrap();

        {
            let vectors = engine.vector.read().unwrap();
            let vectors = vectors.as_ref().expect("vector index should be rebuilt");
            assert_eq!(vectors.len(), total);
            assert!(vectors.version_for_test(&last_id).is_some());
        }
        assert!(engine.get_document(&last_id).unwrap().is_some());
        let last_vector = [
            ((total - 1) % 997) as f32 + 1.0,
            1.0,
            ((total - 1) % 13) as f32,
        ];
        let hits = engine.search_knn("emb", &last_vector, 100).unwrap();
        assert!(hits.iter().any(|hit| hit["_id"] == json!(last_id)));
    }

    #[test]
    fn writer_rebuild_replays_vector_state_before_next_write() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &std::collections::HashMap::from([(
                "emb".into(),
                FieldMapping {
                    field_type: FieldType::KnnVector,
                    dimension: Some(3),
                },
            )]),
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        engine
            .add_document_with_receipt_at_term("doc", json!({"emb": [1.0, 0.0, 0.0]}), 1)
            .unwrap();
        engine.refresh().unwrap();
        engine.inject_engine_apply_failures_for_test(5, 1);
        assert!(
            engine
                .add_document_with_receipt_at_term("doc", json!({"emb": [0.0, 1.0, 0.0]}), 1)
                .is_err()
        );
        engine
            .add_document_with_receipt_at_term("trigger", json!({"emb": [0.0, 0.0, 1.0]}), 1)
            .unwrap();

        let vector_version = engine
            .vector
            .read()
            .unwrap()
            .as_ref()
            .and_then(|index| index.version_for_test("doc"));
        assert_eq!(vector_version.map(|version| version.seq_no), Some(1));
        assert_eq!(
            engine.get_document("doc").unwrap().unwrap()["emb"],
            json!([0.0, 1.0, 0.0])
        );
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn document_without_vectors_skips_vector_indexing() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "no vectors here"}))
            .unwrap();
        engine.refresh().unwrap();

        // Vector index should NOT be created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none(), "no vector index for text-only docs");
    }

    #[test]
    fn mixed_docs_with_and_without_vectors() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "text only"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "with vec", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "text only too"}))
            .unwrap();
        engine.refresh().unwrap();

        assert_eq!(engine.doc_count(), 3);

        // knn should only find d2
        let hits = engine.search_knn("emb", &[1.0, 0.0], 5).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn non_numeric_array_not_treated_as_vector() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"tags": ["rust", "search"]}))
            .unwrap();
        engine.refresh().unwrap();

        // String arrays should NOT create a vector index
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none());
    }

    #[test]
    fn search_knn_includes_score_and_distance() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);

        let score = hits[0]["_score"].as_f64().unwrap();
        let distance = hits[0]["_knn_distance"].as_f64().unwrap();
        assert!(score > 0.99, "exact match should have score ~1.0");
        assert!(distance < 0.001, "exact match should have distance ~0.0");
        assert_eq!(hits[0]["_knn_field"], "emb");
    }

    // ── Hybrid search (text + kNN) ─────────────────────────────────────

    #[test]
    fn hybrid_text_and_knn_both_return_hits() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "rust search engine", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python web framework", "emb": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "rust compiler internals", "emb": [0.0, 0.0, 1.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Text search: "rust" should match d1 and d3
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (text_hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(text_hits.len(), 3, "match_all should return all 3 docs");

        // kNN search: vector closest to [1.0, 0.0, 0.0] should be d1
        let knn_hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(knn_hits.len(), 2);
        assert_eq!(knn_hits[0]["_id"], "d1", "d1 should be nearest neighbor");

        // Both searches independently produce results — confirms hybrid is possible
        assert!(!text_hits.is_empty());
        assert!(!knn_hits.is_empty());
    }

    #[test]
    fn search_query_with_match_finds_docs_via_body_fallback() {
        // Verifies that match queries on named fields fall back to the "body"
        // catch-all and still find documents (since Tantivy schema is dynamic).
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "movie one"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "movie two"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "book three"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Match({
                let mut m = std::collections::HashMap::new();
                m.insert("title".to_string(), json!("movie"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 2, "match on 'movie' should find d1 and d2");
    }

    #[test]
    fn knn_only_search_request_returns_vector_hits() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "doc A", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "doc B", "emb": [0.0, 1.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // kNN-only (no text query clause exercised at engine level)
        let hits = engine.search_knn("emb", &[0.9, 0.1], 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    // ── Pre-filtered kNN search ────────────────────────────────────────

    #[test]
    fn knn_filtered_returns_only_matching_docs() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "rust search", "year": "2020", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python web", "year": "2021", "emb": [0.9, 0.1, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "rust compiler", "year": "2022", "emb": [0.8, 0.2, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Without filter: k=3 should return all 3 docs
        let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(hits.len(), 3);

        // With filter: only docs matching "rust" should be returned
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 3, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 2, "filter should only return d1 and d3");
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
        assert!(
            !ids.contains(&"d2"),
            "d2 ('python web') should be filtered out"
        );
    }

    #[test]
    fn knn_filtered_respects_k_limit() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust a", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust b", "emb": [0.9, 0.1, 0.0]}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust c", "emb": [0.8, 0.2, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        // All 3 match the filter, but k=1 should return only the nearest
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 1, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1", "d1 should be nearest neighbor");
    }

    #[test]
    fn knn_filtered_none_filter_returns_all() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "a", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "b", "emb": [0.0, 1.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // No filter should return same as search_knn
        let hits_unfiltered = engine.search_knn("emb", &[1.0, 0.0], 2).unwrap();
        let hits_none_filter = engine
            .search_knn_filtered("emb", &[1.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(hits_unfiltered.len(), hits_none_filter.len());
    }

    #[test]
    fn knn_filtered_with_no_matches_returns_empty() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust only", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // Filter for "python" — no docs match
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("python"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 5, Some(&filter))
            .unwrap();
        assert!(hits.is_empty(), "no docs match filter, should return empty");
    }

    #[test]
    fn knn_filtered_with_range_filter() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "old movie", "year": "2000", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "new movie", "year": "2025", "emb": [0.95, 0.05, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "mid movie", "year": "2015", "emb": [0.5, 0.5, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Filter: match_all (should get everything — range on dynamic fields
        // won't work without typed fields, so we test with match_all + term)
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("new"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 3, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn knn_filtered_preserves_knn_metadata() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 1, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_knn_field"], "emb");
        assert!(hits[0]["_knn_distance"].as_f64().is_some());
        assert!(hits[0]["_score"].as_f64().unwrap() > 0.0);
        assert!(hits[0]["_source"].is_object());
    }

    // ── Checkpoint tracking ─────────────────────────────────────────────

    #[test]
    fn local_checkpoint_starts_empty() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.local_checkpoint(), None);
    }

    #[test]
    fn update_local_checkpoint_advances() {
        let (_dir, engine) = create_engine();
        for seq_no in 0..=5 {
            engine.update_local_checkpoint(seq_no);
        }
        assert_eq!(engine.local_checkpoint(), Some(5));

        // fetch_max semantics: only advances
        engine.update_local_checkpoint(3);
        assert_eq!(
            engine.local_checkpoint(),
            Some(5),
            "checkpoint should not go backward"
        );

        engine.update_local_checkpoint(10);
        assert_eq!(
            engine.local_checkpoint(),
            Some(5),
            "a gap must hold the checkpoint"
        );
        assert_eq!(engine.sequence_stats().max_seq_no, Some(10));
        for seq_no in 6..10 {
            engine.update_local_checkpoint(seq_no);
        }
        assert_eq!(engine.local_checkpoint(), Some(10));
    }

    #[test]
    fn global_checkpoint_starts_unavailable() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.global_checkpoint(), None);
    }

    #[test]
    fn update_global_checkpoint_stores_value() {
        let (_dir, engine) = create_engine();
        engine.update_global_checkpoint(42);
        assert_eq!(engine.global_checkpoint(), Some(42));
    }

    #[test]
    fn flush_with_global_checkpoint_retains_entries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        // Index 3 documents — each creates a WAL entry
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();
        engine.add_document("d3", json!({"x": 3})).unwrap();

        // Set global checkpoint to 1 (entries 0 and 1 are safe to discard)
        engine.update_global_checkpoint(1);

        // Flush with checkpoint-aware truncation
        engine.flush_with_global_checkpoint().unwrap();

        // Verify documents are still searchable
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 3);
    }

    #[test]
    fn maybe_auto_flush_zero_threshold_disables_auto_flush() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();

        let before = engine.text.translog_size_bytes();
        assert!(before > 0);

        let flushed = engine.maybe_auto_flush(0).unwrap();

        assert!(!flushed);
        assert_eq!(engine.text.translog_size_bytes(), before);
    }

    #[test]
    fn maybe_auto_flush_skips_when_global_checkpoint_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();

        let before = engine.text.translog_size_bytes();
        assert!(before > 0);
        assert_eq!(engine.global_checkpoint(), None);

        let flushed = engine.maybe_auto_flush(1).unwrap();

        assert!(!flushed);
        assert_eq!(engine.text.translog_size_bytes(), before);
    }

    #[test]
    fn maybe_auto_flush_rolls_then_prunes_checkpointed_generations() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();
        engine.add_document("d3", json!({"x": 3})).unwrap();
        engine.update_global_checkpoint(1);

        let before = engine.text.translog_size_bytes();
        let flushed = engine.maybe_auto_flush(1).unwrap();
        let after_first = engine.text.translog_size_bytes();

        assert!(flushed);
        assert!(after_first > 0);
        assert!(after_first >= before);

        engine.add_document("d4", json!({"x": 4})).unwrap();
        engine.update_global_checkpoint(2);

        let before_second = engine.text.translog_size_bytes();
        let flushed_second = engine.maybe_auto_flush(1).unwrap();
        let after_second = engine.text.translog_size_bytes();

        assert!(flushed_second);
        assert!(after_second > 0);
        assert!(after_second < before_second);
    }

    #[test]
    fn review_c3_replica_auto_flush_prunes_only_persisted_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let apply = |seq_no: u64| {
            engine
                .apply_replica_operation(crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"value": seq_no}),
                    },
                })
                .unwrap()
        };

        let first = apply(0);
        assert_eq!(first.sequence.persisted_checkpoint, Some(0));
        assert!(engine.maybe_auto_flush(1).unwrap());
        assert!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .is_empty(),
            "the committed persisted prefix should be pruned"
        );

        let above_gap = apply(2);
        assert_eq!(above_gap.sequence.persisted_checkpoint, Some(0));
        assert!(engine.maybe_auto_flush(1).unwrap());
        assert_eq!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .iter()
                .map(|operation| operation.seq_no)
                .collect::<Vec<_>>(),
            vec![2],
            "auto-flush must retain operations above a permanent gap"
        );

        let filled = apply(1);
        assert_eq!(filled.sequence.persisted_checkpoint, Some(2));
        assert!(engine.maybe_auto_flush(1).unwrap());
        assert!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .is_empty(),
            "once the gap closes, the newly committed persisted prefix may be pruned"
        );
    }

    #[test]
    fn maybe_auto_flush_succeeds_with_vectors_present() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine
            .add_document("d1", json!({"emb": [1.0, 0.0, 0.0], "x": 1}))
            .unwrap();
        engine
            .add_document("d2", json!({"emb": [0.0, 1.0, 0.0], "x": 2}))
            .unwrap();
        engine.update_global_checkpoint(1);

        let flushed = engine.maybe_auto_flush(1).unwrap();

        assert!(flushed, "text + vector auto-flush should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn background_maintenance_tick_does_not_starve_async_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap());

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let lock_engine = engine.clone();
        let holder = std::thread::spawn(move || {
            let _guard = lock_engine.text.writer_lock_for_test();
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });
        locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let maintenance_engine = engine.clone();
        let maintenance = tokio::spawn(async move {
            let _ = started_tx.send(());
            CompositeEngine::run_background_maintenance(maintenance_engine, None).await
        });

        let start = std::time::Instant::now();
        started_rx.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "background maintenance blocked the async runtime for {elapsed:?}"
        );

        maintenance.await.unwrap().unwrap();
        holder.join().unwrap();
    }

    // ── Checkpoint auto-update on writes ────────────────────────────────

    #[test]
    fn add_document_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.local_checkpoint(), None);

        engine.add_document("a", json!({"x": 1})).unwrap();
        assert_eq!(
            engine.local_checkpoint(),
            Some(0),
            "first WAL entry is seq_no 0"
        );

        engine.add_document("b", json!({"x": 2})).unwrap();
        assert_eq!(engine.local_checkpoint(), Some(1));

        engine.add_document("c", json!({"x": 3})).unwrap();
        assert_eq!(engine.local_checkpoint(), Some(2));
    }

    #[test]
    fn bulk_add_documents_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        let docs = vec![
            ("b1".into(), json!({"x": 1})),
            ("b2".into(), json!({"x": 2})),
            ("b3".into(), json!({"x": 3})),
        ];
        engine.bulk_add_documents(docs).unwrap();
        assert_eq!(
            engine.local_checkpoint(),
            Some(2),
            "3 docs → seq_nos 0,1,2 → checkpoint=2"
        );
    }

    #[test]
    fn delete_document_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        let cp_after_add = engine.local_checkpoint();

        engine.delete_document("d1").unwrap();
        assert!(
            engine.local_checkpoint() > cp_after_add,
            "delete should advance checkpoint"
        );
    }

    #[test]
    fn mixed_writes_advance_checkpoint_monotonically() {
        let (_dir, engine) = create_engine();

        engine.add_document("a", json!({"v": 1})).unwrap();
        let cp1 = engine.local_checkpoint();

        engine
            .bulk_add_documents(vec![
                ("b".into(), json!({"v": 2})),
                ("c".into(), json!({"v": 3})),
            ])
            .unwrap();
        let cp2 = engine.local_checkpoint();
        assert!(cp2 > cp1, "bulk after single should advance");

        engine.delete_document("a").unwrap();
        let cp3 = engine.local_checkpoint();
        assert!(cp3 > cp2, "delete after bulk should advance");

        engine.add_document("d", json!({"v": 4})).unwrap();
        let cp4 = engine.local_checkpoint();
        assert!(cp4 > cp3, "add after delete should advance");
    }

    #[test]
    fn explicit_seq_write_updates_local_checkpoint() {
        let (_dir, engine) = create_engine();

        engine
            .apply_replica_operation(super::super::SequencedOperation {
                seq_no: 7,
                primary_term: 1,
                mutation: super::super::DocumentMutation::Index {
                    doc_id: "replica-doc".into(),
                    source: json!({"x": 1}),
                },
            })
            .unwrap();

        assert_eq!(engine.sequence_stats().processed_checkpoint, None);
        assert_eq!(engine.sequence_stats().max_seq_no, Some(7));
    }

    #[test]
    fn explicit_seq_bulk_updates_local_checkpoint() {
        let (_dir, engine) = create_engine();

        engine
            .apply_replica_batch(
                ["b1", "b2", "b3"]
                    .into_iter()
                    .enumerate()
                    .map(|(offset, doc_id)| super::super::SequencedOperation {
                        seq_no: 10 + offset as u64,
                        primary_term: 1,
                        mutation: super::super::DocumentMutation::Index {
                            doc_id: doc_id.into(),
                            source: json!({"x": offset + 1}),
                        },
                    })
                    .collect(),
            )
            .unwrap();

        assert_eq!(engine.sequence_stats().processed_checkpoint, None);
        assert_eq!(engine.sequence_stats().max_seq_no, Some(12));
    }

    #[test]
    fn update_global_checkpoint_never_regresses() {
        let (_dir, engine) = create_engine();
        engine.update_global_checkpoint(10);
        assert_eq!(engine.global_checkpoint(), Some(10));

        engine.update_global_checkpoint(5);
        assert_eq!(engine.global_checkpoint(), Some(10));
    }

    #[test]
    fn primary_receipts_survive_later_writes_and_empty_batches() {
        let (_dir, engine) = create_engine();
        let engine = Arc::new(engine);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_engine = engine.clone();
        let writer_barrier = barrier.clone();
        let first = std::thread::spawn(move || {
            let receipt = writer_engine
                .add_document_with_receipt("first", json!({"body": "first"}))
                .unwrap();
            writer_barrier.wait();
            writer_barrier.wait();
            assert_eq!(writer_engine.local_checkpoint(), Some(3));
            receipt
        });
        barrier.wait();
        let batch = engine
            .bulk_add_documents_with_receipt(vec![
                ("second".into(), json!({"body": "second"})),
                ("third".into(), json!({"body": "third"})),
            ])
            .unwrap();
        assert_eq!(batch.start_seq_no, Some(1));
        assert_eq!(batch.last_seq_no().unwrap(), Some(2));
        let deleted = engine.delete_document_with_receipt("second").unwrap();
        assert_eq!(deleted.seq_no, 3);
        let empty = engine.bulk_add_documents_with_receipt(vec![]).unwrap();
        assert_eq!(empty.start_seq_no, None);
        assert_eq!(engine.local_checkpoint(), Some(3));
        barrier.wait();
        assert_eq!(first.join().unwrap().seq_no, 0);
        assert_eq!(
            engine
                .add_document_with_receipt("fourth", json!({"body": "fourth"}))
                .unwrap()
                .seq_no,
            4
        );
    }

    #[test]
    fn primary_terms_are_preserved_in_receipts_and_wal_entries() {
        let (_dir, engine) = create_engine();
        let index = engine
            .add_document_with_receipt_at_term("one", json!({"value": 1}), 7)
            .unwrap();
        let bulk = engine
            .bulk_add_documents_with_receipt_at_term(vec![("two".into(), json!({"value": 2}))], 7)
            .unwrap();
        let delete = engine
            .delete_document_with_receipt_at_term("one", 7)
            .unwrap();
        assert_eq!(index.primary_term, 7);
        assert_eq!(bulk.primary_term, 7);
        assert_eq!(delete.primary_term, 7);

        let operations = engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations;
        assert_eq!(
            operations
                .iter()
                .map(|entry| entry.primary_term)
                .collect::<Vec<_>>(),
            [7, 7, 7]
        );
    }

    #[test]
    fn numeric_keyword_arrays_do_not_create_vector_indexes() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new_with_mappings(
            directory.path(),
            Duration::from_secs(60),
            &std::collections::HashMap::from([(
                "tags".into(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            )]),
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        engine.add_document("one", json!({"tags": [1, 2]})).unwrap();
        engine
            .bulk_add_documents(vec![("two".into(), json!({"tags": [3, 4]}))])
            .unwrap();
        engine
            .apply_replica_batch(vec![super::super::SequencedOperation {
                seq_no: 10,
                primary_term: 1,
                mutation: super::super::DocumentMutation::Index {
                    doc_id: "replica".into(),
                    source: json!({"tags": [5, 6]}),
                },
            }])
            .unwrap();
        assert!(engine.vector.read().unwrap().is_none());
    }

    #[test]
    fn reserved_document_keys_fail_before_wal_and_reopen_remains_healthy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        for field in [
            "_id",
            "_doc_id",
            "_source",
            "_seq_no",
            "_primary_term",
            "_version",
            "_index",
            "_routing",
        ] {
            let source = json!({ (field): 999 });
            let error = engine
                .add_document_with_receipt("single", source.clone())
                .unwrap_err();
            assert!(
                error.is::<crate::common::ReservedDocumentFieldError>(),
                "{field}: {error:#}"
            );

            let error = engine
                .bulk_add_documents_with_receipt(vec![("bulk".into(), source)])
                .unwrap_err();
            assert!(
                error.is::<crate::common::ReservedDocumentFieldError>(),
                "{field}: {error:#}"
            );
            assert_eq!(engine.sequence_stats().max_seq_no, None);
        }

        engine
            .add_document_with_receipt(
                "healthy",
                json!({"body": "body remains a supported source field", "value": 1}),
            )
            .unwrap();
        engine.refresh().unwrap();
        drop(engine);

        let reopened = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let source = reopened.get_document("healthy").unwrap().unwrap();
        assert_eq!(source["body"], "body remains a supported source field");
        assert_eq!(source["value"], 1);
    }
}
