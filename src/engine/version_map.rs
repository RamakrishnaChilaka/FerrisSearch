use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_VERSION_MAP_MAX_BYTES: usize = 64 * 1024 * 1024;
const ENTRY_OVERHEAD_BYTES: usize = 64;

pub(crate) trait MonotonicClock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Default)]
struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexVersionValue {
    pub seq_no: u64,
    pub primary_term: u64,
    pub wal_position: Option<crate::wal::WalCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeleteVersionValue {
    pub seq_no: u64,
    pub primary_term: u64,
    pub deleted_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionValue {
    Index(IndexVersionValue),
    Delete(DeleteVersionValue),
}

impl VersionValue {
    pub(crate) fn seq_no(self) -> u64 {
        match self {
            Self::Index(value) => value.seq_no,
            Self::Delete(value) => value.seq_no,
        }
    }

    pub(crate) fn primary_term(self) -> u64 {
        match self {
            Self::Index(value) => value.primary_term,
            Self::Delete(value) => value.primary_term,
        }
    }

    fn kind(self) -> &'static str {
        match self {
            Self::Index(_) => "index",
            Self::Delete(_) => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrunedTombstone {
    pub key: u64,
    pub seq_no: u64,
    pub primary_term: u64,
}

pub(crate) type RetiredIndexVersions = HashMap<Box<str>, IndexVersionValue>;

pub(crate) struct LiveVersionMap {
    current: HashMap<Box<str>, IndexVersionValue>,
    old: HashMap<Box<str>, IndexVersionValue>,
    tombstones: HashMap<Box<str>, DeleteVersionValue>,
    reader_visible_checkpoint: Option<u64>,
    estimated_bytes: usize,
    current_bytes: usize,
    old_bytes: usize,
    max_bytes: usize,
    clock: Arc<dyn MonotonicClock>,
}

impl LiveVersionMap {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self::with_clock(max_bytes, Arc::new(SystemMonotonicClock))
    }

    pub(crate) fn with_clock(max_bytes: usize, clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            current: HashMap::new(),
            old: HashMap::new(),
            tombstones: HashMap::new(),
            reader_visible_checkpoint: None,
            estimated_bytes: 0,
            current_bytes: 0,
            old_bytes: 0,
            max_bytes,
            clock,
        }
    }

    pub(crate) fn lookup(&self, doc_id: &str) -> Result<Option<VersionValue>> {
        let mut winner = None;
        if let Some(value) = self.current.get(doc_id) {
            merge_candidate(&mut winner, VersionValue::Index(*value), doc_id)?;
        }
        if let Some(value) = self.old.get(doc_id) {
            merge_candidate(&mut winner, VersionValue::Index(*value), doc_id)?;
        }
        if let Some(value) = self.tombstones.get(doc_id) {
            merge_candidate(&mut winner, VersionValue::Delete(*value), doc_id)?;
        }
        Ok(winner)
    }

    pub(crate) fn apply_index_at(
        &mut self,
        doc_id: &str,
        seq_no: u64,
        primary_term: u64,
        wal_position: crate::wal::WalCursor,
    ) {
        self.insert_index(doc_id, seq_no, primary_term, Some(wal_position));
    }

    #[cfg(test)]
    fn apply_index(&mut self, doc_id: &str, seq_no: u64, primary_term: u64) {
        self.insert_index(doc_id, seq_no, primary_term, None);
    }

    fn insert_index(
        &mut self,
        doc_id: &str,
        seq_no: u64,
        primary_term: u64,
        wal_position: Option<crate::wal::WalCursor>,
    ) {
        if self
            .current
            .insert(
                Box::<str>::from(doc_id),
                IndexVersionValue {
                    seq_no,
                    primary_term,
                    wal_position,
                },
            )
            .is_none()
        {
            self.current_bytes = self
                .current_bytes
                .saturating_add(estimated_entry_bytes(doc_id));
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(estimated_entry_bytes(doc_id));
        }
        if self
            .tombstones
            .get(doc_id)
            .is_some_and(|tombstone| tombstone.seq_no < seq_no)
            && let Some((key, _)) = self.tombstones.remove_entry(doc_id)
        {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(estimated_entry_bytes(&key));
        }
    }

    pub(crate) fn apply_delete(&mut self, doc_id: &str, seq_no: u64, primary_term: u64) {
        if self
            .current
            .get(doc_id)
            .is_some_and(|value| value.seq_no < seq_no)
            && let Some((key, _)) = self.current.remove_entry(doc_id)
        {
            self.current_bytes = self
                .current_bytes
                .saturating_sub(estimated_entry_bytes(&key));
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(estimated_entry_bytes(&key));
        }
        if self
            .old
            .get(doc_id)
            .is_some_and(|value| value.seq_no < seq_no)
            && let Some((key, _)) = self.old.remove_entry(doc_id)
        {
            self.old_bytes = self.old_bytes.saturating_sub(estimated_entry_bytes(&key));
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(estimated_entry_bytes(&key));
        }
        if self
            .tombstones
            .insert(
                Box::<str>::from(doc_id),
                DeleteVersionValue {
                    seq_no,
                    primary_term,
                    deleted_at: self.clock.now(),
                },
            )
            .is_none()
        {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(estimated_entry_bytes(doc_id));
        }
    }

    pub(crate) fn rotate_current_into_old(&mut self) -> Result<()> {
        if self.old.is_empty() {
            std::mem::swap(&mut self.current, &mut self.old);
            std::mem::swap(&mut self.current_bytes, &mut self.old_bytes);
            return Ok(());
        }
        let current = std::mem::take(&mut self.current);
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_sub(std::mem::take(&mut self.current_bytes));
        for (doc_id, value) in current {
            match self.old.get(doc_id.as_ref()).copied() {
                Some(existing) if existing.seq_no > value.seq_no => {}
                Some(existing)
                    if existing.seq_no == value.seq_no
                        && existing.primary_term != value.primary_term =>
                {
                    return Err(VersionMapCollisionError {
                        doc_id: doc_id.into(),
                        seq_no: value.seq_no,
                        existing_term: existing.primary_term,
                        incoming_term: value.primary_term,
                        existing_kind: "index",
                        incoming_kind: "index",
                    }
                    .into());
                }
                _ => {
                    let bytes = estimated_entry_bytes(&doc_id);
                    if self.old.insert(doc_id, value).is_none() {
                        self.old_bytes = self.old_bytes.saturating_add(bytes);
                        self.estimated_bytes = self.estimated_bytes.saturating_add(bytes);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn rollback_refresh(&mut self) -> Result<()> {
        let old = std::mem::take(&mut self.old);
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_sub(std::mem::take(&mut self.old_bytes));
        for (doc_id, value) in old {
            match self.current.get(doc_id.as_ref()).copied() {
                Some(existing) if existing.seq_no > value.seq_no => {}
                Some(existing)
                    if existing.seq_no == value.seq_no
                        && existing.primary_term != value.primary_term =>
                {
                    return Err(VersionMapCollisionError {
                        doc_id: doc_id.into(),
                        seq_no: value.seq_no,
                        existing_term: existing.primary_term,
                        incoming_term: value.primary_term,
                        existing_kind: "index",
                        incoming_kind: "index",
                    }
                    .into());
                }
                _ => {
                    let bytes = estimated_entry_bytes(&doc_id);
                    if self.current.insert(doc_id, value).is_none() {
                        self.current_bytes = self.current_bytes.saturating_add(bytes);
                        self.estimated_bytes = self.estimated_bytes.saturating_add(bytes);
                    }
                }
            }
        }
        Ok(())
    }

    /// Drop the returned retired map only after releasing the version-map lock.
    pub(crate) fn complete_reader_reload(
        &mut self,
        reader_visible_checkpoint: Option<u64>,
        retention: Duration,
    ) -> (Vec<PrunedTombstone>, RetiredIndexVersions) {
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_sub(std::mem::take(&mut self.old_bytes));
        let retired = std::mem::take(&mut self.old);
        self.reader_visible_checkpoint = reader_visible_checkpoint;
        let now = self.clock.now();
        let processed_checkpoint = reader_visible_checkpoint;
        let mut pruned = Vec::new();
        let mut removed_bytes = 0usize;
        self.tombstones.retain(|doc_id, tombstone| {
            let old_enough = now.saturating_duration_since(tombstone.deleted_at) >= retention;
            let checkpoint_safe = processed_checkpoint
                .is_some_and(|checkpoint| tombstone.seq_no <= checkpoint)
                && self
                    .reader_visible_checkpoint
                    .is_some_and(|checkpoint| tombstone.seq_no <= checkpoint);
            let no_newer_index = self
                .current
                .get(doc_id.as_ref())
                .into_iter()
                .chain(self.old.get(doc_id.as_ref()))
                .all(|value| value.seq_no <= tombstone.seq_no);
            let should_prune = old_enough && checkpoint_safe && no_newer_index;
            if should_prune {
                removed_bytes = removed_bytes.saturating_add(estimated_entry_bytes(doc_id));
                pruned.push(PrunedTombstone {
                    key: crate::engine::routing::hash_string(doc_id),
                    seq_no: tombstone.seq_no,
                    primary_term: tombstone.primary_term,
                });
            }
            !should_prune
        });
        self.estimated_bytes = self.estimated_bytes.saturating_sub(removed_bytes);
        (pruned, retired)
    }

    pub(crate) fn reset(&mut self) {
        self.current.clear();
        self.old.clear();
        self.tombstones.clear();
        self.reader_visible_checkpoint = None;
        self.estimated_bytes = 0;
        self.current_bytes = 0;
        self.old_bytes = 0;
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub(crate) fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub(crate) fn current_and_old_are_empty(&self) -> bool {
        self.current.is_empty() && self.old.is_empty()
    }

    pub(crate) fn can_reserve(&self, additional_bytes: usize) -> bool {
        self.estimated_bytes
            .checked_add(additional_bytes)
            .is_some_and(|total| total <= self.max_bytes)
    }

    pub(crate) fn estimate_reservation<'a>(doc_ids: impl IntoIterator<Item = &'a str>) -> usize {
        let mut unique = HashSet::new();
        doc_ids
            .into_iter()
            .filter(|doc_id| unique.insert(*doc_id))
            .map(estimated_entry_bytes)
            .sum()
    }

    #[cfg(feature = "protocol-trace")]
    pub(crate) fn protocol_trace_versions(&self) -> Result<Vec<(String, VersionValue)>> {
        let mut doc_ids = self
            .current
            .keys()
            .chain(self.old.keys())
            .chain(self.tombstones.keys())
            .map(|doc_id| doc_id.to_string())
            .collect::<Vec<_>>();
        doc_ids.sort();
        doc_ids.dedup();
        doc_ids
            .into_iter()
            .map(|doc_id| {
                self.lookup(&doc_id)?
                    .map(|version| (doc_id.clone(), version))
                    .ok_or_else(|| anyhow::anyhow!("version map lost document [{doc_id}]"))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn set_max_bytes_for_test(&mut self, max_bytes: usize) {
        self.max_bytes = max_bytes;
    }

    #[cfg(test)]
    fn full_recount_estimated_bytes(&self) -> usize {
        self.current
            .keys()
            .chain(self.old.keys())
            .chain(self.tombstones.keys())
            .map(|doc_id| estimated_entry_bytes(doc_id))
            .sum()
    }
}

fn estimated_entry_bytes(doc_id: &str) -> usize {
    ENTRY_OVERHEAD_BYTES
        .saturating_add(doc_id.len())
        .saturating_add(size_of::<VersionValue>())
}

fn merge_candidate(
    winner: &mut Option<VersionValue>,
    candidate: VersionValue,
    doc_id: &str,
) -> Result<()> {
    let Some(existing) = *winner else {
        *winner = Some(candidate);
        return Ok(());
    };
    if candidate.seq_no() > existing.seq_no() {
        *winner = Some(candidate);
        return Ok(());
    }
    if candidate.seq_no() < existing.seq_no() {
        return Ok(());
    }
    if candidate.primary_term() != existing.primary_term() || candidate.kind() != existing.kind() {
        return Err(VersionMapCollisionError {
            doc_id: doc_id.to_string(),
            seq_no: candidate.seq_no(),
            existing_term: existing.primary_term(),
            incoming_term: candidate.primary_term(),
            existing_kind: existing.kind(),
            incoming_kind: candidate.kind(),
        }
        .into());
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error(
    "version-map collision for document [{doc_id}] at sequence {seq_no}: existing {existing_kind} term {existing_term}, incoming {incoming_kind} term {incoming_term}"
)]
pub(crate) struct VersionMapCollisionError {
    doc_id: String,
    seq_no: u64,
    existing_term: u64,
    incoming_term: u64,
    existing_kind: &'static str,
    incoming_kind: &'static str,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "version map capacity exceeded: estimated {estimated_bytes} bytes plus reservation {reservation_bytes} exceeds limit {max_bytes}"
)]
pub struct VersionMapCapacityError {
    pub estimated_bytes: usize,
    pub reservation_bytes: usize,
    pub max_bytes: usize,
}

pub(crate) const VERSION_MAP_CAPACITY_STATUS_PREFIX: &str = "version_map_capacity_exceeded: ";

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct ManualClock {
        now: Mutex<Instant>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                now: Mutex::new(Instant::now()),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock().unwrap();
            *now += duration;
        }
    }

    impl MonotonicClock for ManualClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    fn assert_byte_accounting(versions: &LiveVersionMap) {
        assert_eq!(
            versions.current_bytes,
            versions
                .current
                .keys()
                .map(|key| estimated_entry_bytes(key))
                .sum::<usize>()
        );
        assert_eq!(
            versions.old_bytes,
            versions
                .old
                .keys()
                .map(|key| estimated_entry_bytes(key))
                .sum::<usize>()
        );
        assert_eq!(
            versions.estimated_bytes(),
            versions.full_recount_estimated_bytes()
        );
    }

    #[test]
    fn lookup_chooses_maximum_sequence_across_all_windows() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 2, 1);
        versions.rotate_current_into_old().unwrap();
        versions.apply_index("doc", 4, 1);
        versions.apply_delete("doc", 3, 1);
        assert_eq!(
            versions.lookup("doc").unwrap(),
            Some(VersionValue::Index(IndexVersionValue {
                seq_no: 4,
                primary_term: 1,
                wal_position: None,
            }))
        );
    }

    #[test]
    fn equal_sequence_incompatible_candidates_fail_closed() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 2, 1);
        versions.apply_delete("doc", 2, 1);
        let error = versions.lookup("doc").unwrap_err();
        assert!(error.is::<VersionMapCollisionError>());
    }

    #[test]
    fn review_c3_version_map_collision_is_definitive() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 2, 1);
        versions.apply_delete("doc", 2, 1);

        let error = versions.lookup("doc").unwrap_err();
        assert!(crate::shard::ShardManager::is_definitive_copy_failure(
            &error
        ));
    }

    #[test]
    fn failed_refresh_merges_old_back_into_current_by_max_sequence() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 3, 1);
        versions.rotate_current_into_old().unwrap();
        versions.apply_index("doc", 4, 1);
        versions.rollback_refresh().unwrap();
        assert!(versions.old.is_empty());
        assert_eq!(versions.current["doc"].seq_no, 4);
    }

    #[test]
    fn steady_refresh_rotation_preserves_key_allocation_and_version() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 7, 3);
        let key = versions.current.keys().next().unwrap().as_ptr();
        let bytes = versions.estimated_bytes();
        versions.rotate_current_into_old().unwrap();
        assert!(versions.current.is_empty());
        assert_eq!(versions.old.keys().next().unwrap().as_ptr(), key);
        assert_eq!(versions.lookup("doc").unwrap().unwrap().seq_no(), 7);
        assert_eq!(versions.lookup("doc").unwrap().unwrap().primary_term(), 3);
        assert_eq!(versions.estimated_bytes(), bytes);
        assert_eq!(
            versions.estimated_bytes(),
            versions.full_recount_estimated_bytes()
        );
    }

    #[test]
    fn completed_reload_detaches_old_and_keeps_current_accounting() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("doc", 1, 2);
        versions.apply_index("other-old", 2, 2);
        versions.rotate_current_into_old().unwrap();
        versions.apply_index("doc", 3, 2);
        versions.apply_index("new-current", 4, 2);
        let (pruned, retired) = versions.complete_reader_reload(Some(2), Duration::from_secs(60));
        assert!(pruned.is_empty());
        assert_eq!(retired.len(), 2);
        assert_eq!(retired["doc"].seq_no, 1);
        assert!(versions.old.is_empty());
        assert_eq!(versions.old_bytes, 0);
        assert_eq!(versions.lookup("doc").unwrap().unwrap().seq_no(), 3);
        assert_eq!(versions.lookup("new-current").unwrap().unwrap().seq_no(), 4);
        assert_byte_accounting(&versions);
        let bytes = versions.estimated_bytes();
        drop(retired);
        assert_eq!(versions.estimated_bytes(), bytes);
    }

    #[test]
    fn window_byte_accounting_covers_merge_rollback_delete_and_retirement() {
        let clock = Arc::new(ManualClock::new());
        let mut versions = LiveVersionMap::with_clock(usize::MAX, clock.clone());
        for seq_no in 0..500 {
            let doc_id = format!("variable-length-document-{}", seq_no % 17);
            match seq_no % 7 {
                0 | 1 => versions.apply_index(&doc_id, seq_no, 1),
                2 => versions.apply_delete(&doc_id, seq_no, 1),
                3 | 4 => versions.rotate_current_into_old().unwrap(),
                5 => versions.rollback_refresh().unwrap(),
                _ => {
                    let (_, retired) =
                        versions.complete_reader_reload(Some(seq_no), Duration::ZERO);
                    drop(retired);
                }
            }
            assert_byte_accounting(&versions);
            clock.advance(Duration::from_millis(1));
        }
        versions.reset();
        assert_byte_accounting(&versions);
    }

    #[test]
    fn failed_merge_keeps_window_byte_accounting_exact() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("collision", 1, 1);
        versions.rotate_current_into_old().unwrap();
        versions.apply_index("collision", 1, 2);
        versions.apply_index("another-current", 2, 2);
        let error = versions.rotate_current_into_old().unwrap_err();
        assert!(error.is::<VersionMapCollisionError>());
        assert_byte_accounting(&versions);

        versions.apply_index("collision", 1, 2);
        let error = versions.rollback_refresh().unwrap_err();
        assert!(error.is::<VersionMapCollisionError>());
        assert_byte_accounting(&versions);
    }

    #[test]
    fn tombstone_pruning_requires_age_and_visible_checkpoint() {
        let clock = Arc::new(ManualClock::new());
        let mut versions = LiveVersionMap::with_clock(usize::MAX, clock.clone());
        versions.apply_delete("doc", 3, 2);
        assert!(
            versions
                .complete_reader_reload(Some(3), Duration::from_secs(60))
                .0
                .is_empty()
        );
        clock.advance(Duration::from_secs(61));
        let (pruned, _) = versions.complete_reader_reload(Some(2), Duration::from_secs(60));
        assert!(pruned.is_empty());
        let (pruned, _) = versions.complete_reader_reload(Some(3), Duration::from_secs(60));
        assert_eq!(pruned.len(), 1);
        assert!(versions.lookup("doc").unwrap().is_none());
    }

    #[test]
    fn newer_index_prevents_stale_tombstone_prune_notification() {
        let clock = Arc::new(ManualClock::new());
        let mut versions = LiveVersionMap::with_clock(usize::MAX, clock.clone());
        versions.apply_delete("doc", 3, 2);
        versions.apply_index("doc", 4, 2);
        clock.advance(Duration::from_secs(61));
        assert!(
            versions
                .complete_reader_reload(Some(4), Duration::from_secs(60))
                .0
                .is_empty()
        );
        assert_eq!(versions.lookup("doc").unwrap().unwrap().seq_no(), 4);
    }

    #[test]
    fn capacity_estimate_deduplicates_document_ids() {
        let one = LiveVersionMap::estimate_reservation(["same"]);
        let duplicates = LiveVersionMap::estimate_reservation(["same", "same", "same"]);
        assert_eq!(one, duplicates);
    }

    #[test]
    fn reset_clears_all_process_local_state() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        versions.apply_index("a", 0, 1);
        versions.apply_delete("b", 1, 1);
        versions.rotate_current_into_old().unwrap();
        versions.reset();
        assert!(versions.current.is_empty());
        assert!(versions.old.is_empty());
        assert!(versions.tombstones.is_empty());
        assert_eq!(versions.estimated_bytes(), 0);
    }

    #[test]
    fn incremental_byte_accounting_matches_full_recount_and_scales_linearly() {
        let mut versions = LiveVersionMap::new(usize::MAX);
        let started = std::time::Instant::now();
        for seq_no in 0..20_000u64 {
            versions.apply_index(&format!("doc-{seq_no}"), seq_no, 1);
        }
        assert_eq!(
            versions.estimated_bytes(),
            versions.full_recount_estimated_bytes()
        );
        versions.rotate_current_into_old().unwrap();
        assert_eq!(
            versions.estimated_bytes(),
            versions.full_recount_estimated_bytes()
        );
        for seq_no in 0..10_000u64 {
            versions.apply_delete(&format!("doc-{seq_no}"), 20_000 + seq_no, 1);
        }
        assert_eq!(
            versions.estimated_bytes(),
            versions.full_recount_estimated_bytes()
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "incremental accounting regressed from linear time"
        );
    }
}
