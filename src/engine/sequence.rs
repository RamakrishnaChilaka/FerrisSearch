use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

pub(crate) const COMMITTED_BOUNDARY_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceStats {
    pub processed_checkpoint: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
    pub max_seq_no: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalCheckpointTracker {
    processed_checkpoint: Option<u64>,
    processed_above: SeqNoIntervals,
    persisted_checkpoint: Option<u64>,
    persisted_above: SeqNoIntervals,
    max_seq_no: Option<u64>,
}

impl LocalCheckpointTracker {
    pub(crate) fn new(committed: CommittedBoundaryRecord) -> Result<Self> {
        committed.validate()?;
        let mut tracker = Self {
            processed_checkpoint: committed.processed_checkpoint,
            processed_above: SeqNoIntervals::default(),
            persisted_checkpoint: None,
            persisted_above: SeqNoIntervals::default(),
            max_seq_no: committed.max_seq_no,
        };
        tracker.mark_persisted_through(committed.persisted_checkpoint);
        Ok(tracker)
    }

    pub(crate) fn stats(&self) -> SequenceStats {
        SequenceStats {
            processed_checkpoint: self.processed_checkpoint,
            persisted_checkpoint: self.persisted_checkpoint,
            max_seq_no: self.max_seq_no,
        }
    }

    pub(crate) fn has_processed(&self, seq_no: u64) -> bool {
        self.processed_checkpoint
            .is_some_and(|checkpoint| seq_no <= checkpoint)
            || self.processed_above.contains(seq_no)
    }

    pub(crate) fn mark_processed(&mut self, seq_no: u64) {
        if self
            .processed_checkpoint
            .is_some_and(|checkpoint| seq_no <= checkpoint)
        {
            return;
        }
        self.processed_above.insert(seq_no);
        self.advance_processed_checkpoint();
        self.advance_persisted_checkpoint();
    }

    pub(crate) fn mark_persisted(&mut self, seq_no: u64) {
        if self
            .persisted_checkpoint
            .is_some_and(|checkpoint| seq_no <= checkpoint)
        {
            return;
        }
        self.persisted_above.insert(seq_no);
        self.advance_persisted_checkpoint();
    }

    pub(crate) fn advance_max_seq_no(&mut self, seq_no: u64) {
        self.max_seq_no = Some(
            self.max_seq_no
                .map_or(seq_no, |current| current.max(seq_no)),
        );
    }

    /// Record a contiguous durable prefix. Callers must supply a checkpoint
    /// whose WAL bytes are known durable as one prefix, not a maximum observed
    /// sequence number.
    pub(crate) fn mark_persisted_through(&mut self, checkpoint: Option<u64>) {
        let Some(end) = checkpoint else {
            return;
        };
        let Some(start) = next_seq_no(self.persisted_checkpoint) else {
            return;
        };
        if start <= end {
            self.persisted_above.insert_range(start, end);
            self.advance_persisted_checkpoint();
        }
    }

    pub(crate) fn reset_to_commit(&mut self, committed: CommittedBoundaryRecord) -> Result<()> {
        committed.validate()?;
        self.processed_checkpoint = committed.processed_checkpoint;
        self.processed_above = SeqNoIntervals::default();
        self.persisted_checkpoint = None;
        self.persisted_above = SeqNoIntervals::default();
        self.max_seq_no = committed.max_seq_no;
        self.mark_persisted_through(committed.persisted_checkpoint);
        Ok(())
    }

    pub(crate) fn processed_checkpoint(&self) -> Option<u64> {
        self.processed_checkpoint
    }

    pub(crate) fn persisted_checkpoint(&self) -> Option<u64> {
        self.persisted_checkpoint
    }

    pub(crate) fn max_seq_no(&self) -> Option<u64> {
        self.max_seq_no
    }

    pub(crate) fn missing_intervals_through(&self, end: u64) -> Vec<RangeInclusive<u64>> {
        let Some(start) = next_seq_no(self.processed_checkpoint) else {
            return Vec::new();
        };
        self.processed_above.missing_ranges(start, end)
    }

    fn advance_processed_checkpoint(&mut self) {
        while let Some(next) = next_seq_no(self.processed_checkpoint) {
            let Some(end) = self.processed_above.remove_contiguous_from(next, u64::MAX) else {
                break;
            };
            self.processed_checkpoint = Some(end);
        }
    }

    fn advance_persisted_checkpoint(&mut self) {
        while let Some(next) = next_seq_no(self.persisted_checkpoint) {
            let Some(processed_checkpoint) = self.processed_checkpoint else {
                break;
            };
            if next > processed_checkpoint {
                break;
            }
            let Some(end) = self
                .persisted_above
                .remove_contiguous_from(next, processed_checkpoint)
            else {
                break;
            };
            self.persisted_checkpoint = Some(end);
        }
    }
}

fn next_seq_no(checkpoint: Option<u64>) -> Option<u64> {
    match checkpoint {
        Some(checkpoint) => checkpoint.checked_add(1),
        None => Some(0),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SeqNoIntervals {
    ranges: BTreeMap<u64, u64>,
}

impl SeqNoIntervals {
    pub(crate) fn contains(&self, seq_no: u64) -> bool {
        self.ranges
            .range(..=seq_no)
            .next_back()
            .is_some_and(|(_, end)| seq_no <= *end)
    }

    pub(crate) fn insert(&mut self, seq_no: u64) {
        self.insert_range(seq_no, seq_no);
    }

    pub(crate) fn insert_range(&mut self, mut start: u64, mut end: u64) {
        if start > end {
            return;
        }

        if let Some((&previous_start, &previous_end)) = self.ranges.range(..=start).next_back()
            && previous_end
                .checked_add(1)
                .is_none_or(|adjacent_end| adjacent_end >= start)
        {
            start = previous_start;
            end = end.max(previous_end);
            self.ranges.remove(&previous_start);
        }

        loop {
            let next = self
                .ranges
                .range(start..)
                .next()
                .map(|(&next_start, &next_end)| (next_start, next_end));
            let Some((next_start, next_end)) = next else {
                break;
            };
            if end
                .checked_add(1)
                .is_some_and(|adjacent_end| next_start > adjacent_end)
            {
                break;
            }
            end = end.max(next_end);
            self.ranges.remove(&next_start);
        }

        self.ranges.insert(start, end);
    }

    pub(crate) fn ranges(&self) -> Vec<SeqNoRange> {
        self.ranges
            .iter()
            .map(|(&start, &end)| SeqNoRange { start, end })
            .collect()
    }

    pub(crate) fn from_ranges(ranges: &[SeqNoRange]) -> Result<Self> {
        let mut intervals = Self::default();
        let mut previous_end = None;
        for range in ranges {
            if range.start > range.end {
                return Err(sequence_state_corruption(format!(
                    "sequence interval starts at {} after ending at {}",
                    range.start, range.end
                )));
            }
            if previous_end
                .is_some_and(|end: u64| end.checked_add(1).is_none_or(|next| range.start <= next))
            {
                return Err(sequence_state_corruption(
                    "sequence intervals are not normalized",
                ));
            }
            intervals.ranges.insert(range.start, range.end);
            previous_end = Some(range.end);
        }
        Ok(intervals)
    }

    pub(crate) fn missing_ranges(&self, start: u64, end: u64) -> Vec<RangeInclusive<u64>> {
        if start > end {
            return Vec::new();
        }

        let mut missing = Vec::new();
        let mut cursor = start;
        for (&range_start, &range_end) in self.ranges.range(..=end) {
            if range_end < cursor {
                continue;
            }
            if range_start > cursor {
                missing.push(cursor..=range_start - 1);
            }
            let Some(next) = range_end.checked_add(1) else {
                return missing;
            };
            cursor = cursor.max(next);
            if cursor > end {
                return missing;
            }
        }
        if cursor <= end {
            missing.push(cursor..=end);
        }
        missing
    }

    fn remove_contiguous_from(&mut self, start: u64, limit: u64) -> Option<u64> {
        let (&range_start, &range_end) = self.ranges.range(..=start).next_back()?;
        if start < range_start || start > range_end {
            return None;
        }

        self.ranges.remove(&range_start);
        if range_start < start {
            self.ranges.insert(range_start, start - 1);
        }
        let removed_end = range_end.min(limit);
        if removed_end < range_end {
            self.ranges.insert(removed_end + 1, range_end);
        }
        Some(removed_end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeqNoRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrimaryTermSequenceRecord {
    pub current_term: u64,
    pub max_seq_no_at_term_start: Option<u64>,
    pub processed_in_current_term_below_start_max: Vec<SeqNoRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommittedBoundaryRecord {
    pub version: u32,
    pub processed_checkpoint: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
    pub max_seq_no: Option<u64>,
    pub max_seq_no_of_updates_or_deletes: Option<u64>,
    pub legacy_migration_checkpoint: Option<u64>,
    pub term_sequence_state: PrimaryTermSequenceRecord,
}

impl CommittedBoundaryRecord {
    pub(crate) fn empty(current_term: u64) -> Self {
        Self {
            version: COMMITTED_BOUNDARY_FORMAT_VERSION,
            processed_checkpoint: None,
            persisted_checkpoint: None,
            max_seq_no: None,
            max_seq_no_of_updates_or_deletes: None,
            legacy_migration_checkpoint: None,
            term_sequence_state: PrimaryTermSequenceRecord {
                current_term,
                max_seq_no_at_term_start: None,
                processed_in_current_term_below_start_max: Vec::new(),
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != COMMITTED_BOUNDARY_FORMAT_VERSION {
            return Err(UnsupportedCommittedBoundaryVersionError {
                found: self.version,
                expected: COMMITTED_BOUNDARY_FORMAT_VERSION,
            }
            .into());
        }
        if let (Some(persisted), Some(processed)) =
            (self.persisted_checkpoint, self.processed_checkpoint)
            && persisted > processed
        {
            return Err(committed_boundary_corruption(format!(
                "persisted checkpoint {persisted} exceeds processed checkpoint {processed}"
            )));
        }
        if self.persisted_checkpoint.is_some() && self.processed_checkpoint.is_none() {
            return Err(committed_boundary_corruption(
                "persisted checkpoint requires a processed checkpoint",
            ));
        }
        if self.processed_checkpoint.is_some() && self.max_seq_no.is_none() {
            return Err(committed_boundary_corruption(
                "processed checkpoint requires a maximum sequence number",
            ));
        }
        for (name, value) in [
            ("processed checkpoint", self.processed_checkpoint),
            ("persisted checkpoint", self.persisted_checkpoint),
            (
                "maximum sequence number of updates or deletes",
                self.max_seq_no_of_updates_or_deletes,
            ),
            (
                "legacy migration checkpoint",
                self.legacy_migration_checkpoint,
            ),
        ] {
            if let (Some(value), Some(max_seq_no)) = (value, self.max_seq_no)
                && value > max_seq_no
            {
                return Err(committed_boundary_corruption(format!(
                    "{name} {value} exceeds maximum sequence number {max_seq_no}"
                )));
            }
        }
        if self.max_seq_no.is_none()
            && (self.max_seq_no_of_updates_or_deletes.is_some()
                || self.legacy_migration_checkpoint.is_some()
                || self.term_sequence_state.max_seq_no_at_term_start.is_some()
                || !self
                    .term_sequence_state
                    .processed_in_current_term_below_start_max
                    .is_empty())
        {
            return Err(committed_boundary_corruption(
                "empty committed boundary contains sequence metadata",
            ));
        }
        if self.max_seq_no.is_some() && self.term_sequence_state.current_term == 0 {
            return Err(committed_boundary_corruption(
                "non-empty committed boundary has a zero primary term",
            ));
        }
        if let (Some(term_start_max), Some(max_seq_no)) = (
            self.term_sequence_state.max_seq_no_at_term_start,
            self.max_seq_no,
        ) && term_start_max > max_seq_no
        {
            return Err(committed_boundary_corruption(format!(
                "term-start maximum {term_start_max} exceeds maximum sequence number {max_seq_no}"
            )));
        }

        let intervals = SeqNoIntervals::from_ranges(
            &self
                .term_sequence_state
                .processed_in_current_term_below_start_max,
        )?;
        if let Some(term_start_max) = self.term_sequence_state.max_seq_no_at_term_start {
            if intervals
                .ranges
                .values()
                .any(|range_end| *range_end > term_start_max)
            {
                return Err(committed_boundary_corruption(format!(
                    "current-term processed interval exceeds term-start maximum {term_start_max}"
                )));
            }
        } else if !intervals.ranges.is_empty() {
            return Err(committed_boundary_corruption(
                "current-term processed intervals require a term-start maximum",
            ));
        }
        Ok(())
    }

    pub(crate) fn load(path: &Path) -> Result<Option<Self>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let header =
            serde_json::from_slice::<CommittedBoundaryVersionHeader>(&bytes).map_err(|error| {
                committed_boundary_corruption(format!(
                    "decode committed boundary version {path:?}: {error}"
                ))
            })?;
        if header.version != COMMITTED_BOUNDARY_FORMAT_VERSION {
            return Err(UnsupportedCommittedBoundaryVersionError {
                found: header.version,
                expected: COMMITTED_BOUNDARY_FORMAT_VERSION,
            }
            .into());
        }
        let record = serde_json::from_slice::<Self>(&bytes).map_err(|error| {
            committed_boundary_corruption(format!("decode committed boundary {path:?}: {error}"))
        })?;
        record.validate()?;
        Ok(Some(record))
    }

    pub(crate) fn load_or_initialize_empty(
        path: &Path,
        current_term: u64,
        wal_max_seq_no: Option<u64>,
    ) -> Result<Self> {
        if let Some(record) = Self::load(path)? {
            return Ok(record);
        }
        if let Some(max_seq_no) = wal_max_seq_no {
            return Err(committed_boundary_corruption(format!(
                "missing committed boundary {path:?} for non-empty WAL with maximum sequence {max_seq_no}"
            )));
        }
        let record = Self::empty(current_term);
        record.persist(path)?;
        Ok(record)
    }

    pub(crate) fn persist(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let parent = path.parent().ok_or_else(|| {
            anyhow::anyhow!("committed boundary path {path:?} has no parent directory")
        })?;
        let tmp_path = temporary_path(path);
        let bytes = serde_json::to_vec(self)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct CommittedBoundaryVersionHeader {
    version: u32,
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map_or_else(|| "committed-boundary".into(), |name| name.to_os_string());
    file_name.push(".tmp");
    path.with_file_name(file_name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrimaryTermSequenceState {
    current_term: u64,
    max_seq_no_at_term_start: Option<u64>,
    processed_in_current_term_below_start_max: SeqNoIntervals,
}

impl PrimaryTermSequenceState {
    pub(crate) fn from_record(record: &PrimaryTermSequenceRecord) -> Result<Self> {
        Ok(Self {
            current_term: record.current_term,
            max_seq_no_at_term_start: record.max_seq_no_at_term_start,
            processed_in_current_term_below_start_max: SeqNoIntervals::from_ranges(
                &record.processed_in_current_term_below_start_max,
            )?,
        })
    }

    pub(crate) fn to_record(&self) -> PrimaryTermSequenceRecord {
        PrimaryTermSequenceRecord {
            current_term: self.current_term,
            max_seq_no_at_term_start: self.max_seq_no_at_term_start,
            processed_in_current_term_below_start_max: self
                .processed_in_current_term_below_start_max
                .ranges(),
        }
    }

    pub(crate) fn current_term(&self) -> u64 {
        self.current_term
    }

    pub(crate) fn raise_term(
        &mut self,
        new_term: u64,
        max_seq_no_at_term_start: Option<u64>,
    ) -> Result<bool> {
        if new_term < self.current_term {
            return Err(sequence_state_corruption(format!(
                "cannot lower primary term from {} to {new_term}",
                self.current_term
            )));
        }
        if new_term == self.current_term {
            return Ok(false);
        }
        if new_term == 0 {
            return Err(sequence_state_corruption(
                "primary term transition cannot install term zero",
            ));
        }
        self.current_term = new_term;
        self.max_seq_no_at_term_start = max_seq_no_at_term_start;
        self.processed_in_current_term_below_start_max = SeqNoIntervals::default();
        Ok(true)
    }

    pub(crate) fn check_before_redelivery(
        &self,
        primary_term: u64,
        seq_no: u64,
        already_processed: bool,
    ) -> Result<()> {
        self.validate_term(primary_term)?;
        if already_processed
            && self
                .max_seq_no_at_term_start
                .is_some_and(|maximum| seq_no <= maximum)
            && !self
                .processed_in_current_term_below_start_max
                .contains(seq_no)
        {
            return Err(PrimaryTermSequenceCollisionError {
                primary_term,
                seq_no,
                max_seq_no_at_term_start: self.max_seq_no_at_term_start,
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn mark_processed(&mut self, primary_term: u64, seq_no: u64) -> Result<()> {
        self.validate_term(primary_term)?;
        if self
            .max_seq_no_at_term_start
            .is_some_and(|maximum| seq_no <= maximum)
        {
            self.processed_in_current_term_below_start_max
                .insert(seq_no);
        }
        Ok(())
    }

    fn validate_term(&self, primary_term: u64) -> Result<()> {
        if primary_term != self.current_term {
            return Err(PrimaryTermSequenceStateTermError {
                expected: self.current_term,
                found: primary_term,
            }
            .into());
        }
        Ok(())
    }
}

pub(crate) fn initialize_term_sequence_state(
    identity_fence: u64,
    identity_fence_max_seq_no: Option<u64>,
    committed: &CommittedBoundaryRecord,
) -> Result<PrimaryTermSequenceState> {
    committed.validate()?;
    let committed_term = committed.term_sequence_state.current_term;
    if identity_fence < committed_term {
        return Err(sequence_state_corruption(format!(
            "durable identity fence {identity_fence} is older than committed term {committed_term}"
        )));
    }
    if identity_fence == committed_term {
        if identity_fence_max_seq_no != committed.term_sequence_state.max_seq_no_at_term_start {
            return Err(sequence_state_corruption(format!(
                "durable identity fence maximum {:?} does not match committed term-start maximum {:?}",
                identity_fence_max_seq_no, committed.term_sequence_state.max_seq_no_at_term_start
            )));
        }
        return PrimaryTermSequenceState::from_record(&committed.term_sequence_state);
    }
    if identity_fence == 0 {
        return Err(sequence_state_corruption(
            "durable identity fence cannot be zero",
        ));
    }
    Ok(PrimaryTermSequenceState {
        current_term: identity_fence,
        max_seq_no_at_term_start: identity_fence_max_seq_no,
        processed_in_current_term_below_start_max: SeqNoIntervals::default(),
    })
}

#[derive(Debug, thiserror::Error)]
#[error(
    "operation ({primary_term}, {seq_no}) collides with a sequence processed before term {primary_term} started at maximum {max_seq_no_at_term_start:?}"
)]
pub(crate) struct PrimaryTermSequenceCollisionError {
    pub primary_term: u64,
    pub seq_no: u64,
    pub max_seq_no_at_term_start: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
#[error("operation primary term {found} does not match local sequence state term {expected}")]
pub(crate) struct PrimaryTermSequenceStateTermError {
    expected: u64,
    found: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported committed boundary version {found} (expected {expected})")]
pub(crate) struct UnsupportedCommittedBoundaryVersionError {
    found: u32,
    expected: u32,
}

#[derive(Debug, thiserror::Error)]
#[error("committed sequence boundary is corrupt: {message}")]
pub(crate) struct CommittedBoundaryCorruptionError {
    message: String,
}

#[derive(Debug, thiserror::Error)]
#[error("sequence state is corrupt: {message}")]
pub(crate) struct SequenceStateCorruptionError {
    message: String,
}

fn committed_boundary_corruption(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(CommittedBoundaryCorruptionError {
        message: message.into(),
    })
}

fn sequence_state_corruption(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(SequenceStateCorruptionError {
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn empty_record() -> CommittedBoundaryRecord {
        CommittedBoundaryRecord::empty(1)
    }

    #[test]
    fn checkpoints_distinguish_no_operations_from_sequence_zero() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        assert_eq!(tracker.processed_checkpoint(), None);
        assert_eq!(tracker.persisted_checkpoint(), None);

        tracker.advance_max_seq_no(0);
        tracker.mark_processed(0);
        tracker.mark_persisted(0);
        assert_eq!(
            tracker.stats(),
            SequenceStats {
                processed_checkpoint: Some(0),
                persisted_checkpoint: Some(0),
                max_seq_no: Some(0),
            }
        );
    }

    #[test]
    fn out_of_order_sequences_advance_only_after_gap_closes() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        tracker.advance_max_seq_no(10);
        tracker.mark_processed(10);
        tracker.mark_persisted(10);
        assert_eq!(tracker.processed_checkpoint(), None);
        assert_eq!(tracker.persisted_checkpoint(), None);

        for seq_no in 0..10 {
            tracker.mark_processed(seq_no);
            tracker.mark_persisted(seq_no);
        }
        assert_eq!(tracker.processed_checkpoint(), Some(10));
        assert_eq!(tracker.persisted_checkpoint(), Some(10));
    }

    #[test]
    fn fsync_before_processing_does_not_advance_persisted_checkpoint_early() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        tracker.advance_max_seq_no(1);
        tracker.mark_persisted(0);
        tracker.mark_persisted(1);
        assert_eq!(tracker.persisted_checkpoint(), None);

        tracker.mark_processed(0);
        assert_eq!(tracker.persisted_checkpoint(), Some(0));
        tracker.mark_processed(1);
        assert_eq!(tracker.persisted_checkpoint(), Some(1));
    }

    #[test]
    fn duplicate_marks_are_idempotent_and_max_tracks_gaps() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        tracker.advance_max_seq_no(7);
        tracker.mark_processed(7);
        tracker.mark_processed(7);
        tracker.mark_persisted(7);
        tracker.mark_persisted(7);
        assert!(tracker.has_processed(7));
        assert_eq!(tracker.max_seq_no(), Some(7));
        assert_eq!(tracker.processed_checkpoint(), None);
        assert_eq!(tracker.missing_intervals_through(7), vec![0..=6]);
    }

    #[test]
    fn persisted_through_waits_for_processed_prefix() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        tracker.advance_max_seq_no(3);
        tracker.mark_persisted_through(Some(3));
        tracker.mark_processed(0);
        tracker.mark_processed(2);
        assert_eq!(tracker.persisted_checkpoint(), Some(0));
        tracker.mark_processed(1);
        assert_eq!(tracker.persisted_checkpoint(), Some(2));
        tracker.mark_processed(3);
        assert_eq!(tracker.persisted_checkpoint(), Some(3));
    }

    #[test]
    fn reset_to_commit_discards_uncommitted_interval_state() {
        let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
        tracker.advance_max_seq_no(5);
        tracker.mark_processed(5);
        tracker.mark_persisted(5);

        let mut committed = empty_record();
        committed.processed_checkpoint = Some(2);
        committed.persisted_checkpoint = Some(1);
        committed.max_seq_no = Some(4);
        tracker.reset_to_commit(committed).unwrap();
        assert_eq!(
            tracker.stats(),
            SequenceStats {
                processed_checkpoint: Some(2),
                persisted_checkpoint: Some(1),
                max_seq_no: Some(4),
            }
        );
        assert!(!tracker.has_processed(5));
    }

    #[test]
    fn interval_insert_merges_overlap_and_adjacency() {
        let mut intervals = SeqNoIntervals::default();
        intervals.insert_range(5, 7);
        intervals.insert_range(1, 2);
        intervals.insert_range(3, 4);
        intervals.insert(8);
        assert_eq!(intervals.ranges(), vec![SeqNoRange { start: 1, end: 8 }]);
    }

    #[test]
    fn missing_ranges_are_bounded_and_precise() {
        let mut intervals = SeqNoIntervals::default();
        intervals.insert_range(2, 4);
        intervals.insert_range(7, 8);
        assert_eq!(intervals.missing_ranges(0, 10), vec![0..=1, 5..=6, 9..=10]);
        assert!(intervals.missing_ranges(11, 10).is_empty());
    }

    proptest! {
        #[test]
        fn interval_set_matches_btree_set_oracle(values in prop::collection::vec(0u8..64, 0..256)) {
            let mut intervals = SeqNoIntervals::default();
            let mut oracle = BTreeSet::new();
            for value in values {
                let seq_no = u64::from(value);
                intervals.insert(seq_no);
                oracle.insert(seq_no);
                for candidate in 0..64 {
                    prop_assert_eq!(intervals.contains(candidate), oracle.contains(&candidate));
                }
            }
            let expected_missing = (0..64)
                .filter(|candidate| !oracle.contains(candidate))
                .map(|candidate| candidate..=candidate)
                .collect::<Vec<_>>();
            let flattened_missing = intervals
                .missing_ranges(0, 63)
                .into_iter()
                .flat_map(|range| range.collect::<Vec<_>>())
                .map(|candidate| candidate..=candidate)
                .collect::<Vec<_>>();
            prop_assert_eq!(flattened_missing, expected_missing);
        }

        #[test]
        fn checkpoint_tracker_matches_set_oracle(
            actions in prop::collection::vec((0u8..64, any::<bool>()), 0..512)
        ) {
            let mut tracker = LocalCheckpointTracker::new(empty_record()).unwrap();
            let mut processed = BTreeSet::new();
            let mut durable = BTreeSet::new();
            let mut maximum = None;

            for (value, is_persisted_mark) in actions {
                let seq_no = u64::from(value);
                maximum = Some(maximum.map_or(seq_no, |current: u64| current.max(seq_no)));
                tracker.advance_max_seq_no(seq_no);
                if is_persisted_mark {
                    durable.insert(seq_no);
                    tracker.mark_persisted(seq_no);
                } else {
                    processed.insert(seq_no);
                    tracker.mark_processed(seq_no);
                }

                let processed_checkpoint = oracle_checkpoint(&processed);
                let persisted_checkpoint =
                    oracle_checkpoint(&processed.intersection(&durable).copied().collect());
                prop_assert_eq!(tracker.processed_checkpoint(), processed_checkpoint);
                prop_assert_eq!(tracker.persisted_checkpoint(), persisted_checkpoint);
                prop_assert_eq!(tracker.max_seq_no(), maximum);
            }
        }
    }

    fn oracle_checkpoint(values: &BTreeSet<u64>) -> Option<u64> {
        let mut checkpoint = None;
        for value in values {
            if Some(*value) != next_seq_no(checkpoint) {
                break;
            }
            checkpoint = Some(*value);
        }
        checkpoint
    }

    #[test]
    fn committed_boundary_json_roundtrip_preserves_term_intervals() {
        let record = CommittedBoundaryRecord {
            version: COMMITTED_BOUNDARY_FORMAT_VERSION,
            processed_checkpoint: Some(8),
            persisted_checkpoint: Some(7),
            max_seq_no: Some(10),
            max_seq_no_of_updates_or_deletes: Some(9),
            legacy_migration_checkpoint: None,
            term_sequence_state: PrimaryTermSequenceRecord {
                current_term: 3,
                max_seq_no_at_term_start: Some(6),
                processed_in_current_term_below_start_max: vec![
                    SeqNoRange { start: 1, end: 2 },
                    SeqNoRange { start: 5, end: 6 },
                ],
            },
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("translog.committed");
        record.persist(&path).unwrap();
        assert_eq!(CommittedBoundaryRecord::load(&path).unwrap(), Some(record));
        assert!(!path.with_file_name("translog.committed.tmp").exists());
    }

    #[test]
    fn committed_boundary_rejects_contradictory_state() {
        let mut record = empty_record();
        record.persisted_checkpoint = Some(1);
        let error = record.validate().unwrap_err();
        assert!(error.is::<CommittedBoundaryCorruptionError>());

        let mut record = empty_record();
        record.processed_checkpoint = Some(2);
        record.max_seq_no = Some(1);
        let error = record.validate().unwrap_err();
        assert!(error.is::<CommittedBoundaryCorruptionError>());

        let mut record = empty_record();
        record.version += 1;
        let error = record.validate().unwrap_err();
        assert!(error.is::<UnsupportedCommittedBoundaryVersionError>());
    }

    #[test]
    fn term_collision_check_runs_before_sequence_redelivery() {
        let committed = CommittedBoundaryRecord {
            term_sequence_state: PrimaryTermSequenceRecord {
                current_term: 1,
                max_seq_no_at_term_start: Some(11),
                processed_in_current_term_below_start_max: vec![SeqNoRange { start: 11, end: 11 }],
            },
            processed_checkpoint: Some(11),
            persisted_checkpoint: Some(11),
            max_seq_no: Some(11),
            ..empty_record()
        };
        let mut state = initialize_term_sequence_state(2, Some(11), &committed).unwrap();
        let error = state.check_before_redelivery(2, 11, true).unwrap_err();
        assert!(error.is::<PrimaryTermSequenceCollisionError>());

        state.check_before_redelivery(2, 10, false).unwrap();
        state.mark_processed(2, 10).unwrap();
        state.check_before_redelivery(2, 10, true).unwrap();
    }

    #[test]
    fn newer_identity_fence_wins_over_older_committed_term() {
        let mut committed = empty_record();
        committed.term_sequence_state.current_term = 2;
        committed.max_seq_no = Some(9);
        let state = initialize_term_sequence_state(4, Some(12), &committed).unwrap();
        assert_eq!(state.current_term(), 4);
        assert_eq!(state.to_record().max_seq_no_at_term_start, Some(12));
        state.check_before_redelivery(4, 9, false).unwrap();
    }

    #[test]
    fn equal_identity_and_committed_term_restore_processed_set() {
        let committed = CommittedBoundaryRecord {
            processed_checkpoint: Some(4),
            persisted_checkpoint: Some(4),
            max_seq_no: Some(4),
            term_sequence_state: PrimaryTermSequenceRecord {
                current_term: 3,
                max_seq_no_at_term_start: Some(4),
                processed_in_current_term_below_start_max: vec![SeqNoRange { start: 2, end: 4 }],
            },
            ..empty_record()
        };
        let state = initialize_term_sequence_state(3, Some(4), &committed).unwrap();
        state.check_before_redelivery(3, 3, true).unwrap();
        assert_eq!(state.to_record(), committed.term_sequence_state);
    }

    #[test]
    fn older_identity_fence_is_corruption() {
        let mut committed = empty_record();
        committed.term_sequence_state.current_term = 3;
        let error = initialize_term_sequence_state(2, None, &committed).unwrap_err();
        assert!(error.is::<SequenceStateCorruptionError>());
    }

    #[test]
    fn consecutive_term_raises_reset_collision_membership() {
        let mut state = PrimaryTermSequenceState::from_record(&PrimaryTermSequenceRecord {
            current_term: 1,
            max_seq_no_at_term_start: Some(3),
            processed_in_current_term_below_start_max: vec![SeqNoRange { start: 1, end: 1 }],
        })
        .unwrap();
        assert!(state.raise_term(2, Some(5)).unwrap());
        state.mark_processed(2, 4).unwrap();
        state.check_before_redelivery(2, 4, true).unwrap();
        assert!(state.raise_term(4, Some(8)).unwrap());
        assert_eq!(state.current_term(), 4);
        assert!(state.check_before_redelivery(4, 4, true).is_err());
    }
}
