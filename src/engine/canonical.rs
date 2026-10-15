use super::{PeerRecoveryRetentionPin, SequencedOperation};
use crate::wal::WalCursor;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_HISTORY_WINDOW: u64 = 1024;
pub const MAX_HISTORY_BATCH_BYTES: usize = crate::wal::MAX_WAL_FRAME_BYTES + 64 * 1024;

pub struct FrozenEngineHistory {
    pub wal_end: WalCursor,
    pub processed_next: u64,
    pub persisted_next: u64,
    pub max_next: u64,
    pub retention_pin: PeerRecoveryRetentionPin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryOperationMetadata {
    pub seq_no: u64,
    pub primary_term: u64,
    pub digest: [u8; 32],
    pub cursor: WalCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryInventory {
    pub start_seq_no: u64,
    pub end_seq_no: u64,
    pub operations: Vec<HistoryOperationMetadata>,
}

impl HistoryInventory {
    pub fn validate(&self) -> Result<()> {
        validate_window(self.start_seq_no, self.end_seq_no)?;
        let mut previous = None;
        for operation in &self.operations {
            if operation.seq_no < self.start_seq_no
                || operation.seq_no >= self.end_seq_no
                || previous.is_some_and(|previous| previous >= operation.seq_no)
                || operation.primary_term == 0
                || operation.digest == [0; 32]
            {
                bail!("invalid, duplicate, or unordered canonical history inventory");
            }
            previous = Some(operation.seq_no);
        }
        Ok(())
    }
}

pub(crate) fn validate_window(start: u64, end: u64) -> Result<()> {
    if end <= start || end - start > MAX_HISTORY_WINDOW {
        bail!(
            "canonical inventory requires a nonempty window of at most {MAX_HISTORY_WINDOW} sequences"
        );
    }
    Ok(())
}

pub(crate) fn operation_digest(operation: &SequencedOperation) -> Result<[u8; 32]> {
    Ok(Sha256::digest(serde_json::to_vec(operation)?).into())
}

pub(crate) fn validate_batch(operations: &[SequencedOperation]) -> Result<()> {
    if operations.is_empty() || operations.len() as u64 > MAX_HISTORY_WINDOW {
        bail!("canonical append requires a bounded nonempty batch");
    }
    let mut previous = None;
    let mut bytes = 0usize;
    for operation in operations {
        if operation.primary_term == 0
            || previous.is_some_and(|previous| previous >= operation.seq_no)
        {
            bail!("canonical append operations must be strictly ordered and have positive terms");
        }
        previous = Some(operation.seq_no);
        bytes = bytes
            .checked_add(serde_json::to_vec(operation)?.len())
            .ok_or_else(|| anyhow::anyhow!("canonical append byte count overflow"))?;
        if bytes > MAX_HISTORY_BATCH_BYTES {
            bail!("canonical append exceeds its encoded byte budget");
        }
    }
    Ok(())
}

pub(crate) fn exclusive_next(checkpoint: Option<u64>) -> Result<u64> {
    match checkpoint {
        Some(checkpoint) => checkpoint
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canonical history sequence watermark overflow")),
        None => Ok(0),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("canonical history conflicts with retained sequence {seq_no}")]
pub(crate) struct CanonicalHistoryConflict {
    pub seq_no: u64,
}

pub(crate) struct CanonicalHistoryAccumulator {
    authority: crate::cluster::state::WriteAuthorityIdentity,
    digest: Sha256,
    next_seq_no: u64,
}

impl CanonicalHistoryAccumulator {
    pub(crate) fn new(
        authority: &crate::cluster::state::WriteAuthorityIdentity,
        previous: Option<&crate::cluster::state::CanonicalHistory>,
    ) -> Result<Self> {
        authority.validate().map_err(anyhow::Error::msg)?;
        if let Some(previous) = previous {
            previous.validate().map_err(anyhow::Error::msg)?;
            if previous.primary_term >= authority.primary_term
                || previous.activation_id >= authority.activation_id
            {
                bail!("canonical activation must supersede its committed history anchor");
            }
        }
        let mut digest = Sha256::new();
        digest.update(b"ferrissearch-canonical-history-v1\0");
        digest.update(serde_json::to_vec(authority)?);
        digest.update(serde_json::to_vec(&previous)?);
        Ok(Self {
            authority: authority.clone(),
            digest,
            next_seq_no: exclusive_next(previous.and_then(|history| history.max_seq_no))?,
        })
    }

    pub(crate) fn next_seq_no(&self) -> u64 {
        self.next_seq_no
    }

    pub(crate) fn record(&mut self, operation: &HistoryOperationMetadata) -> Result<()> {
        if operation.seq_no != self.next_seq_no
            || operation.primary_term == 0
            || operation.primary_term > self.authority.primary_term
            || operation.digest == [0; 32]
        {
            bail!(
                "canonical certificate requires exact ordered, bounded-term operation identities"
            );
        }
        self.digest.update(operation.seq_no.to_le_bytes());
        self.digest.update(operation.primary_term.to_le_bytes());
        self.digest.update(operation.digest);
        self.next_seq_no = self
            .next_seq_no
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canonical certificate sequence overflow"))?;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        max_seq_no: Option<u64>,
    ) -> Result<crate::cluster::state::CanonicalHistory> {
        if self.next_seq_no != exclusive_next(max_seq_no)? {
            bail!("canonical certificate does not cover its complete declared history");
        }
        Ok(crate::cluster::state::CanonicalHistory {
            primary_term: self.authority.primary_term,
            activation_id: self.authority.activation_id,
            max_seq_no,
            digest: self.digest.finalize().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::write_authority::{CopyWriteBinding, CopyWriteState};
    use crate::engine::{
        ApplyOutcome, ClientOperationIntent, ClientOperationRecord, ClientOperationResult,
        CompositeEngine, DocumentMutation, SearchEngine, WriteCondition,
    };
    use crate::wal::TranslogDurability;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn index(seq_no: u64, value: u64) -> SequencedOperation {
        SequencedOperation {
            seq_no,
            primary_term: 2,
            document_version: seq_no + 1,
            client_operation: None,
            mutation: DocumentMutation::Index {
                doc_id: "doc".into(),
                source: json!({"value": value}),
            },
        }
    }

    #[tokio::test]
    async fn canonical_history_freeze_keeps_gaps_and_inventory_is_logical_not_physical() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.apply_replica_operation(index(2, 3)).unwrap();
        engine.apply_replica_operation(index(0, 1)).unwrap();
        engine.reconcile_term_sequence_state(3, Some(2)).unwrap();
        let frozen = engine.freeze_canonical_history().unwrap();
        assert_eq!(frozen.max_next, 3);
        assert_eq!(frozen.processed_next, 1);
        assert_eq!(frozen.persisted_next, 1);
        let low = engine.canonical_inventory(frozen.wal_end, 0, 1).unwrap();
        assert_eq!(low.operations.len(), 1);
        assert_eq!(low.operations[0].seq_no, 0);
        let all = engine.canonical_inventory(frozen.wal_end, 0, 3).unwrap();
        assert_eq!(
            all.operations
                .iter()
                .map(|op| op.seq_no)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let read = engine
            .canonical_operation_at(all.operations[1].cursor, frozen.wal_end)
            .unwrap();
        assert_eq!(read, index(2, 3));
        assert!(
            engine
                .canonical_inventory(frozen.wal_end, 0, MAX_HISTORY_WINDOW + 1)
                .is_err()
        );
        assert_eq!(engine.sequence_stats().max_seq_no, Some(2));
        frozen.retention_pin.release().unwrap();
    }

    #[tokio::test]
    async fn canonical_history_adopts_original_peer_gap_without_overwriting_a_newer_document() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.apply_replica_operation(index(0, 1)).unwrap();
        engine.apply_replica_operation(index(2, 3)).unwrap();
        engine.reconcile_term_sequence_state(3, Some(2)).unwrap();
        let frozen = engine.freeze_canonical_history().unwrap();
        let mut adopted = index(1, 2);
        let source = json!({"value": 2});
        let intent = ClientOperationIntent::new(
            "adopted-operation".into(),
            ClientOperationIntent::fingerprint_json(&source).unwrap(),
        )
        .unwrap();
        adopted.client_operation = Some(
            ClientOperationRecord::new(
                intent.clone(),
                ClientOperationResult::Index {
                    version: 2,
                    created: false,
                },
            )
            .unwrap(),
        );
        let receipt = engine
            .append_canonical_operations(&[adopted.clone()])
            .unwrap();
        assert_eq!(receipt.outcomes, vec![ApplyOutcome::Stale]);
        assert!(receipt.all_operations_processed);
        assert!(receipt.all_operations_persisted);
        assert_eq!(receipt.sequence.persisted_checkpoint, Some(2));
        let current = engine.peer_recovery_barrier().unwrap();
        let inventory = engine.canonical_inventory(current.wal_end, 1, 2).unwrap();
        assert_eq!(
            inventory.operations[0].digest,
            operation_digest(&adopted).unwrap()
        );
        let retry = engine
            .add_document_with_operation_at_term(
                "doc",
                source,
                3,
                WriteCondition::Unconditional,
                Some(intent),
            )
            .unwrap();
        assert!(retry.deduplicated);
        assert_eq!((retry.seq_no, retry.primary_term, retry.version), (1, 2, 2));
        engine.refresh().unwrap();
        assert_eq!(
            engine.get_document("doc").unwrap().unwrap()["value"],
            json!(3)
        );
        frozen.retention_pin.release().unwrap();
    }

    #[tokio::test]
    async fn canonical_history_resync_compares_the_whole_requested_batch_before_append() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.apply_replica_operation(index(0, 1)).unwrap();
        engine.apply_replica_operation(index(2, 3)).unwrap();
        engine.reconcile_term_sequence_state(3, Some(2)).unwrap();
        let frozen = engine.freeze_canonical_history().unwrap();
        let error = engine
            .append_canonical_operations(&[index(1, 2), index(2, 99)])
            .unwrap_err();
        assert!(error.is::<CanonicalHistoryConflict>(), "{error:#}");
        assert_eq!(
            engine.peer_recovery_barrier().unwrap().wal_end,
            frozen.wal_end
        );
        assert_eq!(engine.sequence_stats().processed_checkpoint, Some(0));
        assert!(engine.ensure_write_health().is_ok());
        frozen.retention_pin.release().unwrap();
    }

    #[tokio::test]
    async fn canonical_history_bound_flush_retains_all_post_certificate_operations() {
        let dir = tempfile::tempdir().unwrap();
        let copy = CopyWriteState::for_copy(
            CopyWriteBinding {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                allocation_id: 7,
            },
            true,
        )
        .unwrap();
        let engine = CompositeEngine::open_with_copy_state(
            dir.path(),
            Duration::from_secs(60),
            &HashMap::new(),
            TranslogDurability::Request,
            Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
            copy.clone(),
            false,
        )
        .unwrap();
        engine.apply_replica_operation(index(0, 1)).unwrap();
        engine.apply_replica_operation(index(1, 2)).unwrap();
        engine.update_global_checkpoint(1);
        engine.flush().unwrap();
        let end = engine.peer_recovery_barrier().unwrap().wal_end;
        assert_eq!(
            engine
                .canonical_inventory(end, 0, 2)
                .unwrap()
                .operations
                .len(),
            2
        );
        copy.install_canonical_history(&crate::cluster::state::CanonicalHistory {
            primary_term: 2,
            activation_id: 9,
            max_seq_no: Some(0),
            digest: [1; 32],
        })
        .unwrap();
        engine.flush().unwrap();
        let end = engine.peer_recovery_barrier().unwrap().wal_end;
        assert_eq!(
            engine.canonical_inventory(end, 1, 2).unwrap().operations[0].seq_no,
            1
        );
    }
}
