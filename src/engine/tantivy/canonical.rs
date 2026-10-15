use super::*;
use crate::engine::SequencedOperation;
use crate::engine::canonical::{
    CanonicalHistoryConflict, FrozenEngineHistory, HistoryInventory, HistoryOperationMetadata,
    exclusive_next, operation_digest, validate_batch, validate_window,
};
use std::collections::BTreeMap;

impl HotEngine {
    pub(super) fn retained_client_operation_inner(
        &self,
        seq_no: u64,
        primary_term: u64,
        record: &crate::engine::ClientOperationRecord,
    ) -> Result<SequencedOperation> {
        record.validate()?;
        self.with_translog("retained client operation", |translog| {
            let entry = translog.find_entry(seq_no)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "original client operation [{}] at term {primary_term} sequence {seq_no} is no longer retained",
                    record.id,
                )
            })?;
            let operation = sequenced_operation_from_entry(entry)?;
            if operation.primary_term != primary_term
                || operation.client_operation.as_ref() != Some(record)
            {
                anyhow::bail!("retained client operation conflicts at sequence {seq_no}");
            }
            Ok(operation)
        })
    }

    pub(super) fn freeze_canonical_history_inner(&self) -> Result<FrozenEngineHistory> {
        let _maintenance = self.maintenance_guard("canonical history freeze")?;
        self.with_translog("canonical history freeze", |translog| {
            self.writer
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .fail("canonical activation requires full local WAL replay");
            drop(self.writer_state_with_replay(translog, "canonical history freeze")?);
            translog.sync()?;
            let mut state = self
                .apply_state
                .lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
            state.checkpoints.mark_all_processed_persisted();
            let stats = state.checkpoints.stats();
            let wal_end = translog.recovery_read_snapshot()?.end_cursor();
            let pin_id = translog.register_retention_pin(0)?;
            Ok(FrozenEngineHistory {
                wal_end,
                processed_next: exclusive_next(stats.processed_checkpoint)?,
                persisted_next: exclusive_next(stats.persisted_checkpoint)?,
                max_next: exclusive_next(
                    stats
                        .max_seq_no
                        .into_iter()
                        .chain(translog.max_seq_no())
                        .max(),
                )?,
                retention_pin: crate::engine::PeerRecoveryRetentionPin::new(
                    self.translog.clone(),
                    pin_id,
                ),
            })
        })
    }

    pub(super) fn canonical_inventory_inner(
        &self,
        end_cursor: crate::wal::WalCursor,
        start_seq_no: u64,
        end_seq_no: u64,
    ) -> Result<HistoryInventory> {
        validate_window(start_seq_no, end_seq_no)?;
        let snapshot = self.with_translog("canonical inventory snapshot", |translog| {
            translog.recovery_read_snapshot()
        })?;
        let mut operations = BTreeMap::<u64, HistoryOperationMetadata>::new();
        snapshot.for_each_captured_at(end_cursor, |cursor, entry| {
            if entry.seq_no < start_seq_no || entry.seq_no >= end_seq_no {
                return Ok(());
            }
            let operation = sequenced_operation_from_entry(entry)?;
            let metadata = HistoryOperationMetadata {
                seq_no: operation.seq_no,
                primary_term: operation.primary_term,
                digest: operation_digest(&operation)?,
                cursor,
            };
            if let Some(previous) = operations.get(&operation.seq_no) {
                if previous.primary_term != metadata.primary_term
                    || previous.digest != metadata.digest
                {
                    return Err(CanonicalHistoryConflict {
                        seq_no: operation.seq_no,
                    }
                    .into());
                }
            } else {
                operations.insert(operation.seq_no, metadata);
            }
            Ok(())
        })?;
        let inventory = HistoryInventory {
            start_seq_no,
            end_seq_no,
            operations: operations.into_values().collect(),
        };
        inventory.validate()?;
        Ok(inventory)
    }

    pub(super) fn canonical_operation_at_inner(
        &self,
        cursor: crate::wal::WalCursor,
        end_cursor: crate::wal::WalCursor,
    ) -> Result<crate::engine::SequencedOperation> {
        let snapshot = self.with_translog("canonical operation snapshot", |translog| {
            translog.recovery_read_snapshot()
        })?;
        let mut batch =
            snapshot.read_bounded_cursor(cursor, end_cursor, 1, crate::wal::MAX_WAL_FRAME_BYTES)?;
        if batch.entries.len() != 1 {
            anyhow::bail!("canonical operation cursor does not identify one retained record");
        }
        sequenced_operation_from_entry(batch.entries.remove(0))
    }

    pub(crate) fn validate_canonical_append(
        &self,
        operations: &[crate::engine::SequencedOperation],
    ) -> Result<()> {
        validate_batch(operations)?;
        validate_sequenced_document_versions(operations)?;
        let expected = operations
            .iter()
            .map(|operation| (operation.seq_no, operation))
            .collect::<BTreeMap<_, _>>();
        self.with_translog("canonical append comparison", |translog| {
            let snapshot = translog.recovery_read_snapshot()?;
            let mut found = std::collections::HashSet::new();
            snapshot.for_each_captured_at(snapshot.end_cursor(), |_, entry| {
                let Some(expected) = expected.get(&entry.seq_no) else {
                    return Ok(());
                };
                if entry.primary_term != expected.primary_term
                    || !wal_entry_matches_operation(&entry, expected)?
                {
                    return Err(CanonicalHistoryConflict {
                        seq_no: entry.seq_no,
                    }
                    .into());
                }
                found.insert(entry.seq_no);
                Ok(())
            })?;
            let state = self
                .apply_state
                .lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
            for operation in operations {
                if operation.primary_term > state.term_sequences.current_term() {
                    anyhow::bail!("canonical operation exceeds its durable authority envelope");
                }
                if !found.contains(&operation.seq_no)
                    && state.checkpoints.has_processed(operation.seq_no)
                {
                    anyhow::bail!(
                        "cannot prove a retired canonical operation at sequence {}",
                        operation.seq_no
                    );
                }
            }
            Ok(())
        })
    }

    pub(crate) fn seal_processed_history(&self) -> Result<SequenceStats> {
        self.with_translog("canonical history seal", |translog| {
            translog.sync()?;
            let mut state = self
                .apply_state
                .lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
            state.checkpoints.mark_all_processed_persisted();
            Ok(state.checkpoints.stats())
        })
    }
}
