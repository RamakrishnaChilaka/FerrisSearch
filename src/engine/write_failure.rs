#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteMutationState {
    pub wal_attempted: bool,
    pub seq_no: Option<u64>,
    pub last_seq_no: Option<u64>,
    pub primary_term: u64,
}

impl WriteMutationState {
    pub fn run<T>(
        primary_term: u64,
        operation: impl FnOnce(&mut Self) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let mut mutation = Self::new(primary_term);
        operation(&mut mutation).map_err(|error| mutation.error(error))
    }

    pub fn new(primary_term: u64) -> Self {
        Self {
            wal_attempted: false,
            seq_no: None,
            last_seq_no: None,
            primary_term,
        }
    }

    pub fn error(self, error: anyhow::Error) -> anyhow::Error {
        if error.is::<WriteMutationError>() {
            return error;
        }
        let reason = error.to_string();
        error.context(WriteMutationError {
            mutation: self,
            reason,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub(crate) struct WriteMutationError {
    pub mutation: WriteMutationState,
    reason: String,
}

pub(crate) fn is_expected_pre_wal_rejection(error: &anyhow::Error) -> bool {
    super::is_write_validation_error(error)
        || error.is::<super::VersionConflictError>()
        || error.is::<super::ClientOperationConflictError>()
        || error.is::<super::version_map::VersionMapCapacityError>()
        || error.is::<super::sequence::PrimaryTermSequenceStateTermError>()
        || error.is::<super::write_authority::WriteAuthorityUnavailable>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{CompositeEngine, SearchEngine, WriteCondition};
    use crate::transport::proto::WriteFailureOutcome;
    use crate::transport::write_failure::WriteFailure;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn mutation_context_preserves_io_downcasts_and_inner_operation_identity() {
        let mut mutation = WriteMutationState::new(7);
        mutation.wal_attempted = true;
        mutation.seq_no = Some(0);
        let error = mutation.error(std::io::Error::from_raw_os_error(28).into());
        let error = WriteMutationState::new(99).error(error.context("composite apply"));
        assert_eq!(
            error.downcast_ref::<WriteMutationError>().unwrap().mutation,
            mutation
        );
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(28)
        );
        let failure = WriteFailure::from_engine(&error, 99);
        assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
        assert_eq!(failure.seq_no, Some(0));
        assert_eq!(failure.primary_term, Some(7));
        assert!(failure.reason.contains("composite apply"));
    }

    #[test]
    fn primary_pre_wal_conflict_does_not_allocate_a_sequence() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        let receipt = engine
            .add_document_with_receipt_at_term("doc", json!({"value": 1}), 7)
            .unwrap();
        let before = engine.sequence_stats();
        let error = engine
            .add_document_with_condition_at_term(
                "doc",
                json!({"value": 2}),
                7,
                WriteCondition::Create,
            )
            .unwrap_err();
        assert!(error.is::<crate::engine::VersionConflictError>());
        assert!(
            !error
                .downcast_ref::<WriteMutationError>()
                .unwrap()
                .mutation
                .wal_attempted
        );
        let failure = WriteFailure::from_engine(&error, 7);
        assert_eq!(failure.outcome, WriteFailureOutcome::Rejected);
        assert_eq!(failure.status, 409);
        assert_eq!(failure.seq_no, None);
        assert_eq!(engine.sequence_stats(), before);
        assert_eq!(
            engine
                .get_document_with_metadata("doc", true)
                .unwrap()
                .unwrap()
                .seq_no,
            receipt.seq_no
        );
        assert!(engine.write_failure_reason().is_none());
        assert_eq!(
            engine
                .add_document_with_receipt_at_term("later", json!({"value": 3}), 7)
                .unwrap()
                .seq_no,
            1
        );
    }

    #[test]
    fn failed_wal_invocation_never_borrows_a_previous_successful_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        let receipt = engine
            .add_document_with_receipt_at_term("earlier", json!({"value": 1}), 7)
            .unwrap();
        assert_eq!(receipt.seq_no, 0);
        engine.inject_wal_write_failures_for_test(28, 1);
        let error = engine
            .add_document_with_receipt_at_term("failed", json!({"value": 2}), 7)
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(28)
        );
        let failure = WriteFailure::from_engine(&error, 7);
        assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
        assert_eq!(failure.seq_no, None);
        assert_eq!(failure.primary_term, Some(7));
        assert_eq!(failure.last_seq_no, None);
    }

    #[test]
    fn post_wal_side_effect_error_is_not_reclassified_as_client_rejection() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        let error = engine
            .text_engine()
            .add_primary_index_with_side_effect("doc", json!({"value": 1}), 7, |_| {
                Err(
                    crate::engine::DocumentValidationError("post-WAL side effect failed".into())
                        .into(),
                )
            })
            .unwrap_err();
        assert!(crate::engine::is_write_validation_error(&error));
        let failure = WriteFailure::from_engine(&error, 7);
        assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
        assert_eq!(failure.status, 500);
        assert_eq!(failure.seq_no, Some(0));
        assert_eq!(failure.primary_term, Some(7));
        assert!(failure.reason.contains("post-WAL side effect failed"));
    }

    #[test]
    fn post_wal_bulk_failure_owns_the_whole_request_order_range() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        engine.inject_engine_apply_failures_for_test(28, 1);
        let error = engine
            .bulk_add_documents_with_receipt_at_term(
                vec![
                    ("duplicate".into(), json!({"value": 1})),
                    ("duplicate".into(), json!({"value": 2})),
                    ("other".into(), json!({"value": 3})),
                ],
                7,
            )
            .unwrap_err();
        let failure = WriteFailure::from_engine(&error, 7);
        assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
        assert_eq!(failure.seq_no, Some(0));
        assert_eq!(failure.primary_term, Some(7));
        assert_eq!(failure.last_seq_no, Some(2));
        for offset in 0..3 {
            assert_eq!(
                failure.item_receipt(offset, 3).unwrap(),
                Some(offset as u64)
            );
        }
        assert!(failure.item_receipt(0, 2).is_err());
    }

    #[test]
    fn fs013_fail_stop_wal_failure_rejects_later_index_delete_bulk_and_replay_without_new_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        engine
            .add_document_with_receipt_at_term("earlier", json!({"value": 1}), 7)
            .unwrap();
        let generation = directory.path().join("translog-00000000000000000000.bin");
        let bytes = std::fs::read(&generation).unwrap();
        engine.inject_wal_write_failures_for_test(28, 1);
        let first = engine
            .add_document_with_receipt_at_term("failed", json!({"value": 2}), 7)
            .unwrap_err();
        assert_eq!(
            WriteFailure::from_engine(&first, 7).outcome,
            WriteFailureOutcome::Indeterminate
        );
        assert!(
            engine
                .write_failure_reason()
                .unwrap()
                .contains("No space left")
        );
        for result in [
            engine
                .add_document_with_receipt_at_term("later", json!({"value": 3}), 7)
                .map(|_| ()),
            engine
                .delete_document_with_receipt_at_term("earlier", 7)
                .map(|_| ()),
            engine
                .bulk_add_documents_with_receipt_at_term(
                    vec![("bulk".into(), json!({"value": 4}))],
                    7,
                )
                .map(|_| ()),
        ] {
            let error = result.unwrap_err();
            assert!(error.is::<crate::wal::WalFailStoppedError>());
            let failure = WriteFailure::from_engine(&error, 7);
            assert_eq!(failure.outcome, WriteFailureOutcome::NotExecuted);
            assert_eq!(failure.seq_no, None);
        }
        assert!(engine.refresh().is_err());
        assert!(engine.flush().is_err());
        assert_eq!(std::fs::read(generation).unwrap(), bytes);
        assert_eq!(engine.wal_max_seq_no(), Some(0));
    }

    #[test]
    fn fs013_fail_stop_post_wal_apply_requires_reopen_and_preserves_the_failed_operation() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        engine.inject_engine_apply_failures_for_test(5, 1);
        let error = engine
            .add_document_with_receipt_at_term("retained", json!({"value": 1}), 7)
            .unwrap_err();
        let failure = WriteFailure::from_engine(&error, 7);
        assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
        assert_eq!(failure.seq_no, Some(0));
        assert!(engine.ensure_write_health().is_err());
        assert!(engine.refresh().is_err());
        assert!(engine.prepare_primary_activation(8).is_err());
        assert!(
            engine
                .add_document_with_receipt_at_term("later", json!({}), 8)
                .is_err()
        );
        assert_eq!(engine.wal_max_seq_no(), Some(0));
        drop(engine);

        let reopened = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        let retained = reopened
            .get_document_with_metadata("retained", true)
            .unwrap()
            .unwrap();
        assert_eq!(retained.source, json!({"value": 1}));
        assert_eq!(
            (retained.seq_no, retained.primary_term, retained.version),
            (0, 7, 1)
        );
        reopened.prepare_primary_activation(8).unwrap();
        let later = reopened
            .add_document_with_receipt_at_term("later", json!({"value": 2}), 8)
            .unwrap();
        assert_eq!((later.seq_no, later.primary_term), (1, 8));
    }

    #[test]
    fn fs013_fail_stop_is_visible_to_a_queued_writer_before_the_failing_apply_unlocks() {
        let directory = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(
            CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap(),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let first_engine = engine.clone();
            let first = scope.spawn(move || {
                first_engine
                    .text_engine()
                    .add_primary_index_with_side_effect("failed", json!({"value": 1}), 7, |_| {
                        entered_tx.send(()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                        Err(std::io::Error::from_raw_os_error(5).into())
                    })
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let queued_engine = engine.clone();
            let queued = scope.spawn(move || {
                queued_engine.add_document_with_receipt_at_term("queued", json!({"value": 2}), 7)
            });
            release_tx.send(()).unwrap();
            assert!(first.join().unwrap().is_err());
            let error = queued.join().unwrap().unwrap_err();
            assert!(error.is::<crate::wal::WalFailStoppedError>());
            assert_eq!(
                WriteFailure::from_engine(&error, 7).outcome,
                WriteFailureOutcome::NotExecuted
            );
        });
        assert_eq!(engine.wal_max_seq_no(), Some(0));
    }

    #[test]
    fn fs013_fail_stop_reopen_cannot_invent_an_empty_frontier_after_wal_file_loss() {
        let directory = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(directory.path(), Duration::from_secs(3600)).unwrap();
        engine
            .add_document_with_receipt_at_term("protected", json!({"value": 1}), 7)
            .unwrap();
        drop(engine);
        let manifest = directory.path().join("translog.manifest");
        std::fs::remove_file(&manifest).unwrap();
        std::fs::remove_file(directory.path().join("translog-00000000000000000000.bin")).unwrap();

        let error = CompositeEngine::new(directory.path(), Duration::from_secs(3600))
            .err()
            .unwrap();
        assert!(error.is::<crate::wal::WalCorruptionError>());
        assert!(format!("{error:#}").contains("missing its protected translog manifest"));
        assert!(!manifest.exists());
    }
}
