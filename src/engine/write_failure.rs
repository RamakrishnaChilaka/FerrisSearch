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
}
