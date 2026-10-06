use super::proto::{WriteFailureDetails, WriteFailureOutcome};
use crate::engine::write_failure::WriteMutationError;
use prost::Message;

const FAILURE_HEADER: &str = "x-ferris-write-failure";

#[derive(Debug, Clone, thiserror::Error)]
#[error("{reason}")]
pub(crate) struct WriteFailure {
    pub outcome: WriteFailureOutcome,
    pub status: u16,
    pub error_type: String,
    pub reason: String,
    pub seq_no: Option<u64>,
    pub primary_term: Option<u64>,
    pub last_seq_no: Option<u64>,
}

impl WriteFailure {
    pub fn before_wal(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<WriteMutationError>()
            .is_none_or(|error| !error.mutation.wal_attempted)
    }

    pub fn rejected(status: u16, error_type: &str, reason: impl Into<String>) -> Self {
        Self {
            outcome: WriteFailureOutcome::Rejected,
            status,
            error_type: error_type.into(),
            reason: reason.into(),
            seq_no: None,
            primary_term: None,
            last_seq_no: None,
        }
    }

    pub fn not_executed(reason: impl Into<String>) -> Self {
        Self {
            outcome: WriteFailureOutcome::NotExecuted,
            status: 503,
            error_type: "shard_not_available_exception".into(),
            reason: reason.into(),
            seq_no: None,
            primary_term: None,
            last_seq_no: None,
        }
    }

    pub fn indeterminate(
        reason: impl Into<String>,
        seq_no: Option<u64>,
        primary_term: Option<u64>,
        last_seq_no: Option<u64>,
    ) -> Self {
        Self {
            outcome: WriteFailureOutcome::Indeterminate,
            status: 500,
            error_type: "write_outcome_unknown".into(),
            reason: reason.into(),
            seq_no,
            primary_term,
            last_seq_no,
        }
    }

    pub fn from_engine(error: &anyhow::Error, primary_term: u64) -> Self {
        let mutation = error
            .downcast_ref::<WriteMutationError>()
            .map(|error| error.mutation);
        if mutation.is_some_and(|mutation| mutation.wal_attempted) {
            let mutation = mutation.expect("checked mutation context");
            return Self::indeterminate(
                format!("{error:#}"),
                mutation.seq_no,
                Some(mutation.primary_term),
                mutation.last_seq_no,
            );
        }
        if error.is::<crate::engine::VersionConflictError>() {
            return Self::rejected(409, "version_conflict_engine_exception", error.to_string());
        }
        if crate::engine::is_write_validation_error(error) {
            return Self::rejected(400, "mapper_parsing_exception", error.to_string());
        }
        if error.is::<crate::engine::version_map::VersionMapCapacityError>() {
            return Self::rejected(429, "version_map_capacity_exceeded", error.to_string());
        }
        match mutation {
            Some(_) => Self::not_executed(format!("{error:#}")),
            None => Self::indeterminate(format!("{error:#}"), None, Some(primary_term), None),
        }
    }

    pub fn before_mutation_status(status: tonic::Status) -> tonic::Status {
        let failure = match status.code() {
            tonic::Code::InvalidArgument => {
                Self::rejected(400, "mapper_parsing_exception", status.message())
            }
            tonic::Code::AlreadyExists => {
                Self::rejected(409, "version_conflict_engine_exception", status.message())
            }
            tonic::Code::NotFound => {
                Self::rejected(404, "index_not_found_exception", status.message())
            }
            tonic::Code::Unimplemented => {
                Self::rejected(501, "not_implemented_exception", status.message())
            }
            tonic::Code::ResourceExhausted
                if status.message().starts_with(
                    crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX,
                ) =>
            {
                Self::rejected(429, "version_map_capacity_exceeded", status.message())
            }
            _ => Self::not_executed(status.to_string()),
        };
        let code = if status.code() == tonic::Code::Aborted {
            tonic::Code::Aborted
        } else {
            failure.rpc_code()
        };
        failure.into_status_with_code(code)
    }

    pub fn to_wire(&self) -> WriteFailureDetails {
        WriteFailureDetails {
            outcome: self.outcome as i32,
            status: u32::from(self.status),
            error_type: self.error_type.clone(),
            reason: self.reason.clone(),
            seq_no: self.seq_no,
            primary_term: self.primary_term,
            last_seq_no: self.last_seq_no,
        }
    }

    pub fn index_response(self, doc_id: String) -> super::proto::ShardDocResponse {
        super::proto::ShardDocResponse {
            success: false,
            doc_id,
            error: self.reason.clone(),
            seq_no: self.seq_no,
            primary_term: self.primary_term,
            failure: Some(self.to_wire()),
            ..Default::default()
        }
    }

    pub fn delete_response(self, deleted: u64) -> super::proto::ShardDeleteResponse {
        super::proto::ShardDeleteResponse {
            success: false,
            deleted,
            error: self.reason.clone(),
            seq_no: self.seq_no,
            primary_term: self.primary_term,
            failure: Some(self.to_wire()),
            ..Default::default()
        }
    }

    pub fn bulk_response(self, doc_ids: Vec<String>) -> super::proto::ShardBulkResponse {
        super::proto::ShardBulkResponse {
            success: false,
            doc_ids,
            error: self.reason.clone(),
            start_seq_no: self.seq_no,
            primary_term: self.primary_term,
            failure: Some(self.to_wire()),
            ..Default::default()
        }
    }

    pub fn from_wire(wire: WriteFailureDetails) -> anyhow::Result<Self> {
        let outcome = WriteFailureOutcome::try_from(wire.outcome)?;
        let status = u16::try_from(wire.status)?;
        let valid = match outcome {
            WriteFailureOutcome::Rejected => (400..500).contains(&status) || status == 501,
            WriteFailureOutcome::NotExecuted => {
                status == 503 && wire.error_type == "shard_not_available_exception"
            }
            WriteFailureOutcome::Indeterminate => {
                status == 500 && wire.error_type == "write_outcome_unknown"
            }
            _ => false,
        };
        if !valid || wire.reason.is_empty() || wire.error_type.is_empty() {
            anyhow::bail!("write failure has an invalid outcome, status, type, or cause");
        }
        if wire.primary_term == Some(0)
            || (wire.seq_no.is_some() && wire.primary_term.is_none())
            || (outcome != WriteFailureOutcome::Indeterminate
                && (wire.seq_no.is_some()
                    || wire.primary_term.is_some()
                    || wire.last_seq_no.is_some()))
            || wire
                .last_seq_no
                .is_some_and(|last| wire.seq_no.is_none_or(|start| last < start))
        {
            anyhow::bail!("write failure has inconsistent operation identity");
        }
        Ok(Self {
            outcome,
            status,
            error_type: wire.error_type,
            reason: wire.reason,
            seq_no: wire.seq_no,
            primary_term: wire.primary_term,
            last_seq_no: wire.last_seq_no,
        })
    }

    fn rpc_code(&self) -> tonic::Code {
        match self.status {
            400 => tonic::Code::InvalidArgument,
            401 => tonic::Code::Unauthenticated,
            403 => tonic::Code::PermissionDenied,
            404 => tonic::Code::NotFound,
            409 => tonic::Code::AlreadyExists,
            429 => tonic::Code::ResourceExhausted,
            501 => tonic::Code::Unimplemented,
            503 => tonic::Code::Unavailable,
            _ => tonic::Code::Internal,
        }
    }

    pub fn into_status(self) -> tonic::Status {
        let code = self.rpc_code();
        self.into_status_with_code(code)
    }

    fn into_status_with_code(self, code: tonic::Code) -> tonic::Status {
        let mut status =
            tonic::Status::with_details(code, &self.reason, self.to_wire().encode_to_vec().into());
        status.metadata_mut().insert(
            FAILURE_HEADER,
            tonic::metadata::MetadataValue::from_static("1"),
        );
        status
    }

    pub fn from_rpc_status(status: tonic::Status) -> Self {
        if let Some(version) = status.metadata().get(FAILURE_HEADER) {
            let decoded = (|| {
                if version != "1" {
                    anyhow::bail!("unsupported write failure details version");
                }
                let wire = WriteFailureDetails::decode(status.details())?;
                let failure = Self::from_wire(wire)?;
                let code_matches = failure.rpc_code() == status.code()
                    || (failure.outcome == WriteFailureOutcome::NotExecuted
                        && status.code() == tonic::Code::Aborted);
                if !code_matches || failure.reason != status.message() {
                    anyhow::bail!(
                        "write failure details contradict their RPC status or cause; details cause: {}",
                        failure.reason
                    );
                }
                Ok(failure)
            })();
            return match decoded {
                Ok(failure) => failure,
                Err(error) => Self::indeterminate(
                    format!("malformed write failure response: {error:#}; {status}"),
                    None,
                    None,
                    None,
                ),
            };
        }
        Self::indeterminate(status.to_string(), None, None, None)
    }

    pub fn after_dispatch(error: anyhow::Error) -> anyhow::Error {
        if error.is::<Self>() {
            error
        } else {
            Self::indeterminate(format!("{error:#}"), None, None, None).into()
        }
    }

    pub fn before_dispatch(error: impl Into<anyhow::Error>) -> anyhow::Error {
        let error = error.into();
        Self::not_executed(format!("{error:#}")).into()
    }

    pub fn bulk_item(self, doc_id: String) -> super::proto::ShardBulkItemResponse {
        super::proto::ShardBulkItemResponse {
            doc_id,
            status: u32::from(self.status),
            error: self.reason.clone(),
            error_type: self.error_type.clone(),
            seq_no: self.seq_no,
            primary_term: self.primary_term,
            failure: Some(self.to_wire()),
            ..Default::default()
        }
    }

    pub fn response_failure(
        failure: Option<WriteFailureDetails>,
        reason: &str,
        seq_no: Option<u64>,
        primary_term: Option<u64>,
    ) -> anyhow::Result<Self> {
        let failure = Self::from_wire(failure.ok_or_else(|| {
            anyhow::anyhow!(
                "failed write response is missing typed outcome details; server cause: {reason}"
            )
        })?)
        .map_err(|error| {
            error.context(format!(
                "invalid write failure details; server cause: {reason}"
            ))
        })?;
        if failure.reason != reason
            || failure.seq_no != seq_no
            || failure.primary_term != primary_term
        {
            anyhow::bail!(
                "failed write response has inconsistent cause or operation identity; server cause: {reason}; details cause: {}",
                failure.reason
            );
        }
        Ok(failure)
    }

    pub fn item_receipt(&self, offset: usize, count: usize) -> anyhow::Result<Option<u64>> {
        let Some(start) = self.seq_no else {
            return Ok(None);
        };
        let end = start
            .checked_add(count.checked_sub(1).ok_or_else(|| {
                anyhow::anyhow!("write failure range cannot describe an empty batch")
            })? as u64)
            .ok_or_else(|| anyhow::anyhow!("write failure range overflows"))?;
        if self.last_seq_no != Some(end) || offset >= count {
            anyhow::bail!(
                "write failure range does not match the submitted batch: start={start}, last={:?}, count={count}, offset={offset}",
                self.last_seq_no
            );
        }
        Ok(start.checked_add(offset as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_rpc_failures_roundtrip_classification_and_zero_sequence() {
        for failure in [
            WriteFailure::rejected(400, "mapper_parsing_exception", "bad source"),
            WriteFailure::rejected(404, "index_not_found_exception", "UUID changed"),
            WriteFailure::rejected(409, "version_conflict_engine_exception", "CAS conflict"),
            WriteFailure::rejected(429, "version_map_capacity_exceeded", "capacity"),
            WriteFailure::rejected(501, "not_implemented_exception", "unsupported"),
            WriteFailure::not_executed("primary activation is unavailable"),
            WriteFailure::indeterminate("replica failed", Some(0), Some(7), None),
            WriteFailure::indeterminate("batch replica failed", Some(0), Some(7), Some(2)),
        ] {
            let decoded = WriteFailure::from_rpc_status(failure.clone().into_status());
            assert_eq!(decoded.outcome, failure.outcome);
            assert_eq!(decoded.status, failure.status);
            assert_eq!(decoded.error_type, failure.error_type);
            assert_eq!(decoded.reason, failure.reason);
            assert_eq!(decoded.seq_no, failure.seq_no);
            assert_eq!(decoded.primary_term, failure.primary_term);
            assert_eq!(decoded.last_seq_no, failure.last_seq_no);
        }
    }

    #[test]
    fn unmarked_rpc_codes_cannot_prove_non_execution_or_a_retryable_conflict() {
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::AlreadyExists,
            tonic::Code::Aborted,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Internal,
        ] {
            let failure =
                WriteFailure::from_rpc_status(tonic::Status::new(code, "ambiguous RPC failure"));
            assert_eq!(
                failure.outcome,
                WriteFailureOutcome::Indeterminate,
                "{code:?}"
            );
            assert_eq!(failure.status, 500);
            assert_eq!(failure.error_type, "write_outcome_unknown");
            assert_eq!(failure.seq_no, None);
            assert_eq!(failure.primary_term, None);
            assert!(failure.reason.contains("ambiguous RPC failure"));
        }
    }

    #[test]
    fn pre_mutation_aborted_keeps_its_rpc_code_with_explicit_non_execution_proof() {
        let status = WriteFailure::before_mutation_status(tonic::Status::aborted(
            "shard UUID changed during dynamic mapping reopen",
        ));
        assert_eq!(status.code(), tonic::Code::Aborted);
        let failure = WriteFailure::from_rpc_status(status);
        assert_eq!(failure.outcome, WriteFailureOutcome::NotExecuted);
        assert_eq!(failure.status, 503);
        assert_eq!(failure.seq_no, None);
        assert!(failure.reason.contains("shard UUID changed"));
    }

    #[test]
    fn malformed_failure_wire_data_is_rejected_without_fabricated_identity() {
        let valid =
            WriteFailure::indeterminate("apply failed", Some(0), Some(7), Some(2)).to_wire();
        let mut invalid = Vec::new();
        for outcome in [0, 99] {
            invalid.push(WriteFailureDetails {
                outcome,
                ..valid.clone()
            });
        }
        invalid.extend([
            WriteFailureDetails {
                status: 503,
                ..valid.clone()
            },
            WriteFailureDetails {
                error_type: "shard_failure".into(),
                ..valid.clone()
            },
            WriteFailureDetails {
                reason: String::new(),
                ..valid.clone()
            },
            WriteFailureDetails {
                primary_term: Some(0),
                ..valid.clone()
            },
            WriteFailureDetails {
                primary_term: None,
                ..valid.clone()
            },
            WriteFailureDetails {
                seq_no: None,
                ..valid.clone()
            },
            WriteFailureDetails {
                seq_no: Some(3),
                ..valid.clone()
            },
            WriteFailureDetails {
                seq_no: Some(0),
                primary_term: Some(7),
                ..WriteFailure::not_executed("unavailable").to_wire()
            },
        ]);
        for wire in invalid {
            assert!(WriteFailure::from_wire(wire).is_err());
        }
        assert!(
            WriteFailure::response_failure(
                Some(valid.clone()),
                "different cause",
                Some(0),
                Some(7)
            )
            .is_err()
        );
        assert!(
            WriteFailure::response_failure(Some(valid), "apply failed", Some(1), Some(7)).is_err()
        );
    }

    #[test]
    fn corrupt_or_contradictory_rpc_details_are_diagnostic_indeterminate_failures() {
        let failure = WriteFailure::not_executed("known pre-WAL failure");
        let mut statuses = vec![
            tonic::Status::with_details(
                tonic::Code::Unavailable,
                "broken protobuf",
                vec![255].into(),
            ),
            tonic::Status::with_details(
                tonic::Code::Internal,
                &failure.reason,
                failure.to_wire().encode_to_vec().into(),
            ),
            tonic::Status::with_details(
                tonic::Code::Unavailable,
                "wrong cause",
                failure.to_wire().encode_to_vec().into(),
            ),
        ];
        for status in &mut statuses {
            status.metadata_mut().insert(
                FAILURE_HEADER,
                tonic::metadata::MetadataValue::from_static("1"),
            );
        }
        let mut unsupported = failure.into_status();
        unsupported.metadata_mut().insert(
            FAILURE_HEADER,
            tonic::metadata::MetadataValue::from_static("2"),
        );
        statuses.push(unsupported);
        for status in statuses {
            let failure = WriteFailure::from_rpc_status(status);
            assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
            assert_eq!(failure.seq_no, None);
            assert_eq!(failure.primary_term, None);
            assert!(failure.reason.contains("malformed write failure response"));
        }
    }

    #[test]
    fn failed_bulk_range_requires_actual_submitted_cardinality_and_ordered_offset() {
        let failure = WriteFailure::indeterminate("batch failed", Some(0), Some(7), Some(2));
        assert_eq!(failure.item_receipt(0, 3).unwrap(), Some(0));
        assert_eq!(failure.item_receipt(2, 3).unwrap(), Some(2));
        for (offset, count) in [(0, 0), (0, 2), (0, 4), (3, 3)] {
            assert!(failure.item_receipt(offset, count).is_err());
        }
        let failure =
            WriteFailure::indeterminate("batch failed", Some(u64::MAX), Some(7), Some(u64::MAX));
        assert_eq!(failure.item_receipt(0, 1).unwrap(), Some(u64::MAX));
        assert!(failure.item_receipt(0, 2).is_err());
    }
}
