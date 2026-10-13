use anyhow::Result;

use super::proto::{
    ClientOperationIntent as WireClientOperationIntent, ClientOperationMetadata,
    ClientOperationResultKind,
};

pub(crate) fn intent_to_proto(
    intent: &crate::engine::ClientOperationIntent,
) -> WireClientOperationIntent {
    WireClientOperationIntent {
        id: intent.id.clone(),
        fingerprint: intent.fingerprint.clone(),
    }
}

pub(crate) fn intent_from_proto(
    intent: Option<WireClientOperationIntent>,
) -> Result<Option<crate::engine::ClientOperationIntent>> {
    intent
        .map(|intent| crate::engine::ClientOperationIntent::new(intent.id, intent.fingerprint))
        .transpose()
}

pub(crate) fn record_to_proto(
    record: &crate::engine::ClientOperationRecord,
) -> ClientOperationMetadata {
    let (result_kind, version, created, deleted, response_seq_no, response_primary_term) =
        match record.result {
            crate::engine::ClientOperationResult::Index { version, created } => (
                ClientOperationResultKind::ClientOperationIndex,
                version,
                created,
                0,
                None,
                None,
            ),
            crate::engine::ClientOperationResult::Delete { version, deleted } => (
                ClientOperationResultKind::ClientOperationDelete,
                version,
                false,
                deleted,
                None,
                None,
            ),
            crate::engine::ClientOperationResult::UpdateNoOp {
                seq_no,
                primary_term,
                version,
            } => (
                ClientOperationResultKind::ClientOperationUpdateNoop,
                version,
                false,
                0,
                Some(seq_no),
                Some(primary_term),
            ),
        };
    ClientOperationMetadata {
        id: record.id.clone(),
        fingerprint: record.fingerprint.clone(),
        result_kind: result_kind as i32,
        version,
        created,
        deleted,
        response_seq_no,
        response_primary_term,
    }
}

pub(crate) fn record_from_proto(
    record: Option<ClientOperationMetadata>,
) -> Result<Option<crate::engine::ClientOperationRecord>> {
    record
        .map(|record| {
            let result = match ClientOperationResultKind::try_from(record.result_kind)
                .map_err(|_| anyhow::anyhow!("unknown client operation result kind"))?
            {
                ClientOperationResultKind::ClientOperationIndex => {
                    crate::engine::ClientOperationResult::Index {
                        version: record.version,
                        created: record.created,
                    }
                }
                ClientOperationResultKind::ClientOperationDelete => {
                    crate::engine::ClientOperationResult::Delete {
                        version: record.version,
                        deleted: record.deleted,
                    }
                }
                ClientOperationResultKind::ClientOperationUpdateNoop => {
                    crate::engine::ClientOperationResult::UpdateNoOp {
                        seq_no: record.response_seq_no.ok_or_else(|| {
                            anyhow::anyhow!(
                                "client operation update no-op is missing response sequence"
                            )
                        })?,
                        primary_term: record.response_primary_term.ok_or_else(|| {
                            anyhow::anyhow!(
                                "client operation update no-op is missing response primary term"
                            )
                        })?,
                        version: record.version,
                    }
                }
            };
            crate::engine::ClientOperationRecord::new(
                crate::engine::ClientOperationIntent::new(record.id, record.fingerprint)?,
                result,
            )
        })
        .transpose()
}
