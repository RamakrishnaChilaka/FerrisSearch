use anyhow::Result;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

pub const CLIENT_OPERATION_ID_MAX_BYTES: usize = 256;
pub const CLIENT_OPERATION_RETENTION_OPERATIONS: u64 = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientOperationIntent {
    pub id: String,
    pub fingerprint: String,
}

impl ClientOperationIntent {
    pub fn validate_id(id: &str) -> Result<()> {
        validate_operation_id(id)
    }

    pub fn stable_document_id(id: &str) -> Result<String> {
        validate_operation_id(id)?;
        Ok(format!("{:x}", Sha256::digest(id.as_bytes())))
    }

    pub fn new(id: String, fingerprint: String) -> Result<Self> {
        validate_operation_id(&id)?;
        validate_fingerprint(&fingerprint)?;
        Ok(Self { id, fingerprint })
    }

    pub fn fingerprint_json(value: &serde_json::Value) -> Result<String> {
        let bytes = serde_json::to_vec(value)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum ClientOperationResult {
    Index {
        version: u64,
        created: bool,
    },
    Delete {
        version: u64,
        deleted: u64,
    },
    UpdateNoOp {
        seq_no: u64,
        primary_term: u64,
        version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientOperationRecord {
    pub id: String,
    pub fingerprint: String,
    pub result: ClientOperationResult,
}

impl ClientOperationRecord {
    pub fn new(intent: ClientOperationIntent, result: ClientOperationResult) -> Result<Self> {
        let record = Self {
            id: intent.id,
            fingerprint: intent.fingerprint,
            result,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn intent(&self) -> ClientOperationIntent {
        ClientOperationIntent {
            id: self.id.clone(),
            fingerprint: self.fingerprint.clone(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_operation_id(&self.id)?;
        validate_fingerprint(&self.fingerprint)?;
        match self.result {
            ClientOperationResult::Index { version, .. }
            | ClientOperationResult::Delete { version, .. }
                if version == 0 =>
            {
                anyhow::bail!("client operation document result requires a nonzero version");
            }
            ClientOperationResult::Delete { deleted, .. } if deleted > 1 => {
                anyhow::bail!("client operation delete result must be zero or one");
            }
            ClientOperationResult::UpdateNoOp {
                primary_term,
                version,
                ..
            } if primary_term == 0 || version == 0 => {
                anyhow::bail!(
                    "client operation update no-op result requires nonzero term and version"
                );
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("client operation ID [{operation_id}] was already used for a different request")]
pub struct ClientOperationConflictError {
    pub operation_id: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ClientOperationState {
    records: HashMap<String, (u64, u64, ClientOperationRecord)>,
    sequences: BTreeMap<u64, Vec<String>>,
}

impl ClientOperationState {
    pub fn lookup(
        &self,
        intent: &ClientOperationIntent,
    ) -> Result<Option<(u64, u64, &ClientOperationRecord)>> {
        let Some((seq_no, primary_term, record)) = self.records.get(&intent.id) else {
            return Ok(None);
        };
        if record.fingerprint != intent.fingerprint {
            return Err(ClientOperationConflictError {
                operation_id: intent.id.clone(),
            }
            .into());
        }
        Ok(Some((*seq_no, *primary_term, record)))
    }

    pub fn register(
        &mut self,
        seq_no: u64,
        primary_term: u64,
        record: ClientOperationRecord,
    ) -> Result<bool> {
        record.validate()?;
        if primary_term == 0 {
            anyhow::bail!("client operation record requires a nonzero primary term");
        }
        if let Some((existing_seq_no, existing_primary_term, existing)) =
            self.records.get(&record.id)
        {
            if *existing_seq_no == seq_no
                && *existing_primary_term == primary_term
                && existing == &record
            {
                return Ok(false);
            }
            return Err(ClientOperationConflictError {
                operation_id: record.id,
            }
            .into());
        }
        self.sequences
            .entry(seq_no)
            .or_default()
            .push(record.id.clone());
        self.records
            .insert(record.id.clone(), (seq_no, primary_term, record));
        Ok(true)
    }

    pub fn prune_for_max_seq_no(&mut self, max_seq_no: u64) {
        let minimum_retained = max_seq_no
            .saturating_add(1)
            .saturating_sub(CLIENT_OPERATION_RETENTION_OPERATIONS);
        let expired = self
            .sequences
            .range(..minimum_retained)
            .map(|(seq_no, _)| *seq_no)
            .collect::<Vec<_>>();
        for seq_no in expired {
            if let Some(ids) = self.sequences.remove(&seq_no) {
                for id in ids {
                    if self
                        .records
                        .get(&id)
                        .is_some_and(|(stored_seq_no, _, _)| *stored_seq_no == seq_no)
                    {
                        self.records.remove(&id);
                    }
                }
            }
        }
    }

    pub fn oldest_seq_no(&self) -> Option<u64> {
        self.sequences.first_key_value().map(|(seq_no, _)| *seq_no)
    }
}

fn validate_operation_id(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("client operation ID must not be empty");
    }
    if id.len() > CLIENT_OPERATION_ID_MAX_BYTES {
        anyhow::bail!(
            "client operation ID is {} bytes, exceeding maximum {}",
            id.len(),
            CLIENT_OPERATION_ID_MAX_BYTES
        );
    }
    Ok(())
}

fn validate_fingerprint(fingerprint: &str) -> Result<()> {
    if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("client operation fingerprint must be a 64-character hexadecimal SHA-256");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(id: &str, fingerprint: &str) -> ClientOperationIntent {
        ClientOperationIntent::new(id.to_string(), fingerprint.to_string()).unwrap()
    }

    #[test]
    fn state_returns_exact_record_and_rejects_conflicting_fingerprint() {
        let fingerprint = "a".repeat(64);
        let record = ClientOperationRecord::new(
            intent("op", &fingerprint),
            ClientOperationResult::Index {
                version: 1,
                created: true,
            },
        )
        .unwrap();
        let mut state = ClientOperationState::default();
        assert!(state.register(7, 3, record.clone()).unwrap());
        assert_eq!(
            state.lookup(&intent("op", &fingerprint)).unwrap(),
            Some((7, 3, &record))
        );
        assert!(
            state
                .lookup(&intent("op", &"b".repeat(64)))
                .unwrap_err()
                .is::<ClientOperationConflictError>()
        );
    }

    #[test]
    fn state_prunes_by_sequence_window() {
        let mut state = ClientOperationState::default();
        let record = ClientOperationRecord::new(
            intent("old", &"a".repeat(64)),
            ClientOperationResult::Delete {
                version: 1,
                deleted: 0,
            },
        )
        .unwrap();
        state.register(0, 1, record).unwrap();
        state.prune_for_max_seq_no(CLIENT_OPERATION_RETENTION_OPERATIONS - 1);
        assert!(
            state
                .lookup(&intent("old", &"a".repeat(64)))
                .unwrap()
                .is_some()
        );
        state.prune_for_max_seq_no(CLIENT_OPERATION_RETENTION_OPERATIONS);
        assert!(
            state
                .lookup(&intent("old", &"a".repeat(64)))
                .unwrap()
                .is_none()
        );
    }
}
