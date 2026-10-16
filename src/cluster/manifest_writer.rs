use serde::{Deserialize, Serialize};

pub const DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS: u64 = 15_000;

/// Lifecycle phase of the Raft-replicated manifest writer authority for one
/// `remote_store` index (ADR 0002 section 3.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestWriterPhase {
    Unassigned,
    Fencing {
        holder_node: String,
        writer_epoch: u64,
        lease_id: u64,
    },
    Active {
        holder_node: String,
        writer_epoch: u64,
        lease_id: u64,
        fenced_pointer_revision: u64,
        fenced_pointer_token: String,
    },
}

impl ManifestWriterPhase {
    pub fn holder_node(&self) -> Option<&str> {
        match self {
            Self::Unassigned => None,
            Self::Fencing { holder_node, .. } | Self::Active { holder_node, .. } => {
                Some(holder_node.as_str())
            }
        }
    }

    pub fn writer_epoch(&self) -> Option<u64> {
        match self {
            Self::Unassigned => None,
            Self::Fencing { writer_epoch, .. } | Self::Active { writer_epoch, .. } => {
                Some(*writer_epoch)
            }
        }
    }

    pub fn lease_id(&self) -> Option<u64> {
        match self {
            Self::Unassigned => None,
            Self::Fencing { lease_id, .. } | Self::Active { lease_id, .. } => Some(*lease_id),
        }
    }

    pub fn is_unassigned(&self) -> bool {
        matches!(self, Self::Unassigned)
    }

    pub fn is_fencing(&self) -> bool {
        matches!(self, Self::Fencing { .. })
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn matches_identity(&self, identity: &ManifestWriterIdentity) -> bool {
        match self {
            Self::Unassigned => false,
            Self::Fencing {
                holder_node,
                writer_epoch,
                lease_id,
            }
            | Self::Active {
                holder_node,
                writer_epoch,
                lease_id,
                ..
            } => {
                holder_node == &identity.holder_node
                    && *writer_epoch == identity.writer_epoch
                    && *lease_id == identity.lease_id
            }
        }
    }

    pub fn validate(&self, last_writer_epoch: u64, last_lease_id: u64) -> Result<(), String> {
        match self {
            Self::Unassigned => Ok(()),
            Self::Fencing {
                holder_node,
                writer_epoch,
                lease_id,
            } => {
                if holder_node.is_empty() || *writer_epoch == 0 || *lease_id == 0 {
                    return Err(
                        "fencing manifest writer phase requires non-empty holder_node and positive writer_epoch/lease_id"
                            .into(),
                    );
                }
                if *writer_epoch > last_writer_epoch || *lease_id > last_lease_id {
                    return Err(
                        "fencing manifest writer phase exceeds recorded last_writer_epoch or last_lease_id"
                            .into(),
                    );
                }
                Ok(())
            }
            Self::Active {
                holder_node,
                writer_epoch,
                lease_id,
                fenced_pointer_revision,
                fenced_pointer_token,
            } => {
                if holder_node.is_empty()
                    || *writer_epoch == 0
                    || *lease_id == 0
                    || *fenced_pointer_revision == 0
                    || fenced_pointer_token.is_empty()
                {
                    return Err(
                        "active manifest writer phase requires non-empty holder_node/token and positive writer_epoch/lease_id/fenced_pointer_revision"
                            .into(),
                    );
                }
                if *writer_epoch > last_writer_epoch || *lease_id > last_lease_id {
                    return Err(
                        "active manifest writer phase exceeds recorded last_writer_epoch or last_lease_id"
                            .into(),
                    );
                }
                Ok(())
            }
        }
    }
}

/// Authoritative Raft-replicated sequencer lease and monotonic epoch record for
/// one `remote_store` index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestWriterAuthority {
    pub index_name: String,
    pub index_uuid: String,
    /// Highest writer epoch ever allocated for this index incarnation.
    /// Monotonically incremented on every Acquire; never reused.
    pub last_writer_epoch: u64,
    /// Highest lease ID ever allocated for this index incarnation.
    pub last_lease_id: u64,
    /// Monotonic renewal counter within the current lease.
    pub renewal_seq: u64,
    /// Raft leader term under which the current lease was acquired or last renewed.
    pub leader_term: u64,
    /// Leader-evaluated expiration deadline in epoch milliseconds (0 when Unassigned).
    pub lease_expires_at_millis: u64,
    pub phase: ManifestWriterPhase,
}

impl ManifestWriterAuthority {
    pub fn unassigned(index_name: impl Into<String>, index_uuid: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
            index_uuid: index_uuid.into(),
            last_writer_epoch: 0,
            last_lease_id: 0,
            renewal_seq: 0,
            leader_term: 0,
            lease_expires_at_millis: 0,
            phase: ManifestWriterPhase::Unassigned,
        }
    }

    pub fn expected_snapshot(&self) -> ExpectedManifestWriterAuthority {
        ExpectedManifestWriterAuthority {
            index_uuid: self.index_uuid.clone(),
            last_writer_epoch: self.last_writer_epoch,
            last_lease_id: self.last_lease_id,
            phase: self.phase.clone(),
        }
    }

    pub fn current_identity(&self) -> Option<ManifestWriterIdentity> {
        match &self.phase {
            ManifestWriterPhase::Unassigned => None,
            ManifestWriterPhase::Fencing {
                holder_node,
                writer_epoch,
                lease_id,
            }
            | ManifestWriterPhase::Active {
                holder_node,
                writer_epoch,
                lease_id,
                ..
            } => Some(ManifestWriterIdentity {
                index_uuid: self.index_uuid.clone(),
                holder_node: holder_node.clone(),
                writer_epoch: *writer_epoch,
                lease_id: *lease_id,
            }),
        }
    }

    pub fn matches_expected(&self, expected: &ExpectedManifestWriterAuthority) -> bool {
        self.index_uuid == expected.index_uuid
            && self.last_writer_epoch == expected.last_writer_epoch
            && self.last_lease_id == expected.last_lease_id
            && self.phase == expected.phase
    }

    pub fn revoke_to_unassigned(&mut self) {
        self.phase = ManifestWriterPhase::Unassigned;
        self.lease_expires_at_millis = 0;
        self.renewal_seq = 0;
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.index_name.is_empty() || self.index_uuid.is_empty() {
            return Err(
                "manifest writer authority requires non-empty index_name and index_uuid".into(),
            );
        }
        self.phase
            .validate(self.last_writer_epoch, self.last_lease_id)?;
        match &self.phase {
            ManifestWriterPhase::Unassigned => {
                if self.lease_expires_at_millis != 0 || self.renewal_seq != 0 {
                    return Err(
                        "unassigned manifest writer authority must have zero lease_expires_at_millis and renewal_seq"
                            .into(),
                    );
                }
            }
            ManifestWriterPhase::Fencing { writer_epoch, .. }
            | ManifestWriterPhase::Active { writer_epoch, .. } => {
                if *writer_epoch != self.last_writer_epoch {
                    return Err(
                        "assigned manifest writer phase epoch must equal last_writer_epoch".into(),
                    );
                }
                if self.leader_term == 0 || self.lease_expires_at_millis == 0 {
                    return Err(
                        "assigned manifest writer authority requires positive leader_term and lease_expires_at_millis"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }
}

/// Exact holder identity for an in-progress (`Fencing`) or active (`Active`)
/// manifest writer lease.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestWriterIdentity {
    pub index_uuid: String,
    pub holder_node: String,
    pub writer_epoch: u64,
    pub lease_id: u64,
}

impl ManifestWriterIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.index_uuid.is_empty()
            || self.holder_node.is_empty()
            || self.writer_epoch == 0
            || self.lease_id == 0
        {
            return Err(
                "manifest writer identity requires non-empty index_uuid/holder_node and positive writer_epoch/lease_id"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Complete preceding authority snapshot required for conditional `Acquire`
/// transitions (acquire from unassigned, replace after expiry/holder loss, or
/// transfer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedManifestWriterAuthority {
    pub index_uuid: String,
    pub last_writer_epoch: u64,
    pub last_lease_id: u64,
    pub phase: ManifestWriterPhase,
}

impl ExpectedManifestWriterAuthority {
    pub fn validate(&self) -> Result<(), String> {
        if self.index_uuid.is_empty() {
            return Err("expected manifest writer authority requires non-empty index_uuid".into());
        }
        self.phase
            .validate(self.last_writer_epoch, self.last_lease_id)
    }
}

/// Reason a candidate requests a new `Fencing` epoch on an index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestLeaseAcquireReason {
    /// Acquire when no holder is currently assigned (`phase == Unassigned`).
    Unassigned,
    /// Replace an existing `Fencing` or `Active` holder whose lease has expired
    /// (`now_millis >= lease_expires_at_millis`).
    Expired,
    /// Replace a holder node that is no longer a registered cluster member.
    HolderUnavailable,
    /// Explicitly transfer ownership from `from_holder` to the candidate node
    /// with a strictly greater `writer_epoch`.
    Transfer { from_holder: String },
    /// Explicitly revoke the expected holder and start a new fencing epoch.
    RevokeAndReplace,
}

impl ManifestLeaseAcquireReason {
    pub fn validate(&self) -> Result<(), String> {
        if let Self::Transfer { from_holder } = self
            && from_holder.is_empty()
        {
            return Err("manifest writer transfer requires a non-empty from_holder".into());
        }
        Ok(())
    }
}

/// Conditional Raft mutation on an index's [`ManifestWriterAuthority`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestWriterMutation {
    Acquire {
        expected: ExpectedManifestWriterAuthority,
        candidate_node: String,
        reason: ManifestLeaseAcquireReason,
        now_millis: u64,
        lease_duration_ms: u64,
        leader_term: u64,
    },
    Activate {
        identity: ManifestWriterIdentity,
        fenced_pointer_revision: u64,
        fenced_pointer_token: String,
        now_millis: u64,
        lease_duration_ms: u64,
        leader_term: u64,
    },
    Renew {
        identity: ManifestWriterIdentity,
        expected_fenced_pointer_revision: u64,
        expected_fenced_pointer_token: String,
        now_millis: u64,
        lease_duration_ms: u64,
        leader_term: u64,
    },
    Release {
        identity: ManifestWriterIdentity,
    },
}

impl ManifestWriterMutation {
    pub fn index_uuid(&self) -> &str {
        match self {
            Self::Acquire { expected, .. } => &expected.index_uuid,
            Self::Activate { identity, .. }
            | Self::Renew { identity, .. }
            | Self::Release { identity } => &identity.index_uuid,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Acquire {
                expected,
                candidate_node,
                reason,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                expected.validate()?;
                reason.validate()?;
                if candidate_node.is_empty() {
                    return Err("manifest writer acquire requires non-empty candidate_node".into());
                }
                if *now_millis == 0 || *lease_duration_ms == 0 || *leader_term == 0 {
                    return Err(
                        "manifest writer acquire requires positive now_millis, lease_duration_ms, and leader_term"
                            .into(),
                    );
                }
                if now_millis.checked_add(*lease_duration_ms).is_none() {
                    return Err("manifest writer acquire lease expiration overflows u64".into());
                }
                Ok(())
            }
            Self::Activate {
                identity,
                fenced_pointer_revision,
                fenced_pointer_token,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                identity.validate()?;
                if *fenced_pointer_revision == 0 || fenced_pointer_token.is_empty() {
                    return Err(
                        "manifest writer activation requires positive fenced_pointer_revision and non-empty fenced_pointer_token"
                            .into(),
                    );
                }
                if *now_millis == 0 || *lease_duration_ms == 0 || *leader_term == 0 {
                    return Err(
                        "manifest writer activation requires positive now_millis, lease_duration_ms, and leader_term"
                            .into(),
                    );
                }
                if now_millis.checked_add(*lease_duration_ms).is_none() {
                    return Err("manifest writer activation lease expiration overflows u64".into());
                }
                Ok(())
            }
            Self::Renew {
                identity,
                expected_fenced_pointer_revision,
                expected_fenced_pointer_token,
                now_millis,
                lease_duration_ms,
                leader_term,
            } => {
                identity.validate()?;
                if *expected_fenced_pointer_revision == 0
                    || expected_fenced_pointer_token.is_empty()
                {
                    return Err(
                        "manifest writer renewal requires positive expected_fenced_pointer_revision and non-empty expected_fenced_pointer_token"
                            .into(),
                    );
                }
                if *now_millis == 0 || *lease_duration_ms == 0 || *leader_term == 0 {
                    return Err(
                        "manifest writer renewal requires positive now_millis, lease_duration_ms, and leader_term"
                            .into(),
                    );
                }
                if now_millis.checked_add(*lease_duration_ms).is_none() {
                    return Err("manifest writer renewal lease expiration overflows u64".into());
                }
                Ok(())
            }
            Self::Release { identity } => identity.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        ClusterState, IndexEngine, IndexMetadata, IndexSettings, IndexUuid, NodeInfo, NodeRole,
    };
    use std::collections::HashMap;

    fn make_node(id: &str, raft_node_id: u64) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id,
        }
    }

    fn make_remote_index(name: &str, uuid: &str) -> IndexMetadata {
        IndexMetadata {
            name: name.into(),
            uuid: IndexUuid::new(uuid),
            number_of_shards: 0,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings {
                engine: IndexEngine::RemoteStore,
                ..Default::default()
            },
        }
    }

    #[test]
    fn manifest_writer_authority_and_mutations_validate_and_roundtrip_serde() {
        let mut authority = ManifestWriterAuthority::unassigned("events", "uuid-1");
        assert!(authority.validate().is_ok());

        authority.last_writer_epoch = 2;
        authority.last_lease_id = 11;
        authority.renewal_seq = 3;
        authority.leader_term = 4;
        authority.lease_expires_at_millis = 50_000;
        authority.phase = ManifestWriterPhase::Active {
            holder_node: "node-1".into(),
            writer_epoch: 2,
            lease_id: 11,
            fenced_pointer_revision: 5,
            fenced_pointer_token: "fs1:s3:aa:-:bb".into(),
        };
        assert!(authority.validate().is_ok());

        let json = serde_json::to_string(&authority).unwrap();
        let restored: ManifestWriterAuthority = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, authority);

        // Invalid: Active with epoch != last_writer_epoch or zero revision.
        let mut bad = authority.clone();
        bad.last_writer_epoch = 3;
        assert!(bad.validate().is_err());

        let mut bad_rev = authority.clone();
        bad_rev.phase = ManifestWriterPhase::Active {
            holder_node: "node-1".into(),
            writer_epoch: 2,
            lease_id: 11,
            fenced_pointer_revision: 0,
            fenced_pointer_token: "tok".into(),
        };
        assert!(bad_rev.validate().is_err());

        let mutations = [
            ManifestWriterMutation::Acquire {
                expected: authority.expected_snapshot(),
                candidate_node: "node-2".into(),
                reason: ManifestLeaseAcquireReason::Transfer {
                    from_holder: "node-1".into(),
                },
                now_millis: 10_000,
                lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                leader_term: 4,
            },
            ManifestWriterMutation::Activate {
                identity: authority.current_identity().unwrap(),
                fenced_pointer_revision: 6,
                fenced_pointer_token: "tok-6".into(),
                now_millis: 10_000,
                lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                leader_term: 4,
            },
            ManifestWriterMutation::Renew {
                identity: authority.current_identity().unwrap(),
                expected_fenced_pointer_revision: 5,
                expected_fenced_pointer_token: "fs1:s3:aa:-:bb".into(),
                now_millis: 12_000,
                lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                leader_term: 4,
            },
            ManifestWriterMutation::Release {
                identity: authority.current_identity().unwrap(),
            },
        ];
        for mutation in mutations {
            assert!(mutation.validate().is_ok());
            let encoded = serde_json::to_string(&mutation).unwrap();
            let decoded: ManifestWriterMutation = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, mutation);
        }
    }

    #[test]
    fn cluster_state_validates_active_manifest_writer_and_pauses_on_expiry_or_leader_term_change() {
        let mut state = ClusterState::new("c".into());
        state.add_node(make_node("node-1", 1));
        state.add_index(make_remote_index("events", "uuid-1"));

        let authority = state.manifest_writer_authorities.get_mut("events").unwrap();
        authority.last_writer_epoch = 1;
        authority.last_lease_id = 7;
        authority.leader_term = 2;
        authority.lease_expires_at_millis = 20_000;
        authority.phase = ManifestWriterPhase::Active {
            holder_node: "node-1".into(),
            writer_epoch: 1,
            lease_id: 7,
            fenced_pointer_revision: 1,
            fenced_pointer_token: "tok-1".into(),
        };
        let identity = authority.current_identity().unwrap();

        // Valid before expiry under matching leader term 2.
        assert!(
            state
                .validate_active_manifest_writer("events", &identity, 19_999, Some(2))
                .is_ok()
        );

        // Expired at 20_000 fails closed.
        let expired_err = state
            .validate_active_manifest_writer("events", &identity, 20_000, Some(2))
            .unwrap_err();
        assert!(expired_err.contains("expired"));

        // Leader change to term 3 conservatively pauses publication until renewed.
        let term_err = state
            .validate_active_manifest_writer("events", &identity, 15_000, Some(3))
            .unwrap_err();
        assert!(term_err.contains("current leader term is 3"));

        // Removing the holder node revokes the lease to Unassigned while preserving last_writer_epoch.
        state.remove_node(&"node-1".into());
        let after_remove = &state.manifest_writer_authorities["events"];
        assert!(after_remove.phase.is_unassigned());
        assert_eq!(after_remove.last_writer_epoch, 1);
        assert_eq!(after_remove.last_lease_id, 7);
    }

    #[test]
    fn cluster_state_rejects_old_shape_missing_manifest_writer_authorities_field() {
        let mut state = ClusterState::new("c".into());
        state.add_index(make_remote_index("events", "uuid-1"));

        let mut raw = serde_json::to_value(&state).unwrap();
        let restored: ClusterState = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(
            restored.manifest_writer_authorities,
            state.manifest_writer_authorities
        );

        raw.as_object_mut()
            .unwrap()
            .remove("manifest_writer_authorities");
        assert!(
            serde_json::from_value::<ClusterState>(raw).is_err(),
            "old ClusterState JSON missing manifest_writer_authorities must be rejected"
        );
    }
}
