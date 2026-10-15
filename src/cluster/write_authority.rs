use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryAuthorityPhase {
    Inactive,
    Promoting,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalHistory {
    pub activation_id: u64,
    pub primary_term: u64,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub max_seq_no: Option<u64>,
    pub digest: [u8; 32],
}

impl CanonicalHistory {
    pub fn validate(&self) -> Result<(), String> {
        if self.activation_id == 0 || self.primary_term == 0 || self.digest == [0; 32] {
            return Err(
                "canonical history requires a positive activation, term, and digest".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryAuthority {
    pub phase: PrimaryAuthorityPhase,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub activation_id: Option<u64>,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub history: Option<CanonicalHistory>,
}

impl Default for PrimaryAuthority {
    fn default() -> Self {
        Self {
            phase: PrimaryAuthorityPhase::Inactive,
            activation_id: None,
            history: None,
        }
    }
}

impl PrimaryAuthority {
    pub fn invalidate(&mut self) {
        self.phase = PrimaryAuthorityPhase::Inactive;
        self.activation_id = None;
    }

    pub fn reserve(&mut self, activation_id: u64) {
        self.phase = PrimaryAuthorityPhase::Promoting;
        self.activation_id = Some(activation_id);
    }

    pub fn finish(&mut self, history: CanonicalHistory) {
        self.phase = PrimaryAuthorityPhase::Active;
        self.activation_id = Some(history.activation_id);
        self.history = Some(history);
    }

    pub fn validate(&self, primary_term: u64, initialized: bool) -> Result<(), String> {
        if let Some(history) = &self.history {
            history.validate()?;
            if !initialized || history.primary_term > primary_term {
                return Err("canonical history exceeds initialized shard authority".into());
            }
        }
        match self.phase {
            PrimaryAuthorityPhase::Inactive => {
                if self.activation_id.is_some() {
                    return Err("inactive primary authority cannot carry an activation ID".into());
                }
            }
            PrimaryAuthorityPhase::Promoting | PrimaryAuthorityPhase::Active => {
                let activation_id = self
                    .activation_id
                    .filter(|id| *id > 0)
                    .ok_or("primary authority requires a positive activation ID")?;
                if !initialized {
                    return Err("uninitialized shard cannot grant primary authority".into());
                }
                match (&self.history, self.phase) {
                    (Some(history), PrimaryAuthorityPhase::Active)
                        if history.activation_id == activation_id
                            && history.primary_term == primary_term => {}
                    (_, PrimaryAuthorityPhase::Active) => {
                        return Err("active primary requires its exact canonical history".into());
                    }
                    (Some(history), PrimaryAuthorityPhase::Promoting)
                        if history.primary_term >= primary_term
                            || history.activation_id >= activation_id =>
                    {
                        return Err(
                            "promotion must supersede the previous canonical authority".into()
                        );
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteAuthorityIdentity {
    pub index_uuid: String,
    pub shard_id: u32,
    pub primary_node_id: String,
    pub primary_allocation_id: u64,
    pub primary_term: u64,
    pub activation_id: u64,
}

impl WriteAuthorityIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.index_uuid.is_empty()
            || self.primary_node_id.is_empty()
            || self.primary_allocation_id == 0
            || self.primary_term == 0
            || self.activation_id == 0
        {
            return Err(
                "write authority requires exact UUID, primary, allocation, term, and activation"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardCopyAllocation {
    pub node_id: String,
    pub allocation_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedCopyExclusion {
    pub authority: WriteAuthorityIdentity,
    pub captured_state_version: u64,
    pub transition_id: String,
    pub failed_copies: Vec<ShardCopyAllocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryActivationFinish {
    pub authority: WriteAuthorityIdentity,
    pub history: CanonicalHistory,
    pub eligible_replicas: Vec<ShardCopyAllocation>,
}

impl PrimaryActivationFinish {
    pub fn validate(&self) -> Result<(), String> {
        self.authority.validate()?;
        self.history.validate()?;
        if self.history.activation_id != self.authority.activation_id
            || self.history.primary_term != self.authority.primary_term
        {
            return Err("activation history does not match the reserved authority".into());
        }
        let mut seen = HashSet::new();
        for copy in &self.eligible_replicas {
            if copy.node_id.is_empty()
                || copy.node_id == self.authority.primary_node_id
                || copy.allocation_id == 0
                || !seen.insert(&copy.node_id)
            {
                return Err("activation contains an invalid or duplicate eligible replica".into());
            }
        }
        Ok(())
    }
}

impl FailedCopyExclusion {
    pub fn validate(&self) -> Result<(), String> {
        self.authority.validate()?;
        if self.captured_state_version == 0 {
            return Err("failed-copy exclusion requires its captured applied-state version".into());
        }
        if !uuid::Uuid::parse_str(&self.transition_id)
            .is_ok_and(|id| id.to_string() == self.transition_id)
        {
            return Err("failed-copy exclusion requires a canonical UUID transition ID".into());
        }
        if self.failed_copies.is_empty() {
            return Err("failed-copy exclusion requires at least one failed allocation".into());
        }
        let mut seen = HashSet::new();
        for copy in &self.failed_copies {
            if copy.node_id.is_empty()
                || copy.node_id == self.authority.primary_node_id
                || copy.allocation_id == 0
                || !seen.insert(&copy.node_id)
            {
                return Err(
                    "failed-copy exclusion contains an invalid or duplicate replica".into(),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_authority_requires_an_exact_canonical_certificate() {
        let mut authority = PrimaryAuthority::default();
        authority.reserve(9);
        assert!(authority.validate(3, true).is_ok());
        assert!(authority.validate(3, false).is_err());
        authority.finish(CanonicalHistory {
            activation_id: 9,
            primary_term: 3,
            max_seq_no: Some(7),
            digest: [1; 32],
        });
        assert!(authority.validate(3, true).is_ok());
        assert!(authority.validate(4, true).is_err());
        authority.invalidate();
        assert!(authority.validate(4, true).is_ok());
        authority.reserve(10);
        assert!(authority.validate(4, true).is_ok());
    }

    #[test]
    fn exclusion_identity_rejects_aba_ambiguous_and_primary_targets() {
        let mut exclusion = FailedCopyExclusion {
            authority: WriteAuthorityIdentity {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                primary_node_id: "primary".into(),
                primary_allocation_id: 7,
                primary_term: 3,
                activation_id: 9,
            },
            captured_state_version: 10,
            transition_id: uuid::Uuid::new_v4().to_string(),
            failed_copies: vec![ShardCopyAllocation {
                node_id: "replica".into(),
                allocation_id: 8,
            }],
        };
        assert!(exclusion.validate().is_ok());
        exclusion.failed_copies[0].allocation_id = 0;
        assert!(exclusion.validate().is_err());
        exclusion.failed_copies[0].allocation_id = 8;
        exclusion.failed_copies[0].node_id = "primary".into();
        assert!(exclusion.validate().is_err());
    }
}
