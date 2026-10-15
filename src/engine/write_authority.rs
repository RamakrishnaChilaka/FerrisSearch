use crate::cluster::write_authority::{CanonicalHistory, WriteAuthorityIdentity};
use anyhow::{Result, bail};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_IN_FLIGHT_WRITES_PER_SHARD: usize = 1024;

#[derive(Debug, thiserror::Error)]
#[error("write authority is unavailable: {reason}")]
pub(crate) struct WriteAuthorityUnavailable {
    reason: String,
}

fn unavailable(reason: impl Into<String>) -> anyhow::Error {
    WriteAuthorityUnavailable {
        reason: reason.into(),
    }
    .into()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CopyWriteBinding {
    pub index_uuid: String,
    pub shard_id: u32,
    pub allocation_id: u64,
}

#[derive(Debug)]
pub struct CopyWriteState {
    binding: Option<CopyWriteBinding>,
    enforce_primary_authority: AtomicBool,
    failure: Arc<OnceLock<String>>,
    primary_epoch: RwLock<Option<Arc<WriteAuthorityEpoch>>>,
    canonical_history: RwLock<Option<CanonicalHistory>>,
    local_mutations: RwLock<()>,
}

impl CopyWriteState {
    pub fn unbound() -> Arc<Self> {
        Self::with_failure(None, false, Arc::new(OnceLock::new()))
    }

    #[cfg(test)]
    pub(crate) fn unbound_with_failure(failure: Arc<OnceLock<String>>) -> Arc<Self> {
        Self::with_failure(None, false, failure)
    }

    pub(crate) fn for_copy(binding: CopyWriteBinding, enforce: bool) -> Result<Arc<Self>> {
        if binding.index_uuid.is_empty() || binding.allocation_id == 0 {
            bail!("copy write state requires exact UUID and allocation identity");
        }
        Ok(Self::with_failure(
            Some(binding),
            enforce,
            Arc::new(OnceLock::new()),
        ))
    }

    fn with_failure(
        binding: Option<CopyWriteBinding>,
        enforce_primary_authority: bool,
        failure: Arc<OnceLock<String>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            binding,
            enforce_primary_authority: AtomicBool::new(enforce_primary_authority),
            failure,
            primary_epoch: RwLock::new(None),
            canonical_history: RwLock::new(None),
            local_mutations: RwLock::new(()),
        })
    }

    pub(crate) fn failure_cell(&self) -> Arc<OnceLock<String>> {
        self.failure.clone()
    }

    pub(crate) fn require_primary_authority(&self) {
        self.enforce_primary_authority
            .store(true, Ordering::Release);
    }

    pub(crate) fn enforces_primary_authority(&self) -> bool {
        self.enforce_primary_authority.load(Ordering::Acquire)
    }

    pub(crate) fn canonical_history(&self) -> Result<Option<CanonicalHistory>> {
        self.canonical_history
            .read()
            .map(|history| history.clone())
            .map_err(|_| unavailable("canonical history lock was poisoned"))
    }

    pub(crate) fn install_canonical_history(&self, history: &CanonicalHistory) -> Result<()> {
        history.validate().map_err(anyhow::Error::msg)?;
        let mut current = self
            .canonical_history
            .write()
            .map_err(|_| unavailable("canonical history lock was poisoned"))?;
        if current.as_ref().is_some_and(|previous| {
            previous != history
                && (history.primary_term <= previous.primary_term
                    || history.activation_id <= previous.activation_id
                    || history.max_seq_no < previous.max_seq_no)
        }) {
            self.fail("durable and in-memory canonical copy history disagree");
            bail!("canonical copy history cannot regress or change within an epoch");
        }
        *current = Some(history.clone());
        Ok(())
    }

    pub fn failure_reason(&self) -> Option<String> {
        self.failure.get().cloned()
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        match self.failure.get() {
            Some(reason) => Err(crate::wal::WalFailStoppedError {
                reason: reason.clone(),
            }
            .into()),
            None => Ok(()),
        }
    }

    pub fn fail(&self, reason: &str) {
        self.failure.get_or_init(|| reason.to_string());
        match self.primary_epoch.read() {
            Ok(epoch) => {
                if let Some(epoch) = epoch.as_ref() {
                    epoch.revoke(reason);
                }
            }
            Err(poisoned) => {
                if let Some(epoch) = poisoned.into_inner().as_ref() {
                    epoch.revoke("primary authority lock was poisoned");
                }
            }
        }
    }

    pub(crate) fn mutation_guard(&self) -> Result<RwLockReadGuard<'_, ()>> {
        let guard = self.local_mutations.read().map_err(|_| {
            self.fail("local mutation gate was poisoned");
            unavailable("local mutation gate was poisoned")
        })?;
        self.ensure_healthy()?;
        Ok(guard)
    }

    pub(crate) fn primary_epoch(&self) -> Result<Option<Arc<WriteAuthorityEpoch>>> {
        self.primary_epoch
            .read()
            .map(|epoch| epoch.clone())
            .map_err(|_| {
                self.fail("primary authority lock was poisoned");
                unavailable("primary authority lock was poisoned")
            })
    }

    pub(crate) fn validate_primary_mutation(&self, primary_term: u64) -> Result<()> {
        self.ensure_healthy()?;
        match self.primary_epoch()? {
            Some(epoch) => epoch.validate_mutation(primary_term),
            None if !self.enforce_primary_authority.load(Ordering::Acquire) => Ok(()),
            None => Err(unavailable(
                "primary has not completed canonical activation",
            )),
        }
    }

    pub(crate) fn install_primary_epoch(&self, epoch: Arc<WriteAuthorityEpoch>) -> Result<()> {
        self.ensure_healthy()?;
        if !Arc::ptr_eq(&self.failure, &epoch.failure) {
            bail!("primary epoch must use this exact copy's durability state");
        }
        if let Some(binding) = &self.binding
            && (binding.index_uuid != epoch.identity.index_uuid
                || binding.shard_id != epoch.identity.shard_id
                || binding.allocation_id != epoch.identity.primary_allocation_id)
        {
            bail!("primary write epoch does not match this exact shard copy");
        }
        let mut current = self
            .primary_epoch
            .write()
            .map_err(|_| unavailable("primary authority lock was poisoned"))?;
        if let Some(previous) = current.as_ref()
            && !Arc::ptr_eq(previous, &epoch)
        {
            if epoch.identity.primary_term <= previous.identity.primary_term {
                return Err(unavailable(
                    "a primary epoch cannot be revived or replaced at the same term",
                ));
            }
            previous.revoke("superseded by a newer primary epoch");
        }
        *current = Some(epoch);
        Ok(())
    }

    pub(crate) fn revoke_and_drain(&self, reason: &str) -> Result<()> {
        if let Some(epoch) = self.primary_epoch()? {
            epoch.revoke(reason);
        }
        let _drained = self.local_mutations.write().map_err(|_| {
            self.fail("local mutation drain was poisoned");
            unavailable("local mutation drain was poisoned")
        })?;
        self.ensure_healthy()
    }
}

#[derive(Debug)]
pub struct WriteAuthorityEpoch {
    pub(crate) identity: WriteAuthorityIdentity,
    failure: Arc<OnceLock<String>>,
    revoked: OnceLock<String>,
    exclusion_debt: Mutex<HashSet<String>>,
    admission: Arc<Semaphore>,
    applied_authority: Option<AppliedWriteAuthority>,
}

struct AppliedWriteAuthority {
    index: String,
    cluster: Arc<crate::cluster::ClusterManager>,
}

impl std::fmt::Debug for AppliedWriteAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppliedWriteAuthority")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl WriteAuthorityEpoch {
    #[cfg(test)]
    pub(crate) fn new(
        identity: WriteAuthorityIdentity,
        copy: &CopyWriteState,
    ) -> Result<Arc<Self>> {
        Self::build(identity, copy, None)
    }

    pub(crate) fn for_applied_authority(
        identity: WriteAuthorityIdentity,
        copy: &CopyWriteState,
        index: String,
        cluster: Arc<crate::cluster::ClusterManager>,
    ) -> Result<Arc<Self>> {
        Self::build(
            identity,
            copy,
            Some(AppliedWriteAuthority { index, cluster }),
        )
    }

    fn build(
        identity: WriteAuthorityIdentity,
        copy: &CopyWriteState,
        applied_authority: Option<AppliedWriteAuthority>,
    ) -> Result<Arc<Self>> {
        identity.validate().map_err(anyhow::Error::msg)?;
        copy.ensure_healthy()?;
        Ok(Arc::new(Self {
            identity,
            failure: copy.failure_cell(),
            revoked: OnceLock::new(),
            exclusion_debt: Mutex::new(HashSet::new()),
            admission: Arc::new(Semaphore::new(MAX_IN_FLIGHT_WRITES_PER_SHARD)),
            applied_authority,
        }))
    }

    pub(crate) fn revoke(&self, reason: &str) {
        self.revoked.get_or_init(|| reason.to_string());
    }

    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked.get().is_some()
    }

    #[cfg(test)]
    pub(crate) fn available_admission_slots_for_test(&self) -> usize {
        self.admission.available_permits()
    }

    pub(crate) fn close_exclusion_admission(&self, transition_id: &str) -> Result<()> {
        self.exclusion_debt
            .lock()
            .map_err(|_| {
                self.revoke("exclusion debt lock was poisoned");
                unavailable("exclusion debt lock was poisoned")
            })?
            .insert(transition_id.to_string());
        Ok(())
    }

    pub(crate) fn settle_exclusion(&self, transition_id: &str) -> Result<()> {
        self.exclusion_debt
            .lock()
            .map_err(|_| {
                self.revoke("exclusion debt lock was poisoned");
                unavailable("exclusion debt lock was poisoned")
            })?
            .remove(transition_id);
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if let Some(reason) = self.revoked.get() {
            return Err(unavailable(format!("primary permit was revoked: {reason}")));
        }
        if let Some(reason) = self.failure.get() {
            return Err(crate::wal::WalFailStoppedError {
                reason: reason.clone(),
            }
            .into());
        }
        if let Some(view) = &self.applied_authority {
            view.cluster.with_state(|state| {
                if let Err(reason) = state.validate_write_authority(
                    &view.index,
                    &self.identity,
                    crate::cluster::state::PrimaryAuthorityPhase::Active,
                ) {
                    self.revoke(&reason);
                    return Err(unavailable(reason));
                }
                if state.primary_unavailable(&view.index, self.identity.shard_id) {
                    self.revoke("applied metadata marks this primary copy unavailable");
                    return Err(unavailable(
                        "applied metadata marks this primary copy unavailable",
                    ));
                }
                Ok(())
            })?;
        }
        if !self
            .exclusion_debt
            .lock()
            .map_err(|_| {
                self.revoke("exclusion debt lock was poisoned");
                unavailable("exclusion debt lock was poisoned")
            })?
            .is_empty()
        {
            return Err(unavailable(
                "failed-copy exclusions are not definitively settled",
            ));
        }
        Ok(())
    }

    fn validate_mutation(&self, primary_term: u64) -> Result<()> {
        self.validate()?;
        if primary_term != self.identity.primary_term {
            return Err(unavailable(format!(
                "mutation term {primary_term} does not match permitted term {}",
                self.identity.primary_term,
            )));
        }
        Ok(())
    }

    pub(crate) fn permit(self: &Arc<Self>) -> Result<WritePermit> {
        self.validate()?;
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable("primary write admission capacity is exhausted"))?;
        self.validate()?;
        Ok(WritePermit {
            epoch: self.clone(),
            _admission: Arc::new(admission),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WritePermit {
    epoch: Arc<WriteAuthorityEpoch>,
    _admission: Arc<OwnedSemaphorePermit>,
}

impl WritePermit {
    pub(crate) fn validate(&self) -> Result<()> {
        self.epoch.validate()
    }

    pub(crate) fn identity(&self) -> &WriteAuthorityIdentity {
        &self.epoch.identity
    }

    pub(crate) fn close_exclusion_admission(&self, transition_id: &str) -> Result<()> {
        self.epoch.close_exclusion_admission(transition_id)
    }

    pub(crate) fn revoke(&self, reason: &str) {
        self.epoch.revoke(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch() -> (Arc<CopyWriteState>, Arc<WriteAuthorityEpoch>) {
        let copy = CopyWriteState::for_copy(
            CopyWriteBinding {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                allocation_id: 7,
            },
            true,
        )
        .unwrap();
        let epoch = WriteAuthorityEpoch::new(
            WriteAuthorityIdentity {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                primary_node_id: "primary".into(),
                primary_allocation_id: 7,
                primary_term: 3,
                activation_id: 9,
            },
            &copy,
        )
        .unwrap();
        copy.install_primary_epoch(epoch.clone()).unwrap();
        (copy, epoch)
    }

    #[test]
    fn revocation_invalidates_issued_and_queued_permits_without_reviving_on_debt_clear() {
        let (copy, epoch) = epoch();
        let permit = epoch.permit().unwrap();
        epoch.close_exclusion_admission("transition-1").unwrap();
        assert!(permit.validate().is_err());
        assert!(copy.validate_primary_mutation(3).is_err());
        epoch.settle_exclusion("transition-1").unwrap();
        permit.validate().unwrap();
        epoch.revoke("conditional exclusion rejected stale primary authority");
        assert!(permit.validate().is_err());
        assert!(epoch.permit().is_err());
        epoch.settle_exclusion("transition-1").unwrap();
        assert!(copy.validate_primary_mutation(3).is_err());
    }

    #[test]
    fn settling_one_intent_cannot_clear_another_not_yet_journaled_intent() {
        let (_, epoch) = epoch();
        let permit = epoch.permit().unwrap();
        epoch.close_exclusion_admission("journaled").unwrap();
        epoch.close_exclusion_admission("waiting-for-disk").unwrap();
        epoch.settle_exclusion("journaled").unwrap();
        assert!(permit.validate().is_err());
        assert!(epoch.permit().is_err());
        epoch.settle_exclusion("waiting-for-disk").unwrap();
        permit.validate().unwrap();
    }

    #[test]
    fn admission_is_bounded_and_clones_hold_the_same_slot_until_last_drop() {
        let (_, epoch) = epoch();
        let mut permits = (0..MAX_IN_FLIGHT_WRITES_PER_SHARD)
            .map(|_| epoch.permit().unwrap())
            .collect::<Vec<_>>();
        assert!(epoch.permit().is_err());
        let retained = permits.pop().unwrap().clone();
        assert!(epoch.permit().is_err());
        drop(retained);
        assert!(epoch.permit().is_ok());
    }

    #[test]
    fn wal_failure_cell_revokes_existing_permits_without_a_control_plane_roundtrip() {
        let (copy, epoch) = epoch();
        let permit = epoch.permit().unwrap();
        copy.failure_cell().set("fsync failed".into()).unwrap();
        assert!(
            permit
                .validate()
                .unwrap_err()
                .is::<crate::wal::WalFailStoppedError>()
        );
        assert!(copy.validate_primary_mutation(3).is_err());
    }

    #[test]
    fn applied_authority_revokes_existing_permits_without_waiting_for_lifecycle_reconciliation() {
        use crate::cluster::state::{CanonicalHistory, ClusterState, IndexMetadata, IndexUuid};
        let mut metadata = IndexMetadata::build_shard_routing("idx", 1, 0, &["primary".into()]);
        metadata.uuid = IndexUuid::new("uuid-1");
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 3;
        let mut state = ClusterState::new("test".into());
        state.add_index_with_allocation_id(metadata, 7).unwrap();
        let ids = state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        ids.primary_initialized = true;
        ids.primary_authority.finish(CanonicalHistory {
            activation_id: 9,
            primary_term: 3,
            max_seq_no: None,
            digest: [1; 32],
        });
        let shared = Arc::new(RwLock::new(state));
        let cluster = Arc::new(crate::cluster::ClusterManager::with_shared_state(
            shared.clone(),
        ));
        let copy = CopyWriteState::for_copy(
            CopyWriteBinding {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                allocation_id: 7,
            },
            true,
        )
        .unwrap();
        let epoch = WriteAuthorityEpoch::for_applied_authority(
            WriteAuthorityIdentity {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                primary_node_id: "primary".into(),
                primary_allocation_id: 7,
                primary_term: 3,
                activation_id: 9,
            },
            &copy,
            "idx".into(),
            cluster,
        )
        .unwrap();
        copy.install_primary_epoch(epoch.clone()).unwrap();
        let permit = epoch.permit().unwrap();
        permit.validate().unwrap();
        {
            let mut state = shared.write().unwrap();
            state
                .indices
                .get_mut("idx")
                .unwrap()
                .shard_routing
                .get_mut(&0)
                .unwrap()
                .primary_term = 4;
            state
                .shard_allocations
                .get_mut("idx")
                .unwrap()
                .get_mut(&0)
                .unwrap()
                .primary_authority
                .reserve(10);
        }
        assert!(permit.validate().is_err());
        assert!(epoch.is_revoked());
        assert!(copy.validate_primary_mutation(3).is_err());
        copy.ensure_healthy().unwrap();
    }

    #[test]
    fn primary_mutation_requires_activation_and_exact_term_on_a_bound_copy() {
        let copy = CopyWriteState::for_copy(
            CopyWriteBinding {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                allocation_id: 7,
            },
            true,
        )
        .unwrap();
        assert!(copy.validate_primary_mutation(3).is_err());
        let epoch = WriteAuthorityEpoch::new(
            WriteAuthorityIdentity {
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                primary_node_id: "primary".into(),
                primary_allocation_id: 7,
                primary_term: 3,
                activation_id: 9,
            },
            &copy,
        )
        .unwrap();
        copy.install_primary_epoch(epoch).unwrap();
        assert!(copy.validate_primary_mutation(2).is_err());
        copy.validate_primary_mutation(3).unwrap();
    }
}
