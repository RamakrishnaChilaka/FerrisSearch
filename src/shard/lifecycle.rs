use super::*;
use crate::cluster::ClusterManager;
use crate::cluster::state::ClusterState;

pub(super) struct AppliedShardAuthority {
    cluster_manager: Arc<ClusterManager>,
    pub(super) local_node_id: String,
}

#[derive(Debug)]
pub(crate) struct IndexIncarnationRetirementFailure {
    pub(crate) index: String,
    pub(crate) index_uuid: String,
    pub(crate) error: anyhow::Error,
}

#[derive(Debug, Default)]
pub(crate) struct IndexIncarnationRetirementErrors {
    pub(crate) failures: Vec<IndexIncarnationRetirementFailure>,
}

impl std::fmt::Display for IndexIncarnationRetirementErrors {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "index incarnation retirement failed")?;
        for failure in &self.failures {
            write!(
                formatter,
                "; [{}] UUID [{}]: {:#}",
                failure.index, failure.index_uuid, failure.error
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for IndexIncarnationRetirementErrors {}

impl IndexIncarnationRetirementErrors {
    fn into_result(self) -> Result<(), Self> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl ShardManager {
    pub(crate) fn bind_applied_shard_authority(
        &self,
        cluster_manager: Arc<ClusterManager>,
        local_node_id: String,
    ) {
        let authority = self
            .applied_authority
            .get_or_init(|| AppliedShardAuthority {
                cluster_manager: cluster_manager.clone(),
                local_node_id: local_node_id.clone(),
            });
        assert!(
            authority
                .cluster_manager
                .shares_state_with(&cluster_manager)
                && authority.local_node_id == local_node_id,
            "a shard manager must keep one applied Raft authority"
        );
    }

    pub(super) fn index_lifecycle_lock(&self, index: &str) -> Arc<RwLock<()>> {
        self.index_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(index.to_string())
            .or_default()
            .clone()
    }

    pub(super) fn with_current_copy<T>(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        allocation_id: Option<AllocationId>,
        operation: impl FnOnce(Option<&ClusterState>) -> Result<T>,
    ) -> Result<T> {
        let Some(authority) = self.applied_authority.get() else {
            // Only local helpers and no-Raft transport tests use an unbound manager.
            return operation(None);
        };
        authority.cluster_manager.with_state(|state| {
            let metadata = state.indices.get(&key.index);
            let uuid_matches =
                metadata.is_some_and(|metadata| metadata.uuid.as_str() == index_uuid);
            let allocation_matches = allocation_id.is_none_or(|allocation_id| {
                state.shard_allocation_id(&key.index, key.shard_id, &authority.local_node_id)
                    == Some(allocation_id)
            });
            if !uuid_matches || !allocation_matches {
                return Err(ShardReopenAborted {
                    index: key.index.clone(),
                    shard_id: key.shard_id,
                    expected_uuid: index_uuid.to_string(),
                    reason: format!(
                        "applied shard assignment changed: UUID {:?}, allocation {:?}",
                        metadata.map(|metadata| metadata.uuid.as_str()),
                        state.shard_allocation_id(
                            &key.index,
                            key.shard_id,
                            &authority.local_node_id,
                        )
                    ),
                }
                .into());
            }
            operation(Some(state))
        })
    }

    pub(super) fn copy_is_current(&self, state: &ClusterState, key: &ShardKey) -> bool {
        let Some(authority) = self.applied_authority.get() else {
            return true;
        };
        let Some(metadata) = state.indices.get(&key.index) else {
            return false;
        };
        self.copy_identity(&key.index, key.shard_id)
            .is_some_and(|identity| {
                identity.index_uuid == metadata.uuid.as_str()
                    && state.shard_allocation_id(&key.index, key.shard_id, &authority.local_node_id)
                        == Some(identity.allocation_id)
            })
    }

    pub(super) fn with_applied_state<T>(
        &self,
        operation: impl FnOnce(Option<&ClusterState>) -> T,
    ) -> T {
        match self.applied_authority.get() {
            Some(authority) => authority
                .cluster_manager
                .with_state(|state| operation(Some(state))),
            None => operation(None),
        }
    }

    fn local_index_incarnations(&self) -> Vec<(String, String)> {
        let mut incarnations = self
            .index_uuids
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(index, uuid)| (index.clone(), uuid.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        // A name registry cannot represent overlapping local incarnations.
        incarnations.extend(
            self.copy_identities
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .map(|(key, identity)| (key.index.clone(), identity.index_uuid.clone())),
        );
        incarnations.into_iter().collect()
    }

    fn incarnation_is_obsolete(&self, state: &ClusterState, index: &str, index_uuid: &str) -> bool {
        self.with_applied_state(|applied| {
            applied
                .unwrap_or(state)
                .indices
                .get(index)
                .is_none_or(|metadata| metadata.uuid.as_str() != index_uuid)
        })
    }

    #[cfg(test)]
    pub(crate) fn reconcile_index_incarnations(
        &self,
        state: &ClusterState,
    ) -> Result<(), IndexIncarnationRetirementErrors> {
        let mut errors = IndexIncarnationRetirementErrors::default();
        for (index, uuid) in self.local_index_incarnations() {
            if self.incarnation_is_obsolete(state, &index, &uuid)
                && let Err(error) = self.close_index_incarnation(
                    &index,
                    &uuid,
                    "applied_index_incarnation_retired",
                    false,
                    Some(state),
                )
            {
                errors.failures.push(IndexIncarnationRetirementFailure {
                    index,
                    index_uuid: uuid,
                    error,
                });
            }
        }
        errors.into_result()
    }

    pub(crate) fn retire_obsolete_shard_copy(
        &self,
        index: &str,
        shard_id: u32,
        state: &ClusterState,
        local_node_id: &str,
    ) {
        let key = ShardKey::new(index, shard_id);
        let lock = self.shard_open_lock(&key);
        let _guard = lock.lock().unwrap_or_else(|error| error.into_inner());
        let removed = self.with_applied_state(|applied| {
            let state = applied.unwrap_or(state);
            let identity = self.copy_identity(index, shard_id);
            let uuid = identity
                .as_ref()
                .map(|identity| identity.index_uuid.clone())
                .or_else(|| self.index_uuid(index));
            let allocation = state.shard_allocation_id(index, shard_id, local_node_id);
            let current = state.indices.get(index).is_some_and(|metadata| {
                uuid.as_deref() == Some(metadata.uuid.as_str())
                    && allocation.is_some()
                    && identity.is_none_or(|identity| Some(identity.allocation_id) == allocation)
            });
            if current {
                None
            } else {
                self.remove_serving_shard_copy(&key)
            }
        });
        drop(removed);
    }

    pub(crate) async fn quarantine_shard_copy_for_allocation_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
        allocation_id: AllocationId,
    ) -> Result<()> {
        let manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let lock = manager.shard_open_lock(&key);
            let _guard = lock.lock().unwrap_or_else(|error| error.into_inner());
            let removed = manager.with_applied_state(|state| {
                let applied_matches = state.is_none_or(|state| {
                    let authority = manager
                        .applied_authority
                        .get()
                        .expect("bound applied state");
                    state
                        .indices
                        .get(&index)
                        .is_some_and(|metadata| metadata.uuid.as_str() == index_uuid)
                        && state.shard_allocation_id(&index, shard_id, &authority.local_node_id)
                            == Some(allocation_id)
                });
                let cached_matches =
                    manager
                        .copy_identity(&index, shard_id)
                        .is_none_or(|identity| {
                            identity.index_uuid == index_uuid
                                && identity.allocation_id == allocation_id
                        });
                if !applied_matches || !cached_matches {
                    tracing::debug!(
                        index,
                        shard_id,
                        uuid = index_uuid,
                        allocation_id,
                        "Skipping stale shard-copy quarantine"
                    );
                    None
                } else {
                    manager.remove_serving_shard_copy(&key)
                }
            });
            drop(removed);
        })
        .await
        .map_err(|error| {
            anyhow::anyhow!("blocking allocation-scoped quarantine failed: {error}")
        })?;
        Ok(())
    }

    pub(super) fn remove_serving_shard_copy(
        &self,
        key: &ShardKey,
    ) -> Option<Arc<dyn SearchEngine>> {
        let removed = self
            .shards
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(key);
        self.copy_identities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(key);
        self.isr_tracker.remove_shard(&key.index, key.shard_id);
        removed
    }

    pub(crate) async fn reconcile_index_incarnations_blocking(
        self: &Arc<Self>,
        state: ClusterState,
    ) -> Result<(), IndexIncarnationRetirementErrors> {
        let mut errors = IndexIncarnationRetirementErrors::default();
        for (index, uuid) in self.local_index_incarnations() {
            if self.incarnation_is_obsolete(&state, &index, &uuid)
                && let Err(error) = self
                    .close_index_incarnation_blocking(
                        index.clone(),
                        uuid.clone(),
                        "applied_index_incarnation_retired",
                        false,
                        Some(state.clone()),
                    )
                    .await
            {
                errors.failures.push(IndexIncarnationRetirementFailure {
                    index,
                    index_uuid: uuid,
                    error,
                });
            }
        }
        errors.into_result()
    }

    pub(super) async fn retire_replaced_index_blocking(
        self: &Arc<Self>,
        index: &str,
        index_uuid: &str,
    ) -> Result<()> {
        let Some(authority) = self.applied_authority.get() else {
            return Ok(());
        };
        if self.index_uuid(index).as_deref() == Some(index_uuid) {
            return Ok(());
        }
        if self.index_uuid(index).is_none() {
            // A cold older open can hold the shared lock before registering its UUID.
            let manager = self.clone();
            let index = index.to_string();
            tokio::task::spawn_blocking(move || {
                let lock = manager.index_lifecycle_lock(&index);
                let _guard = lock.write().unwrap_or_else(|error| error.into_inner());
            })
            .await
            .map_err(|error| {
                anyhow::anyhow!("blocking index-open synchronization failed: {error}")
            })?;
        }
        let state = authority.cluster_manager.get_state();
        if let Some(uuid) = self.index_uuid(index)
            && uuid != index_uuid
            && self.incarnation_is_obsolete(&state, index, &uuid)
        {
            self.close_index_incarnation_blocking(
                index.to_string(),
                uuid,
                SHARD_DATA_REMOVE_REASON_STALE_UUID_REPLACEMENT,
                false,
                Some(state),
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) fn start_applied_index_reconciler(self: &Arc<Self>) {
        if self
            .applied_reconciler_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut reconciled_version = None;
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(manager) = weak.upgrade() else {
                    break;
                };
                let authority = manager
                    .applied_authority
                    .get()
                    .expect("applied index reconciliation requires a Raft authority");
                if reconciled_version == Some(authority.cluster_manager.version()) {
                    continue;
                }
                let state = authority.cluster_manager.get_state();
                let version = state.version;
                match manager.reconcile_index_incarnations_blocking(state).await {
                    Ok(()) => reconciled_version = Some(version),
                    Err(error) => {
                        tracing::warn!(
                            version,
                            error = %format!("{error:#}"),
                            "Applied index incarnation cleanup failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    pub(crate) async fn close_index_shards_for_uuid_blocking_with_reason(
        self: &Arc<Self>,
        index: String,
        index_uuid: String,
        reason: &'static str,
    ) -> Result<()> {
        self.close_index_incarnation_blocking(index, index_uuid, reason, true, None)
            .await
    }

    pub(super) fn close_unregistered_index_bookkeeping(&self, index: &str) -> Result<()> {
        let lock = self.index_lifecycle_lock(index);
        let _guard = lock.write().unwrap_or_else(|error| error.into_inner());
        if self.index_uuid(index).is_none() {
            self.isr_tracker.remove_index(index);
            self.settings_managers
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(index);
        }
        Ok(())
    }

    pub(super) fn close_index_incarnation(
        &self,
        index: &str,
        index_uuid: &str,
        reason: &'static str,
        delete_data: bool,
        state: Option<&ClusterState>,
    ) -> Result<()> {
        if index_uuid.is_empty() {
            anyhow::bail!("cannot close index [{index}] with an empty UUID");
        }
        let lifecycle_lock = self.index_lifecycle_lock(index);
        #[cfg(test)]
        if let Some(sender) = self
            .index_close_before_lock_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = sender.send(());
        }
        let _lifecycle_guard = lifecycle_lock
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if state.is_some_and(|state| !self.incarnation_is_obsolete(state, index, index_uuid)) {
            return Ok(());
        }
        let mut open_locks = self
            .open_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|(key, _)| key.index == index)
            .map(|(key, lock)| (key.shard_id, lock.clone()))
            .collect::<Vec<_>>();
        open_locks.sort_unstable_by_key(|(shard_id, _)| *shard_id);
        let _open_guards = open_locks
            .iter()
            .map(|(_, lock)| lock.lock().unwrap_or_else(|error| error.into_inner()))
            .collect::<Vec<_>>();
        let copy_uuids = self
            .copy_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|(key, _)| key.index == index)
            .map(|(key, identity)| (key.clone(), identity.index_uuid.clone()))
            .collect::<HashMap<_, _>>();
        let (retired_engines, remaining_keys) = {
            let mut shards = self
                .shards
                .write()
                .unwrap_or_else(|error| error.into_inner());
            let retired_engines = copy_uuids
                .iter()
                .filter(|(_, uuid)| uuid.as_str() == index_uuid)
                .filter_map(|(key, _)| shards.remove(key))
                .collect::<Vec<_>>();
            let remaining_keys = shards
                .keys()
                .filter(|key| key.index == index)
                .cloned()
                .collect::<Vec<_>>();
            (retired_engines, remaining_keys)
        };
        self.copy_identities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key, identity| key.index != index || identity.index_uuid != index_uuid);
        for (key, uuid) in &copy_uuids {
            if uuid == index_uuid {
                self.isr_tracker.remove_shard(index, key.shard_id);
            }
        }
        if remaining_keys.is_empty() {
            self.isr_tracker.remove_index(index);
            self.settings_managers
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(index);
        }
        self.with_applied_state(|state| {
            let remaining_uuid = remaining_keys.iter().find_map(|key| {
                if state.is_some_and(|state| !self.copy_is_current(state, key)) {
                    None
                } else {
                    copy_uuids.get(key).cloned()
                }
            });
            let mut uuids = self
                .index_uuids
                .write()
                .unwrap_or_else(|error| error.into_inner());
            if uuids.get(index).is_some_and(|uuid| uuid == index_uuid) {
                match remaining_uuid {
                    Some(uuid) => {
                        uuids.insert(index.to_string(), uuid);
                    }
                    None => {
                        uuids.remove(index);
                    }
                }
            }
        });
        drop(retired_engines);
        tracing::info!(
            index,
            uuid = index_uuid,
            reason,
            "Retired local index incarnation"
        );
        self.peer_recovery_targets
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key, target| {
                let uuid = match target {
                    PeerRecoveryTargetState::Recovering { index_uuid, .. } => index_uuid,
                    PeerRecoveryTargetState::FinalizedAwaitingMembership(pending) => {
                        &pending.index_uuid
                    }
                };
                key.index != index || uuid != index_uuid
            });
        self.copy_io_retries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key, _| key.index_uuid != index_uuid);
        self.copy_io_attempt_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key, _| key.index_uuid != index_uuid);
        if delete_data {
            let path = self.data_dir.join(index_uuid);
            if path.try_exists()? {
                tracing::warn!(
                    index,
                    uuid = index_uuid,
                    reason,
                    ?path,
                    "Removing shard data directory"
                );
                Self::remove_dir_all_with_retry(&path)?;
                tracing::info!(
                    index,
                    uuid = index_uuid,
                    reason,
                    ?path,
                    "Removed shard data directory"
                );
            }
        }
        Ok(())
    }

    async fn close_index_incarnation_blocking(
        self: &Arc<Self>,
        index: String,
        index_uuid: String,
        reason: &'static str,
        delete_data: bool,
        state: Option<ClusterState>,
    ) -> Result<()> {
        let mut lifecycle_locks = self
            .source_recovery_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|((uuid, _), _)| uuid == &index_uuid)
            .map(|((_, shard_id), lock)| (*shard_id, lock.clone()))
            .collect::<Vec<_>>();
        lifecycle_locks.sort_unstable_by_key(|(shard_id, _)| *shard_id);
        let mut source_guards = Vec::new();
        for (_, lock) in lifecycle_locks {
            let guard = match lock.clone().try_lock_owned() {
                Ok(guard) => guard,
                Err(_) => {
                    #[cfg(test)]
                    if let Some(sender) = self
                        .close_lifecycle_waiting_sender
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        let _ = sender.send(());
                    }
                    lock.lock_owned().await
                }
            };
            source_guards.push(guard);
        }
        self.abort_source_recoveries_for_index(&index_uuid).await?;
        let manager = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            manager.close_index_incarnation(
                &index,
                &index_uuid,
                reason,
                delete_data,
                state.as_ref(),
            )
        })
        .await
        .map_err(|error| anyhow::anyhow!("blocking index incarnation cleanup failed: {error}"))?;
        drop(source_guards);
        result
    }
}

#[cfg(test)]
#[path = "lifecycle/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lifecycle/review_tests.rs"]
mod review_tests;
