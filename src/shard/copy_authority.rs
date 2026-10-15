use super::*;
use crate::cluster::state::{
    CanonicalHistory, ClusterState, FailedCopyExclusion, PrimaryAuthorityPhase,
    WriteAuthorityIdentity,
};

impl ShardCopyIdentity {
    pub(super) fn advance_canonical_history(&mut self, history: &CanonicalHistory) -> Result<()> {
        history.validate().map_err(anyhow::Error::msg)?;
        if history.primary_term != self.replica_fence {
            anyhow::bail!("canonical history requires the exact durable primary fence");
        }
        if let Some(previous) = &self.canonical_history {
            if previous == history {
                return Ok(());
            }
            if history.primary_term <= previous.primary_term
                || history.activation_id <= previous.activation_id
                || history.max_seq_no < previous.max_seq_no
            {
                anyhow::bail!("canonical copy history cannot regress or change within an epoch");
            }
        }
        self.canonical_history = Some(history.clone());
        Ok(())
    }
}

impl ShardManager {
    pub(super) fn validate_replica_write_authority(
        &self,
        index: &str,
        shard_id: u32,
        identity: &ShardCopyIdentity,
        context: &ReplicaApplyContext<'_>,
    ) -> Result<()> {
        let Some(source) = context.authority else {
            if self.applied_authority.get().is_some() {
                anyhow::bail!("live replica apply requires exact current primary authority");
            }
            return Ok(());
        };
        source.validate().map_err(anyhow::Error::msg)?;
        if source.index_uuid != context.index_uuid
            || source.shard_id != shard_id
            || source.primary_term != context.message_term
            || identity.canonical_history.as_ref().is_none_or(|history| {
                history.primary_term != source.primary_term
                    || history.activation_id != source.activation_id
            })
        {
            anyhow::bail!("live replica apply does not match its durable canonical authority");
        }
        self.with_current_copy(
            &ShardKey::new(index, shard_id),
            context.index_uuid,
            Some(context.allocation_id),
            |state| {
                let Some(state) = state else {
                    return Ok(());
                };
                state
                    .validate_write_authority(index, source, PrimaryAuthorityPhase::Active)
                    .map_err(anyhow::Error::msg)?;
                let local = &self
                    .applied_authority
                    .get()
                    .expect("bound state")
                    .local_node_id;
                let routing = &state.indices[index].shard_routing[&shard_id];
                if state.primary_unavailable(index, shard_id)
                    || routing.primary == *local
                    || (!routing.is_replica_in_sync(local)
                        && !self.accepts_live_replication_while_pending(index, shard_id))
                    || state
                        .shard_allocation_ids(index, shard_id)
                        .and_then(|ids| ids.primary_authority.history.as_ref())
                        != identity.canonical_history.as_ref()
                {
                    anyhow::bail!("live replica apply authority or canonical certificate changed");
                }
                Ok(())
            },
        )
    }

    pub(super) fn update_copy_authority<T>(
        &self,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: AllocationId,
        update: impl FnOnce(&mut ShardCopyIdentity, &Arc<dyn SearchEngine>) -> Result<T>,
    ) -> Result<T> {
        let key = ShardKey::new(index, shard_id);
        let lifecycle = self.index_lifecycle_lock(index);
        let _lifecycle = lifecycle.read().unwrap_or_else(|error| error.into_inner());
        let lock = self.shard_open_lock(&key);
        let _guard = lock.lock().unwrap_or_else(|error| error.into_inner());
        self.with_current_copy(&key, index_uuid, Some(allocation_id), |_| Ok(()))?;
        let mut identity = self.validated_cached_copy_identity(&key, index_uuid, allocation_id)?;
        let engine = self
            .shards
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("shard engine is not open for authority publication"))?;
        engine.ensure_write_health()?;
        let previous = identity.clone();
        let result = update(&mut identity, &engine)?;
        if identity != previous {
            let shard_dir = self
                .data_dir
                .join(index_uuid)
                .join(format!("shard_{shard_id}"));
            if let Err(error) = Self::persist_copy_identity(&shard_dir, &mut identity) {
                let reason = format!("copy authority publication failed: {error:#}");
                engine.fail_stop_writes(&reason);
                return Err(error.context(crate::wal::WalFailStoppedError { reason }));
            }
            self.cache_copy_identity(&key, identity.clone());
        }
        let retention_history = self.with_applied_state(|state| match state {
            Some(state) => state
                .shard_allocation_ids(index, shard_id)
                .and_then(|ids| ids.primary_authority.history.clone()),
            None => identity.canonical_history.clone(),
        });
        if let Some(history) = &retention_history {
            engine
                .copy_write_state()
                .install_canonical_history(history)?;
        }
        if let Some(epoch) = engine.copy_write_state().primary_epoch()? {
            for pending in &identity.pending_exclusions {
                epoch.close_exclusion_admission(&pending.transition_id)?;
            }
        }
        if let Some(checkpoint) = identity.safe_global_checkpoint {
            engine.update_global_checkpoint(checkpoint);
        }
        Ok(result)
    }

    /// Persist the certificate after its canonical history has been verified and installed.
    pub fn persist_copy_canonical_history(
        &self,
        index: &str,
        allocation_id: AllocationId,
        authority: &WriteAuthorityIdentity,
        history: &CanonicalHistory,
    ) -> Result<()> {
        authority.validate().map_err(anyhow::Error::msg)?;
        history.validate().map_err(anyhow::Error::msg)?;
        if history.primary_term != authority.primary_term
            || history.activation_id != authority.activation_id
        {
            anyhow::bail!("canonical history does not belong to the proposed authority");
        }
        self.update_copy_authority(
            index,
            authority.shard_id,
            &authority.index_uuid,
            allocation_id,
            |identity, engine| {
                self.with_applied_state(|state| -> Result<()> {
                    if let Some(state) = state {
                        let phase = state
                            .shard_allocation_ids(index, authority.shard_id)
                            .ok_or_else(|| {
                                anyhow::anyhow!("canonical copy has no applied authority")
                            })?
                            .primary_authority
                            .phase;
                        if phase == PrimaryAuthorityPhase::Inactive {
                            anyhow::bail!("inactive primary cannot publish canonical history");
                        }
                        state
                            .validate_write_authority(index, authority, phase)
                            .map_err(anyhow::Error::msg)?;
                        if phase == PrimaryAuthorityPhase::Active
                            && state
                                .shard_allocation_ids(index, authority.shard_id)
                                .and_then(|ids| ids.primary_authority.history.as_ref())
                                != Some(history)
                        {
                            anyhow::bail!(
                                "copy certificate does not match committed ACTIVE history"
                            );
                        }
                    }
                    Ok(())
                })?;
                if identity.replica_fence != history.primary_term {
                    anyhow::bail!("canonical history requires the exact durable primary fence");
                }
                if history.max_seq_no > engine.sequence_stats().persisted_checkpoint {
                    anyhow::bail!("canonical history exceeds the verified gap-free durable prefix");
                }
                identity.advance_canonical_history(history)
            },
        )
    }

    pub(crate) fn persist_failed_copy_exclusion(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
    ) -> Result<()> {
        exclusion.validate().map_err(anyhow::Error::msg)?;
        let authority = &exclusion.authority;
        let state = self.copy_write_state_for_binding(CopyWriteBinding {
            index_uuid: authority.index_uuid.clone(),
            shard_id: authority.shard_id,
            allocation_id: authority.primary_allocation_id,
        })?;
        let epoch = state.primary_epoch()?;
        if let Some(epoch) = &epoch {
            if epoch.identity != *authority {
                anyhow::bail!("failed-copy exclusion belongs to a superseded primary epoch");
            }
            epoch.close_exclusion_admission(&exclusion.transition_id)?;
        } else if self.applied_authority.get().is_some() {
            anyhow::bail!("failed-copy exclusion has no active local primary epoch");
        }
        let result = self.update_copy_authority(
            index,
            authority.shard_id,
            &authority.index_uuid,
            authority.primary_allocation_id,
            |identity, _| {
                self.with_applied_state(|state| -> Result<()> {
                    if let Some(state) = state {
                        state
                            .validate_write_authority(
                                index,
                                authority,
                                PrimaryAuthorityPhase::Active,
                            )
                            .map_err(anyhow::Error::msg)?;
                        if state.version < exclusion.captured_state_version {
                            anyhow::bail!(
                                "exclusion journal is ahead of locally applied authority"
                            );
                        }
                    }
                    Ok(())
                })?;
                if identity.replica_fence != authority.primary_term
                    || !identity.canonical_history.as_ref().is_some_and(|history| {
                        history.primary_term == authority.primary_term
                            && history.activation_id == authority.activation_id
                    })
                {
                    anyhow::bail!(
                        "failed-copy exclusion requires the exact canonical copy authority"
                    );
                }
                if let Some(previous) = identity
                    .pending_exclusions
                    .iter()
                    .find(|pending| pending.transition_id == exclusion.transition_id)
                {
                    if previous != exclusion {
                        anyhow::bail!(
                            "failed-copy exclusion transition ID was reused with different content"
                        );
                    }
                } else {
                    identity.pending_exclusions.push(exclusion.clone());
                }
                Ok(())
            },
        );
        if let Err(error) = &result
            && let Some(epoch) = epoch
        {
            epoch.revoke(&format!(
                "failed-copy exclusion could not be durably journaled: {error:#}"
            ));
        }
        result
    }

    pub(crate) fn settle_copy_exclusions(
        &self,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: AllocationId,
        committed_transition_id: Option<&str>,
    ) -> Result<usize> {
        let state = self.copy_write_state_for_binding(CopyWriteBinding {
            index_uuid: index_uuid.to_string(),
            shard_id,
            allocation_id,
        })?;
        let settled = self.update_copy_authority(
            index,
            shard_id,
            index_uuid,
            allocation_id,
            |identity, _| {
                let mut settled = Vec::new();
                self.with_applied_state(|state| {
                    identity.pending_exclusions.retain(|pending| {
                        let definitive = committed_transition_id
                            == Some(pending.transition_id.as_str())
                            || state.is_some_and(|state| {
                                exclusion_is_definitively_settled(index, pending, state)
                            });
                        if definitive {
                            settled.push(pending.transition_id.clone());
                        }
                        !definitive
                    });
                });
                Ok(settled)
            },
        )?;
        if let Some(epoch) = state.primary_epoch()? {
            for transition_id in &settled {
                epoch.settle_exclusion(transition_id)?;
            }
        }
        Ok(settled.len())
    }

    pub(crate) fn persist_safe_global_checkpoint(
        &self,
        index: &str,
        allocation_id: AllocationId,
        authority: &WriteAuthorityIdentity,
        checkpoint: u64,
    ) -> Result<()> {
        authority.validate().map_err(anyhow::Error::msg)?;
        self.update_copy_authority(
            index,
            authority.shard_id,
            &authority.index_uuid,
            allocation_id,
            |identity, engine| {
                self.with_applied_state(|state| -> Result<()> {
                    if let Some(state) = state {
                        state
                            .validate_write_authority(
                                index,
                                authority,
                                PrimaryAuthorityPhase::Active,
                            )
                            .map_err(anyhow::Error::msg)?;
                    }
                    Ok(())
                })?;
                if identity.replica_fence != authority.primary_term
                    || !identity.canonical_history.as_ref().is_some_and(|history| {
                        history.primary_term == authority.primary_term
                            && history.activation_id == authority.activation_id
                    })
                {
                    anyhow::bail!("global progress requires the exact canonical copy authority");
                }
                if Some(checkpoint) > engine.sequence_stats().persisted_checkpoint {
                    anyhow::bail!("safe global checkpoint exceeds this copy's durable prefix");
                }
                if identity
                    .safe_global_checkpoint
                    .is_none_or(|previous| checkpoint > previous)
                {
                    identity.safe_global_checkpoint = Some(checkpoint);
                }
                Ok(())
            },
        )
    }
}

fn exclusion_is_definitively_settled(
    index: &str,
    pending: &FailedCopyExclusion,
    state: &ClusterState,
) -> bool {
    if state.version < pending.captured_state_version {
        return false;
    }
    let authority = &pending.authority;
    let Some(metadata) = state.indices.get(index) else {
        return true;
    };
    if metadata.uuid.as_str() != authority.index_uuid {
        return true;
    }
    let Some(routing) = metadata.shard_routing.get(&authority.shard_id) else {
        return true;
    };
    if routing.primary_term < authority.primary_term {
        return false;
    }
    if state
        .validate_write_authority(index, authority, PrimaryAuthorityPhase::Active)
        .is_err()
    {
        return true;
    }
    pending.failed_copies.iter().all(|copy| {
        !routing.in_sync_replicas.contains(&copy.node_id)
            || state.shard_allocation_id(index, authority.shard_id, &copy.node_id)
                != Some(copy.allocation_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{IndexMetadata, IndexUuid, ShardCopyAllocation};
    use crate::engine::write_authority::WriteAuthorityEpoch;
    use crate::engine::{DocumentMutation, SequencedOperation};
    use serde_json::json;

    fn authority() -> WriteAuthorityIdentity {
        WriteAuthorityIdentity {
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary_node_id: "primary".into(),
            primary_allocation_id: 7,
            primary_term: 3,
            activation_id: 9,
        }
    }

    fn history() -> CanonicalHistory {
        CanonicalHistory {
            activation_id: 9,
            primary_term: 3,
            max_seq_no: Some(0),
            digest: [1; 32],
        }
    }

    fn exclusion() -> FailedCopyExclusion {
        FailedCopyExclusion {
            authority: authority(),
            captured_state_version: 10,
            transition_id: uuid::Uuid::new_v4().to_string(),
            failed_copies: vec![ShardCopyAllocation {
                node_id: "replica".into(),
                allocation_id: 8,
            }],
        }
    }

    fn active_state() -> ClusterState {
        let mut metadata =
            IndexMetadata::build_shard_routing("idx", 1, 1, &["primary".into(), "replica".into()]);
        metadata.uuid = IndexUuid::new("uuid-1");
        let routing = metadata.shard_routing.get_mut(&0).unwrap();
        routing.primary_term = 3;
        routing.in_sync_replicas = vec!["replica".into()];
        let mut state = ClusterState::new("test".into());
        state.add_index_with_allocation_id(metadata, 7).unwrap();
        let ids = state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        ids.primary_initialized = true;
        ids.primary_authority.finish(history());
        ids.replicas.insert("replica".into(), 8);
        state.version = 10;
        state
    }

    fn open(manager: &ShardManager, create: bool) -> Arc<dyn SearchEngine> {
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 3,
                    allow_empty_creation: create,
                },
            )
            .unwrap()
    }

    fn populate(engine: &Arc<dyn SearchEngine>) {
        engine
            .apply_replica_operation(SequencedOperation {
                seq_no: 0,
                primary_term: 3,
                document_version: 1,
                client_operation: None,
                mutation: DocumentMutation::Index {
                    doc_id: "doc".into(),
                    source: json!({"value": 1}),
                },
            })
            .unwrap();
    }

    #[tokio::test]
    async fn copy_authority_journal_certificate_and_global_prefix_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let pending = exclusion();
        {
            let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
            let engine = open(&manager, true);
            populate(&engine);
            manager
                .persist_copy_canonical_history("idx", 7, &authority(), &history())
                .unwrap();
            let epoch = WriteAuthorityEpoch::new(authority(), &engine.copy_write_state()).unwrap();
            engine
                .copy_write_state()
                .install_primary_epoch(epoch.clone())
                .unwrap();
            let permit = epoch.permit().unwrap();
            manager
                .persist_failed_copy_exclusion("idx", &pending)
                .unwrap();
            manager
                .persist_failed_copy_exclusion("idx", &pending)
                .unwrap();
            assert!(permit.validate().is_err());
            manager
                .persist_safe_global_checkpoint("idx", 7, &authority(), 0)
                .unwrap();
            assert!(
                permit.validate().is_err(),
                "global publication cannot clear debt"
            );
            assert!(
                manager
                    .persist_safe_global_checkpoint("idx", 7, &authority(), 1)
                    .is_err()
            );
            assert_eq!(engine.global_checkpoint(), Some(0));
            assert_eq!(
                manager
                    .copy_identity("idx", 0)
                    .unwrap()
                    .pending_exclusions
                    .len(),
                1
            );
        }
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = open(&manager, false);
        let identity = manager.copy_identity("idx", 0).unwrap();
        assert_eq!(identity.pending_exclusions, vec![pending.clone()]);
        assert_eq!(identity.canonical_history, Some(history()));
        assert_eq!(identity.safe_global_checkpoint, Some(0));
        assert_eq!(engine.global_checkpoint(), Some(0));
        assert_eq!(
            manager
                .settle_copy_exclusions("idx", 0, "uuid-1", 7, Some("unknown"))
                .unwrap(),
            0
        );
        assert_eq!(
            manager
                .settle_copy_exclusions("idx", 0, "uuid-1", 7, Some(&pending.transition_id))
                .unwrap(),
            1
        );
        let durable = ShardManager::load_copy_identity(&dir.path().join("uuid-1/shard_0")).unwrap();
        assert!(durable.pending_exclusions.is_empty());
        assert_eq!(durable.canonical_history, Some(history()));
        assert_eq!(durable.safe_global_checkpoint, Some(0));
    }

    #[tokio::test]
    async fn copy_authority_rejects_certificate_replacement_without_poisoning_storage() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = open(&manager, true);
        populate(&engine);
        manager
            .persist_copy_canonical_history("idx", 7, &authority(), &history())
            .unwrap();
        let mut conflicting = history();
        conflicting.digest = [2; 32];
        assert!(
            manager
                .persist_copy_canonical_history("idx", 7, &authority(), &conflicting)
                .is_err()
        );
        assert!(engine.ensure_write_health().is_ok());
        assert_eq!(
            manager.copy_identity("idx", 0).unwrap().canonical_history,
            Some(history())
        );
        assert!(
            manager
                .persist_copy_canonical_history("idx", 8, &authority(), &history())
                .is_err()
        );
    }

    #[tokio::test]
    async fn copy_authority_publication_failure_is_first_error_fail_stop() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = open(&manager, true);
        populate(&engine);
        manager
            .persist_copy_canonical_history("idx", 7, &authority(), &history())
            .unwrap();
        let temporary = dir
            .path()
            .join("uuid-1/shard_0/SHARD_COPY_IDENTITY.json.tmp");
        std::fs::create_dir(&temporary).unwrap();
        let error = manager
            .persist_failed_copy_exclusion("idx", &exclusion())
            .unwrap_err();
        assert!(error.is::<crate::wal::WalFailStoppedError>(), "{error:#}");
        let first = engine.copy_write_state().failure_reason().unwrap();
        std::fs::remove_dir(&temporary).unwrap();
        assert!(manager.get_shard("idx", 0).is_none());
        assert!(
            manager
                .persist_safe_global_checkpoint("idx", 7, &authority(), 0)
                .is_err()
        );
        assert_eq!(
            engine.copy_write_state().failure_reason().as_deref(),
            Some(first.as_str())
        );
        let durable = ShardManager::load_copy_identity(&dir.path().join("uuid-1/shard_0")).unwrap();
        assert!(durable.pending_exclusions.is_empty());
        assert_eq!(durable.safe_global_checkpoint, None);
    }

    #[tokio::test]
    async fn copy_authority_prepared_certificate_cannot_advance_retention_before_raft_commit_or_restart()
     {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = open(&manager, true);
        populate(&engine);
        engine
            .apply_replica_operation(SequencedOperation {
                seq_no: 1,
                primary_term: 3,
                document_version: 1,
                client_operation: None,
                mutation: DocumentMutation::Index {
                    doc_id: "peer-only".into(),
                    source: json!({"value": 2}),
                },
            })
            .unwrap();
        manager
            .persist_copy_canonical_history("idx", 7, &authority(), &history())
            .unwrap();
        let mut state = active_state();
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
        let shared = Arc::new(RwLock::new(state));
        let cluster = Arc::new(crate::cluster::ClusterManager::with_shared_state(shared));
        manager.bind_applied_shard_authority(cluster.clone(), "primary".into());
        manager
            .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), 7, 4)
            .await
            .unwrap();
        let mut next_authority = authority();
        next_authority.primary_term = 4;
        next_authority.activation_id = 10;
        let prepared = CanonicalHistory {
            primary_term: 4,
            activation_id: 10,
            max_seq_no: Some(1),
            digest: [2; 32],
        };
        manager
            .persist_copy_canonical_history("idx", 7, &next_authority, &prepared)
            .unwrap();
        assert_eq!(
            engine.copy_write_state().canonical_history().unwrap(),
            Some(history())
        );
        engine.update_global_checkpoint(1);
        engine.flush().unwrap();
        let end = engine.peer_recovery_barrier().unwrap().wal_end;
        assert_eq!(
            engine
                .canonical_inventory(end, 1, 2)
                .unwrap()
                .operations
                .len(),
            1
        );
        drop(engine);
        drop(manager);

        let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
        restarted.bind_applied_shard_authority(cluster, "primary".into());
        let engine = restarted
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 4,
                    allow_empty_creation: false,
                },
            )
            .unwrap();
        assert_eq!(
            restarted.copy_identity("idx", 0).unwrap().canonical_history,
            Some(prepared)
        );
        assert_eq!(
            engine.copy_write_state().canonical_history().unwrap(),
            Some(history())
        );
        engine.flush().unwrap();
        let end = engine.peer_recovery_barrier().unwrap().wal_end;
        assert_eq!(
            engine
                .canonical_inventory(end, 1, 2)
                .unwrap()
                .operations
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn copy_authority_recovery_transfers_original_post_certificate_history() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_manager = ShardManager::new(source_dir.path(), Duration::from_secs(60));
        let source = open(&source_manager, true);
        populate(&source);
        source_manager
            .persist_copy_canonical_history("idx", 7, &authority(), &history())
            .unwrap();
        let operation = SequencedOperation {
            seq_no: 1,
            primary_term: 3,
            document_version: 1,
            client_operation: None,
            mutation: DocumentMutation::Index {
                doc_id: "original-peer-history".into(),
                source: json!({"value": 2}),
            },
        };
        source.apply_replica_operation(operation.clone()).unwrap();
        let snapshot_dir = source_dir.path().join("recovery");
        let snapshot = source.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let target = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let shard_dir = target
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 8)
            .await
            .unwrap();
        for file in &snapshot.files {
            let path = shard_dir.join("index").join(&file.name);
            std::fs::copy(snapshot_dir.join(&file.name), &path).unwrap();
            std::fs::File::open(path).unwrap().sync_all().unwrap();
        }
        let installed = target
            .finalize_peer_recovery_target_blocking(PeerRecoveryTargetInstall {
                index: "idx".into(),
                shard_id: 0,
                mappings: HashMap::new(),
                settings: IndexSettings::default(),
                index_uuid: "uuid-1".into(),
                allocation_id: 8,
                primary_term: 3,
                shard_dir,
                committed_boundary: snapshot.committed_boundary.clone(),
                expected_files: snapshot
                    .files
                    .iter()
                    .map(|file| file.name.clone())
                    .collect(),
            })
            .await
            .unwrap();
        assert_eq!(
            target.copy_identity("idx", 0).unwrap().canonical_history,
            Some(history())
        );
        let frozen = installed.freeze_canonical_history().unwrap();
        let inventory = installed.canonical_inventory(frozen.wal_end, 1, 2).unwrap();
        assert_eq!(inventory.operations.len(), 1);
        assert_eq!(
            installed
                .canonical_operation_at(inventory.operations[0].cursor, frozen.wal_end)
                .unwrap(),
            operation
        );
        assert_eq!(
            installed
                .get_document("original-peer-history")
                .unwrap()
                .unwrap()["value"],
            json!(2)
        );
        frozen.retention_pin.release().unwrap();
        source
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
    }

    #[tokio::test]
    async fn copy_authority_old_request_cannot_close_or_revoke_a_new_primary_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = open(&manager, true);
        populate(&engine);
        manager
            .persist_copy_canonical_history("idx", 7, &authority(), &history())
            .unwrap();
        let copy = engine.copy_write_state();
        let old = WriteAuthorityEpoch::new(authority(), &copy).unwrap();
        copy.install_primary_epoch(old.clone()).unwrap();
        let old_permit = old.permit().unwrap();
        manager
            .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), 7, 4)
            .await
            .unwrap();
        let mut next_authority = authority();
        next_authority.primary_term = 4;
        next_authority.activation_id = 10;
        let next_history = CanonicalHistory {
            primary_term: 4,
            activation_id: 10,
            max_seq_no: Some(0),
            digest: [2; 32],
        };
        manager
            .persist_copy_canonical_history("idx", 7, &next_authority, &next_history)
            .unwrap();
        let current = WriteAuthorityEpoch::new(next_authority, &copy).unwrap();
        copy.install_primary_epoch(current.clone()).unwrap();
        let permit = current.permit().unwrap();
        assert!(
            manager
                .persist_failed_copy_exclusion("idx", &exclusion())
                .is_err()
        );
        permit.validate().unwrap();
        assert!(!current.is_revoked());
        assert!(old_permit.validate().is_err());
        assert!(
            manager
                .copy_identity("idx", 0)
                .unwrap()
                .pending_exclusions
                .is_empty()
        );
    }

    #[test]
    fn copy_authority_current_format_requires_nullable_fields_and_checksum() {
        let original = ShardCopyIdentity::new("uuid-1", 7, 3, None).unwrap();
        for field in [
            "canonical_history",
            "fence_max_seq_no",
            "safe_global_checkpoint",
        ] {
            let mut encoded = serde_json::to_value(&original).unwrap();
            encoded.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<ShardCopyIdentity>(encoded).is_err(),
                "{field}"
            );
        }
        let mut corrupt = original;
        corrupt.safe_global_checkpoint = Some(9);
        assert!(corrupt.validate().is_err());
    }

    #[test]
    fn copy_authority_exclusion_requires_definitive_not_absent_or_stale_evidence() {
        let pending = exclusion();
        let mut state = active_state();
        assert!(!exclusion_is_definitively_settled("idx", &pending, &state));
        let mut old = state.clone();
        old.version = 9;
        old.indices.remove("idx");
        assert!(!exclusion_is_definitively_settled("idx", &pending, &old));
        let mut aba = state.clone();
        aba.shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .replicas
            .insert("replica".into(), 11);
        assert!(exclusion_is_definitively_settled("idx", &pending, &aba));
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
            .reserve(12);
        assert!(exclusion_is_definitively_settled("idx", &pending, &state));
    }
}
