use super::*;
use crate::cluster::state::{
    CanonicalHistory, ClusterState, PrimaryActivationFinish, PrimaryAuthorityPhase,
    ShardCopyAllocation, WriteAuthorityIdentity,
};
use crate::engine::canonical::{
    CanonicalHistoryAccumulator, HistoryInventory, HistoryOperationMetadata, MAX_HISTORY_WINDOW,
    operation_digest,
};
use crate::engine::write_authority::WriteAuthorityEpoch;
use crate::engine::{DocumentMutation, SequencedOperation};
use crate::shard::promotion::{
    FrozenCopyReport, HistoryView, PromotionCommand, PromotionReply, PromotionRequest,
};
use anyhow::{Context, bail};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(test)]
mod tests;

pub(super) struct PromotionJobBudget(Arc<Semaphore>);

impl Default for PromotionJobBudget {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(2)))
    }
}

impl PromotionJobBudget {
    pub(super) async fn acquire(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.0.clone().acquire_owned().await.map_err(|error| {
            Status::unavailable(format!(
                "canonical history work admission is closed: {error}"
            ))
        })
    }
}

struct PromotionPeer {
    node: crate::cluster::state::NodeInfo,
    report: FrozenCopyReport,
    eligible: bool,
}

#[derive(Default)]
struct PeerGapChoice {
    selected: Option<(usize, HistoryOperationMetadata)>,
    conflicting: bool,
}

impl TransportService {
    fn active_primary_from_epoch(
        &self,
        index: &str,
        shard: u32,
    ) -> anyhow::Result<Option<ActivatedPrimary>> {
        let Some(engine) = self.shard_manager.get_shard(index, shard) else {
            return Ok(None);
        };
        let Some(epoch) = engine.copy_write_state().primary_epoch()? else {
            return Ok(None);
        };
        let state = self.cluster_manager.get_state();
        if epoch.is_revoked() {
            return Ok(None);
        }
        if let Err(reason) =
            state.validate_write_authority(index, &epoch.identity, PrimaryAuthorityPhase::Active)
        {
            epoch.revoke(&reason);
            return Ok(None);
        }
        let identity = self
            .shard_manager
            .copy_identity(index, shard)
            .ok_or_else(|| anyhow::anyhow!("active primary has no durable copy identity"))?;
        if !identity.pending_exclusions.is_empty() {
            bail!("primary has unresolved durable failed-copy exclusions");
        }
        let expected_history = state
            .shard_allocation_ids(index, shard)
            .and_then(|ids| ids.primary_authority.history.as_ref());
        if identity.canonical_history.as_ref() != expected_history {
            epoch.revoke("durable primary certificate does not match committed ACTIVE history");
            bail!("durable primary certificate does not match committed ACTIVE history");
        }
        Ok(Some(ActivatedPrimary::from_permit(epoch.permit()?)))
    }

    pub(super) async fn ensure_canonical_primary_activated(
        &self,
        index: &str,
        shard: u32,
    ) -> Result<ActivatedPrimary, String> {
        self.canonical_primary_activation(index, shard)
            .await
            .map_err(|error| format!("{error:#}"))
    }

    async fn canonical_primary_activation(
        &self,
        index: &str,
        shard: u32,
    ) -> anyhow::Result<ActivatedPrimary> {
        self.shard_manager
            .bind_applied_shard_authority(self.cluster_manager.clone(), self.local_node_id.clone());
        self.primary_routing(index, shard)
            .map_err(anyhow::Error::msg)?;
        self.reconcile_pending_write_exclusions(index, shard)
            .await
            .map_err(anyhow::Error::msg)?;
        if let Some(active) = self.active_primary_from_epoch(index, shard)? {
            return Ok(active);
        }
        let assigned = self
            .primary_routing(index, shard)
            .map_err(anyhow::Error::msg)?;
        let key = (assigned.index_uuid.clone(), shard, assigned.allocation_id);
        let locks = self.primary_activation_state.copy_locks(&key);
        let _activation = locks.activation.lock().await;
        if let Some(active) = self.active_primary_from_epoch(index, shard)? {
            return Ok(active);
        }
        let _coordinator_admission = self
            .primary_activation_state
            .promotion_coordinators
            .acquire()
            .await?;
        if let Some(active) = self.active_primary_from_epoch(index, shard)? {
            return Ok(active);
        }
        let current = self
            .primary_routing(index, shard)
            .map_err(anyhow::Error::msg)?;
        if (current.index_uuid.clone(), shard, current.allocation_id) != key {
            bail!("primary allocation changed before canonical activation");
        }
        let engine = match self
            .shard_manager
            .open_primary_assigned_shard_with_settings_blocking(
                index.to_string(),
                shard,
                current.mappings,
                current.settings,
                current.index_uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id: current.allocation_id,
                    primary_term: current.primary_term,
                    allow_empty_creation: current.allow_empty_creation,
                },
            )
            .await
        {
            Ok(engine) => engine,
            Err(error) => {
                self.report_local_copy_failure(
                    index,
                    &current.index_uuid,
                    shard,
                    current.allocation_id,
                    current.primary_term,
                    &error,
                )
                .await;
                return Err(error.context("open protected primary for canonical activation"));
            }
        };
        let drain = engine.copy_write_state();
        tokio::task::spawn_blocking(move || {
            drain.revoke_and_drain("a fresh committed canonical activation is required")
        })
        .await
        .context("primary mutation drain worker failed")??;
        self.reserve_canonical_activation(
            index,
            shard,
            &current.index_uuid,
            current.allocation_id,
            current.primary_term,
        )
        .await?;
        let next_term = current
            .primary_term
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("primary term exhausted"))?;
        let (authority, promoting_state) = self
            .observe_reserved_activation(
                index,
                shard,
                &current.index_uuid,
                current.allocation_id,
                next_term,
            )
            .await?;
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::authority::record(
            "canonical_promoting",
            &authority,
            serde_json::json!({
                "state_version": promoting_state.version,
                "previous_history": promoting_state.shard_allocation_ids(index, shard)
                    .and_then(|ids| ids.primary_authority.history.as_ref()),
            }),
        );
        let _recovery_barrier = tokio::time::timeout(
            Duration::from_secs(20),
            self.peer_recovery_exclusive_guard(&authority.index_uuid, shard),
        )
        .await
        .context("prior peer recovery admission did not settle after the new committed term")?;
        let version = promoting_state.version;
        let local = self
            .freeze_promotion_copy(
                index,
                &authority,
                authority.primary_allocation_id,
                version,
                None,
            )
            .await?;
        let routing = &promoting_state.indices[index].shard_routing[&shard];
        let mut peers = Vec::new();
        for node_id in &routing.in_sync_replicas {
            let allocation_id = promoting_state
                .shard_allocation_id(index, shard, node_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("in-sync replica has no exact allocation identity")
                })?;
            let Some(node) = promoting_state.nodes.get(node_id) else {
                tracing::warn!(
                    index,
                    shard,
                    node_id,
                    allocation_id,
                    "Promotion omits unavailable in-sync copy whose node is absent; its history is not an empty report"
                );
                continue;
            };
            match self
                .freeze_promotion_copy(index, &authority, allocation_id, version, Some(node))
                .await
            {
                Ok(report) if report.committed_history == local.committed_history => {
                    peers.push(PromotionPeer {
                        node: node.clone(),
                        report,
                        eligible: true,
                    });
                }
                Ok(_) => tracing::warn!(
                    index,
                    shard,
                    node_id,
                    allocation_id,
                    "Promotion omits copy with a different committed history anchor"
                ),
                Err(error) => {
                    tracing::warn!(index, shard, node_id, allocation_id, error = %format!("{error:#}"), "Promotion omits unavailable frozen report, not an empty history")
                }
            }
        }
        let max_next = peers
            .iter()
            .map(|peer| peer.report.max_next)
            .chain([local.max_next])
            .max()
            .unwrap_or(0);
        let history = self
            .build_canonical_history(index, &local, &peers, version, max_next)
            .await?;
        self.promotion_seal(index, &local, version, &history, None)
            .await?;
        for peer in &mut peers {
            let result = self
                .resync_promotion_peer(index, &local, peer, version, &history)
                .await;
            if let Err(error) = result {
                peer.eligible = false;
                tracing::warn!(
                    index, shard, node_id = peer.node.id, allocation_id = peer.report.allocation_id,
                    error = %format!("{error:#}"), "Canonical promotion excludes a copy without a complete matching durable history"
                );
            }
        }
        let finish = PrimaryActivationFinish {
            authority: authority.clone(),
            history: history.clone(),
            eligible_replicas: peers
                .iter()
                .filter(|peer| peer.eligible)
                .map(|peer| ShardCopyAllocation {
                    node_id: peer.node.id.clone(),
                    allocation_id: peer.report.allocation_id,
                })
                .collect(),
        };
        self.finish_canonical_activation(index, &finish).await?;
        self.observe_active_canonical_history(index, &authority, &history)
            .await?;
        let manager = self.shard_manager.clone();
        let index_owned = index.to_string();
        let copied_authority = authority.clone();
        tokio::task::spawn_blocking(move || {
            manager.settle_copy_exclusions(
                &index_owned,
                copied_authority.shard_id,
                &copied_authority.index_uuid,
                copied_authority.primary_allocation_id,
                None,
            )
        })
        .await
        .context("obsolete exclusion settlement worker failed")??;
        self.promotion_call(
            None,
            promotion_request(index, &local, version, PromotionCommand::Release),
        )
        .await?;
        let epoch = WriteAuthorityEpoch::for_applied_authority(
            authority.clone(),
            &engine.copy_write_state(),
            index.to_string(),
            self.cluster_manager.clone(),
        )?;
        engine
            .copy_write_state()
            .install_primary_epoch(epoch.clone())?;
        let permit = epoch.permit()?;
        permit.validate()?;
        crate::metrics::CANONICAL_ACTIVATIONS_TOTAL.inc();
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::authority::record(
            "canonical_active",
            &authority,
            serde_json::json!({
                "history": history,
                "eligible_replicas": finish.eligible_replicas,
            }),
        );
        self.primary_activation_state
            .activated_terms
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key, authority.primary_term);
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::record_primary_activated(
            &crate::protocol_trace::TraceCopy {
                node: self.local_node_id.clone(),
                index_uuid: authority.index_uuid.clone(),
                shard,
                allocation: authority.primary_allocation_id,
            },
            authority.primary_term,
        );
        for peer in peers.iter().filter(|peer| peer.eligible) {
            if let Err(error) = self
                .promotion_call(
                    Some(&peer.node),
                    promotion_request(
                        index,
                        &peer.report,
                        self.cluster_manager.version(),
                        PromotionCommand::Release,
                    ),
                )
                .await
            {
                tracing::warn!(index, shard, node_id = peer.node.id, error = %format!("{error:#}"), "ACTIVE replica retains a promotion pin until local reconciliation releases it");
            }
        }
        Ok(ActivatedPrimary::from_permit(permit))
    }

    async fn reserve_canonical_activation(
        &self,
        index: &str,
        shard: u32,
        uuid: &str,
        allocation_id: u64,
        expected_term: u64,
    ) -> anyhow::Result<()> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("canonical activation requires Raft"))?;
        tokio::time::timeout(Duration::from_secs(5), async {
            if raft.is_leader() {
                raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
                    index_name: index.to_string(),
                    index_uuid: uuid.to_string(),
                    shard_id: shard,
                    primary: self.local_node_id.clone(),
                    allocation_id,
                    expected_term,
                })
                .await
                .context("reserve PROMOTING authority in Raft")?
                .data
                .into_result()
            } else {
                let leader = self.authority_leader()?;
                self.transport_client
                    .forward_activate_primary(
                        &leader,
                        ActivatePrimaryRequest {
                            index_name: index.to_string(),
                            index_uuid: uuid.to_string(),
                            shard_id: shard,
                            primary_node_id: self.local_node_id.clone(),
                            expected_term,
                            allocation_id: Some(allocation_id),
                        },
                    )
                    .await
            }
        })
        .await
        .context("PROMOTING reservation deadline expired")?
    }

    pub(super) fn authority_leader(&self) -> anyhow::Result<crate::cluster::state::NodeInfo> {
        let state = self.cluster_manager.get_state();
        state
            .master_node
            .as_ref()
            .and_then(|node| state.nodes.get(node))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("no applied Raft leader is available for shard authority mutation")
            })
    }

    async fn observe_reserved_activation(
        &self,
        index: &str,
        shard: u32,
        uuid: &str,
        allocation_id: u64,
        reserved_term: u64,
    ) -> anyhow::Result<(WriteAuthorityIdentity, ClusterState)> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.cluster_manager.get_state();
                let metadata = state
                    .indices
                    .get(index)
                    .ok_or_else(|| anyhow::anyhow!("reserved primary index disappeared"))?;
                let routing = metadata
                    .shard_routing
                    .get(&shard)
                    .ok_or_else(|| anyhow::anyhow!("reserved primary shard disappeared"))?;
                if metadata.uuid.as_str() != uuid
                    || routing.primary != self.local_node_id
                    || state.primary_allocation_id(index, shard) != Some(allocation_id)
                    || routing.primary_term > reserved_term
                {
                    bail!("canonical primary reservation was superseded before observation");
                }
                if routing.primary_term == reserved_term {
                    let ids = state.shard_allocation_ids(index, shard).ok_or_else(|| {
                        anyhow::anyhow!("reserved shard has no allocation authority")
                    })?;
                    let activation_id = ids.primary_authority.activation_id.ok_or_else(|| {
                        anyhow::anyhow!("reserved primary has no activation identity")
                    })?;
                    let identity = WriteAuthorityIdentity {
                        index_uuid: uuid.to_string(),
                        shard_id: shard,
                        primary_node_id: self.local_node_id.clone(),
                        primary_allocation_id: allocation_id,
                        primary_term: reserved_term,
                        activation_id,
                    };
                    state
                        .validate_write_authority(
                            index,
                            &identity,
                            PrimaryAuthorityPhase::Promoting,
                        )
                        .map_err(anyhow::Error::msg)?;
                    return Ok((identity, state));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("timed out observing exact committed PROMOTING authority")?
    }

    async fn finish_canonical_activation(
        &self,
        index: &str,
        finish: &PrimaryActivationFinish,
    ) -> anyhow::Result<()> {
        finish.validate().map_err(anyhow::Error::msg)?;
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("canonical activation requires Raft"))?;
        tokio::time::timeout(Duration::from_secs(5), async {
            if raft.is_leader() {
                raft.client_write(
                    crate::consensus::types::ClusterCommand::FinishPrimaryActivation {
                        index_name: index.to_string(),
                        activation: finish.clone(),
                    },
                )
                .await
                .context("commit canonical ACTIVE authority in Raft")?
                .data
                .into_result()
            } else {
                self.transport_client
                    .forward_finish_primary_activation(&self.authority_leader()?, index, finish)
                    .await
            }
        })
        .await
        .context("canonical ACTIVE commit deadline expired")?
    }

    async fn observe_active_canonical_history(
        &self,
        index: &str,
        authority: &WriteAuthorityIdentity,
        history: &CanonicalHistory,
    ) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.cluster_manager.get_state();
                let assigned = self
                    .primary_routing(index, authority.shard_id)
                    .map_err(anyhow::Error::msg)?;
                if assigned.index_uuid != authority.index_uuid
                    || assigned.allocation_id != authority.primary_allocation_id
                    || assigned.primary_term != authority.primary_term
                {
                    bail!("canonical primary lost its exact reservation while waiting for ACTIVE");
                }
                if state
                    .validate_write_authority(index, authority, PrimaryAuthorityPhase::Active)
                    .is_ok()
                {
                    if state
                        .shard_allocation_ids(index, authority.shard_id)
                        .and_then(|ids| ids.primary_authority.history.as_ref())
                        != Some(history)
                    {
                        bail!(
                            "committed ACTIVE certificate differs from the sealed canonical history"
                        );
                    }
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("timed out observing exact canonical ACTIVE authority")?
    }

    async fn promotion_call(
        &self,
        node: Option<&crate::cluster::state::NodeInfo>,
        request: PromotionRequest,
    ) -> anyhow::Result<PromotionReply> {
        match node {
            Some(node) => {
                self.transport_client
                    .reconcile_primary_history(node, &request)
                    .await
            }
            None => tokio::time::timeout(
                Duration::from_secs(20),
                self.execute_promotion_history(request),
            )
            .await
            .context("local canonical history command deadline expired")?
            .map_err(anyhow::Error::new),
        }
    }

    async fn freeze_promotion_copy(
        &self,
        index: &str,
        authority: &WriteAuthorityIdentity,
        target_allocation_id: u64,
        version: u64,
        node: Option<&crate::cluster::state::NodeInfo>,
    ) -> anyhow::Result<FrozenCopyReport> {
        let reply = self
            .promotion_call(
                node,
                PromotionRequest {
                    index: index.to_string(),
                    authority: authority.clone(),
                    target_allocation_id,
                    sender_state_version: version,
                    token: None,
                    command: PromotionCommand::Freeze,
                },
            )
            .await?;
        let PromotionReply::Frozen(report) = reply else {
            bail!("history freeze returned an inconsistent reply kind");
        };
        report.validate()?;
        if report.authority != *authority || report.allocation_id != target_allocation_id {
            bail!("history freeze report does not bind the exact requested authority and target");
        }
        Ok(report)
    }

    async fn promotion_inventory(
        &self,
        index: &str,
        report: &FrozenCopyReport,
        version: u64,
        window: std::ops::Range<u64>,
        view: HistoryView,
        node: Option<&crate::cluster::state::NodeInfo>,
    ) -> anyhow::Result<HistoryInventory> {
        let reply = self
            .promotion_call(
                node,
                promotion_request(
                    index,
                    report,
                    version,
                    PromotionCommand::Inventory {
                        start_seq_no: window.start,
                        end_seq_no: window.end,
                        view,
                    },
                ),
            )
            .await?;
        let PromotionReply::Inventory(inventory) = reply else {
            bail!("history inventory returned an inconsistent reply kind");
        };
        inventory.validate()?;
        if inventory.start_seq_no != window.start || inventory.end_seq_no != window.end {
            bail!("history inventory returned a different logical window");
        }
        Ok(inventory)
    }

    async fn promotion_read(
        &self,
        index: &str,
        report: &FrozenCopyReport,
        version: u64,
        metadata: &HistoryOperationMetadata,
        view: HistoryView,
        node: Option<&crate::cluster::state::NodeInfo>,
    ) -> anyhow::Result<SequencedOperation> {
        let reply = self
            .promotion_call(
                node,
                promotion_request(
                    index,
                    report,
                    version,
                    PromotionCommand::ReadOperation {
                        cursor: metadata.cursor,
                        view,
                    },
                ),
            )
            .await?;
        let PromotionReply::Operation(operation) = reply else {
            bail!("history read returned an inconsistent reply kind");
        };
        if operation.seq_no != metadata.seq_no
            || operation.primary_term != metadata.primary_term
            || operation_digest(&operation)? != metadata.digest
        {
            bail!("history operation does not match its frozen inventory identity");
        }
        Ok(operation)
    }

    async fn promotion_append(
        &self,
        index: &str,
        report: &FrozenCopyReport,
        version: u64,
        operation: SequencedOperation,
        node: Option<&crate::cluster::state::NodeInfo>,
    ) -> anyhow::Result<()> {
        let next = operation
            .seq_no
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canonical operation sequence overflow"))?;
        let reply = self
            .promotion_call(
                node,
                promotion_request(
                    index,
                    report,
                    version,
                    PromotionCommand::Append {
                        operations: vec![operation],
                    },
                ),
            )
            .await?;
        match reply {
            PromotionReply::Appended {
                processed_next,
                persisted_next,
                max_next,
            } if processed_next == persisted_next
                && processed_next <= max_next
                && max_next >= next =>
            {
                Ok(())
            }
            _ => bail!("canonical append returned no exact durable operation proof"),
        }
    }

    async fn promotion_seal(
        &self,
        index: &str,
        report: &FrozenCopyReport,
        version: u64,
        history: &CanonicalHistory,
        node: Option<&crate::cluster::state::NodeInfo>,
    ) -> anyhow::Result<()> {
        let reply = self
            .promotion_call(
                node,
                promotion_request(
                    index,
                    report,
                    version,
                    PromotionCommand::Seal {
                        history: history.clone(),
                    },
                ),
            )
            .await?;
        match reply {
            PromotionReply::Sealed(actual) if actual == *history => Ok(()),
            _ => bail!("copy did not seal the exact canonical certificate"),
        }
    }

    async fn build_canonical_history(
        &self,
        index: &str,
        local: &FrozenCopyReport,
        peers: &[PromotionPeer],
        version: u64,
        max_next: u64,
    ) -> anyhow::Result<CanonicalHistory> {
        let mut digest =
            CanonicalHistoryAccumulator::new(&local.authority, local.committed_history.as_ref())?;
        while digest.next_seq_no() < max_next {
            let start = digest.next_seq_no();
            let end = start.saturating_add(MAX_HISTORY_WINDOW).min(max_next);
            let own = self
                .promotion_inventory(index, local, version, start..end, HistoryView::Frozen, None)
                .await?;
            let own = own
                .operations
                .into_iter()
                .map(|operation| (operation.seq_no, operation))
                .collect::<BTreeMap<_, _>>();
            let mut gaps = BTreeMap::<u64, PeerGapChoice>::new();
            for (peer_index, peer) in peers.iter().enumerate() {
                let inventory = self
                    .promotion_inventory(
                        index,
                        &peer.report,
                        version,
                        start..end,
                        HistoryView::Frozen,
                        Some(&peer.node),
                    )
                    .await?;
                for operation in inventory.operations {
                    if own.contains_key(&operation.seq_no) {
                        continue;
                    }
                    let choice = gaps.entry(operation.seq_no).or_default();
                    match &choice.selected {
                        Some((_, previous))
                            if previous.primary_term != operation.primary_term
                                || previous.digest != operation.digest =>
                        {
                            choice.conflicting = true
                        }
                        None => choice.selected = Some((peer_index, operation)),
                        _ => {}
                    }
                }
            }
            for seq_no in start..end {
                let metadata = if let Some(metadata) = own.get(&seq_no) {
                    metadata.clone()
                } else {
                    let choice = gaps.remove(&seq_no).unwrap_or_default();
                    let operation = match choice.selected.filter(|_| !choice.conflicting) {
                        Some((source, metadata)) => {
                            let peer = &peers[source];
                            self.promotion_read(
                                index,
                                &peer.report,
                                version,
                                &metadata,
                                HistoryView::Frozen,
                                Some(&peer.node),
                            )
                            .await?
                        }
                        None => SequencedOperation {
                            seq_no,
                            primary_term: local.authority.primary_term,
                            document_version: 0,
                            client_operation: None,
                            mutation: DocumentMutation::NoOp {
                                reason: "canonical promotion gap".to_string(),
                            },
                        },
                    };
                    let metadata = HistoryOperationMetadata {
                        seq_no,
                        primary_term: operation.primary_term,
                        digest: operation_digest(&operation)?,
                        cursor: local.wal_end,
                    };
                    self.promotion_append(index, local, version, operation, None)
                        .await?;
                    metadata
                };
                digest.record(&metadata)?;
            }
        }
        digest.finish(max_next.checked_sub(1))
    }

    async fn resync_promotion_peer(
        &self,
        index: &str,
        local: &FrozenCopyReport,
        peer: &PromotionPeer,
        version: u64,
        history: &CanonicalHistory,
    ) -> anyhow::Result<()> {
        let end = crate::engine::canonical::exclusive_next(history.max_seq_no)?;
        let base = crate::engine::canonical::exclusive_next(
            local
                .committed_history
                .as_ref()
                .and_then(|history| history.max_seq_no),
        )?;
        let mut start = base;
        while start < end {
            let window_end = start.saturating_add(MAX_HISTORY_WINDOW).min(end);
            let canonical = self
                .promotion_inventory(
                    index,
                    local,
                    version,
                    start..window_end,
                    HistoryView::Current,
                    None,
                )
                .await?;
            let canonical = canonical
                .operations
                .into_iter()
                .map(|operation| (operation.seq_no, operation))
                .collect::<BTreeMap<_, _>>();
            let existing = self
                .promotion_inventory(
                    index,
                    &peer.report,
                    version,
                    start..window_end,
                    HistoryView::Frozen,
                    Some(&peer.node),
                )
                .await?;
            for operation in &existing.operations {
                if !canonical.get(&operation.seq_no).is_some_and(|expected| {
                    expected.primary_term == operation.primary_term
                        && expected.digest == operation.digest
                }) {
                    bail!(
                        "peer retains a conflicting operation at sequence {}; eligible history is never rewritten",
                        operation.seq_no
                    );
                }
            }
            start = window_end;
        }
        start = base;
        while start < end {
            let window_end = start.saturating_add(MAX_HISTORY_WINDOW).min(end);
            let canonical = self
                .promotion_inventory(
                    index,
                    local,
                    version,
                    start..window_end,
                    HistoryView::Current,
                    None,
                )
                .await?;
            let existing = self
                .promotion_inventory(
                    index,
                    &peer.report,
                    version,
                    start..window_end,
                    HistoryView::Frozen,
                    Some(&peer.node),
                )
                .await?;
            let existing = existing
                .operations
                .into_iter()
                .map(|operation| (operation.seq_no, operation))
                .collect::<BTreeMap<_, _>>();
            for metadata in &canonical.operations {
                if !existing.contains_key(&metadata.seq_no) {
                    let operation = self
                        .promotion_read(index, local, version, metadata, HistoryView::Current, None)
                        .await?;
                    self.promotion_append(
                        index,
                        &peer.report,
                        version,
                        operation,
                        Some(&peer.node),
                    )
                    .await?;
                }
            }
            start = window_end;
        }
        self.promotion_seal(index, &peer.report, version, history, Some(&peer.node))
            .await
    }

    pub(super) async fn execute_promotion_history(
        &self,
        request: PromotionRequest,
    ) -> Result<PromotionReply, Status> {
        let admission = self
            .primary_activation_state
            .promotion_jobs
            .acquire()
            .await?;
        self.execute_promotion_history_with_budget(request, admission)
            .await
    }

    pub(super) async fn execute_promotion_history_with_budget(
        &self,
        request: PromotionRequest,
        admission: OwnedSemaphorePermit,
    ) -> Result<PromotionReply, Status> {
        request
            .validate()
            .map_err(|error| Status::invalid_argument(format!("{error:#}")))?;
        if self
            .shard_manager
            .validate_promotion_authority(&request)
            .is_err()
            && self.cluster_manager.version() < request.sender_state_version
        {
            self.cluster_manager
                .wait_for_version(request.sender_state_version)
                .await
                .map_err(|error| {
                    Status::unavailable(format!("promotion authority has not applied: {error}"))
                })?;
        }
        self.shard_manager
            .validate_promotion_authority(&request)
            .map_err(|error| Status::failed_precondition(format!("{error:#}")))?;
        if matches!(request.command, PromotionCommand::Freeze)
            && self
                .shard_manager
                .get_shard(&request.index, request.authority.shard_id)
                .is_none()
        {
            let assigned = self
                .assigned_local_shard(&request.index, request.authority.shard_id)
                .map_err(Status::failed_precondition)?;
            self.shard_manager
                .open_assigned_shard_with_settings_blocking(
                    request.index.clone(),
                    request.authority.shard_id,
                    assigned.mappings,
                    assigned.settings,
                    assigned.index_uuid,
                    crate::shard::AssignedShardOpen {
                        allocation_id: assigned.allocation_id,
                        primary_term: assigned.primary_term,
                        allow_empty_creation: false,
                    },
                )
                .await
                .map_err(|error| {
                    Status::failed_precondition(format!("open protected promotion copy: {error:#}"))
                })?;
        }
        let manager = self.shard_manager.clone();
        tokio::task::spawn_blocking(move || {
            let _admission = admission;
            manager.reconcile_primary_history(request)
        })
        .await
        .map_err(|error| Status::internal(format!("promotion worker failed: {error}")))?
        .map_err(|error| {
            if error.is::<crate::wal::WalFailStoppedError>() {
                Status::data_loss(format!("promotion copy is fail-stopped: {error:#}"))
            } else {
                Status::failed_precondition(format!("promotion history rejected: {error:#}"))
            }
        })
    }
}

fn promotion_request(
    index: &str,
    report: &FrozenCopyReport,
    version: u64,
    command: PromotionCommand,
) -> PromotionRequest {
    PromotionRequest {
        index: index.to_string(),
        authority: report.authority.clone(),
        target_allocation_id: report.allocation_id,
        sender_state_version: version,
        token: Some(report.token.clone()),
        command,
    }
}
