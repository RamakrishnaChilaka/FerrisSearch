use super::*;
use crate::cluster::state::{CanonicalHistory, PrimaryAuthorityPhase, WriteAuthorityIdentity};
use crate::engine::canonical::{
    CanonicalHistoryAccumulator, HistoryInventory, MAX_HISTORY_WINDOW, exclusive_next,
};
use crate::engine::{PeerRecoveryRetentionPin, SequencedOperation};
use crate::wal::WalCursor;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrozenCopyReport {
    pub authority: WriteAuthorityIdentity,
    pub allocation_id: AllocationId,
    pub token: String,
    pub wal_end: WalCursor,
    pub processed_next: u64,
    pub persisted_next: u64,
    pub max_next: u64,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub committed_history: Option<CanonicalHistory>,
}

impl FrozenCopyReport {
    pub(crate) fn validate(&self) -> Result<()> {
        self.authority.validate().map_err(anyhow::Error::msg)?;
        if self.allocation_id == 0
            || !uuid::Uuid::parse_str(&self.token).is_ok_and(|id| id.to_string() == self.token)
            || self.persisted_next != self.processed_next
            || self.processed_next > self.max_next
        {
            anyhow::bail!("frozen history report has invalid identity or sequence watermarks");
        }
        if let Some(history) = &self.committed_history {
            history.validate().map_err(anyhow::Error::msg)?;
            if history.primary_term >= self.authority.primary_term
                || history.activation_id >= self.authority.activation_id
                || exclusive_next(history.max_seq_no)? > self.persisted_next
            {
                anyhow::bail!(
                    "frozen copy does not cover the previous committed canonical history"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryView {
    Frozen,
    Current,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub(crate) enum PromotionCommand {
    Freeze,
    Inventory {
        start_seq_no: u64,
        end_seq_no: u64,
        view: HistoryView,
    },
    ReadOperation {
        cursor: WalCursor,
        view: HistoryView,
    },
    Append {
        operations: Vec<SequencedOperation>,
    },
    Seal {
        history: CanonicalHistory,
    },
    Release,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PromotionRequest {
    pub index: String,
    pub authority: WriteAuthorityIdentity,
    pub target_allocation_id: AllocationId,
    pub sender_state_version: u64,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub token: Option<String>,
    pub command: PromotionCommand,
}

impl PromotionRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        self.authority.validate().map_err(anyhow::Error::msg)?;
        if self.index.is_empty() || self.target_allocation_id == 0 || self.sender_state_version == 0
        {
            anyhow::bail!(
                "promotion request requires exact index, target allocation, and applied version"
            );
        }
        if matches!(self.command, PromotionCommand::Freeze) {
            if self.token.is_some() {
                anyhow::bail!("initial history freeze cannot supply a previous process token");
            }
        } else if !self.token.as_ref().is_some_and(|token| {
            uuid::Uuid::parse_str(token).is_ok_and(|id| id.to_string() == *token)
        }) {
            anyhow::bail!("promotion command requires the current process's exact history token");
        }
        if let PromotionCommand::Append { operations } = &self.command {
            crate::engine::canonical::validate_batch(operations)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    tag = "kind",
    content = "data",
    deny_unknown_fields
)]
pub(crate) enum PromotionReply {
    Frozen(FrozenCopyReport),
    Inventory(HistoryInventory),
    Operation(SequencedOperation),
    Appended {
        processed_next: u64,
        persisted_next: u64,
        max_next: u64,
    },
    Sealed(CanonicalHistory),
    Released,
}

pub(super) struct FrozenCopyHistory {
    index: String,
    report: FrozenCopyReport,
    engine: Arc<dyn SearchEngine>,
    _retention_pin: PeerRecoveryRetentionPin,
    sealed: OnceLock<CanonicalHistory>,
}

enum FrozenPinAction {
    Keep,
    Retire(Option<CanonicalHistory>),
}

impl ShardManager {
    pub(crate) fn validate_promotion_authority(&self, request: &PromotionRequest) -> Result<()> {
        request.validate()?;
        let authority = &request.authority;
        self.with_current_copy(
            &ShardKey::new(&request.index, authority.shard_id),
            &authority.index_uuid,
            Some(request.target_allocation_id),
            |state| {
                let Some(state) = state else {
                    return Ok(());
                };
                let phase = state
                    .shard_allocation_ids(&request.index, authority.shard_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("promotion target has no applied shard authority")
                    })?
                    .primary_authority
                    .phase;
                if phase != PrimaryAuthorityPhase::Promoting
                    && !(matches!(request.command, PromotionCommand::Release)
                        && phase == PrimaryAuthorityPhase::Active)
                {
                    anyhow::bail!("history reconciliation requires committed PROMOTING authority");
                }
                state
                    .validate_write_authority(&request.index, authority, phase)
                    .map_err(anyhow::Error::msg)?;
                let local_node = &self
                    .applied_authority
                    .get()
                    .expect("bound state")
                    .local_node_id;
                let routing = &state.indices[&request.index].shard_routing[&authority.shard_id];
                if routing.primary != *local_node && !routing.in_sync_replicas.contains(local_node)
                {
                    anyhow::bail!("promotion target is not an authoritative in-sync copy");
                }
                Ok(())
            },
        )
    }

    fn frozen_copy(&self, request: &PromotionRequest) -> Result<Arc<FrozenCopyHistory>> {
        let binding = CopyWriteBinding {
            index_uuid: request.authority.index_uuid.clone(),
            shard_id: request.authority.shard_id,
            allocation_id: request.target_allocation_id,
        };
        let frozen = self
            .frozen_histories
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&binding)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("frozen history process token is no longer present"))?;
        if frozen.report.authority != request.authority
            || request.token.as_ref() != Some(&frozen.report.token)
        {
            anyhow::bail!("frozen history token does not match the exact promotion authority");
        }
        frozen.engine.ensure_write_health()?;
        Ok(frozen)
    }

    fn validate_frozen_session(
        &self,
        request: &PromotionRequest,
        frozen: &Arc<FrozenCopyHistory>,
    ) -> Result<()> {
        self.validate_promotion_authority(request)?;
        let current = self.frozen_copy(request)?;
        if !Arc::ptr_eq(&current, frozen) {
            anyhow::bail!("promotion process token was superseded while its command waited");
        }
        Ok(())
    }

    fn freeze_copy_history(&self, request: &PromotionRequest) -> Result<FrozenCopyReport> {
        let authority = &request.authority;
        let binding = CopyWriteBinding {
            index_uuid: authority.index_uuid.clone(),
            shard_id: authority.shard_id,
            allocation_id: request.target_allocation_id,
        };
        let current = self
            .frozen_histories
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&binding)
            .cloned();
        if let Some(current) = current
            && current.report.authority == *authority
        {
            self.validate_promotion_authority(request)?;
            current.engine.ensure_write_health()?;
            return Ok(current.report.clone());
        }
        let (engine, fence_max_seq_no) = self.update_copy_authority(
            &request.index,
            authority.shard_id,
            &authority.index_uuid,
            request.target_allocation_id,
            |identity, engine| {
                self.validate_promotion_authority(request)?;
                engine
                    .copy_write_state()
                    .revoke_and_drain("committed primary promotion froze this copy")?;
                if identity.replica_fence > authority.primary_term {
                    anyhow::bail!(
                        "local durable fence is ahead of the requested promotion authority"
                    );
                }
                if identity.replica_fence < authority.primary_term {
                    identity.replica_fence = authority.primary_term;
                    identity.fence_max_seq_no = engine
                        .sequence_stats()
                        .max_seq_no
                        .into_iter()
                        .chain(engine.wal_max_seq_no())
                        .max();
                }
                Ok((engine.clone(), identity.fence_max_seq_no))
            },
        )?;
        engine.reconcile_term_sequence_state(authority.primary_term, fence_max_seq_no)?;
        self.update_copy_authority(
            &request.index,
            authority.shard_id,
            &authority.index_uuid,
            request.target_allocation_id,
            |identity, engine| {
                self.validate_promotion_authority(request)?;
                if identity.replica_fence != authority.primary_term {
                    anyhow::bail!("copy fence changed while its history freeze was waiting");
                }
                let committed_history =
                    self.with_applied_state(|state| -> Result<Option<CanonicalHistory>> {
                        match state {
                            Some(state) => Ok(state
                                .shard_allocation_ids(&request.index, authority.shard_id)
                                .and_then(|ids| ids.primary_authority.history.clone())),
                            None => engine.copy_write_state().canonical_history(),
                        }
                    })?;
                if let Some(anchor) = &committed_history
                    && !identity.canonical_history.as_ref().is_some_and(|local| {
                        local == anchor
                            || (local.primary_term > anchor.primary_term
                                && local.activation_id > anchor.activation_id
                                && local.max_seq_no >= anchor.max_seq_no)
                    })
                {
                    anyhow::bail!("copy is missing its last committed canonical certificate");
                }
                let frozen = engine.freeze_canonical_history()?;
                let report = FrozenCopyReport {
                    authority: authority.clone(),
                    allocation_id: request.target_allocation_id,
                    token: uuid::Uuid::new_v4().to_string(),
                    wal_end: frozen.wal_end,
                    processed_next: frozen.processed_next,
                    persisted_next: frozen.persisted_next,
                    max_next: frozen.max_next,
                    committed_history,
                };
                report.validate()?;
                let previous = self
                    .frozen_histories
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(
                        binding.clone(),
                        Arc::new(FrozenCopyHistory {
                            index: request.index.clone(),
                            report: report.clone(),
                            engine: engine.clone(),
                            _retention_pin: frozen.retention_pin,
                            sealed: OnceLock::new(),
                        }),
                    );
                drop(previous);
                Ok(report)
            },
        )
    }

    pub(crate) fn reconcile_primary_history(
        &self,
        request: PromotionRequest,
    ) -> Result<PromotionReply> {
        self.validate_promotion_authority(&request)?;
        if matches!(request.command, PromotionCommand::Freeze) {
            let report = self.freeze_copy_history(&request)?;
            #[cfg(feature = "protocol-trace")]
            if let Some(applied) = self.applied_authority.get() {
                crate::protocol_trace::authority::record(
                    "canonical_frozen",
                    &request.authority,
                    serde_json::json!({
                        "copy": {
                            "node_id": applied.local_node_id,
                            "allocation_id": request.target_allocation_id,
                        },
                        "fence_term": request.authority.primary_term,
                        "persisted_next": report.persisted_next,
                        "max_next": report.max_next,
                    }),
                );
            }
            return Ok(PromotionReply::Frozen(report));
        }
        if matches!(request.command, PromotionCommand::Release) {
            return self.release_promotion_history(&request);
        }
        let frozen = self.frozen_copy(&request)?;
        let reply = self.update_copy_authority(
            &request.index, request.authority.shard_id, &request.authority.index_uuid,
            request.target_allocation_id, |identity, engine| {
                self.validate_frozen_session(&request, &frozen)?;
                if identity.replica_fence != request.authority.primary_term {
                    anyhow::bail!("promotion command does not match the durable target fence");
                }
                let end_for = |view| -> Result<WalCursor> {
                    match view {
                        HistoryView::Frozen => Ok(frozen.report.wal_end),
                        HistoryView::Current => Ok(engine.peer_recovery_barrier()?.wal_end),
                    }
                };
                match &request.command {
                    PromotionCommand::Inventory { start_seq_no, end_seq_no, view } => {
                        engine.canonical_inventory(end_for(*view)?, *start_seq_no, *end_seq_no)
                            .map(PromotionReply::Inventory)
                    }
                    PromotionCommand::ReadOperation { cursor, view } => {
                        engine.canonical_operation_at(*cursor, end_for(*view)?).map(PromotionReply::Operation)
                    }
                    PromotionCommand::Append { operations } => {
                        if frozen.sealed.get().is_some() {
                            anyhow::bail!("sealed promotion history cannot accept more appends");
                        }
                        let receipt = engine.append_canonical_operations(operations)?;
                        if !receipt.all_operations_processed || !receipt.all_operations_persisted {
                            anyhow::bail!("canonical resynchronization did not prove every operation durable");
                        }
                        Ok(PromotionReply::Appended {
                            processed_next: exclusive_next(receipt.sequence.processed_checkpoint)?,
                            persisted_next: exclusive_next(receipt.sequence.persisted_checkpoint)?,
                            max_next: exclusive_next(receipt.sequence.max_seq_no)?,
                        })
                    }
                    PromotionCommand::Seal { history } => {
                        history.validate().map_err(anyhow::Error::msg)?;
                        let barrier = engine.peer_recovery_barrier()?;
                        let end = exclusive_next(history.max_seq_no)?;
                        if exclusive_next(barrier.sequence.persisted_checkpoint)? != end
                            || barrier.sequence.max_seq_no != history.max_seq_no
                        {
                            anyhow::bail!("copy does not cover the exact gap-free canonical head");
                        }
                        let mut digest = CanonicalHistoryAccumulator::new(
                            &request.authority, frozen.report.committed_history.as_ref(),
                        )?;
                        while digest.next_seq_no() < end {
                            let start = digest.next_seq_no();
                            let window_end = start.saturating_add(MAX_HISTORY_WINDOW).min(end);
                            let inventory = engine.canonical_inventory(barrier.wal_end, start, window_end)?;
                            for operation in &inventory.operations { digest.record(operation)?; }
                            if digest.next_seq_no() != window_end {
                                anyhow::bail!("copy is missing retained operations required for the canonical digest");
                            }
                        }

                        if digest.finish(history.max_seq_no)? != *history {
                            anyhow::bail!("copy's canonical history digest does not match the candidate");
                        }
                        if let Some(previous) = frozen.sealed.get() {
                            if previous != history { anyhow::bail!("promotion history was sealed with another certificate"); }
                        } else if frozen.sealed.set(history.clone()).is_err() {
                            anyhow::bail!("promotion history seal changed concurrently");
                        }
                        identity.advance_canonical_history(history)?;
                        Ok(PromotionReply::Sealed(history.clone()))
                    }
                    PromotionCommand::Freeze | PromotionCommand::Release => unreachable!(),
                }
            },
        )?;
        #[cfg(feature = "protocol-trace")]
        if let PromotionReply::Sealed(history) = &reply
            && let Some(applied) = self.applied_authority.get()
        {
            crate::protocol_trace::authority::record(
                "canonical_sealed",
                &request.authority,
                serde_json::json!({
                    "copy": {
                        "node_id": applied.local_node_id,
                        "allocation_id": request.target_allocation_id,
                    },
                    "history": history,
                }),
            );
        }
        Ok(reply)
    }

    fn release_promotion_history(&self, request: &PromotionRequest) -> Result<PromotionReply> {
        let binding = CopyWriteBinding {
            index_uuid: request.authority.index_uuid.clone(),
            shard_id: request.authority.shard_id,
            allocation_id: request.target_allocation_id,
        };
        let removed = self.update_copy_authority(
            &request.index,
            request.authority.shard_id,
            &request.authority.index_uuid,
            request.target_allocation_id,
            |identity, _| {
                self.validate_promotion_authority(request)?;
                let current = self
                    .frozen_histories
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&binding)
                    .cloned();
                if let Some(current) = current {
                    if current.report.authority != request.authority
                        || request.token.as_ref() != Some(&current.report.token)
                    {
                        anyhow::bail!(
                            "promotion release token does not identify the current frozen process"
                        );
                    }
                    Ok(self
                        .frozen_histories
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .remove(&binding))
                } else {
                    self.with_applied_state(|state| -> Result<()> {
                        let state = state.ok_or_else(|| {
                            anyhow::anyhow!(
                                "unbound copy cannot attest an absent promotion session"
                            )
                        })?;
                        state
                            .validate_write_authority(
                                &request.index,
                                &request.authority,
                                PrimaryAuthorityPhase::Active,
                            )
                            .map_err(anyhow::Error::msg)?;
                        if state
                            .shard_allocation_ids(&request.index, request.authority.shard_id)
                            .and_then(|ids| ids.primary_authority.history.as_ref())
                            != identity.canonical_history.as_ref()
                        {
                            anyhow::bail!(
                                "released copy does not hold the committed ACTIVE certificate"
                            );
                        }
                        Ok(())
                    })?;
                    Ok(None)
                }
            },
        )?;
        drop(removed);
        Ok(PromotionReply::Released)
    }

    pub(crate) async fn reconcile_frozen_histories_blocking(self: &Arc<Self>) -> Result<()> {
        let manager = self.clone();
        tokio::task::spawn_blocking(move || manager.reconcile_frozen_histories())
            .await
            .map_err(|error| anyhow::anyhow!("promotion-pin reconciliation task failed: {error}"))?
    }

    fn reconcile_frozen_histories(&self) -> Result<()> {
        let sessions = self
            .frozen_histories
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(binding, session)| (binding.clone(), session.clone()))
            .collect::<Vec<_>>();
        let mut retired = Vec::new();
        for (binding, frozen) in sessions {
            let removed = {
                let lifecycle = self.index_lifecycle_lock(&frozen.index);
                let _lifecycle = lifecycle.read().unwrap_or_else(|error| error.into_inner());
                let lock = self.shard_open_lock(&ShardKey::new(&frozen.index, binding.shard_id));
                let _guard = lock.lock().unwrap_or_else(|error| error.into_inner());
                if !self
                    .frozen_histories
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&binding)
                    .is_some_and(|current| Arc::ptr_eq(current, &frozen))
                {
                    continue;
                }
                let action = self.with_applied_state(|state| -> Result<FrozenPinAction> {
                    let Some(state) = state else {
                        return Ok(FrozenPinAction::Keep);
                    };
                    let authority = &frozen.report.authority;
                    let local_node = &self
                        .applied_authority
                        .get()
                        .expect("bound state")
                        .local_node_id;
                    let target_is_current = state
                        .indices
                        .get(&frozen.index)
                        .is_some_and(|metadata| metadata.uuid.as_str() == binding.index_uuid)
                        && state.shard_allocation_id(&frozen.index, binding.shard_id, local_node)
                            == Some(binding.allocation_id);
                    if target_is_current
                        && state
                            .validate_write_authority(
                                &frozen.index,
                                authority,
                                PrimaryAuthorityPhase::Promoting,
                            )
                            .is_ok()
                    {
                        return Ok(FrozenPinAction::Keep);
                    }
                    if target_is_current
                        && state
                            .validate_write_authority(
                                &frozen.index,
                                authority,
                                PrimaryAuthorityPhase::Active,
                            )
                            .is_ok()
                    {
                        let history = state
                            .shard_allocation_ids(&frozen.index, binding.shard_id)
                            .and_then(|ids| ids.primary_authority.history.clone())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "ACTIVE copy has no committed canonical certificate"
                                )
                            })?;
                        return Ok(FrozenPinAction::Retire(Some(history)));
                    }
                    Ok(FrozenPinAction::Retire(None))
                })?;
                let FrozenPinAction::Retire(history) = action else {
                    continue;
                };
                if let Some(history) = history {
                    let identity = self.validated_cached_copy_identity(
                        &ShardKey::new(&frozen.index, binding.shard_id),
                        &binding.index_uuid,
                        binding.allocation_id,
                    )?;
                    if identity.canonical_history.as_ref() != Some(&history) {
                        let reason = "completed promotion copy does not hold its committed ACTIVE certificate";
                        frozen.engine.fail_stop_writes(reason);
                        return Err(definitive_shard_copy_failure(reason));
                    }
                    frozen
                        .engine
                        .copy_write_state()
                        .install_canonical_history(&history)?;
                }
                self.frozen_histories
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&binding)
            };
            if let Some(removed) = removed {
                retired.push(removed);
            }
        }
        drop(retired);
        Ok(())
    }
}
