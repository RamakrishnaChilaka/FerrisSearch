use super::*;
use crate::cluster::state::{
    ClusterState, FailedCopyExclusion, PrimaryAuthorityPhase, ShardCopyAllocation,
};
use crate::consensus::types::{ClusterCommand, ClusterResponse, WriteAuthorityRejectedError};
use crate::engine::write_authority::WritePermit;
use crate::engine::{SearchEngine, SequenceStats};
use crate::replication::ReplicationResult;
use crate::transport::proto::shard_write_copy_result::Outcome;
use crate::transport::refresh_deadline::RefreshDeadline;
use std::time::Duration;
use tokio::sync::OwnedRwLockReadGuard;

const EXCLUSION_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(10);
const EXCLUSION_PROPOSAL_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct WriteAcknowledgementContext<'a> {
    pub index: &'a str,
    pub shard_id: u32,
    pub engine: &'a Arc<dyn SearchEngine>,
    pub primary: &'a ActivatedPrimary,
    pub captured: &'a ClusterState,
    pub guard: &'a Arc<OwnedRwLockReadGuard<()>>,
    pub primary_sequence: SequenceStats,
    pub deadline: RefreshDeadline,
}

impl TransportService {
    fn revoke_exclusion_epoch(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
        permit: Option<&WritePermit>,
        reason: &str,
    ) {
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::authority::record(
            "old_authority_revoked",
            &exclusion.authority,
            serde_json::json!({}),
        );
        if let Some(permit) = permit {
            permit.revoke(reason);
        } else if let Some(engine) = self
            .shard_manager
            .get_shard(index, exclusion.authority.shard_id)
        {
            match engine.copy_write_state().primary_epoch() {
                Ok(Some(epoch)) if epoch.identity == exclusion.authority => epoch.revoke(reason),
                Ok(_) => {}
                Err(error) => tracing::error!(
                    index, shard_id = exclusion.authority.shard_id, error = %format!("{error:#}"),
                    "Failed-copy exclusion could not inspect its old epoch for revocation"
                ),
            }
        }
    }

    fn validate_exclusion_source(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
        permit: Option<&WritePermit>,
    ) -> Result<(), String> {
        let state = self.cluster_manager.get_state();
        let result = state
            .validate_write_authority(index, &exclusion.authority, PrimaryAuthorityPhase::Active)
            .and_then(|()| {
                if state.primary_unavailable(index, exclusion.authority.shard_id) {
                    Err("failed-copy exclusion source is now unavailable".into())
                } else {
                    Ok(())
                }
            });
        if let Err(reason) = &result {
            self.revoke_exclusion_epoch(index, exclusion, permit, reason);
        }
        result
    }

    async fn settle_exclusion_journal(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
        accepted: bool,
    ) -> Result<bool, String> {
        let manager = self.shard_manager.clone();
        let worker_index = index.to_string();
        let pending = exclusion.clone();
        let settled = tokio::task::spawn_blocking(move || {
            manager.settle_copy_exclusions(
                &worker_index,
                pending.authority.shard_id,
                &pending.authority.index_uuid,
                pending.authority.primary_allocation_id,
                accepted.then_some(pending.transition_id.as_str()),
            )?;
            let identity = manager
                .copy_identity(&worker_index, pending.authority.shard_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("exclusion settlement has no durable copy identity")
                })?;
            Ok::<_, anyhow::Error>(
                !identity
                    .pending_exclusions
                    .iter()
                    .any(|entry| entry.transition_id == pending.transition_id),
            )
        })
        .await
        .map_err(|error| format!("exclusion settlement worker failed: {error}"))?
        .map_err(|error| format!("exclusion journal settlement failed: {error:#}"))?;
        #[cfg(feature = "protocol-trace")]
        if settled {
            let state = self.cluster_manager.get_state();
            let metadata = state.indices.get(index);
            let routing = metadata
                .and_then(|metadata| metadata.shard_routing.get(&exclusion.authority.shard_id));
            let ids = state.shard_allocation_ids(index, exclusion.authority.shard_id);
            crate::protocol_trace::authority::record(
                "exclusion_settled",
                &exclusion.authority,
                serde_json::json!({
                    "exclusion": exclusion,
                    "accepted": accepted,
                    "observed": {
                        "version": state.version,
                        "index_uuid": metadata.map(|metadata| metadata.uuid.as_str()),
                        "primary": routing.map(|routing| routing.primary.as_str()),
                        "primary_allocation": ids.and_then(|ids| ids.primary),
                        "primary_term": routing.map(|routing| routing.primary_term),
                        "activation_id": ids.and_then(|ids| ids.primary_authority.activation_id),
                        "phase": ids.map(|ids| ids.primary_authority.phase),
                        "primary_unavailable": ids.map(|ids| ids.primary_unavailable),
                        "failed_copies": exclusion.failed_copies.iter().map(|copy| serde_json::json!({
                            "node_id": copy.node_id,
                            "allocation_id": state.shard_allocation_id(index, exclusion.authority.shard_id, &copy.node_id),
                            "authoritative": routing.is_some_and(|routing|
                                routing.primary == copy.node_id || routing.is_replica_in_sync(&copy.node_id)),
                        })).collect::<Vec<_>>(),
                    },
                }),
            );
        }
        Ok(settled)
    }

    async fn submit_failed_copy_exclusion(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
    ) -> anyhow::Result<()> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("failed-copy exclusion requires Raft"))?;
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::authority::record(
            "exclusion_proposed",
            &exclusion.authority,
            serde_json::json!({"exclusion": exclusion}),
        );
        let result = tokio::time::timeout(EXCLUSION_PROPOSAL_TIMEOUT, async {
            if raft.is_leader() {
                let response = raft
                    .client_write(ClusterCommand::ExcludeFailedShardCopies {
                        index_name: index.to_string(),
                        exclusion: exclusion.clone(),
                    })
                    .await?
                    .data;
                match response {
                    ClusterResponse::ExclusionApplied { transition_id }
                        if transition_id == exclusion.transition_id =>
                    {
                        Ok(())
                    }
                    ClusterResponse::WriteAuthorityRejected { reason } => {
                        Err(WriteAuthorityRejectedError { reason }.into())
                    }
                    other => anyhow::bail!("inconsistent failed-copy exclusion result: {other:?}"),
                }
            } else {
                self.transport_client
                    .forward_exclude_failed_shard_copies(
                        &self.authority_leader()?,
                        index,
                        exclusion,
                    )
                    .await
            }
        })
        .await
        .map_err(|error| {
            anyhow::anyhow!("failed-copy exclusion proposal deadline expired: {error}")
        })?;
        #[cfg(feature = "protocol-trace")]
        if result.is_ok() {
            crate::protocol_trace::authority::record(
                "exclusion_committed",
                &exclusion.authority,
                serde_json::json!({"exclusion": exclusion}),
            );
        }
        result
    }

    async fn settle_failed_copy_exclusion(
        &self,
        index: &str,
        exclusion: &FailedCopyExclusion,
        permit: Option<&WritePermit>,
    ) -> Result<(), String> {
        let result = tokio::time::timeout(EXCLUSION_SETTLEMENT_TIMEOUT, async {
            let manager = self.shard_manager.clone();
            let journal_index = index.to_string();
            let journal = exclusion.clone();
            tokio::task::spawn_blocking(move || {
                manager.persist_failed_copy_exclusion(&journal_index, &journal)
            })
            .await
            .map_err(|error| format!("failed-copy exclusion journal worker failed: {error}"))?
            .map_err(|error| format!("failed-copy exclusion journal failed: {error:#}"))?;
            #[cfg(feature = "protocol-trace")]
            crate::protocol_trace::authority::record(
                "exclusion_journaled",
                &exclusion.authority,
                serde_json::json!({"exclusion": exclusion}),
            );

            let locks = self.primary_activation_state.copy_locks(&(
                exclusion.authority.index_uuid.clone(),
                exclusion.authority.shard_id,
                exclusion.authority.primary_allocation_id,
            ));
            let _settlement = locks.exclusion_settlement.lock().await;
            if self
                .settle_exclusion_journal(index, exclusion, false)
                .await?
            {
                return self.validate_exclusion_source(index, exclusion, permit);
            }
            self.validate_exclusion_source(index, exclusion, permit)?;
            let result = self.submit_failed_copy_exclusion(index, exclusion).await;
            let accepted = result.is_ok();
            if let Err(error) = &result {
                if error.is::<WriteAuthorityRejectedError>() {
                    self.revoke_exclusion_epoch(
                        index,
                        exclusion,
                        permit,
                        &format!(
                            "committed failed-copy exclusion rejected old authority: {error:#}"
                        ),
                    );
                    return Err(format!(
                        "failed-copy exclusion rejected old authority: {error:#}"
                    ));
                }
                tracing::warn!(
                    index,
                    shard_id = exclusion.authority.shard_id,
                    transition_id = exclusion.transition_id,
                    error = %format!("{error:#}"),
                    "Failed-copy exclusion is unresolved; observing applied authority"
                );
            }
            loop {
                if self
                    .settle_exclusion_journal(index, exclusion, accepted)
                    .await?
                {
                    return self.validate_exclusion_source(index, exclusion, permit);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(format!(
                "failed-copy exclusion is indeterminate and remains admission-blocking: {error}"
            )),
        };
        crate::metrics::WRITE_EXCLUSION_SETTLEMENTS_TOTAL
            .with_label_values(&[if result.is_ok() { "settled" } else { "failed" }])
            .inc();
        result
    }

    pub(super) async fn reconcile_pending_write_exclusions(
        &self,
        index: &str,
        shard_id: u32,
    ) -> Result<(), String> {
        let Some(engine) = self.shard_manager.get_shard(index, shard_id) else {
            return Ok(());
        };
        let Some(epoch) = engine
            .copy_write_state()
            .primary_epoch()
            .map_err(|error| format!("pending exclusion epoch is unavailable: {error:#}"))?
        else {
            // Restart must establish fresh canonical authority before using this copy.
            return Ok(());
        };
        let pending = self
            .shard_manager
            .copy_identity(index, shard_id)
            .ok_or_else(|| "pending exclusion has no durable copy identity".to_string())?
            .pending_exclusions;
        tokio::time::timeout(EXCLUSION_SETTLEMENT_TIMEOUT, async {
            for exclusion in pending {
                if self
                    .settle_exclusion_journal(index, &exclusion, false)
                    .await?
                {
                    continue;
                }
                if epoch.identity != exclusion.authority {
                    return Err("pending exclusion belongs to an unresolved older authority".into());
                }
                self.settle_failed_copy_exclusion(index, &exclusion, None)
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(|error| format!("pending failed-copy exclusion reconciliation expired: {error}"))?
    }

    pub(super) async fn acknowledge_replicated_write(
        &self,
        context: WriteAcknowledgementContext<'_>,
        replication: ReplicationResult,
    ) -> Result<ShardWriteAcknowledgement, String> {
        let replication_cause = replication
            .as_ref()
            .err()
            .map(|failure| Self::replication_failure_message(&failure.failures));
        let result = self
            .certify_replicated_write(context, replication)
            .await
            .map_err(|reason| match replication_cause {
                Some(cause) if !cause.is_empty() => format!("{reason}; replica failures: {cause}"),
                _ => reason,
            });
        let outcome = match &result {
            Ok(report)
                if report
                    .copies
                    .iter()
                    .any(|copy| matches!(copy.outcome, Some(Outcome::Error(_)))) =>
            {
                "after_exclusion"
            }
            Ok(_) => "all_copies",
            Err(_) => "failed",
        };
        crate::metrics::WRITE_CERTIFICATIONS_TOTAL
            .with_label_values(&[outcome])
            .inc();
        result
    }

    async fn certify_replicated_write(
        &self,
        context: WriteAcknowledgementContext<'_>,
        replication: ReplicationResult,
    ) -> Result<ShardWriteAcknowledgement, String> {
        let WriteAcknowledgementContext {
            index,
            shard_id,
            engine,
            primary,
            captured,
            guard,
            primary_sequence,
            deadline,
        } = context;
        let metadata = captured
            .indices
            .get(index)
            .ok_or_else(|| "write acknowledgement has no captured index".to_string())?;
        let routing = metadata
            .shard_routing
            .get(&shard_id)
            .ok_or_else(|| "write acknowledgement has no captured shard".to_string())?;
        let required = routing
            .in_sync_replicas
            .iter()
            .map(|node| {
                captured
                    .shard_allocation_id(index, shard_id, node)
                    .filter(|allocation| *allocation > 0)
                    .map(|allocation_id| ShardCopyAllocation {
                        node_id: node.clone(),
                        allocation_id,
                    })
                    .ok_or_else(|| format!("captured replica [{node}] has no exact allocation ID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (acknowledged, errors) = match replication {
            Ok(acknowledged) => (acknowledged, Vec::new()),
            Err(failure) => (failure.acknowledged, failure.failures),
        };
        let mut proven = HashSet::new();
        for proof in &acknowledged {
            if !required.iter().any(|copy| {
                copy.node_id == proof.node_id && copy.allocation_id == proof.allocation_id
            }) || !proven.insert((proof.node_id.clone(), proof.allocation_id))
            {
                if let Some(permit) = &primary.permit {
                    permit.revoke("replication returned an unrequested or duplicate copy proof");
                }
                return Err("replication returned an unrequested or duplicate copy proof".into());
            }
        }
        let failed = required
            .iter()
            .filter(|copy| !proven.contains(&(copy.node_id.clone(), copy.allocation_id)))
            .cloned()
            .collect::<Vec<_>>();
        if !failed.is_empty() {
            let permit = primary.permit.as_ref().ok_or_else(|| {
                format!(
                    "replication failed without production exclusion authority: {}",
                    Self::replication_failure_message(&errors)
                )
            })?;
            let exclusion = FailedCopyExclusion {
                authority: permit.identity().clone(),
                captured_state_version: captured.version,
                transition_id: uuid::Uuid::new_v4().to_string(),
                failed_copies: failed.clone(),
            };
            exclusion.validate()?;
            permit
                .close_exclusion_admission(&exclusion.transition_id)
                .map_err(|error| {
                    format!("cannot close failed-copy exclusion admission: {error:#}")
                })?;
            #[cfg(feature = "protocol-trace")]
            crate::protocol_trace::authority::record(
                "exclusion_closed",
                &exclusion.authority,
                serde_json::json!({"exclusion": exclusion}),
            );
            let service = self.clone();
            let job_index = index.to_string();
            let job_permit = permit.clone();
            let job_guard = guard.clone();
            let job = tokio::spawn(async move {
                let _guard = job_guard;
                let result = service
                    .settle_failed_copy_exclusion(&job_index, &exclusion, Some(&job_permit))
                    .await;
                if let Err(error) = &result {
                    tracing::warn!(
                        index = job_index,
                        shard_id,
                        transition_id = exclusion.transition_id,
                        error,
                        "Failed-copy exclusion remains unresolved after the write"
                    );
                }
                result
            });
            tokio::time::timeout(deadline.remaining(), job)
                .await
                .map_err(|error| format!(
                    "write response deadline expired; failed-copy exclusion remains sticky: {error}"
                ))?
                .map_err(|error| {
                    permit.revoke("failed-copy exclusion task failed before definitive settlement");
                    format!("failed-copy exclusion task failed: {error}")
                })??;
        } else if !errors.is_empty() {
            return Err(format!(
                "replication returned contradictory failures after complete copy proofs: {}",
                Self::replication_failure_message(&errors),
            ));
        }
        if deadline.remaining().is_zero() {
            return Err("write response deadline expired after local mutation".into());
        }
        let current = self.validated_primary_write_state(index, shard_id, primary)?;
        let current_metadata = &current.indices[index];
        let current_routing = &current_metadata.shard_routing[&shard_id];
        let mut eligible_proven = 1u64;
        for node in &current_routing.in_sync_replicas {
            let allocation_id = current
                .shard_allocation_id(index, shard_id, node)
                .ok_or_else(|| format!("current in-sync replica [{node}] has no allocation ID"))?;
            if proven.contains(&(node.clone(), allocation_id)) {
                eligible_proven += 1;
            } else if !failed
                .iter()
                .any(|copy| copy.node_id == *node && copy.allocation_id == allocation_id)
            {
                return Err(format!(
                    "current in-sync replica [{node}] allocation {allocation_id} has no operation proof or settled exclusion"
                ));
            }
        }
        let minimum = metadata
            .settings
            .minimum_durable_copies
            .max(current_metadata.settings.minimum_durable_copies);
        if eligible_proven < u64::from(minimum) {
            return Err(format!(
                "minimum_durable_copies requires {minimum} eligible operation proofs; only {eligible_proven} remain after committed exclusions"
            ));
        }
        self.validate_primary_ack(engine, index, shard_id, primary)?;
        let checkpoint_service = self.clone();
        let checkpoint_engine = engine.clone();
        let checkpoint_index = index.to_string();
        let checkpoint_primary = primary.clone();
        let checkpoint_guard = guard.clone();
        let publication = tokio::task::spawn_blocking(move || {
            let _guard = checkpoint_guard;
            checkpoint_service.record_replica_checkpoints(
                &checkpoint_engine,
                &checkpoint_index,
                shard_id,
                &checkpoint_primary,
                primary_sequence,
                &acknowledged,
            )
        });
        tokio::time::timeout(deadline.remaining(), publication)
        .await
        .map_err(|error| format!(
            "write response deadline expired during durable global progress publication: {error}"
        ))?
        .map_err(|error| {
            engine.fail_stop_writes(&format!("global progress worker failed after mutation: {error}"));
            format!("global progress worker failed after mutation: {error}")
        })??;
        if deadline.remaining().is_zero() {
            return Err("write response deadline expired after global progress publication".into());
        }
        self.validate_primary_ack(engine, index, shard_id, primary)?;
        let mut copies = vec![ShardWriteCopyResult {
            node_id: self.local_node_id.clone(),
            allocation_id: primary.allocation_id,
            primary: true,
            outcome: Some(Outcome::Acknowledged(true)),
        }];
        for copy in required {
            let outcome = if proven.contains(&(copy.node_id.clone(), copy.allocation_id)) {
                Outcome::Acknowledged(true)
            } else {
                let reason = errors
                    .iter()
                    .find(|failure| {
                        failure.node_id == copy.node_id
                            && failure.allocation_id == Some(copy.allocation_id)
                    })
                    .map_or_else(
                        || {
                            "no exact operation proof; copy excluded by committed authority"
                                .to_string()
                        },
                        |failure| {
                            format!("{}; copy excluded by committed authority", failure.message)
                        },
                    );
                Outcome::Error(reason)
            };
            copies.push(ShardWriteCopyResult {
                node_id: copy.node_id,
                allocation_id: copy.allocation_id,
                primary: false,
                outcome: Some(outcome),
            });
        }
        #[cfg(feature = "protocol-trace")]
        if let Some(permit) = &primary.permit {
            crate::protocol_trace::authority::record(
                "write_certified",
                permit.identity(),
                serde_json::json!({
                    "minimum": minimum,
                    "eligible_proven": eligible_proven,
                    "copies": copies.iter().map(|copy| serde_json::json!({
                        "node_id": copy.node_id,
                        "allocation_id": copy.allocation_id,
                        "primary": copy.primary,
                        "acknowledged": matches!(copy.outcome, Some(Outcome::Acknowledged(true))),
                    })).collect::<Vec<_>>(),
                }),
            );
        }
        Ok(ShardWriteAcknowledgement { copies })
    }
}
