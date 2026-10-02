use super::*;
use crate::transport::proto::shard_copy_refresh_result::Outcome;
use crate::transport::refresh_deadline::{COPY_REFRESH_TIMEOUT, RefreshDeadline};
use crate::transport::write_refresh::{copy_refresh_failure, validate_refresh_request};
use std::time::Duration;

fn refresh_timeout_message(budget: Duration) -> String {
    format!(
        "refresh timed out after {} ms; write acknowledgement is unchanged",
        budget.as_millis()
    )
}

impl TransportService {
    pub(super) async fn refresh_acknowledged_write(
        &self,
        write_state: &crate::cluster::state::ClusterState,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
        deadline: RefreshDeadline,
    ) -> ShardWriteRefreshResult {
        let primary_failure = |reason: String| ShardWriteRefreshResult {
            copies: vec![copy_refresh_failure(
                &self.local_node_id,
                Some(activated_primary.allocation_id).filter(|allocation| *allocation > 0),
                true,
                reason,
            )],
        };
        let Some(metadata) = write_state.indices.get(index_name) else {
            return primary_failure(format!(
                "refresh index [{index_name}] is missing from captured Raft state"
            ));
        };
        let Some(routing) = metadata.shard_routing.get(&shard_id) else {
            return primary_failure(format!(
                "refresh shard [{index_name}][{shard_id}] is missing from captured Raft state"
            ));
        };
        if routing.primary != self.local_node_id
            || metadata.uuid.as_str() != activated_primary.index_uuid
            || routing.primary_term != activated_primary.primary_term
        {
            return primary_failure(
                "refresh authority contradicts the captured primary identity".into(),
            );
        }
        let budget = deadline.copy_budget(self.copy_refresh_limit());
        let targets = std::iter::once(&routing.primary).chain(&routing.in_sync_replicas);
        let copies = futures::future::join_all(targets.map(|node_id| async move {
            let primary = *node_id == routing.primary;
            let Some(allocation_id) = write_state
                .shard_allocation_id(index_name, shard_id, node_id)
                .filter(|allocation| *allocation > 0)
            else {
                return copy_refresh_failure(
                    node_id,
                    None,
                    primary,
                    format!("refresh copy [{node_id}] has no allocation ID in captured Raft state"),
                );
            };
            let request = ShardCopyRefreshRequest {
                index_name: index_name.to_string(),
                index_uuid: activated_primary.index_uuid.clone(),
                shard_id,
                primary_node_id: routing.primary.clone(),
                primary_term: Some(activated_primary.primary_term),
                target_allocation_id: Some(allocation_id),
            };
            if budget.is_zero() {
                return copy_refresh_failure(
                    node_id,
                    Some(allocation_id),
                    primary,
                    refresh_timeout_message(budget),
                );
            }
            if node_id == &self.local_node_id {
                return self.refresh_local_copy(request, budget).await;
            }
            let result = match write_state.nodes.get(node_id) {
                Some(node) => match tokio::time::timeout(
                    budget,
                    self.transport_client
                        .refresh_shard_copy(node, request, budget),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!(refresh_timeout_message(budget))),
                },
                None => Err(anyhow::anyhow!(
                    "refresh target node [{node_id}] is absent from captured cluster state"
                )),
            };
            match result {
                Ok(result) => result,
                Err(error) => {
                    tracing::error!(
                        index = index_name, shard_id, node = node_id, allocation_id,
                        error = %format!("{error:#}"),
                        "Post-write copy refresh failed after replication acknowledged"
                    );
                    copy_refresh_failure(
                        node_id,
                        Some(allocation_id),
                        primary,
                        format!("{error:#}"),
                    )
                }
            }
        }))
        .await;
        ShardWriteRefreshResult { copies }
    }

    pub(super) async fn refresh_local_copy(
        &self,
        request: ShardCopyRefreshRequest,
        budget: Duration,
    ) -> ShardCopyRefreshResult {
        let operation = async {
            validate_refresh_request(&request)?;
            match self
                .shard_manager
                .get_shard(&request.index_name, request.shard_id)
            {
                Some(engine) => {
                    let service = self.clone();
                    let request = request.clone();
                    crate::worker::spawn_engine_maintenance("post-write copy refresh", move || {
                        service.validate_refresh_copy(&request, &engine)?;
                        engine.refresh()?;
                        service.validate_refresh_copy(&request, &engine)
                    })
                    .await
                }
                None => Err(anyhow::anyhow!("acknowledged shard copy is no longer open")),
            }
        };
        let result = if budget.is_zero() {
            Err(anyhow::anyhow!(refresh_timeout_message(budget)))
        } else {
            match tokio::time::timeout(budget, operation).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(refresh_timeout_message(budget))),
            }
        };
        let outcome = match result {
            Ok(()) => Outcome::Refreshed(Empty {}),
            Err(error) => {
                tracing::error!(
                    index = request.index_name, shard_id = request.shard_id,
                    node = self.local_node_id,
                    error = %format!("{error:#}"),
                    "Post-write copy refresh failed after replication acknowledged"
                );
                Outcome::Error(format!("{error:#}"))
            }
        };
        ShardCopyRefreshResult {
            node_id: self.local_node_id.clone(),
            allocation_id: request.target_allocation_id,
            primary: self.local_node_id == request.primary_node_id,
            outcome: Some(outcome),
        }
    }

    pub(super) async fn refresh_primary_shard_writes(
        &self,
        request: ShardCopyRefreshRequest,
        deadline: RefreshDeadline,
    ) -> Result<ShardWriteRefreshResult, Status> {
        validate_refresh_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let prepare = async {
            let current = self
                .primary_routing(&request.index_name, request.shard_id)
                .map_err(Status::failed_precondition)?;
            if request.primary_node_id != self.local_node_id
                || current.index_uuid != request.index_uuid
                || Some(current.allocation_id) != request.target_allocation_id
                || Some(current.primary_term) != request.primary_term
            {
                return Err(Status::failed_precondition(
                    "bulk refresh primary identity changed",
                ));
            }
            // Activation may take the exclusive barrier; it must precede the shared guard.
            let activated = self
                .ensure_primary_activated(&request.index_name, request.shard_id)
                .await
                .map_err(Status::failed_precondition)?;
            let _guard = self
                .peer_recovery_write_guard(&request.index_name, request.shard_id)
                .await
                .map_err(Status::failed_precondition)?;
            let state = self
                .validated_primary_write_state(&request.index_name, request.shard_id, &activated)
                .map_err(Status::failed_precondition)?;
            if activated.index_uuid != request.index_uuid
                || Some(activated.allocation_id) != request.target_allocation_id
                || Some(activated.primary_term) != request.primary_term
            {
                return Err(Status::failed_precondition(
                    "bulk refresh primary identity changed during activation",
                ));
            }
            Ok(self
                .refresh_acknowledged_write(
                    &state,
                    &request.index_name,
                    request.shard_id,
                    &activated,
                    deadline,
                )
                .await)
        };
        tokio::time::timeout(deadline.remaining(), prepare)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("primary-owned bulk refresh timed out before reply")
            })?
    }

    pub(super) fn copy_refresh_limit(&self) -> Duration {
        #[cfg(test)]
        if let Some(limit) = *self
            .primary_activation_state
            .refresh_limit_for_test
            .lock()
            .unwrap_or_else(|error| error.into_inner())
        {
            return limit;
        }
        COPY_REFRESH_TIMEOUT
    }

    #[cfg(test)]
    pub(crate) fn set_copy_refresh_limit_for_test(&self, limit: Duration) {
        *self
            .primary_activation_state
            .refresh_limit_for_test
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(limit);
    }

    fn validate_refresh_copy(
        &self,
        request: &ShardCopyRefreshRequest,
        engine: &Arc<dyn crate::engine::SearchEngine>,
    ) -> anyhow::Result<()> {
        let state = self.cluster_manager.get_state();
        let metadata = state.indices.get(&request.index_name).ok_or_else(|| {
            anyhow::anyhow!("refresh index [{}] no longer exists", request.index_name)
        })?;
        let routing = metadata
            .shard_routing
            .get(&request.shard_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "refresh shard [{}][{}] no longer exists",
                    request.index_name,
                    request.shard_id
                )
            })?;
        if metadata.uuid.as_str() != request.index_uuid
            || routing.primary != request.primary_node_id
            || Some(routing.primary_term) != request.primary_term
            || state.shard_allocation_id(&request.index_name, request.shard_id, &self.local_node_id)
                != request.target_allocation_id
            || (routing.primary != self.local_node_id
                && !routing.is_replica_in_sync(&self.local_node_id))
        {
            anyhow::bail!(
                "refresh copy authority changed for [{}][{}] on [{}]: expected UUID {}, allocation {:?}, primary {}, term {:?}; observed UUID {}, allocation {:?}, primary {}, term {}, in-sync replica {}",
                request.index_name,
                request.shard_id,
                self.local_node_id,
                request.index_uuid,
                request.target_allocation_id,
                request.primary_node_id,
                request.primary_term,
                metadata.uuid,
                state.shard_allocation_id(
                    &request.index_name,
                    request.shard_id,
                    &self.local_node_id
                ),
                routing.primary,
                routing.primary_term,
                routing.is_replica_in_sync(&self.local_node_id),
            );
        }
        self.shard_manager.validate_open_copy_identity(
            &request.index_name,
            request.shard_id,
            &request.index_uuid,
            request
                .target_allocation_id
                .filter(|allocation| *allocation > 0)
                .ok_or_else(|| anyhow::anyhow!("refresh copy has no positive allocation ID"))?,
        )?;
        let identity = self
            .shard_manager
            .copy_identity(&request.index_name, request.shard_id)
            .ok_or_else(|| anyhow::anyhow!("refresh copy has no durable identity"))?;
        let term = request
            .primary_term
            .filter(|term| *term > 0)
            .ok_or_else(|| anyhow::anyhow!("refresh copy has no positive primary term"))?;
        if identity.replica_fence > term {
            anyhow::bail!(
                "refresh copy has accepted primary term {}, above requested term {:?}",
                identity.replica_fence,
                request.primary_term,
            );
        }

        if self
            .shard_manager
            .rejects_live_replication(&request.index_name, request.shard_id)
        {
            anyhow::bail!("refresh copy is installing peer recovery");
        }
        if self
            .shard_manager
            .get_shard(&request.index_name, request.shard_id)
            .is_none_or(|current| !Arc::ptr_eq(&current, engine))
        {
            anyhow::bail!("refresh copy engine was closed or replaced");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{ClusterState, IndexMetadata};
    use serde_json::json;

    #[tokio::test]
    async fn refresh_capture_invariant_errors_are_attributable_without_panicking() {
        let directory = tempfile::tempdir().unwrap();
        let service = build_transport_service_for_test(
            Arc::new(ClusterManager::new("refresh-invariants".into())),
            Arc::new(ShardManager::new(directory.path(), Duration::from_secs(60))),
            crate::transport::TransportClient::new(),
            Arc::new(crate::tasks::TaskManager::new()),
            "primary".into(),
        );
        let mut metadata = IndexMetadata::from_create_request_body(
            "idx",
            &json!({"settings": {"number_of_shards": 1, "number_of_replicas": 1}}),
            &["primary".into(), "replica".into()],
        )
        .unwrap();
        metadata.shard_routing.get_mut(&0).unwrap().in_sync_replicas = vec!["replica".into()];
        let activated = ActivatedPrimary {
            index_uuid: metadata.uuid.to_string(),
            allocation_id: 9,
            primary_term: metadata.shard_routing[&0].primary_term,
        };
        let deadline = RefreshDeadline::from_request(&Request::new(())).unwrap();
        let empty = ClusterState::new("refresh-invariants".into());
        let mut missing_shard = empty.clone();
        let mut missing_routing = metadata.clone();
        missing_routing.shard_routing.clear();
        missing_shard.indices.insert("idx".into(), missing_routing);
        let mut wrong_primary = empty.clone();
        let mut wrong_routing = metadata.clone();
        wrong_routing.shard_routing.get_mut(&0).unwrap().primary = "other-primary".into();
        wrong_primary.indices.insert("idx".into(), wrong_routing);
        for (state, reason) in [
            (&empty, "index [idx] is missing"),
            (&missing_shard, "shard [idx][0] is missing"),
            (&wrong_primary, "authority contradicts"),
        ] {
            let result = service
                .refresh_acknowledged_write(state, "idx", 0, &activated, deadline)
                .await;
            crate::transport::write_refresh::validate_write_refresh_response(
                Some(&result),
                true,
                "primary",
            )
            .unwrap();
            assert_eq!(result.copies.len(), 1, "{result:?}");
            assert_eq!(result.copies[0].allocation_id, Some(9));
            assert!(
                matches!(&result.copies[0].outcome, Some(Outcome::Error(error)) if error.contains(reason)),
                "{result:?}"
            );
        }
        let mut missing_allocations = empty;
        missing_allocations.indices.insert("idx".into(), metadata);
        let result = service
            .refresh_acknowledged_write(&missing_allocations, "idx", 0, &activated, deadline)
            .await;
        crate::transport::write_refresh::validate_write_refresh_response(
            Some(&result),
            true,
            "primary",
        )
        .unwrap();
        assert_eq!(result.copies.len(), 2, "{result:?}");
        for copy in result.copies {
            assert!(copy.allocation_id.is_none(), "{copy:?}");
            assert!(
                matches!(&copy.outcome, Some(Outcome::Error(error)) if error.contains("has no allocation ID")),
                "{copy:?}"
            );
        }
    }
}
