use super::*;
use crate::transport::proto::shard_copy_refresh_result::Outcome;

impl TransportService {
    pub(super) async fn refresh_acknowledged_write(
        &self,
        write_state: &crate::cluster::state::ClusterState,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) -> ShardWriteRefreshResult {
        let routing = &write_state.indices[index_name].shard_routing[&shard_id];
        // Keep the operation's acknowledgement set, not a newer coordinator view.
        let targets = std::iter::once(&routing.primary).chain(&routing.in_sync_replicas);
        let copies = futures::future::join_all(targets.map(|node_id| async move {
            let allocation_id = write_state
                .shard_allocation_id(index_name, shard_id, node_id)
                .expect("acknowledged shard copy has an allocation ID");
            let primary = *node_id == routing.primary;
            let request = ShardCopyRefreshRequest {
                index_name: index_name.to_string(),
                index_uuid: activated_primary.index_uuid.clone(),
                shard_id,
                primary_node_id: routing.primary.clone(),
                primary_term: Some(activated_primary.primary_term),
                target_allocation_id: Some(allocation_id),
            };
            if node_id == &self.local_node_id {
                return self.refresh_local_copy(request).await;
            }
            let result = match write_state.nodes.get(node_id) {
                Some(node) => {
                    self.transport_client
                        .refresh_shard_copy(node, request)
                        .await
                }
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
                    ShardCopyRefreshResult {
                        node_id: node_id.clone(),
                        allocation_id,
                        primary,
                        outcome: Some(Outcome::Error(format!("{error:#}"))),
                    }
                }
            }
        }))
        .await;
        ShardWriteRefreshResult { copies }
    }

    pub(super) async fn refresh_local_copy(
        &self,
        request: ShardCopyRefreshRequest,
    ) -> ShardCopyRefreshResult {
        let result = match self
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
            allocation_id: request
                .target_allocation_id
                .expect("validated refresh allocation"),
            primary: self.local_node_id == request.primary_node_id,
            outcome: Some(outcome),
        }
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
                .expect("validated refresh allocation"),
        )?;
        let identity = self
            .shard_manager
            .copy_identity(&request.index_name, request.shard_id)
            .ok_or_else(|| anyhow::anyhow!("refresh copy has no durable identity"))?;
        if identity.replica_fence > request.primary_term.expect("validated refresh term") {
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
