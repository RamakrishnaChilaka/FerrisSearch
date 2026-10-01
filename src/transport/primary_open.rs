use crate::cluster::ClusterManager;
use crate::shard::ShardManager;
use crate::transport::TransportClient;
use anyhow::Context;
use futures::stream::{self, StreamExt};
use std::sync::Arc;

#[cfg(test)]
struct OpeningProbe<'a>(&'a std::sync::atomic::AtomicUsize);

#[cfg(test)]
impl Drop for OpeningProbe<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(crate) async fn open_local_index_primaries(
    cluster_manager: &ClusterManager,
    shard_manager: &Arc<ShardManager>,
    local_node_id: &str,
    index_name: &str,
) -> anyhow::Result<()> {
    let state = cluster_manager.get_state();
    let metadata = state
        .indices
        .get(index_name)
        .ok_or_else(|| anyhow::anyhow!("no such index [{index_name}]"))?;
    let primaries = metadata
        .shard_routing
        .iter()
        .filter(|(_, routing)| routing.primary == local_node_id)
        .map(|(shard_id, routing)| (*shard_id, routing.primary_term))
        .collect::<Vec<_>>();
    #[cfg(test)]
    let active = std::sync::atomic::AtomicUsize::new(0);
    let results = stream::iter(primaries)
        .map(|(shard_id, primary_term)| {
            let state = &state;
            #[cfg(test)]
            let active = &active;
            async move {
                #[cfg(test)]
                let _probe = {
                    let count = active.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    cluster_manager
                        .primary_open_peak
                        .fetch_max(count, std::sync::atomic::Ordering::Relaxed);
                    OpeningProbe(active)
                };
                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_millis(
                    cluster_manager
                        .primary_open_delay_millis
                        .load(std::sync::atomic::Ordering::Relaxed),
                ))
                .await;
                let allocation_id = state
                    .shard_allocation_id(index_name, shard_id, local_node_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "primary shard [{index_name}][{shard_id}] has no allocation identity"
                        )
                    })?;
                shard_manager
                    .open_primary_assigned_shard_with_settings_blocking(
                        index_name.to_string(),
                        shard_id,
                        metadata.mappings.clone(),
                        metadata.settings.clone(),
                        metadata.uuid.clone(),
                        crate::shard::AssignedShardOpen {
                            allocation_id,
                            primary_term,
                            allow_empty_creation: state.may_create_initial_empty_copy(
                                index_name,
                                shard_id,
                                local_node_id,
                            ),
                        },
                    )
                    .await
                    .with_context(|| format!("open primary shard [{index_name}][{shard_id}]"))?;
                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    results.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}

pub(crate) async fn wait_for_index_primaries(
    cluster_manager: &ClusterManager,
    shard_manager: &Arc<ShardManager>,
    transport_client: &TransportClient,
    local_node_id: &str,
    index_name: &str,
) -> bool {
    let state = cluster_manager.get_state();
    let opening_timeout = cluster_manager.primary_open_wait_timeout();
    let readiness_timeout = opening_timeout + cluster_manager.forwarding_wait_timeout();
    let opening = async {
        let local = async {
            tokio::time::timeout(
                opening_timeout,
                open_local_index_primaries(
                    cluster_manager,
                    shard_manager,
                    local_node_id,
                    index_name,
                ),
            )
            .await
            .with_context(|| {
                format!("timed out opening local primaries for index [{index_name}]")
            })?
        };
        let remote =
            transport_client.open_remote_index_primaries(&state, index_name, local_node_id);
        let (local, remote) = tokio::join!(local, remote);
        local?;
        remote?;
        Ok::<(), anyhow::Error>(())
    };
    let result = tokio::time::timeout(readiness_timeout, opening)
        .await
        .with_context(|| format!("timed out awaiting primary readiness for index [{index_name}]"))
        .and_then(|result| result);
    match result {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(index = index_name, error = %format!("{error:#}"),
                "Index creation committed without primary readiness acknowledgement");
            false
        }
    }
}
