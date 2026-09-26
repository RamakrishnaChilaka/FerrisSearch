//! Cluster join and seed host discovery helpers.

use crate::cluster::state::NodeInfo;
use crate::consensus::types::RaftInstance;
use crate::transport::client::TransportClient;
use std::sync::Arc;
use std::time::Duration;

pub(super) fn remote_seed_hosts(seed_hosts: &[String], local_transport_port: u16) -> Vec<String> {
    seed_hosts
        .iter()
        .filter(|h| {
            let port = h
                .rsplit_once(':')
                .and_then(|(_, p)| p.parse::<u16>().ok())
                .unwrap_or(9300);
            port != local_transport_port
        })
        .cloned()
        .collect()
}

pub(super) async fn try_join_cluster(
    client: &TransportClient,
    remote_seeds: &[String],
    local_node: &NodeInfo,
    raft_node_id: u64,
    attempts: usize,
    raft: Option<Arc<RaftInstance>>,
) -> Option<crate::cluster::state::ClusterState> {
    if remote_seeds.is_empty() {
        return None;
    }

    for attempt in 0..attempts {
        // If this node became the Raft leader, it doesn't need to join
        // via seeds — the lifecycle loop handles SetMaster and other
        // leader duties.  Short-circuiting here avoids dozens of futile
        // join retries that block the lifecycle loop from starting.
        if let Some(ref raft) = raft
            && raft.is_leader()
        {
            tracing::info!(
                "Became Raft leader during join attempts — skipping remaining seed-based join"
            );
            return None;
        }

        if let Some(state) = client
            .join_cluster(remote_seeds, local_node, raft_node_id)
            .await
        {
            return Some(state);
        }

        if attempt + 1 < attempts {
            // Exponential backoff: 500ms, 1s, 2s, 4s, capped at 5s
            let delay = std::cmp::min(500 * (1 << attempt), 5000);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

    None
}
