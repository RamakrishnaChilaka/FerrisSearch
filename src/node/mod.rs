//! Node lifecycle management.
//! Manages node lifecycle including startup, shutdown, and Raft consensus.

use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{NodeInfo, NodeRole};
use crate::config::AppConfig;
use crate::consensus::types::{ClusterCommand, RaftInstance};
use crate::shard::ShardManager;
use crate::transport::client::TransportClient;
use crate::wal::TranslogDurability;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

pub struct Node {
    pub config: AppConfig,
    pub cluster_manager: Arc<ClusterManager>,
    pub transport_client: TransportClient,
    pub shard_manager: Arc<ShardManager>,
    pub raft: Option<Arc<RaftInstance>>,
}

impl Node {
    pub async fn new(config: AppConfig) -> anyhow::Result<Self> {
        // Create Raft consensus instance — the state machine owns the
        // authoritative ClusterState, so ClusterManager shares it.
        let (raft, state_handle) = crate::consensus::create_raft_instance(
            config.raft_node_id,
            config.cluster_name.clone(),
            &config.data_dir,
        )
        .await?;

        let cluster_manager = Arc::new(ClusterManager::with_shared_state(state_handle));
        let transport_client = TransportClient::new();

        let durability = match config.translog_durability.as_str() {
            "async" => TranslogDurability::Async {
                sync_interval_ms: config.translog_sync_interval_ms.unwrap_or(5000),
            },
            _ => TranslogDurability::Request,
        };
        let shard_manager = Arc::new(ShardManager::new_with_durability(
            &config.data_dir,
            Duration::from_secs(5),
            durability,
        ));

        Ok(Self {
            config,
            cluster_manager,
            transport_client,
            shard_manager,
            raft: Some(raft),
        })
    }

    /// Starts the node, including all subsystems (HTTP, Transport, Cluster, Engine)
    pub async fn start(&self) -> anyhow::Result<()> {
        info!(
            "Starting FerrisSearch Node: {} (Cluster: {}, raft_id={})",
            self.config.node_name, self.config.cluster_name, self.config.raft_node_id
        );
        info!("Data directory: {}", self.config.data_dir);

        let local_node = NodeInfo {
            id: self.config.node_name.clone(),
            name: self.config.node_name.clone(),
            host: "127.0.0.1".into(),
            transport_port: self.config.transport_port,
            http_port: self.config.http_port,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: self.config.raft_node_id,
        };

        let app_state = crate::api::AppState {
            cluster_manager: self.cluster_manager.clone(),
            shard_manager: self.shard_manager.clone(),
            transport_client: self.transport_client.clone(),
            local_node_id: self.config.node_name.clone(),
            raft: self.raft.clone(),
        };

        // 1. Start internal gRPC Transport Server (Port 9300)
        let transport_service = if let Some(ref raft) = self.raft {
            crate::transport::server::create_transport_service_with_raft(
                self.cluster_manager.clone(),
                self.shard_manager.clone(),
                self.transport_client.clone(),
                raft.clone(),
            )
        } else {
            crate::transport::server::create_transport_service(
                self.cluster_manager.clone(),
                self.shard_manager.clone(),
                self.transport_client.clone(),
            )
        };
        let transport_addr = SocketAddr::from(([0, 0, 0, 0], self.config.transport_port));
        info!("gRPC Transport listening on {}", transport_addr);

        let transport_handle = tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(transport_service)
                .serve(transport_addr)
                .await
            {
                tracing::error!("gRPC transport server failed: {}", e);
            }
        });

        // 2. Start HTTP API Server (Port 9200)
        let app = crate::api::create_router(app_state);
        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.http_port));
        info!("HTTP API listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let http_handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("HTTP server failed: {}", e);
            }
        });

        // 3. Cluster Discovery & Raft Lifecycle Loop
        let client = self.transport_client.clone();
        let seed_hosts = self.config.seed_hosts.clone();
        let manager = self.cluster_manager.clone();
        let local_id = self.config.node_name.clone();
        let raft = self.raft.clone();
        let raft_node_id = self.config.raft_node_id;
        let manager_clone = self.shard_manager.clone();
        let remote_seeds = remote_seed_hosts(&seed_hosts, local_node.transport_port);

        tokio::spawn(async move {
            // Give servers a tiny moment to bind
            tokio::time::sleep(Duration::from_millis(500)).await;

            // ── Bootstrap or join ──────────────────────────────────────
            if let Some(ref raft) = raft {
                // If Raft is already initialized (recovered from disk), skip
                // bootstrap, but still re-register with the leader so its
                // recovered cluster state contains this node again.
                let already_initialized = raft.is_initialized().await.unwrap_or(false);

                if already_initialized {
                    info!("Raft already initialized (recovered from disk), rejoining cluster");
                    if try_join_cluster(&client, &remote_seeds, &local_node, raft_node_id, 20).await
                    {
                        info!("Recovered node re-registered with the cluster leader");
                    } else if !remote_seeds.is_empty() {
                        tracing::warn!(
                            "Recovered node could not re-register via seed hosts yet; will retry in lifecycle loop"
                        );
                    }
                } else {
                    // Try to join an existing cluster (leader handles Raft membership)
                    let joined = if !remote_seeds.is_empty() {
                        client
                            .join_cluster(&remote_seeds, &local_node, raft_node_id)
                            .await
                    } else {
                        None
                    };

                    if joined.is_some() {
                        // Don't call update_state — Raft log replication will
                        // propagate the authoritative state to our state machine.
                        info!(
                            "Joined existing cluster via seed hosts (Raft membership managed by leader)"
                        );
                    } else {
                        // No peers reachable — bootstrap a single-node Raft cluster
                        info!("No cluster found, bootstrapping single-node Raft cluster");
                        let transport_addr = format!("127.0.0.1:{}", local_node.transport_port);
                        if let Err(e) = crate::consensus::bootstrap_single_node(
                            raft,
                            raft_node_id,
                            transport_addr,
                        )
                        .await
                        {
                            tracing::warn!(
                                "Raft bootstrap failed (may already be initialised): {}",
                                e
                            );
                        }

                        // Wait for leader election
                        for _ in 0..50 {
                            if raft.current_leader().await.is_some() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }

                        // Register self and set master through Raft
                        if let Err(e) = raft
                            .client_write(ClusterCommand::AddNode {
                                node: local_node.clone(),
                            })
                            .await
                        {
                            tracing::error!("Failed to register self via Raft: {}", e);
                        }
                        if let Err(e) = raft
                            .client_write(ClusterCommand::SetMaster {
                                node_id: local_id.clone(),
                            })
                            .await
                        {
                            tracing::error!("Failed to set master via Raft: {}", e);
                        }
                        info!("Bootstrapped as master: {}", local_id);
                    }
                }
            }

            let state = manager.get_state();
            open_local_assigned_shards(&state, &local_id, manager_clone.as_ref());

            // ── Lifecycle loop ─────────────────────────────────────────
            let mut leader_since: Option<Instant> = None;

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let state = manager.get_state();
                open_local_assigned_shards(&state, &local_id, manager_clone.as_ref());

                if let Some(ref raft) = raft {
                    if raft.is_leader() {
                        // Track when we became leader for grace period
                        let now = Instant::now();
                        let became_leader_at = *leader_since.get_or_insert(now);

                        // ── Leader duties ──────────────────────────
                        // Ensure master_node is set to us
                        if state.master_node.as_deref() != Some(&local_id)
                            && let Err(e) = raft
                                .client_write(ClusterCommand::SetMaster {
                                    node_id: local_id.clone(),
                                })
                                .await
                        {
                            tracing::warn!("Failed to set master: {}", e);
                        }

                        // Grace period: don't scan for dead nodes until followers
                        // have had time to discover and ping the new leader.
                        let leader_age = now.duration_since(became_leader_at);
                        let mut dead_nodes = Vec::new();

                        if leader_age > Duration::from_secs(20) {
                            for (node_id, last_seen) in &state.last_seen {
                                if node_id != &local_id
                                    && now.duration_since(*last_seen) > Duration::from_secs(15)
                                {
                                    dead_nodes.push(node_id.clone());
                                }
                            }
                        }

                        for dead in &dead_nodes {
                            tracing::warn!("Node {} has died. Removing via Raft.", dead);

                            // ── Shard failover: promote replicas for orphaned primaries ──
                            // Before removing the node, handle shard routing updates:
                            // 1. Find all indices where the dead node hosts a primary or replica
                            // 2. For orphaned primaries: pick best replica (highest checkpoint) and promote
                            // 3. For lost replicas: increment unassigned count for re-allocation
                            for idx_meta in state.indices.values() {
                                let mut updated = idx_meta.clone();
                                let orphaned_primaries = updated.remove_node(dead);
                                let mut changed = false;

                                for shard_id in &orphaned_primaries {
                                    // Pick the best replica from ISR (highest checkpoint)
                                    let cps = manager_clone
                                        .isr_tracker
                                        .replica_checkpoints(&idx_meta.name, *shard_id);

                                    let promoted = if let Some((best_node, best_cp)) = cps
                                        .iter()
                                        .filter(|(nid, _)| nid != dead)
                                        .max_by_key(|(_, cp)| *cp)
                                    {
                                        tracing::info!(
                                            "Promoting replica '{}' (checkpoint={}) to primary for {}/shard_{}",
                                            best_node,
                                            best_cp,
                                            idx_meta.name,
                                            shard_id
                                        );
                                        updated.promote_replica_to(*shard_id, best_node)
                                    } else {
                                        // No ISR data — fall back to first available replica
                                        tracing::info!(
                                            "Promoting first available replica for {}/shard_{} (no ISR data)",
                                            idx_meta.name,
                                            shard_id
                                        );
                                        updated.promote_replica(*shard_id)
                                    };

                                    if promoted {
                                        changed = true;
                                        // The promoted replica's old slot is now lost — mark as unassigned
                                        if let Some(routing) =
                                            updated.shard_routing.get_mut(shard_id)
                                        {
                                            routing.unassigned_replicas += 1;
                                        }
                                    } else {
                                        tracing::error!(
                                            "No replicas available to promote for {}/shard_{} — shard is unavailable!",
                                            idx_meta.name,
                                            shard_id
                                        );
                                    }
                                }

                                // If dead node was a replica (not primary), its slot was removed by remove_node.
                                // Increment unassigned_replicas so the allocator can reassign it.
                                if orphaned_primaries.is_empty() {
                                    // Dead node was a replica for some shards in this index
                                    for (shard_id, routing) in &idx_meta.shard_routing {
                                        if routing.replicas.contains(dead)
                                            && let Some(r) = updated.shard_routing.get_mut(shard_id)
                                        {
                                            r.unassigned_replicas += 1;
                                            changed = true;
                                        }
                                    }
                                }

                                if changed
                                    && let Err(e) = raft
                                        .client_write(ClusterCommand::UpdateIndex {
                                            metadata: updated,
                                        })
                                        .await
                                {
                                    tracing::error!(
                                        "Failed to update shard routing for '{}' after node death: {}",
                                        idx_meta.name,
                                        e
                                    );
                                }
                            }

                            // Remove from Raft membership first
                            if let Some(dead_info) = state.nodes.get(dead)
                                && dead_info.raft_node_id > 0
                            {
                                let remaining: std::collections::BTreeSet<u64> = raft
                                    .voter_ids()
                                    .filter(|id| *id != dead_info.raft_node_id)
                                    .collect();
                                if !remaining.is_empty()
                                    && let Err(e) = raft.change_membership(remaining, false).await
                                {
                                    tracing::error!("Failed to remove {} from Raft: {}", dead, e);
                                }
                            }

                            // Then remove from cluster state via Raft
                            if let Err(e) = raft
                                .client_write(ClusterCommand::RemoveNode {
                                    node_id: dead.clone(),
                                })
                                .await
                            {
                                tracing::error!(
                                    "Failed to remove dead node {} via Raft: {}",
                                    dead,
                                    e
                                );
                            }
                        }

                        // ── Shard allocator: assign unassigned replicas to available nodes ──
                        let data_nodes: Vec<String> = state
                            .nodes
                            .values()
                            .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
                            .map(|n| n.id.clone())
                            .collect();

                        for idx_meta in state.indices.values() {
                            if idx_meta.unassigned_replica_count() > 0 {
                                let mut updated = idx_meta.clone();
                                if updated.allocate_unassigned_replicas(&data_nodes) {
                                    tracing::info!(
                                        "Allocating unassigned replicas for index '{}' ({} remaining)",
                                        updated.name,
                                        updated.unassigned_replica_count()
                                    );
                                    if let Err(e) = raft
                                        .client_write(ClusterCommand::UpdateIndex {
                                            metadata: updated,
                                        })
                                        .await
                                    {
                                        tracing::error!(
                                            "Failed to update shard routing via Raft: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        // Reset leader_since when we're not leader
                        leader_since = None;

                        // ── Follower duties ────────────────────────
                        if !state.nodes.contains_key(&local_id)
                            && try_join_cluster(
                                &client,
                                &remote_seeds,
                                &local_node,
                                raft_node_id,
                                1,
                            )
                            .await
                        {
                            tracing::info!(
                                "Recovered follower {} re-registered with leader",
                                local_id
                            );
                        }

                        // Ping the master for health monitoring
                        if let Some(master_id) = &state.master_node
                            && let Some(master_info) = state.nodes.get(master_id)
                            && let Err(e) = client.send_ping(master_info, &local_id).await
                        {
                            tracing::warn!("Failed to ping master {}: {}", master_id, e);
                        }

                        // ── Replica recovery ────────────────────────
                        // Check if we host any replica shards that need translog-based recovery.
                        // For each shard where we are a replica, compare our local checkpoint
                        // against the primary and request missing operations if behind.
                        for idx_meta in state.indices.values() {
                            for (shard_id, routing) in &idx_meta.shard_routing {
                                // Only process shards where we are a replica
                                if !routing.replicas.contains(&local_id) {
                                    continue;
                                }
                                let primary_node = match state.nodes.get(&routing.primary) {
                                    Some(n) => n,
                                    None => continue,
                                };

                                // Check if we have the shard open and get its checkpoint
                                let engine =
                                    match manager_clone.get_shard(&idx_meta.name, *shard_id) {
                                        Some(e) => e,
                                        None => continue, // shard not open yet
                                    };

                                let local_cp = engine.local_checkpoint();
                                if local_cp == 0 {
                                    // Never received any data — request full recovery
                                    match client
                                        .request_recovery(
                                            primary_node,
                                            &idx_meta.name,
                                            *shard_id,
                                            0,
                                        )
                                        .await
                                    {
                                        Ok(result) => {
                                            if !result.operations.is_empty() {
                                                apply_recovery_ops(&engine, &result.operations);
                                                tracing::info!(
                                                    "Replica recovery for {}/shard_{}: applied {} ops (primary at {})",
                                                    idx_meta.name,
                                                    shard_id,
                                                    result.ops_replayed,
                                                    result.primary_checkpoint
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Recovery request for {}/shard_{} failed: {}",
                                                idx_meta.name,
                                                shard_id,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let _ = tokio::try_join!(http_handle, transport_handle)?;

        Ok(())
    }
}

fn open_local_assigned_shards(
    state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
    shard_manager: &ShardManager,
) {
    for (index_name, metadata) in &state.indices {
        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == local_node_id
                || routing
                    .replicas
                    .iter()
                    .any(|node_id| node_id == local_node_id);
            if !assigned_here || shard_manager.get_shard(index_name, *shard_id).is_some() {
                continue;
            }

            if let Err(e) = shard_manager.open_shard_with_settings(
                index_name,
                *shard_id,
                &metadata.mappings,
                &metadata.settings,
            ) {
                tracing::warn!(
                    "Failed to reopen local shard {}/{} during lifecycle reconciliation: {}",
                    index_name,
                    shard_id,
                    e
                );
            }
        }
    }
}

fn remote_seed_hosts(seed_hosts: &[String], local_transport_port: u16) -> Vec<String> {
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

async fn try_join_cluster(
    client: &TransportClient,
    remote_seeds: &[String],
    local_node: &NodeInfo,
    raft_node_id: u64,
    attempts: usize,
) -> bool {
    if remote_seeds.is_empty() {
        return false;
    }

    for attempt in 0..attempts {
        if client
            .join_cluster(remote_seeds, local_node, raft_node_id)
            .await
            .is_some()
        {
            return true;
        }

        if attempt + 1 < attempts {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    false
}

/// Apply recovered translog operations from the primary to a replica shard engine.
fn apply_recovery_ops(
    engine: &Arc<dyn crate::engine::SearchEngine>,
    operations: &[crate::transport::proto::RecoverReplicaOp],
) {
    for op in operations {
        match op.op.as_str() {
            "index" => {
                if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&op.payload_json)
                    && let Err(e) = engine.add_document_with_seq(&op.doc_id, payload, op.seq_no)
                {
                    tracing::error!("Recovery: failed to index doc '{}': {}", op.doc_id, e);
                }
            }
            "delete" => {
                if let Err(e) = engine.delete_document_with_seq(&op.doc_id, op.seq_no) {
                    tracing::error!("Recovery: failed to delete doc '{}': {}", op.doc_id, e);
                }
            }
            other => {
                tracing::warn!("Recovery: unknown op type '{}'", other);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{IndexMetadata, IndexSettings, ShardRoutingEntry};
    use crate::engine::CompositeEngine;
    use crate::transport::proto::RecoverReplicaOp;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_engine() -> (tempfile::TempDir, Arc<dyn crate::engine::SearchEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        (dir, Arc::new(engine))
    }

    #[test]
    fn apply_recovery_ops_indexes_documents() {
        let (_dir, engine) = make_engine();
        let ops = vec![
            RecoverReplicaOp {
                seq_no: 0,
                op: "index".into(),
                doc_id: "r1".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"x": 1})).unwrap(),
            },
            RecoverReplicaOp {
                seq_no: 1,
                op: "index".into(),
                doc_id: "r2".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"x": 2})).unwrap(),
            },
        ];

        apply_recovery_ops(&engine, &ops);

        engine.refresh().unwrap();
        assert!(engine.get_document("r1").unwrap().is_some());
        assert!(engine.get_document("r2").unwrap().is_some());
        assert!(engine.local_checkpoint() >= 1);
    }

    #[test]
    fn apply_recovery_ops_deletes_documents() {
        let (_dir, engine) = make_engine();
        engine
            .add_document("d1", serde_json::json!({"x": 1}))
            .unwrap();
        engine.refresh().unwrap();
        assert!(engine.get_document("d1").unwrap().is_some());

        let ops = vec![RecoverReplicaOp {
            seq_no: 5,
            op: "delete".into(),
            doc_id: "d1".into(),
            payload_json: vec![],
        }];

        apply_recovery_ops(&engine, &ops);

        engine.refresh().unwrap();
        assert!(
            engine.get_document("d1").unwrap().is_none(),
            "document should be deleted"
        );
        assert!(engine.local_checkpoint() >= 5);
    }

    #[test]
    fn apply_recovery_ops_skips_unknown_ops() {
        let (_dir, engine) = make_engine();
        let cp_before = engine.local_checkpoint();

        let ops = vec![RecoverReplicaOp {
            seq_no: 10,
            op: "unknown_op".into(),
            doc_id: "x".into(),
            payload_json: vec![],
        }];

        apply_recovery_ops(&engine, &ops);

        // Checkpoint should not change for unknown ops
        assert_eq!(engine.local_checkpoint(), cp_before);
    }

    #[test]
    fn apply_recovery_ops_empty_is_noop() {
        let (_dir, engine) = make_engine();
        let cp_before = engine.local_checkpoint();
        apply_recovery_ops(&engine, &[]);
        assert_eq!(engine.local_checkpoint(), cp_before);
    }

    #[test]
    fn apply_recovery_ops_mixed_index_and_delete() {
        let (_dir, engine) = make_engine();

        let ops = vec![
            RecoverReplicaOp {
                seq_no: 0,
                op: "index".into(),
                doc_id: "m1".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"v": 1})).unwrap(),
            },
            RecoverReplicaOp {
                seq_no: 1,
                op: "index".into(),
                doc_id: "m2".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"v": 2})).unwrap(),
            },
            RecoverReplicaOp {
                seq_no: 2,
                op: "delete".into(),
                doc_id: "m1".into(),
                payload_json: vec![],
            },
        ];

        apply_recovery_ops(&engine, &ops);

        engine.refresh().unwrap();
        assert!(
            engine.get_document("m1").unwrap().is_none(),
            "m1 should be deleted"
        );
        assert!(
            engine.get_document("m2").unwrap().is_some(),
            "m2 should exist"
        );
        assert!(engine.local_checkpoint() >= 2);
    }

    #[tokio::test]
    async fn open_local_assigned_shards_opens_unopened_local_shards() {
        let dir = tempfile::tempdir().unwrap();
        let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

        let mut state = crate::cluster::state::ClusterState::new("node-test".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        );
        state.add_index(IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing,
            mappings: HashMap::new(),
            settings: IndexSettings::default(),
        });

        assert!(shard_manager.get_shard("idx", 0).is_none());
        open_local_assigned_shards(&state, "node-1", &shard_manager);
        assert!(shard_manager.get_shard("idx", 0).is_some());
    }

    #[test]
    fn remote_seed_hosts_excludes_local_transport_port() {
        let seeds = vec![
            "127.0.0.1:9300".to_string(),
            "127.0.0.1:9301".to_string(),
            "127.0.0.1:9302".to_string(),
        ];

        let remote = remote_seed_hosts(&seeds, 9301);

        assert_eq!(
            remote,
            vec!["127.0.0.1:9300".to_string(), "127.0.0.1:9302".to_string()]
        );
    }
}
