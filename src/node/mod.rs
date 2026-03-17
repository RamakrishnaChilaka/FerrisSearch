//! Node lifecycle management.
//! Manages node lifecycle including startup, shutdown, and Raft consensus.

use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{NodeInfo, NodeRole};
use crate::config::AppConfig;
use crate::consensus::types::{ClusterCommand, RaftInstance};
use crate::shard::ShardManager;
use crate::transport::client::TransportClient;
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
        let (raft, state_handle) =
            crate::consensus::create_raft_instance(
                config.raft_node_id,
                config.cluster_name.clone(),
                &config.data_dir,
            ).await?;

        let cluster_manager = Arc::new(ClusterManager::with_shared_state(state_handle));
        let transport_client = TransportClient::new();
        let shard_manager = Arc::new(ShardManager::new(&config.data_dir, Duration::from_secs(5)));

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

        tokio::spawn(async move {
            // Give servers a tiny moment to bind
            tokio::time::sleep(Duration::from_millis(500)).await;

            // ── Bootstrap or join ──────────────────────────────────────
            if let Some(ref raft) = raft {
                // If Raft is already initialized (recovered from disk), skip
                // bootstrap/join — just wait for the cluster to catch up.
                let already_initialized = raft.is_initialized().await.unwrap_or(false);

                if already_initialized {
                    info!("Raft already initialized (recovered from disk), rejoining cluster");
                } else {
                    // Filter out seed hosts that point to ourselves
                    let remote_seeds: Vec<String> = seed_hosts.iter()
                        .filter(|h| {
                            let port = h.rsplit_once(':')
                                .and_then(|(_, p)| p.parse::<u16>().ok())
                                .unwrap_or(9300);
                            port != local_node.transport_port
                        })
                        .cloned()
                        .collect();

                    // Try to join an existing cluster (leader handles Raft membership)
                    let joined = if !remote_seeds.is_empty() {
                        client.join_cluster(&remote_seeds, &local_node, raft_node_id).await
                    } else {
                        None
                    };

                    if joined.is_some() {
                        // Don't call update_state — Raft log replication will
                        // propagate the authoritative state to our state machine.
                        info!("Joined existing cluster via seed hosts (Raft membership managed by leader)");
                    } else {
                        // No peers reachable — bootstrap a single-node Raft cluster
                        info!("No cluster found, bootstrapping single-node Raft cluster");
                        let transport_addr = format!("127.0.0.1:{}", local_node.transport_port);
                        if let Err(e) = crate::consensus::bootstrap_single_node(
                            raft, raft_node_id, transport_addr,
                        ).await {
                            tracing::warn!("Raft bootstrap failed (may already be initialised): {}", e);
                        }

                        // Wait for leader election
                        for _ in 0..50 {
                            if raft.current_leader().await.is_some() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }

                        // Register self and set master through Raft
                        if let Err(e) = raft.client_write(ClusterCommand::AddNode { node: local_node.clone() }).await {
                            tracing::error!("Failed to register self via Raft: {}", e);
                        }
                        if let Err(e) = raft.client_write(ClusterCommand::SetMaster { node_id: local_id.clone() }).await {
                            tracing::error!("Failed to set master via Raft: {}", e);
                        }
                        info!("Bootstrapped as master: {}", local_id);
                    }
                }
            }

            // ── Lifecycle loop ─────────────────────────────────────────
            let mut leader_since: Option<Instant> = None;

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let state = manager.get_state();

                if let Some(ref raft) = raft {
                    if raft.is_leader() {
                        // Track when we became leader for grace period
                        let now = Instant::now();
                        let became_leader_at = *leader_since.get_or_insert(now);

                        // ── Leader duties ──────────────────────────
                        // Ensure master_node is set to us
                        if state.master_node.as_deref() != Some(&local_id) {
                            if let Err(e) = raft.client_write(ClusterCommand::SetMaster {
                                node_id: local_id.clone(),
                            }).await {
                                tracing::warn!("Failed to set master: {}", e);
                            }
                        }

                        // Grace period: don't scan for dead nodes until followers
                        // have had time to discover and ping the new leader.
                        let leader_age = now.duration_since(became_leader_at);
                        let mut dead_nodes = Vec::new();

                        if leader_age > Duration::from_secs(20) {
                            for (node_id, last_seen) in &state.last_seen {
                                if node_id != &local_id && now.duration_since(*last_seen) > Duration::from_secs(15) {
                                    dead_nodes.push(node_id.clone());
                                }
                            }
                        }

                        for dead in &dead_nodes {
                            tracing::warn!("Node {} has died. Removing via Raft.", dead);

                            // Remove from Raft membership first
                            if let Some(dead_info) = state.nodes.get(dead) {
                                if dead_info.raft_node_id > 0 {
                                    let remaining: std::collections::BTreeSet<u64> = raft.voter_ids()
                                        .filter(|id| *id != dead_info.raft_node_id)
                                        .collect();
                                    if !remaining.is_empty() {
                                        if let Err(e) = raft.change_membership(remaining, false).await {
                                            tracing::error!("Failed to remove {} from Raft: {}", dead, e);
                                        }
                                    }
                                }
                            }

                            // Then remove from cluster state via Raft
                            if let Err(e) = raft.client_write(ClusterCommand::RemoveNode {
                                node_id: dead.clone(),
                            }).await {
                                tracing::error!("Failed to remove dead node {} via Raft: {}", dead, e);
                            }
                        }
                    } else {
                        // Reset leader_since when we're not leader
                        leader_since = None;

                        // ── Follower duties ────────────────────────
                        // Ping the master for health monitoring
                        if let Some(master_id) = &state.master_node {
                            if let Some(master_info) = state.nodes.get(master_id) {
                                if let Err(e) = client.send_ping(master_info, &local_id).await {
                                    tracing::warn!("Failed to ping master {}: {}", master_id, e);
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
