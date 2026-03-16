//! Node lifecycle management.
//! Long-term goal: Manages node lifecycle including startup and shutdown.

use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{NodeInfo, NodeRole};
use crate::config::AppConfig;
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
}

fn elect_master(manager: &Arc<ClusterManager>, _local_id: &str) {
    let mut state = manager.get_state();
    let mut eligible_masters: Vec<_> = state.nodes.values()
        .filter(|n| n.roles.contains(&NodeRole::Master))
        .map(|n| n.id.clone())
        .collect();
    eligible_masters.sort();
    if let Some(new_master) = eligible_masters.first() {
        tracing::info!("Elected new master: {}", new_master);
        state.master_node = Some(new_master.clone());
        manager.update_state(state);
    } else {
        tracing::error!("No eligible master nodes available in the cluster!");
    }
}

impl Node {
    pub fn new(config: AppConfig) -> anyhow::Result<Self> {
        let cluster_manager = Arc::new(ClusterManager::new(config.cluster_name.clone()));
        let transport_client = TransportClient::new();
        // ShardManager owns all per-shard SearchEngine instances; no engines created yet
        // (engines are created on-demand when indices are created or documents arrive)
        let shard_manager = Arc::new(ShardManager::new(&config.data_dir, Duration::from_secs(5)));
        Ok(Self {
            config,
            cluster_manager,
            transport_client,
            shard_manager,
        })
    }

    /// Starts the node, including all subsystems (HTTP, Transport, Cluster, Engine)
    pub async fn start(&self) -> anyhow::Result<()> {
        info!(
            "Starting ROpenSearch Node: {} (Cluster: {})",
            self.config.node_name, self.config.cluster_name
        );
        info!("Data directory: {}", self.config.data_dir);

        // Register self in the local cluster state
        let local_node = NodeInfo {
            id: self.config.node_name.clone(),
            name: self.config.node_name.clone(),
            host: "127.0.0.1".into(),
            transport_port: self.config.transport_port,
            http_port: self.config.http_port,
            roles: vec![NodeRole::Master, NodeRole::Data],
        };
        self.cluster_manager.add_node(local_node.clone());

        let app_state = crate::api::AppState {
            cluster_manager: self.cluster_manager.clone(),
            shard_manager: self.shard_manager.clone(),
            transport_client: self.transport_client.clone(),
            local_node_id: self.config.node_name.clone(),
        };


        // 1. Start internal gRPC Transport Server (Port 9300)
        let transport_service = crate::transport::server::create_transport_service(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
        );
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

        // 3. Cluster Discovery & Lifecycle Loop
        let client = self.transport_client.clone();
        let seed_hosts = self.config.seed_hosts.clone();
        let manager = self.cluster_manager.clone();
        let local_id = self.config.node_name.clone();

        tokio::spawn(async move {
            // Give servers a tiny moment to bind
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Try to join the cluster
            if let Some(state) = client.join_cluster(&seed_hosts, &local_node).await {
                manager.update_state(state);
            } else {
                info!("Could not discover cluster, acting as master of our own.");
                elect_master(&manager, &local_id);
            }

            // Begin continuous lifecycle loop
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let state = manager.get_state();

                if let Some(master_id) = &state.master_node {
                    if master_id == &local_id {
                        // We are the master! Scan for dead nodes.
                        let now = Instant::now();
                        let mut dead_nodes = Vec::new();

                        for (node_id, last_seen) in &state.last_seen {
                            if node_id != &local_id && now.duration_since(*last_seen) > Duration::from_secs(15) {
                                dead_nodes.push(node_id.clone());
                            }
                        }

                        if !dead_nodes.is_empty() {
                            let mut new_state = state.clone();
                            for dead in dead_nodes {
                                tracing::warn!("Node {} has died. Removing from ClusterState.", dead);
                                new_state.remove_node(&dead);
                            }
                            manager.update_state(new_state.clone());
                            // Broadcast the trimmed state
                            client.publish_state(&new_state).await;
                        }
                    } else if let Some(master_info) = state.nodes.get(master_id) {
                        // We are not the master, so we must ping the master.
                        if let Err(e) = client.send_ping(master_info, &local_id).await {
                            tracing::warn!("Failed to ping master {}: {}", master_id, e);
                            // Master might be dead, trigger election next cycle by unsetting it locally
                            let mut new_state = state.clone();
                            new_state.master_node = None;
                            manager.update_state(new_state);
                        }
                    } else {
                        // Master ID is set but node info is missing? Election time.
                        elect_master(&manager, &local_id);
                    }
                } else {
                     // No master? Election time.
                     elect_master(&manager, &local_id);

                     // If we became master after election, publish state.
                     let new_state = manager.get_state();
                     if new_state.master_node.as_ref() == Some(&local_id) {
                         client.publish_state(&new_state).await;
                     }
                }
            }
        });

        let _ = tokio::try_join!(http_handle, transport_handle)?;

        Ok(())
    }
}
