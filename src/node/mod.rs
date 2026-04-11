//! Node lifecycle management.
//! Manages node lifecycle including startup, shutdown, and Raft consensus.

mod lifecycle;
mod reconciliation;

#[cfg(test)]
mod tests;

use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{NodeInfo, NodeRole};
use crate::config::AppConfig;
use crate::consensus::types::{ClusterCommand, RaftInstance};
use crate::shard::ShardManager;
use crate::transport::client::TransportClient;
use crate::wal::TranslogDurability;

use lifecycle::{apply_recovery_ops, remote_seed_hosts, try_join_cluster};
use reconciliation::{
    build_guarded_startup_shards, cleanup_orphaned_data_if_authoritative_blocking,
    open_local_assigned_shards_blocking, should_retry_cluster_join, snapshot_uuid_dirs,
};

#[cfg(test)]
use reconciliation::{
    cleanup_orphaned_data_if_authoritative, collect_guarded_startup_shards,
    open_local_assigned_shards,
};

#[cfg(feature = "transport-tls")]
use anyhow::Context;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

pub struct Node {
    pub config: AppConfig,
    pub cluster_manager: Arc<ClusterManager>,
    pub transport_client: TransportClient,
    pub shard_manager: Arc<ShardManager>,
    pub raft: Arc<RaftInstance>,
    pub task_manager: Arc<crate::tasks::TaskManager>,
}

#[derive(Debug)]
#[cfg_attr(not(feature = "transport-tls"), allow(dead_code))]
struct TransportTlsPaths<'a> {
    ca: &'a str,
    cert: &'a str,
    key: &'a str,
}

fn required_transport_tls_path<'a>(
    field_name: &'static str,
    value: Option<&'a str>,
) -> anyhow::Result<&'a str> {
    value.filter(|path| !path.trim().is_empty()).ok_or_else(|| {
        anyhow::anyhow!("{} is required when transport_tls_enabled=true", field_name)
    })
}

fn resolve_transport_tls_paths(
    config: &AppConfig,
) -> anyhow::Result<Option<TransportTlsPaths<'_>>> {
    if !config.transport_tls_enabled {
        return Ok(None);
    }

    let paths = TransportTlsPaths {
        ca: required_transport_tls_path(
            "transport_tls_ca_file",
            config.transport_tls_ca_file.as_deref(),
        )?,
        cert: required_transport_tls_path(
            "transport_tls_cert_file",
            config.transport_tls_cert_file.as_deref(),
        )?,
        key: required_transport_tls_path(
            "transport_tls_key_file",
            config.transport_tls_key_file.as_deref(),
        )?,
    };

    if !cfg!(feature = "transport-tls") {
        anyhow::bail!("transport_tls_enabled=true requires building with --features transport-tls");
    }

    Ok(Some(paths))
}

fn ping_rejection_requires_rejoin(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == tonic::Code::NotFound)
}

fn follower_join_retry_remaining(
    last_attempt: Option<Instant>,
    now: Instant,
    min_interval: Duration,
) -> Option<Duration> {
    let last_attempt = last_attempt?;
    let elapsed = now.saturating_duration_since(last_attempt);
    if elapsed < min_interval {
        Some(min_interval - elapsed)
    } else {
        None
    }
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
        let transport_tls = resolve_transport_tls_paths(&config)?;

        // Create transport client — with TLS when feature is enabled and configured
        #[cfg(feature = "transport-tls")]
        let transport_client = if let Some(tls) = transport_tls.as_ref() {
            let connector = crate::transport::TonicTlsConnector::from_ca_file(tls.ca)?;
            TransportClient::with_tls_connector(std::sync::Arc::new(connector))
        } else {
            TransportClient::new()
        };
        #[cfg(not(feature = "transport-tls"))]
        let transport_client = {
            let _ = &transport_tls;
            TransportClient::new()
        };

        let durability = match config.translog_durability.as_str() {
            "async" => TranslogDurability::Async {
                sync_interval_ms: config.translog_sync_interval_ms.unwrap_or(5000),
            },
            _ => TranslogDurability::Request,
        };
        let shard_manager = Arc::new(ShardManager::new_full(
            &config.data_dir,
            durability,
            Arc::new(crate::engine::column_cache::ColumnCache::new(
                crate::engine::column_cache::compute_cache_bytes(config.column_cache_size_percent),
                config.column_cache_populate_threshold,
            )),
        ));
        let task_manager = Arc::new(crate::tasks::TaskManager::new());

        Ok(Self {
            config,
            cluster_manager,
            transport_client,
            shard_manager,
            raft,
            task_manager,
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
            worker_pools: crate::worker::WorkerPools::default_for_system(),
            task_manager: self.task_manager.clone(),
            sql_group_by_scan_limit: self.config.sql_group_by_scan_limit,
            sql_approximate_top_k: self.config.sql_approximate_top_k,
        };

        // 1. Start internal gRPC Transport Server (Port 9300)
        let transport_service = crate::transport::server::create_transport_service_with_raft(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
            self.transport_client.clone(),
            self.raft.clone(),
            self.task_manager.clone(),
            self.config.node_name.clone(),
        );
        let transport_addr = SocketAddr::from(([0, 0, 0, 0], self.config.transport_port));
        info!("gRPC Transport listening on {}", transport_addr);
        let transport_tls = resolve_transport_tls_paths(&self.config)?;
        let mut transport_builder = tonic::transport::Server::builder();

        #[cfg(feature = "transport-tls")]
        if let Some(tls) = transport_tls.as_ref() {
            let tls_config = crate::transport::load_server_tls_config(tls.cert, tls.key)?;
            transport_builder = transport_builder
                .tls_config(tls_config)
                .context("failed to configure gRPC transport TLS")?;
            info!("gRPC transport TLS enabled");
        }

        #[cfg(not(feature = "transport-tls"))]
        let _ = &transport_tls;

        let transport_handle = tokio::spawn(async move {
            if let Err(e) = transport_builder
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
            let mut startup_state = None;
            let mut recovered_guard_state = None;

            // ── Bootstrap or join ──────────────────────────────────────
            {
                let raft = &raft;
                // If Raft is already initialized (recovered from disk), skip
                // bootstrap, but still re-register with the leader so its
                // recovered cluster state contains this node again.
                let already_initialized = raft.is_initialized().await.unwrap_or(false);

                if already_initialized {
                    recovered_guard_state = Some(manager.get_state());
                    info!("Raft already initialized (recovered from disk), rejoining cluster");
                    let joined_state = try_join_cluster(
                        &client,
                        &remote_seeds,
                        &local_node,
                        raft_node_id,
                        20,
                        Some(Arc::clone(raft)),
                    )
                    .await;
                    if joined_state.is_some() {
                        info!("Recovered node re-registered with the cluster leader");
                        startup_state = joined_state;
                    } else if raft.is_leader() {
                        // We became leader during join attempts — use the local
                        // state machine as the authoritative startup state.
                        info!("Node became Raft leader — using local state for startup");
                        startup_state = Some(manager.get_state());
                    } else if !remote_seeds.is_empty() {
                        tracing::warn!(
                            "Recovered node could not re-register via seed hosts yet; will retry in lifecycle loop"
                        );
                    }
                } else {
                    // Try to join an existing cluster with retries.
                    // Other nodes may not be ready yet — retry a few times
                    // before falling back to single-node bootstrap.
                    let joined_state = if !remote_seeds.is_empty() {
                        try_join_cluster(&client, &remote_seeds, &local_node, raft_node_id, 5, None)
                            .await
                    } else {
                        None
                    };

                    if let Some(state) = joined_state {
                        // Don't call update_state — Raft log replication will
                        // propagate the authoritative state to our state machine.
                        info!(
                            "Joined existing cluster via seed hosts (Raft membership managed by leader)"
                        );
                        startup_state = Some(state);
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
                        startup_state = Some(manager.get_state());
                    }
                }
            }

            let has_authoritative_startup_state = startup_state.is_some();
            let state = startup_state.clone().unwrap_or_else(|| manager.get_state());
            let guarded_missing_startup_shards =
                build_guarded_startup_shards(recovered_guard_state.as_ref(), &local_id);
            // Keep the recovered-startup guard in place even after we obtain
            // authoritative cluster state. The join/bootstrap snapshot tells us
            // where shards belong, but it does not prove the local shard data
            // still exists. New assignments learned after rejoin are not part
            // of the guard set and may still create fresh directories later.
            // Snapshot which UUID directories exist on disk BEFORE we open
            // any shards. This prevents orphan cleanup from treating
            // freshly-created (empty) shard dirs as evidence that the
            // authoritative data is present.
            let pre_existing_uuid_dirs = snapshot_uuid_dirs(manager_clone.data_dir());
            open_local_assigned_shards_blocking(
                state.clone(),
                local_id.clone(),
                manager_clone.clone(),
                guarded_missing_startup_shards.clone(),
            )
            .await;

            let mut orphan_cleanup_done = cleanup_orphaned_data_if_authoritative_blocking(
                Some(state.clone()),
                local_id.clone(),
                manager_clone.clone(),
                has_authoritative_startup_state,
                pre_existing_uuid_dirs.clone(),
            )
            .await;

            // ── Lifecycle loop ─────────────────────────────────────────
            let mut leader_since: Option<Instant> = None;
            let mut last_follower_join_retry: Option<Instant> = None;
            let follower_join_retry_min_interval = Duration::from_secs(15);

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let state = manager.get_state();
                open_local_assigned_shards_blocking(
                    state.clone(),
                    local_id.clone(),
                    manager_clone.clone(),
                    guarded_missing_startup_shards.clone(),
                )
                .await;

                if !orphan_cleanup_done {
                    orphan_cleanup_done = cleanup_orphaned_data_if_authoritative_blocking(
                        Some(state.clone()),
                        local_id.clone(),
                        manager_clone.clone(),
                        false,
                        pre_existing_uuid_dirs.clone(),
                    )
                    .await;
                }

                {
                    let raft = &raft;
                    if raft.is_leader() {
                        last_follower_join_retry = None;
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
                            // Re-read cluster state so shard routing mutations from
                            // previous dead-node removals are visible. Without this,
                            // multi-node death in the same tick can double-promote or
                            // miscount unassigned_replicas.
                            let fresh_state = manager.get_state();
                            tracing::warn!("Node {} has died. Removing via Raft.", dead);

                            // ── Shard failover: promote replicas for orphaned primaries ──
                            // Before removing the node, handle shard routing updates:
                            // 1. Find all indices where the dead node hosts a primary or replica
                            // 2. For orphaned primaries: pick best replica (highest checkpoint) and promote
                            // 3. For lost replicas: increment unassigned count for re-allocation
                            for idx_meta in fresh_state.indices.values() {
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
                            if let Some(dead_info) = fresh_state.nodes.get(dead)
                                && dead_info.raft_node_id > 0
                            {
                                let remaining: std::collections::BTreeSet<u64> = raft
                                    .voter_ids()
                                    .filter(|id| *id != dead_info.raft_node_id)
                                    .collect();
                                if remaining.is_empty() {
                                    tracing::error!(
                                        "Refusing to remove {} from cluster state because Raft membership would become empty",
                                        dead
                                    );
                                    continue;
                                }
                                if let Err(e) = raft.change_membership(remaining, false).await {
                                    tracing::error!(
                                        "Failed to remove {} from Raft membership; leaving node in cluster state: {}",
                                        dead,
                                        e
                                    );
                                    continue;
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
                        // Re-read state after dead-node removals so allocator sees
                        // current routing and node membership.
                        let alloc_state = if dead_nodes.is_empty() {
                            state.clone()
                        } else {
                            manager.get_state()
                        };
                        let data_nodes: Vec<String> = alloc_state
                            .nodes
                            .values()
                            .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
                            .map(|n| n.id.clone())
                            .collect();

                        for idx_meta in alloc_state.indices.values() {
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
                        // Retry join whenever the authoritative cluster state does not
                        // include us. Recovered nodes are already initialized/voters, so
                        // gating on those flags prevents the promised retry path.
                        let needs_join_retry = should_retry_cluster_join(&state, &local_id);
                        let now = Instant::now();
                        let mut joined_state = if needs_join_retry {
                            if let Some(remaining) = follower_join_retry_remaining(
                                last_follower_join_retry,
                                now,
                                follower_join_retry_min_interval,
                            ) {
                                tracing::debug!(
                                    "Local node {} is still missing from authoritative cluster state; delaying JoinCluster retry for {:?}",
                                    local_id,
                                    remaining
                                );
                                None
                            } else {
                                tracing::info!(
                                    "Local node {} missing from authoritative cluster state; retrying cluster join",
                                    local_id
                                );
                                last_follower_join_retry = Some(now);
                                try_join_cluster(
                                    &client,
                                    &remote_seeds,
                                    &local_node,
                                    raft_node_id,
                                    1,
                                    None,
                                )
                                .await
                            }
                        } else {
                            None
                        };
                        if !needs_join_retry {
                            match state.master_node.as_ref() {
                                Some(master_id) => match state.nodes.get(master_id) {
                                    Some(master_info) => {
                                        if let Err(error) =
                                            client.send_ping(master_info, &local_id).await
                                        {
                                            if ping_rejection_requires_rejoin(&error) {
                                                if let Some(remaining) =
                                                    follower_join_retry_remaining(
                                                        last_follower_join_retry,
                                                        now,
                                                        follower_join_retry_min_interval,
                                                    )
                                                {
                                                    tracing::warn!(
                                                        "Master {} no longer recognizes {}; delaying JoinCluster retry for {:?}: {}",
                                                        master_id,
                                                        local_id,
                                                        remaining,
                                                        error
                                                    );
                                                } else {
                                                    tracing::warn!(
                                                        "Master {} no longer recognizes {}; retrying cluster join: {}",
                                                        master_id,
                                                        local_id,
                                                        error
                                                    );
                                                    last_follower_join_retry = Some(now);
                                                    joined_state = try_join_cluster(
                                                        &client,
                                                        &remote_seeds,
                                                        &local_node,
                                                        raft_node_id,
                                                        1,
                                                        None,
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                tracing::warn!(
                                                    "Failed to ping master {}: {}",
                                                    master_id,
                                                    error
                                                );
                                            }
                                        } else {
                                            last_follower_join_retry = None;
                                        }
                                    }
                                    None => {
                                        tracing::debug!(
                                            "Master {} missing from local cluster state; skipping ping until state catches up",
                                            master_id
                                        );
                                    }
                                },
                                None => {
                                    tracing::debug!(
                                        "No master recorded in local cluster state; skipping master ping until leadership is known"
                                    );
                                }
                            }
                        }
                        if let Some(joined_state) = joined_state {
                            last_follower_join_retry = None;
                            tracing::info!(
                                "Recovered follower {} re-registered with leader",
                                local_id
                            );
                            // Snapshot UUID dirs before opening shards to prevent
                            // cleanup from deleting old data based on freshly-created dirs.
                            let join_pre_existing = snapshot_uuid_dirs(manager_clone.data_dir());
                            open_local_assigned_shards_blocking(
                                joined_state.clone(),
                                local_id.clone(),
                                manager_clone.clone(),
                                guarded_missing_startup_shards.clone(),
                            )
                            .await;
                            if !orphan_cleanup_done {
                                orphan_cleanup_done =
                                    cleanup_orphaned_data_if_authoritative_blocking(
                                        Some(joined_state.clone()),
                                        local_id.clone(),
                                        manager_clone.clone(),
                                        true,
                                        join_pre_existing,
                                    )
                                    .await;
                            }
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
