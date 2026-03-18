//! Raft-based distributed consensus for cluster state management.
//!
//! Uses [openraft](https://docs.rs/openraft) to provide leader election, log
//! replication, and linearisable cluster state updates.

pub mod disk_store;
pub mod network;
pub mod state_machine;
pub mod store;
pub mod types;

pub use types::TypeConfig;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use openraft::{BasicNode, Config};

use crate::cluster::state::ClusterState;
use disk_store::DiskLogStore;
use state_machine::ClusterStateMachine;
use store::MemLogStore;
use types::RaftInstance;

/// Create a new Raft instance and return it together with a shared handle to
/// the cluster state managed by the Raft state machine.
///
/// The returned `Arc<RwLock<ClusterState>>` is the *same* state the state
/// machine writes to when entries are applied — pass it to
/// `ClusterManager::with_shared_state` so the rest of the app reads
/// consistent state.
/// Create a Raft instance with persistent disk-backed log storage.
///
/// `data_dir` should be the node's data directory — a `raft.db` file will be
/// created inside it.
pub async fn create_raft_instance(
    node_id: u64,
    cluster_name: String,
    data_dir: &str,
) -> anyhow::Result<(Arc<RaftInstance>, Arc<RwLock<ClusterState>>)> {
    let config = Config {
        cluster_name: cluster_name.clone(),
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let state_machine = ClusterStateMachine::new(cluster_name);
    let state_handle = state_machine.state_handle();

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(data_dir)?;
    let db_path = std::path::Path::new(data_dir).join("raft.db");
    let log_store = DiskLogStore::open(&db_path)?;

    let network = network::RaftNetworkFactoryImpl;

    let raft = openraft::Raft::new(node_id, Arc::new(config), network, log_store, state_machine)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Raft instance: {}", e))?;

    Ok((Arc::new(raft), state_handle))
}

/// Create a Raft instance with in-memory storage (for tests).
pub async fn create_raft_instance_mem(
    node_id: u64,
    cluster_name: String,
) -> anyhow::Result<(Arc<RaftInstance>, Arc<RwLock<ClusterState>>)> {
    let config = Config {
        cluster_name: cluster_name.clone(),
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let state_machine = ClusterStateMachine::new(cluster_name);
    let state_handle = state_machine.state_handle();
    let log_store = MemLogStore::new();
    let network = network::RaftNetworkFactoryImpl;

    let raft = openraft::Raft::new(node_id, Arc::new(config), network, log_store, state_machine)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Raft instance: {}", e))?;

    Ok((Arc::new(raft), state_handle))
}

/// Bootstrap a single-node Raft cluster (first node only).
pub async fn bootstrap_single_node(
    raft: &RaftInstance,
    node_id: u64,
    addr: String,
) -> anyhow::Result<()> {
    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode { addr });
    raft.initialize(members)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bootstrap Raft cluster: {}", e))?;
    Ok(())
}
