//! Cluster management.
//! Long-term goal: Handles node discovery, membership, and cluster state via gossip or consensus.

pub mod manager;
pub mod manifest_writer;
pub mod settings;
pub mod state;
pub mod write_authority;

pub use manager::ClusterManager;
