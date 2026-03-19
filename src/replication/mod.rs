//! Primary-replica replication logic.
//!
//! After a primary shard writes to its local WAL + engine, it replicates
//! the operation to all replica shards via gRPC. Replication is synchronous
//! (write is only acknowledged after all in-sync replicas confirm).

use crate::cluster::state::ClusterState;
use crate::transport::TransportClient;
use tracing::error;

/// Replicate a single document write to all replica nodes for a shard.
/// Returns Ok(replica_checkpoints) if all replicas acknowledged, Err with details otherwise.
/// The returned Vec contains (node_id, local_checkpoint) for each replica.
pub async fn replicate_write(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    payload: &serde_json::Value,
    op: &str,
    seq_no: u64,
) -> Result<Vec<(String, u64)>, Vec<String>> {
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m,
        None => return Ok(vec![]), // no index metadata, nothing to replicate
    };

    let replica_node_ids = metadata.replica_nodes(shard_id);
    if replica_node_ids.is_empty() {
        return Ok(vec![]);
    }

    let mut errors = Vec::new();
    let mut checkpoints = Vec::new();

    for replica_node_id in &replica_node_ids {
        let node_info = match cluster_state.nodes.get(*replica_node_id) {
            Some(n) => n,
            None => {
                errors.push(format!(
                    "Replica node {} not in cluster state",
                    replica_node_id
                ));
                continue;
            }
        };

        match transport_client
            .replicate_to_shard(node_info, index_name, shard_id, doc_id, payload, op, seq_no)
            .await
        {
            Ok(replica_checkpoint) => {
                checkpoints.push((replica_node_id.to_string(), replica_checkpoint));
            }
            Err(e) => {
                error!(
                    "Replication to {} for {}/shard_{} failed: {}",
                    replica_node_id, index_name, shard_id, e
                );
                errors.push(format!("{}: {}", replica_node_id, e));
            }
        }
    }

    if errors.is_empty() {
        Ok(checkpoints)
    } else {
        Err(errors)
    }
}

/// Replicate a bulk set of writes to all replica nodes for a shard.
/// Returns Ok(replica_checkpoints) with (node_id, local_checkpoint) for each replica.
pub async fn replicate_bulk(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    docs: &[(String, serde_json::Value)],
    start_seq_no: u64,
) -> Result<Vec<(String, u64)>, Vec<String>> {
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m,
        None => return Ok(vec![]),
    };

    let replica_node_ids = metadata.replica_nodes(shard_id);
    if replica_node_ids.is_empty() {
        return Ok(vec![]);
    }

    let mut errors = Vec::new();
    let mut checkpoints = Vec::new();

    for replica_node_id in &replica_node_ids {
        let node_info = match cluster_state.nodes.get(*replica_node_id) {
            Some(n) => n,
            None => {
                errors.push(format!(
                    "Replica node {} not in cluster state",
                    replica_node_id
                ));
                continue;
            }
        };

        match transport_client
            .replicate_bulk_to_shard(node_info, index_name, shard_id, docs, start_seq_no)
            .await
        {
            Ok(replica_checkpoint) => {
                checkpoints.push((replica_node_id.to_string(), replica_checkpoint));
            }
            Err(e) => {
                error!(
                    "Bulk replication to {} for {}/shard_{} failed: {}",
                    replica_node_id, index_name, shard_id, e
                );
                errors.push(format!("{}: {}", replica_node_id, e));
            }
        }
    }

    if errors.is_empty() {
        Ok(checkpoints)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::*;
    use std::collections::HashMap;

    fn make_cluster_state_with_nodes() -> ClusterState {
        let mut cs = ClusterState::new("test-cluster".into());
        cs.add_node(NodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 19300,
            http_port: 19200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        cs.add_node(NodeInfo {
            id: "node-2".into(),
            name: "node-2".into(),
            host: "127.0.0.1".into(),
            transport_port: 19301,
            http_port: 19201,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        cs
    }

    fn add_index_with_routing(cs: &mut ClusterState, name: &str, replicas: Vec<String>) {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas,
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: name.into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
    }

    // ── replicate_write ─────────────────────────────────────────────────

    #[tokio::test]
    async fn write_noop_when_index_missing() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let result = replicate_write(
            &client,
            &cs,
            "nonexistent",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_noop_when_no_replicas() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec![]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_noop_when_shard_not_in_routing() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        // Shard 99 doesn't exist in routing table → no replicas → Ok
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            99,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn write_errors_when_replica_node_not_in_cluster_state() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["ghost-node".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("ghost-node"));
    }

    #[tokio::test]
    async fn write_errors_when_replica_node_unreachable() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        // node-2 is in cluster state but no gRPC server running → connection refused
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("node-2"));
    }

    #[tokio::test]
    async fn write_collects_multiple_errors() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        // Both replicas will fail: one not in state, one unreachable
        add_index_with_routing(&mut cs, "test-idx", vec!["ghost".into(), "node-2".into()]);
        let result = replicate_write(
            &client,
            &cs,
            "test-idx",
            0,
            "doc1",
            &serde_json::json!({"field": "value"}),
            "index",
            0,
        )
        .await;
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 2);
    }

    // ── replicate_bulk ──────────────────────────────────────────────────

    #[tokio::test]
    async fn bulk_noop_when_index_missing() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let result = replicate_bulk(&client, &cs, "nonexistent", 0, &docs, 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bulk_noop_when_no_replicas() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec![]);
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bulk_errors_when_replica_node_not_in_cluster_state() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["phantom".into()]);
        let docs = vec![
            ("d1".into(), serde_json::json!({"a": 1})),
            ("d2".into(), serde_json::json!({"b": 2})),
        ];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0).await;
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("phantom"));
    }

    #[tokio::test]
    async fn bulk_errors_when_replica_unreachable() {
        let client = TransportClient::new();
        let mut cs = make_cluster_state_with_nodes();
        add_index_with_routing(&mut cs, "test-idx", vec!["node-2".into()]);
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let result = replicate_bulk(&client, &cs, "test-idx", 0, &docs, 0).await;
        assert!(result.is_err());
    }

    // ── Return type: checkpoints ────────────────────────────────────────

    #[tokio::test]
    async fn write_noop_returns_empty_checkpoints() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let checkpoints = replicate_write(
            &client,
            &cs,
            "nonexistent",
            0,
            "doc1",
            &serde_json::json!({"f": 1}),
            "index",
            0,
        )
        .await
        .unwrap();
        assert!(checkpoints.is_empty(), "no replicas → empty checkpoints");
    }

    #[tokio::test]
    async fn bulk_noop_returns_empty_checkpoints() {
        let client = TransportClient::new();
        let cs = make_cluster_state_with_nodes();
        let docs = vec![("d1".into(), serde_json::json!({"a": 1}))];
        let checkpoints = replicate_bulk(&client, &cs, "nonexistent", 0, &docs, 0)
            .await
            .unwrap();
        assert!(checkpoints.is_empty());
    }
}
