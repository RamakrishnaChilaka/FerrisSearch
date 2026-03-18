//! Integration tests for the Raft consensus module.
//!
//! These tests spin up real Raft instances (single-node) and exercise the full
//! flow: bootstrap → leader election → client_write → state machine apply.

use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{
    IndexMetadata, NodeInfo, NodeRole, ShardRoutingEntry,
};
use ferrissearch::consensus;
use ferrissearch::consensus::types::{ClusterCommand, ClusterResponse};

use std::collections::HashMap;
use std::sync::Arc;

// ─── Helpers ────────────────────────────────────────────────────────────────

fn make_node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        name: id.into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Master, NodeRole::Data],
        raft_node_id: 0,
    }
}

fn make_index(name: &str) -> IndexMetadata {
    let mut shard_routing = HashMap::new();
    shard_routing.insert(0, ShardRoutingEntry {
        primary: "node-1".into(),
        replicas: vec![],
        unassigned_replicas: 0,
    });
    IndexMetadata {
        name: name.into(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: std::collections::HashMap::new(),
    }
}

/// Wait for the single-node Raft to become leader (should be near-instant).
async fn wait_for_leader(raft: &consensus::types::RaftInstance) {
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("Raft node did not become leader within 5 seconds");
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_node_bootstrap_and_leader_election() {
    let (raft, _state) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .expect("create_raft_instance should succeed");

    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
        .await
        .expect("bootstrap should succeed");

    wait_for_leader(&raft).await;

    let leader = raft.current_leader().await;
    assert_eq!(leader, Some(1), "node 1 should be the leader");
}

#[tokio::test]
async fn double_bootstrap_fails() {
    let (raft, _state) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();

    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19301".into())
        .await
        .unwrap();

    // Second bootstrap should fail (already initialized)
    let result = consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19301".into()).await;
    assert!(result.is_err(), "double bootstrap should fail");
}

#[tokio::test]
async fn client_write_add_node() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19302".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let cmd = ClusterCommand::AddNode { node: make_node("data-node-1") };
    let resp = raft.client_write(cmd).await.expect("client_write should succeed");

    assert!(matches!(resp.data, ClusterResponse::Ok));
    assert!(resp.log_id.index > 0, "applied log should have index > 0");

    // Verify state machine was updated
    let state = state_handle.read().unwrap();
    assert!(state.nodes.contains_key("data-node-1"));
}

#[tokio::test]
async fn client_write_remove_node() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19303".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Add then remove
    raft.client_write(ClusterCommand::AddNode { node: make_node("ephemeral") })
        .await
        .unwrap();
    {
        let state = state_handle.read().unwrap();
        assert!(state.nodes.contains_key("ephemeral"));
    }

    raft.client_write(ClusterCommand::RemoveNode { node_id: "ephemeral".into() })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert!(!state.nodes.contains_key("ephemeral"));
}

#[tokio::test]
async fn client_write_create_and_delete_index() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19304".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create index
    let idx = make_index("products");
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    {
        let state = state_handle.read().unwrap();
        assert!(state.indices.contains_key("products"));
        assert_eq!(state.indices["products"].number_of_shards, 1);
    }

    // Delete index
    raft.client_write(ClusterCommand::DeleteIndex { index_name: "products".into() })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert!(!state.indices.contains_key("products"));
}

#[tokio::test]
async fn multiple_writes_are_ordered() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19305".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Write multiple nodes sequentially; log indices should be monotonically increasing
    let mut last_index = 0;
    for i in 0..5 {
        let cmd = ClusterCommand::AddNode {
            node: make_node(&format!("node-{}", i)),
        };
        let resp = raft.client_write(cmd).await.unwrap();
        assert!(resp.log_id.index > last_index);
        last_index = resp.log_id.index;
    }

    let state = state_handle.read().unwrap();
    assert_eq!(state.nodes.len(), 5);
}

#[tokio::test]
async fn cluster_manager_reads_raft_state() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19306".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // ClusterManager wraps the same state_handle
    let cm = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));

    // Write via Raft
    raft.client_write(ClusterCommand::AddNode { node: make_node("raft-node") })
        .await
        .unwrap();

    // Read via ClusterManager
    let state = cm.get_state();
    assert!(state.nodes.contains_key("raft-node"));
    assert!(state.version > 0);
}

#[tokio::test]
async fn state_version_increments_on_writes() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19307".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let v0 = state_handle.read().unwrap().version;

    raft.client_write(ClusterCommand::AddNode { node: make_node("v1") })
        .await
        .unwrap();
    let v1 = state_handle.read().unwrap().version;
    assert!(v1 > v0, "version should increase after AddNode");

    raft.client_write(ClusterCommand::CreateIndex { metadata: make_index("idx") })
        .await
        .unwrap();
    let v2 = state_handle.read().unwrap().version;
    assert!(v2 > v1, "version should increase after CreateIndex");

    raft.client_write(ClusterCommand::DeleteIndex { index_name: "idx".into() })
        .await
        .unwrap();
    let v3 = state_handle.read().unwrap().version;
    assert!(v3 > v2, "version should increase after DeleteIndex");
}

#[tokio::test]
async fn is_leader_after_bootstrap() {
    let (raft, _state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19308".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    assert!(raft.is_leader(), "single-node cluster should report is_leader = true");
}

#[tokio::test]
async fn create_multiple_indices() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19309".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    for i in 0..3 {
        let idx = make_index(&format!("index-{}", i));
        raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
            .await
            .unwrap();
    }

    let state = state_handle.read().unwrap();
    assert_eq!(state.indices.len(), 3);
    assert!(state.indices.contains_key("index-0"));
    assert!(state.indices.contains_key("index-1"));
    assert!(state.indices.contains_key("index-2"));
}

#[tokio::test]
async fn write_after_remove_node() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19310".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Add two nodes, remove one, add another
    raft.client_write(ClusterCommand::AddNode { node: make_node("a") }).await.unwrap();
    raft.client_write(ClusterCommand::AddNode { node: make_node("b") }).await.unwrap();
    raft.client_write(ClusterCommand::RemoveNode { node_id: "a".into() }).await.unwrap();
    raft.client_write(ClusterCommand::AddNode { node: make_node("c") }).await.unwrap();

    let state = state_handle.read().unwrap();
    assert!(!state.nodes.contains_key("a"));
    assert!(state.nodes.contains_key("b"));
    assert!(state.nodes.contains_key("c"));
    assert_eq!(state.nodes.len(), 2);
}

#[tokio::test]
async fn client_write_set_master() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19311".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Initially no master
    {
        let state = state_handle.read().unwrap();
        assert!(state.master_node.is_none());
    }

    // Set master via Raft
    raft.client_write(ClusterCommand::SetMaster { node_id: "node-1".into() })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(state.master_node.as_deref(), Some("node-1"));
}

#[tokio::test]
async fn raft_node_id_preserved_through_add_node() {
    // Regression: raft_node_id must survive the Raft SM apply roundtrip
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19312".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let mut node = make_node("data-1");
    node.raft_node_id = 42;

    raft.client_write(ClusterCommand::AddNode { node }).await.unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(
        state.nodes["data-1"].raft_node_id, 42,
        "raft_node_id must be preserved through Raft apply"
    );
}

#[tokio::test]
async fn set_master_then_remove_master_node_clears_master() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19313".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    raft.client_write(ClusterCommand::AddNode { node: make_node("m") }).await.unwrap();
    raft.client_write(ClusterCommand::SetMaster { node_id: "m".into() }).await.unwrap();
    {
        let state = state_handle.read().unwrap();
        assert_eq!(state.master_node.as_deref(), Some("m"));
    }

    raft.client_write(ClusterCommand::RemoveNode { node_id: "m".into() }).await.unwrap();

    let state = state_handle.read().unwrap();
    assert!(state.master_node.is_none(), "removing master node should clear master_node");
}

#[tokio::test]
async fn transfer_leader_to_self_succeeds() {
    // In a single-node cluster, transferring to self should succeed (no-op).
    let (raft, _state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19314".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Get current vote from metrics
    let vote = {
        use openraft::type_config::async_runtime::WatchReceiver;
        let m = raft.metrics();
        m.borrow_watched().vote.clone()
    };

    let last_log_id = {
        use openraft::type_config::async_runtime::WatchReceiver;
        let m = raft.metrics();
        m.borrow_watched().last_applied
    };

    let req = openraft::raft::TransferLeaderRequest::new(vote, 1u64, last_log_id);

    // Should not return an error
    raft.handle_transfer_leader(req)
        .await
        .expect("transfer_leader to self should succeed in single-node cluster");

    // Node should still be leader
    assert!(raft.is_leader(), "node should remain leader after self-transfer");
}

// ─── Shard allocator integration tests ──────────────────────────────────────

#[tokio::test]
async fn update_index_via_raft_assigns_replicas() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "alloc-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19310".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create index with 1 shard, 2 replicas on 1 node → 2 unassigned
    let idx = IndexMetadata::build_shard_routing("movies", 1, 2, &["node-1".into()]);
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    {
        let state = state_handle.read().unwrap();
        assert_eq!(state.indices["movies"].unassigned_replica_count(), 2);
    }

    // Simulate allocator: assign replicas to new nodes
    let mut updated = {
        let state = state_handle.read().unwrap();
        state.indices["movies"].clone()
    };
    updated.allocate_unassigned_replicas(&["node-1".into(), "node-2".into(), "node-3".into()]);
    assert_eq!(updated.unassigned_replica_count(), 0);

    // Write updated routing via Raft
    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    // Verify via Raft state
    let state = state_handle.read().unwrap();
    let routing = &state.indices["movies"].shard_routing[&0];
    assert_eq!(routing.replicas.len(), 2, "both replicas should be assigned");
    assert_eq!(routing.unassigned_replicas, 0);
    assert!(routing.replicas.contains(&"node-2".to_string()));
    assert!(routing.replicas.contains(&"node-3".to_string()));
}

#[tokio::test]
async fn update_index_partial_allocation_two_nodes() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "partial-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19311".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create with 1 shard, 2 replicas, 1 node → 2 unassigned
    let idx = IndexMetadata::build_shard_routing("products", 1, 2, &["node-1".into()]);
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    // Only 2 nodes available → should assign 1 replica, 1 still unassigned
    let mut updated = {
        let state = state_handle.read().unwrap();
        state.indices["products"].clone()
    };
    updated.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]);

    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    let routing = &state.indices["products"].shard_routing[&0];
    assert_eq!(routing.replicas.len(), 1, "1 replica assigned to node-2");
    assert_eq!(routing.unassigned_replicas, 1, "1 still unassigned (need 3rd node)");
    assert_eq!(routing.replicas[0], "node-2");
}

#[tokio::test]
async fn create_index_on_three_nodes_all_replicas_assigned() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "three-node-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19312".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create index with 3 nodes available → all replicas assigned immediately
    let idx = IndexMetadata::build_shard_routing("logs", 1, 2, &["n1".into(), "n2".into(), "n3".into()]);
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(state.indices["logs"].unassigned_replica_count(), 0, "all replicas assigned on creation");
    assert_eq!(state.indices["logs"].shard_routing[&0].replicas.len(), 2);
}
