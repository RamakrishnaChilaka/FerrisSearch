//! Integration tests for the Raft consensus module.
//!
//! These tests spin up real Raft instances (single-node) and exercise the full
//! flow: bootstrap → leader election → client_write → state machine apply.

use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{IndexMetadata, NodeInfo, NodeRole, ShardRoutingEntry};
use ferrissearch::consensus;
use ferrissearch::consensus::types::{ClusterCommand, ClusterResponse};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::server::create_transport_service_with_raft;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    IndexMetadata {
        name: name.into(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: std::collections::HashMap::new(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
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

    let cmd = ClusterCommand::AddNode {
        node: make_node("data-node-1"),
    };
    let resp = raft
        .client_write(cmd)
        .await
        .expect("client_write should succeed");

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
    raft.client_write(ClusterCommand::AddNode {
        node: make_node("ephemeral"),
    })
    .await
    .unwrap();
    {
        let state = state_handle.read().unwrap();
        assert!(state.nodes.contains_key("ephemeral"));
    }

    raft.client_write(ClusterCommand::RemoveNode {
        node_id: "ephemeral".into(),
    })
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
    raft.client_write(ClusterCommand::DeleteIndex {
        index_name: "products".into(),
    })
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
    raft.client_write(ClusterCommand::AddNode {
        node: make_node("raft-node"),
    })
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

    raft.client_write(ClusterCommand::AddNode {
        node: make_node("v1"),
    })
    .await
    .unwrap();
    let v1 = state_handle.read().unwrap().version;
    assert!(v1 > v0, "version should increase after AddNode");

    raft.client_write(ClusterCommand::CreateIndex {
        metadata: make_index("idx"),
    })
    .await
    .unwrap();
    let v2 = state_handle.read().unwrap().version;
    assert!(v2 > v1, "version should increase after CreateIndex");

    raft.client_write(ClusterCommand::DeleteIndex {
        index_name: "idx".into(),
    })
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

    assert!(
        raft.is_leader(),
        "single-node cluster should report is_leader = true"
    );
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
    raft.client_write(ClusterCommand::AddNode {
        node: make_node("a"),
    })
    .await
    .unwrap();
    raft.client_write(ClusterCommand::AddNode {
        node: make_node("b"),
    })
    .await
    .unwrap();
    raft.client_write(ClusterCommand::RemoveNode {
        node_id: "a".into(),
    })
    .await
    .unwrap();
    raft.client_write(ClusterCommand::AddNode {
        node: make_node("c"),
    })
    .await
    .unwrap();

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
    raft.client_write(ClusterCommand::SetMaster {
        node_id: "node-1".into(),
    })
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

    raft.client_write(ClusterCommand::AddNode { node })
        .await
        .unwrap();

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

    raft.client_write(ClusterCommand::AddNode {
        node: make_node("m"),
    })
    .await
    .unwrap();
    raft.client_write(ClusterCommand::SetMaster {
        node_id: "m".into(),
    })
    .await
    .unwrap();
    {
        let state = state_handle.read().unwrap();
        assert_eq!(state.master_node.as_deref(), Some("m"));
    }

    raft.client_write(ClusterCommand::RemoveNode {
        node_id: "m".into(),
    })
    .await
    .unwrap();

    let state = state_handle.read().unwrap();
    assert!(
        state.master_node.is_none(),
        "removing master node should clear master_node"
    );
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
        m.borrow_watched().vote
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
    assert!(
        raft.is_leader(),
        "node should remain leader after self-transfer"
    );
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
    assert_eq!(
        routing.replicas.len(),
        2,
        "both replicas should be assigned"
    );
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
    assert_eq!(
        routing.unassigned_replicas, 1,
        "1 still unassigned (need 3rd node)"
    );
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
    let idx =
        IndexMetadata::build_shard_routing("logs", 1, 2, &["n1".into(), "n2".into(), "n3".into()]);
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(
        state.indices["logs"].unassigned_replica_count(),
        0,
        "all replicas assigned on creation"
    );
    assert_eq!(state.indices["logs"].shard_routing[&0].replicas.len(), 2);
}

// ─── Replica promotion via UpdateIndex (simulates dead primary) ────────

#[tokio::test]
async fn update_index_promotes_replica_after_primary_death() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "promote-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19320".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create index with primary=node-A, replicas=[node-B, node-C]
    let idx = IndexMetadata::build_shard_routing(
        "promo",
        1,
        2,
        &["node-A".into(), "node-B".into(), "node-C".into()],
    );
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    {
        let state = state_handle.read().unwrap();
        assert_eq!(state.indices["promo"].shard_routing[&0].primary, "node-A");
    }

    // Simulate what the leader does when node-A dies:
    // 1. remove_node from index metadata → gets orphaned primaries
    // 2. promote_replica_to with chosen node
    // 3. UpdateIndex via Raft
    let mut updated = {
        let state = state_handle.read().unwrap();
        state.indices["promo"].clone()
    };
    let orphaned = updated.remove_node(&"node-A".to_string());
    assert_eq!(orphaned, vec![0], "shard 0 primary was node-A");

    // Promote node-C (simulating ISR tracker picked it as highest checkpoint)
    assert!(updated.promote_replica_to(0, "node-C"));
    // Lost replica slot → mark as unassigned
    if let Some(routing) = updated.shard_routing.get_mut(&0) {
        routing.unassigned_replicas += 1;
    }

    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(
        state.indices["promo"].shard_routing[&0].primary, "node-C",
        "node-C should be promoted to primary"
    );
    assert_eq!(
        state.indices["promo"].shard_routing[&0].replicas,
        vec!["node-B".to_string()],
        "node-B should remain as replica"
    );
    assert_eq!(
        state.indices["promo"].shard_routing[&0].unassigned_replicas, 1,
        "one replica slot lost (was node-A's promotion + dead node)"
    );
}

#[tokio::test]
async fn update_index_removes_dead_replica_and_marks_unassigned() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "replica-death".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19321".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create index: primary=node-A, replicas=[node-B]
    let idx =
        IndexMetadata::build_shard_routing("rdeath", 1, 1, &["node-A".into(), "node-B".into()]);
    raft.client_write(ClusterCommand::CreateIndex { metadata: idx })
        .await
        .unwrap();

    // Simulate node-B dying (replica only)
    let mut updated = {
        let state = state_handle.read().unwrap();
        state.indices["rdeath"].clone()
    };
    let orphaned = updated.remove_node(&"node-B".to_string());
    assert!(orphaned.is_empty(), "node-B was only a replica");

    // Mark the lost replica slot as unassigned
    if let Some(routing) = updated.shard_routing.get_mut(&0) {
        routing.unassigned_replicas += 1;
    }

    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(state.indices["rdeath"].shard_routing[&0].primary, "node-A");
    assert!(
        state.indices["rdeath"].shard_routing[&0]
            .replicas
            .is_empty(),
        "node-B should be removed"
    );
    assert_eq!(
        state.indices["rdeath"].shard_routing[&0].unassigned_replicas, 1,
        "lost replica slot marked for reallocation"
    );
}

// ─── Dynamic Settings Integration Tests ─────────────────────────────────

#[tokio::test]
async fn update_index_settings_via_raft() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19350".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create an index with default settings
    let idx = make_index("settings-test");
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: idx.clone(),
    })
    .await
    .unwrap();

    {
        let state = state_handle.read().unwrap();
        assert_eq!(
            state.indices["settings-test"].settings.refresh_interval_ms,
            None
        );
    }

    // Update settings via UpdateIndex
    let mut updated = idx.clone();
    updated.settings.refresh_interval_ms = Some(2000);
    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(
        state.indices["settings-test"].settings.refresh_interval_ms,
        Some(2000)
    );
}

#[tokio::test]
async fn update_index_settings_preserves_shard_routing() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19351".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let idx = make_index("preserve-routing");
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: idx.clone(),
    })
    .await
    .unwrap();

    // Modify settings but keep routing intact
    let mut updated = idx.clone();
    updated.settings.refresh_interval_ms = Some(8000);
    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    let meta = &state.indices["preserve-routing"];
    assert_eq!(meta.settings.refresh_interval_ms, Some(8000));
    assert_eq!(meta.number_of_shards, 1);
    assert_eq!(meta.shard_routing[&0].primary, "node-1");
}

#[tokio::test]
async fn update_index_replicas_via_raft() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19352".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let idx = make_index("replicas-test");
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: idx.clone(),
    })
    .await
    .unwrap();

    // Increase replicas from 0 to 2
    let mut updated = idx.clone();
    updated.update_number_of_replicas(2);
    updated.settings.refresh_interval_ms = Some(3000);
    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    let meta = &state.indices["replicas-test"];
    assert_eq!(meta.number_of_replicas, 2);
    assert_eq!(meta.shard_routing[&0].unassigned_replicas, 2);
    assert_eq!(meta.settings.refresh_interval_ms, Some(3000));
}

#[tokio::test]
async fn update_index_settings_reset_to_default() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "test-cluster".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19353".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Create with custom refresh
    let mut idx = make_index("reset-test");
    idx.settings.refresh_interval_ms = Some(1000);
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: idx.clone(),
    })
    .await
    .unwrap();

    {
        let state = state_handle.read().unwrap();
        assert_eq!(
            state.indices["reset-test"].settings.refresh_interval_ms,
            Some(1000)
        );
    }

    // Reset to default by setting to None
    let mut updated = idx.clone();
    updated.settings.refresh_interval_ms = None;
    raft.client_write(ClusterCommand::UpdateIndex { metadata: updated })
        .await
        .unwrap();

    let state = state_handle.read().unwrap();
    assert_eq!(
        state.indices["reset-test"].settings.refresh_interval_ms,
        None
    );
}

// ─── gRPC Coordinator Integration Tests ─────────────────────────────────

/// Start a gRPC server backed by a Raft leader.
async fn start_raft_grpc_server(
    raft: Arc<consensus::types::RaftInstance>,
    state_handle: Arc<std::sync::RwLock<ferrissearch::cluster::state::ClusterState>>,
) -> std::net::SocketAddr {
    let cm = Arc::new(ClusterManager::with_shared_state(state_handle));
    // Add a data node so create_index has nodes to assign shards to
    {
        let mut s = cm.get_state();
        s.add_node(NodeInfo {
            id: "data-1".into(),
            name: "data-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        cm.update_state(s);
    }
    let dir = tempfile::tempdir().unwrap();
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let tc = TransportClient::new();
    let service = create_transport_service_with_raft(cm, sm, tc, raft);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
        // Keep dir alive so shard data isn't deleted
        let _ = dir;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect_grpc(
    addr: std::net::SocketAddr,
) -> InternalTransportClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    InternalTransportClient::new(channel)
}

#[tokio::test]
async fn grpc_create_index_on_leader() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "grpc-test".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19360".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let addr = start_raft_grpc_server(raft, state_handle.clone()).await;
    let mut client = connect_grpc(addr).await;

    let body = serde_json::json!({
        "settings": {"number_of_shards": 2, "number_of_replicas": 0}
    });
    let resp = client
        .create_index(tonic::Request::new(
            ferrissearch::transport::proto::CreateIndexRequest {
                index_name: "grpc-idx".into(),
                body_json: serde_json::to_vec(&body).unwrap(),
            },
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.acknowledged);
    assert!(resp.error.is_empty());

    let state = state_handle.read().unwrap();
    assert!(state.indices.contains_key("grpc-idx"));
    assert_eq!(state.indices["grpc-idx"].number_of_shards, 2);
}

#[tokio::test]
async fn grpc_create_index_duplicate_returns_error() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "grpc-dup".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19361".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let addr = start_raft_grpc_server(raft, state_handle.clone()).await;
    let mut client = connect_grpc(addr).await;

    let body = serde_json::json!({"settings": {"number_of_shards": 1}});
    let body_bytes = serde_json::to_vec(&body).unwrap();

    // First create succeeds
    let resp1 = client
        .create_index(tonic::Request::new(
            ferrissearch::transport::proto::CreateIndexRequest {
                index_name: "dup-idx".into(),
                body_json: body_bytes.clone(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(resp1.acknowledged);

    // Duplicate returns error
    let resp2 = client
        .create_index(tonic::Request::new(
            ferrissearch::transport::proto::CreateIndexRequest {
                index_name: "dup-idx".into(),
                body_json: body_bytes,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp2.acknowledged);
    assert!(resp2.error.contains("already exists"));
}

#[tokio::test]
async fn grpc_delete_index_on_leader() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "grpc-del".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19362".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    // Pre-create via Raft
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: make_index("to-delete"),
    })
    .await
    .unwrap();
    {
        let state = state_handle.read().unwrap();
        assert!(state.indices.contains_key("to-delete"));
    }

    let addr = start_raft_grpc_server(raft, state_handle.clone()).await;
    let mut client = connect_grpc(addr).await;

    let resp = client
        .delete_index(tonic::Request::new(
            ferrissearch::transport::proto::DeleteIndexRequest {
                index_name: "to-delete".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.acknowledged);
    assert!(resp.error.is_empty());

    let state = state_handle.read().unwrap();
    assert!(!state.indices.contains_key("to-delete"));
}

#[tokio::test]
async fn grpc_delete_nonexistent_index_returns_not_found() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "grpc-del-nf".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19363".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let addr = start_raft_grpc_server(raft, state_handle).await;
    let mut client = connect_grpc(addr).await;

    let result = client
        .delete_index(tonic::Request::new(
            ferrissearch::transport::proto::DeleteIndexRequest {
                index_name: "ghost".into(),
            },
        ))
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn grpc_create_index_with_mappings_and_settings() {
    let (raft, state_handle) = consensus::create_raft_instance_mem(1, "grpc-map".into())
        .await
        .unwrap();
    consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19364".into())
        .await
        .unwrap();
    wait_for_leader(&raft).await;

    let addr = start_raft_grpc_server(raft, state_handle.clone()).await;
    let mut client = connect_grpc(addr).await;

    let body = serde_json::json!({
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval_ms": 3000
        },
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "year": {"type": "integer"}
            }
        }
    });
    let resp = client
        .create_index(tonic::Request::new(
            ferrissearch::transport::proto::CreateIndexRequest {
                index_name: "mapped-idx".into(),
                body_json: serde_json::to_vec(&body).unwrap(),
            },
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.acknowledged);

    let state = state_handle.read().unwrap();
    let meta = &state.indices["mapped-idx"];
    assert_eq!(meta.settings.refresh_interval_ms, Some(3000));
    assert!(meta.mappings.contains_key("title"));
    assert!(meta.mappings.contains_key("year"));
}
