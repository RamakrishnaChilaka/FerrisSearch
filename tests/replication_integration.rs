//! Integration tests for primary-replica replication.
//!
//! These tests spin up real gRPC transport servers (in-process) and exercise the
//! full write → replicate → read path, similar to OpenSearch's ESIntegTestCase.

use ropensearch::cluster::manager::ClusterManager;
use ropensearch::cluster::state::{
    IndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use ropensearch::shard::ShardManager;
use ropensearch::transport::proto::internal_transport_client::InternalTransportClient;
use ropensearch::transport::proto::{
    self, JoinRequest, PublishStateRequest, ReplicateBulkRequest, ReplicateDocRequest,
    ShardBulkRequest, ShardDeleteRequest, ShardDocRequest, ShardGetRequest,
};
use ropensearch::transport::server::create_transport_service;
use ropensearch::transport::TransportClient;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Start a gRPC transport server on a random port and return the address.
async fn start_grpc_server(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let transport_client = TransportClient::new();
    let service = create_transport_service(cluster_manager, shard_manager, transport_client);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// Connect a gRPC client to the given address.
async fn connect_client(
    addr: std::net::SocketAddr,
) -> InternalTransportClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    InternalTransportClient::new(channel)
}

/// Refresh all shard engines so recently indexed documents become visible.
fn refresh_all(sm: &ShardManager) {
    for (_, engine) in sm.all_shards() {
        engine.refresh().unwrap();
    }
}

/// Build cluster state for two-node replication tests.
fn setup_two_node_cluster_state(
    cm: &ClusterManager,
    index_name: &str,
    replica_port: u16,
) {
    let mut cs = cm.get_state();
    cs.add_node(DomainNodeInfo {
        id: "primary-node".into(),
        name: "primary".into(),
        host: "127.0.0.1".into(),
        transport_port: 29999,
        http_port: 29998,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    cs.add_node(DomainNodeInfo {
        id: "replica-node".into(),
        name: "replica".into(),
        host: "127.0.0.1".into(),
        transport_port: replica_port,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });

    let mut shard_routing = HashMap::new();
    shard_routing.insert(0, ShardRoutingEntry {
        primary: "primary-node".into(),
        replicas: vec!["replica-node".into()],
    });
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing,
    });
    cm.update_state(cs);
}

// ─── Single-node integration tests ─────────────────────────────────────────

#[tokio::test]
async fn index_and_get_document_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index a document
    let payload = serde_json::json!({"title": "Integration Test", "score": 42});
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "test-index".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "index_doc failed: {}", resp.error);
    assert_eq!(resp.doc_id, "doc-1");

    // Refresh so the document becomes visible to the reader
    refresh_all(&sm);

    // Get the document back
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "test-index".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found, "document not found: {}", resp.error);
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["title"], "Integration Test");
    assert_eq!(source["score"], 42);
}

#[tokio::test]
async fn bulk_index_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let documents: Vec<Vec<u8>> = (0..5)
        .map(|i| {
            serde_json::to_vec(&serde_json::json!({
                "_doc_id": format!("bulk-{}", i),
                "_source": {"field": format!("value-{}", i)}
            }))
            .unwrap()
        })
        .collect();

    let resp = client
        .bulk_index(tonic::Request::new(ShardBulkRequest {
            index_name: "bulk-idx".into(),
            shard_id: 0,
            documents_json: documents,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "bulk_index failed: {}", resp.error);
    assert_eq!(resp.doc_ids.len(), 5);
}

#[tokio::test]
async fn delete_document_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    // Index then delete
    let payload = serde_json::json!({"content": "to be deleted"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "del-idx".into(),
            shard_id: 0,
            doc_id: "doomed".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap();

    let resp = client
        .delete_doc(tonic::Request::new(ShardDeleteRequest {
            index_name: "del-idx".into(),
            shard_id: 0,
            doc_id: "doomed".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "delete failed: {}", resp.error);

    // Verify it's gone
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "del-idx".into(),
            shard_id: 0,
            doc_id: "doomed".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
}

#[tokio::test]
async fn replicate_doc_index_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Replicate an index operation (simulates replica receiving from primary)
    let payload = serde_json::json!({"color": "blue", "count": 7});
    let resp = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "replica-idx".into(),
            shard_id: 0,
            doc_id: "rep-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            op: "index".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "replicate_doc failed: {}", resp.error);

    // Refresh so the document becomes visible
    refresh_all(&sm);

    // Verify replica has the document
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "replica-idx".into(),
            shard_id: 0,
            doc_id: "rep-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["color"], "blue");
}

#[tokio::test]
async fn replicate_doc_delete_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    // Index a doc first
    let payload = serde_json::json!({"temp": true});
    client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "rep-del-idx".into(),
            shard_id: 0,
            doc_id: "to-delete".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            op: "index".into(),
        }))
        .await
        .unwrap();

    // Delete via replication
    let resp = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "rep-del-idx".into(),
            shard_id: 0,
            doc_id: "to-delete".into(),
            payload_json: vec![],
            op: "delete".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);

    // Verify deleted
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "rep-del-idx".into(),
            shard_id: 0,
            doc_id: "to-delete".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
}

#[tokio::test]
async fn replicate_bulk_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // ReplicateBulkRequest uses repeated ReplicateDocRequest as ops
    let ops: Vec<ReplicateDocRequest> = (0..3)
        .map(|i| ReplicateDocRequest {
            index_name: String::new(), // ignored — set on the outer request
            shard_id: 0,
            doc_id: format!("bulk-rep-{}", i),
            payload_json: serde_json::to_vec(&serde_json::json!({"n": i})).unwrap(),
            op: "index".into(),
        })
        .collect();

    let resp = client
        .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
            index_name: "bulk-rep-idx".into(),
            shard_id: 0,
            ops,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "replicate_bulk failed: {}", resp.error);

    // Refresh so documents become visible
    refresh_all(&sm);

    // Verify all docs exist
    for i in 0..3 {
        let resp = client
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: "bulk-rep-idx".into(),
                shard_id: 0,
                doc_id: format!("bulk-rep-{}", i),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found, "bulk-rep-{} not found", i);
    }
}

#[tokio::test]
async fn join_cluster_and_publish_state_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("cluster-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm.clone(), sm).await;
    let mut client = connect_client(addr).await;

    // Join cluster
    let join_resp = client
        .join_cluster(tonic::Request::new(JoinRequest {
            node_info: Some(proto::NodeInfo {
                id: "joining-node".into(),
                name: "joiner".into(),
                host: "127.0.0.1".into(),
                transport_port: 9301,
                http_port: 9201,
                roles: vec!["data".into()],
            }),
            raft_node_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(join_resp.state.is_some());
    let state = join_resp.state.unwrap();
    assert_eq!(state.cluster_name, "cluster-test");
    assert!(state.nodes.iter().any(|n| n.id == "joining-node"));

    // Verify cluster manager has the node
    let cs = cm.get_state();
    assert!(cs.nodes.contains_key("joining-node"));
}

#[tokio::test]
async fn publish_state_updates_cluster_and_closes_deleted_indices() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("pub-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    // Pre-create an index so we can test deletion
    sm.open_shard("old-index", 0).unwrap();
    assert!(sm.get_shard("old-index", 0).is_some());

    {
        let mut initial_state = cm.get_state();
        let mut shard_routing = HashMap::new();
        shard_routing.insert(0, ShardRoutingEntry {
            primary: "local".into(),
            replicas: vec![],
        });
        initial_state.add_index(IndexMetadata {
            name: "old-index".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
        });
        cm.update_state(initial_state);
    }

    let addr = start_grpc_server(cm.clone(), sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Publish a new state that does NOT contain "old-index" → should close it
    let new_state = proto::ClusterState {
        cluster_name: "pub-test".into(),
        version: 99,
        master_node: Some("master-1".into()),
        nodes: vec![],
        indices: vec![],
    };

    client
        .publish_state(tonic::Request::new(PublishStateRequest {
            state: Some(new_state),
        }))
        .await
        .unwrap();

    let cs = cm.get_state();
    assert_eq!(cs.version, 99);
    assert!(!cs.indices.contains_key("old-index"));
    // Shard should be closed
    assert!(sm.get_shard("old-index", 0).is_none());
}

// ─── Two-node integration tests: primary → replica replication ──────────────

#[tokio::test]
async fn primary_write_replicates_to_replica_node() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(replica_dir.path(), Duration::from_secs(60)));
    let replica_addr = start_grpc_server(replica_cm, replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(primary_dir.path(), Duration::from_secs(60)));

    setup_two_node_cluster_state(&primary_cm, "replicated-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    // Write a document to the primary
    let payload = serde_json::json!({"message": "hello from primary", "version": 1});
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "replicated-idx".into(),
            shard_id: 0,
            doc_id: "replicated-doc".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "primary index_doc failed: {}", resp.error);

    // Refresh the replica shard so the replicated document becomes visible
    refresh_all(&replica_sm);

    // Connect to the replica and verify the document was replicated
    let mut replica_client = connect_client(replica_addr).await;
    let resp = replica_client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "replicated-idx".into(),
            shard_id: 0,
            doc_id: "replicated-doc".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found, "Document not replicated to replica node");
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["message"], "hello from primary");
}

#[tokio::test]
async fn primary_delete_replicates_to_replica_node() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(replica_dir.path(), Duration::from_secs(60)));
    let replica_addr = start_grpc_server(replica_cm, replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(primary_dir.path(), Duration::from_secs(60)));

    setup_two_node_cluster_state(&primary_cm, "del-repl-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    // Index a document
    let payload = serde_json::json!({"data": "will be deleted"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap();

    // Delete it on the primary
    let resp = client
        .delete_doc(tonic::Request::new(ShardDeleteRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    // Verify deletion replicated to replica
    let mut replica_client = connect_client(replica_addr).await;
    let resp = replica_client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found, "Document should have been deleted on replica");
}

#[tokio::test]
async fn primary_bulk_replicates_to_replica_node() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(replica_dir.path(), Duration::from_secs(60)));
    let replica_addr = start_grpc_server(replica_cm, replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(primary_dir.path(), Duration::from_secs(60)));

    setup_two_node_cluster_state(&primary_cm, "bulk-repl-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    // Bulk index 5 documents on the primary
    let documents: Vec<Vec<u8>> = (0..5)
        .map(|i| {
            serde_json::to_vec(&serde_json::json!({
                "_doc_id": format!("repl-bulk-{}", i),
                "_source": {"idx": i}
            }))
            .unwrap()
        })
        .collect();

    let resp = client
        .bulk_index(tonic::Request::new(ShardBulkRequest {
            index_name: "bulk-repl-idx".into(),
            shard_id: 0,
            documents_json: documents,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success, "bulk index failed: {}", resp.error);

    // Refresh the replica shard so documents become visible
    refresh_all(&replica_sm);

    // Verify all 5 docs replicated to the replica
    let mut replica_client = connect_client(replica_addr).await;
    for i in 0..5 {
        let resp = replica_client
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: "bulk-repl-idx".into(),
                shard_id: 0,
                doc_id: format!("repl-bulk-{}", i),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found, "repl-bulk-{} not replicated to replica", i);
    }
}
