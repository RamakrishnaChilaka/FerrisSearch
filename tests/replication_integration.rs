//! Integration tests for primary-replica replication.
//!
//! These tests spin up real gRPC transport servers (in-process) and exercise the
//! full write → replicate → read path, similar to OpenSearch's ESIntegTestCase.

use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{
    IndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::proto::{
    self, JoinRequest, PublishStateRequest, ReplicateBulkRequest, ReplicateDocRequest,
    ShardBulkRequest, ShardDeleteRequest, ShardDocRequest, ShardGetRequest,
    ShardSearchDslRequest, ShardSearchRequest,
};
use ferrissearch::transport::server::create_transport_service;
use ferrissearch::transport::TransportClient;

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

// ─── Search integration tests ───────────────────────────────────────────────

/// Helper: index a document with vectors via gRPC and return success.
async fn index_doc_with_vectors(
    client: &mut InternalTransportClient<tonic::transport::Channel>,
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    payload: serde_json::Value,
) -> bool {
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index_name.into(),
            shard_id,
            doc_id: doc_id.into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    resp.success
}

#[tokio::test]
async fn search_shard_simple_query_string_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("search-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index two documents
    let payload1 = serde_json::json!({"title": "rust programming language"});
    let payload2 = serde_json::json!({"title": "python web framework"});
    assert!(index_doc_with_vectors(&mut client, "search-idx", 0, "d1", payload1).await);
    assert!(index_doc_with_vectors(&mut client, "search-idx", 0, "d2", payload2).await);
    refresh_all(&sm);

    // Simple query string search
    let resp = client
        .search_shard(tonic::Request::new(ShardSearchRequest {
            index_name: "search-idx".into(),
            shard_id: 0,
            query: "rust".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "search failed: {}", resp.error);
    assert_eq!(resp.hits.len(), 1, "expected 1 hit for 'rust'");
    let hit: serde_json::Value = serde_json::from_slice(&resp.hits[0].source_json).unwrap();
    assert_eq!(hit["_id"], "d1");
}

#[tokio::test]
async fn search_shard_dsl_match_query_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("dsl-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents
    assert!(index_doc_with_vectors(&mut client, "dsl-idx", 0, "d1", serde_json::json!({"title": "the matrix"})).await);
    assert!(index_doc_with_vectors(&mut client, "dsl-idx", 0, "d2", serde_json::json!({"title": "inception movie"})).await);
    assert!(index_doc_with_vectors(&mut client, "dsl-idx", 0, "d3", serde_json::json!({"title": "the dark knight"})).await);
    refresh_all(&sm);

    // DSL match query
    let search_req = serde_json::json!({
        "query": {"match": {"title": "matrix"}},
        "size": 10,
        "from": 0
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "dsl-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "DSL search failed: {}", resp.error);
    assert_eq!(resp.hits.len(), 1, "expected 1 hit for 'matrix'");
    let hit: serde_json::Value = serde_json::from_slice(&resp.hits[0].source_json).unwrap();
    assert_eq!(hit["_id"], "d1");
}

#[tokio::test]
async fn search_shard_dsl_match_all_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("matchall-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    for i in 0..4 {
        let payload = serde_json::json!({"title": format!("doc-{}", i)});
        assert!(index_doc_with_vectors(&mut client, "all-idx", 0, &format!("d{}", i), payload).await);
    }
    refresh_all(&sm);

    let search_req = serde_json::json!({"query": {"match_all": {}}, "size": 10});
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "all-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "match_all failed: {}", resp.error);
    assert_eq!(resp.hits.len(), 4, "expected 4 hits for match_all");
}

#[tokio::test]
async fn search_shard_dsl_knn_only_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("knn-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents with vector embeddings
    assert!(index_doc_with_vectors(&mut client, "knn-idx", 0, "d1",
        serde_json::json!({"title": "nearest", "emb": [1.0, 0.0, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "knn-idx", 0, "d2",
        serde_json::json!({"title": "middle", "emb": [0.5, 0.5, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "knn-idx", 0, "d3",
        serde_json::json!({"title": "farthest", "emb": [0.0, 0.0, 1.0]})).await);
    refresh_all(&sm);

    // kNN-only search: closest to [1.0, 0.0, 0.0] should return d1 first
    let search_req = serde_json::json!({
        "knn": {"emb": {"vector": [1.0, 0.0, 0.0], "k": 2}}
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "knn-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "kNN search failed: {}", resp.error);
    // gRPC returns raw concatenated results: 3 text (match_all) + 2 kNN = 5 total
    let all_hits: Vec<serde_json::Value> = resp.hits.iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    assert!(all_hits.len() >= 2, "expected at least 2 hits, got {}", all_hits.len());

    // Find the kNN hits (they have _knn_field)
    let knn_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 2, "expected 2 kNN hits");
    assert_eq!(knn_hits[0]["_id"], "d1", "d1 should be nearest to query vector");
}

#[tokio::test]
async fn search_shard_dsl_hybrid_text_and_knn_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("hybrid-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents with text and vector fields
    assert!(index_doc_with_vectors(&mut client, "hybrid-idx", 0, "d1",
        serde_json::json!({"title": "the matrix", "emb": [0.9, 0.1, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "hybrid-idx", 0, "d2",
        serde_json::json!({"title": "inception", "emb": [0.1, 0.9, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "hybrid-idx", 0, "d3",
        serde_json::json!({"title": "matrix reloaded", "emb": [0.85, 0.15, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "hybrid-idx", 0, "d4",
        serde_json::json!({"title": "dark knight", "emb": [0.0, 0.0, 1.0]})).await);
    refresh_all(&sm);

    // Hybrid: text match "matrix" + kNN closest to [0.9, 0.1, 0.0]
    let search_req = serde_json::json!({
        "query": {"match": {"title": "matrix"}},
        "knn": {"emb": {"vector": [0.9, 0.1, 0.0], "k": 3}}
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "hybrid-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "hybrid search failed: {}", resp.error);

    let all_hits: Vec<serde_json::Value> = resp.hits.iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();

    // gRPC returns raw concatenated results from a single shard:
    //   text hits: d1, d3 (match "matrix")
    //   kNN hits:  d1, d3, d2 (k=3, closest to [0.9, 0.1, 0.0])
    //   Total: 5 (d1 and d3 appear twice — RRF dedup happens at coordinator)
    assert_eq!(all_hits.len(), 5, "gRPC should return 5 raw hits (2 text + 3 kNN)");

    // Text hits: match on "matrix" should find d1 and d3
    let text_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_none())
        .collect();
    assert_eq!(text_hits.len(), 2, "expected 2 text hits for 'matrix'");
    let text_ids: Vec<&str> = text_hits.iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    assert!(text_ids.contains(&"d1"), "text hits should include d1");
    assert!(text_ids.contains(&"d3"), "text hits should include d3");

    // kNN hits: closest to [0.9, 0.1, 0.0] should include d1 (nearest)
    let knn_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 3, "expected 3 kNN hits (k=3)");
    assert_eq!(knn_hits[0]["_id"], "d1", "d1 should be nearest vector match");
}

#[tokio::test]
async fn search_shard_dsl_knn_returns_empty_when_no_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("no-vec-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index text-only documents (no vectors)
    assert!(index_doc_with_vectors(&mut client, "novecs-idx", 0, "d1",
        serde_json::json!({"title": "text only doc"})).await);
    refresh_all(&sm);

    // kNN search should still succeed but return only text results (no kNN hits)
    let search_req = serde_json::json!({
        "query": {"match_all": {}},
        "knn": {"emb": {"vector": [1.0, 0.0, 0.0], "k": 5}}
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "novecs-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "search should not fail: {}", resp.error);
    // Only text hits (match_all returns 1), no kNN hits
    let all_hits: Vec<serde_json::Value> = resp.hits.iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    let knn_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert!(knn_hits.is_empty(), "no kNN hits expected when no vectors indexed");
    assert_eq!(all_hits.len(), 1, "should return 1 text hit from match_all");
}

#[tokio::test]
async fn search_shard_dsl_nonexistent_shard_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("noshard-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let search_req = serde_json::json!({"query": {"match_all": {}}});
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "nonexistent".into(),
            shard_id: 99,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success, "should fail for nonexistent shard");
    assert!(resp.error.contains("not found"), "error should mention shard not found: {}", resp.error);
}

#[tokio::test]
async fn search_shard_dsl_knn_with_filter_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("filter-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index docs with text + vectors
    assert!(index_doc_with_vectors(&mut client, "filter-idx", 0, "d1",
        serde_json::json!({"title": "rust search", "emb": [1.0, 0.0, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "filter-idx", 0, "d2",
        serde_json::json!({"title": "python web", "emb": [0.9, 0.1, 0.0]})).await);
    assert!(index_doc_with_vectors(&mut client, "filter-idx", 0, "d3",
        serde_json::json!({"title": "rust compiler", "emb": [0.8, 0.2, 0.0]})).await);
    refresh_all(&sm);

    // kNN search WITH filter: only "rust" docs
    let search_req = serde_json::json!({
        "knn": { "emb": { "vector": [1.0, 0.0, 0.0], "k": 3, "filter": { "match": { "title": "rust" } } } }
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "filter-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "filtered kNN search failed: {}", resp.error);
    let all_hits: Vec<serde_json::Value> = resp.hits.iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();

    // kNN hits should only contain d1 and d3 (matching "rust"), not d2 ("python")
    let knn_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 2, "expected 2 filtered kNN hits (d1, d3)");
    let ids: Vec<&str> = knn_hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
    assert!(ids.contains(&"d1"), "d1 should pass the filter");
    assert!(ids.contains(&"d3"), "d3 should pass the filter");
    assert!(!ids.contains(&"d2"), "d2 ('python') should be filtered out");
}

#[tokio::test]
async fn search_shard_dsl_knn_filter_no_matches_returns_empty_knn() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("nofilter-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    assert!(index_doc_with_vectors(&mut client, "nofilter-idx", 0, "d1",
        serde_json::json!({"title": "rust only", "emb": [1.0, 0.0, 0.0]})).await);
    refresh_all(&sm);

    // Filter for "python" — no docs match
    let search_req = serde_json::json!({
        "knn": { "emb": { "vector": [1.0, 0.0, 0.0], "k": 5, "filter": { "match": { "title": "python" } } } }
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "nofilter-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "should succeed even with no matches: {}", resp.error);
    let all_hits: Vec<serde_json::Value> = resp.hits.iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    let knn_hits: Vec<&serde_json::Value> = all_hits.iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert!(knn_hits.is_empty(), "no kNN hits when filter matches nothing");
}
