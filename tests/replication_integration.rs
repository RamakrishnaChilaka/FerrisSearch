//! Integration tests for primary-replica replication.
//!
//! These tests spin up real gRPC transport servers (in-process) and exercise the
//! full write → replicate → read path, similar to OpenSearch's ESIntegTestCase.

use datafusion::arrow::array::StringArray;
use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{
    FieldMapping, FieldType, IndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use ferrissearch::engine::{CompositeEngine, SearchEngine};
use ferrissearch::search::{QueryClause, SearchRequest};
use ferrissearch::shard::ShardManager;
#[cfg(feature = "transport-tls")]
use ferrissearch::transport::TonicTlsConnector;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::proto::{
    self, JoinRequest, PublishStateRequest, ReplicateBulkRequest, ReplicateDocRequest,
    ShardBulkRequest, ShardDeleteRequest, ShardDocRequest, ShardGetRequest, ShardSearchDslRequest,
    ShardSearchRequest,
};
use ferrissearch::transport::server::create_transport_service;
use ferrissearch::wal::{HotTranslog, WriteAheadLog};
use futures::TryStreamExt;

use std::collections::HashMap;
#[cfg(feature = "transport-tls")]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "transport-tls")]
const TEST_TRANSPORT_TLS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC7TCCAdWgAwIBAgIUDrtYB6ruoGKUSdC6xKA7NJiIIPwwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDQwMjIxMDE0OVoXDTI3MDQw
MjIxMDE0OVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAuNpib/06YwZ/Gb2E1kTcvUJAxkzvwS+Wqn1As+Tn7fm5
J6hRtMelw2031/B4lx/HUFxw5Cm4UEnKn2l1JXnO8QSgZnWbYSgeuNYSsbIeQWRq
g0iSaLxJZ2i2RIBhARzkC5x5eF1tZMjITkk1Zo1GInxWUteWqhTPkhSSdNbME8G3
wEvUl6TC5pzXvI6uTsJsCjvZU63CHuc6z9JfI+aF7yBDhVQF8PwEf63ubw6bMdtC
iyYxZTwIoFCxBjb2O11l/FU+H1in+0v8pDFCg6ocd3J+aHwaUaA+BMtqKsdY9Ve+
4Dk0VIfwqYWCd26GXcwWgh7l0ctxX96Yx1JIRIZ7nwIDAQABozcwNTAUBgNVHREE
DTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFJfkq34nKZH+oPYwNx8v5bD2sULtMA0G
CSqGSIb3DQEBCwUAA4IBAQBamnKtY+jqfwGyYjIPXFWnf0Gh+HT54jslTDDQ9qyT
xYHzACqH5KjwuPnbmhPONlPmbRTHeLA492VsYnSDTN1ypTLpC/u/ZXb3GVvU2Hza
YMtSW+slBalR5QffTgb60fDgrgqrBYKBsIBZiqNQOw/0asa+2c72Bxwb5dfLxNAX
zAGCoY+urr4sRbesol+p7R+TwDAn2v5vvd5Vrq2eposipdNMfkJUElKa5uVIvfTs
9OhDLJtK7rMDIivFm4T8jcKaxmjkwUADBq9++p7j8BHhfP1ILMV6BamkTeOeAYwZ
GlYQoYK+y3RWFrJFp2cPJsN4sqkMJqY9RiPyM/E9IhLT
-----END CERTIFICATE-----
"#;

#[cfg(feature = "transport-tls")]
const TEST_TRANSPORT_TLS_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC42mJv/TpjBn8Z
vYTWRNy9QkDGTO/BL5aqfUCz5Oft+bknqFG0x6XDbTfX8HiXH8dQXHDkKbhQScqf
aXUlec7xBKBmdZthKB641hKxsh5BZGqDSJJovElnaLZEgGEBHOQLnHl4XW1kyMhO
STVmjUYifFZS15aqFM+SFJJ01swTwbfAS9SXpMLmnNe8jq5OwmwKO9lTrcIe5zrP
0l8j5oXvIEOFVAXw/AR/re5vDpsx20KLJjFlPAigULEGNvY7XWX8VT4fWKf7S/yk
MUKDqhx3cn5ofBpRoD4Ey2oqx1j1V77gOTRUh/CphYJ3boZdzBaCHuXRy3Ff3pjH
UkhEhnufAgMBAAECggEAA+GR74gBkdKxGHlCML2BZPffJEq5PfUh1LKMiTplJDn6
CTsffAw1DsVcRsxlu8aPCMDoHeJCXG0wM+ii7QaBsc3HEF+nw4J0Iq1b9x8mQ3k4
Q0liyZAqemFYcle/saZJo3TFmCFeCp+slPg0htKwhkjWByc/opKNSSPlb06TOlbt
wjBs5cwfoVlP+FfxEbtmKEs6VVXXw5FDwRu+zScR/j6hpTcTaNqH1pcln2vHyN6i
uNLiXlJ3wKJoeVAeBDN85UfSSMIMcC6gRimr5BnDPMC7cBFjfr3dundAWtg6md7I
oliTeV9gKXUGsbRX2Tau7e8s0WElwvZzpp2rw/DVAQKBgQDadUWo436dP6Hci0Ex
MD9Jcm5Mo6PiQuUHcKP+k6AqP83UTDWRDhnIxmthdbhjEUiDCb0GycQkWyub4ivn
/OxELIQkULBdHhg7fibENdQENX/kcawsSnKZpytmwBT0sIX38/D+hzx0ufXiWQ0a
MYNUCcDapeCN1seFt3v5Gr6YrQKBgQDYnrX38Pt2fp507lek/mzUgrU/MAF74EhH
xHvKEii/58OSOnkhjPD9AOP/2FTg78A4KgrOGpGNXguS9csuL0dA7LTnGEUX2Cq9
R+ld+W+FL1lDfVssg/LDj02gy1UZda2XFbs4aog4zdoEE2WZR1ka6dYW+bEAicts
6OS/+sky+wKBgGrhdXNr2kaVG1wLxZmLQWtt0QkuBsBseiFputKS54nELa/wmUSe
4X6ZlW/ZaJ0Pl6qE2Ta5AH3JHUznGxQlanLwVLZvw9nLH4/76HuW2mQ0yJ27/8Cr
q+YBI/rhf183/lORxhbBk5KIaQSVDRQDpX04SGKxRWwf6P5DBySZMScBAoGBALAx
V51WW5LkJorBmnRPpcGslzPQHkTeBqypOm8AGjkNkFuOSBxsAVAou0rMcS2MlPKZ
77P4lE9CIXPljOACAJjkb7hQW1KrtwfCSCTx0C2qd5aXjeNFZ9583w1cldlhiFKN
kHyw2iAp/5y1Ejx8dhOYA1UovznK2rW5MOaeW6ylAoGABgGob6T47AS0nRCFIUGh
AcPZhTf/aOTCpHONuxC4etnpM5bAZkn4E7KNe+EHbFn51QOegIbip7VDrteSzkFJ
vvobgeFZ8Fq1YNC3d80wtt5mKrQF8ce0A7q0yM1DOQ3mgqO1424znll4IoDcZUos
G0Zd6Hw16rGcTrwdHUeJOA0=
-----END PRIVATE KEY-----
"#;

#[cfg(feature = "transport-tls")]
struct TransportTlsTestFiles {
    _dir: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
}

#[cfg(feature = "transport-tls")]
impl TransportTlsTestFiles {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("transport-cert.pem");
        let key_path = dir.path().join("transport-key.pem");
        let ca_path = dir.path().join("transport-ca.pem");

        std::fs::write(&cert_path, TEST_TRANSPORT_TLS_CERT_PEM).unwrap();
        std::fs::write(&key_path, TEST_TRANSPORT_TLS_KEY_PEM).unwrap();
        std::fs::write(&ca_path, TEST_TRANSPORT_TLS_CERT_PEM).unwrap();

        Self {
            _dir: dir,
            cert_path,
            key_path,
            ca_path,
        }
    }
}

/// Start a gRPC transport server on a random port and return the address.
async fn start_grpc_server(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let transport_client = TransportClient::new();
    let service = create_transport_service(
        cluster_manager,
        shard_manager,
        transport_client,
        "node-1".into(),
    );

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

#[cfg(feature = "transport-tls")]
async fn start_grpc_server_with_tls(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    tls_files: &TransportTlsTestFiles,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let transport_client = TransportClient::new();
    let service = create_transport_service(
        cluster_manager,
        shard_manager,
        transport_client,
        "node-1".into(),
    );
    let tls_config = ferrissearch::transport::load_server_tls_config(
        tls_files.cert_path.to_str().unwrap(),
        tls_files.key_path.to_str().unwrap(),
    )
    .unwrap();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tls_config)
            .unwrap()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

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

#[cfg(feature = "transport-tls")]
async fn connect_tls_client(
    addr: std::net::SocketAddr,
    tls_files: &TransportTlsTestFiles,
) -> InternalTransportClient<tonic::transport::Channel> {
    let connector = TonicTlsConnector::from_ca_file(tls_files.ca_path.to_str().unwrap()).unwrap();
    let transport_client = TransportClient::with_tls_connector(Arc::new(connector));
    transport_client
        .connect("localhost", addr.port())
        .await
        .unwrap()
}

/// Refresh all shard engines so recently indexed documents become visible.
fn refresh_all(sm: &ShardManager) {
    for (_, engine) in sm.all_shards() {
        engine.refresh().unwrap();
    }
}

fn setup_single_node_cluster_state(cm: &ClusterManager, index_name: &str) {
    let mut cs = cm.get_state();
    if !cs.nodes.contains_key("node-1") {
        cs.add_node(DomainNodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
    }

    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: String::new(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    cm.update_state(cs);
}

fn setup_multi_shard_single_node_cluster_state(cm: &ClusterManager, index_name: &str, shards: u32) {
    let mut cs = cm.get_state();
    if !cs.nodes.contains_key("node-1") {
        cs.add_node(DomainNodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
    }

    let mut shard_routing = HashMap::new();
    for shard_id in 0..shards {
        shard_routing.insert(
            shard_id,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
    }
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: String::new(),
        number_of_shards: shards,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    cm.update_state(cs);
}

/// Build cluster state for two-node replication tests.
fn setup_two_node_cluster_state(
    primary_cm: &ClusterManager,
    replica_cm: &ClusterManager,
    index_name: &str,
    replica_port: u16,
) {
    let mut cs = primary_cm.get_state();
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
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "primary-node".into(),
            replicas: vec!["replica-node".into()],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: String::new(),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing,
        mappings: std::collections::HashMap::new(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    primary_cm.update_state(cs.clone());
    replica_cm.update_state(cs);
}

// ─── Single-node integration tests ─────────────────────────────────────────

#[tokio::test]
async fn index_and_get_document_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "test-index");

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

#[cfg(feature = "transport-tls")]
#[tokio::test]
async fn index_and_get_document_via_grpc_with_tls() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test-tls".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "tls-index");

    let tls_files = TransportTlsTestFiles::new();
    let addr = start_grpc_server_with_tls(cm, sm.clone(), &tls_files).await;
    let mut client = connect_tls_client(addr, &tls_files).await;

    let payload = serde_json::json!({"title": "TLS Integration Test", "score": 7});
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "tls-index".into(),
            shard_id: 0,
            doc_id: "tls-doc-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "TLS index_doc failed: {}", resp.error);
    refresh_all(&sm);

    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "tls-index".into(),
            shard_id: 0,
            doc_id: "tls-doc-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found, "TLS document not found: {}", resp.error);
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["title"], "TLS Integration Test");
    assert_eq!(source["score"], 7);
}

#[tokio::test]
async fn bulk_index_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "bulk-idx");

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
    setup_single_node_cluster_state(&cm, "del-idx");

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
    setup_single_node_cluster_state(&cm, "replica-idx");

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
            seq_no: 0,
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

    let tl = HotTranslog::open(sm.shard_data_dir("replica-idx", 0).unwrap()).unwrap();
    let entries = tl.read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq_no, 0);
}

#[tokio::test]
async fn replicate_doc_delete_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "rep-del-idx");

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
            seq_no: 0,
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
            seq_no: 0,
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
    setup_single_node_cluster_state(&cm, "bulk-rep-idx");

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
            seq_no: i,
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

    let tl = HotTranslog::open(sm.shard_data_dir("bulk-rep-idx", 0).unwrap()).unwrap();
    let entries = tl.read_all().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].seq_no, 0);
    assert_eq!(entries[1].seq_no, 1);
    assert_eq!(entries[2].seq_no, 2);
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
                raft_node_id: 0,
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
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "local".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        initial_state.add_index(IndexMetadata {
            name: "old-index".into(),
            uuid: String::new(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: ferrissearch::cluster::state::IndexSettings::default(),
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
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "replicated-idx",
        replica_addr.port(),
    );

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

    let tl = HotTranslog::open(replica_sm.shard_data_dir("replicated-idx", 0).unwrap()).unwrap();
    let entries = tl.read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq_no, 0);
}

#[tokio::test]
async fn primary_delete_replicates_to_replica_node() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "del-repl-idx",
        replica_addr.port(),
    );

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
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "bulk-repl-idx",
        replica_addr.port(),
    );

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

    let tl = HotTranslog::open(replica_sm.shard_data_dir("bulk-repl-idx", 0).unwrap()).unwrap();
    let entries = tl.read_all().unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].seq_no, 0);
    assert_eq!(entries[4].seq_no, 4);
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
    setup_single_node_cluster_state(&cm, "search-idx");

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
async fn search_shard_reopens_persisted_shard_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "restart-search-uuid";

    {
        let sm = ShardManager::new(dir.path(), Duration::from_secs(60));
        sm.register_index_uuid("restart-idx", test_uuid);
        let engine = sm.open_shard("restart-idx", 0).unwrap();
        engine
            .add_document("d1", serde_json::json!({"title": "rust restart"}))
            .unwrap();
        engine.refresh().unwrap();
    }

    let cm = Arc::new(ClusterManager::new("restart-search-test".into()));
    {
        let mut cs = cm.get_state();
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-idx".into(),
            uuid: test_uuid.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: ferrissearch::cluster::state::IndexSettings::default(),
        });
        cm.update_state(cs);
    }
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let resp = client
        .search_shard(tonic::Request::new(ShardSearchRequest {
            index_name: "restart-idx".into(),
            shard_id: 0,
            query: "rust".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "restart search failed: {}", resp.error);
    assert_eq!(
        resp.hits.len(),
        1,
        "expected reopened shard to return 1 hit"
    );
    let hit: serde_json::Value = serde_json::from_slice(&resp.hits[0].source_json).unwrap();
    assert_eq!(hit["_id"], "d1");
}

#[tokio::test]
async fn search_shard_dsl_match_query_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("dsl-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "dsl-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents
    assert!(
        index_doc_with_vectors(
            &mut client,
            "dsl-idx",
            0,
            "d1",
            serde_json::json!({"title": "the matrix"})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "dsl-idx",
            0,
            "d2",
            serde_json::json!({"title": "inception movie"})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "dsl-idx",
            0,
            "d3",
            serde_json::json!({"title": "the dark knight"})
        )
        .await
    );
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
async fn search_shard_dsl_reopens_persisted_shard_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "restart-dsl-uuid";

    {
        let sm = ShardManager::new(dir.path(), Duration::from_secs(60));
        sm.register_index_uuid("restart-dsl-idx", test_uuid);
        let engine = sm.open_shard("restart-dsl-idx", 0).unwrap();
        engine
            .add_document("d1", serde_json::json!({"title": "the restart matrix"}))
            .unwrap();
        engine.refresh().unwrap();
    }

    let cm = Arc::new(ClusterManager::new("restart-dsl-test".into()));
    {
        let mut cs = cm.get_state();
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-dsl-idx".into(),
            uuid: test_uuid.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: ferrissearch::cluster::state::IndexSettings::default(),
        });
        cm.update_state(cs);
    }
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let search_req = serde_json::json!({
        "query": {"match": {"title": "restart"}},
        "size": 10,
        "from": 0
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "restart-dsl-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "restart DSL search failed: {}", resp.error);
    assert_eq!(
        resp.hits.len(),
        1,
        "expected reopened shard to return 1 hit"
    );
    let hit: serde_json::Value = serde_json::from_slice(&resp.hits[0].source_json).unwrap();
    assert_eq!(hit["_id"], "d1");
}

#[tokio::test]
async fn search_shard_dsl_reopens_mapped_shard_with_reordered_metadata_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "restart-mapped-uuid";

    {
        let sm = ShardManager::new(dir.path(), Duration::from_secs(60));
        sm.register_index_uuid("restart-mapped-idx", test_uuid);
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let engine = sm
            .open_shard_with_mappings("restart-mapped-idx", 0, &mappings)
            .unwrap();
        engine
            .add_document(
                "d1",
                serde_json::json!({"title": "schema order", "category": "stable"}),
            )
            .unwrap();
        engine.refresh().unwrap();
    }

    let cm = Arc::new(ClusterManager::new("restart-mapped-test".into()));
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let mut restarted_mappings = HashMap::new();
    restarted_mappings.insert(
        "category".into(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );
    restarted_mappings.insert(
        "title".into(),
        FieldMapping {
            field_type: FieldType::Text,
            dimension: None,
        },
    );

    let mut cs = cm.get_state();
    cs.add_index(IndexMetadata {
        name: "restart-mapped-idx".into(),
        uuid: test_uuid.into(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: restarted_mappings,
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    cm.update_state(cs);

    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let search_req = serde_json::json!({
        "query": {"match": {"title": "schema"}},
        "size": 10,
        "from": 0
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "restart-mapped-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.success,
        "restart mapped DSL search failed after reordered metadata: {}",
        resp.error
    );
    assert_eq!(
        resp.hits.len(),
        1,
        "expected reopened mapped shard to return 1 hit"
    );
    let hit: serde_json::Value = serde_json::from_slice(&resp.hits[0].source_json).unwrap();
    assert_eq!(hit["_id"], "d1");
}

#[tokio::test]
async fn search_shard_dsl_restart_replays_only_uncommitted_entries_after_refresh_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "restart-replay-uuid";

    {
        let shard_dir = dir.path().join(test_uuid).join("shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
        engine
            .add_document(
                "d1",
                serde_json::json!({"title": "committed before restart"}),
            )
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document("d2", serde_json::json!({"title": "pending before restart"}))
            .unwrap();
    }

    let cm = Arc::new(ClusterManager::new("restart-replay-test".into()));
    {
        let mut cs = cm.get_state();
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-replay-idx".into(),
            uuid: test_uuid.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: ferrissearch::cluster::state::IndexSettings::default(),
        });
        cm.update_state(cs);
    }
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let search_req = serde_json::json!({
        "query": {"match_all": {}},
        "size": 10,
        "from": 0
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "restart-replay-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.success,
        "restart replay DSL search failed: {}",
        resp.error
    );
    assert_eq!(
        resp.hits.len(),
        2,
        "expected committed doc plus one replayed pending doc"
    );

    let mut ids: Vec<String> = resp
        .hits
        .iter()
        .map(|hit| {
            serde_json::from_slice::<serde_json::Value>(&hit.source_json).unwrap()["_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["d1".to_string(), "d2".to_string()]);
}

#[tokio::test]
async fn search_shard_dsl_match_all_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("matchall-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "all-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    for i in 0..4 {
        let payload = serde_json::json!({"title": format!("doc-{}", i)});
        assert!(
            index_doc_with_vectors(&mut client, "all-idx", 0, &format!("d{}", i), payload).await
        );
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
async fn search_shard_dsl_aggs_roundtrip_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("agg-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let mut mappings = HashMap::new();
    mappings.insert(
        "category".into(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );
    mappings.insert(
        "price".into(),
        FieldMapping {
            field_type: FieldType::Float,
            dimension: None,
        },
    );

    let mut cs = cm.get_state();
    cs.add_index(IndexMetadata {
        name: "agg-idx".into(),
        uuid: String::new(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    cm.update_state(cs);

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    assert!(
        index_doc_with_vectors(
            &mut client,
            "agg-idx",
            0,
            "d1",
            serde_json::json!({"category": "books", "price": 10.0})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "agg-idx",
            0,
            "d2",
            serde_json::json!({"category": "books", "price": 20.0})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "agg-idx",
            0,
            "d3",
            serde_json::json!({"category": "toys", "price": 30.0})
        )
        .await
    );
    refresh_all(&sm);

    let search_req = serde_json::json!({
        "query": {"match_all": {}},
        "size": 0,
        "aggs": {
            "top_categories": {"terms": {"field": "category", "size": 10}},
            "price_stats": {"stats": {"field": "price"}}
        }
    });
    let resp = client
        .search_shard_dsl(tonic::Request::new(ShardSearchDslRequest {
            index_name: "agg-idx".into(),
            shard_id: 0,
            search_request_json: serde_json::to_vec(&search_req).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "agg search failed: {}", resp.error);
    assert!(resp.hits.is_empty(), "size=0 should not return hits");

    let partial_aggs = ferrissearch::search::decode_partial_aggs(&resp.partial_aggs_json).unwrap();

    let ferrissearch::search::PartialAggResult::Terms { buckets } =
        partial_aggs["top_categories"].clone()
    else {
        panic!("expected terms partial result");
    };
    assert_eq!(buckets[0].key, "books");
    assert_eq!(buckets[0].doc_count, 2);

    let ferrissearch::search::PartialAggResult::Stats {
        count,
        sum,
        min,
        max,
    } = partial_aggs["price_stats"].clone()
    else {
        panic!("expected stats partial result");
    };
    assert_eq!(count, 3);
    assert_eq!(sum, 60.0);
    assert_eq!(min, 10.0);
    assert_eq!(max, 30.0);
}

#[tokio::test]
async fn forward_sql_batch_stream_to_shard_returns_multiple_arrow_batches() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("sql-stream-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let mut mappings = HashMap::new();
    mappings.insert(
        "brand".into(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );

    let mut cs = cm.get_state();
    cs.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    cs.add_index(IndexMetadata {
        name: "sql-stream-idx".into(),
        uuid: String::new(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    cm.update_state(cs);

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut grpc_client = connect_client(addr).await;

    assert!(
        index_doc_with_vectors(
            &mut grpc_client,
            "sql-stream-idx",
            0,
            "d1",
            serde_json::json!({"brand": "Apple"})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut grpc_client,
            "sql-stream-idx",
            0,
            "d2",
            serde_json::json!({"brand": "Apple"})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut grpc_client,
            "sql-stream-idx",
            0,
            "d3",
            serde_json::json!({"brand": "Samsung"})
        )
        .await
    );
    refresh_all(&sm);

    let transport_client = TransportClient::new();
    let remote_node = DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: addr.port(),
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    };
    let search_req = SearchRequest {
        query: QueryClause::MatchAll(serde_json::json!({})),
        size: 10,
        from: 0,
        knn: None,
        sort: vec![],
        aggs: HashMap::new(),
    };

    let (batches, total_hits, streaming_used) = transport_client
        .forward_sql_batch_stream_to_shard(
            &remote_node,
            "sql-stream-idx",
            0,
            &search_req,
            &["brand".to_string()],
            false,
            false,
            1,
        )
        .await
        .unwrap();

    assert_eq!(total_hits, 3);
    assert!(streaming_used);
    assert_eq!(batches.len(), 3);
    assert!(batches.iter().all(|batch| batch.num_rows() == 1));

    let live_stream = transport_client
        .open_sql_batch_stream_to_shard(
            &remote_node,
            "sql-stream-idx",
            0,
            &search_req,
            &["brand".to_string()],
            false,
            false,
            1,
        )
        .await
        .unwrap();

    assert_eq!(live_stream.total_hits(), 3);
    assert_eq!(live_stream.collected_rows(), 3);
    assert!(live_stream.streaming_used());

    let live_batches = live_stream
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(live_batches.len(), 3);
    assert!(live_batches.iter().all(|batch| batch.num_rows() == 1));

    let brand_index = batches[0]
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "brand")
        .expect("brand column present");
    let mut brands: Vec<String> = batches
        .iter()
        .map(|batch| {
            batch
                .column(brand_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        })
        .collect();
    brands.sort();
    assert_eq!(brands, vec!["Apple", "Apple", "Samsung"]);

    let live_brand_index = live_batches[0]
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "brand")
        .expect("brand column present");
    let mut live_brands: Vec<String> = live_batches
        .iter()
        .map(|batch| {
            batch
                .column(live_brand_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        })
        .collect();
    live_brands.sort();
    assert_eq!(live_brands, vec!["Apple", "Apple", "Samsung"]);
}

#[tokio::test]
async fn search_shard_dsl_knn_only_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("knn-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "knn-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents with vector embeddings
    assert!(
        index_doc_with_vectors(
            &mut client,
            "knn-idx",
            0,
            "d1",
            serde_json::json!({"title": "nearest", "emb": [1.0, 0.0, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "knn-idx",
            0,
            "d2",
            serde_json::json!({"title": "middle", "emb": [0.5, 0.5, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "knn-idx",
            0,
            "d3",
            serde_json::json!({"title": "farthest", "emb": [0.0, 0.0, 1.0]})
        )
        .await
    );
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
    let all_hits: Vec<serde_json::Value> = resp
        .hits
        .iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    assert!(
        all_hits.len() >= 2,
        "expected at least 2 hits, got {}",
        all_hits.len()
    );

    // Find the kNN hits (they have _knn_field)
    let knn_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 2, "expected 2 kNN hits");
    assert_eq!(
        knn_hits[0]["_id"], "d1",
        "d1 should be nearest to query vector"
    );
}

#[tokio::test]
async fn search_shard_dsl_hybrid_text_and_knn_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("hybrid-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "hybrid-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index documents with text and vector fields
    assert!(
        index_doc_with_vectors(
            &mut client,
            "hybrid-idx",
            0,
            "d1",
            serde_json::json!({"title": "the matrix", "emb": [0.9, 0.1, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "hybrid-idx",
            0,
            "d2",
            serde_json::json!({"title": "inception", "emb": [0.1, 0.9, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "hybrid-idx",
            0,
            "d3",
            serde_json::json!({"title": "matrix reloaded", "emb": [0.85, 0.15, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "hybrid-idx",
            0,
            "d4",
            serde_json::json!({"title": "dark knight", "emb": [0.0, 0.0, 1.0]})
        )
        .await
    );
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

    let all_hits: Vec<serde_json::Value> = resp
        .hits
        .iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();

    // gRPC returns raw concatenated results from a single shard:
    //   text hits: d1, d3 (match "matrix")
    //   kNN hits:  d1, d3, d2 (k=3, closest to [0.9, 0.1, 0.0])
    //   Total: 5 (d1 and d3 appear twice — RRF dedup happens at coordinator)
    assert_eq!(
        all_hits.len(),
        5,
        "gRPC should return 5 raw hits (2 text + 3 kNN)"
    );

    // Text hits: match on "matrix" should find d1 and d3
    let text_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_none())
        .collect();
    assert_eq!(text_hits.len(), 2, "expected 2 text hits for 'matrix'");
    let text_ids: Vec<&str> = text_hits
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    assert!(text_ids.contains(&"d1"), "text hits should include d1");
    assert!(text_ids.contains(&"d3"), "text hits should include d3");

    // kNN hits: closest to [0.9, 0.1, 0.0] should include d1 (nearest)
    let knn_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 3, "expected 3 kNN hits (k=3)");
    assert_eq!(
        knn_hits[0]["_id"], "d1",
        "d1 should be nearest vector match"
    );
}

#[tokio::test]
async fn search_shard_dsl_knn_returns_empty_when_no_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("no-vec-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "novecs-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index text-only documents (no vectors)
    assert!(
        index_doc_with_vectors(
            &mut client,
            "novecs-idx",
            0,
            "d1",
            serde_json::json!({"title": "text only doc"})
        )
        .await
    );
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
    let all_hits: Vec<serde_json::Value> = resp
        .hits
        .iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    let knn_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert!(
        knn_hits.is_empty(),
        "no kNN hits expected when no vectors indexed"
    );
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
    assert!(
        resp.error.contains("not found"),
        "error should mention shard not found: {}",
        resp.error
    );
}

#[tokio::test]
async fn search_shard_dsl_knn_with_filter_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("filter-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "filter-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index docs with text + vectors
    assert!(
        index_doc_with_vectors(
            &mut client,
            "filter-idx",
            0,
            "d1",
            serde_json::json!({"title": "rust search", "emb": [1.0, 0.0, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "filter-idx",
            0,
            "d2",
            serde_json::json!({"title": "python web", "emb": [0.9, 0.1, 0.0]})
        )
        .await
    );
    assert!(
        index_doc_with_vectors(
            &mut client,
            "filter-idx",
            0,
            "d3",
            serde_json::json!({"title": "rust compiler", "emb": [0.8, 0.2, 0.0]})
        )
        .await
    );
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
    let all_hits: Vec<serde_json::Value> = resp
        .hits
        .iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();

    // kNN hits should only contain d1 and d3 (matching "rust"), not d2 ("python")
    let knn_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert_eq!(knn_hits.len(), 2, "expected 2 filtered kNN hits (d1, d3)");
    let ids: Vec<&str> = knn_hits
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"d1"), "d1 should pass the filter");
    assert!(ids.contains(&"d3"), "d3 should pass the filter");
    assert!(!ids.contains(&"d2"), "d2 ('python') should be filtered out");
}

#[tokio::test]
async fn search_shard_dsl_knn_filter_no_matches_returns_empty_knn() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("nofilter-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "nofilter-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    assert!(
        index_doc_with_vectors(
            &mut client,
            "nofilter-idx",
            0,
            "d1",
            serde_json::json!({"title": "rust only", "emb": [1.0, 0.0, 0.0]})
        )
        .await
    );
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

    assert!(
        resp.success,
        "should succeed even with no matches: {}",
        resp.error
    );
    let all_hits: Vec<serde_json::Value> = resp
        .hits
        .iter()
        .filter_map(|h| serde_json::from_slice::<serde_json::Value>(&h.source_json).ok())
        .collect();
    let knn_hits: Vec<&serde_json::Value> = all_hits
        .iter()
        .filter(|h| h.get("_knn_field").is_some())
        .collect();
    assert!(
        knn_hits.is_empty(),
        "no kNN hits when filter matches nothing"
    );
}

// ─── Update document integration tests (get + modify + re-index via gRPC) ───

#[tokio::test]
async fn update_document_merges_fields_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("update-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "update-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index a document
    let payload = serde_json::json!({"title": "The Matrix", "year": 1999, "rating": 8.7});
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "update-idx".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    refresh_all(&sm);

    // Get the document
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "update-idx".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    let mut source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["title"], "The Matrix");
    assert_eq!(source["year"], 1999);

    // Merge partial update into existing source
    let partial = serde_json::json!({"rating": 9.0, "genre": "scifi"});
    if let (Some(existing), Some(update)) = (source.as_object_mut(), partial.as_object()) {
        for (k, v) in update {
            existing.insert(k.clone(), v.clone());
        }
    }

    // Re-index the merged document
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "update-idx".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
            payload_json: serde_json::to_vec(&source).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    refresh_all(&sm);

    // Verify the merged document
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "update-idx".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    let updated: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(updated["title"], "The Matrix", "original field preserved");
    assert_eq!(updated["year"], 1999, "original field preserved");
    assert_eq!(updated["rating"], 9.0, "updated field changed");
    assert_eq!(updated["genre"], "scifi", "new field added");
}

#[tokio::test]
async fn update_nonexistent_document_returns_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("update-404-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "update-404-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index a doc so the shard exists
    let payload = serde_json::json!({"title": "exists"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "update-404-idx".into(),
            shard_id: 0,
            doc_id: "exists".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap();
    refresh_all(&sm);

    // Try to get a nonexistent doc (simulating what update_document does)
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "update-404-idx".into(),
            shard_id: 0,
            doc_id: "nonexistent".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found, "document should not be found");
}

// ─── Recovery & Checkpoint integration tests ────────────────────────────────

#[tokio::test]
async fn replicate_doc_returns_local_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "cp-idx");

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    // First replicate — checkpoint should be 0 (or whatever the first seq_no is)
    let payload = serde_json::json!({"title": "checkpoint test 1"});
    let resp = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "cp-idx".into(),
            shard_id: 0,
            doc_id: "cp-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            op: "index".into(),
            seq_no: 5,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(
        resp.local_checkpoint, 5,
        "local checkpoint should match the seq_no we sent"
    );

    // Second replicate with higher seq_no
    let payload2 = serde_json::json!({"title": "checkpoint test 2"});
    let resp2 = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "cp-idx".into(),
            shard_id: 0,
            doc_id: "cp-2".into(),
            payload_json: serde_json::to_vec(&payload2).unwrap(),
            op: "index".into(),
            seq_no: 10,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp2.success);
    assert_eq!(
        resp2.local_checkpoint, 10,
        "checkpoint should advance to 10"
    );
}

#[tokio::test]
async fn replicate_bulk_returns_local_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "bulk-cp-idx");

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let ops: Vec<ReplicateDocRequest> = (0..3)
        .map(|i| ReplicateDocRequest {
            index_name: String::new(),
            shard_id: 0,
            doc_id: format!("bulk-cp-{}", i),
            payload_json: serde_json::to_vec(&serde_json::json!({"n": i})).unwrap(),
            op: "index".into(),
            seq_no: 100 + i as u64,
        })
        .collect();

    let resp = client
        .replicate_bulk(tonic::Request::new(proto::ReplicateBulkRequest {
            index_name: "bulk-cp-idx".into(),
            shard_id: 0,
            ops,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(
        resp.local_checkpoint, 102,
        "checkpoint should equal highest seq_no in batch"
    );
}

#[tokio::test]
async fn primary_write_advances_global_checkpoint() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("gc-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("gc-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(&primary_cm, &replica_cm, "gc-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Write 3 documents — replication succeeds, global checkpoint should advance
    for i in 0..3 {
        let payload = serde_json::json!({"msg": format!("gc-doc-{}", i)});
        let resp = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "gc-idx".into(),
                shard_id: 0,
                doc_id: format!("gc-{}", i),
                payload_json: serde_json::to_vec(&payload).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success, "index_doc failed: {}", resp.error);
    }

    // After successful replication, primary's global checkpoint should be > 0
    let primary_engine = primary_sm.get_shard("gc-idx", 0).unwrap();
    let global_cp = primary_engine.global_checkpoint();
    assert!(
        global_cp > 0,
        "global checkpoint should advance after successful replication, got {}",
        global_cp
    );

    // And the ISR tracker should know about the replica
    let isr =
        primary_sm
            .isr_tracker
            .in_sync_replicas("gc-idx", 0, primary_engine.local_checkpoint());
    assert!(!isr.is_empty(), "ISR should contain the replica node");
}

#[tokio::test]
async fn recover_replica_returns_translog_entries() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("recovery-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "recover-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index some documents to build up the translog
    for i in 0..5 {
        let payload = serde_json::json!({"data": format!("doc-{}", i)});
        let resp = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "recover-idx".into(),
                shard_id: 0,
                doc_id: format!("rec-{}", i),
                payload_json: serde_json::to_vec(&payload).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
    }

    // Request recovery from checkpoint 2 — should get entries 3 and 4
    let resp = client
        .recover_replica(tonic::Request::new(proto::RecoverReplicaRequest {
            index_name: "recover-idx".into(),
            shard_id: 0,
            local_checkpoint: 2,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "recovery should succeed: {}", resp.error);
    assert_eq!(
        resp.ops_replayed, 2,
        "should have 2 entries above checkpoint 2"
    );
    assert_eq!(resp.operations.len(), 2, "should return 2 operations");

    // Verify the operations have correct seq_nos
    assert_eq!(resp.operations[0].seq_no, 3);
    assert_eq!(resp.operations[1].seq_no, 4);
    assert_eq!(resp.operations[0].op, "index");
}

#[tokio::test]
async fn recover_replica_returns_empty_when_caught_up() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("recovery-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "caught-up-idx");

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    // Index 2 docs
    for i in 0..2 {
        let payload = serde_json::json!({"data": i});
        client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "caught-up-idx".into(),
                shard_id: 0,
                doc_id: format!("cu-{}", i),
                payload_json: serde_json::to_vec(&payload).unwrap(),
            }))
            .await
            .unwrap();
    }

    // Request recovery from checkpoint 100 — should get 0 entries
    let resp = client
        .recover_replica(tonic::Request::new(proto::RecoverReplicaRequest {
            index_name: "caught-up-idx".into(),
            shard_id: 0,
            local_checkpoint: 100,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(resp.ops_replayed, 0);
    assert!(resp.operations.is_empty());
}

// ─── Checkpoint + ISR integration tests ────────────────────────────────────

#[tokio::test]
async fn bulk_replication_advances_global_checkpoint() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("bulk-gc".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("bulk-gc".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "bgc-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Bulk index 5 docs
    let documents_json: Vec<Vec<u8>> = (0..5)
        .map(|i| {
            let payload =
                serde_json::json!({"_id": format!("b-{}", i), "msg": format!("bulk-{}", i)});
            serde_json::to_vec(&payload).unwrap()
        })
        .collect();

    let resp = client
        .bulk_index(tonic::Request::new(proto::ShardBulkRequest {
            index_name: "bgc-idx".into(),
            shard_id: 0,
            documents_json,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success, "bulk index failed: {}", resp.error);

    let engine = primary_sm.get_shard("bgc-idx", 0).unwrap();
    assert!(
        engine.global_checkpoint() > 0,
        "global checkpoint should advance after bulk replication, got {}",
        engine.global_checkpoint()
    );
    assert!(
        engine.local_checkpoint() > 0,
        "local checkpoint should be set after bulk write"
    );
}

#[tokio::test]
async fn delete_replication_advances_global_checkpoint() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("del-gc".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("del-gc".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "dgc-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Index a doc first
    let payload = serde_json::json!({"msg": "to-delete"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "dgc-idx".into(),
            shard_id: 0,
            doc_id: "del-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap();

    let cp_after_index = primary_sm
        .get_shard("dgc-idx", 0)
        .unwrap()
        .global_checkpoint();

    // Delete the doc
    let resp = client
        .delete_doc(tonic::Request::new(proto::ShardDeleteRequest {
            index_name: "dgc-idx".into(),
            shard_id: 0,
            doc_id: "del-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    let cp_after_delete = primary_sm
        .get_shard("dgc-idx", 0)
        .unwrap()
        .global_checkpoint();
    assert!(
        cp_after_delete > cp_after_index,
        "global checkpoint should advance after delete replication: {} > {}",
        cp_after_delete,
        cp_after_index
    );
}

#[tokio::test]
async fn isr_tracker_updated_after_replication() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("isr-it".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_grpc_server(replica_cm.clone(), replica_sm).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("isr-it".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "isr-idx", replica_addr.port());

    let primary_addr = start_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Index docs to trigger replication → ISR update
    for i in 0..3 {
        let payload = serde_json::json!({"data": i});
        client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "isr-idx".into(),
                shard_id: 0,
                doc_id: format!("isr-{}", i),
                payload_json: serde_json::to_vec(&payload).unwrap(),
            }))
            .await
            .unwrap();
    }

    // ISR tracker should have the replica checkpoint
    let engine = primary_sm.get_shard("isr-idx", 0).unwrap();
    let isr = primary_sm
        .isr_tracker
        .in_sync_replicas("isr-idx", 0, engine.local_checkpoint());
    assert!(
        !isr.is_empty(),
        "ISR should contain the replica after successful replication"
    );

    // Replica checkpoints should be tracked
    let cps = primary_sm.isr_tracker.replica_checkpoints("isr-idx", 0);
    assert!(!cps.is_empty(), "replica checkpoints should be recorded");
}

#[tokio::test]
async fn recover_replica_ops_have_correct_fields() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("op-fields".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "opf-idx");

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    // Index a doc, then delete it
    let payload = serde_json::json!({"title": "recover-test"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "opf-idx".into(),
            shard_id: 0,
            doc_id: "opf-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
        }))
        .await
        .unwrap();

    client
        .delete_doc(tonic::Request::new(proto::ShardDeleteRequest {
            index_name: "opf-idx".into(),
            shard_id: 0,
            doc_id: "opf-1".into(),
        }))
        .await
        .unwrap();

    // Recover from seq_no 0 — should get both index and delete ops
    let resp = client
        .recover_replica(tonic::Request::new(proto::RecoverReplicaRequest {
            index_name: "opf-idx".into(),
            shard_id: 0,
            local_checkpoint: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert!(!resp.operations.is_empty(), "should have recovery ops");

    // Verify each op has required fields
    for op in &resp.operations {
        assert!(op.seq_no <= 10, "seq_no should be reasonable");
        assert!(
            op.op == "index" || op.op == "delete",
            "op should be index or delete, got: {}",
            op.op
        );
        assert!(!op.doc_id.is_empty(), "doc_id should not be empty");
    }
}

// ─── Shard Stats integration tests ─────────────────────────────────────────

#[tokio::test]
async fn get_shard_stats_returns_empty_when_no_shards() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let resp = client
        .get_shard_stats(tonic::Request::new(proto::ShardStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.shards.is_empty());
}

#[tokio::test]
async fn get_shard_stats_returns_doc_counts_for_open_shards() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "stats-test");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index some docs into shard 0
    for i in 0..5 {
        let payload = serde_json::json!({"title": format!("doc-{}", i)});
        let resp = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "stats-test".into(),
                shard_id: 0,
                doc_id: format!("doc-{}", i),
                payload_json: serde_json::to_vec(&payload).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success, "index_doc failed: {}", resp.error);
    }

    refresh_all(&sm);

    let resp = client
        .get_shard_stats(tonic::Request::new(proto::ShardStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.shards.len(), 1);
    assert_eq!(resp.shards[0].index_name, "stats-test");
    assert_eq!(resp.shards[0].shard_id, 0);
    assert_eq!(resp.shards[0].doc_count, 5);
}

#[tokio::test]
async fn get_shard_stats_returns_multiple_shards() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_multi_shard_single_node_cluster_state(&cm, "multi-shard", 2);

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // Index docs into two different shards on same index
    for shard in [0, 1] {
        let count = if shard == 0 { 3 } else { 7 };
        for i in 0..count {
            let payload = serde_json::json!({"n": i});
            let resp = client
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "multi-shard".into(),
                    shard_id: shard,
                    doc_id: format!("s{}-doc-{}", shard, i),
                    payload_json: serde_json::to_vec(&payload).unwrap(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(resp.success);
        }
    }

    refresh_all(&sm);

    let resp = client
        .get_shard_stats(tonic::Request::new(proto::ShardStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.shards.len(), 2);

    let mut by_shard: HashMap<u32, u64> = HashMap::new();
    for s in &resp.shards {
        by_shard.insert(s.shard_id, s.doc_count);
    }
    assert_eq!(by_shard[&0], 3);
    assert_eq!(by_shard[&1], 7);
}
