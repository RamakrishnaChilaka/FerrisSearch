//! Integration tests for primary-replica replication.
//!
//! These tests spin up real gRPC transport servers (in-process) and exercise the
//! full write → replicate → read path, similar to OpenSearch's ESIntegTestCase.

use datafusion::arrow::array::StringArray;
use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{
    FieldMapping, FieldType, IndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use ferrissearch::search::{QueryClause, SearchRequest};
use ferrissearch::shard::ShardManager;
#[cfg(feature = "transport-tls")]
use ferrissearch::transport::TonicTlsConnector;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::proto::{
    self, JoinRequest, ReplicateBulkRequest, ReplicateDocRequest, ShardBulkRequest,
    ShardDeleteRequest, ShardDocRequest, ShardGetRequest, ShardSearchDslRequest,
    ShardSearchRequest, StartPeerRecoveryRequest,
};
use ferrissearch::transport::server::{
    create_transport_service_for_test, create_transport_service_with_raft,
};
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
    start_grpc_server_for_node(cluster_manager, shard_manager, "node-1").await
}

async fn start_primary_grpc_server(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
) -> std::net::SocketAddr {
    start_grpc_server_for_node(cluster_manager, shard_manager, "primary-node").await
}

async fn start_replica_grpc_server(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
) -> std::net::SocketAddr {
    start_grpc_server_for_node(cluster_manager, shard_manager, "replica-node").await
}

async fn start_grpc_server_for_node(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    local_node_id: &str,
) -> std::net::SocketAddr {
    start_grpc_server_for_node_with_handle(cluster_manager, shard_manager, local_node_id)
        .await
        .0
}

async fn start_grpc_server_for_node_with_handle(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    local_node_id: &str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let transport_client = TransportClient::new();
    let service = create_transport_service_for_test(
        cluster_manager,
        shard_manager,
        transport_client,
        Arc::new(ferrissearch::tasks::TaskManager::new()),
        local_node_id.into(),
    );

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
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
    let service = create_transport_service_for_test(
        cluster_manager,
        shard_manager,
        transport_client,
        Arc::new(ferrissearch::tasks::TaskManager::new()),
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
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new(format!("{index_name}-uuid")),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
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
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
    }
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new(format!("{index_name}-uuid")),
        number_of_shards: shards,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
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
    setup_two_node_cluster_state_with_membership(
        primary_cm,
        replica_cm,
        index_name,
        replica_port,
        true,
    );
}

fn setup_two_node_cluster_state_with_membership(
    primary_cm: &ClusterManager,
    replica_cm: &ClusterManager,
    index_name: &str,
    replica_port: u16,
    replica_in_sync: bool,
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
            primary_term: 1,
            replicas: vec!["replica-node".into()],
            in_sync_replicas: replica_in_sync
                .then(|| "replica-node".to_string())
                .into_iter()
                .collect(),
            unassigned_replicas: 0,
        },
    );
    cs.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new(format!("{index_name}-uuid")),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing,
        mappings: std::collections::HashMap::new(),
        dynamic: Default::default(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    primary_cm.update_state(cs.clone());
    replica_cm.update_state(cs);
}

fn setup_replica_target_state(cm: &ClusterManager, index_name: &str, primary_term: u64) {
    let mut state = cm.get_state();
    state.add_node(DomainNodeInfo {
        id: "primary-node".into(),
        name: "primary".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    state.add_node(DomainNodeInfo {
        id: "replica-node".into(),
        name: "replica".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    state.add_index(IndexMetadata {
        name: index_name.into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new(format!("{index_name}-uuid")),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "primary-node".into(),
                primary_term,
                replicas: vec!["replica-node".into()],
                in_sync_replicas: vec!["replica-node".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut(index_name)
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    cm.update_state(state);
}

fn install_recovered_replica_fixture(
    cluster_manager: &ClusterManager,
    shard_manager: &ShardManager,
    index_name: &str,
) {
    let state = cluster_manager.get_state();
    let metadata = &state.indices[index_name];
    assert!(
        metadata.shard_routing[&0].is_replica_in_sync("replica-node"),
        "fixture must represent an already-recovered replica"
    );
    assert_eq!(
        state.shard_allocation_id(index_name, 0, "replica-node"),
        Some(1)
    );
    shard_manager
        .open_shard_with_settings(
            index_name,
            0,
            &metadata.mappings,
            &metadata.settings,
            metadata.uuid.as_str(),
        )
        .unwrap();
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
            ..Default::default()
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
            ..Default::default()
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
async fn transport_client_accepts_server_generated_document_id() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("transport-client-auto-id".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "auto-id-index");

    let addr = start_grpc_server(cm, sm).await;
    let node = DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: addr.port(),
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    };

    let response = TransportClient::new()
        .forward_index_to_shard(
            &node,
            "auto-id-index",
            0,
            "",
            &serde_json::json!({"message": "generated through transport client"}),
        )
        .await
        .unwrap();

    assert!(
        response["_id"]
            .as_str()
            .is_some_and(|doc_id| !doc_id.is_empty())
    );
    assert_eq!(response["_seq_no"], 0);
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
            ..Default::default()
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
            ..Default::default()
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
async fn primary_write_handlers_reject_non_primary_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_manager = Arc::new(ClusterManager::new("non-primary-write".into()));
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cluster_manager, "non-primary-index");
    let mut state = cluster_manager.get_state();
    state.add_node(DomainNodeInfo {
        id: "node-2".into(),
        name: "node-2".into(),
        host: "127.0.0.1".into(),
        transport_port: 9302,
        http_port: 9202,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    state
        .indices
        .get_mut("non-primary-index")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary = "node-2".into();
    cluster_manager.update_state(state);

    let addr = start_grpc_server(cluster_manager, shard_manager.clone()).await;
    let mut client = connect_client(addr).await;

    let index_response = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "non-primary-index".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!index_response.success);
    assert!(index_response.error.contains("not the primary"));

    let bulk_response = client
        .bulk_index(tonic::Request::new(ShardBulkRequest {
            index_name: "non-primary-index".into(),
            shard_id: 0,
            documents_json: vec![
                serde_json::to_vec(&serde_json::json!({
                    "_doc_id": "doc-2",
                    "_source": {"value": 2}
                }))
                .unwrap(),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!bulk_response.success);
    assert!(bulk_response.error.contains("not the primary"));

    let delete_response = client
        .delete_doc(tonic::Request::new(ShardDeleteRequest {
            index_name: "non-primary-index".into(),
            shard_id: 0,
            doc_id: "doc-1".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!delete_response.success);
    assert!(delete_response.error.contains("not the primary"));

    assert!(
        shard_manager.get_shard("non-primary-index", 0).is_none(),
        "rejected writes must not open or mutate a local shard"
    );
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
            ..Default::default()
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
            ..Default::default()
        }))
        .await
        .unwrap();

    let resp = client
        .delete_doc(tonic::Request::new(ShardDeleteRequest {
            index_name: "del-idx".into(),
            shard_id: 0,
            doc_id: "doomed".into(),
            ..Default::default()
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
            ..Default::default()
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
            index_uuid: "replica-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
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
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["color"], "blue");

    let entries = sm
        .get_shard("replica-idx", 0)
        .unwrap()
        .retained_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq_no, 0);
}

#[tokio::test]
async fn out_of_order_replica_delivery_keeps_the_newer_document_value() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_manager = Arc::new(ClusterManager::new("d1-ordering".into()));
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cluster_manager, "d1-ordering");

    let address = start_grpc_server(cluster_manager, shard_manager.clone()).await;
    let mut client = connect_client(address).await;
    for (seq_no, value) in [(1, 2), (0, 1)] {
        let response = client
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                index_name: "d1-ordering".into(),
                shard_id: 0,
                doc_id: "shared".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": value})).unwrap(),
                op: "index".into(),
                seq_no,
                index_uuid: "d1-ordering-uuid".into(),
                primary_term: Some(1),
                target_allocation_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
    }

    refresh_all(&shard_manager);
    let response = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "d1-ordering".into(),
            shard_id: 0,
            doc_id: "shared".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.found);
    let source: serde_json::Value = serde_json::from_slice(&response.source_json).unwrap();
    assert_eq!(source["value"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_reordering_survives_promotion_and_new_write() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_manager = Arc::new(ClusterManager::new("d1-full-ordering".into()));
    setup_replica_target_state(&cluster_manager, "d1-full-ordering", 1);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    install_recovered_replica_fixture(&cluster_manager, &shard_manager, "d1-full-ordering");
    let (address, server) = start_grpc_server_for_node_with_handle(
        cluster_manager.clone(),
        shard_manager.clone(),
        "replica-node",
    )
    .await;
    let mut client = connect_client(address).await;

    for offset in 0..10u64 {
        let response = client
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                index_name: "d1-full-ordering".into(),
                shard_id: 0,
                doc_id: format!("doc-{offset}"),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 20 + offset}))
                    .unwrap(),
                op: "index".into(),
                seq_no: 20 + offset,
                index_uuid: "d1-full-ordering-uuid".into(),
                primary_term: Some(1),
                target_allocation_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
    }
    let bulk = (0..20u64)
        .map(|seq_no| ReplicateDocRequest {
            index_name: "d1-full-ordering".into(),
            shard_id: 0,
            doc_id: format!("doc-{seq_no}"),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": seq_no})).unwrap(),
            op: "index".into(),
            seq_no,
            index_uuid: "d1-full-ordering-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        })
        .collect();
    let response = client
        .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
            index_name: "d1-full-ordering".into(),
            shard_id: 0,
            ops: bulk,
            index_uuid: "d1-full-ordering-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    refresh_all(&shard_manager);
    for offset in 0..10u64 {
        assert_eq!(
            shard_manager
                .get_shard("d1-full-ordering", 0)
                .unwrap()
                .get_document(&format!("doc-{offset}"))
                .unwrap()
                .unwrap()["value"],
            20 + offset
        );
    }

    let mut promoted = cluster_manager.get_state();
    let routing = promoted
        .indices
        .get_mut("d1-full-ordering")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap();
    routing.primary = "replica-node".into();
    routing.primary_term = 2;
    routing.replicas.clear();
    routing.in_sync_replicas.clear();
    routing.unassigned_replicas = 1;
    promoted
        .shard_allocations
        .get_mut("d1-full-ordering")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    cluster_manager.update_state(promoted);
    let response = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "d1-full-ordering".into(),
            shard_id: 0,
            doc_id: "post-promotion".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": "promoted"})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    shard_manager
        .get_shard("d1-full-ordering", 0)
        .unwrap()
        .refresh()
        .unwrap();
    assert_eq!(
        shard_manager
            .get_shard("d1-full-ordering", 0)
            .unwrap()
            .get_document("post-promotion")
            .unwrap()
            .unwrap()["value"],
        "promoted"
    );
    for offset in 0..10u64 {
        assert_eq!(
            shard_manager
                .get_shard("d1-full-ordering", 0)
                .unwrap()
                .get_document(&format!("doc-{offset}"))
                .unwrap()
                .unwrap()["value"],
            20 + offset
        );
    }
    server.abort();
}

#[tokio::test]
async fn replicate_doc_delete_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "rep-del-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
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
            index_uuid: "rep-del-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
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
            seq_no: 1,
            index_uuid: "rep-del-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "{}", resp.error);

    // Verify deleted
    refresh_all(&sm);
    let resp = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "rep-del-idx".into(),
            shard_id: 0,
            doc_id: "to-delete".into(),
            ..Default::default()
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
            index_name: "bulk-rep-idx".into(),
            shard_id: 0,
            doc_id: format!("bulk-rep-{i}"),
            payload_json: serde_json::to_vec(&serde_json::json!({"n": i})).unwrap(),
            op: "index".into(),
            seq_no: i,
            index_uuid: "bulk-rep-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        })
        .collect();

    let resp = client
        .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
            index_name: "bulk-rep-idx".into(),
            shard_id: 0,
            ops,
            index_uuid: "bulk-rep-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
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
                doc_id: format!("bulk-rep-{i}"),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found, "bulk-rep-{i} not found");
    }

    let entries = sm
        .get_shard("bulk-rep-idx", 0)
        .unwrap()
        .retained_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].seq_no, 0);
    assert_eq!(entries[1].seq_no, 1);
    assert_eq!(entries[2].seq_no, 2);
}

#[tokio::test]
async fn stale_primary_replication_is_rejected_by_promoted_target() {
    let dir = tempfile::tempdir().unwrap();
    let source_cm = Arc::new(ClusterManager::new("stale-primary-source".into()));
    let target_cm = Arc::new(ClusterManager::new("stale-primary-target".into()));
    let target_sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let mut target_state = target_cm.get_state();
    target_state.add_node(DomainNodeInfo {
        id: "old-primary".into(),
        name: "old-primary".into(),
        host: "127.0.0.1".into(),
        transport_port: 29998,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    target_state.add_node(DomainNodeInfo {
        id: "promoted-target".into(),
        name: "promoted-target".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    target_state.add_index(IndexMetadata {
        name: "stale-primary".into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new("stale-primary-uuid"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "old-primary".into(),
                primary_term: 1,
                replicas: vec!["promoted-target".into()],
                in_sync_replicas: vec!["promoted-target".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    {
        let routing = target_state
            .indices
            .get_mut("stale-primary")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap();
        routing.primary = "promoted-target".into();
        routing.primary_term = 3;
        routing.replicas.clear();
        routing.in_sync_replicas.clear();
    }
    target_cm.update_state(target_state);
    target_sm
        .open_shard_with_settings(
            "stale-primary",
            0,
            &HashMap::new(),
            &ferrissearch::cluster::state::IndexSettings::default(),
            "stale-primary-uuid",
        )
        .unwrap();

    let target_addr =
        start_grpc_server_for_node(target_cm, target_sm.clone(), "promoted-target").await;

    let mut source_state = ClusterManager::new("source-template".into()).get_state();
    source_state.add_node(DomainNodeInfo {
        id: "old-primary".into(),
        name: "old-primary".into(),
        host: "127.0.0.1".into(),
        transport_port: 29998,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    source_state.add_node(DomainNodeInfo {
        id: "promoted-target".into(),
        name: "promoted-target".into(),
        host: "127.0.0.1".into(),
        transport_port: target_addr.port(),
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    source_state.add_index(IndexMetadata {
        name: "stale-primary".into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new("stale-primary-uuid"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "old-primary".into(),
                primary_term: 1,
                replicas: vec!["promoted-target".into()],
                in_sync_replicas: vec!["promoted-target".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    source_cm.update_state(source_state);

    let result = ferrissearch::replication::replicate_write(
        &TransportClient::new(),
        &source_cm.get_state(),
        "stale-primary",
        0,
        "stale-write",
        &serde_json::json!({"value": "old-primary"}),
        "index",
        0,
        1,
    )
    .await;

    assert!(
        result.is_err(),
        "the promoted target must reject replication from the stale primary term"
    );
    assert!(
        target_sm
            .get_shard("stale-primary", 0)
            .unwrap()
            .get_document("stale-write")
            .unwrap()
            .is_none(),
        "stale-primary replication must not mutate the promoted copy"
    );
}

#[tokio::test]
async fn stale_target_allocation_recovery_start_is_rejected_before_snapshot_setup() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_cm = Arc::new(ClusterManager::new("recovery-source".into()));
    let target_cm = Arc::new(ClusterManager::new("recovery-target".into()));
    let source_sm = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));

    let mut target_state = target_cm.get_state();
    target_state.add_node(DomainNodeInfo {
        id: "primary-node".into(),
        name: "primary".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    target_state.add_node(DomainNodeInfo {
        id: "replica-node".into(),
        name: "replica".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    target_state.add_index(IndexMetadata {
        name: "recovery-aba".into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new("recovery-aba-uuid"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "primary-node".into(),
                primary_term: 1,
                replicas: vec!["replica-node".into()],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: ferrissearch::cluster::state::IndexSettings::default(),
    });
    let target_allocation_id = target_state
        .shard_allocation_id("recovery-aba", 0, "replica-node")
        .unwrap();
    target_cm.update_state(target_state.clone());

    let mut source_state = target_state;
    source_state
        .shard_allocations
        .get_mut("recovery-aba")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .replicas
        .insert("replica-node".into(), target_allocation_id + 1);
    source_cm.update_state(source_state);

    let source_addr = start_grpc_server_for_node(source_cm, source_sm, "primary-node").await;
    let mut client = connect_client(source_addr).await;
    let error = client
        .start_peer_recovery(tonic::Request::new(StartPeerRecoveryRequest {
            index_name: "recovery-aba".into(),
            index_uuid: "recovery-aba-uuid".into(),
            shard_id: 0,
            target_node_id: "replica-node".into(),
            target_allocation_id: Some(target_allocation_id),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("allocation"));
}

#[tokio::test]
async fn replica_apply_rejects_uuid_allocation_term_and_missing_identity_fields() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("replica-fence-validation".into()));
    setup_replica_target_state(&cm, "fenced-replica", 3);
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shards
        .open_shard_with_settings(
            "fenced-replica",
            0,
            &HashMap::new(),
            &ferrissearch::cluster::state::IndexSettings::default(),
            "fenced-replica-uuid",
        )
        .unwrap();
    let address = start_grpc_server_for_node(cm, shards.clone(), "replica-node").await;
    let mut client = connect_client(address).await;

    let request = |doc_id: &str,
                   index_uuid: &str,
                   primary_term: Option<u64>,
                   allocation_id: Option<u64>| ReplicateDocRequest {
        index_name: "fenced-replica".into(),
        shard_id: 0,
        doc_id: doc_id.into(),
        payload_json: serde_json::to_vec(&serde_json::json!({"value": doc_id})).unwrap(),
        op: "index".into(),
        seq_no: 0,
        index_uuid: index_uuid.into(),
        primary_term,
        target_allocation_id: allocation_id,
    };

    for invalid in [
        request("wrong-uuid", "other-uuid", Some(3), Some(1)),
        request("wrong-allocation", "fenced-replica-uuid", Some(3), Some(2)),
        request("stale-term", "fenced-replica-uuid", Some(2), Some(1)),
    ] {
        let response = client
            .replicate_doc(tonic::Request::new(invalid))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success, "invalid replica identity was accepted");
    }
    for missing in [
        request("missing-term", "fenced-replica-uuid", None, Some(1)),
        request("missing-allocation", "fenced-replica-uuid", Some(3), None),
    ] {
        let error = client
            .replicate_doc(tonic::Request::new(missing))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    let engine = shards.get_shard("fenced-replica", 0).unwrap();
    assert_eq!(engine.doc_count(), 0);
    assert!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .is_empty()
    );
}

#[tokio::test]
async fn replica_fence_is_persisted_before_ack_and_restored_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let first_cm = Arc::new(ClusterManager::new("durable-fence".into()));
    setup_replica_target_state(&first_cm, "durable-fence", 1);
    let first_shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    first_shards
        .open_shard_with_settings(
            "durable-fence",
            0,
            &HashMap::new(),
            &ferrissearch::cluster::state::IndexSettings::default(),
            "durable-fence-uuid",
        )
        .unwrap();
    let (first_address, first_server) =
        start_grpc_server_for_node_with_handle(first_cm, first_shards.clone(), "replica-node")
            .await;
    let mut first_client = connect_client(first_address).await;
    let accepted = first_client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "durable-fence".into(),
            shard_id: 0,
            doc_id: "new-term".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 3})).unwrap(),
            op: "index".into(),
            seq_no: 0,
            index_uuid: "durable-fence-uuid".into(),
            primary_term: Some(3),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(accepted.success, "{}", accepted.error);
    assert_eq!(
        first_shards
            .copy_identity("durable-fence", 0)
            .unwrap()
            .replica_fence,
        3
    );
    drop(first_client);
    first_server.abort();
    let _ = first_server.await;
    first_shards.quarantine_shard_copy("durable-fence", 0);
    drop(first_shards);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let restarted_cm = Arc::new(ClusterManager::new("durable-fence".into()));
    setup_replica_target_state(&restarted_cm, "durable-fence", 1);
    let restarted_shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let restarted_address =
        start_grpc_server_for_node(restarted_cm, restarted_shards.clone(), "replica-node").await;
    let mut restarted_client = connect_client(restarted_address).await;
    let rejected = restarted_client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "durable-fence".into(),
            shard_id: 0,
            doc_id: "stale-after-restart".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            op: "index".into(),
            seq_no: 1,
            index_uuid: "durable-fence-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!rejected.success);
    assert!(
        rejected.error.contains("below local fence"),
        "{}",
        rejected.error
    );
    let engine = restarted_shards.get_shard("durable-fence", 0).unwrap();
    assert!(engine.get_document("new-term").unwrap().is_some());
    assert!(
        engine
            .get_document("stale-after-restart")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn bulk_replication_validates_common_identity_before_first_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("bulk-fence".into()));
    setup_replica_target_state(&cm, "bulk-fence", 1);
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shards
        .open_shard_with_settings(
            "bulk-fence",
            0,
            &HashMap::new(),
            &ferrissearch::cluster::state::IndexSettings::default(),
            "bulk-fence-uuid",
        )
        .unwrap();
    let address = start_grpc_server_for_node(cm, shards.clone(), "replica-node").await;
    let mut client = connect_client(address).await;
    let operation = |doc_id: &str, seq_no: u64, allocation_id: u64| ReplicateDocRequest {
        index_name: "bulk-fence".into(),
        shard_id: 0,
        doc_id: doc_id.into(),
        payload_json: serde_json::to_vec(&serde_json::json!({"value": doc_id})).unwrap(),
        op: "index".into(),
        seq_no,
        index_uuid: "bulk-fence-uuid".into(),
        primary_term: Some(2),
        target_allocation_id: Some(allocation_id),
    };
    let missing_term = client
        .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
            index_name: "bulk-fence".into(),
            shard_id: 0,
            ops: vec![operation("missing-term", 0, 1)],
            index_uuid: "bulk-fence-uuid".into(),
            primary_term: None,
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap_err();
    assert_eq!(missing_term.code(), tonic::Code::InvalidArgument);
    let error = client
        .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
            index_name: "bulk-fence".into(),
            shard_id: 0,
            ops: vec![
                operation("first", 0, 1),
                operation("wrong-allocation", 1, 2),
            ],
            index_uuid: "bulk-fence-uuid".into(),
            primary_term: Some(2),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let engine = shards.get_shard("bulk-fence", 0).unwrap();
    assert!(engine.get_document("first").unwrap().is_none());
    assert!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .is_empty()
    );
    assert_eq!(
        shards.copy_identity("bulk-fence", 0).unwrap().replica_fence,
        1,
        "invalid bulk must not raise the fence before envelope validation completes"
    );
}

#[tokio::test]
async fn join_existing_raft_voter_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        ferrissearch::consensus::create_raft_instance_mem(1, "cluster-test".into())
            .await
            .unwrap();
    ferrissearch::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let existing = DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Data],
        raft_node_id: 1,
    };
    assert_eq!(
        raft.client_write(ferrissearch::consensus::types::ClusterCommand::AddNode {
            node: existing.clone(),
        })
        .await
        .unwrap()
        .data,
        ferrissearch::consensus::types::ClusterResponse::Ok
    );
    let cm = Arc::new(ClusterManager::with_shared_state(shared_state));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = create_transport_service_with_raft(
        cm.clone(),
        sm,
        TransportClient::new(),
        raft,
        Arc::new(ferrissearch::tasks::TaskManager::new()),
        "node-1".into(),
    );
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut client = connect_client(addr).await;

    let join_resp = client
        .join_cluster(tonic::Request::new(JoinRequest {
            node_info: Some(proto::NodeInfo {
                id: existing.id.clone(),
                name: existing.name.clone(),
                host: existing.host.clone(),
                transport_port: u32::from(existing.transport_port),
                http_port: u32::from(existing.http_port),
                roles: vec!["data".into()],
                raft_node_id: 1,
            }),
            raft_node_id: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(join_resp.state.is_some());
    let state = join_resp.state.unwrap();
    assert_eq!(state.cluster_name, "cluster-test");
    assert_eq!(state.format_version, 1);
    assert!(state.nodes.iter().any(|n| n.id == "node-1"));

    let cs = cm.get_state();
    assert!(cs.nodes.contains_key("node-1"));
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
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

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
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "replicated-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    // Write a document to the primary
    let payload = serde_json::json!({"message": "hello from primary", "version": 1});
    let resp = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "replicated-idx".into(),
            shard_id: 0,
            doc_id: "replicated-doc".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            ..Default::default()
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
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found, "Document not replicated to replica node");
    let source: serde_json::Value = serde_json::from_slice(&resp.source_json).unwrap();
    assert_eq!(source["message"], "hello from primary");

    let entries = replica_sm
        .get_shard("replicated-idx", 0)
        .unwrap()
        .retained_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq_no, 0);
}

#[tokio::test]
async fn reserved_source_never_mutates_primary_or_replica() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("reserved-source".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("reserved-source".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "reserved-source-idx",
        replica_addr.port(),
    );
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "reserved-source-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut primary_client = connect_client(primary_addr).await;

    let error = primary_client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "reserved-source-idx".into(),
            shard_id: 0,
            doc_id: "poison".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"_seq_no": 999})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("Field [_seq_no] is a metadata field")
    );

    let error = primary_client
        .bulk_index(tonic::Request::new(ShardBulkRequest {
            index_name: "reserved-source-idx".into(),
            shard_id: 0,
            documents_json: vec![
                serde_json::to_vec(&serde_json::json!({
                    "_source": {"value": 999}
                }))
                .unwrap(),
            ],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("Field [_source] is a metadata field")
    );

    for manager in [&primary_sm, &replica_sm] {
        let engine = manager.get_shard("reserved-source-idx", 0).unwrap();
        assert_eq!(engine.sequence_stats().max_seq_no, None);
        assert!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .is_empty()
        );
    }

    let response = primary_client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "reserved-source-idx".into(),
            shard_id: 0,
            doc_id: "healthy".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);

    refresh_all(&primary_sm);
    refresh_all(&replica_sm);
    for address in [primary_addr, replica_addr] {
        let mut client = connect_client(address).await;
        let response = client
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: "reserved-source-idx".into(),
                shard_id: 0,
                doc_id: "healthy".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.found);
        let source: serde_json::Value = serde_json::from_slice(&response.source_json).unwrap();
        assert_eq!(source["value"], 1);
    }
}

#[tokio::test]
async fn out_of_sync_replica_receives_no_live_writes_and_cannot_fail_them() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("out-of-sync".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("out-of-sync".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state_with_membership(
        &primary_cm,
        &replica_cm,
        "out-of-sync-idx",
        replica_addr.port(),
        false,
    );

    let primary_addr = start_primary_grpc_server(primary_cm.clone(), primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    let first = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "out-of-sync-idx".into(),
            shard_id: 0,
            doc_id: "not-replicated".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        first.success,
        "out-of-sync replica blocked write: {}",
        first.error
    );
    assert!(
        replica_sm.get_shard("out-of-sync-idx", 0).is_none(),
        "an out-of-sync replica must not receive or open for live replication"
    );

    let unused_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unreachable_port = unused_listener.local_addr().unwrap().port();
    drop(unused_listener);
    let mut state = primary_cm.get_state();
    state.nodes.get_mut("replica-node").unwrap().transport_port = unreachable_port;
    primary_cm.update_state(state);

    let second = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "out-of-sync-idx".into(),
            shard_id: 0,
            doc_id: "still-acknowledged".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 2})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        second.success,
        "unreachable out-of-sync replica blocked write: {}",
        second.error
    );
    assert!(replica_sm.get_shard("out-of-sync-idx", 0).is_none());
}

#[tokio::test]
async fn unreachable_in_sync_replica_still_fails_live_write() {
    let unused_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unreachable_port = unused_listener.local_addr().unwrap().port();
    drop(unused_listener);

    let replica_cm = Arc::new(ClusterManager::new("required-replica".into()));
    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("required-replica".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "required-replica-idx",
        unreachable_port,
    );

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;
    let response = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "required-replica-idx".into(),
            shard_id: 0,
            doc_id: "must-fail".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.success);
    assert!(
        response.error.contains("Replication failed"),
        "{}",
        response.error
    );
}

#[tokio::test]
async fn primary_delete_replicates_to_replica_node() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("repl-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

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
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "del-repl-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm).await;
    let mut client = connect_client(primary_addr).await;

    // Index a document
    let payload = serde_json::json!({"data": "will be deleted"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap();

    // Delete it on the primary
    let resp = client
        .delete_doc(tonic::Request::new(ShardDeleteRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    // Verify deletion replicated to replica
    refresh_all(&replica_sm);
    let mut replica_client = connect_client(replica_addr).await;
    let resp = replica_client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: "del-repl-idx".into(),
            shard_id: 0,
            doc_id: "del-doc".into(),
            ..Default::default()
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
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

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
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "bulk-repl-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm).await;
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
            ..Default::default()
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
                doc_id: format!("repl-bulk-{i}"),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.found, "repl-bulk-{i} not replicated to replica");
    }

    let entries = replica_sm
        .get_shard("bulk-repl-idx", 0)
        .unwrap()
        .retained_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
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
            ..Default::default()
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
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-idx".into(),
            uuid: ferrissearch::cluster::state::IndexUuid::new(test_uuid),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            dynamic: Default::default(),
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
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-dsl-idx".into(),
            uuid: ferrissearch::cluster::state::IndexUuid::new(test_uuid),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            dynamic: Default::default(),
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
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
        uuid: ferrissearch::cluster::state::IndexUuid::new(test_uuid),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: restarted_mappings,
        dynamic: Default::default(),
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
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.register_index_uuid("restart-replay-idx", test_uuid);
        let engine = manager
            .open_shard_with_settings(
                "restart-replay-idx",
                0,
                &HashMap::new(),
                &ferrissearch::cluster::state::IndexSettings::default(),
                test_uuid,
            )
            .unwrap();
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
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(IndexMetadata {
            name: "restart-replay-idx".into(),
            uuid: ferrissearch::cluster::state::IndexUuid::new(test_uuid),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            dynamic: Default::default(),
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
        assert!(index_doc_with_vectors(&mut client, "all-idx", 0, &format!("d{i}"), payload).await);
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
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
        uuid: ferrissearch::cluster::state::IndexUuid::new("agg-idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        dynamic: Default::default(),
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
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
        uuid: ferrissearch::cluster::state::IndexUuid::new("sql-stream-idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        dynamic: Default::default(),
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
        search_after: None,
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found, "document should not be found");
}

// ─── Recovery & Checkpoint integration tests ────────────────────────────────

#[tokio::test]
async fn replicate_doc_returns_sequence_proof() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "cp-idx");

    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    // A higher sequence can be processed without advancing the contiguous checkpoint.
    let payload = serde_json::json!({"title": "checkpoint test 1"});
    let resp = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "cp-idx".into(),
            shard_id: 0,
            doc_id: "cp-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            op: "index".into(),
            seq_no: 5,
            index_uuid: "cp-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(resp.processed_checkpoint, None);
    assert_eq!(resp.persisted_checkpoint, None);
    assert!(resp.operation_processed);
    assert!(resp.operation_persisted);

    // Filling the missing prefix closes the gap through sequence 5.
    let mut checkpoint = None;
    for seq_no in 0..5 {
        let response = client
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                index_name: "cp-idx".into(),
                shard_id: 0,
                doc_id: format!("cp-prefix-{seq_no}"),
                payload_json: serde_json::to_vec(&serde_json::json!({"seq": seq_no})).unwrap(),
                op: "index".into(),
                seq_no,
                index_uuid: "cp-idx-uuid".into(),
                primary_term: Some(1),
                target_allocation_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        assert!(response.operation_processed);
        assert!(response.operation_persisted);
        checkpoint = response.processed_checkpoint;
    }
    assert_eq!(checkpoint, Some(5));

    // A second gap holds the checkpoint at 5 until 6..9 arrive.
    let payload2 = serde_json::json!({"title": "checkpoint test 2"});
    let resp2 = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "cp-idx".into(),
            shard_id: 0,
            doc_id: "cp-2".into(),
            payload_json: serde_json::to_vec(&payload2).unwrap(),
            op: "index".into(),
            seq_no: 10,
            index_uuid: "cp-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp2.success);
    assert_eq!(resp2.processed_checkpoint, Some(5));
    assert!(resp2.operation_processed);
    assert!(resp2.operation_persisted);
    for seq_no in 6..10 {
        let response = client
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                index_name: "cp-idx".into(),
                shard_id: 0,
                doc_id: format!("cp-gap-{seq_no}"),
                payload_json: serde_json::to_vec(&serde_json::json!({"seq": seq_no})).unwrap(),
                op: "index".into(),
                seq_no,
                index_uuid: "cp-idx-uuid".into(),
                primary_term: Some(1),
                target_allocation_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        checkpoint = response.processed_checkpoint;
    }
    assert_eq!(checkpoint, Some(10));
}

#[tokio::test]
async fn replicate_bulk_returns_sequence_proof() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("integ-test".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "bulk-cp-idx");

    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let ops: Vec<ReplicateDocRequest> = (0..3)
        .map(|i| ReplicateDocRequest {
            index_name: "bulk-cp-idx".into(),
            shard_id: 0,
            doc_id: format!("bulk-cp-{i}"),
            payload_json: serde_json::to_vec(&serde_json::json!({"n": i})).unwrap(),
            op: "index".into(),
            seq_no: i as u64,
            index_uuid: "bulk-cp-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        })
        .collect();

    let resp = client
        .replicate_bulk(tonic::Request::new(proto::ReplicateBulkRequest {
            index_name: "bulk-cp-idx".into(),
            shard_id: 0,
            ops,
            index_uuid: "bulk-cp-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(
        resp.processed_checkpoint,
        Some(2),
        "checkpoint should equal the batch's contiguous final sequence"
    );
    assert_eq!(resp.persisted_checkpoint, Some(2));
    assert!(resp.all_operations_processed);
    assert!(resp.all_operations_persisted);
}

#[tokio::test]
async fn async_replica_response_separates_processed_from_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("async-sequence-proof".into()));
    let sm = Arc::new(ShardManager::new_with_durability(
        dir.path(),
        Duration::from_secs(60),
        ferrissearch::wal::TranslogDurability::Async {
            sync_interval_ms: 3_600_000,
        },
    ));
    setup_single_node_cluster_state(&cm, "async-proof-idx");
    let addr = start_grpc_server(cm, sm).await;
    let mut client = connect_client(addr).await;

    let response = client
        .replicate_doc(tonic::Request::new(ReplicateDocRequest {
            index_name: "async-proof-idx".into(),
            shard_id: 0,
            doc_id: "doc".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            op: "index".into(),
            seq_no: 0,
            index_uuid: "async-proof-idx-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "{}", response.error);
    assert_eq!(response.processed_checkpoint, Some(0));
    assert_eq!(response.persisted_checkpoint, None);
    assert!(response.operation_processed);
    assert!(!response.operation_persisted);
}

#[tokio::test]
async fn sequence_state_probe_reports_exact_open_copy_and_activation() {
    let dir = tempfile::tempdir().unwrap();
    let cm = Arc::new(ClusterManager::new("sequence-probe".into()));
    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    setup_single_node_cluster_state(&cm, "probe-idx");
    let addr = start_grpc_server(cm, sm.clone()).await;
    let mut client = connect_client(addr).await;

    let write = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "probe-idx".into(),
            shard_id: 0,
            doc_id: "doc".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(write.success, "{}", write.error);

    let response = client
        .get_shard_sequence_state(tonic::Request::new(proto::GetShardSequenceStateRequest {
            index_name: "probe-idx".into(),
            index_uuid: "probe-idx-uuid".into(),
            shard_id: 0,
            allocation_id: Some(1),
            expected_primary_term: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.processed_checkpoint, Some(0));
    assert_eq!(response.persisted_checkpoint, Some(0));
    assert_eq!(response.max_seq_no, Some(0));
    assert_eq!(
        response.sequence_format_version,
        ferrissearch::engine::SEQUENCE_FORMAT_VERSION
    );
    assert!(response.active_primary);

    let error = client
        .get_shard_sequence_state(tonic::Request::new(proto::GetShardSequenceStateRequest {
            index_name: "probe-idx".into(),
            index_uuid: "probe-idx-uuid".into(),
            shard_id: 0,
            allocation_id: Some(2),
            expected_primary_term: 1,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(sm.get_shard("probe-idx", 0).is_some());
}

#[tokio::test]
async fn primary_write_advances_global_checkpoint() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("gc-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("gc-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));

    setup_two_node_cluster_state(&primary_cm, &replica_cm, "gc-idx", replica_addr.port());
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "gc-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Write 3 documents — replication succeeds, global checkpoint should advance
    for i in 0..3 {
        let payload = serde_json::json!({"msg": format!("gc-doc-{}", i)});
        let resp = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "gc-idx".into(),
                shard_id: 0,
                doc_id: format!("gc-{i}"),
                payload_json: serde_json::to_vec(&payload).unwrap(),
                ..Default::default()
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
        global_cp.is_some_and(|checkpoint| checkpoint > 0),
        "global checkpoint should advance after successful replication, got {global_cp:?}"
    );

    // And the ISR tracker should know about the replica
    let isr = primary_sm.isr_tracker.in_sync_replicas(
        "gc-idx",
        0,
        primary_engine.local_checkpoint().unwrap_or(0),
    );
    assert!(!isr.is_empty(), "ISR should contain the replica node");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_regression_conditional_rest_updates_and_mixed_bulk_keep_replica_documents_and_sequences_identical()
 {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("writes-replicated".into()));
    let replica_cm = Arc::new(ClusterManager::new("writes-replicated".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let (replica_addr, replica_server) = start_grpc_server_for_node_with_handle(
        replica_cm.clone(),
        replica_sm.clone(),
        "replica-node",
    )
    .await;
    setup_two_node_cluster_state(
        &primary_cm,
        &replica_cm,
        "writes-replicated",
        replica_addr.port(),
    );
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "writes-replicated");
    let (primary_addr, primary_server) = start_grpc_server_for_node_with_handle(
        primary_cm.clone(),
        primary_sm.clone(),
        "primary-node",
    )
    .await;
    let mut cluster_state = primary_cm.get_state();
    cluster_state
        .nodes
        .get_mut("primary-node")
        .unwrap()
        .transport_port = primary_addr.port();
    primary_cm.update_state(cluster_state.clone());
    replica_cm.update_state(cluster_state);
    let mut primary_client = connect_client(primary_addr).await;
    primary_client
        .ping(tonic::Request::new(proto::PingRequest {
            source_node_id: "primary-node".into(),
        }))
        .await
        .unwrap();
    let (raft, _) =
        ferrissearch::consensus::create_raft_instance_mem(1, "writes-replicated".into())
            .await
            .unwrap();
    let state = ferrissearch::api::AppState {
        cluster_manager: primary_cm,
        shard_manager: primary_sm.clone(),
        transport_client: TransportClient::new(),
        local_node_id: "primary-node".into(),
        raft: raft.clone(),
        worker_pools: ferrissearch::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(ferrissearch::tasks::TaskManager::new()),
        storage_manager: Arc::new(
            ferrissearch::storage::StorageManager::new_in_path(primary_dir.path()).unwrap(),
        ),
        security_manager: Arc::new(ferrissearch::security::SecurityManager::disabled()),
        remote_store_reader_cache: Arc::new(
            ferrissearch::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
        sql_group_by_scan_limit: 1_000_000,
        sql_approximate_top_k: false,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let http_server = tokio::spawn(async move {
        axum::serve(listener, ferrissearch::api::create_router(state))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let first = client
        .put(format!("{base}/writes-replicated/_doc/x"))
        .json(&serde_json::json!({"base": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);
    let first: serde_json::Value = first.json().await.unwrap();
    let term = first["_primary_term"].as_u64().unwrap();
    let conditional = format!("{base}/writes-replicated/_doc/x?if_seq_no=0&if_primary_term={term}");
    let second = client
        .put(&conditional)
        .json(&serde_json::json!({"base": 2, "before_bulk": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(
        second.json::<serde_json::Value>().await.unwrap()["_seq_no"],
        1
    );
    let rejected = client
        .put(&conditional)
        .json(&serde_json::json!({"wrong": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::CONFLICT);
    let delete_target = client
        .put(format!("{base}/writes-replicated/_doc/z"))
        .json(&serde_json::json!({"base": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_target.status(), reqwest::StatusCode::CREATED);
    let request = format!(
        "{{\"update\":{{\"_id\":\"x\"}}}}\n{{\"doc\":{{\"bulk\":4}}}}\n\
         {{\"delete\":{{\"_id\":\"z\"}}}}\n\
         {{\"create\":{{\"_id\":\"x\"}}}}\n{{\"wrong\":true}}\n\
         {{\"index\":{{\"_id\":\"x\",\"if_seq_no\":3,\"if_primary_term\":{term}}}}}\n{{\"base\":5,\"bulk\":4}}\n\
         {{\"update\":{{\"_id\":\"x\"}}}}\n{{\"doc\":{{\"after\":6}}}}\n\
         {{\"create\":{{\"_id\":\"y\"}}}}\n{{\"base\":7}}\n\
         {{\"delete\":{{\"_id\":\"y\",\"if_seq_no\":7,\"if_primary_term\":{term}}}}}\n\
         {{\"update\":{{\"_id\":\"y\"}}}}\n{{\"doc\":{{\"base\":8}},\"doc_as_upsert\":true}}\n\
         {{\"delete\":{{\"_id\":\"absent\"}}}}\n"
    );
    let response = client
        .post(format!("{base}/writes-replicated/_bulk"))
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response: serde_json::Value = response.json().await.unwrap();
    assert_eq!(response["items"].as_array().unwrap().len(), 9, "{response}");
    assert_eq!(response["items"][2]["create"]["status"], 409, "{response}");
    assert_eq!(response["items"][7]["update"]["result"], "created");
    assert_eq!(response["items"][8]["delete"]["status"], 404);
    let primary = primary_sm.get_shard("writes-replicated", 0).unwrap();
    let replica = replica_sm.get_shard("writes-replicated", 0).unwrap();
    for id in ["x", "y", "z", "absent"] {
        assert_eq!(
            primary.get_document_with_metadata(id, true).unwrap(),
            replica.get_document_with_metadata(id, true).unwrap(),
            "replica state differs for {id}"
        );
    }
    let x = primary
        .get_document_with_metadata("x", true)
        .unwrap()
        .unwrap();
    assert_eq!(
        x.source,
        serde_json::json!({"base": 5, "bulk": 4, "after": 6})
    );
    assert_eq!((x.seq_no, x.primary_term), (6, term));
    let y = primary
        .get_document_with_metadata("y", true)
        .unwrap()
        .unwrap();
    assert_eq!(y.source, serde_json::json!({"base": 8}));
    assert_eq!((y.seq_no, y.primary_term), (9, term));
    assert!(
        primary
            .get_document_with_metadata("z", true)
            .unwrap()
            .is_none()
    );
    let wal = |engine: &Arc<dyn ferrissearch::engine::SearchEngine>| {
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .into_iter()
            .map(|entry| (entry.seq_no, entry.primary_term, entry.op, entry.payload))
            .collect::<Vec<_>>()
    };
    assert_eq!(wal(&primary), wal(&replica));
    assert_eq!(primary.sequence_stats().processed_checkpoint, Some(10));
    assert_eq!(replica.sequence_stats().processed_checkpoint, Some(10));
    http_server.abort();
    primary_server.abort();
    replica_server.abort();
    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn gcp_transport_rejects_checkpoint_observations_from_previous_copy_authority() {
    for (old_uuid_suffix, old_allocation, old_term) in
        [("uuid", 1, 1), ("uuid", 2, 2), ("old-uuid", 1, 2)]
    {
        let index = "gcp-authority";
        let replica_dir = tempfile::tempdir().unwrap();
        let primary_dir = tempfile::tempdir().unwrap();
        let replica_cm = Arc::new(ClusterManager::new("gcp-authority".into()));
        let primary_cm = Arc::new(ClusterManager::new("gcp-authority".into()));
        let replica_sm = Arc::new(ShardManager::new(
            replica_dir.path(),
            Duration::from_secs(60),
        ));
        let primary_sm = Arc::new(ShardManager::new(
            primary_dir.path(),
            Duration::from_secs(60),
        ));
        let (replica_addr, replica_server) = start_grpc_server_for_node_with_handle(
            replica_cm.clone(),
            replica_sm.clone(),
            "replica-node",
        )
        .await;
        setup_two_node_cluster_state(&primary_cm, &replica_cm, index, replica_addr.port());
        for manager in [&primary_cm, &replica_cm] {
            let mut state = manager.get_state();
            let metadata = state.indices.get_mut(index).unwrap();
            metadata.shard_routing.get_mut(&0).unwrap().primary_term = 2;
            metadata.dynamic = ferrissearch::cluster::state::DynamicMapping::Strict;
            metadata.mappings.insert(
                "message".into(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            );
            manager.update_state(state);
        }
        install_recovered_replica_fixture(&replica_cm, &replica_sm, index);
        primary_sm.isr_tracker.update_replica_checkpoint(
            index,
            &format!("{index}-{old_uuid_suffix}"),
            0,
            old_term,
            Some(99),
            ferrissearch::shard::ReplicaCheckpointUpdate {
                node_id: "replica-node".into(),
                allocation_id: old_allocation,
                processed_checkpoint: Some(99),
                persisted_checkpoint: Some(99),
            },
        );
        let (primary_addr, primary_server) =
            start_grpc_server_for_node_with_handle(primary_cm, primary_sm.clone(), "primary-node")
                .await;
        let mut client = connect_client(primary_addr).await;
        let response = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: index.into(),
                shard_id: 0,
                doc_id: "new-term".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"message": "new term"}))
                    .unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
        assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
        let primary = primary_sm.get_shard(index, 0).unwrap();
        let replica = replica_sm.get_shard(index, 0).unwrap();
        assert_eq!(primary.global_checkpoint(), Some(0));
        assert_eq!(replica.sequence_stats().persisted_checkpoint, Some(0));
        assert!(primary.global_checkpoint() <= replica.sequence_stats().persisted_checkpoint);
        assert_eq!(
            primary
                .get_document_with_metadata("new-term", true)
                .unwrap(),
            replica
                .get_document_with_metadata("new-term", true)
                .unwrap(),
        );
        assert_eq!(
            primary_sm.isr_tracker.replica_checkpoints(index, 0),
            vec![("replica-node".into(), 0)],
            "old authority must not contaminate the current copy's checkpoint",
        );
        primary_server.abort();
        replica_server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_primary_receipts_match_primary_and_replica_wal() {
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_cm = Arc::new(ClusterManager::new("receipt-cluster".into()));
    let replica_sm = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;
    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("receipt-cluster".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    let index = "receipt-index";
    setup_two_node_cluster_state(&primary_cm, &replica_cm, index, replica_addr.port());
    for manager in [&primary_cm, &replica_cm] {
        let mut state = manager.get_state();
        let metadata = state.indices.get_mut(index).unwrap();
        metadata.dynamic = ferrissearch::cluster::state::DynamicMapping::Strict;
        metadata.mappings.insert(
            "message".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        manager.update_state(state);
    }
    install_recovered_replica_fixture(&replica_cm, &replica_sm, index);
    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;
    let seed = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index.into(),
            shard_id: 0,
            doc_id: "seed".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"message": "seed"})).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(seed.success, "{}", seed.error);
    assert_eq!(seed.seq_no, Some(0));

    let jobs = (0..24).map(|number| {
        let mut client = client.clone();
        async move {
            match number % 3 {
                0 => {
                    let id = format!("single-{number}");
                    let response = client
                        .index_doc(tonic::Request::new(ShardDocRequest {
                            index_name: index.into(),
                            shard_id: 0,
                            doc_id: id.clone(),
                            payload_json: serde_json::to_vec(&serde_json::json!({"message": id}))
                                .unwrap(),
                            ..Default::default()
                        }))
                        .await
                        .unwrap()
                        .into_inner();
                    assert!(response.success, "{}", response.error);
                    vec![(
                        response.seq_no.unwrap(),
                        ("index".to_string(), response.doc_id),
                    )]
                }
                1 => {
                    let ids: Vec<_> = (0..3).map(|item| format!("bulk-{number}-{item}")).collect();
                    let response = client
                        .bulk_index(tonic::Request::new(ShardBulkRequest {
                            index_name: index.into(),
                            shard_id: 0,
                            documents_json: ids
                                .iter()
                                .map(|id| {
                                    serde_json::to_vec(&serde_json::json!({
                                        "_doc_id": id,
                                        "_source": {"message": id}
                                    }))
                                    .unwrap()
                                })
                                .collect(),
                            ..Default::default()
                        }))
                        .await
                        .unwrap()
                        .into_inner();
                    assert!(response.success, "{}", response.error);
                    assert_eq!(response.doc_ids, ids);
                    let start = response.start_seq_no.unwrap();
                    response
                        .doc_ids
                        .into_iter()
                        .enumerate()
                        .map(|(offset, id)| (start + offset as u64, ("index".to_string(), id)))
                        .collect()
                }
                _ => {
                    let id = format!("delete-{number}");
                    let response = client
                        .delete_doc(tonic::Request::new(ShardDeleteRequest {
                            index_name: index.into(),
                            shard_id: 0,
                            doc_id: id.clone(),
                            ..Default::default()
                        }))
                        .await
                        .unwrap()
                        .into_inner();
                    assert!(response.success, "{}", response.error);
                    vec![(response.seq_no.unwrap(), ("delete".to_string(), id))]
                }
            }
        }
    });
    let results = tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(jobs))
        .await
        .unwrap();
    let mut receipts =
        std::collections::BTreeMap::from([(0, ("index".to_string(), "seed".to_string()))]);
    for (seq_no, identity) in results.into_iter().flatten() {
        assert!(
            receipts.insert(seq_no, identity).is_none(),
            "duplicate sequence {seq_no}"
        );
    }
    assert_eq!(
        receipts.keys().copied().collect::<Vec<_>>(),
        (0..41).collect::<Vec<_>>()
    );
    for shard_manager in [&primary_sm, &replica_sm] {
        let entries = shard_manager
            .get_shard(index, 0)
            .unwrap()
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations;
        assert_eq!(entries.len(), receipts.len());
        let actual: std::collections::BTreeMap<_, _> = entries
            .into_iter()
            .map(|entry| {
                (
                    entry.seq_no,
                    (
                        entry.op.as_str().to_string(),
                        entry.payload["_doc_id"].as_str().unwrap().to_string(),
                    ),
                )
            })
            .collect();
        assert_eq!(
            actual.len(),
            receipts.len(),
            "WAL has duplicate sequence identities"
        );
        assert_eq!(actual, receipts);
    }
    assert_eq!(
        primary_sm.get_shard(index, 0).unwrap().global_checkpoint(),
        Some(40)
    );
}

#[tokio::test]
async fn replicate_bulk_rejects_invalid_sequence_ranges_before_writing() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(ClusterManager::new("replica-validation".into()));
    let shards = Arc::new(ShardManager::new(directory.path(), Duration::from_secs(60)));
    let index = "replica-validation";
    setup_single_node_cluster_state(&manager, index);
    let address = start_grpc_server(manager, shards.clone()).await;
    let mut client = connect_client(address).await;
    let operation = |seq_no, op: &str| ReplicateDocRequest {
        index_name: index.into(),
        shard_id: 0,
        doc_id: format!("doc-{seq_no}"),
        payload_json: serde_json::to_vec(&serde_json::json!({"body": "value"})).unwrap(),
        op: op.to_string(),
        seq_no,
        index_uuid: format!("{index}-uuid"),
        primary_term: Some(1),
        target_allocation_id: Some(1),
    };
    for ops in [
        vec![operation(10, "index"), operation(12, "index")],
        vec![operation(u64::MAX, "index"), operation(0, "index")],
        vec![operation(0, "delete")],
    ] {
        let error = client
            .replicate_bulk(tonic::Request::new(ReplicateBulkRequest {
                index_name: index.into(),
                shard_id: 0,
                ops,
                index_uuid: format!("{index}-uuid"),
                primary_term: Some(1),
                target_allocation_id: Some(1),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    assert!(
        shards
            .get_shard(index, 0)
            .unwrap()
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .is_empty()
    );
    assert_eq!(shards.get_shard(index, 0).unwrap().doc_count(), 0);
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
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("bulk-gc".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "bgc-idx", replica_addr.port());
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "bgc-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Bulk index 5 docs
    let documents_json: Vec<Vec<u8>> = (0..5)
        .map(|i| {
            let payload = serde_json::json!({
                "_doc_id": format!("b-{i}"),
                "_source": {"msg": format!("bulk-{i}")}
            });
            serde_json::to_vec(&payload).unwrap()
        })
        .collect();

    let resp = client
        .bulk_index(tonic::Request::new(proto::ShardBulkRequest {
            index_name: "bgc-idx".into(),
            shard_id: 0,
            documents_json,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success, "bulk index failed: {}", resp.error);

    let engine = primary_sm.get_shard("bgc-idx", 0).unwrap();
    assert!(
        engine
            .global_checkpoint()
            .is_some_and(|checkpoint| checkpoint > 0),
        "global checkpoint should advance after bulk replication, got {:?}",
        engine.global_checkpoint()
    );
    assert!(
        engine
            .local_checkpoint()
            .is_some_and(|checkpoint| checkpoint > 0),
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
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("del-gc".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "dgc-idx", replica_addr.port());
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "dgc-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Index a doc first
    let payload = serde_json::json!({"msg": "to-delete"});
    client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: "dgc-idx".into(),
            shard_id: 0,
            doc_id: "del-1".into(),
            payload_json: serde_json::to_vec(&payload).unwrap(),
            ..Default::default()
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
            ..Default::default()
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
        "global checkpoint should advance after delete replication: {cp_after_delete:?} > {cp_after_index:?}"
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
    let replica_addr = start_replica_grpc_server(replica_cm.clone(), replica_sm.clone()).await;

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_cm = Arc::new(ClusterManager::new("isr-it".into()));
    let primary_sm = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    setup_two_node_cluster_state(&primary_cm, &replica_cm, "isr-idx", replica_addr.port());
    install_recovered_replica_fixture(&replica_cm, &replica_sm, "isr-idx");

    let primary_addr = start_primary_grpc_server(primary_cm, primary_sm.clone()).await;
    let mut client = connect_client(primary_addr).await;

    // Index docs to trigger replication → ISR update
    for i in 0..3 {
        let payload = serde_json::json!({"data": i});
        client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "isr-idx".into(),
                shard_id: 0,
                doc_id: format!("isr-{i}"),
                payload_json: serde_json::to_vec(&payload).unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    // ISR tracker should have the replica checkpoint
    let engine = primary_sm.get_shard("isr-idx", 0).unwrap();
    let isr = primary_sm.isr_tracker.in_sync_replicas(
        "isr-idx",
        0,
        engine.local_checkpoint().unwrap_or(0),
    );
    assert!(
        !isr.is_empty(),
        "ISR should contain the replica after successful replication"
    );

    // Replica checkpoints should be tracked
    let cps = primary_sm.isr_tracker.replica_checkpoints("isr-idx", 0);
    assert!(!cps.is_empty(), "replica checkpoints should be recorded");
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
                doc_id: format!("doc-{i}"),
                payload_json: serde_json::to_vec(&payload).unwrap(),
                ..Default::default()
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
                    doc_id: format!("s{shard}-doc-{i}"),
                    payload_json: serde_json::to_vec(&payload).unwrap(),
                    ..Default::default()
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
