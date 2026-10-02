use crate::api::{AppState, create_router};
use crate::cluster::ClusterManager;
use crate::cluster::state::{NodeInfo, NodeRole};
use crate::consensus::state_machine::{ClusterStateMachine, TestApplyGate};
use crate::consensus::types::{ClusterCommand, RaftInstance};
use crate::transport::TransportClient;
use crate::transport::server::create_transport_service_with_raft;
use axum::http::StatusCode;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

#[path = "refresh_tests.rs"]
mod refresh;
#[path = "forwarding_review_tests.rs"]
mod review;

struct ForwardingNode {
    _data: tempfile::TempDir,
    state: AppState,
    url: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    refresh_requests: Arc<std::sync::atomic::AtomicUsize>,
    refresh_request_started: Arc<tokio::sync::Notify>,
    reject_refresh_requests: Arc<std::sync::atomic::AtomicBool>,
}

struct ForwardingCluster {
    nodes: Vec<ForwardingNode>,
    gate: Arc<TestApplyGate>,
    client: reqwest::Client,
}

impl Drop for ForwardingCluster {
    fn drop(&mut self) {
        self.gate.resume();
        for node in &self.nodes {
            for task in &node.tasks {
                task.abort();
            }
        }
    }
}

impl ForwardingCluster {
    async fn start() -> Self {
        Self::start_with_roles(&[vec![NodeRole::Master], vec![NodeRole::Data]]).await
    }

    async fn start_with_roles(roles: &[Vec<NodeRole>]) -> Self {
        let gate = Arc::new(TestApplyGate::default());
        let mut nodes = Vec::new();
        let mut addresses = Vec::new();
        for id in 1..=roles.len() as u64 {
            let data = tempfile::tempdir().unwrap();
            let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http_addr = http.local_addr().unwrap();
            let grpc_addr = grpc.local_addr().unwrap();
            addresses.push(grpc_addr);
            let mut machine = ClusterStateMachine::new("forwarding-test".into());
            if id == 2 {
                machine.set_apply_gate(gate.clone());
            }
            let cluster_manager =
                Arc::new(ClusterManager::with_shared_state(machine.state_handle()));
            let raft: Arc<RaftInstance> = Arc::new(
                openraft::Raft::new(
                    id,
                    Arc::new(crate::consensus::default_raft_config(
                        "forwarding-test".into(),
                    )),
                    crate::consensus::network::RaftNetworkFactoryImpl,
                    crate::consensus::store::MemLogStore::new(),
                    machine,
                )
                .await
                .unwrap(),
            );
            let shard_manager = Arc::new(crate::shard::ShardManager::new(
                data.path(),
                Duration::from_secs(60),
            ));
            let task_manager = Arc::new(crate::tasks::TaskManager::new());
            let transport_client = TransportClient::new();
            let state = AppState {
                cluster_manager: cluster_manager.clone(),
                shard_manager: shard_manager.clone(),
                transport_client: transport_client.clone(),
                local_node_id: format!("node-{id}"),
                raft: raft.clone(),
                worker_pools: crate::worker::WorkerPools::new(2, 2),
                task_manager: task_manager.clone(),
                storage_manager: Arc::new(
                    crate::storage::StorageManager::new_in_path(data.path()).unwrap(),
                ),
                security_manager: Arc::new(crate::security::SecurityManager::disabled()),
                remote_store_reader_cache: Arc::new(
                    crate::engine::remote_store::RemoteSplitReaderCache::default(),
                ),
                sql_group_by_scan_limit: 1_000_000,
                sql_approximate_top_k: false,
            };
            let service = create_transport_service_with_raft(
                cluster_manager,
                shard_manager,
                transport_client,
                raft,
                task_manager,
                state.local_node_id.clone(),
            );
            let refresh_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let refresh_request_started = Arc::new(tokio::sync::Notify::new());
            let reject_refresh_requests = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let requests = refresh_requests.clone();
            let request_started = refresh_request_started.clone();
            let reject_requests = reject_refresh_requests.clone();
            let grpc_task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .layer(tower::util::MapRequestLayer::new(
                        move |mut request: axum::http::Request<tonic::body::Body>| {
                            if request.uri().path().ends_with("/RefreshShardCopy") {
                                requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                request_started.notify_one();
                                if reject_requests.load(std::sync::atomic::Ordering::Relaxed) {
                                    *request.uri_mut() =
                                        "/transport.InternalTransport/TestRejectedRefresh"
                                            .parse()
                                            .unwrap();
                                }
                            }
                            request
                        },
                    ))
                    .add_service(service)
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc))
                    .await
                    .unwrap();
            });
            let router = create_router(state.clone());
            let http_task = tokio::spawn(async move {
                axum::serve(http, router).await.unwrap();
            });
            nodes.push(ForwardingNode {
                _data: data,
                state,
                url: format!("http://{http_addr}"),
                tasks: vec![grpc_task, http_task],
                refresh_requests,
                refresh_request_started,
                reject_refresh_requests,
            });
        }
        let leader = &nodes[0].state.raft;
        crate::consensus::bootstrap_single_node(leader, 1, addresses[0].to_string())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !leader.is_leader() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        leader
            .client_write(ClusterCommand::SetMaster {
                node_id: "node-1".into(),
            })
            .await
            .unwrap();
        for (offset, address) in addresses.iter().enumerate().skip(1) {
            leader
                .add_learner(
                    offset as u64 + 1,
                    openraft::BasicNode {
                        addr: address.to_string(),
                    },
                    true,
                )
                .await
                .unwrap();
        }
        leader
            .client_write(ClusterCommand::SetMaster {
                node_id: "node-1".into(),
            })
            .await
            .unwrap();
        leader.change_membership([1, 2], false).await.unwrap();
        for (offset, node) in nodes.iter().enumerate() {
            leader
                .client_write(ClusterCommand::AddNode {
                    node: NodeInfo {
                        id: node.state.local_node_id.clone(),
                        name: node.state.local_node_id.clone(),
                        host: "127.0.0.1".into(),
                        transport_port: addresses[offset].port(),
                        http_port: node.url.rsplit(':').next().unwrap().parse().unwrap(),
                        roles: roles[offset].clone(),
                        raft_node_id: offset as u64 + 1,
                    },
                })
                .await
                .unwrap();
        }
        leader
            .client_write(ClusterCommand::SetMaster {
                node_id: "node-1".into(),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while nodes.iter().skip(1).any(|node| {
                node.state
                    .cluster_manager
                    .get_state()
                    .master_node
                    .as_deref()
                    != Some("node-1")
            }) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        for node in &nodes {
            assert!(
                client
                    .get(&node.url)
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
            node.state
                .transport_client
                .send_ping(
                    &nodes[0].state.cluster_manager.get_state().nodes[&node.state.local_node_id],
                    &node.state.local_node_id,
                )
                .await
                .unwrap();
        }
        Self {
            nodes,
            gate,
            client,
        }
    }

    async fn request(
        &self,
        node: usize,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.nodes[node].url));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status();
        (status, response.json().await.unwrap())
    }

    async fn create(&self, index: &str) -> (StatusCode, Value) {
        self.request(
            0,
            reqwest::Method::PUT,
            &format!("/{index}"),
            Some(json!({
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"value": {"type": "integer"}}}
            })),
        )
        .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_create_then_index_waits_for_raft_apply() {
    let cluster = ForwardingCluster::start().await;
    cluster.gate.pause();
    let release = cluster.gate.clone();
    let release_task = tokio::spawn(async move {
        release.wait_until_entered().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        release.resume();
    });
    let (status, body) = cluster.create("lag-index").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/lag-index/_doc/a",
            Some(json!({"value": 7})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    release_task.await.unwrap();
    let (status, body) = cluster
        .request(0, reqwest::Method::GET, "/lag-index/_doc/a", None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["value"], 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_bulk_update_delete_and_get_wait_for_raft_apply() {
    let cluster = ForwardingCluster::start().await;
    let mut failures = Vec::new();
    for (index, operation) in [
        ("lag-bulk", "bulk"),
        ("lag-update", "update"),
        ("lag-delete", "delete"),
        ("lag-get", "get"),
    ] {
        cluster.gate.pause();
        let release = cluster.gate.clone();
        let release_task = tokio::spawn(async move {
            release.wait_until_entered().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
            release.resume();
        });
        let (status, body) = cluster.create(index).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = match operation {
            "bulk" => {
                let response = cluster
                    .client
                    .post(format!("{}/{index}/_bulk", cluster.nodes[0].url))
                    .header("content-type", "application/x-ndjson")
                    .body("{\"index\":{\"_id\":\"a\"}}\n{\"value\":8}\n")
                    .send()
                    .await
                    .unwrap();
                (response.status(), response.json::<Value>().await.unwrap())
            }
            "update" => {
                cluster
                    .request(
                        0,
                        reqwest::Method::POST,
                        &format!("/{index}/_update/a"),
                        Some(json!({"doc": {"value": 9}, "doc_as_upsert": true})),
                    )
                    .await
            }
            "delete" => {
                cluster
                    .request(
                        0,
                        reqwest::Method::DELETE,
                        &format!("/{index}/_doc/a"),
                        None,
                    )
                    .await
            }
            "get" => {
                cluster
                    .request(0, reqwest::Method::GET, &format!("/{index}/_doc/a"), None)
                    .await
            }
            _ => unreachable!(),
        };
        release_task.await.unwrap();
        let correct = match operation {
            "bulk" => {
                status == StatusCode::OK
                    && body["errors"] == false
                    && body["items"][0]["index"]["status"] == 201
            }
            "update" => status == StatusCode::CREATED && body["result"] == "created",
            "delete" => {
                status == StatusCode::NOT_FOUND
                    && body["result"] == "not_found"
                    && body["_seq_no"].is_u64()
            }
            "get" => status == StatusCode::NOT_FOUND && body["found"] == false,
            _ => unreachable!(),
        };
        if !correct {
            failures.push(format!("{operation}: {status}: {body}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_deadline_returns_retryable_503_with_cause() {
    let cluster = ForwardingCluster::start().await;
    cluster.nodes[1]
        .state
        .cluster_manager
        .forwarding_wait_millis
        .store(100, std::sync::atomic::Ordering::Relaxed);
    cluster.gate.pause();
    let create = cluster
        .client
        .put(format!("{}/lag-timeout", cluster.nodes[0].url))
        .json(&json!({"settings": {"number_of_replicas": 0}}))
        .send();
    tokio::pin!(create);
    tokio::select! {
        response = &mut create => {
            assert_eq!(response.unwrap().status(), StatusCode::OK);
            cluster.gate.wait_until_entered().await;
        }
        () = cluster.gate.wait_until_entered() => {}
    }
    for (method, path, payload) in [
        (
            reqwest::Method::PUT,
            "/lag-timeout/_doc/a",
            Some(json!({"body": "never applied"})),
        ),
        (
            reqwest::Method::POST,
            "/lag-timeout/_update/a",
            Some(json!({"doc": {"body": "never applied"}, "doc_as_upsert": true})),
        ),
        (reqwest::Method::DELETE, "/lag-timeout/_doc/a", None),
        (reqwest::Method::GET, "/lag-timeout/_doc/a", None),
        (reqwest::Method::GET, "/lag-timeout/_search", None),
        (
            reqwest::Method::POST,
            "/lag-timeout/_search",
            Some(json!({"query": {"match_all": {}}})),
        ),
        (reqwest::Method::GET, "/lag-timeout/_count", None),
        (
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": "SELECT body FROM \"lag-timeout\""})),
        ),
        (
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": "SELECT count(*) FROM \"lag-timeout\""})),
        ),
        (
            reqwest::Method::POST,
            "/_sql/stream",
            Some(json!({"query": "SELECT body FROM \"lag-timeout\""})),
        ),
    ] {
        let started = std::time::Instant::now();
        let (status, body) = cluster.request(0, method, path, payload).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {body}");
        assert_eq!(
            body["error"]["type"], "shard_not_available_exception",
            "{path}: {body}"
        );
        let reason = body["error"]["reason"].as_str().unwrap();
        assert!(
            reason.contains("cluster state") && reason.contains("version"),
            "{path}: {body}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(90),
            "{path}: {body}"
        );
        assert!(started.elapsed() < Duration::from_secs(2), "{path}: {body}");
    }
    let response = cluster.client.post(format!("{}/lag-timeout/_bulk", cluster.nodes[0].url))
        .header("content-type", "application/x-ndjson")
        .body("{\"index\":{\"_id\":\"a\"}}\n{\"body\":\"never applied\"}\n{\"update\":{\"_id\":\"b\"}}\n{\"doc\":{\"body\":\"never applied\"},\"doc_as_upsert\":true}\n")
        .send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["errors"], true, "{body}");
    for (position, action) in [(0, "index"), (1, "update")] {
        assert_eq!(body["items"][position][action]["status"], 503, "{body}");
        assert!(
            body["items"][position][action]["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("cluster state"),
            "{body}"
        );
    }
    assert!(
        cluster.nodes[1]
            .state
            .shard_manager
            .get_shard("lag-timeout", 0)
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_create_then_write_50_iterations_without_hook() {
    let cluster = ForwardingCluster::start().await;
    let mut failures = Vec::new();
    for iteration in 0..50 {
        let index = format!("forward-loop-{iteration}");
        let (status, body) = cluster.create(&index).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = cluster
            .request(
                0,
                reqwest::Method::PUT,
                &format!("/{index}/_doc/a"),
                Some(json!({"value": iteration})),
            )
            .await;
        if status != StatusCode::CREATED {
            failures.push(format!("{iteration}: {status}: {body}"));
        } else {
            let (status, body) = cluster
                .request(0, reqwest::Method::GET, &format!("/{index}/_doc/a"), None)
                .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["_source"]["value"], iteration);
        }
    }
    println!(
        "create-then-write: {}/50 failures ({}%)",
        failures.len(),
        failures.len() * 2
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_search_count_and_sql_after_create() {
    let cluster = ForwardingCluster::start().await;
    cluster.gate.pause();
    let release = cluster.gate.clone();
    let release_task = tokio::spawn(async move {
        release.wait_until_entered().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        release.resume();
    });
    let (status, body) = cluster.create("lag-reads").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut failures = Vec::new();
    for (method, path, body) in [
        (reqwest::Method::GET, "/lag-reads/_search", None),
        (
            reqwest::Method::POST,
            "/lag-reads/_search",
            Some(json!({"query": {"match_all": {}}})),
        ),
        (reqwest::Method::GET, "/lag-reads/_count", None),
        (
            reqwest::Method::POST,
            "/lag-reads/_count",
            Some(json!({"query": {"term": {"value": 7}}})),
        ),
        (
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": "SELECT value FROM \"lag-reads\""})),
        ),
        (
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": "SELECT count(*) FROM \"lag-reads\""})),
        ),
    ] {
        let (status, body) = cluster.request(0, method, path, body).await;
        if status != StatusCode::OK
            || body["_shards"]["successful"] != 1
            || body["_shards"]["failed"] != 0
        {
            failures.push(format!("{path}: {status}: {body}"));
        }
    }
    release_task.await.unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_auto_create_waits_for_coordinator_and_primary() {
    let cluster = ForwardingCluster::start().await;
    cluster.gate.pause();
    let release = cluster.gate.clone();
    let release_task = tokio::spawn(async move {
        release.wait_until_entered().await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        release.resume();
    });
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/lag-auto/_doc/a",
            Some(json!({"body": "auto created"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    release_task.await.unwrap();
    let (status, body) = cluster
        .request(0, reqwest::Method::GET, "/lag-auto/_doc/a", None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["body"], "auto created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_lag_mapping_and_setting_then_write_waits_for_new_version() {
    let cluster = ForwardingCluster::start().await;
    let (status, body) = cluster.create("lag-metadata").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !cluster.nodes[1]
            .state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("lag-metadata")
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/lag-metadata/_doc/a",
            Some(json!({"value": 0})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for mutation in ["settings", "mappings"] {
        cluster.gate.pause();
        if mutation == "settings" {
            let (status, body) = cluster
                .request(
                    0,
                    reqwest::Method::PUT,
                    "/lag-metadata/_settings",
                    Some(json!({"index": {"refresh_interval_ms": 123_000}})),
                )
                .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        } else {
            let master = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-1"].clone();
            cluster.nodes[0]
                .state
                .transport_client
                .forward_add_mappings(
                    &master,
                    "lag-metadata",
                    &Default::default(),
                    &crate::cluster::state::DynamicMapping::Strict,
                )
                .await
                .unwrap();
        }
        cluster.gate.wait_until_entered().await;
        let required_version = cluster.nodes[0].state.cluster_manager.get_state().version;
        assert!(cluster.nodes[1].state.cluster_manager.get_state().version < required_version);
        let release = cluster.gate.clone();
        let release_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            release.resume();
        });
        let started = std::time::Instant::now();
        let (status, body) = cluster
            .request(
                0,
                reqwest::Method::PUT,
                "/lag-metadata/_doc/a",
                Some(json!({"value": 42})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{mutation}: {body}");
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "{mutation} did not wait: {body}"
        );
        assert!(cluster.nodes[1].state.cluster_manager.get_state().version >= required_version);
        release_task.await.unwrap();
    }
    let (status, body) = cluster
        .request(0, reqwest::Method::GET, "/lag-metadata/_doc/a", None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["value"], 42);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_rpc_rejects_missing_or_malformed_watermark_before_mutation() {
    let cluster = ForwardingCluster::start().await;
    let node = &cluster.nodes[0].state.cluster_manager.get_state().nodes["node-2"];
    let mut client =
        crate::transport::proto::internal_transport_client::InternalTransportClient::connect(
            format!("http://{}:{}", node.host, node.transport_port),
        )
        .await
        .unwrap();
    for header in [None, Some("not-a-version"), Some("-1")] {
        let mut request = tonic::Request::new(crate::transport::proto::ShardDocRequest {
            index_name: "invalid-context".into(),
            shard_id: 0,
            doc_id: "a".into(),
            payload_json: serde_json::to_vec(&json!({"body": "never written"})).unwrap(),
            ..Default::default()
        });
        if let Some(value) = header {
            request.metadata_mut().insert(
                crate::transport::state_wait::STATE_VERSION_HEADER,
                value.parse().unwrap(),
            );
        }

        let error = client.index_doc(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
        assert!(
            error.message().contains("cluster-state-version")
                || error.message().contains("cluster state version"),
            "{error}"
        );
    }
    assert!(
        cluster.nodes[1]
            .state
            .shard_manager
            .get_shard("invalid-context", 0)
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_acknowledged_metadata_fences_following_requests() {
    let cluster = ForwardingCluster::start().await;
    let (status, body) = cluster.create("lag-ack").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let follower = &cluster.nodes[1].state;
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(100, std::sync::atomic::Ordering::Relaxed);
    cluster.gate.pause();
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/lag-ack/_settings",
            Some(json!({"index": {"refresh_interval_ms": 321_000}})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("version"),
        "{body}"
    );
    cluster.gate.resume();
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(5_000, std::sync::atomic::Ordering::Relaxed);
    follower
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(100, std::sync::atomic::Ordering::Relaxed);
    cluster.gate.pause();
    let master = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-1"].clone();
    follower
        .transport_client
        .forward_add_mappings(
            &master,
            "lag-ack",
            &Default::default(),
            &crate::cluster::state::DynamicMapping::Strict,
        )
        .await
        .unwrap();
    assert_eq!(
        follower.transport_client.required_state_version("lag-ack"),
        cluster.nodes[0].state.cluster_manager.version()
    );
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/lag-ack/_doc/a",
            Some(json!({"value": 7})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        follower
            .shard_manager
            .get_shard("lag-ack", 0)
            .unwrap()
            .sequence_stats()
            .processed_checkpoint
            .is_none()
    );
    cluster.gate.resume();
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(5_000, std::sync::atomic::Ordering::Relaxed);
    follower
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/lag-ack/_doc/a",
            Some(json!({"value": 7})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/lag-ack/_doc/b",
            Some(json!({"unmapped": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("strict mapping"),
        "{body}"
    );
}
