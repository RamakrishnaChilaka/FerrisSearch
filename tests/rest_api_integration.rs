use anyhow::Result;
use ferrissearch::api::{AppState, create_router};
use ferrissearch::cluster::ClusterManager;
use ferrissearch::cluster::state::{
    ClusterState, FieldMapping, FieldType, IndexMetadata, IndexSettings, NodeInfo, NodeRole,
    ShardRoutingEntry,
};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::{
    PingRequest, internal_transport_client::InternalTransportClient,
};
use ferrissearch::transport::server::create_transport_service_with_raft;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

struct RestTestHarness {
    _temp_dir: TempDir,
    client: Client,
    base_url: String,
    transport_addr: std::net::SocketAddr,
    http_handle: JoinHandle<()>,
    transport_handle: JoinHandle<()>,
}

struct MultiNodeRestHarness {
    client: Client,
    nodes: Vec<MultiNodeRestNode>,
}

struct MultiNodeRestNode {
    _temp_dir: TempDir,
    app_state: AppState,
    base_url: String,
    transport_addr: std::net::SocketAddr,
    http_handle: JoinHandle<()>,
    transport_handle: JoinHandle<()>,
}

struct PendingMultiNodeRestNode {
    temp_dir: TempDir,
    node_id: String,
    http_listener: tokio::net::TcpListener,
    transport_listener: tokio::net::TcpListener,
    http_addr: std::net::SocketAddr,
    transport_addr: std::net::SocketAddr,
}

async fn make_test_raft(
    node_id: u64,
    cluster_name: &str,
    bootstrap_addr: Option<String>,
) -> (
    std::sync::Arc<ferrissearch::consensus::types::RaftInstance>,
    std::sync::Arc<std::sync::RwLock<ClusterState>>,
) {
    let (raft, shared_state) =
        ferrissearch::consensus::create_raft_instance_mem(node_id, cluster_name.into())
            .await
            .unwrap();
    if let Some(addr) = bootstrap_addr {
        ferrissearch::consensus::bootstrap_single_node(&raft, node_id, addr)
            .await
            .unwrap();
        for _ in 0..100 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    (raft, shared_state)
}

async fn post_json_to_base_url(
    client: &Client,
    base_url: &str,
    path: &str,
    body: Value,
) -> Result<(StatusCode, Value)> {
    let response = client
        .post(format!("{}{}", base_url, path))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value = response.json().await?;
    Ok((status, value))
}

impl RestTestHarness {
    async fn start() -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let http_addr = http_listener.local_addr()?;
        let transport_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let transport_addr = transport_listener.local_addr()?;

        let local_node = NodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: transport_addr.port(),
            http_port: http_addr.port(),
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: 1,
        };

        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(local_node);
        cluster_state.master_node = Some("node-1".into());

        let (raft, shared_state) = make_test_raft(
            1,
            "test-cluster",
            Some(format!("127.0.0.1:{}", transport_addr.port())),
        )
        .await;
        let manager = ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state);

        let shard_manager = Arc::new(ShardManager::new(temp_dir.path(), Duration::from_secs(60)));
        let cluster_manager = Arc::new(manager);
        let transport_client = TransportClient::new();
        let task_manager = Arc::new(ferrissearch::tasks::TaskManager::new());
        let app_state = AppState {
            cluster_manager: cluster_manager.clone(),
            shard_manager: shard_manager.clone(),
            transport_client: transport_client.clone(),
            local_node_id: "node-1".into(),
            raft: raft.clone(),
            worker_pools: ferrissearch::worker::WorkerPools::new(2, 2),
            task_manager: task_manager.clone(),
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
        };

        let transport_service = create_transport_service_with_raft(
            cluster_manager,
            shard_manager,
            transport_client,
            raft,
            task_manager,
            "node-1".into(),
        );
        let transport_handle = tokio::spawn(async move {
            let incoming = TcpListenerStream::new(transport_listener);
            if let Err(error) = tonic::transport::Server::builder()
                .add_service(transport_service)
                .serve_with_incoming(incoming)
                .await
            {
                tracing::error!("Test gRPC transport server failed: {}", error);
            }
        });

        let app = create_router(app_state);
        let http_handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(http_listener, app).await {
                tracing::error!("Test HTTP server failed: {}", error);
            }
        });

        let harness = Self {
            _temp_dir: temp_dir,
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
            base_url: format!("http://{}", http_addr),
            transport_addr,
            http_handle,
            transport_handle,
        };

        harness.wait_until_ready().await?;
        Ok(harness)
    }

    async fn wait_until_ready(&self) -> Result<()> {
        for _ in 0..50 {
            let http_ready =
                if let Ok(response) = self.client.get(format!("{}/", self.base_url)).send().await {
                    response.status() == StatusCode::OK
                } else {
                    false
                };
            let transport_ready = if let Ok(mut client) =
                InternalTransportClient::connect(format!("http://{}", self.transport_addr)).await
            {
                client
                    .ping(tonic::Request::new(PingRequest {
                        source_node_id: "rest-test-ready".into(),
                    }))
                    .await
                    .is_ok()
            } else {
                false
            };

            if http_ready && transport_ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!(
            "test servers did not become ready in time (http={}, transport={})",
            self.base_url,
            self.transport_addr
        );
    }

    async fn put_json(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .put(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn post_json(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn post_json_text(&self, path: &str, body: Value) -> Result<(StatusCode, String)> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        Ok((status, text))
    }

    async fn get_json(&self, path: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn get_text(&self, path: &str) -> Result<(StatusCode, String)> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        Ok((status, text))
    }

    async fn delete_json(&self, path: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .delete(format!("{}{}", self.base_url, path))
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn head_status(&self, path: &str) -> Result<StatusCode> {
        let response = self
            .client
            .head(format!("{}{}", self.base_url, path))
            .send()
            .await?;
        Ok(response.status())
    }

    async fn post_ndjson(&self, path: &str, body: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(body.to_string())
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }
}

impl MultiNodeRestHarness {
    async fn start_three_nodes() -> Result<Self> {
        let mut pending_nodes = Vec::new();
        for index in 1..=3 {
            let temp_dir = tempfile::tempdir()?;
            let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let http_addr = http_listener.local_addr()?;
            let transport_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let transport_addr = transport_listener.local_addr()?;

            pending_nodes.push(PendingMultiNodeRestNode {
                temp_dir,
                node_id: format!("node-{index}"),
                http_listener,
                transport_listener,
                http_addr,
                transport_addr,
            });
        }

        let all_nodes: Vec<NodeInfo> = pending_nodes
            .iter()
            .enumerate()
            .map(|(index, node)| NodeInfo {
                id: node.node_id.clone(),
                name: node.node_id.clone(),
                host: "127.0.0.1".into(),
                transport_port: node.transport_addr.port(),
                http_port: node.http_addr.port(),
                roles: if index == 0 {
                    vec![NodeRole::Master, NodeRole::Data]
                } else {
                    vec![NodeRole::Data]
                },
                raft_node_id: (index + 1) as u64,
            })
            .collect();

        let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
        let mut nodes = Vec::new();

        for (idx, pending) in pending_nodes.into_iter().enumerate() {
            let mut cluster_state = ClusterState::new("test-cluster".into());
            for node in &all_nodes {
                cluster_state.add_node(node.clone());
            }
            cluster_state.master_node = Some("node-1".into());

            let raft_id = (idx + 1) as u64;
            let bootstrap_addr =
                (idx == 0).then(|| format!("127.0.0.1:{}", pending.transport_addr.port()));
            let (raft, shared_state) =
                make_test_raft(raft_id, "test-cluster", bootstrap_addr).await;
            let manager = ClusterManager::with_shared_state(shared_state);
            manager.update_state(cluster_state);

            let cluster_manager = Arc::new(manager);
            let shard_manager = Arc::new(ShardManager::new(
                pending.temp_dir.path(),
                Duration::from_secs(60),
            ));
            let transport_client = TransportClient::new();
            let task_manager = Arc::new(ferrissearch::tasks::TaskManager::new());
            let app_state = AppState {
                cluster_manager: cluster_manager.clone(),
                shard_manager: shard_manager.clone(),
                transport_client: transport_client.clone(),
                local_node_id: pending.node_id.clone(),
                raft: raft.clone(),
                worker_pools: ferrissearch::worker::WorkerPools::new(2, 2),
                task_manager: task_manager.clone(),
                sql_group_by_scan_limit: 1_000_000,
                sql_approximate_top_k: false,
            };

            let transport_service = create_transport_service_with_raft(
                cluster_manager,
                shard_manager,
                transport_client,
                raft,
                task_manager,
                pending.node_id.clone(),
            );
            let transport_handle = tokio::spawn(async move {
                let incoming = TcpListenerStream::new(pending.transport_listener);
                if let Err(error) = tonic::transport::Server::builder()
                    .add_service(transport_service)
                    .serve_with_incoming(incoming)
                    .await
                {
                    tracing::error!("Test gRPC transport server failed: {}", error);
                }
            });

            let app = create_router(app_state.clone());
            let http_handle = tokio::spawn(async move {
                if let Err(error) = axum::serve(pending.http_listener, app).await {
                    tracing::error!("Test HTTP server failed: {}", error);
                }
            });

            nodes.push(MultiNodeRestNode {
                _temp_dir: pending.temp_dir,
                app_state,
                base_url: format!("http://{}", pending.http_addr),
                transport_addr: pending.transport_addr,
                http_handle,
                transport_handle,
            });
        }

        let harness = Self { client, nodes };
        harness.wait_until_ready().await?;
        Ok(harness)
    }

    async fn wait_until_ready(&self) -> Result<()> {
        for node in &self.nodes {
            let mut ready = false;
            for _ in 0..50 {
                let http_ready = if let Ok(response) =
                    self.client.get(format!("{}/", node.base_url)).send().await
                {
                    response.status() == StatusCode::OK
                } else {
                    false
                };
                let transport_ready = if let Ok(mut client) =
                    InternalTransportClient::connect(format!("http://{}", node.transport_addr))
                        .await
                {
                    client
                        .ping(tonic::Request::new(PingRequest {
                            source_node_id: "multi-rest-test-ready".into(),
                        }))
                        .await
                        .is_ok()
                } else {
                    false
                };

                if http_ready && transport_ready {
                    ready = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            if !ready {
                anyhow::bail!(
                    "multi-node test servers did not become ready in time (http={}, transport={})",
                    node.base_url,
                    node.transport_addr
                );
            }
        }
        Ok(())
    }
}

impl Drop for RestTestHarness {
    fn drop(&mut self) {
        self.http_handle.abort();
        self.transport_handle.abort();
    }
}

impl Drop for MultiNodeRestHarness {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.http_handle.abort();
            node.transport_handle.abort();
        }
    }
}

async fn create_distributed_stories_index_and_docs(harness: &MultiNodeRestHarness) -> Result<()> {
    let mut shard_routing = std::collections::HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        2,
        ShardRoutingEntry {
            primary: "node-3".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let metadata = IndexMetadata {
        name: "stories".into(),
        uuid: ferrissearch::cluster::state::IndexUuid::new_random(),
        number_of_shards: 3,
        number_of_replicas: 0,
        shard_routing,
        mappings: std::collections::HashMap::from([
            (
                "upvotes".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            ),
            (
                "author".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            ),
        ]),
        settings: IndexSettings::default(),
    };

    for node in &harness.nodes {
        let mut cluster_state = node.app_state.cluster_manager.get_state();
        cluster_state.add_index(metadata.clone());
        node.app_state.cluster_manager.update_state(cluster_state);
    }

    for (shard_id, node) in harness.nodes.iter().enumerate() {
        node.app_state.shard_manager.open_shard_with_settings(
            "stories",
            shard_id as u32,
            &metadata.mappings,
            &metadata.settings,
            &metadata.uuid,
        )?;
    }

    let shard_docs = [
        vec![(0_i64, "alice"), (1_i64, "alice"), (2_i64, "bob")],
        vec![(0_i64, "alice"), (1_i64, "carol"), (3_i64, "carol")],
        vec![(0_i64, "carol"), (2_i64, "dave")],
    ];

    for (shard_id, docs) in shard_docs.into_iter().enumerate() {
        let engine = harness.nodes[shard_id]
            .app_state
            .shard_manager
            .get_shard("stories", shard_id as u32)
            .expect("shard should be open");
        for (doc_index, (value, author)) in docs.into_iter().enumerate() {
            engine.add_document(
                &format!("doc-{shard_id}-{doc_index}"),
                json!({
                    "title": format!("story-{shard_id}-{doc_index}"),
                    "upvotes": value,
                    "author": author,
                }),
            )?;
        }
        engine.refresh()?;
    }

    Ok(())
}

async fn create_products_index(harness: &RestTestHarness) -> Result<()> {
    create_products_index_named(harness, "products").await
}

async fn create_products_index_named(harness: &RestTestHarness, index_name: &str) -> Result<()> {
    let (status, body) = harness
        .put_json(
            &format!("/{index_name}"),
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "properties": {
                        "title": { "type": "keyword" },
                        "description": { "type": "text" },
                        "brand": { "type": "keyword" },
                        "price": { "type": "float" }
                    }
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));
    Ok(())
}

async fn index_product_docs(harness: &RestTestHarness) -> Result<()> {
    index_product_docs_named(harness, "products").await
}

async fn index_product_docs_named(harness: &RestTestHarness, index_name: &str) -> Result<()> {
    for (doc_id, payload) in [
        (
            "1",
            json!({"title": "iPhone Pro", "description": "iphone flagship", "brand": "Apple", "price": 999.0}),
        ),
        (
            "2",
            json!({"title": "iPhone", "description": "iphone standard", "brand": "Apple", "price": 899.0}),
        ),
        (
            "3",
            json!({"title": "Galaxy", "description": "iphone competitor", "brand": "Samsung", "price": 799.0}),
        ),
    ] {
        let (status, body) = harness
            .put_json(
                &format!("/{index_name}/_doc/{}?refresh=true", doc_id),
                payload,
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["_id"], json!(doc_id));
    }
    Ok(())
}

async fn create_products_index_and_docs(harness: &RestTestHarness) -> Result<()> {
    create_products_index(harness).await?;
    index_product_docs(harness).await
}

async fn create_events_index_and_docs(harness: &RestTestHarness) -> Result<()> {
    let (status, body) = harness
        .put_json(
            "/events",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "properties": {
                        "title": { "type": "keyword" },
                        "created_at": { "type": "date" }
                    }
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));

    for (doc_id, payload) in [
        (
            "1",
            json!({
                "title": "offset",
                "created_at": "2025-01-05T08:15:00+05:30"
            }),
        ),
        (
            "2",
            json!({
                "title": "utc",
                "created_at": "2025-01-05T08:00:00Z"
            }),
        ),
    ] {
        let (status, body) = harness
            .put_json(&format!("/events/_doc/{}?refresh=true", doc_id), payload)
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["_id"], json!(doc_id));
    }

    Ok(())
}

async fn create_products_index_and_docs_named(
    harness: &RestTestHarness,
    index_name: &str,
) -> Result<()> {
    create_products_index_named(harness, index_name).await?;
    index_product_docs_named(harness, index_name).await
}

async fn create_mixed_case_rides_index(harness: &RestTestHarness) -> Result<()> {
    let (status, body) = harness
        .put_json(
            "/rides",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "properties": {
                        "PULocationID": { "type": "integer" },
                        "DOLocationID": { "type": "integer" },
                        "trip_miles": { "type": "float" }
                    }
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));
    Ok(())
}

async fn index_mixed_case_rides_docs(harness: &RestTestHarness) -> Result<()> {
    for (doc_id, payload) in [
        (
            "1",
            json!({"PULocationID": 101, "DOLocationID": 201, "trip_miles": 1.4}),
        ),
        (
            "2",
            json!({"PULocationID": 102, "DOLocationID": 202, "trip_miles": 2.1}),
        ),
        (
            "3",
            json!({"PULocationID": 205, "DOLocationID": 303, "trip_miles": 6.3}),
        ),
    ] {
        let (status, body) = harness
            .put_json(&format!("/rides/_doc/{}?refresh=true", doc_id), payload)
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["_id"], json!(doc_id));
    }
    Ok(())
}

async fn create_mixed_case_rides_index_and_docs(harness: &RestTestHarness) -> Result<()> {
    create_mixed_case_rides_index(harness).await?;
    index_mixed_case_rides_docs(harness).await
}

async fn create_scored_products_index_and_docs(harness: &RestTestHarness) -> Result<()> {
    create_products_index(harness).await?;

    for (doc_id, payload) in [
        (
            "1",
            json!({
                "title": "iPhone Ultra",
                "description": "iphone iphone iphone iphone iphone",
                "brand": "Apple",
                "price": 1099.0
            }),
        ),
        (
            "2",
            json!({
                "title": "iPhone Pro",
                "description": "iphone iphone",
                "brand": "Apple",
                "price": 999.0
            }),
        ),
        (
            "3",
            json!({
                "title": "Galaxy",
                "description": "iphone",
                "brand": "Samsung",
                "price": 799.0
            }),
        ),
    ] {
        let (status, body) = harness
            .put_json(&format!("/products/_doc/{}?refresh=true", doc_id), payload)
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["_id"], json!(doc_id));
    }

    Ok(())
}

async fn create_products_index_with_real_score_and_docs(harness: &RestTestHarness) -> Result<()> {
    let (status, body) = harness
        .put_json(
            "/products",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "properties": {
                        "title": { "type": "keyword" },
                        "description": { "type": "text" },
                        "brand": { "type": "keyword" },
                        "price": { "type": "float" },
                        "score": { "type": "float" }
                    }
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));

    for (doc_id, payload) in [
        (
            "1",
            json!({
                "title": "iPhone Ultra",
                "description": "iphone iphone iphone iphone iphone",
                "brand": "Apple",
                "price": 1099.0,
                "score": 3.0
            }),
        ),
        (
            "2",
            json!({
                "title": "iPhone Pro",
                "description": "iphone iphone",
                "brand": "Apple",
                "price": 999.0,
                "score": 100.0
            }),
        ),
        (
            "3",
            json!({
                "title": "Galaxy",
                "description": "iphone",
                "brand": "Samsung",
                "price": 799.0,
                "score": 50.0
            }),
        ),
    ] {
        let (status, body) = harness
            .put_json(&format!("/products/_doc/{}?refresh=true", doc_id), payload)
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["_id"], json!(doc_id));
    }

    Ok(())
}

#[tokio::test]
async fn rest_root_and_cluster_health_work() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (root_status, root_body) = harness.get_json("/").await?;
    assert_eq!(root_status, StatusCode::OK);
    assert_eq!(root_body["engine"], json!("tantivy"));

    let (health_status, health_body) = harness.get_json("/_cluster/health").await?;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(health_body["cluster_name"], json!("test-cluster"));

    let (state_status, state_body) = harness.get_json("/_cluster/state").await?;
    assert_eq!(state_status, StatusCode::OK);
    assert_eq!(state_body["cluster_name"], json!("test-cluster"));
    assert_eq!(state_body["master_node"], json!("node-1"));

    let (transfer_status, transfer_body) = harness
        .post_json("/_cluster/transfer_master", json!({ "node_id": "node-1" }))
        .await?;
    assert_eq!(transfer_status, StatusCode::OK);
    assert_eq!(transfer_body["acknowledged"], json!(true));
    assert_eq!(
        transfer_body["message"],
        json!("Leadership transfer initiated to node 'node-1'")
    );

    Ok(())
}

#[tokio::test]
async fn rest_cat_endpoints_work_on_single_node() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (nodes_status, nodes_body) = harness.get_text("/_cat/nodes?v").await?;
    assert_eq!(nodes_status, StatusCode::OK);
    assert!(nodes_body.contains("id"));
    assert!(nodes_body.contains("node-1"));

    let (shards_status, shards_body) = harness.get_text("/_cat/shards?v&local").await?;
    assert_eq!(shards_status, StatusCode::OK);
    assert!(shards_body.contains("products"));
    assert!(shards_body.contains("STARTED"));

    let (indices_status, indices_body) = harness.get_text("/_cat/indices?v&local").await?;
    assert_eq!(indices_status, StatusCode::OK);
    assert!(indices_body.contains("products"));
    assert!(indices_body.contains("green"));

    let (master_status, master_body) = harness.get_text("/_cat/master?v").await?;
    assert_eq!(master_status, StatusCode::OK);
    assert!(master_body.contains("node-1"));

    Ok(())
}

#[tokio::test]
async fn rest_can_create_update_settings_and_delete_index() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    assert_eq!(
        harness.head_status("/products").await?,
        StatusCode::NOT_FOUND
    );
    create_products_index(&harness).await?;
    assert_eq!(harness.head_status("/products").await?, StatusCode::OK);

    let (settings_status, settings_body) = harness.get_json("/products/_settings").await?;
    assert_eq!(settings_status, StatusCode::OK);
    assert_eq!(
        settings_body["products"]["settings"]["index"]["number_of_shards"],
        json!(1)
    );
    assert_eq!(
        settings_body["products"]["settings"]["index"]["number_of_replicas"],
        json!(0)
    );

    let (update_status, update_body) = harness
        .put_json(
            "/products/_settings",
            json!({ "index": { "number_of_replicas": 0, "refresh_interval_ms": 250 } }),
        )
        .await?;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(update_body["acknowledged"], json!(true));

    let (settings_after_status, settings_after_body) =
        harness.get_json("/products/_settings").await?;
    assert_eq!(settings_after_status, StatusCode::OK);
    assert_eq!(
        settings_after_body["products"]["settings"]["index"]["number_of_shards"],
        json!(1)
    );
    assert_eq!(
        settings_after_body["products"]["settings"]["index"]["number_of_replicas"],
        json!(0)
    );

    let (delete_status, delete_body) = harness.delete_json("/products").await?;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(delete_body["acknowledged"], json!(true));
    assert_eq!(
        harness.head_status("/products").await?,
        StatusCode::NOT_FOUND
    );

    Ok(())
}

#[tokio::test]
async fn rest_can_index_get_update_delete_and_refresh_flush_documents() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index(&harness).await?;

    let (create_auto_status, create_auto_body) = harness
        .post_json(
            "/products/_doc?refresh=true",
            json!({"title": "Pixel", "description": "android phone", "brand": "Google", "price": 699.0}),
        )
        .await?;
    assert_eq!(create_auto_status, StatusCode::CREATED);
    let auto_id = create_auto_body["_id"].as_str().unwrap().to_string();
    assert!(!auto_id.is_empty());

    let (put_status, put_body) = harness
        .put_json(
            "/products/_doc/1?refresh=true",
            json!({"title": "iPhone Pro", "description": "iphone flagship", "brand": "Apple", "price": 999.0}),
        )
        .await?;
    assert_eq!(put_status, StatusCode::CREATED);
    assert_eq!(put_body["_id"], json!("1"));

    let (doc_status, doc_body) = harness.get_json("/products/_doc/1").await?;
    assert_eq!(doc_status, StatusCode::OK);
    assert_eq!(doc_body["found"], json!(true));
    assert_eq!(doc_body["_source"]["brand"], json!("Apple"));

    let (update_status, update_body) = harness
        .post_json(
            "/products/_update/1",
            json!({"doc": {"price": 1099.0, "color": "black"}}),
        )
        .await?;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(update_body["result"], json!("updated"));

    let (refresh_status, refresh_body) = harness.get_json("/products/_refresh").await?;
    assert_eq!(refresh_status, StatusCode::OK);
    assert_eq!(refresh_body["_shards"]["successful"], json!(1));

    let (_refreshed_status, refreshed_body) = harness.get_json("/products/_doc/1").await?;
    assert_eq!(refreshed_body["_source"]["price"], json!(1099.0));
    assert_eq!(refreshed_body["_source"]["color"], json!("black"));

    let (refresh_get_status, refresh_get_body) = harness.get_json("/products/_refresh").await?;
    assert_eq!(refresh_get_status, StatusCode::OK);
    assert_eq!(refresh_get_body["_shards"]["successful"], json!(1));

    let (refresh_post_status, refresh_post_body) =
        harness.post_json("/products/_refresh", json!({})).await?;
    assert_eq!(refresh_post_status, StatusCode::OK);
    assert_eq!(refresh_post_body["_shards"]["successful"], json!(1));

    let (flush_get_status, flush_get_body) = harness.get_json("/products/_flush").await?;
    assert_eq!(flush_get_status, StatusCode::OK);
    assert_eq!(flush_get_body["_shards"]["successful"], json!(1));

    let (flush_post_status, flush_post_body) =
        harness.post_json("/products/_flush", json!({})).await?;
    assert_eq!(flush_post_status, StatusCode::OK);
    assert_eq!(flush_post_body["_shards"]["successful"], json!(1));

    let (delete_status, delete_body) = harness.delete_json("/products/_doc/1").await?;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(delete_body["result"], json!("deleted"));

    let (refresh_after_delete_status, refresh_after_delete_body) =
        harness.get_json("/products/_refresh").await?;
    assert_eq!(refresh_after_delete_status, StatusCode::OK);
    assert_eq!(refresh_after_delete_body["_shards"]["successful"], json!(1));

    let (missing_status, missing_body) = harness.get_json("/products/_doc/1").await?;
    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    assert_eq!(missing_body["found"], json!(false));

    let (auto_doc_status, auto_doc_body) = harness
        .get_json(&format!("/products/_doc/{}", auto_id))
        .await?;
    assert_eq!(auto_doc_status, StatusCode::OK);
    assert_eq!(auto_doc_body["_source"]["brand"], json!("Google"));

    Ok(())
}

#[tokio::test]
async fn rest_forcemerge_returns_task_and_task_endpoint_reports_completion() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index(&harness).await?;

    let (force_merge_status, force_merge_body) = harness
        .post_json("/products/_forcemerge?max_num_segments=1", json!({}))
        .await?;
    assert_eq!(force_merge_status, StatusCode::ACCEPTED);
    assert_eq!(
        force_merge_body["task"]["action"],
        "indices:admin/forcemerge"
    );
    assert_eq!(force_merge_body["task"]["coordinator_node"], "node-1");
    let task_id = force_merge_body["task"]["id"]
        .as_str()
        .expect("force-merge should return a task id")
        .to_string();

    let task_body = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, body) = harness.get_json(&format!("/_tasks/{}", task_id)).await?;
            if status == StatusCode::OK && body["task"]["status"] == "completed" {
                break Ok::<Value, anyhow::Error>(body);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    assert_eq!(task_body["task"]["id"], task_id);
    assert_eq!(task_body["task"]["status"], "completed");
    assert_eq!(task_body["task"]["coordinator_node"], "node-1");
    assert_eq!(task_body["_nodes"]["total"], 1);
    assert_eq!(task_body["_nodes"]["completed"], 1);
    assert_eq!(task_body["nodes"][0]["status"], "completed");

    Ok(())
}

#[tokio::test]
async fn rest_distributed_forcemerge_is_async_and_tracks_all_nodes() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;

    let (force_merge_status, force_merge_body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
        "/stories/_forcemerge?max_num_segments=1",
        json!({}),
    )
    .await?;
    assert_eq!(force_merge_status, StatusCode::ACCEPTED);
    assert_eq!(
        force_merge_body["task"]["action"],
        "indices:admin/forcemerge"
    );
    assert_eq!(force_merge_body["task"]["coordinator_node"], "node-2");
    assert_eq!(force_merge_body["_nodes"]["total"], 3);
    assert_eq!(force_merge_body["_nodes"]["started"], 3);
    assert_eq!(force_merge_body["_nodes"]["failed"], 0);
    let task_id = force_merge_body["task"]["id"]
        .as_str()
        .expect("force-merge should return a task id")
        .to_string();

    let task_body = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = harness
                .client
                .get(format!("{}/_tasks/{}", harness.nodes[1].base_url, task_id))
                .send()
                .await?;
            let status = response.status();
            let body: Value = response.json().await?;
            if status == StatusCode::OK && body["task"]["status"] == "completed" {
                break Ok::<Value, anyhow::Error>(body);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    assert_eq!(task_body["task"]["coordinator_node"], "node-2");
    assert_eq!(task_body["_nodes"]["total"], 3);
    assert_eq!(task_body["_nodes"]["completed"], 3);
    assert_eq!(task_body["nodes"].as_array().unwrap().len(), 3);
    assert!(
        task_body["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["status"] == "completed")
    );

    Ok(())
}

#[tokio::test]
async fn rest_can_bulk_index_and_search_via_query_and_dsl() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index(&harness).await?;

    let global_bulk = concat!(
        "{\"index\":{\"_index\":\"products\",\"_id\":\"1\"}}\n",
        "{\"title\":\"iPhone Pro\",\"description\":\"iphone flagship\",\"brand\":\"Apple\",\"price\":999.0}\n",
        "{\"index\":{\"_index\":\"products\",\"_id\":\"2\"}}\n",
        "{\"title\":\"Galaxy\",\"description\":\"iphone competitor\",\"brand\":\"Samsung\",\"price\":799.0}\n"
    );
    let (global_bulk_status, global_bulk_body) = harness
        .post_ndjson("/_bulk?refresh=true", global_bulk)
        .await?;
    assert_eq!(global_bulk_status, StatusCode::OK);
    assert_eq!(global_bulk_body["errors"], json!(false));
    assert_eq!(global_bulk_body["items"].as_array().map(Vec::len), Some(2));

    let index_bulk = concat!(
        "{\"index\":{\"_id\":\"3\"}}\n",
        "{\"title\":\"iPhone\",\"description\":\"iphone standard\",\"brand\":\"Apple\",\"price\":899.0}\n"
    );
    let (index_bulk_status, index_bulk_body) = harness
        .post_ndjson("/products/_bulk?refresh=true", index_bulk)
        .await?;
    assert_eq!(index_bulk_status, StatusCode::OK);
    assert_eq!(index_bulk_body["errors"], json!(false));
    assert_eq!(index_bulk_body["items"].as_array().map(Vec::len), Some(1));

    let (search_status, search_body) = harness.get_json("/products/_search?q=iphone").await?;
    assert_eq!(search_status, StatusCode::OK);
    assert_eq!(search_body["hits"]["total"]["value"], json!(3));
    assert_eq!(
        search_body["hits"]["hits"].as_array().map(Vec::len),
        Some(3)
    );

    let (dsl_status, dsl_body) = harness
        .post_json(
            "/products/_search",
            json!({
                "query": { "match": { "description": "iphone" } },
                "size": 2,
                "from": 0
            }),
        )
        .await?;
    assert_eq!(dsl_status, StatusCode::OK);
    assert_eq!(dsl_body["hits"]["total"]["value"], json!(3));
    assert_eq!(dsl_body["hits"]["hits"].as_array().map(Vec::len), Some(2));

    Ok(())
}

#[tokio::test]
async fn rest_can_create_index_index_get_and_search_documents() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (doc_status, doc_body) = harness.get_json("/products/_doc/1").await?;
    assert_eq!(doc_status, StatusCode::OK);
    assert_eq!(doc_body["found"], json!(true));
    assert_eq!(doc_body["_source"]["brand"], json!("Apple"));

    let (search_status, search_body) = harness.get_json("/products/_search?q=iphone").await?;
    assert_eq!(search_status, StatusCode::OK);
    assert_eq!(search_body["hits"]["total"]["value"], json!(3));
    assert_eq!(
        search_body["hits"]["hits"].as_array().map(Vec::len),
        Some(3)
    );

    Ok(())
}

#[tokio::test]
async fn rest_date_fields_normalize_in_doc_search_and_sql_results() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_events_index_and_docs(&harness).await?;

    let (doc_status, doc_body) = harness.get_json("/events/_doc/1").await?;
    assert_eq!(doc_status, StatusCode::OK);
    assert_eq!(
        doc_body["_source"]["created_at"],
        json!("2025-01-05T02:45:00Z")
    );

    let (search_status, search_body) = harness
        .post_json(
            "/events/_search",
            json!({
                "query": {
                    "range": {
                        "created_at": {
                            "gte": "2025-01-05T02:00:00Z",
                            "lt": "2025-01-05T03:00:00Z"
                        }
                    }
                },
                "sort": [{ "created_at": "asc" }]
            }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK);
    assert_eq!(search_body["hits"]["total"]["value"], json!(1));
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("1"));
    assert_eq!(
        search_body["hits"]["hits"][0]["_source"]["created_at"],
        json!("2025-01-05T02:45:00Z")
    );

    let (sql_status, sql_body) = harness
        .post_json(
            "/events/_sql",
            json!({
                "query": "SELECT created_at FROM events ORDER BY created_at ASC"
            }),
        )
        .await?;
    assert_eq!(sql_status, StatusCode::OK);
    assert_eq!(sql_body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(
        sql_body["rows"],
        json!([
            { "created_at": "2025-01-05T02:45:00Z" },
            { "created_at": "2025-01-05T08:00:00Z" }
        ])
    );

    Ok(())
}

#[tokio::test]
async fn rest_sql_uses_tantivy_fast_fields_for_supported_query() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC, brand ASC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_grouped_partials"));
    assert_eq!(body["planner"]["group_by_columns"], json!(["brand"]));
    assert_eq!(body["matched_hits"], json!(3));
    Ok(())
}

#[tokio::test]
async fn rest_sql_duplicate_grouped_output_alias_returns_ambiguous_error() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand AS total, count(*) AS total FROM products GROUP BY brand"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], json!("parsing_exception"));
    let reason = body["error"]["reason"]
        .as_str()
        .expect("error reason should be present");
    assert!(
        reason.contains("ambiguous column reference 'total'"),
        "expected ambiguous-column error, got: {reason}"
    );

    Ok(())
}

#[tokio::test]
async fn rest_sql_uses_materialized_hits_fallback_for_select_star() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT * FROM products WHERE text_match(description, 'iphone')"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("materialized_hits_fallback"));
    assert_eq!(body["matched_hits"], json!(3));
    assert!(body["rows"].as_array().is_some());
    Ok(())
}

#[tokio::test]
async fn rest_sql_fast_field_path_returns_correct_ids_and_values() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // Non-grouped, specific columns → should use tantivy_fast_fields path
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') ORDER BY price DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 3);

    // Verify ordering (price DESC) and correct values
    let first_price = rows[0]["price"].as_f64().unwrap();
    let last_price = rows[rows.len() - 1]["price"].as_f64().unwrap();
    assert!(
        first_price >= last_price,
        "rows should be ordered by price DESC"
    );

    // Verify brand values are present and correct
    let brands: Vec<&str> = rows.iter().filter_map(|r| r["brand"].as_str()).collect();
    assert_eq!(brands.len(), 3);

    Ok(())
}

#[tokio::test]
async fn rest_sql_grouped_partials_accepts_case_insensitive_unquoted_columns() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT BRAND AS brand, AVG(PRICE) AS avg_price FROM products WHERE text_match(DESCRIPTION, 'iphone') AND PRICE > 700 GROUP BY BRAND ORDER BY avg_price DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_grouped_partials"));
    assert_eq!(body["planner"]["group_by_columns"], json!(["brand"]));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[0]["avg_price"], json!(949.0));
    assert_eq!(rows[1]["brand"], json!("Samsung"));
    assert_eq!(rows[1]["avg_price"], json!(799.0));

    Ok(())
}

#[tokio::test]
async fn rest_sql_fast_fields_accepts_case_insensitive_unquoted_columns_with_limit() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // This exercises the tantivy_fast_fields path (no GROUP BY) with uppercase
    // source columns. The LIMIT workaround in project_batch_to_sql_columns must
    // match canonicalized column names case-insensitively.
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT BRAND AS brand, PRICE AS price FROM products WHERE text_match(DESCRIPTION, 'iphone') ORDER BY PRICE DESC LIMIT 2"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[0]["price"], json!(999.0));
    assert_eq!(rows[1]["brand"], json!("Apple"));
    assert_eq!(rows[1]["price"], json!(899.0));

    Ok(())
}

#[tokio::test]
async fn rest_sql_explain_plan_only_shows_canonicalized_fields() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql/explain",
            json!({
                "query": "SELECT BRAND, PRICE FROM products WHERE text_match(DESCRIPTION, 'iphone')"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    // Plan-only EXPLAIN should show canonicalized field names
    let pipeline = body["pipeline"].as_array().expect("pipeline");
    let search_stage = &pipeline[0];
    let text_match = &search_stage["text_match"];
    assert_eq!(text_match["field"], json!("description"));

    Ok(())
}

#[tokio::test]
async fn rest_sql_truncated_flag_not_set_for_explicit_limit() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // LIMIT 1 but 3 docs match — user explicitly asked for 1, NOT truncated
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') LIMIT 1"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["matched_hits"], json!(3));
    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 1);

    // No LIMIT — all 3 docs fit within default 100K limit → not truncated
    let (status2, body2) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone')"
            }),
        )
        .await?;

    assert_eq!(status2, StatusCode::OK);
    assert_eq!(body2["truncated"], json!(false));
    assert_eq!(body2["matched_hits"], json!(3));

    Ok(())
}

#[tokio::test]
async fn rest_sql_limit_returns_exact_row_count() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // LIMIT 2: must return exactly 2 rows, not 2 × number_of_shards
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') LIMIT 2"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(
        rows.len(),
        2,
        "LIMIT 2 must return exactly 2 rows, got {}",
        rows.len()
    );

    // LIMIT 1 with ORDER BY: must return exactly 1 row
    let (status2, body2) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') ORDER BY price DESC LIMIT 1"
            }),
        )
        .await?;

    assert_eq!(status2, StatusCode::OK);
    let rows2 = body2["rows"].as_array().expect("rows should be array");
    assert_eq!(
        rows2.len(),
        1,
        "LIMIT 1 must return exactly 1 row, got {}",
        rows2.len()
    );

    // Verify it's the highest price (ORDER BY price DESC)
    let price = rows2[0]["price"].as_f64().unwrap();
    assert!(
        price >= 999.0,
        "ORDER BY DESC should return highest price first"
    );

    Ok(())
}

#[tokio::test]
async fn rest_sql_count_star_without_group_by_returns_correct_count() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // SELECT count(*) without GROUP BY — must return the actual match count, not 0
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT count(*) AS total FROM products WHERE text_match(description, 'iphone')"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 1, "count(*) should return exactly 1 row");
    let total = rows[0]["total"].as_i64().unwrap();
    assert_eq!(total, 3, "count(*) should return 3 matching docs");

    Ok(())
}

#[tokio::test]
async fn rest_sql_explain_analyze_returns_timings_and_rows() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // EXPLAIN ANALYZE: execute query and return plan + timings + rows
    let (status, body) = harness
        .post_json(
            "/products/_sql/explain",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone')",
                "analyze": true
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    // Plan fields present
    assert!(body.get("execution_strategy").is_some());
    assert!(body.get("pipeline").is_some());
    assert!(body.get("rewritten_sql").is_some());

    // Timings present with non-negative values
    let timings = &body["timings"];
    assert!(
        timings["planning_ms"].as_f64().unwrap() >= 0.0,
        "planning_ms should be non-negative"
    );
    assert!(
        timings["search_ms"].as_f64().unwrap() >= 0.0,
        "search_ms should be non-negative"
    );
    assert!(
        timings["total_ms"].as_f64().unwrap() > 0.0,
        "total_ms should be positive"
    );

    // Execution results present
    assert_eq!(body["matched_hits"], json!(3));
    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 3);
    assert!(body["row_count"].as_u64().unwrap() == 3);

    // Plain EXPLAIN (no analyze) should NOT have timings or rows
    let (status2, body2) = harness
        .post_json(
            "/products/_sql/explain",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone')"
            }),
        )
        .await?;

    assert_eq!(status2, StatusCode::OK);
    assert!(
        body2.get("timings").is_none(),
        "plain EXPLAIN must not have timings"
    );
    assert!(
        body2.get("rows").is_none(),
        "plain EXPLAIN must not have rows"
    );
    assert!(
        body2.get("matched_hits").is_none(),
        "plain EXPLAIN must not have matched_hits"
    );

    Ok(())
}

#[tokio::test]
async fn rest_sql_same_index_semijoin_uses_grouped_inner_having_and_text_match() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT title, brand, price FROM products WHERE brand IN (SELECT brand FROM products WHERE text_match(description, 'iphone') GROUP BY brand HAVING COUNT(*) > 1) ORDER BY price DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["matched_hits"], json!(2));
    assert_eq!(body["planner"]["has_residual_predicates"], json!(false));
    assert_eq!(body["planner"]["semijoin"]["outer_key"], json!("brand"));
    assert_eq!(body["planner"]["semijoin"]["inner_key"], json!("brand"));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["title"], json!("iPhone Pro"));
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[1]["title"], json!("iPhone"));
    assert_eq!(rows[1]["brand"], json!("Apple"));

    Ok(())
}

#[tokio::test]
async fn rest_sql_same_index_semijoin_zero_keys_preserves_aggregate_semantics() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT count(*) AS total FROM products WHERE brand IN (SELECT brand FROM products GROUP BY brand HAVING COUNT(*) > 10)"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_grouped_partials"));
    assert_eq!(body["matched_hits"], json!(0));
    assert_eq!(body["planner"]["semijoin"]["outer_key"], json!("brand"));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["total"], json!(0));

    Ok(())
}

#[tokio::test]
async fn rest_sql_explain_analyze_surfaces_semijoin_pipeline() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql/explain",
            json!({
                "query": "SELECT title, brand, price FROM products WHERE brand IN (SELECT brand FROM products WHERE text_match(description, 'iphone') GROUP BY brand HAVING COUNT(*) > 1) ORDER BY price DESC",
                "analyze": true
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["pipeline"][0]["name"], json!("semijoin_key_build"));
    assert_eq!(body["semijoin"]["outer_key"], json!("brand"));
    assert_eq!(body["semijoin"]["resolved_key_count"], json!(1));
    assert_eq!(
        body["semijoin"]["inner_plan"]["execution_strategy"],
        json!("tantivy_grouped_partials")
    );
    assert!(body["timings"]["semijoin_ms"].as_f64().unwrap() >= 0.0);

    Ok(())
}

#[tokio::test]
async fn rest_sql_in_and_between_pushdown() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // BETWEEN: price BETWEEN 800 AND 1000 should match iPhone Pro (999) and iPhone (899)
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') AND price BETWEEN 800 AND 1000"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(
        rows.len(),
        2,
        "BETWEEN 800..1000 should match 2 docs, got {}",
        rows.len()
    );
    // Verify pushed down (no residual predicates)
    assert_eq!(body["planner"]["has_residual_predicates"], json!(false));

    // IN: brand IN ('Samsung') should match 1 doc
    let (status2, body2) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT brand, price FROM products WHERE text_match(description, 'iphone') AND brand IN ('Samsung')"
            }),
        )
        .await?;

    assert_eq!(status2, StatusCode::OK);
    let rows2 = body2["rows"].as_array().expect("rows");
    assert_eq!(rows2.len(), 1, "IN ('Samsung') should match 1 doc");
    assert_eq!(rows2[0]["brand"], json!("Samsung"));
    assert_eq!(body2["planner"]["has_residual_predicates"], json!(false));

    Ok(())
}

#[tokio::test]
async fn rest_cat_shards_shows_started_state() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (_, _) = harness.post_json("/products/_refresh", json!({})).await?;

    // cat/shards should show STARTED for the local shard after docs are indexed
    let (status, text) = harness.get_text("/_cat/shards?v").await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        text.contains("STARTED"),
        "cat/shards must show STARTED for active shards, got: {text}"
    );
    // Should NOT show INITIALIZING after docs are loaded and refreshed
    assert!(
        !text.contains("INITIALIZING"),
        "cat/shards must not show INITIALIZING after docs are loaded, got: {text}"
    );

    Ok(())
}

#[tokio::test]
async fn rest_count_returns_correct_total() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // GET _count (match_all) — should return all 3 docs
    let (status, body) = harness.get_json("/products/_count").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], json!(3));
    assert_eq!(body["_shards"]["successful"], json!(1));
    assert_eq!(body["_shards"]["failed"], json!(0));

    // POST _count with a match query — should return matching docs only
    let (status, body) = harness
        .post_json(
            "/products/_count",
            json!({
                "query": { "term": { "brand": "Apple" } }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], json!(2));

    // POST _count with empty body — match_all
    let (status, body) = harness.post_json("/products/_count", json!({})).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], json!(3));

    // _count on non-existent index
    let (status, _body) = harness.get_json("/nonexistent/_count").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn rest_sql_count_star_uses_fast_path() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // SQL count(*) without WHERE should use count_star_fast execution mode
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({"query": "SELECT count(*) AS total FROM \"products\""}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("count_star_fast"));
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["total"], json!(3));

    // SQL count(*) WITH WHERE should NOT use count_star_fast
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({"query": "SELECT count(*) AS total FROM \"products\" WHERE brand = 'Apple'"}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_grouped_partials"));
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["total"], json!(2));

    Ok(())
}

#[tokio::test]
async fn rest_sql_expression_group_by_uses_fast_fields_fallback() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    // Expression GROUP BY (LOWER) can't use grouped_partials — falls to tantivy_fast_fields.
    // With only 3 docs this is well under the 1M scan limit, so it should succeed.
    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT LOWER(brand) AS brand_lower, count(*) AS cnt FROM products GROUP BY LOWER(brand) ORDER BY cnt DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["execution_mode"],
        json!("tantivy_fast_fields"),
        "expression GROUP BY should fall to tantivy_fast_fields, not grouped_partials"
    );
    assert_eq!(body["streaming_used"], json!(true));
    assert_eq!(body["truncated"], json!(false));

    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2, "should have 2 groups: apple and samsung");

    Ok(())
}

#[tokio::test]
async fn rest_sql_stream_endpoint_returns_ndjson_frames() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/products/_sql/stream",
            json!({
                "query": "SELECT LOWER(brand) AS brand_lower, count(*) AS cnt FROM products GROUP BY LOWER(brand) ORDER BY brand_lower ASC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    let frames: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;

    assert!(!frames.is_empty(), "expected at least one NDJSON frame");
    assert_eq!(frames[0]["type"], json!("meta"));
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(frames[0]["streaming_used"], json!(true));

    let rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| {
            frame["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
        })
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand_lower"], json!("apple"));
    assert_eq!(rows[0]["cnt"], json!(2));
    assert_eq!(rows[1]["brand_lower"], json!("samsung"));
    assert_eq!(rows[1]["cnt"], json!(1));

    Ok(())
}

#[tokio::test]
async fn rest_sql_stream_text_expression_group_by_preserves_nonstreaming_meta() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/products/_sql/stream",
            json!({
                "query": "SELECT LOWER(description) AS desc_lower, count(*) AS cnt FROM products WHERE text_match(description, 'iphone') GROUP BY LOWER(description) ORDER BY desc_lower ASC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    let frames: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;

    assert!(!frames.is_empty(), "expected at least one NDJSON frame");
    assert_eq!(frames[0]["type"], json!("meta"));
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(frames[0]["streaming_used"], json!(false));

    let rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| {
            frame["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
        })
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["desc_lower"], json!("iphone competitor"));
    assert_eq!(rows[0]["cnt"], json!(1));
    assert_eq!(rows[1]["desc_lower"], json!("iphone flagship"));
    assert_eq!(rows[2]["desc_lower"], json!("iphone standard"));

    Ok(())
}

#[tokio::test]
async fn rest_global_sql_stream_endpoint_routes_select_queries() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/_sql/stream",
            json!({
                "query": "SELECT brand, price FROM \"products\" ORDER BY brand ASC, price DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    let frames: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;

    assert!(!frames.is_empty(), "expected at least one NDJSON frame");
    assert_eq!(frames[0]["type"], json!("meta"));
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));

    let rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| {
            frame["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
        })
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[0]["price"], json!(999.0));
    assert_eq!(rows[1]["brand"], json!("Apple"));
    assert_eq!(rows[1]["price"], json!(899.0));
    assert_eq!(rows[2]["brand"], json!("Samsung"));
    assert_eq!(rows[2]["price"], json!(799.0));

    Ok(())
}

#[tokio::test]
async fn rest_global_sql_stream_accepts_case_insensitive_unquoted_columns() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/_sql/stream",
            json!({
                "query": "SELECT BRAND AS brand, PRICE AS price FROM \"products\" WHERE text_match(DESCRIPTION, 'iphone') AND PRICE > 800 ORDER BY PRICE DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    let frames: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;

    assert!(!frames.is_empty(), "expected at least one NDJSON frame");
    assert_eq!(frames[0]["type"], json!("meta"));
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));

    let rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| {
            frame["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
        })
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[0]["price"], json!(999.0));
    assert_eq!(rows[1]["brand"], json!("Apple"));
    assert_eq!(rows[1]["price"], json!(899.0));

    Ok(())
}

#[tokio::test]
async fn rest_sql_residual_path_accepts_mixed_case_mapping_fields_unquoted() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_mixed_case_rides_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/rides/_sql",
            json!({
                "query": "SELECT CAST(PULocationID / 100 AS INT) AS bucket, COUNT(*) AS rides FROM rides GROUP BY CAST(PULocationID / 100 AS INT) ORDER BY bucket"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["streaming_used"], json!(true));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["bucket"], json!(1));
    assert_eq!(rows[0]["rides"], json!(2));
    assert_eq!(rows[1]["bucket"], json!(2));
    assert_eq!(rows[1]["rides"], json!(1));

    Ok(())
}

#[tokio::test]
async fn rest_sql_stream_residual_path_preserves_quoted_mixed_case_mapping_fields() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_mixed_case_rides_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/rides/_sql/stream",
            json!({
                "query": "SELECT CAST(\"PULocationID\" / 100 AS INT) AS bucket, COUNT(*) AS rides FROM rides GROUP BY CAST(\"PULocationID\" / 100 AS INT) ORDER BY bucket"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);

    let frames: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;

    assert!(!frames.is_empty(), "expected at least one NDJSON frame");
    assert_eq!(frames[0]["type"], json!("meta"));
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(frames[0]["streaming_used"], json!(true));

    let rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| {
            frame["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
        })
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["bucket"], json!(1));
    assert_eq!(rows[0]["rides"], json!(2));
    assert_eq!(rows[1]["bucket"], json!(2));
    assert_eq!(rows[1]["rides"], json!(1));

    Ok(())
}

#[tokio::test]
async fn rest_global_sql_stream_count_star_handles_quoted_hyphenated_indices_case_insensitively()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs_named(&harness, "product-catalog").await?;

    for query in [
        r#"SELECT count(*) FROM "product-catalog""#,
        r#"SELECT count(*) from "product-catalog""#,
    ] {
        let (status, body) = harness
            .post_json_text("/_sql/stream", json!({ "query": query }))
            .await?;

        assert_eq!(status, StatusCode::OK, "query should succeed: {query}");

        let frames: Vec<Value> = body
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<_, _>>()?;

        assert!(!frames.is_empty(), "expected at least one NDJSON frame");
        assert_eq!(frames[0]["type"], json!("meta"));
        assert_eq!(frames[0]["execution_mode"], json!("count_star_fast"));

        let rows: Vec<Value> = frames
            .iter()
            .skip(1)
            .flat_map(|frame| {
                frame["rows"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
            })
            .collect();

        assert_eq!(rows.len(), 1, "count(*) should return exactly one row");
        let row = rows[0].as_object().expect("count row should be an object");
        assert_eq!(row.len(), 1, "count(*) row should have a single column");
        assert_eq!(row.values().next(), Some(&json!(3)));
    }

    Ok(())
}

#[tokio::test]
async fn rest_sql_expression_group_by_with_text_column_preserves_values() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT LOWER(description) AS desc_lower, count(*) AS cnt FROM products WHERE text_match(description, 'iphone') GROUP BY LOWER(description) ORDER BY desc_lower ASC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["streaming_used"], json!(false));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(
        rows.len(),
        3,
        "text-valued GROUP BY should keep all 3 groups"
    );

    let grouped: Vec<(&str, i64)> = rows
        .iter()
        .map(|row| {
            (
                row["desc_lower"]
                    .as_str()
                    .expect("group key should be present"),
                row["cnt"].as_i64().expect("count should be numeric"),
            )
        })
        .collect();
    assert_eq!(
        grouped,
        vec![
            ("iphone competitor", 1),
            ("iphone flagship", 1),
            ("iphone standard", 1),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn rest_sql_group_by_underscore_score_preserves_distinct_scores() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_scored_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT _score, count(*) AS cnt FROM products WHERE text_match(description, 'iphone') GROUP BY _score ORDER BY _score DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["streaming_used"], json!(false));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(
        rows.len(),
        3,
        "_score GROUP BY should keep one bucket per hit"
    );

    let scores: Vec<f64> = rows
        .iter()
        .map(|row| row["_score"].as_f64().expect("_score should be numeric"))
        .collect();
    assert!(scores[0] > scores[1] && scores[1] > scores[2]);
    assert!(rows.iter().all(|row| row["cnt"] == json!(1)));

    Ok(())
}

#[tokio::test]
async fn rest_sql_distinguishes_real_score_from_synthetic_underscore_score() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_products_index_with_real_score_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json(
            "/products/_sql",
            json!({
                "query": "SELECT title, score, _score FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["streaming_used"], json!(false));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(rows.len(), 3);

    let titles: Vec<&str> = rows
        .iter()
        .map(|row| row["title"].as_str().expect("title should be string"))
        .collect();
    assert_eq!(titles, vec!["iPhone Ultra", "iPhone Pro", "Galaxy"]);

    let real_scores: Vec<f64> = rows
        .iter()
        .map(|row| row["score"].as_f64().expect("real score should be numeric"))
        .collect();
    assert_eq!(real_scores, vec![3.0, 100.0, 50.0]);

    let relevance_scores: Vec<f64> = rows
        .iter()
        .map(|row| row["_score"].as_f64().expect("_score should be numeric"))
        .collect();
    assert!(relevance_scores[0] > relevance_scores[1] && relevance_scores[1] > relevance_scores[2]);

    Ok(())
}

#[tokio::test]
async fn rest_distributed_search_applies_custom_sort_when_one_shard_matches() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;

    let (status, body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
        "/stories/_search",
        json!({
            "query": { "wildcard": { "title": "story-0-*" } },
            "sort": [{ "title": "desc" }],
            "size": 10,
            "from": 0
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hits"]["total"]["value"], json!(3));
    assert_eq!(body["_shards"]["successful"], json!(3));
    assert_eq!(body["_shards"]["failed"], json!(0));

    let hits = body["hits"]["hits"]
        .as_array()
        .expect("hits should be an array");
    let titles: Vec<&str> = hits
        .iter()
        .map(|hit| {
            hit["_source"]["title"]
                .as_str()
                .expect("title should be a string")
        })
        .collect();
    assert_eq!(titles, vec!["story-0-2", "story-0-1", "story-0-0"]);

    Ok(())
}

#[tokio::test]
async fn rest_cat_segments_lists_cluster_segments_by_default() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;

    let expected_segment_rows: usize = harness
        .nodes
        .iter()
        .map(|node| {
            node.app_state
                .shard_manager
                .all_shards()
                .into_iter()
                .map(|(_, engine)| engine.segment_infos().len())
                .sum::<usize>()
        })
        .sum();

    let response = harness
        .client
        .get(format!("{}/_cat/segments?v", harness.nodes[1].base_url))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;

    assert_eq!(status, StatusCode::OK);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        expected_segment_rows + 1,
        "expected header plus every started segment row: {text}"
    );

    let mut shard_ids = Vec::new();
    for line in lines.iter().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields[0], "stories");
        shard_ids.push(fields[1].parse::<u32>()?);
    }
    shard_ids.sort_unstable();
    shard_ids.dedup();
    assert_eq!(shard_ids, vec![0, 1, 2]);

    Ok(())
}

#[tokio::test]
async fn rest_sql_distributed_grouped_partials_merge_numeric_keys_across_shards() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;

    let (status, body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
            "/stories/_sql",
            json!({
                "query": "SELECT upvotes, count(*) AS cnt FROM stories GROUP BY upvotes ORDER BY upvotes ASC LIMIT 10"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_grouped_partials"));
    assert_eq!(body["matched_hits"], json!(8));
    assert_eq!(body["_shards"]["successful"], json!(3));
    assert_eq!(body["_shards"]["failed"], json!(0));

    let rows = body["rows"].as_array().expect("rows should be array");
    assert_eq!(
        rows.len(),
        4,
        "numeric group keys must merge into 4 buckets"
    );

    let grouped: Vec<(i64, i64)> = rows
        .iter()
        .map(|row| {
            (
                row["upvotes"].as_i64().expect("upvotes should be integer"),
                row["cnt"].as_i64().expect("count should be integer"),
            )
        })
        .collect();
    assert_eq!(grouped, vec![(0, 3), (1, 2), (2, 2), (3, 1)]);

    Ok(())
}

#[tokio::test]
async fn rest_sql_distributed_semijoin_merges_inner_groups_across_shards() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;

    let (status, body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
            "/stories/_sql",
            json!({
                "query": "SELECT title, author FROM stories WHERE author IN (SELECT author FROM stories GROUP BY author HAVING COUNT(*) > 2) ORDER BY title ASC"
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["matched_hits"], json!(6));
    assert_eq!(body["_shards"]["successful"], json!(3));
    assert_eq!(body["_shards"]["failed"], json!(0));
    assert_eq!(body["planner"]["semijoin"]["outer_key"], json!("author"));

    let rows = body["rows"].as_array().expect("rows should be array");
    let titles: Vec<&str> = rows
        .iter()
        .map(|row| row["title"].as_str().expect("title should be present"))
        .collect();
    assert_eq!(
        titles,
        vec![
            "story-0-0",
            "story-0-1",
            "story-1-0",
            "story-1-1",
            "story-1-2",
            "story-2-0",
        ]
    );

    Ok(())
}
