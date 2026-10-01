use anyhow::Result;
use ferrissearch::api::{AppState, create_router};
use ferrissearch::cluster::ClusterManager;
use ferrissearch::cluster::state::{
    ClusterState, FieldMapping, FieldType, IndexMetadata, IndexSettings, NodeInfo, NodeRole,
    ShardRoutingEntry,
};
use ferrissearch::security::{SecurityApiKeyConfig, SecurityConfig};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::{
    PingRequest, internal_transport_client::InternalTransportClient,
};
use ferrissearch::transport::server::{
    create_transport_service_with_raft, create_transport_service_with_raft_and_storage,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

const RESERVED_METADATA_KEYS_FOR_TEST: &[&str] = &[
    "_id",
    "_doc_id",
    "_source",
    "_seq_no",
    "_primary_term",
    "_version",
    "_index",
    "_routing",
];

struct RestTestHarness {
    _temp_dir: TempDir,
    app_state: AppState,
    client: Client,
    base_url: String,
    transport_addr: std::net::SocketAddr,
    http_handle: JoinHandle<()>,
    transport_handle: JoinHandle<()>,
}

struct MultiNodeRestHarness {
    client: Client,
    nodes: Vec<MultiNodeRestNode>,
    _shared_remote_store: Option<TempDir>,
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
        .post(format!("{base_url}{path}"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value = response.json().await?;
    Ok((status, value))
}

async fn put_json_to_base_url(
    client: &Client,
    base_url: &str,
    path: &str,
    body: Value,
) -> Result<(StatusCode, Value)> {
    let response = client
        .put(format!("{base_url}{path}"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value = response.json().await?;
    Ok((status, value))
}

async fn get_json_from_base_url(
    client: &Client,
    base_url: &str,
    path: &str,
) -> Result<(StatusCode, Value)> {
    let response = client.get(format!("{base_url}{path}")).send().await?;
    let status = response.status();
    let value = response.json().await?;
    Ok((status, value))
}

fn assert_mapper_parsing_error(status: StatusCode, body: &Value, field: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
    assert_eq!(
        body["error"]["type"],
        json!("mapper_parsing_exception"),
        "{field}: {body}"
    );
    assert_eq!(
        body["error"]["reason"],
        json!(format!(
            "Field [{field}] is a metadata field and cannot be added inside a document. Use the index API request parameters."
        )),
        "{field}: {body}"
    );
}

fn assert_body_mapping_error(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"]["type"],
        json!("mapper_parsing_exception"),
        "{body}"
    );
    assert_eq!(
        body["error"]["reason"],
        json!("Field [body] is the built-in catch-all text field and can only be mapped as [text]"),
        "{body}"
    );
}

impl RestTestHarness {
    async fn start() -> Result<Self> {
        Self::start_with_column_cache(0, 0).await
    }

    async fn start_with_roles(roles: Vec<NodeRole>) -> Result<Self> {
        Self::start_internal(0, 0, roles, None).await
    }

    async fn start_with_column_cache(
        column_cache_bytes: u64,
        populate_threshold_percent: u8,
    ) -> Result<Self> {
        Self::start_internal(
            column_cache_bytes,
            populate_threshold_percent,
            vec![NodeRole::Master, NodeRole::Data],
            None,
        )
        .await
    }

    /// Start a single-node harness with security enabled and the given bootstrap
    /// API keys. The `SecurityManager` shares the same Raft-replicated cluster
    /// state, so dynamically-created keys/roles are visible on the auth path.
    async fn start_security_enabled(bootstrap_api_keys: Vec<SecurityApiKeyConfig>) -> Result<Self> {
        Self::start_internal(
            0,
            0,
            vec![NodeRole::Master, NodeRole::Data],
            Some(SecurityConfig {
                enabled: true,
                auto_create_security_index: false,
                bootstrap_api_keys,
            }),
        )
        .await
    }

    async fn start_internal(
        column_cache_bytes: u64,
        populate_threshold_percent: u8,
        roles: Vec<NodeRole>,
        security_config: Option<SecurityConfig>,
    ) -> Result<Self> {
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
            roles,
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
        // Clone the shared cluster state for the SecurityManager before it is
        // moved into the ClusterManager, so dynamic API keys/roles applied via
        // Raft are visible on the authentication path.
        let security_state = shared_state.clone();
        let manager = ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state);

        let column_cache = Arc::new(ferrissearch::engine::column_cache::ColumnCache::new(
            column_cache_bytes,
            populate_threshold_percent,
        ));
        let shard_manager = Arc::new(ShardManager::new_full(
            temp_dir.path(),
            ferrissearch::wal::TranslogDurability::Request,
            column_cache,
        ));
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
            storage_manager: Arc::new(
                ferrissearch::storage::StorageManager::new_in_path(temp_dir.path()).unwrap(),
            ),
            security_manager: Arc::new(match security_config {
                Some(config) => ferrissearch::security::SecurityManager::with_cluster_state(
                    config,
                    security_state,
                )
                .expect("valid security config"),
                None => ferrissearch::security::SecurityManager::disabled(),
            }),
            remote_store_reader_cache: Arc::new(
                ferrissearch::engine::remote_store::RemoteSplitReaderCache::default(),
            ),
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

        let app = create_router(app_state.clone());
        let http_handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(http_listener, app).await {
                tracing::error!("Test HTTP server failed: {}", error);
            }
        });

        let harness = Self {
            _temp_dir: temp_dir,
            app_state,
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
            base_url: format!("http://{http_addr}"),
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
                    // A 401 still proves the HTTP server is up (security enabled).
                    let s = response.status();
                    s == StatusCode::OK || s == StatusCode::UNAUTHORIZED
                } else {
                    false
                };
            let transport_ready = if let Ok(mut client) =
                InternalTransportClient::connect(format!("http://{}", self.transport_addr)).await
            {
                client
                    .ping(tonic::Request::new(PingRequest {
                        source_node_id: "node-1".into(),
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

    // ─── Authenticated request helpers (security-enabled harness) ─────────────

    async fn post_json_auth(
        &self,
        path: &str,
        api_key: &str,
        body: Value,
    ) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("ApiKey {api_key}"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn get_json_auth(&self, path: &str, api_key: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("ApiKey {api_key}"))
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn delete_json_auth(&self, path: &str, api_key: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .delete(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("ApiKey {api_key}"))
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn put_json_auth(
        &self,
        path: &str,
        api_key: &str,
        body: Value,
    ) -> Result<(StatusCode, Value)> {
        let response = self
            .client
            .put(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("ApiKey {api_key}"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    /// GET that returns the raw status without parsing a body — useful for
    /// asserting auth outcomes on endpoints whose success body is large.
    async fn get_status_auth(&self, path: &str, api_key: &str) -> Result<StatusCode> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("Authorization", format!("ApiKey {api_key}"))
            .send()
            .await?;
        Ok(response.status())
    }
}

impl MultiNodeRestHarness {
    async fn start_three_nodes() -> Result<Self> {
        Self::start_three_nodes_internal(vec![NodeRole::Master, NodeRole::Data], None).await
    }

    async fn start_three_nodes_with_shared_remote_store(
        master_roles: Vec<NodeRole>,
    ) -> Result<Self> {
        Self::start_three_nodes_internal(master_roles, Some(tempfile::tempdir()?)).await
    }

    async fn start_three_nodes_internal(
        master_roles: Vec<NodeRole>,
        shared_remote_store: Option<TempDir>,
    ) -> Result<Self> {
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
                    master_roles.clone()
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
            let storage_manager = Arc::new(match shared_remote_store.as_ref() {
                Some(shared_root) => ferrissearch::storage::StorageManager::new(
                    shared_root.path().to_string_lossy().into_owned(),
                    pending.temp_dir.path().join("_remote_store_workdir"),
                )?,
                None => {
                    ferrissearch::storage::StorageManager::new_in_path(pending.temp_dir.path())?
                }
            });
            let remote_store_reader_cache =
                Arc::new(ferrissearch::engine::remote_store::RemoteSplitReaderCache::default());
            let app_state = AppState {
                cluster_manager: cluster_manager.clone(),
                shard_manager: shard_manager.clone(),
                transport_client: transport_client.clone(),
                local_node_id: pending.node_id.clone(),
                raft: raft.clone(),
                worker_pools: ferrissearch::worker::WorkerPools::new(2, 2),
                task_manager: task_manager.clone(),
                storage_manager: storage_manager.clone(),
                security_manager: Arc::new(ferrissearch::security::SecurityManager::disabled()),
                remote_store_reader_cache: remote_store_reader_cache.clone(),
                sql_group_by_scan_limit: 1_000_000,
                sql_approximate_top_k: false,
            };

            let transport_service = create_transport_service_with_raft_and_storage(
                cluster_manager,
                shard_manager,
                transport_client,
                raft,
                task_manager,
                ferrissearch::transport::server::RemoteStoreTransportResources {
                    storage_manager,
                    remote_store_reader_cache,
                },
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

        let harness = Self {
            client,
            nodes,
            _shared_remote_store: shared_remote_store,
        };
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
                            source_node_id: node.app_state.local_node_id.clone(),
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        2,
        ShardRoutingEntry {
            primary: "node-3".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
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
        dynamic: Default::default(),
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

#[tokio::test]
async fn rest_keyword_arrays_and_write_receipts_are_preserved() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json(
            "/array-receipts",
            json!({
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"dynamic": "strict", "properties": {"tags": {"type": "keyword"}}}
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    let source = json!({"tags": ["b", "a", "a", null]});
    let (status, first) = harness
        .put_json("/array-receipts/_doc/one?refresh=true", source.clone())
        .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(first["_seq_no"], json!(0));
    let (status, stored) = harness.get_json("/array-receipts/_doc/one").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["_source"], source);

    let (status, rejected) = harness
        .put_json(
            "/array-receipts/_doc/invalid",
            json!({"tags": ["valid", {"bad": "object"}]}),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert!(
        rejected["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("tags")
    );
    let bulk = concat!(
        "{\"index\":{\"_id\":\"two\"}}\n{\"tags\":[\"b\"]}\n",
        "{\"index\":{\"_id\":\"three\"}}\n{\"tags\":[\"a\"]}\n"
    );
    let (status, response) = harness
        .post_ndjson("/array-receipts/_bulk?refresh=true", bulk)
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["errors"], json!(false), "{response}");
    assert_eq!(response["items"][0]["index"]["_seq_no"], json!(1));
    assert_eq!(response["items"][1]["index"]["_seq_no"], json!(2));
    let (_, aggregate) = harness
        .post_json(
            "/array-receipts/_search",
            json!({
                "size": 0, "aggs": {"tags": {"terms": {"field": "tags", "size": 10}}}
            }),
        )
        .await?;
    let buckets: std::collections::BTreeMap<_, _> = aggregate["aggregations"]["tags"]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|bucket| {
            (
                bucket["key"].as_str().unwrap().to_string(),
                bucket["doc_count"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        buckets,
        std::collections::BTreeMap::from([("a".into(), 2), ("b".into(), 2)])
    );
    let (_, invalid_bulk) = harness
        .post_ndjson(
            "/array-receipts/_bulk",
            "{\"index\":{\"_id\":\"invalid-bulk\"}}\n{\"tags\":[{\"bad\":\"object\"}]}\n",
        )
        .await?;
    assert_eq!(invalid_bulk["errors"], json!(true));
    assert_eq!(invalid_bulk["items"][0]["index"]["status"], json!(400));
    assert_eq!(
        invalid_bulk["items"][0]["index"]["error"]["type"],
        json!("mapper_parsing_exception")
    );
    let (status, updated) = harness
        .post_json(
            "/array-receipts/_update/three",
            json!({"doc": {"tags": ["c"]}}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["_seq_no"], json!(3));
    let (status, deleted) = harness.delete_json("/array-receipts/_doc/two").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["_seq_no"], json!(4));
    Ok(())
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
                &format!("/{index_name}/_doc/{doc_id}?refresh=true"),
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
            .put_json(&format!("/events/_doc/{doc_id}?refresh=true"), payload)
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
            .put_json(&format!("/rides/_doc/{doc_id}?refresh=true"), payload)
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
            .put_json(&format!("/products/_doc/{doc_id}?refresh=true"), payload)
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
            .put_json(&format!("/products/_doc/{doc_id}?refresh=true"), payload)
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
    assert_eq!(
        settings_body["products"]["settings"]["index"]["engine"],
        json!("local_shards")
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
    assert_eq!(
        settings_after_body["products"]["settings"]["index"]["engine"],
        json!("local_shards")
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
        .get_json(&format!("/products/_doc/{auto_id}"))
        .await?;
    assert_eq!(auto_doc_status, StatusCode::OK);
    assert_eq!(auto_doc_body["_source"]["brand"], json!("Google"));

    Ok(())
}

async fn create_write_contract_index(harness: &RestTestHarness, index: &str) -> Result<()> {
    let (status, body) = harness
        .put_json(
            &format!("/{index}"),
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 60000,
                    "flush_threshold_bytes": 0
                },
                "mappings": {"dynamic": false}
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}

const WRITE_PARAMETER_ENDPOINTS: &[(&str, &str)] = &[
    ("POST", "_doc"),
    ("PUT", "_doc/candidate"),
    ("POST", "_doc/candidate"),
    ("PUT", "_create/candidate"),
    ("POST", "_create/candidate"),
    ("POST", "_update/seed"),
    ("DELETE", "_doc/seed"),
    ("POST", "_bulk"),
    ("POST", "global_bulk"),
];

async fn write_parameter_request(
    harness: &RestTestHarness,
    index: &str,
    method: &str,
    endpoint: &str,
    query: &str,
) -> Result<(StatusCode, Value)> {
    let path = if endpoint == "global_bulk" {
        format!("/_bulk{query}")
    } else {
        format!("/{index}/{endpoint}{query}")
    };
    if matches!(endpoint, "_bulk" | "global_bulk") {
        return harness
            .post_ndjson(
                &path,
                &format!(
                    "{{\"index\":{{\"_index\":\"{index}\",\"_id\":\"candidate\",\"wait_for_active_shards\":\"1\"}}}}\n{{\"value\":\"changed\"}}\n"
                ),
            )
            .await;
    }
    let body = if endpoint.starts_with("_update/") {
        json!({"doc": {"value": "changed"}})
    } else {
        json!({"value": "changed"})
    };
    match method {
        "PUT" => harness.put_json(&path, body).await,
        "POST" => harness.post_json(&path, body).await,
        "DELETE" => harness.delete_json(&path).await,
        _ => unreachable!("test endpoint uses a write method"),
    }
}

fn assert_write_parameter_rejection(status: StatusCode, body: &Value, parameter: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{parameter}: {body}");
    assert_eq!(body["status"], 400, "{parameter}: {body}");
    assert_eq!(
        body["error"]["type"], "illegal_argument_exception",
        "{parameter}: {body}"
    );
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains(&format!("[{parameter}]")),
        "{parameter}: {body}"
    );
}

async fn assert_write_parameter_no_writes(
    harness: &RestTestHarness,
    index: &str,
    seed: &Value,
) -> Result<()> {
    let (status, document) = harness
        .get_json(&format!("/{index}/_doc/seed?realtime=true"))
        .await?;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_source"], json!({"value": "original"}));
    assert_eq!(document["_seq_no"], seed["_seq_no"], "{document}");
    assert_eq!(
        harness
            .get_json(&format!("/{index}/_doc/candidate?realtime=true"))
            .await?
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, refreshed) = harness
        .post_json(&format!("/{index}/_refresh"), json!({}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{refreshed}");
    let (status, count) = harness.get_json(&format!("/{index}/_count")).await?;
    assert_eq!(status, StatusCode::OK, "{count}");
    assert_eq!(count["count"], 1, "unexpected write: {count}");
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_rejects_unsupported_query_keys_without_writes() -> Result<()>
{
    let harness = RestTestHarness::start().await?;
    let index = "write-params";
    create_write_contract_index(&harness, index).await?;
    let (status, seed) = harness
        .put_json(&format!("/{index}/_doc/seed"), json!({"value": "original"}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{seed}");
    for &(method, endpoint) in WRITE_PARAMETER_ENDPOINTS {
        for (parameter, value) in [
            ("routing", "tenant"),
            ("routing", ""),
            ("_routing", "tenant"),
            ("pipeline", "ingest"),
            ("version", "2"),
            ("_version", "2"),
            ("version_type", "external"),
            ("_version_type", "external"),
            ("require_alias", "true"),
            ("require_alias", "false"),
            ("require_data_stream", "true"),
            ("dynamic_templates", "%7B%7D"),
            ("wait_for_active_shards", "2"),
            ("wait_for_active_shards", "all"),
            ("wait_for_active_shards", "0"),
            ("wait_for_active_shards", "-1"),
            ("wait_for_active_shards", "foo"),
            ("wait_for_active_shards", ""),
        ] {
            let (status, error) = write_parameter_request(
                &harness,
                index,
                method,
                endpoint,
                &format!("?{parameter}={value}"),
            )
            .await?;
            assert_write_parameter_rejection(status, &error, parameter);
            assert_write_parameter_no_writes(&harness, index, &seed).await?;
        }
        let additional = if endpoint.starts_with("_create/") {
            vec![
                ("retry_on_conflict", "0"),
                ("retry_on_conflict", "2"),
                ("op_type", "index"),
                ("op_type", "foo"),
            ]
        } else if method == "DELETE" {
            vec![
                ("retry_on_conflict", "0"),
                ("retry_on_conflict", "2"),
                ("op_type", "index"),
                ("op_type", "create"),
            ]
        } else if endpoint.starts_with("_update/") {
            vec![("op_type", "create")]
        } else if matches!(endpoint, "_bulk" | "global_bulk") {
            vec![
                ("retry_on_conflict", "0"),
                ("retry_on_conflict", "2"),
                ("if_seq_no", "0"),
                ("if_primary_term", "1"),
                ("op_type", "create"),
            ]
        } else {
            vec![("retry_on_conflict", "0"), ("retry_on_conflict", "2")]
        };
        for (parameter, value) in additional {
            let (status, error) = write_parameter_request(
                &harness,
                index,
                method,
                endpoint,
                &format!("?{parameter}={value}"),
            )
            .await?;
            assert_write_parameter_rejection(status, &error, parameter);
            assert_write_parameter_no_writes(&harness, index, &seed).await?;
        }
    }
    for path in [
        "/params-no-auto-create/_doc/1?routing=tenant",
        "/params-no-auto-create/_create/1?pipeline=ingest",
    ] {
        let (status, error) = harness.put_json(path, json!({"value": 1})).await?;
        assert_write_parameter_rejection(
            status,
            &error,
            if path.contains("routing") {
                "routing"
            } else {
                "pipeline"
            },
        );
        assert_eq!(
            harness.head_status("/params-no-auto-create").await?,
            StatusCode::NOT_FOUND
        );
    }
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_rejects_invalid_refresh_without_writes() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let index = "write-refresh-params";
    create_write_contract_index(&harness, index).await?;
    let (_, seed) = harness
        .put_json(&format!("/{index}/_doc/seed"), json!({"value": "original"}))
        .await?;
    for &(method, endpoint) in WRITE_PARAMETER_ENDPOINTS {
        for value in ["foo", "wait_for", "TRUE", "1"] {
            let (status, error) = write_parameter_request(
                &harness,
                index,
                method,
                endpoint,
                &format!("?refresh={value}"),
            )
            .await?;
            assert_write_parameter_rejection(status, &error, "refresh");
            assert!(error["error"]["reason"].as_str().unwrap().contains(value));
            assert_write_parameter_no_writes(&harness, index, &seed).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_bulk_metadata_rejects_whole_request_without_writes()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    let index = "bulk-metadata-params";
    create_write_contract_index(&harness, index).await?;
    let (_, seed) = harness
        .put_json(&format!("/{index}/_doc/seed"), json!({"value": "original"}))
        .await?;
    for endpoint in ["_bulk", "global_bulk"] {
        for action in ["index", "create", "update", "delete"] {
            let mut parameters = vec![
                ("routing", json!("tenant")),
                ("_routing", json!("tenant")),
                ("pipeline", json!("ingest")),
                ("version", json!(2)),
                ("_version", json!(2)),
                ("version_type", json!("external")),
                ("_version_type", json!("external")),
                ("require_alias", json!(true)),
                ("require_alias", json!(false)),
                ("require_data_stream", json!(true)),
                ("dynamic_templates", json!({"field": "template"})),
                ("op_type", json!("create")),
                ("refresh", json!(true)),
                ("wait_for_active_shards", json!(2)),
                ("wait_for_active_shards", json!("all")),
            ];
            if action != "update" {
                parameters.extend([
                    ("retry_on_conflict", json!(0)),
                    ("retry_on_conflict", json!(2)),
                ]);
            }
            for (offset, (parameter, value)) in parameters.into_iter().enumerate() {
                let rejected_position = offset % 3 + 1;
                let mut request = String::new();
                for position in 1..=3 {
                    if position == rejected_position {
                        request.push_str(
                            &json!({(action): {
                                "_index": index,
                                "_id": if matches!(action, "update" | "delete") {
                                    "seed"
                                } else {
                                    "candidate"
                                },
                                (parameter): value
                            }})
                            .to_string(),
                        );
                        request.push('\n');
                        if action != "delete" {
                            request.push_str(if action == "update" {
                                "{\"doc\":{\"value\":\"changed\"}}\n"
                            } else {
                                "{\"value\":\"changed\"}\n"
                            });
                        }
                    } else {
                        request.push_str(
                            &format!(
                                "{{\"index\":{{\"_index\":\"{index}\",\"_id\":\"candidate-{position}\"}}}}\n{{\"value\":\"changed\"}}\n"
                            ),
                        );
                    }
                }
                let path = if endpoint == "global_bulk" {
                    "/_bulk".to_string()
                } else {
                    format!("/{index}/_bulk")
                };
                let (status, error) = harness.post_ndjson(&path, &request).await?;
                assert_write_parameter_rejection(status, &error, parameter);
                assert!(
                    error["error"]["reason"]
                        .as_str()
                        .unwrap()
                        .contains(&format!("item [{rejected_position}]")),
                    "{error}"
                );
                assert_write_parameter_no_writes(&harness, index, &seed).await?;
                for position in 1..=3 {
                    assert_eq!(
                        harness
                            .get_json(&format!("/{index}/_doc/candidate-{position}?realtime=true"))
                            .await?
                            .0,
                        StatusCode::NOT_FOUND
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_preserves_supported_and_benign_query_parameters()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    let index = "accepted-write-params";
    create_write_contract_index(&harness, index).await?;
    for &(method, endpoint) in WRITE_PARAMETER_ENDPOINTS {
        for query in [
            "",
            "?wait_for_active_shards=1",
            "?refresh=true",
            "?refresh=false",
            "?refresh",
            "?refresh=",
            "?timeout=1s&pretty&human=true&error_trace=true&filter_path=_id&wait_for_active_shards=1",
        ] {
            let (_, seed) = harness
                .put_json(&format!("/{index}/_doc/seed"), json!({"value": "original"}))
                .await?;
            let query = if endpoint.starts_with("_update/") {
                format!(
                    "{}{}retry_on_conflict=2&if_seq_no={}&if_primary_term={}",
                    query,
                    if query.is_empty() { "?" } else { "&" },
                    seed["_seq_no"],
                    seed["_primary_term"]
                )
            } else if matches!(endpoint, "_doc" | "_doc/candidate" | "_create/candidate") {
                format!(
                    "{}{}op_type=create",
                    query,
                    if query.is_empty() { "?" } else { "&" }
                )
            } else if method == "DELETE" {
                format!(
                    "{}{}if_seq_no={}&if_primary_term={}",
                    query,
                    if query.is_empty() { "?" } else { "&" },
                    seed["_seq_no"],
                    seed["_primary_term"]
                )
            } else {
                query.to_string()
            };
            let (status, body) =
                write_parameter_request(&harness, index, method, endpoint, &query).await?;
            assert!(status.is_success(), "{method} {endpoint}{query}: {body}");
            if matches!(endpoint, "_bulk" | "global_bulk") {
                assert_eq!(body["errors"], false, "{body}");
                assert_eq!(
                    harness
                        .get_json(&format!("/{index}/_doc/candidate?realtime=true"))
                        .await?
                        .1["_source"],
                    json!({"value": "changed"})
                );
                assert_eq!(
                    harness
                        .delete_json(&format!("/{index}/_doc/candidate"))
                        .await?
                        .0,
                    StatusCode::OK
                );
            } else if method == "DELETE" {
                assert_eq!(body["result"], "deleted", "{body}");
            } else {
                let id = body["_id"].as_str().unwrap();
                let (status, document) = harness
                    .get_json(&format!(
                        "/{index}/_doc/{id}?realtime=true&_source=value&_source_includes=value&_source_excludes=missing&pretty&filter_path=_source"
                    ))
                    .await?;
                assert_eq!(status, StatusCode::OK, "{document}");
                assert_eq!(document["_source"], json!({"value": "changed"}));
                assert_eq!(
                    harness.delete_json(&format!("/{index}/_doc/{id}")).await?.0,
                    StatusCode::OK
                );
            }
        }
    }
    let index = "accepted-bulk-update-params";
    create_write_contract_index(&harness, index).await?;
    let (_, first) = harness
        .put_json(&format!("/{index}/_doc/seed"), json!({"value": 1}))
        .await?;
    let request = format!(
        "{{\"update\":{{\"_id\":\"seed\",\"retry_on_conflict\":2,\"if_seq_no\":{},\"if_primary_term\":{},\"wait_for_active_shards\":1}}}}\n{{\"doc\":{{\"value\":2}}}}\n",
        first["_seq_no"], first["_primary_term"]
    );
    let (status, body) = harness
        .post_ndjson(
            &format!("/{index}/_bulk?refresh=true&timeout=1s&pretty"),
            &request,
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false, "{body}");
    assert_eq!(
        body["items"][0]["update"]["_seq_no"],
        first["_seq_no"].as_u64().unwrap() + 1
    );
    assert_eq!(
        harness.get_json(&format!("/{index}/_doc/seed")).await?.1["_source"],
        json!({"value": 2})
    );
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_delete_refreshes_local_reader() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let index = "delete-refresh-params";
    create_write_contract_index(&harness, index).await?;
    for query in ["?refresh=true", "?refresh", "?refresh="] {
        let (status, body) = harness
            .put_json(
                &format!("/{index}/_doc/seed?refresh=true"),
                json!({"value": 1}),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(
            harness
                .get_json(&format!("/{index}/_doc/seed?realtime=false"))
                .await?
                .0,
            StatusCode::OK
        );
        let (status, body) = harness
            .delete_json(&format!("/{index}/_doc/seed{query}"))
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            harness
                .get_json(&format!("/{index}/_doc/seed?realtime=false"))
                .await?
                .0,
            StatusCode::NOT_FOUND
        );
    }
    Ok(())
}

#[tokio::test]
async fn writes_regression_write_params_index_creation_rejects_unsupported_safety_parameters()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    for (parameter, value) in [
        ("routing", "tenant"),
        ("pipeline", "ingest"),
        ("version", "2"),
        ("version_type", "external"),
        ("wait_for_active_shards", "2"),
        ("wait_for_active_shards", "all"),
        ("refresh", "wait_for"),
    ] {
        let (status, body) = harness
            .put_json(
                &format!("/rejected-create-params?{parameter}={value}"),
                json!({"settings": {"number_of_shards": 1, "number_of_replicas": 0}}),
            )
            .await?;
        assert_write_parameter_rejection(status, &body, parameter);
        assert_eq!(
            harness.head_status("/rejected-create-params").await?,
            StatusCode::NOT_FOUND
        );
    }
    let (status, body) = harness
        .put_json(
            "/accepted-create-params?wait_for_active_shards=1&timeout=1s&pretty&human=true&error_trace=true&filter_path=acknowledged",
            json!({"settings": {"number_of_shards": 1, "number_of_replicas": 0}}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    Ok(())
}

#[tokio::test]
async fn writes_regression_update_preserves_unrefreshed_put() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "update-repro").await?;
    let (status, body) = harness
        .put_json(
            "/update-repro/_doc/42?refresh=true",
            json!({"name": "a", "price": 10}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = harness
        .put_json("/update-repro/_doc/42", json!({"name": "b", "price": 20}))
        .await?;
    assert!(status.is_success(), "{body}");
    let (status, body) = harness
        .post_json("/update-repro/_update/42", json!({"doc": {"stock": 5}}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    harness
        .post_json("/update-repro/_refresh", json!({}))
        .await?;
    let (status, body) = harness.get_json("/update-repro/_doc/42").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["_source"],
        json!({"name": "b", "price": 20, "stock": 5}),
        "an acknowledged PUT must survive the following update"
    );
    Ok(())
}

#[tokio::test]
async fn writes_regression_cas_rejects_recreated_index_with_matching_document_token() -> Result<()>
{
    use ferrissearch::transport::proto::ShardDocRequest;

    let harness = RestTestHarness::start().await?;
    let index = "index-incarnation";
    create_write_contract_index(&harness, index).await?;
    let (status, _) = harness
        .put_json(
            "/index-incarnation/_doc/same",
            json!({"body": "old incarnation"}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, old_document) = harness.get_json("/index-incarnation/_doc/same").await?;
    assert_eq!(status, StatusCode::OK);
    let old_uuid = harness.app_state.cluster_manager.get_state().indices[index]
        .uuid
        .to_string();
    let (status, _) = harness.delete_json("/index-incarnation").await?;
    assert_eq!(status, StatusCode::OK);
    create_write_contract_index(&harness, index).await?;
    let current_source = json!({"body": "new incarnation"});
    let (status, _) = harness
        .put_json("/index-incarnation/_doc/same", current_source.clone())
        .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, current_document) = harness.get_json("/index-incarnation/_doc/same").await?;
    assert_eq!(status, StatusCode::OK);
    let new_uuid = harness.app_state.cluster_manager.get_state().indices[index]
        .uuid
        .to_string();
    assert_ne!(old_uuid, new_uuid);
    assert_eq!(old_document["_seq_no"], current_document["_seq_no"]);
    assert_eq!(
        old_document["_primary_term"],
        current_document["_primary_term"]
    );

    let engine = harness.app_state.shard_manager.get_shard(index, 0).unwrap();
    let sequence_before = engine.sequence_stats();
    let wal_before = engine.retained_recovery_ops(0, 100, 1024 * 1024)?;
    let mut client =
        InternalTransportClient::connect(format!("http://{}", harness.transport_addr)).await?;
    let result = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index.into(),
            shard_id: 0,
            doc_id: "same".into(),
            payload_json: serde_json::to_vec(&json!({"body": "stale update"}))?,
            if_seq_no: old_document["_seq_no"].as_u64(),
            if_primary_term: old_document["_primary_term"].as_u64(),
            index_uuid: Some(old_uuid.clone()),
            ..Default::default()
        }))
        .await;
    assert!(
        matches!(&result, Err(error) if error.code() == tonic::Code::NotFound),
        "stale incarnation CAS must return NOT_FOUND, not mutate the new index: {result:?}"
    );
    assert_eq!(old_document["_index_uuid"], old_uuid);
    assert_eq!(current_document["_index_uuid"], new_uuid);
    let (status, missing) = harness.get_json("/index-incarnation/_doc/missing").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["_index_uuid"], new_uuid);
    let missing_upsert = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index.into(),
            shard_id: 0,
            doc_id: "missing".into(),
            payload_json: serde_json::to_vec(&json!({"body": "stale upsert"}))?,
            create_only: true,
            index_uuid: Some(old_uuid),
            ..Default::default()
        }))
        .await;
    assert!(
        matches!(&missing_upsert, Err(error) if error.code() == tonic::Code::NotFound),
        "{missing_upsert:?}"
    );
    assert_eq!(engine.sequence_stats(), sequence_before);
    let wal_after = engine.retained_recovery_ops(0, 100, 1024 * 1024)?;
    assert_eq!(wal_after.operations.len(), wal_before.operations.len());
    let (status, after) = harness.get_json("/index-incarnation/_doc/same").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["_source"], current_source);
    assert_eq!(after["_seq_no"], current_document["_seq_no"]);
    assert_eq!(after["_primary_term"], current_document["_primary_term"]);
    let valid = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index.into(),
            shard_id: 0,
            doc_id: "same".into(),
            payload_json: serde_json::to_vec(&current_source)?,
            if_seq_no: current_document["_seq_no"].as_u64(),
            if_primary_term: current_document["_primary_term"].as_u64(),
            index_uuid: Some(new_uuid.clone()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(valid.success, "{valid:?}");
    assert_eq!(
        valid.seq_no,
        current_document["_seq_no"]
            .as_u64()
            .map(|sequence| sequence + 1)
    );
    let (status, _) = harness.delete_json("/index-incarnation").await?;
    assert_eq!(status, StatusCode::OK);
    let disappeared = client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: index.into(),
            shard_id: 0,
            doc_id: "same".into(),
            payload_json: serde_json::to_vec(&current_source)?,
            if_seq_no: valid.seq_no,
            if_primary_term: valid.primary_term,
            index_uuid: Some(new_uuid),
            ..Default::default()
        }))
        .await;
    assert!(
        matches!(&disappeared, Err(error) if error.code() == tonic::Code::NotFound),
        "{disappeared:?}"
    );
    Ok(())
}

#[tokio::test]
async fn writes_regression_bulk_delete_and_update_preserve_action_boundaries() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "bulk-repro").await?;
    harness
        .put_json("/bulk-repro/_doc/old?refresh=true", json!({"keep": 1}))
        .await?;
    let body = concat!(
        "{\"delete\":{\"_id\":\"old\"}}\n",
        "{\"index\":{\"_id\":\"new\"}}\n",
        "{\"keep\":2}\n",
        "{\"update\":{\"_id\":\"new\"}}\n",
        "{\"doc\":{\"added\":3}}\n"
    );
    let (status, response) = harness
        .post_ndjson("/bulk-repro/_bulk?refresh=true", body)
        .await?;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["errors"], json!(false), "{response}");
    assert_eq!(
        response["items"].as_array().map(Vec::len),
        Some(3),
        "{response}"
    );
    assert_eq!(response["items"][0]["delete"]["result"], json!("deleted"));
    assert_eq!(response["items"][1]["index"]["result"], json!("created"));
    assert_eq!(response["items"][2]["update"]["result"], json!("updated"));
    assert_eq!(
        harness.get_json("/bulk-repro/_doc/old").await?.0,
        StatusCode::NOT_FOUND
    );
    let (status, document) = harness.get_json("/bulk-repro/_doc/new").await?;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_source"], json!({"keep": 2, "added": 3}));
    Ok(())
}

#[tokio::test]
async fn writes_regression_realtime_get_survives_delete_and_wal_flush() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "realtime").await?;
    let (_, first) = harness
        .put_json("/realtime/_doc/1", json!({"value": 1}))
        .await?;
    let (status, document) = harness.get_json("/realtime/_doc/1").await?;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_source"], json!({"value": 1}));
    assert_eq!(document["_seq_no"], first["_seq_no"]);
    assert_eq!(document["_primary_term"], first["_primary_term"]);
    assert_eq!(
        harness.get_json("/realtime/_doc/1?realtime=false").await?.0,
        StatusCode::NOT_FOUND
    );
    harness.post_json("/realtime/_refresh", json!({})).await?;
    let (_, second) = harness
        .put_json("/realtime/_doc/1", json!({"value": 2}))
        .await?;
    assert_eq!(second["result"], "updated");
    assert_eq!(
        harness.get_json("/realtime/_doc/1").await?.1["_source"]["value"],
        2
    );
    assert_eq!(
        harness.get_json("/realtime/_doc/1?realtime=false").await?.1["_source"]["value"],
        1
    );
    let (status, deleted) = harness.delete_json("/realtime/_doc/1").await?;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(
        harness.get_json("/realtime/_doc/1").await?.0,
        StatusCode::NOT_FOUND
    );
    let (status, recreated) = harness
        .put_json("/realtime/_doc/1", json!({"value": 3}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{recreated}");
    let (status, flushed) = harness.post_json("/realtime/_flush", json!({})).await?;
    assert_eq!(status, StatusCode::OK, "{flushed}");
    assert_eq!(flushed["_shards"]["failed"], 0);
    let (status, document) = harness.get_json("/realtime/_doc/1").await?;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_source"], json!({"value": 3}));
    assert_eq!(document["_seq_no"], recreated["_seq_no"]);
    assert_eq!(document["_primary_term"], recreated["_primary_term"]);
    Ok(())
}

#[tokio::test]
async fn writes_regression_conditions_create_upsert_and_noop() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "conditions").await?;
    let (status, first) = harness
        .put_json("/conditions/_create/1", json!({"nested": {"keep": 1}}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    let seq = first["_seq_no"].as_u64().unwrap();
    let term = first["_primary_term"].as_u64().unwrap();
    for path in ["/conditions/_create/1", "/conditions/_doc/1?op_type=create"] {
        let (status, conflict) = harness.put_json(path, json!({"wrong": true})).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
        assert_eq!(
            conflict["error"]["type"],
            "version_conflict_engine_exception"
        );
        assert!(
            conflict["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("document already exists")
        );
    }
    assert_eq!(
        harness
            .post_json("/conditions/_create/1", json!({}))
            .await?
            .0,
        StatusCode::CONFLICT
    );
    for query in ["if_seq_no=0", "if_primary_term=1"] {
        let (status, error) = harness
            .put_json(&format!("/conditions/_doc/1?{query}"), json!({}))
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(error["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            harness
                .delete_json(&format!("/conditions/_doc/1?{query}"))
                .await?
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let path = format!("/conditions/_doc/1?if_seq_no={seq}&if_primary_term={term}");
    let (status, second) = harness
        .put_json(&path, json!({"nested": {"keep": 1}, "value": 2}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["_seq_no"], seq + 1);
    let (status, conflict) = harness.put_json(&path, json!({"wrong": true})).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert!(
        conflict["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("current document has seqNo")
    );
    let (status, noop) = harness
        .post_json("/conditions/_update/1", json!({"doc": {"value": 2}}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{noop}");
    assert_eq!(noop["result"], "noop");
    assert_eq!(noop["_seq_no"], second["_seq_no"]);
    let (_, updated) = harness
        .post_json(
            "/conditions/_update/1",
            json!({
                "doc": {"nested": {"added": 3}}, "detect_noop": false
            }),
        )
        .await?;
    assert_eq!(updated["_seq_no"], seq + 2);
    let (_, document) = harness.get_json("/conditions/_doc/1").await?;
    assert_eq!(
        document["_source"]["nested"],
        json!({"keep": 1, "added": 3})
    );
    let delete = format!(
        "/conditions/_doc/1?if_seq_no={}&if_primary_term={term}",
        seq + 2
    );
    assert_eq!(harness.delete_json(&delete).await?.0, StatusCode::OK);
    assert_eq!(harness.delete_json(&delete).await?.0, StatusCode::CONFLICT);
    let (status, missing_delete) = harness.delete_json("/conditions/_doc/1").await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing_delete}");
    assert_eq!(missing_delete["result"], "not_found");
    assert!(missing_delete.get("error").is_none());
    let (status, missing) = harness
        .post_json("/conditions/_update/missing", json!({"doc": {"a": 1}}))
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    assert_eq!(missing["error"]["type"], "document_missing_exception");
    let (status, upsert) = harness
        .post_json(
            "/conditions/_update/upsert",
            json!({
                "doc": {"ignored_on_create": 1}, "upsert": {"from_upsert": 2}
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{upsert}");
    assert_eq!(upsert["result"], "created");
    assert_eq!(
        harness.get_json("/conditions/_doc/upsert").await?.1["_source"],
        json!({"from_upsert": 2})
    );
    let (status, upsert) = harness
        .post_json(
            "/conditions/_update/doc-upsert",
            json!({
                "doc": {"from_doc": 3}, "doc_as_upsert": true
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{upsert}");
    for key in ["script", "scripted_upsert", "fields", "_source", "unknown"] {
        let (status, error) = harness
            .post_json(
                "/conditions/_update/upsert",
                json!({
                    "doc": {"from_upsert": 4}, (key): true
                }),
            )
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(error["error"]["type"], "illegal_argument_exception");
        assert!(error["error"]["reason"].as_str().unwrap().contains(key));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_regression_concurrent_updates_preserve_every_acknowledged_field() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "concurrent-updates").await?;
    harness
        .put_json("/concurrent-updates/_doc/1", json!({"seed": true}))
        .await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(24));
    let writes = (0..24).map(|id| {
        let client = harness.client.clone();
        let url = format!("{}/concurrent-updates/_update/1", harness.base_url);
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            let response = client
                .post(url)
                .json(&json!({"doc": {format!("field-{id}"): id}}))
                .send()
                .await?;
            let status = response.status();
            let body: Value = response.json().await?;
            Ok::<_, anyhow::Error>((id, status, body))
        }
    });
    let mut acknowledged = Vec::new();
    for result in futures::future::join_all(writes).await {
        let (id, status, body) = result?;
        match status {
            StatusCode::OK => {
                assert_eq!(body["result"], "updated", "{body}");
                acknowledged.push(id);
            }
            StatusCode::CONFLICT => {
                assert_eq!(body["error"]["type"], "version_conflict_engine_exception")
            }
            _ => panic!("unexpected update outcome: {status}: {body}"),
        }
    }
    assert!(!acknowledged.is_empty());
    let (_, document) = harness.get_json("/concurrent-updates/_doc/1").await?;
    assert_eq!(document["_source"]["seed"], true);
    assert_eq!(document["_seq_no"], acknowledged.len() as u64);
    for id in acknowledged {
        assert_eq!(document["_source"][format!("field-{id}")], id, "{document}");
    }
    let retry_writes = (0..12).map(|id| {
        let client = harness.client.clone();
        let url = format!(
            "{}/concurrent-updates/_update/1?retry_on_conflict=24",
            harness.base_url
        );
        async move {
            let response = client
                .post(url)
                .json(&json!({"doc": {format!("retry-{id}"): id}}))
                .send()
                .await?;
            let status = response.status();
            let body: Value = response.json().await?;
            assert_eq!(status, StatusCode::OK, "{body}");
            Ok::<_, anyhow::Error>(())
        }
    });
    for result in futures::future::join_all(retry_writes).await {
        result?;
    }
    let (_, document) = harness.get_json("/concurrent-updates/_doc/1").await?;
    for id in 0..12 {
        assert_eq!(document["_source"][format!("retry-{id}")], id, "{document}");
    }
    Ok(())
}

#[tokio::test]
async fn writes_regression_bulk_order_conflicts_and_missing_delete() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "bulk-order").await?;
    let (_, seed) = harness.put_json("/bulk-order/_doc/seed", json!({})).await?;
    let term = seed["_primary_term"].as_u64().unwrap();
    let request = concat!(
        "{\"index\":{\"_id\":\"x\"}}\n{\"a\":1}\n",
        "{\"update\":{\"_id\":\"x\",\"retry_on_conflict\":2}}\n{\"doc\":{\"b\":2}}\n",
        "{\"create\":{\"_id\":\"x\"}}\n{\"wrong\":true}\n",
        "{\"delete\":{\"_id\":\"x\"}}\n",
        "{\"create\":{\"_id\":\"x\"}}\n{\"c\":3}\n",
        "{\"update\":{\"_id\":\"x\"}}\n{\"doc\":{\"d\":4}}\n",
        "{\"update\":{\"_id\":\"x\"}}\n{\"doc\":{\"d\":4}}\n",
        "{\"delete\":{\"_id\":\"x\"}}\n",
        "{\"delete\":{\"_id\":\"x\"}}\n",
        "{\"index\":{\"_id\":\"x\"}}\n{\"final\":9}\n",
        "{\"update\":{\"_id\":\"x\"}}\n{\"doc\":{\"retained\":10}}\n"
    );
    let (status, response) = harness.post_ndjson("/bulk-order/_bulk", request).await?;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["errors"], true);
    let actions = [
        "index", "update", "create", "delete", "create", "update", "update", "delete", "delete",
        "index", "update",
    ];
    let sequences = [
        Some(1),
        Some(2),
        None,
        Some(3),
        Some(4),
        Some(5),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
    ];
    assert_eq!(response["items"].as_array().unwrap().len(), actions.len());
    for (position, (action, seq)) in actions.into_iter().zip(sequences).enumerate() {
        assert_eq!(
            response["items"][position][action]["_seq_no"].as_u64(),
            seq,
            "{response}"
        );
    }
    assert_eq!(response["items"][2]["create"]["status"], 409);
    assert_eq!(response["items"][6]["update"]["result"], "noop");
    assert_eq!(response["items"][8]["delete"]["status"], 404);
    assert!(response["items"][8]["delete"].get("error").is_none());
    assert_eq!(
        harness.get_json("/bulk-order/_doc/x").await?.1["_source"],
        json!({"final": 9, "retained": 10})
    );
    let request = format!(
        "{{\"index\":{{\"_id\":\"x\",\"if_seq_no\":8,\"if_primary_term\":{term}}}}}\n{{\"wrong\":true}}\n\
         {{\"update\":{{\"_id\":\"x\",\"if_seq_no\":9,\"if_primary_term\":{term}}}}}\n{{\"doc\":{{\"conditional\":11}}}}\n"
    );
    let (_, response) = harness.post_ndjson("/bulk-order/_bulk", &request).await?;
    assert_eq!(response["items"][0]["index"]["status"], 409, "{response}");
    assert_eq!(response["items"][1]["update"]["_seq_no"], 10, "{response}");
    let (_, response) = harness
        .post_ndjson("/bulk-order/_bulk", "{\"delete\":{\"_id\":\"absent\"}}\n")
        .await?;
    assert_eq!(response["errors"], false, "{response}");
    assert_eq!(response["items"][0]["delete"]["status"], 404);
    Ok(())
}

#[tokio::test]
async fn writes_regression_bulk_rejects_action_errors_without_writing_and_keeps_source_errors()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "bulk-parse").await?;
    for malformed in [
        "not-json",
        "{\"unknown\":{}}",
        "{\"index\":{},\"delete\":{}}",
        "{\"index\":[]}",
        "[]",
        "{}",
    ] {
        let request =
            format!("{{\"index\":{{\"_id\":\"must-not-write\"}}}}\n{{\"a\":1}}\n{malformed}\n");
        let (status, error) = harness.post_ndjson("/bulk-parse/_bulk", &request).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(error["error"]["type"], "illegal_argument_exception");
        assert!(
            error["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("line [3]")
        );
        assert_eq!(
            harness.get_json("/bulk-parse/_doc/must-not-write").await?.0,
            StatusCode::NOT_FOUND
        );
    }
    let (status, error) = harness
        .post_ndjson(
            "/bulk-parse/_bulk",
            "{\"update\":{\"_id\":\"missing-source\"}}\n",
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["error"]["type"], "illegal_argument_exception");
    let (_, response) = harness
        .post_ndjson(
            "/bulk-parse/_bulk",
            concat!(
                "{\"index\":{\"_id\":\"invalid\"}}\n{broken\n",
                "{\"index\":{\"_id\":\"valid\"}}\n{\"value\":1}\n"
            ),
        )
        .await?;
    assert_eq!(response["items"].as_array().unwrap().len(), 2, "{response}");
    assert_eq!(
        response["items"][0]["index"]["error"]["type"],
        "mapper_parsing_exception"
    );
    assert_eq!(response["items"][0]["index"]["status"], 400);
    assert_eq!(response["items"][1]["index"]["_seq_no"], 0);
    assert_eq!(
        harness.get_json("/bulk-parse/_doc/valid").await?.1["_source"],
        json!({"value": 1})
    );
    let (_, response) = harness
        .post_ndjson(
            "/bulk-parse/_bulk",
            "{\"index\":{\"_index\":\"bulk-other\",\"_id\":\"override\"}}\n{\"value\":2}\n",
        )
        .await?;
    assert_eq!(response["errors"], false, "{response}");
    assert_eq!(
        harness.get_json("/bulk-other/_doc/override").await?.0,
        StatusCode::OK
    );
    assert_eq!(
        harness.get_json("/bulk-parse/_doc/override").await?.0,
        StatusCode::NOT_FOUND
    );
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
            let (status, body) = harness.get_json(&format!("/_tasks/{task_id}")).await?;
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
async fn moved_bulk_and_update_sources_preserve_values_and_receipt_order() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json(
            "/owned-sources",
            json!({"settings": {"number_of_shards": 1, "number_of_replicas": 0}}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    let original = json!({
        "body": "quoted \" slash \\ newline \n",
        "number": 9_007_199_254_740_993u64,
        "metadata": {"labels": ["a", "b"], "null": null}
    });
    let latest = json!({
        "body": "latest nested source",
        "number": 9_007_199_254_740_993u64,
        "metadata": {"labels": ["last"], "flag": true}
    });
    for endpoint in ["/owned-sources/_bulk?refresh=true", "/_bulk?refresh=true"] {
        let mut ndjson = String::new();
        for (doc_id, source) in [("same", &original), ("other", &original), ("same", &latest)] {
            ndjson.push_str(
                &json!({"index": {"_index": "owned-sources", "_id": doc_id}}).to_string(),
            );
            ndjson.push('\n');
            ndjson.push_str(&source.to_string());
            ndjson.push('\n');
        }
        let (status, body) = harness.post_ndjson(endpoint, &ndjson).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["errors"], false);
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        let first_seq_no = items[0]["index"]["_seq_no"].as_u64().unwrap();
        for (offset, doc_id) in ["same", "other", "same"].into_iter().enumerate() {
            assert_eq!(items[offset]["index"]["_id"], doc_id);
            assert_eq!(
                items[offset]["index"]["_seq_no"].as_u64(),
                Some(first_seq_no + offset as u64)
            );
        }
        let (status, body) = harness.get_json("/owned-sources/_doc/same").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["_source"], latest);
        let (_, body) = harness.get_json("/owned-sources/_doc/other").await?;
        assert_eq!(body["_source"], original);
    }
    let (status, body) = harness
        .post_json(
            "/owned-sources/_update/same",
            json!({"doc": {"metadata": {"updated": true}, "extra": "moved"}}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "updated");
    let (status, _) = harness.get_json("/owned-sources/_refresh").await?;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = harness.get_json("/owned-sources/_doc/same").await?;
    assert_eq!(status, StatusCode::OK);
    let mut expected = latest;
    expected["metadata"]["updated"] = json!(true);
    expected["extra"] = json!("moved");
    assert_eq!(body["_source"], expected);
    Ok(())
}

fn qsearch_url(base_url: &str, path: &str, params: &[(&str, &str)]) -> Result<url::Url> {
    let mut url = url::Url::parse(&format!("{base_url}{path}"))?;
    url.query_pairs_mut().extend_pairs(params.iter().copied());
    Ok(url)
}

#[tokio::test]
async fn rest_qsearch_match_all_counts_every_document_across_three_shards() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;
    for (shard_id, node) in harness.nodes.iter().enumerate() {
        let engine = node
            .app_state
            .shard_manager
            .get_shard("stories", shard_id as u32)
            .unwrap();
        let docs = (0..105)
            .map(|id| {
                (
                    format!("extra-{shard_id}-{id}"),
                    json!({"title": "extra", "author": "extra", "upvotes": id}),
                )
            })
            .collect();
        engine.bulk_add_documents(docs)?;
        engine.refresh()?;
    }

    for (query, expected) in [("*:*", 323), ("author:alice", 3), ("*", 323)] {
        let response = harness
            .client
            .get(qsearch_url(
                &harness.nodes[1].base_url,
                "/stories/_search",
                &[("q", query), ("size", "400")],
            )?)
            .send()
            .await?;
        let status = response.status();
        let body: Value = response.json().await?;
        assert_eq!(status, StatusCode::OK, "query [{query}]: {body}");
        assert_eq!(body["_shards"]["successful"], 3, "{body}");
        assert_eq!(body["_shards"]["failed"], 0, "{body}");
        assert_eq!(body["hits"]["total"]["value"], expected, "{body}");
        assert_eq!(
            body["hits"]["hits"].as_array().map(Vec::len),
            Some(expected as usize),
            "{body}"
        );

        let (status, body) = post_json_to_base_url(
            &harness.client,
            &harness.nodes[1].base_url,
            "/stories/_search",
            json!({"query": {"query_string": {"query": query}}, "size": 400}),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "query [{query}]: {body}");
        assert_eq!(body["hits"]["total"]["value"], expected, "{body}");
        assert_eq!(
            body["hits"]["hits"].as_array().map(Vec::len),
            Some(expected as usize),
            "{body}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn rest_qsearch_wildcards_distinguish_missing_null_and_empty_keyword_values() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/presence",
            json!({
                "settings": {"number_of_shards": 2, "number_of_replicas": 0},
                "mappings": {
                    "dynamic": "strict",
                    "properties": {"tag": {"type": "keyword"}, "number": {"type": "integer"}}
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = harness
        .post_ndjson(
            "/presence/_bulk?refresh=true",
            concat!(
                "{\"index\":{\"_id\":\"value\"}}\n{\"tag\":\"rust\",\"number\":0}\n",
                "{\"index\":{\"_id\":\"empty\"}}\n{\"tag\":\"\"}\n",
                "{\"index\":{\"_id\":\"null\"}}\n{\"tag\":null}\n",
                "{\"index\":{\"_id\":\"missing\"}}\n{}\n"
            ),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false, "{body}");
    for (params, expected) in [
        (
            vec![("q", "*:*")],
            vec!["empty", "missing", "null", "value"],
        ),
        (vec![("q", "*")], vec!["value"]),
        (vec![("q", "tag:*")], vec!["empty", "value"]),
        (vec![("q", "*"), ("df", "tag")], vec!["empty", "value"]),
        (vec![("q", "number:*")], vec!["value"]),
        (vec![("q", "unknown:*")], vec![]),
        (vec![], vec!["empty", "missing", "null", "value"]),
    ] {
        let response = harness
            .client
            .get(qsearch_url(
                &harness.base_url,
                "/presence/_search",
                &params,
            )?)
            .send()
            .await?;
        let status = response.status();
        let body: Value = response.json().await?;
        assert_eq!(status, StatusCode::OK, "{params:?}: {body}");
        assert_eq!(body["hits"]["total"]["value"], expected.len(), "{body}");
        let mut ids: Vec<_> = body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["_id"].as_str().unwrap())
            .collect();
        ids.sort();
        assert_eq!(ids, expected, "{params:?}: {body}");
    }
    let (status, body) = harness
        .post_json(
            "/presence/_count",
            json!({"query": {"query_string": {"query": "*", "default_field": "tag"}}}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 2, "{body}");
    Ok(())
}

fn assert_qsearch_all_parse_failures(status: StatusCode, body: &Value, query: &str, count: usize) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["status"], 400, "{body}");
    assert_eq!(
        body["error"]["type"], "search_phase_execution_exception",
        "{body}"
    );
    assert_eq!(body["error"]["reason"], "all shards failed", "{body}");
    let failures = body["error"]["failed_shards"].as_array().unwrap();
    assert_eq!(failures.len(), count, "{body}");
    for failure in failures {
        assert_eq!(failure["index"], "stories", "{body}");
        assert_eq!(failure["reason"]["type"], "query_shard_exception", "{body}");
        assert!(
            failure["reason"]["reason"]
                .as_str()
                .unwrap()
                .contains(query),
            "{body}"
        );
        assert_eq!(
            failure["reason"]["caused_by"]["type"], "parse_exception",
            "{body}"
        );
        assert!(
            failure["reason"]["caused_by"]["reason"]
                .as_str()
                .unwrap()
                .contains("Syntax"),
            "{body}"
        );
    }
    let mut shards: Vec<_> = failures
        .iter()
        .map(|failure| failure["shard"].as_u64().unwrap())
        .collect();
    shards.sort_unstable();
    assert_eq!(shards, (0..count as u64).collect::<Vec<_>>(), "{body}");
}

#[tokio::test]
async fn rest_qsearch_invalid_syntax_returns_400_with_all_shard_causes() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;
    let query = "title:(";
    let response = harness
        .client
        .get(qsearch_url(
            &harness.nodes[1].base_url,
            "/stories/_search",
            &[("q", query)],
        )?)
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await?;
    assert_qsearch_all_parse_failures(status, &body, query, 3);

    for route in ["/stories/_search", "/stories/_count"] {
        let (status, body) = post_json_to_base_url(
            &harness.client,
            &harness.nodes[1].base_url,
            route,
            json!({"query": {"query_string": {"query": query}}}),
        )
        .await?;
        assert_qsearch_all_parse_failures(status, &body, query, 3);
    }
    Ok(())
}

#[tokio::test]
async fn rest_qsearch_all_unavailable_shards_return_503_with_reasons() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;
    let mut cluster_state = harness.nodes[1].app_state.cluster_manager.get_state();
    for routing in cluster_state
        .indices
        .get_mut("stories")
        .unwrap()
        .shard_routing
        .values_mut()
    {
        routing.primary = "absent-node".to_string();
    }
    harness.nodes[1]
        .app_state
        .cluster_manager
        .update_state(cluster_state);

    for route in ["/stories/_search", "/stories/_count"] {
        let (status, body) =
            get_json_from_base_url(&harness.client, &harness.nodes[1].base_url, route).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{route}: {body}");
        assert_eq!(
            body["error"]["type"], "search_phase_execution_exception",
            "{body}"
        );
        let failures = body["error"]["failed_shards"].as_array().unwrap();
        assert_eq!(failures.len(), 3, "{body}");
        for failure in failures {
            assert_eq!(
                failure["reason"]["type"], "shard_not_available_exception",
                "{body}"
            );
            assert!(
                failure["reason"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains("absent-node"),
                "{body}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn rest_qsearch_partial_remote_failure_stays_200_and_keeps_healthy_hits() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;
    harness.nodes[2].transport_handle.abort();
    for body in [None, Some(json!({"query": {"match_all": {}}}))] {
        let (status, response) = match body {
            Some(body) => {
                post_json_to_base_url(
                    &harness.client,
                    &harness.nodes[1].base_url,
                    "/stories/_search",
                    body,
                )
                .await?
            }
            None => {
                get_json_from_base_url(
                    &harness.client,
                    &harness.nodes[1].base_url,
                    "/stories/_search",
                )
                .await?
            }
        };
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["_shards"]["total"], 3, "{response}");
        assert_eq!(response["_shards"]["successful"], 2, "{response}");
        assert_eq!(response["_shards"]["failed"], 1, "{response}");
        assert_eq!(response["hits"]["total"]["value"], 6, "{response}");
        assert_eq!(
            response["hits"]["hits"].as_array().map(Vec::len),
            Some(6),
            "{response}"
        );
        assert_eq!(
            response["_shards"]["failures"].as_array().map(Vec::len),
            Some(1),
            "{response}"
        );
        let failure = &response["_shards"]["failures"][0];
        assert_eq!(failure["shard"], 2, "{response}");
        assert_eq!(failure["index"], "stories", "{response}");
        assert_eq!(failure["node"], "node-3", "{response}");
        assert!(
            !failure["reason"]["reason"].as_str().unwrap().is_empty(),
            "{response}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn rest_qsearch_existing_match_parse_errors_are_not_success_shaped() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;
    create_distributed_stories_index_and_docs(&harness).await?;
    let (status, body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
        "/stories/_search",
        json!({"query": {"match": {"title": "("}}}),
    )
    .await?;
    assert_qsearch_all_parse_failures(status, &body, "(", 3);
    Ok(())
}

#[tokio::test]
async fn rest_qsearch_remote_store_match_all_and_parse_errors() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json("/remote-qsearch", json!({"engine": "remote_store"}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = harness
        .post_json(
            "/remote-qsearch/_remote_store/publish",
            json!({"docs": [{"_id": "text", "body": "rust"}, {"_id": "empty"}]}),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    for (query, expected_status, expected_count) in [
        ("*:*", StatusCode::OK, 2),
        ("*", StatusCode::OK, 1),
        ("body:(", StatusCode::BAD_REQUEST, 0),
    ] {
        let response = harness
            .client
            .get(qsearch_url(
                &harness.base_url,
                "/remote-qsearch/_search",
                &[("q", query)],
            )?)
            .send()
            .await?;
        let status = response.status();
        let body: Value = response.json().await?;
        assert_eq!(status, expected_status, "query [{query}]: {body}");
        if status == StatusCode::OK {
            assert_eq!(body["hits"]["total"]["value"], expected_count, "{body}");
        } else {
            assert_eq!(
                body["error"]["type"], "search_phase_execution_exception",
                "{body}"
            );
            assert_eq!(
                body["error"]["failed_shards"].as_array().map(Vec::len),
                Some(1),
                "{body}"
            );
            assert!(
                body["error"]["failed_shards"][0]["reason"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains(query),
                "{body}"
            );
        }
    }
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
async fn rest_sql_grouped_partials_populates_grouped_cache_once() -> Result<()> {
    let harness = RestTestHarness::start_with_column_cache(1024 * 1024, 0).await?;
    create_products_index_and_docs(&harness).await?;

    assert_eq!(
        harness.app_state.shard_manager.column_cache_entry_count(),
        0
    );

    let query = json!({
        "query": "SELECT brand, AVG(price) AS avg_price FROM products GROUP BY brand ORDER BY brand ASC"
    });

    let (first_status, first_body) = harness.post_json("/products/_sql", query.clone()).await?;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(
        first_body["execution_mode"],
        json!("tantivy_grouped_partials")
    );

    let cached_entries = harness.app_state.shard_manager.column_cache_entry_count();
    assert!(
        cached_entries >= 2,
        "expected grouped-partials direct scan to populate grouped cache entries"
    );

    let (second_status, second_body) = harness.post_json("/products/_sql", query).await?;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(
        second_body["execution_mode"],
        json!("tantivy_grouped_partials")
    );
    assert_eq!(second_body["rows"], first_body["rows"]);
    assert_eq!(
        harness.app_state.shard_manager.column_cache_entry_count(),
        cached_entries
    );

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
async fn rest_global_sql_stream_grouped_partials_meta_includes_grouped_merge_timings() -> Result<()>
{
    let harness = RestTestHarness::start().await?;
    create_products_index_and_docs(&harness).await?;

    let (status, body) = harness
        .post_json_text(
            "/_sql/stream",
            json!({
                "query": "SELECT brand, count(*) AS total FROM \"products\" WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY total DESC, brand ASC"
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
    assert_eq!(
        frames[0]["execution_mode"],
        json!("tantivy_grouped_partials")
    );
    assert!(frames[0]["timings"]["total_ms"].as_f64().unwrap() > 0.0);
    assert!(
        frames[0]["timings"]["grouped_merge"]["partial_merge_ms"]
            .as_f64()
            .unwrap()
            >= 0.0
    );
    assert!(
        frames[0]["timings"]["grouped_merge"]["merged_buckets"]
            .as_u64()
            .unwrap()
            >= 1
    );

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
    assert_eq!(rows[0]["total"], json!(2));
    assert_eq!(rows[1]["brand"], json!("Samsung"));
    assert_eq!(rows[1]["total"], json!(1));

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

// ── Dynamic Mapping Integration Tests ──────────────────────────────────

#[tokio::test]
async fn reserved_metadata_fields_are_rejected_across_rest_write_entries() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    for (offset, field) in RESERVED_METADATA_KEYS_FOR_TEST.iter().enumerate() {
        let (status, body) = harness
            .put_json(
                &format!("/reserved-mapping-{offset}"),
                json!({
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            (*field): { "type": "keyword" }
                        }
                    }
                }),
            )
            .await?;
        assert_mapper_parsing_error(status, &body, field);
    }

    let (status, body) = harness
        .put_json(
            "/reserved-docs",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    for (offset, field) in RESERVED_METADATA_KEYS_FOR_TEST.iter().enumerate() {
        let source = json!({ (*field): 999 });
        let (status, body) = harness
            .post_json("/reserved-docs/_doc", source.clone())
            .await?;
        assert_mapper_parsing_error(status, &body, field);

        let (status, body) = harness
            .put_json(&format!("/reserved-docs/_doc/put-{offset}"), source)
            .await?;
        assert_mapper_parsing_error(status, &body, field);
    }

    let (status, body) = harness
        .put_json("/reserved-docs/_doc/base?refresh=true", json!({"value": 1}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    for field in RESERVED_METADATA_KEYS_FOR_TEST {
        let (status, body) = harness
            .post_json(
                "/reserved-docs/_update/base",
                json!({"doc": { (*field): 999 }}),
            )
            .await?;
        assert_mapper_parsing_error(status, &body, field);

        let (status, body) = harness
            .post_json(
                "/reserved-docs/_update/base",
                json!({
                    "doc": {"value": 2},
                    "upsert": { (*field): 999 }
                }),
            )
            .await?;
        assert_mapper_parsing_error(status, &body, field);
    }

    let (status, body) = harness
        .put_json(
            "/reserved-docs/_doc/healthy?refresh=true",
            json!({"body": "allowed", "value": 2}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = harness.get_json("/reserved-docs/_doc/healthy").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["body"], json!("allowed"));

    Ok(())
}

#[tokio::test]
async fn create_index_accepts_plain_text_body_mapping_without_duplicate_schema() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/explicit-body",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "body": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = harness
        .put_json(
            "/explicit-body/_doc/1?refresh=true",
            json!({"body": "searchable value"}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let state = harness.app_state.cluster_manager.get_state();
    assert_eq!(
        state.indices["explicit-body"].mappings["body"].field_type,
        FieldType::Text
    );
    let (status, body) = harness
        .get_json("/explicit-body/_search?q=searchable")
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], json!(1), "{body}");

    Ok(())
}

#[tokio::test]
async fn create_index_rejects_non_text_or_parameterized_body_mappings() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    for (suffix, definition) in [
        ("keyword", json!({"type": "keyword"})),
        ("integer", json!({"type": "integer"})),
        (
            "parameterized",
            json!({"type": "text", "analyzer": "keyword"}),
        ),
    ] {
        let (status, body) = harness
            .put_json(
                &format!("/invalid-body-{suffix}"),
                json!({
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            "body": definition
                        }
                    }
                }),
            )
            .await?;
        assert_body_mapping_error(status, &body);
    }

    Ok(())
}

#[tokio::test]
async fn bulk_reserved_metadata_fields_are_per_item_errors() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/reserved-bulk",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let mut ndjson = String::new();
    let mut invalid_items = 0usize;
    for action in ["index", "create", "update"] {
        for field in RESERVED_METADATA_KEYS_FOR_TEST {
            let doc_id = format!("{action}-{invalid_items}");
            ndjson.push_str(&serde_json::to_string(&json!({
                (action): { "_id": doc_id }
            }))?);
            ndjson.push('\n');
            let source = if action == "update" {
                json!({"doc": { (*field): 999 }, "upsert": {"value": 0}})
            } else {
                json!({ (*field): 999 })
            };
            ndjson.push_str(&serde_json::to_string(&source)?);
            ndjson.push('\n');
            invalid_items += 1;
        }
    }
    ndjson.push_str("{\"index\":{\"_id\":\"healthy\"}}\n");
    ndjson.push_str("{\"body\":\"allowed\",\"value\":1}\n");

    let response = harness
        .client
        .post(format!(
            "{}/reserved-bulk/_bulk?refresh=true",
            harness.base_url
        ))
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(ndjson)
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], json!(true));
    let items = body["items"]
        .as_array()
        .expect("bulk items should be an array");
    assert_eq!(items.len(), invalid_items + 1);
    for item in &items[..invalid_items] {
        let result = item
            .as_object()
            .and_then(|object| object.values().next())
            .expect("bulk item should have one operation result");
        assert_eq!(result["status"], json!(400), "{result}");
        assert_eq!(
            result["error"]["type"],
            json!("mapper_parsing_exception"),
            "{result}"
        );
    }
    let healthy = items
        .last()
        .and_then(Value::as_object)
        .and_then(|object| object.values().next())
        .expect("healthy bulk item should have a result");
    assert_eq!(healthy["status"], json!(201), "{healthy}");

    let (status, body) = harness.get_json("/reserved-bulk/_doc/healthy").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["body"], json!("allowed"));

    Ok(())
}

#[tokio::test]
async fn bulk_update_wrapper_reserved_fields_are_isolated_per_item() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/reserved-bulk-update",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let bulk = concat!(
        "{\"index\":{\"_id\":\"good\"}}\n",
        "{\"value\":1}\n",
        "{\"update\":{\"_id\":\"source-option\"}}\n",
        "{\"doc\":{\"value\":2},\"_source\":true}\n",
        "{\"update\":{\"_id\":\"sequence-option\"}}\n",
        "{\"doc\":{\"value\":3},\"_seq_no\":999}\n"
    );
    let (status, body) = harness
        .post_ndjson("/reserved-bulk-update/_bulk?refresh=true", bulk)
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], json!(true), "{body}");
    assert_eq!(body["items"].as_array().map(Vec::len), Some(3), "{body}");
    assert_eq!(body["items"][0]["index"]["status"], json!(201), "{body}");
    for position in [1, 2] {
        assert_eq!(
            body["items"][position]["update"]["status"],
            json!(400),
            "{body}"
        );
        assert_eq!(
            body["items"][position]["update"]["error"]["type"],
            json!("illegal_argument_exception"),
            "{body}"
        );
    }

    let (status, body) = harness.get_json("/reserved-bulk-update/_doc/good").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"], json!({"value": 1}));

    let (status, body) = harness
        .put_json("/reserved-bulk-update/_doc/after", json!({"value": 4}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    Ok(())
}

#[tokio::test]
async fn dynamic_true_auto_creates_mappings_on_index() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Create index with dynamic: true, no explicit mappings.
    let (status, body) = harness
        .put_json(
            "/dyntest",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "dynamic": "true"
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));

    // Index a document with previously unknown fields.
    let (status, body) = harness
        .put_json(
            "/dyntest/_doc/1?refresh=true",
            json!({
                "title": "hello world",
                "count": 42,
                "price": 9.99,
                "active": true
            }),
        )
        .await?;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "expected 201, got {status}: {body}"
    );
    assert_eq!(body["_id"], json!("1"));

    // Verify cluster state has the auto-detected mappings.
    let cs = harness.app_state.cluster_manager.get_state();
    let idx = cs.indices.get("dyntest").expect("index should exist");
    assert_eq!(
        idx.dynamic,
        ferrissearch::cluster::state::DynamicMapping::True
    );
    assert_eq!(idx.mappings["title"].field_type, FieldType::Text);
    assert_eq!(idx.mappings["count"].field_type, FieldType::Integer);
    assert_eq!(idx.mappings["price"].field_type, FieldType::Float);
    assert_eq!(idx.mappings["active"].field_type, FieldType::Boolean);

    // Retrieve the document.
    let (status, body) = harness.get_json("/dyntest/_doc/1").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_source"]["title"], "hello world");
    assert_eq!(body["_source"]["count"], 42);

    // The newly mapped numeric field must be searchable immediately, without a
    // restart or unrelated shard reopen.
    let (status, body) = harness
        .post_json(
            "/dyntest/_search",
            json!({
                "query": {
                    "term": {
                        "count": 42
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hits"]["total"]["value"], json!(1));
    assert_eq!(body["hits"]["hits"][0]["_id"], json!("1"));

    Ok(())
}

#[tokio::test]
async fn dynamic_true_keeps_body_on_builtin_catch_all_across_reopen() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/dynamic-body",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "dynamic": "true"
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = harness
        .put_json("/dynamic-body/_doc/1?refresh=true", json!({"body": 42}))
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(
        !harness.app_state.cluster_manager.get_state().indices["dynamic-body"]
            .mappings
            .contains_key("body")
    );

    let (status, body) = harness
        .put_json(
            "/dynamic-body/_doc/2?refresh=true",
            json!({"body": "still searchable", "count": 2}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = harness.get_json("/dynamic-body/_refresh").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = harness.get_json("/dynamic-body/_doc/1").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["body"], json!(42));

    let state = harness.app_state.cluster_manager.get_state();
    let metadata = state.indices["dynamic-body"].clone();
    let allocation_id = state
        .primary_allocation_id("dynamic-body", 0)
        .expect("dynamic-body primary allocation should exist");
    drop(state);
    harness
        .app_state
        .shard_manager
        .reopen_shard(
            "dynamic-body".into(),
            0,
            metadata.mappings,
            metadata.settings,
            metadata.uuid.to_string(),
            allocation_id,
        )
        .await?;

    let (status, body) = harness.get_json("/dynamic-body/_doc/2").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"]["body"], json!("still searchable"));
    let (status, body) = harness.get_json("/dynamic-body/_search?q=42").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], json!(1), "{body}");

    Ok(())
}

#[tokio::test]
async fn dynamic_builtin_body_keeps_direct_sql_path_without_persisted_mapping() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let index = "dynamic-body-sql";
    let (status, body) = harness
        .put_json(
            &format!("/{index}"),
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "dynamic": true
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    for (id, source) in [
        ("1", json!({"body": "hello world", "n": 1})),
        ("2", json!({"body": "other text", "n": 2})),
    ] {
        let (status, body) = harness
            .put_json(&format!("/{index}/_doc/{id}?refresh=true"), source)
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    assert!(
        !harness.app_state.cluster_manager.get_state().indices[index]
            .mappings
            .contains_key("body")
    );

    let query = format!("SELECT body, n FROM \"{index}\" ORDER BY n");
    let (status, body) = harness
        .post_json(&format!("/{index}/_sql"), json!({"query": query.clone()}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(
        body["rows"],
        json!([
            {"body": "hello world", "n": 1},
            {"body": "other text", "n": 2}
        ])
    );

    let (status, stream) = harness
        .post_json_text(&format!("/{index}/_sql/stream"), json!({"query": query}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{stream}");
    let frames: Vec<Value> = stream
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(frames[0]["execution_mode"], json!("tantivy_fast_fields"));
    let streamed_rows: Vec<Value> = frames
        .iter()
        .skip(1)
        .flat_map(|frame| frame["rows"].as_array().cloned().unwrap_or_default())
        .collect();
    assert_eq!(streamed_rows, body["rows"].as_array().unwrap().clone());

    let (status, describe) = harness
        .post_json("/_sql", json!({"query": format!("DESCRIBE \"{index}\"")}))
        .await?;
    assert_eq!(status, StatusCode::OK, "{describe}");
    assert!(
        describe["rows"]
            .as_array()
            .unwrap()
            .contains(&json!({"field": "body", "type": "text"}))
    );

    Ok(())
}

#[tokio::test]
async fn dynamic_false_index_does_not_add_mappings() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Create index with dynamic: false (default), no explicit field mappings.
    let (status, _) = harness
        .put_json(
            "/statictest",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    // Index a document — fields should go into the "body" catch-all.
    let (status, _) = harness
        .put_json(
            "/statictest/_doc/1?refresh=true",
            json!({"title": "hello", "count": 42}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED);

    // Cluster state should have NO auto-detected mappings.
    let cs = harness.app_state.cluster_manager.get_state();
    let idx = cs.indices.get("statictest").expect("index should exist");
    assert!(
        idx.mappings.is_empty(),
        "dynamic=false should not auto-create mappings, got: {:?}",
        idx.mappings
    );

    Ok(())
}

#[tokio::test]
async fn dynamic_strict_rejects_unknown_fields() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Create index with dynamic: strict + one known field.
    let (status, body) = harness
        .put_json(
            "/stricttest",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "title": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], json!(true));

    // Index a document with a known field — should succeed.
    let (status, _) = harness
        .put_json(
            "/stricttest/_doc/ok?refresh=true",
            json!({"title": "hello"}),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED);

    // Index a document with an unknown field — should be rejected.
    let (status, body) = harness
        .put_json(
            "/stricttest/_doc/bad",
            json!({"title": "hello", "unknown_field": 123}),
        )
        .await?;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "strict mode should reject unknown fields, got status {status}"
    );
    let error_msg = body.to_string();
    assert!(
        error_msg.contains("unknown_field") || error_msg.contains("strict"),
        "error should mention the unknown field or strict mode, got: {error_msg}"
    );

    // Arrays/objects are not inferable, but strict mode must still reject them
    // as unknown top-level fields.
    let (status, body) = harness
        .put_json(
            "/stricttest/_doc/nested",
            json!({"title": "hello", "metadata": {"nested": true}, "tags": ["x"]}),
        )
        .await?;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "strict mode should reject unknown object/array fields, got status {status}"
    );
    let error_msg = body.to_string();
    assert!(
        error_msg.contains("metadata") || error_msg.contains("tags"),
        "error should mention unknown nested fields, got: {error_msg}"
    );

    Ok(())
}

#[tokio::test]
async fn dynamic_true_bulk_index_creates_mappings() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Create index with dynamic: true.
    let (status, _) = harness
        .put_json(
            "/bulkdyn",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval_ms": 100
                },
                "mappings": {
                    "dynamic": "true"
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    // Bulk index with NDJSON.
    let ndjson = r#"{"index":{"_id":"b1"}}
{"name":"Alice","age":30}
{"index":{"_id":"b2"}}
{"name":"Bob","age":25,"score":9.5}
"#;
    let response = harness
        .client
        .post(format!("{}/bulkdyn/_bulk?refresh=true", harness.base_url))
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(ndjson.to_string())
        .send()
        .await?;
    assert!(response.status().is_success());

    // Verify mappings were auto-detected.
    let cs = harness.app_state.cluster_manager.get_state();
    let idx = cs.indices.get("bulkdyn").expect("index should exist");
    assert_eq!(idx.mappings["name"].field_type, FieldType::Text);
    assert_eq!(idx.mappings["age"].field_type, FieldType::Integer);
    assert_eq!(idx.mappings["score"].field_type, FieldType::Float);

    Ok(())
}

#[tokio::test]
async fn get_settings_exposes_dynamic_field() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Create index with dynamic: true.
    let (status, _) = harness
        .put_json(
            "/settingstest",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "dynamic": "true"
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = harness.get_json("/settingstest/_settings").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["settingstest"]["settings"]["index"]["dynamic"],
        json!("true"),
        "GET _settings should expose the dynamic field"
    );

    Ok(())
}

#[tokio::test]
async fn create_index_with_remote_store_engine_allows_create_but_rejects_writes() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // Creation of a remote_store index is allowed — it is a shardless index
    // whose read path fetches splits from the configured object store.
    let (status, _body) = harness
        .put_json(
            "/remoteidx",
            json!({
                "engine": "remote_store"
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(harness.head_status("/remoteidx").await?, StatusCode::OK);

    // Writes are not yet implemented for remote_store — must be rejected with 501.
    let (write_status, write_body) = harness
        .post_json("/remoteidx/_doc", json!({ "title": "should-be-rejected" }))
        .await?;
    assert_eq!(write_status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        write_body["error"]["type"],
        json!("illegal_argument_exception")
    );
    assert!(
        write_body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("remote_store")
    );

    Ok(())
}

#[tokio::test]
async fn create_index_rejects_settings_engine_and_names_top_level_field() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/nested-engine",
            json!({
                "settings": {
                    "engine": "remote_store",
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));
    assert_eq!(
        body["error"]["reason"],
        json!(
            "index engine must be specified in the top-level [engine] field, not [settings.engine]"
        )
    );
    assert!(
        !harness
            .app_state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("nested-engine")
    );

    Ok(())
}

#[tokio::test]
async fn create_index_with_remote_store_engine_when_forwarded_to_leader() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;

    let (status, _body) = put_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
        "/remoteidx-forwarded",
        json!({
            "engine": "remote_store"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);

    // The test harness uses isolated Raft instances (not actually replicated),
    // so only the leader (node-1) sees the applied CreateIndex. That is enough
    // to prove the follower successfully forwarded to the leader.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let leader_status = harness
            .client
            .head(format!("{}/remoteidx-forwarded", harness.nodes[0].base_url))
            .send()
            .await?
            .status();
        if leader_status == StatusCode::OK {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("remote_store index did not appear on leader: {leader_status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Writes are rejected at whichever node processes them \u2014 send to the leader
    // directly so we do not depend on Raft replication semantics in this harness.
    let (write_status, write_body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[0].base_url,
        "/remoteidx-forwarded/_doc",
        json!({ "title": "should-be-rejected" }),
    )
    .await?;
    assert_eq!(write_status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        write_body["error"]["type"],
        json!("illegal_argument_exception")
    );

    Ok(())
}

#[tokio::test]
async fn create_index_returns_no_data_nodes_exception_on_master_only_node() -> Result<()> {
    let harness = RestTestHarness::start_with_roles(vec![NodeRole::Master]).await?;

    let (status, body) = harness
        .put_json(
            "/nodataidx",
            json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }),
        )
        .await?;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"]["type"], json!("no_data_nodes_exception"));
    assert_eq!(
        body["error"]["reason"],
        json!("No data nodes available to assign shards")
    );
    assert_eq!(
        harness.head_status("/nodataidx").await?,
        StatusCode::NOT_FOUND
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_search_returns_hits_from_published_split() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    // 1. Create the remote_store index.
    let (status, _body) = harness
        .put_json(
            "/remotehits",
            json!({
                "engine": "remote_store"
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    // 2. Publish one split via the new publish API.
    let (publish_status, publish_body) = harness
        .post_json(
            "/remotehits/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-1", "title": "remote store hit", "body": "first published split" }
                ]
            }),
        )
        .await?;
    assert_eq!(
        publish_status,
        StatusCode::OK,
        "publish body: {publish_body}"
    );
    assert_eq!(publish_body["generation"], json!(1));
    assert_eq!(publish_body["doc_count"], json!(1));
    assert!(
        publish_body["split_id"].as_str().is_some(),
        "split_id must be a string: {publish_body}"
    );

    // 3. POST /_search and assert the published doc comes back.
    let (search_status, search_body) = harness
        .post_json(
            "/remotehits/_search",
            json!({ "query": { "match_all": {} } }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK);
    assert_eq!(search_body["_shards"]["successful"], json!(1));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert!(
        search_body["hits"]["total"]["value"].as_u64().unwrap_or(0) >= 1,
        "expected at least one hit, got: {search_body}"
    );
    let hits = search_body["hits"]["hits"]
        .as_array()
        .expect("hits.hits must be an array");
    assert!(!hits.is_empty());
    assert_eq!(hits[0]["_id"], json!("doc-1"));

    Ok(())
}

#[tokio::test]
async fn remote_store_query_string_search_returns_hits_from_published_split() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, _body) = harness
        .put_json(
            "/remotequery",
            json!({
                "engine": "remote_store"
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (publish_status, publish_body) = harness
        .post_json(
            "/remotequery/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-1", "title": "warm remote store hit", "body": "first published split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let (search_status, search_body) = harness.get_json("/remotequery/_search?q=warm").await?;
    assert_eq!(search_status, StatusCode::OK);
    assert_eq!(search_body["_shards"]["successful"], json!(1));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert_eq!(search_body["hits"]["total"]["value"], json!(1));
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("doc-1"));
    assert_eq!(
        search_body["remote_store"]["pruning"],
        json!({
            "published_splits": 1,
            "candidate_splits": 1,
            "pruned_splits": 0,
            "assigned_splits": 1
        })
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_count_match_all_uses_manifest_doc_counts() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, _body) = harness
        .put_json(
            "/remotecount",
            json!({
                "engine": "remote_store"
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (publish_status, publish_body) = harness
        .post_json(
            "/remotecount/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-1", "title": "warm remote store hit", "body": "first published split" },
                    { "_id": "doc-2", "title": "second remote hit", "body": "same split second doc" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let (count_status, count_body) = harness
        .post_json(
            "/remotecount/_count",
            json!({
                "query": { "match_all": {} }
            }),
        )
        .await?;
    assert_eq!(count_status, StatusCode::OK);
    assert_eq!(count_body["count"], json!(2));
    assert_eq!(count_body["_shards"]["successful"], json!(1));
    assert_eq!(count_body["_shards"]["failed"], json!(0));

    Ok(())
}

#[tokio::test]
async fn remote_store_publish_populates_split_pruning_summaries() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remotesummary",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "status": { "type": "keyword" },
                        "count": { "type": "integer" },
                        "price": { "type": "float" },
                        "ts": { "type": "date" },
                        "title": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remotesummary/_remote_store/publish",
            json!({
                "docs": [
                    {
                        "_id": "doc-1",
                        "status": "error",
                        "count": 5,
                        "price": 1.5,
                        "ts": "2026-04-01T00:00:00Z",
                        "title": "first"
                    },
                    {
                        "_id": "doc-2",
                        "status": "warn",
                        "count": 9,
                        "price": 2.5,
                        "ts": "2026-04-02T00:00:00Z",
                        "title": "second"
                    }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remotesummary")
        .cloned()
        .expect("index metadata should exist");
    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?
        .expect("manifest should exist after publish");
    let split = manifest
        .published_splits()
        .next()
        .expect("publish should create one split");

    assert_eq!(split.field_terms["status"].values, vec!["error", "warn"]);
    assert_eq!(split.field_ranges["count"].min, "5");
    assert_eq!(split.field_ranges["count"].max, "9");
    assert_eq!(split.field_ranges["price"].min, "1.5");
    assert_eq!(split.field_ranges["price"].max, "2.5");
    assert_eq!(
        split.field_ranges["ts"].min,
        ferrissearch::common::date::parse_iso8601_to_epoch_millis("2026-04-01T00:00:00Z")
            .unwrap()
            .to_string()
    );
    assert_eq!(
        split.field_ranges["ts"].max,
        ferrissearch::common::date::parse_iso8601_to_epoch_millis("2026-04-02T00:00:00Z")
            .unwrap()
            .to_string()
    );
    assert!(!split.field_terms.contains_key("title"));

    Ok(())
}

#[tokio::test]
async fn remote_store_term_filter_prunes_unmatched_split_before_cache_fetch() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remotepruneterm",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "status": { "type": "keyword" },
                        "title": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remotepruneterm/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-error", "status": "error", "title": "matching split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let (publish_status, publish_body) = harness
        .post_json(
            "/remotepruneterm/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-ok", "status": "ok", "title": "pruned split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remotepruneterm")
        .cloned()
        .expect("index metadata should exist");
    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?
        .expect("manifest should exist after publish");
    let splits: Vec<_> = manifest.published_splits().collect();
    assert_eq!(splits.len(), 2);
    let error_split = splits
        .iter()
        .copied()
        .find(|split| split.field_terms["status"].values == vec!["error"])
        .expect("error split should have a status summary");
    let ok_split = splits
        .iter()
        .copied()
        .find(|split| split.field_terms["status"].values == vec!["ok"])
        .expect("ok split should have a status summary");

    let (search_status, search_body) = harness
        .post_json(
            "/remotepruneterm/_search",
            json!({
                "query": { "term": { "status": "error" } }
            }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK, "{search_body}");
    assert_eq!(search_body["_shards"]["successful"], json!(1));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert_eq!(search_body["hits"]["total"]["value"], json!(1));
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("doc-error"));
    assert_eq!(
        search_body["remote_store"]["pruning"],
        json!({
            "published_splits": 2,
            "candidate_splits": 1,
            "pruned_splits": 1,
            "assigned_splits": 1
        })
    );

    assert!(
        harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &error_split.split_id,
                &error_split.checksum,
            )
            .artifact_cached
    );
    assert!(
        !harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &ok_split.split_id,
                &ok_split.checksum
            )
            .artifact_cached,
        "pruned split should not be fetched into the node-local cache"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_keyword_arrays_are_summarized_without_false_negative_pruning() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remoteprunekeywordarray",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "tags": { "type": "keyword" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunekeywordarray/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "scalar", "tags": "x" },
                    {
                        "_id": "array",
                        "tags": [["y", "z", null], 7, true, ["y", false, 7]]
                    }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let matching_split_id = publish_body["split_id"]
        .as_str()
        .expect("matching split id")
        .to_string();

    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunekeywordarray/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "other", "tags": "other" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let pruned_split_id = publish_body["split_id"]
        .as_str()
        .expect("pruned split id")
        .to_string();

    for value in [json!("y"), json!(7), json!(true), json!(false)] {
        let (search_status, search_body) = harness
            .post_json(
                "/remoteprunekeywordarray/_search",
                json!({
                    "query": { "term": { "tags": value } }
                }),
            )
            .await?;
        assert_eq!(search_status, StatusCode::OK, "{search_body}");
        assert_eq!(
            search_body["hits"]["total"]["value"],
            json!(1),
            "{search_body}"
        );
        assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("array"));
        assert_eq!(
            search_body["remote_store"]["pruning"],
            json!({
                "published_splits": 2,
                "candidate_splits": 1,
                "pruned_splits": 1,
                "assigned_splits": 1
            })
        );
    }

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remoteprunekeywordarray")
        .cloned()
        .expect("index metadata should exist");
    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?
        .expect("manifest should exist after publish");
    let matching_split = manifest
        .published_splits()
        .find(|split| split.split_id == matching_split_id)
        .expect("matching split should exist");
    let pruned_split = manifest
        .published_splits()
        .find(|split| split.split_id == pruned_split_id)
        .expect("pruned split should exist");

    assert_eq!(
        matching_split.field_terms["tags"].values,
        vec!["7", "false", "true", "x", "y", "z"]
    );
    assert_eq!(pruned_split.field_terms["tags"].values, vec!["other"]);
    assert!(
        harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &matching_split.split_id,
                &matching_split.checksum,
            )
            .artifact_cached
    );
    assert!(
        !harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &pruned_split.split_id,
                &pruned_split.checksum,
            )
            .artifact_cached,
        "nonmatching split should be pruned before cache fetch"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_capped_keyword_array_summary_keeps_the_split_conservatively() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remoteprunekeywordcap",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "tags": { "type": "keyword" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let array_values: Vec<Value> = (0..=64).map(|idx| json!(format!("value-{idx}"))).collect();
    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunekeywordcap/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "anchor", "tags": "anchor" },
                    { "_id": "capped", "tags": array_values }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let capped_split_id = publish_body["split_id"]
        .as_str()
        .expect("capped split id")
        .to_string();

    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunekeywordcap/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "other", "tags": "other" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let pruned_split_id = publish_body["split_id"]
        .as_str()
        .expect("pruned split id")
        .to_string();

    let (search_status, search_body) = harness
        .post_json(
            "/remoteprunekeywordcap/_search",
            json!({
                "query": { "term": { "tags": "value-64" } }
            }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK, "{search_body}");
    assert_eq!(
        search_body["hits"]["total"]["value"],
        json!(1),
        "{search_body}"
    );
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("capped"));
    assert_eq!(
        search_body["remote_store"]["pruning"],
        json!({
            "published_splits": 2,
            "candidate_splits": 1,
            "pruned_splits": 1,
            "assigned_splits": 1
        })
    );

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remoteprunekeywordcap")
        .cloned()
        .expect("index metadata should exist");
    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?
        .expect("manifest should exist after publish");
    let capped_split = manifest
        .published_splits()
        .find(|split| split.split_id == capped_split_id)
        .expect("capped split should exist");
    let pruned_split = manifest
        .published_splits()
        .find(|split| split.split_id == pruned_split_id)
        .expect("pruned split should exist");

    assert!(
        !capped_split.field_terms.contains_key("tags"),
        "an incomplete capped summary must be omitted"
    );
    assert_eq!(pruned_split.field_terms["tags"].values, vec!["other"]);
    assert!(
        harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &capped_split.split_id,
                &capped_split.checksum,
            )
            .artifact_cached
    );
    assert!(
        !harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &pruned_split.split_id,
                &pruned_split.checksum,
            )
            .artifact_cached,
        "the exact nonmatching split should still be pruned"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_range_filter_prunes_unmatched_split_before_cache_fetch() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remoteprunerange",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "count": { "type": "integer" },
                        "title": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunerange/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-low", "count": 5, "title": "pruned split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunerange/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-high", "count": 200, "title": "matching split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remoteprunerange")
        .cloned()
        .expect("index metadata should exist");
    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?
        .expect("manifest should exist after publish");
    let splits: Vec<_> = manifest.published_splits().collect();
    assert_eq!(splits.len(), 2);
    let low_split = splits
        .iter()
        .copied()
        .find(|split| split.field_ranges["count"].max == "5")
        .expect("low split should have a count range summary");
    let high_split = splits
        .iter()
        .copied()
        .find(|split| split.field_ranges["count"].min == "200")
        .expect("high split should have a count range summary");

    let (search_status, search_body) = harness
        .post_json(
            "/remoteprunerange/_search",
            json!({
                "query": { "range": { "count": { "gte": 100 } } }
            }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK, "{search_body}");
    assert_eq!(search_body["_shards"]["successful"], json!(1));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert_eq!(search_body["hits"]["total"]["value"], json!(1));
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("doc-high"));
    assert_eq!(
        search_body["remote_store"]["pruning"],
        json!({
            "published_splits": 2,
            "candidate_splits": 1,
            "pruned_splits": 1,
            "assigned_splits": 1
        })
    );

    assert!(
        harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &high_split.split_id,
                &high_split.checksum,
            )
            .artifact_cached
    );
    assert!(
        !harness
            .app_state
            .storage_manager
            .cached_split_status(
                metadata.uuid.as_str(),
                &low_split.split_id,
                &low_split.checksum
            )
            .artifact_cached,
        "pruned split should not be fetched into the node-local cache"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_unsupported_match_query_reports_no_split_pruning() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remoteprunematch",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "title": { "type": "text" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunematch/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-match", "title": "matching split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let (publish_status, publish_body) = harness
        .post_json(
            "/remoteprunematch/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-other", "title": "other split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let (search_status, search_body) = harness
        .post_json(
            "/remoteprunematch/_search",
            json!({
                "query": { "match": { "title": "matching" } }
            }),
        )
        .await?;
    assert_eq!(search_status, StatusCode::OK, "{search_body}");
    assert_eq!(search_body["_shards"]["successful"], json!(2));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert_eq!(search_body["hits"]["total"]["value"], json!(1));
    assert_eq!(search_body["hits"]["hits"][0]["_id"], json!("doc-match"));
    assert_eq!(
        search_body["remote_store"]["pruning"],
        json!({
            "published_splits": 2,
            "candidate_splits": 2,
            "pruned_splits": 0,
            "assigned_splits": 2
        })
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_sql_explain_analyze_reports_pruning_counters() -> Result<()> {
    let harness = RestTestHarness::start().await?;

    let (status, body) = harness
        .put_json(
            "/remotesqlexplain",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "status": { "type": "keyword" },
                        "title": { "type": "keyword" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (publish_status, publish_body) = harness
        .post_json(
            "/remotesqlexplain/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-error", "status": "error", "title": "matching split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");
    let (publish_status, publish_body) = harness
        .post_json(
            "/remotesqlexplain/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "doc-ok", "status": "ok", "title": "pruned split" }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let (explain_status, explain_body) = harness
        .post_json(
            "/remotesqlexplain/_sql/explain",
            json!({
                "query": "SELECT title FROM remotesqlexplain WHERE status = 'error'",
                "analyze": true
            }),
        )
        .await?;
    assert_eq!(explain_status, StatusCode::OK, "{explain_body}");
    assert_eq!(
        explain_body["execution_mode"],
        json!("materialized_hits_fallback")
    );
    assert_eq!(explain_body["matched_hits"], json!(1));
    assert_eq!(explain_body["row_count"], json!(1));
    assert_eq!(explain_body["rows"][0]["title"], json!("matching split"));
    assert!(explain_body["timings"]["total_ms"].as_f64().unwrap() > 0.0);
    assert_eq!(
        explain_body["remote_store"]["pruning"],
        json!({
            "published_splits": 2,
            "candidate_splits": 1,
            "pruned_splits": 1,
            "assigned_splits": 1
        })
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_search_fans_out_from_master_only_coordinator() -> Result<()> {
    let harness =
        MultiNodeRestHarness::start_three_nodes_with_shared_remote_store(vec![NodeRole::Master])
            .await?;

    let coordinator = &harness.nodes[0];
    let (status, _body) = put_json_to_base_url(
        &harness.client,
        &coordinator.base_url,
        "/remotedist",
        json!({ "engine": "remote_store" }),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let metadata = coordinator
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("remotedist")
        .cloned()
        .expect("leader should hold remotedist metadata");
    // This harness intentionally uses isolated in-memory Raft instances rather
    // than a fully replicated cluster, so seed follower metadata explicitly.
    // The leaf-side metadata dependency itself is covered directly in the
    // transport regression `search_remote_store_splits_requires_local_index_metadata`.
    for node in harness.nodes.iter().skip(1) {
        let mut cluster_state = node.app_state.cluster_manager.get_state();
        cluster_state.add_index(metadata.clone());
        node.app_state.cluster_manager.update_state(cluster_state);
    }

    let (publish_status, publish_body) = post_json_to_base_url(
        &harness.client,
        &coordinator.base_url,
        "/remotedist/_remote_store/publish",
        json!({
            "docs": [
                { "_id": "doc-1", "title": "distributed remote hit", "body": "leaf fanout" }
            ]
        }),
    )
    .await?;
    assert_eq!(publish_status, StatusCode::OK, "{publish_body}");

    let manifest = coordinator
        .app_state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), None)
        .await?
        .expect("manifest should exist after publish");
    let split = manifest
        .published_splits()
        .next()
        .cloned()
        .expect("publish should create one published split");

    let (search_status, search_body) = post_json_to_base_url(
        &harness.client,
        &coordinator.base_url,
        "/remotedist/_search",
        json!({ "query": { "match_all": {} } }),
    )
    .await?;
    assert_eq!(search_status, StatusCode::OK, "{search_body}");
    assert_eq!(search_body["_shards"]["successful"], json!(1));
    assert_eq!(search_body["_shards"]["failed"], json!(0));
    assert_eq!(
        search_body["hits"]["hits"][0]["_id"],
        json!("doc-1"),
        "unexpected search response: {search_body}"
    );

    assert!(
        !coordinator
            .app_state
            .storage_manager
            .cached_split_status(metadata.uuid.as_str(), &split.split_id, &split.checksum)
            .artifact_cached,
        "master-only coordinator should not execute remote_store split locally"
    );
    assert!(
        harness.nodes[1..].iter().any(|node| {
            node.app_state
                .storage_manager
                .cached_split_status(metadata.uuid.as_str(), &split.split_id, &split.checksum)
                .artifact_cached
        }),
        "at least one data leaf should fetch the split into its local cache"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_publish_rejects_empty_docs_array() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/pubempty", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (publish_status, publish_body) = harness
        .post_json("/pubempty/_remote_store/publish", json!({ "docs": [] }))
        .await?;
    assert_eq!(publish_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        publish_body["error"]["type"],
        json!("illegal_argument_exception")
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_publish_rejects_keyword_objects_without_publishing() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json(
            "/pubinvalidkeyword",
            json!({
                "engine": "remote_store",
                "mappings": {
                    "properties": {
                        "tags": { "type": "keyword" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("pubinvalidkeyword")
        .cloned()
        .expect("index metadata should exist");
    let (publish_status, publish_body) = harness
        .post_json(
            "/pubinvalidkeyword/_remote_store/publish",
            json!({
                "docs": [
                    { "_id": "valid", "tags": ["ok"] },
                    { "_id": "invalid", "tags": ["ok", { "nested": "invalid" }] }
                ]
            }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::BAD_REQUEST, "{publish_body}");
    assert_eq!(
        publish_body["error"]["type"],
        json!("mapper_parsing_exception")
    );
    assert!(
        publish_body["error"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("field [tags]")),
        "expected field-specific validation error, got {publish_body}"
    );

    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?;
    assert!(
        manifest.is_none(),
        "invalid publication must not publish a manifest"
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_publish_rejects_reserved_source_without_publishing() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .put_json("/pubreserved", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    let metadata = harness
        .app_state
        .cluster_manager
        .get_state()
        .indices
        .get("pubreserved")
        .cloned()
        .expect("index metadata should exist");
    for field in RESERVED_METADATA_KEYS_FOR_TEST
        .iter()
        .copied()
        .filter(|field| *field != "_id")
    {
        let (status, body) = harness
            .post_json(
                "/pubreserved/_remote_store/publish",
                json!({
                    "docs": [
                        { "_id": "valid", "title": "allowed request metadata" },
                        { "_id": "invalid", (field): 7, "title": "reserved source metadata" }
                    ]
                }),
            )
            .await?;
        assert_mapper_parsing_error(status, &body, field);
    }

    let manifest = harness
        .app_state
        .storage_manager
        .load_current_manifest(
            metadata.uuid.as_str(),
            Some(&ferrissearch::storage::compute_schema_hash(
                &metadata.mappings,
            )),
        )
        .await?;
    assert!(
        manifest.is_none(),
        "reserved source metadata must not publish a manifest"
    );

    Ok(())
}

#[tokio::test]
async fn remote_store_publish_rejects_on_local_shards_engine() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/pubwrong", json!({ "engine": "local_shards" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (publish_status, publish_body) = harness
        .post_json(
            "/pubwrong/_remote_store/publish",
            json!({ "docs": [ { "_id": "a" } ] }),
        )
        .await?;
    assert_eq!(publish_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        publish_body["error"]["type"],
        json!("illegal_argument_exception")
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_publish_appends_across_generations() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/pubgen", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (s1, b1) = harness
        .post_json(
            "/pubgen/_remote_store/publish",
            json!({ "docs": [ { "_id": "d1", "title": "one" } ] }),
        )
        .await?;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    assert_eq!(b1["generation"], json!(1));

    let (s2, b2) = harness
        .post_json(
            "/pubgen/_remote_store/publish",
            json!({ "docs": [ { "_id": "d2", "title": "two" } ] }),
        )
        .await?;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    assert_eq!(b2["generation"], json!(2));

    // Search should see both published splits.
    let (ss, sb) = harness
        .post_json("/pubgen/_search", json!({ "query": { "match_all": {} } }))
        .await?;
    assert_eq!(ss, StatusCode::OK);
    assert_eq!(sb["_shards"]["successful"], json!(2));
    assert!(
        sb["hits"]["total"]["value"].as_u64().unwrap_or(0) >= 2,
        "expected >=2 hits across two splits, got {sb}"
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_publish_survives_orphan_cleanup() -> Result<()> {
    // Regression: the node startup orphan scanner walks `<data_dir>/*` and
    // deletes any top-level directory whose name is not in the known-UUIDs
    // set. The remote_store root lives at `<data_dir>/_remote_store/` and is
    // NOT a per-index UUID dir, so treating it as an orphan would wipe every
    // published split on every restart. This test pins the contract that the
    // scanner must preserve `_remote_store/`.
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/pubrestart", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    // Publish two generations so we have real on-disk state under _remote_store/.
    let (s1, b1) = harness
        .post_json(
            "/pubrestart/_remote_store/publish",
            json!({ "docs": [ { "_id": "r1", "title": "alpha" } ] }),
        )
        .await?;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    let (s2, b2) = harness
        .post_json(
            "/pubrestart/_remote_store/publish",
            json!({ "docs": [ { "_id": "r2", "title": "beta" } ] }),
        )
        .await?;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    assert_eq!(b2["generation"], json!(2));

    // Sanity: search sees both docs before cleanup.
    let (ss, sb) = harness
        .post_json(
            "/pubrestart/_search",
            json!({ "query": { "match_all": {} } }),
        )
        .await?;
    assert_eq!(ss, StatusCode::OK);
    assert_eq!(sb["_shards"]["successful"], json!(2));
    assert!(sb["hits"]["total"]["value"].as_u64().unwrap_or(0) >= 2);

    // Simulate the restart-path orphan scan: empty known_uuids means every
    // top-level dir is "unknown" except ones the scanner explicitly reserves.
    // Before the fix, this would have blown away `_remote_store/`.
    harness
        .app_state
        .shard_manager
        .cleanup_orphaned_data(&std::collections::HashSet::new());

    // The remote_store root and its published content must still exist.
    let remote_store_root = harness
        ._temp_dir
        .path()
        .join(ferrissearch::storage::REMOTE_STORE_DIR_NAME);
    assert!(
        remote_store_root.exists(),
        "_remote_store dir must survive orphan cleanup"
    );

    // Search must still succeed and return the published docs.
    let (ss2, sb2) = harness
        .post_json(
            "/pubrestart/_search",
            json!({ "query": { "match_all": {} } }),
        )
        .await?;
    assert_eq!(ss2, StatusCode::OK);
    assert_eq!(sb2["_shards"]["successful"], json!(2));
    assert_eq!(sb2["_shards"]["failed"], json!(0));
    assert!(
        sb2["hits"]["total"]["value"].as_u64().unwrap_or(0) >= 2,
        "expected >=2 hits after orphan-cleanup sweep, got {sb2}"
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_verify_reports_ok_for_untouched_splits() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/vok", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    for i in 0..3 {
        let (ps, pb) = harness
            .post_json(
                "/vok/_remote_store/publish",
                json!({ "docs": [ { "_id": format!("d{}", i), "title": format!("doc {}", i) } ] }),
            )
            .await?;
        assert_eq!(ps, StatusCode::OK, "{pb}");
    }

    let (vs, vb) = harness
        .post_json("/vok/_remote_store/verify", json!({}))
        .await?;
    assert_eq!(vs, StatusCode::OK, "{vb}");
    assert_eq!(vb["index"], json!("vok"));
    assert_eq!(vb["generation"], json!(3));
    assert_eq!(vb["ok_count"], json!(3));
    assert_eq!(vb["mismatch_count"], json!(0));
    assert_eq!(vb["missing_count"], json!(0));
    assert_eq!(vb["unsupported_count"], json!(0));
    let splits = vb["splits"].as_array().expect("splits array");
    assert_eq!(splits.len(), 3);
    for s in splits {
        assert_eq!(s["status"], json!("ok"), "split: {s}");
    }
    Ok(())
}

#[tokio::test]
async fn remote_store_verify_reports_mismatch_after_file_tamper() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/vbad", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (ps, pb) = harness
        .post_json(
            "/vbad/_remote_store/publish",
            json!({ "docs": [ { "_id": "x", "title": "original" } ] }),
        )
        .await?;
    assert_eq!(ps, StatusCode::OK, "{pb}");
    let split_id = pb["split_id"]
        .as_str()
        .expect("split_id string")
        .to_string();

    // Corrupt the published bundle file so the sha256 computed during verify
    // no longer matches the value stored in the manifest. With the new
    // bundle layout, each split is a single opaque file at
    // `<storage_root>/<uuid>/splits/<split_id>/bundle`. We flip a byte near
    // the tail of the file so the framing header is untouched and the
    // download still succeeds.
    let remote_store_root = harness
        ._temp_dir
        .path()
        .join(ferrissearch::storage::REMOTE_STORE_DIR_NAME);
    let mut victim: Option<std::path::PathBuf> = None;
    for uuid_entry in std::fs::read_dir(&remote_store_root)? {
        let uuid_path = uuid_entry?.path();
        let candidate = uuid_path.join("splits").join(&split_id).join("bundle");
        if candidate.exists() {
            victim = Some(candidate);
            break;
        }
    }
    let victim = victim.expect("split bundle file must exist after publish");
    let mut bytes = std::fs::read(&victim)?;
    assert!(!bytes.is_empty(), "bundle file must not be empty");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&victim, &bytes)?;

    let (vs, vb) = harness
        .post_json("/vbad/_remote_store/verify", json!({}))
        .await?;
    assert_eq!(vs, StatusCode::OK, "{vb}");
    assert_eq!(vb["ok_count"], json!(0));
    assert_eq!(vb["mismatch_count"], json!(1));
    let splits = vb["splits"].as_array().expect("splits array");
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0]["split_id"], json!(split_id));
    assert_eq!(splits[0]["status"], json!("mismatch"));
    let expected = splits[0]["expected"].as_str().unwrap();
    let actual = splits[0]["actual"].as_str().unwrap();
    assert!(expected.starts_with("sha256:"));
    assert!(actual.starts_with("sha256:"));
    assert_ne!(expected, actual);
    Ok(())
}

#[tokio::test]
async fn remote_store_verify_rejects_non_remote_store_engine() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/vlocal", json!({ "engine": "local_shards" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (vs, vb) = harness
        .post_json("/vlocal/_remote_store/verify", json!({}))
        .await?;
    assert_eq!(vs, StatusCode::BAD_REQUEST);
    assert_eq!(
        vb["error"]["type"],
        json!("illegal_argument_exception"),
        "{vb}"
    );
    Ok(())
}

#[tokio::test]
async fn remote_store_verify_missing_index_returns_404() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (vs, vb) = harness
        .post_json("/nope/_remote_store/verify", json!({}))
        .await?;
    assert_eq!(vs, StatusCode::NOT_FOUND);
    assert_eq!(vb["error"]["type"], json!("index_not_found_exception"));
    Ok(())
}

#[tokio::test]
async fn remote_store_verify_empty_index_returns_zero_counts() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let (status, _) = harness
        .put_json("/vempty", json!({ "engine": "remote_store" }))
        .await?;
    assert_eq!(status, StatusCode::OK);

    let (vs, vb) = harness
        .post_json("/vempty/_remote_store/verify", json!({}))
        .await?;
    assert_eq!(vs, StatusCode::OK, "{vb}");
    assert_eq!(vb["generation"], json!(0));
    assert_eq!(vb["ok_count"], json!(0));
    assert_eq!(vb["mismatch_count"], json!(0));
    assert_eq!(vb["missing_count"], json!(0));
    assert_eq!(vb["unsupported_count"], json!(0));
    assert_eq!(vb["splits"].as_array().unwrap().len(), 0);
    Ok(())
}

async fn create_search_after_index_and_docs(
    harness: &RestTestHarness,
    index_name: &str,
    doc_count: usize,
) -> Result<()> {
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
                        "n": { "type": "integer" }
                    }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "create index: {body}");

    for i in 0..doc_count {
        let doc_id = format!("d{i:03}");
        let (status, body) = harness
            .put_json(
                &format!("/{index_name}/_doc/{doc_id}?refresh=true"),
                json!({ "n": i as i64 }),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "index {doc_id}: {body}");
    }
    Ok(())
}

#[tokio::test]
async fn rest_search_after_paginates_integer_ascending() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_search_after_index_and_docs(&harness, "sapage", 30).await?;

    let page_size = 10usize;
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<Value> = None;

    for page_idx in 0..3 {
        let mut body = json!({
            "size": page_size,
            "sort": [{ "n": "asc" }]
        });
        if let Some(c) = cursor.clone() {
            body["search_after"] = c;
        }

        let (status, resp) = harness.post_json("/sapage/_search", body).await?;
        assert_eq!(status, StatusCode::OK, "page {page_idx}: {resp}");

        let hits = resp["hits"]["hits"]
            .as_array()
            .expect("hits.hits must be array");
        assert_eq!(hits.len(), page_size, "page {page_idx} size mismatch");

        for hit in hits {
            let id = hit["_id"].as_str().expect("_id").to_string();
            let sort = hit["sort"].as_array().expect("sort must be present");
            assert_eq!(sort.len(), 1, "sort tuple len");
            assert!(!seen.contains(&id), "duplicate _id {id} on page {page_idx}");
            seen.push(id);
        }

        cursor = Some(hits.last().unwrap()["sort"].clone());
    }

    assert_eq!(seen.len(), 30, "should see all 30 docs across 3 pages");
    let expected: Vec<String> = (0..30).map(|i| format!("d{i:03}")).collect();
    assert_eq!(seen, expected, "global ordering across pages");

    // Page past the end returns 0 hits.
    let (status, resp) = harness
        .post_json(
            "/sapage/_search",
            json!({
                "size": page_size,
                "sort": [{ "n": "asc" }],
                "search_after": cursor.unwrap()
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["hits"]["hits"].as_array().map(Vec::len), Some(0));

    Ok(())
}

#[tokio::test]
async fn rest_search_after_rejects_invalid_shapes() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_search_after_index_and_docs(&harness, "sareject", 3).await?;

    // Missing sort
    let (status, body) = harness
        .post_json("/sareject/_search", json!({ "search_after": [0] }))
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"]["type"],
        json!("illegal_argument_exception"),
        "{body}"
    );

    // Length mismatch
    let (status, body) = harness
        .post_json(
            "/sareject/_search",
            json!({
                "sort": [{ "n": "asc" }],
                "search_after": [0, 1]
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));

    // _score in sort
    let (status, body) = harness
        .post_json(
            "/sareject/_search",
            json!({
                "sort": ["_score"],
                "search_after": [0.0]
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));

    // from != 0
    let (status, body) = harness
        .post_json(
            "/sareject/_search",
            json!({
                "from": 1,
                "sort": [{ "n": "asc" }],
                "search_after": [0]
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));

    // search_after + knn is rejected (cursor filter does not apply to kNN leg).
    let (status, body) = harness
        .post_json(
            "/sareject/_search",
            json!({
                "sort": [{ "n": "asc" }],
                "search_after": [0],
                "knn": {
                    "vec": { "vector": [0.1, 0.2, 0.3], "k": 5 }
                }
            }),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));
    let reason = body["error"]["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("k-NN") || reason.contains("knn"),
        "expected kNN rejection reason, got: {reason}"
    );

    Ok(())
}

// ── Cosmetic fix: response shape matches OpenSearch for sorted vs unsorted ──
#[tokio::test]
async fn rest_search_response_shape_unsorted_emits_max_score_and_per_hit_score() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_search_after_index_and_docs(&harness, "shapeu", 5).await?;

    // Unsorted match_all: max_score is a numeric value (BM25 score on match_all
    // is typically 1.0, but we only assert it's a number, not null). Per-hit
    // _score is numeric too.
    let (status, resp) = harness
        .post_json(
            "/shapeu/_search",
            json!({ "size": 3, "query": { "match_all": {} } }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert!(
        resp["hits"]["max_score"].is_number(),
        "unsorted response must emit numeric max_score, got: {}",
        resp["hits"]["max_score"]
    );
    for hit in resp["hits"]["hits"].as_array().expect("hits.hits array") {
        assert!(
            hit["_score"].is_number(),
            "unsorted hit must carry numeric _score, got: {hit}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn rest_search_response_shape_sorted_nulls_score_and_max_score() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_search_after_index_and_docs(&harness, "shapes", 5).await?;

    // Sorted: max_score is null and every hit's _score is null (OpenSearch parity).
    let (status, resp) = harness
        .post_json(
            "/shapes/_search",
            json!({ "size": 3, "sort": [{ "n": "asc" }] }),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert!(
        resp["hits"]["max_score"].is_null(),
        "sorted response must emit max_score: null, got: {}",
        resp["hits"]["max_score"]
    );
    for hit in resp["hits"]["hits"].as_array().expect("hits.hits array") {
        assert!(
            hit["_score"].is_null(),
            "sorted hit must carry _score: null, got: {hit}"
        );
        assert!(
            hit["sort"].is_array(),
            "sorted hit must carry sort array, got: {hit}"
        );
    }
    Ok(())
}

// ── High #2 fix: total + aggs unchanged across search_after pages ──
#[tokio::test]
async fn rest_search_after_total_and_aggs_invariant_across_pages() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_search_after_index_and_docs(&harness, "satotal", 20).await?;

    let body1 = json!({
        "size": 5,
        "sort": [{ "n": "asc" }],
        "aggs": {
            "n_count": { "value_count": { "field": "n" } }
        }
    });
    let (status1, resp1) = harness.post_json("/satotal/_search", body1.clone()).await?;
    assert_eq!(status1, StatusCode::OK, "{resp1}");
    let total1 = resp1["hits"]["total"]["value"].as_u64().expect("total1");
    let agg1 = resp1["aggregations"]["n_count"]["value"]
        .as_f64()
        .expect("agg1");
    assert_eq!(total1, 20, "page 1 total");
    assert_eq!(agg1, 20.0, "page 1 agg");

    let cursor = resp1["hits"]["hits"]
        .as_array()
        .and_then(|h| h.last())
        .and_then(|h| h.get("sort"))
        .cloned()
        .expect("page 1 cursor");

    let mut body2 = body1.clone();
    body2["search_after"] = cursor;

    let (status2, resp2) = harness.post_json("/satotal/_search", body2).await?;
    assert_eq!(status2, StatusCode::OK, "{resp2}");
    let total2 = resp2["hits"]["total"]["value"].as_u64().expect("total2");
    let agg2 = resp2["aggregations"]["n_count"]["value"]
        .as_f64()
        .expect("agg2");
    // The High #2 fix: cursor filter must NOT bias totals or aggs.
    // Without the fix, total2 would shrink to 15 and agg2 to 15.0.
    assert_eq!(total2, 20, "page 2 total must equal page 1 total");
    assert_eq!(agg2, 20.0, "page 2 agg must equal page 1 agg");
    Ok(())
}

// ───────────────────────── Security control plane ─────────────────────────
//
// Dynamic API-key and custom-role management via `/_security/*`. Secrets are
// stored as SHA-256 hashes in Raft-replicated ClusterState (the AddMappings
// idiom); the plaintext secret is returned exactly once and never persisted.

fn bootstrap_admin_key(secret: &str) -> SecurityApiKeyConfig {
    SecurityApiKeyConfig {
        id: "bootstrap-admin".into(),
        name: "bootstrap-admin".into(),
        hash_sha256: ferrissearch::security::sha256_hex(secret),
        roles: vec!["admin".into()],
        indices: vec![],
    }
}

#[tokio::test]
async fn security_api_key_create_use_revoke_lifecycle() -> Result<()> {
    let admin = "admin-bootstrap-secret-1";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    // Create a dynamic read-only key as the admin.
    let (status, body) = harness
        .post_json_auth(
            "/_security/api_key",
            admin,
            json!({ "name": "ci-key", "roles": ["read"] }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let secret = body["api_key"]
        .as_str()
        .expect("api_key returned once")
        .to_string();
    let id = body["id"].as_str().expect("id").to_string();
    assert_eq!(body["created"], json!(true));
    assert_eq!(body["roles"], json!(["read"]));

    // The new key authenticates on a normal endpoint (read grants ClusterMonitor).
    let health = harness.get_status_auth("/_cluster/health", &secret).await?;
    assert_eq!(health, StatusCode::OK);

    // Revoke it as the admin.
    let (del_status, del_body) = harness
        .delete_json_auth(&format!("/_security/api_key/{id}"), admin)
        .await?;
    assert_eq!(del_status, StatusCode::OK, "{del_body}");
    assert_eq!(del_body["deleted"], json!(true));

    // Revocation is immediate: the secret no longer authenticates.
    let denied = harness.get_status_auth("/_cluster/health", &secret).await?;
    assert_eq!(denied, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn security_api_key_list_and_get_never_leak_hash() -> Result<()> {
    let admin = "admin-bootstrap-secret-2";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    let (status, created) = harness
        .post_json_auth(
            "/_security/api_key",
            admin,
            json!({ "name": "leak-check", "roles": ["read"] }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap().to_string();

    let (ls, list) = harness.get_json_auth("/_security/api_key", admin).await?;
    assert_eq!(ls, StatusCode::OK, "{list}");
    let arr = list["api_keys"].as_array().expect("api_keys array");
    assert_eq!(arr.len(), 1);
    let entry = &arr[0];
    assert_eq!(entry["id"], json!(id));
    assert_eq!(entry["name"], json!("leak-check"));
    assert!(
        entry.get("hash_sha256").is_none(),
        "list must not leak hash"
    );
    assert!(entry.get("api_key").is_none(), "list must not leak secret");

    let (gs, one) = harness
        .get_json_auth(&format!("/_security/api_key/{id}"), admin)
        .await?;
    assert_eq!(gs, StatusCode::OK, "{one}");
    assert!(one.get("hash_sha256").is_none(), "get must not leak hash");
    assert!(one.get("api_key").is_none(), "get must not leak secret");
    Ok(())
}

#[tokio::test]
async fn security_non_admin_principal_denied_on_security_endpoints() -> Result<()> {
    let admin = "admin-bootstrap-secret-3";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    let (status, created) = harness
        .post_json_auth(
            "/_security/api_key",
            admin,
            json!({ "name": "reader", "roles": ["read"] }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let reader_secret = created["api_key"].as_str().unwrap().to_string();

    // The `read` role lacks SecurityAdmin → 403 on /_security/*.
    let denied = harness
        .get_status_auth("/_security/api_key", &reader_secret)
        .await?;
    assert_eq!(denied, StatusCode::FORBIDDEN);

    // Missing credentials → 401.
    let (unauth, _) = harness.get_json("/_security/api_key").await?;
    assert_eq!(unauth, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn security_static_and_dynamic_keys_coexist() -> Result<()> {
    let admin = "admin-bootstrap-secret-4";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    // Bootstrap (static) key works.
    assert_eq!(
        harness.get_status_auth("/_cluster/health", admin).await?,
        StatusCode::OK
    );

    // Create a dynamic key; it also works.
    let (status, created) = harness
        .post_json_auth(
            "/_security/api_key",
            admin,
            json!({ "name": "dyn", "roles": ["read"] }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let dyn_secret = created["api_key"].as_str().unwrap().to_string();
    assert_eq!(
        harness
            .get_status_auth("/_cluster/health", &dyn_secret)
            .await?,
        StatusCode::OK
    );

    // A bogus key is rejected.
    assert_eq!(
        harness
            .get_status_auth("/_cluster/health", "totally-wrong-secret")
            .await?,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test]
async fn security_custom_role_grants_scoped_index_access() -> Result<()> {
    let admin = "admin-bootstrap-secret-5";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    // Define a custom role limited to logs-* with read privilege.
    let (rs, rb) = harness
        .put_json_auth(
            "/_security/role/logs-reader",
            admin,
            json!({ "indices": ["logs-*"], "index_privileges": ["read"] }),
        )
        .await?;
    assert_eq!(rs, StatusCode::OK, "{rb}");

    // Create a matching index as admin, then a key bound to the custom role.
    let (cs, cb) = harness
        .put_json_auth(
            "/logs-app",
            admin,
            json!({ "settings": { "number_of_shards": 1, "number_of_replicas": 0 } }),
        )
        .await?;
    assert_eq!(cs, StatusCode::OK, "{cb}");

    let (ks, kb) = harness
        .post_json_auth(
            "/_security/api_key",
            admin,
            json!({ "name": "logs-key", "roles": ["logs-reader"] }),
        )
        .await?;
    assert_eq!(ks, StatusCode::CREATED, "{kb}");
    let key = kb["api_key"].as_str().unwrap().to_string();

    // Allowed: search a matching index (authz passes).
    let allowed = harness.get_status_auth("/logs-app/_search", &key).await?;
    assert_eq!(
        allowed,
        StatusCode::OK,
        "custom role must allow logs-* read"
    );

    // Denied: a non-matching index → 403 (index pattern mismatch in the role).
    let denied = harness
        .get_status_auth("/other-index/_search", &key)
        .await?;
    assert_eq!(
        denied,
        StatusCode::FORBIDDEN,
        "custom role must not allow non-matching indices"
    );
    Ok(())
}

#[tokio::test]
async fn security_create_api_key_requires_name() -> Result<()> {
    let admin = "admin-bootstrap-secret-6";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;
    let (status, body) = harness
        .post_json_auth("/_security/api_key", admin, json!({ "roles": ["read"] }))
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("illegal_argument_exception"));
    Ok(())
}

#[tokio::test]
async fn security_delete_unknown_api_key_returns_404() -> Result<()> {
    let admin = "admin-bootstrap-secret-7";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;
    let (status, body) = harness
        .delete_json_auth("/_security/api_key/does-not-exist", admin)
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["type"], json!("resource_not_found_exception"));
    Ok(())
}

#[tokio::test]
async fn security_role_create_get_list_delete() -> Result<()> {
    let admin = "admin-bootstrap-secret-8";
    let harness = RestTestHarness::start_security_enabled(vec![bootstrap_admin_key(admin)]).await?;

    let (ps, pb) = harness
        .put_json_auth(
            "/_security/role/analytics",
            admin,
            json!({
                "cluster": ["monitor"],
                "indices": ["metrics-*"],
                "index_privileges": ["read", "write"]
            }),
        )
        .await?;
    assert_eq!(ps, StatusCode::OK, "{pb}");
    assert_eq!(pb["acknowledged"], json!(true));

    let (gs, gb) = harness
        .get_json_auth("/_security/role/analytics", admin)
        .await?;
    assert_eq!(gs, StatusCode::OK, "{gb}");
    assert_eq!(gb["name"], json!("analytics"));
    assert_eq!(gb["cluster"], json!(["monitor"]));
    assert_eq!(gb["indices"], json!(["metrics-*"]));
    assert_eq!(gb["index_privileges"], json!(["read", "write"]));

    let (lss, lb) = harness.get_json_auth("/_security/role", admin).await?;
    assert_eq!(lss, StatusCode::OK, "{lb}");
    assert_eq!(lb["roles"].as_array().unwrap().len(), 1);

    let (ds, db) = harness
        .delete_json_auth("/_security/role/analytics", admin)
        .await?;
    assert_eq!(ds, StatusCode::OK, "{db}");
    assert_eq!(db["deleted"], json!(true));

    let (gs2, _) = harness
        .get_json_auth("/_security/role/analytics", admin)
        .await?;
    assert_eq!(gs2, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn security_endpoints_work_when_security_disabled() -> Result<()> {
    // Handlers must function when security is disabled (consistent with the
    // rest of the API): no auth header required, secret still returned once.
    let harness = RestTestHarness::start().await?;
    let (status, body) = harness
        .post_json(
            "/_security/api_key",
            json!({ "name": "nosec", "roles": ["read"] }),
        )
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(
        body["api_key"].as_str().is_some(),
        "secret returned even when security disabled"
    );

    let (ls, list) = harness.get_json("/_security/api_key").await?;
    assert_eq!(ls, StatusCode::OK, "{list}");
    assert_eq!(list["api_keys"].as_array().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn security_put_api_key_forwarded_from_follower_to_leader() -> Result<()> {
    let harness = MultiNodeRestHarness::start_three_nodes().await?;

    // POST to node-2 (a follower). The coordinator pattern must forward the
    // PutApiKey write to the leader (node-1) instead of erroring.
    let (status, body) = post_json_to_base_url(
        &harness.client,
        &harness.nodes[1].base_url,
        "/_security/api_key",
        json!({ "name": "via-follower", "roles": ["read"] }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let id = body["id"].as_str().expect("id").to_string();
    assert!(body["api_key"].as_str().is_some());

    // The leader (node-1) now has the key in its replicated cluster state.
    let (ls, list) = get_json_from_base_url(
        &harness.client,
        &harness.nodes[0].base_url,
        "/_security/api_key",
    )
    .await?;
    assert_eq!(ls, StatusCode::OK, "{list}");
    let found = list["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|k| k["id"] == json!(id));
    assert!(found, "leader must have the follower-forwarded key");
    Ok(())
}

#[tokio::test]
async fn security_put_api_key_transport_rpc_applies_to_state() -> Result<()> {
    use ferrissearch::transport::proto::PutApiKeyRequest;

    // Single-node leader harness. Drive the typed transport RPC directly.
    let harness = RestTestHarness::start().await?;
    let mut client =
        InternalTransportClient::connect(format!("http://{}", harness.transport_addr)).await?;

    let record = ferrissearch::cluster::state::SecurityApiKeyRecord {
        id: "rpc-key-1".into(),
        name: "rpc".into(),
        hash_sha256: ferrissearch::security::sha256_hex("rpc-secret"),
        roles: vec!["read".into()],
        indices: vec![],
        created_at_millis: 123,
    };
    let resp = client
        .put_api_key(tonic::Request::new(PutApiKeyRequest {
            record_json: serde_json::to_string(&record)?,
        }))
        .await?
        .into_inner();
    assert!(resp.acknowledged, "ack failed: {}", resp.error);
    assert!(resp.error.is_empty());

    // The applied key is visible via the HTTP list endpoint (shared state).
    let (ls, list) = harness.get_json("/_security/api_key").await?;
    assert_eq!(ls, StatusCode::OK, "{list}");
    let found = list["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|k| k["id"] == json!("rpc-key-1"));
    assert!(found, "transport-applied key must be visible");
    Ok(())
}

#[tokio::test]
async fn security_put_api_key_transport_rpc_rejects_malformed_hash() -> Result<()> {
    use ferrissearch::transport::proto::PutApiKeyRequest;

    // The leader must validate the record at the transport trust boundary and
    // reject a record whose hash is not a 64-char hex digest.
    let harness = RestTestHarness::start().await?;
    let mut client =
        InternalTransportClient::connect(format!("http://{}", harness.transport_addr)).await?;

    let bad = ferrissearch::cluster::state::SecurityApiKeyRecord {
        id: "bad-key".into(),
        name: "bad".into(),
        hash_sha256: "not-a-valid-hash".into(),
        roles: vec!["read".into()],
        indices: vec![],
        created_at_millis: 1,
    };
    let status = client
        .put_api_key(tonic::Request::new(PutApiKeyRequest {
            record_json: serde_json::to_string(&bad)?,
        }))
        .await
        .expect_err("malformed hash must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");

    // Nothing was committed to cluster state.
    let (ls, list) = harness.get_json("/_security/api_key").await?;
    assert_eq!(ls, StatusCode::OK, "{list}");
    assert!(
        list["api_keys"].as_array().unwrap().is_empty(),
        "rejected key must not be stored"
    );
    Ok(())
}

#[tokio::test]
async fn group_by_unmapped_builtin_body_is_rejected_as_text() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    for (index, mappings) in [
        ("gb-body-dynamic", json!({"dynamic": true})),
        (
            "gb-body-strict",
            json!({"dynamic": "strict", "properties": {"n": {"type": "integer"}}}),
        ),
    ] {
        let (status, body) = harness
            .put_json(
                &format!("/{index}"),
                json!({"settings": {"number_of_shards": 1, "number_of_replicas": 0}, "mappings": mappings}),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = harness
            .put_json(
                &format!("/{index}/_doc/1?refresh=true"),
                json!({"body": "hello world", "n": 1}),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let query = format!("SELECT body, count(*) AS c FROM \"{index}\" GROUP BY body");
        let (status, body) = harness
            .post_json(&format!("/{index}/_sql"), json!({"query": query}))
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{index}: {body}");
        assert_eq!(
            body["error"]["type"], "group_by_text_field_exception",
            "{body}"
        );
    }
    Ok(())
}
