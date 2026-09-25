use anyhow::{Context, Result, bail};
use ferrissearch::engine::routing::calculate_shard;
use ferrissearch::transport::proto::ShardGetRequest;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::OwnedMutexGuard;

const INDEX_NAME: &str = "restart-regression";
const CLUSTER_NAME: &str = "restart-regression-cluster";
const DOC_COUNT: usize = 12_000;
const BATCH_SIZE: usize = 500;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const FAILOVER_TIMEOUT: Duration = Duration::from_secs(90);
const DOC_BODY: &str = "restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload";

#[derive(Clone)]
struct NodeConfig {
    name: String,
    data_dir: PathBuf,
    log_path: PathBuf,
    http_port: u16,
    transport_port: u16,
    raft_node_id: u64,
    max_concurrent_peer_recoveries: Option<usize>,
}

impl NodeConfig {
    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }
}

struct NodeProcess {
    config: NodeConfig,
    child: Child,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RoutingSnapshot {
    primary: String,
    replicas: Vec<String>,
    in_sync_replicas: Vec<String>,
    unassigned_replicas: u32,
}

impl NodeProcess {
    fn spawn(config: NodeConfig, seed_hosts: &str) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.log_path)
            .with_context(|| format!("open log file {:?}", config.log_path))?;
        let stderr = stdout.try_clone()?;

        let binary = std::env::var("CARGO_BIN_EXE_ferrissearch")
            .context("CARGO_BIN_EXE_ferrissearch is not set for restart regression")?;
        let mut cmd = Command::new(binary);
        for (key, _) in std::env::vars() {
            if key.starts_with("FERRISSEARCH_") {
                cmd.env_remove(key);
            }
        }

        cmd.current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("RUST_LOG", "info")
            .env("FERRISSEARCH_NODE_NAME", &config.name)
            .env("FERRISSEARCH_CLUSTER_NAME", CLUSTER_NAME)
            .env("FERRISSEARCH_HTTP_PORT", config.http_port.to_string())
            .env(
                "FERRISSEARCH_TRANSPORT_PORT",
                config.transport_port.to_string(),
            )
            .env("FERRISSEARCH_DATA_DIR", &config.data_dir)
            .env("FERRISSEARCH_SEED_HOSTS", seed_hosts)
            .env("FERRISSEARCH_RAFT_NODE_ID", config.raft_node_id.to_string())
            .env("FERRISSEARCH_COLUMN_CACHE_SIZE_PERCENT", "0")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if let Some(limit) = config.max_concurrent_peer_recoveries {
            cmd.env(
                "FERRISSEARCH_MAX_CONCURRENT_PEER_RECOVERIES",
                limit.to_string(),
            );
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("spawn node {}", config.name))?;
        Ok(Self { config, child })
    }

    fn ensure_running(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "node {} exited unexpectedly with status {}\n{}",
                self.config.name,
                status,
                self.log_tail(80)
            );
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        Ok(())
    }

    fn log_tail(&self, max_lines: usize) -> String {
        let text = fs::read_to_string(&self.config.log_path).unwrap_or_default();
        let mut lines: Vec<&str> = text.lines().collect();
        if lines.len() > max_lines {
            lines.drain(..lines.len() - max_lines);
        }
        lines.join("\n")
    }
}

struct RestartClusterHarness {
    _process_guard: OwnedMutexGuard<()>,
    _temp_dir: TempDir,
    client: Client,
    seed_hosts: String,
    nodes: Vec<NodeProcess>,
}

impl RestartClusterHarness {
    async fn start() -> Result<Self> {
        Self::start_with_peer_recovery_limit(None).await
    }

    async fn start_with_peer_recovery_limit(limit: Option<usize>) -> Result<Self> {
        let process_guard = process_test_lock().clone().lock_owned().await;
        let temp_dir = tempfile::tempdir()?;
        let port_reservations = (0..6)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let ports = port_reservations
            .iter()
            .map(|listener| listener.local_addr().map(|address| address.port()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let node_configs: Vec<NodeConfig> = (1..=3)
            .enumerate()
            .map(|(position, idx)| NodeConfig {
                name: format!("node-{idx}"),
                data_dir: temp_dir.path().join(format!("node-{idx}")),
                log_path: temp_dir.path().join(format!("node-{idx}.log")),
                http_port: ports[position * 2],
                transport_port: ports[position * 2 + 1],
                raft_node_id: idx as u64,
                max_concurrent_peer_recoveries: limit,
            })
            .collect();

        let seed_hosts = node_configs
            .iter()
            .map(|cfg| format!("127.0.0.1:{}", cfg.transport_port))
            .collect::<Vec<_>>()
            .join(",");
        drop(port_reservations);

        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
        let mut nodes = Vec::new();

        let mut configs = node_configs.into_iter();
        if let Some(first) = configs.next() {
            nodes.push(NodeProcess::spawn(first, &seed_hosts)?);
            wait_for_http_ready(&client, nodes.last_mut().unwrap()).await?;
            wait_for_single_node_bootstrap(&client, nodes.last_mut().unwrap()).await?;
        }
        for config in configs {
            nodes.push(NodeProcess::spawn(config, &seed_hosts)?);
            wait_for_http_ready(&client, nodes.last_mut().unwrap()).await?;
        }

        let mut harness = Self {
            _process_guard: process_guard,
            _temp_dir: temp_dir,
            client,
            seed_hosts,
            nodes,
        };
        harness.wait_for_cluster_state(3, None).await?;
        Ok(harness)
    }

    async fn restart_all(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            node.stop()?;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let configs: Vec<NodeConfig> = self.nodes.iter().map(|node| node.config.clone()).collect();
        self.nodes.clear();
        let mut configs = configs.into_iter();
        if let Some(first) = configs.next() {
            self.nodes
                .push(NodeProcess::spawn(first, &self.seed_hosts)?);
            wait_for_http_ready(&self.client, self.nodes.last_mut().unwrap()).await?;
            wait_for_single_node_bootstrap(&self.client, self.nodes.last_mut().unwrap()).await?;
        }
        for config in configs {
            self.nodes
                .push(NodeProcess::spawn(config, &self.seed_hosts)?);
            wait_for_http_ready(&self.client, self.nodes.last_mut().unwrap()).await?;
        }
        self.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
        Ok(())
    }

    async fn restart_node(&mut self, stopped: NodeProcess) -> Result<()> {
        let config = stopped.config.clone();
        let mut restarted = NodeProcess::spawn(config, &self.seed_hosts)?;
        wait_for_http_ready(&self.client, &mut restarted).await?;
        self.nodes.push(restarted);
        self.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
        Ok(())
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value)> {
        let coordinator = self
            .nodes
            .first()
            .context("no running node is available as an HTTP coordinator")?;
        let url = format!("{}{}", coordinator.config.base_url(), path);
        let builder = self.client.request(method, url);
        let builder = if let Some(body) = body {
            builder.json(&body)
        } else {
            builder
        };
        let response = builder.send().await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn post_ndjson(&self, path: &str, body: String) -> Result<(StatusCode, Value)> {
        let coordinator = self
            .nodes
            .first()
            .context("no running node is available as an HTTP coordinator")?;
        let url = format!("{}{}", coordinator.config.base_url(), path);
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json().await?;
        Ok((status, value))
    }

    async fn request_text(&self, path: &str) -> Result<(StatusCode, String)> {
        let coordinator = self
            .nodes
            .first()
            .context("no running node is available as an HTTP coordinator")?;
        let response = self
            .client
            .get(format!("{}{}", coordinator.config.base_url(), path))
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        Ok((status, text))
    }

    fn stop_node(&mut self, node_id: &str) -> Result<NodeProcess> {
        let position = self
            .nodes
            .iter()
            .position(|node| node.config.name == node_id)
            .with_context(|| format!("node {node_id} is not running in the harness"))?;
        let mut node = self.nodes.remove(position);
        node.stop()?;
        Ok(node)
    }

    async fn wait_for_cluster_state(
        &mut self,
        expected_nodes: usize,
        expected_index: Option<&str>,
    ) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
            {
                let nodes_len = state["nodes"]
                    .as_object()
                    .map(|nodes| nodes.len())
                    .unwrap_or(0);
                let index_ready = expected_index
                    .is_none_or(|index_name| state["indices"].get(index_name).is_some());
                if nodes_len == expected_nodes && index_ready {
                    return Ok(state);
                }
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "cluster did not reach {} nodes with index {:?}\n{}",
                    expected_nodes,
                    expected_index,
                    self.logs_summary()
                );
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_index_shards(
        &mut self,
        index_name: &str,
        expected_started: usize,
        expected_unassigned: usize,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            if let Ok((status, text)) = self.request_text("/_cat/shards?v").await
                && status == StatusCode::OK
            {
                let rows = text
                    .lines()
                    .skip(1)
                    .filter_map(|line| {
                        let columns = line.split_whitespace().collect::<Vec<_>>();
                        (columns.first().copied() == Some(index_name)).then_some(columns)
                    })
                    .collect::<Vec<_>>();
                let started = rows
                    .iter()
                    .filter(|columns| columns.get(3).copied() == Some("STARTED"))
                    .count();
                let unassigned = rows
                    .iter()
                    .filter(|columns| columns.get(3).copied() == Some("UNASSIGNED"))
                    .count();

                if rows.len() == expected_started + expected_unassigned
                    && started == expected_started
                    && unassigned == expected_unassigned
                {
                    return Ok(());
                }
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "shards for {index_name} did not reach {expected_started} STARTED and {expected_unassigned} UNASSIGNED copies\n{}",
                    self.logs_summary()
                );
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_node_loss_accounting(
        &mut self,
        index_name: &str,
        dead_node: &str,
    ) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
            {
                let nodes = state["nodes"].as_object();
                let master = state["master_node"].as_str();
                let routing = routing_snapshot(&state, index_name);
                let accounted = routing.as_ref().is_ok_and(|shards| {
                    shards.len() == 3
                        && shards.values().all(|entry| {
                            entry.primary != dead_node
                                && !entry.replicas.iter().any(|node| node == dead_node)
                                && entry.replicas.len() == 1
                                && entry.unassigned_replicas == 1
                        })
                });

                if nodes.is_some_and(|nodes| {
                    nodes.len() == 2
                        && !nodes.contains_key(dead_node)
                        && master
                            .is_some_and(|master| master != dead_node && nodes.contains_key(master))
                }) && accounted
                {
                    return Ok(state);
                }
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "cluster did not remove {dead_node} and account one lost copy per shard within {FAILOVER_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_count(&mut self, expected_count: usize) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            if let Ok((status, body)) = self
                .request_json(Method::GET, &format!("/{INDEX_NAME}/_count"), None)
                .await
                && status == StatusCode::OK
                && body["count"].as_u64() == Some(expected_count as u64)
            {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "count for {} did not converge to {}\n{}",
                    INDEX_NAME,
                    expected_count,
                    self.logs_summary()
                );
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn create_index(&mut self, number_of_replicas: u32) -> Result<String> {
        self.create_index_with_shards(3, number_of_replicas).await
    }

    async fn create_index_with_shards(
        &mut self,
        number_of_shards: u32,
        number_of_replicas: u32,
    ) -> Result<String> {
        let (status, body) = self
            .request_json(
                Method::PUT,
                &format!("/{INDEX_NAME}"),
                Some(json!({
                    "settings": {
                        "number_of_shards": number_of_shards,
                        "number_of_replicas": number_of_replicas,
                        "refresh_interval_ms": 60000
                    },
                    "mappings": {
                        "properties": {
                            "title": { "type": "text" },
                            "author": { "type": "keyword" },
                            "payload": { "type": "text" },
                            "n": { "type": "integer" }
                        }
                    }
                })),
            )
            .await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected create-index body: {body}"
        );

        let state = self.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
        let uuid = state["indices"][INDEX_NAME]["uuid"]
            .as_str()
            .context("cluster state is missing index uuid")?
            .to_string();
        Ok(uuid)
    }

    async fn put_document(&self, doc_id: &str, source: Value) -> Result<Value> {
        let (status, body) = self
            .request_json(
                Method::PUT,
                &format!("/{INDEX_NAME}/_doc/{doc_id}"),
                Some(source),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "index {doc_id}: {body}");
        assert_eq!(body["_id"], json!(doc_id), "index receipt: {body}");
        assert!(
            body["_seq_no"].as_u64().is_some(),
            "index receipt is missing _seq_no: {body}"
        );
        Ok(body)
    }

    async fn delete_document(&self, doc_id: &str) -> Result<Value> {
        let (status, body) = self
            .request_json(
                Method::DELETE,
                &format!("/{INDEX_NAME}/_doc/{doc_id}"),
                None,
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "delete {doc_id}: {body}");
        assert_eq!(body["result"], json!("deleted"), "delete receipt: {body}");
        assert!(
            body["_seq_no"].as_u64().is_some(),
            "delete receipt is missing _seq_no: {body}"
        );
        Ok(body)
    }

    async fn get_document(&self, doc_id: &str) -> Result<(StatusCode, Value)> {
        self.request_json(Method::GET, &format!("/{INDEX_NAME}/_doc/{doc_id}"), None)
            .await
    }

    async fn refresh_index(&self) -> Result<()> {
        let (status, body) = self
            .request_json(
                Method::POST,
                &format!("/{INDEX_NAME}/_refresh"),
                Some(json!({})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "refresh failed: {body}");
        assert_eq!(
            body["_shards"]["failed"],
            json!(0),
            "refresh reported failures: {body}"
        );
        Ok(())
    }

    async fn bulk_index_documents(&self, doc_count: usize) -> Result<()> {
        self.bulk_index_document_range(0, doc_count).await
    }

    async fn bulk_index_document_range(&self, start: usize, end: usize) -> Result<()> {
        for batch_start in (start..end).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, end);
            let mut body = String::with_capacity((batch_end - batch_start) * 320);
            for doc_id in batch_start..batch_end {
                body.push_str(&format!("{{\"index\":{{\"_id\":\"doc-{doc_id}\"}}}}\n"));
                body.push_str(&format!(
                    "{{\"title\":\"restart regression doc {doc_id}\",\"author\":\"author-{}\",\"payload\":\"{}\",\"n\":{doc_id}}}\n",
                    doc_id % 17,
                    DOC_BODY,
                ));
            }

            let (status, response_body) = self
                .post_ndjson(&format!("/{INDEX_NAME}/_bulk"), body)
                .await?;
            assert_eq!(
                status,
                StatusCode::OK,
                "bulk request failed: {response_body}"
            );
            assert_eq!(
                response_body["errors"],
                json!(false),
                "bulk response had item errors: {response_body}"
            );
        }
        Ok(())
    }

    async fn flush_index(&self) -> Result<()> {
        self.flush_index_copies(3).await
    }

    async fn flush_index_copies(&self, expected_successful: u64) -> Result<()> {
        let (status, body) = self
            .request_json(
                Method::POST,
                &format!("/{INDEX_NAME}/_flush"),
                Some(json!({})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "flush failed: {body}");
        assert_eq!(
            body["_shards"]["successful"],
            json!(expected_successful),
            "expected {expected_successful} shard copies to flush: {body}"
        );
        Ok(())
    }

    async fn update_replica_count(&self, number_of_replicas: u32) -> Result<()> {
        let (status, body) = self
            .request_json(
                Method::PUT,
                &format!("/{INDEX_NAME}/_settings"),
                Some(json!({
                    "index": {
                        "number_of_replicas": number_of_replicas
                    }
                })),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "replica update failed: {body}");
        assert_eq!(body["acknowledged"], json!(true), "{body}");
        Ok(())
    }

    async fn wait_for_out_of_sync_replica(&mut self) -> Result<(Value, RoutingSnapshot)> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
                && let Ok(routing) = routing_snapshot(&state, INDEX_NAME)
                && let Some(shard) = routing.get(&0)
                && shard.replicas.len() == 1
                && shard.in_sync_replicas.is_empty()
                && shard.unassigned_replicas == 0
            {
                return Ok((state, shard.clone()));
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "replica was not assigned out of sync within {READY_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_in_sync_replicas(
        &mut self,
        expected_replicas: usize,
    ) -> Result<(Value, RoutingSnapshot)> {
        let deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }
            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
                && let Ok(routing) = routing_snapshot(&state, INDEX_NAME)
                && let Some(shard) = routing.get(&0)
                && shard.replicas.len() == expected_replicas
                && shard.in_sync_replicas.len() == expected_replicas
                && shard
                    .replicas
                    .iter()
                    .all(|replica| shard.in_sync_replicas.contains(replica))
                && shard.unassigned_replicas == 0
            {
                return Ok((state, shard.clone()));
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "replicas did not become in sync within {FAILOVER_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_primary_promotion(&mut self, old_primary: &str) -> Result<RoutingSnapshot> {
        let deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }
            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
                && state["nodes"]
                    .as_object()
                    .is_some_and(|nodes| !nodes.contains_key(old_primary))
                && let Ok(routing) = routing_snapshot(&state, INDEX_NAME)
                && let Some(shard) = routing.get(&0)
                && shard.primary != old_primary
            {
                return Ok(shard.clone());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "primary {old_primary} was not promoted within {FAILOVER_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_green(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }
            if let Ok((status, health)) = self
                .request_json(Method::GET, "/_cluster/health", None)
                .await
                && status == StatusCode::OK
                && health["status"] == json!("green")
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "cluster did not become green within {FAILOVER_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_primary_removal_without_promotion(
        &mut self,
        primary: &str,
        replica: &str,
    ) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }
            if let Ok((status, state)) = self
                .request_json(Method::GET, "/_cluster/state", None)
                .await
                && status == StatusCode::OK
                && state["nodes"]
                    .as_object()
                    .is_some_and(|nodes| nodes.len() == 2 && !nodes.contains_key(primary))
                && state["master_node"]
                    .as_str()
                    .is_some_and(|master| master != primary)
                && let Ok(routing) = routing_snapshot(&state, INDEX_NAME)
                && let Some(shard) = routing.get(&0)
                && shard.primary == primary
                && shard.replicas == [replica]
                && shard.in_sync_replicas.is_empty()
            {
                return Ok(state);
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "dead primary {primary} was not removed without promotion within {FAILOVER_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_exact_documents(&mut self, expected_count: usize) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            for node in &mut self.nodes {
                node.ensure_running()?;
            }

            let mut complete = true;
            for doc_id in 0..expected_count {
                let expected = expected_document(doc_id);
                match self.get_document(&format!("doc-{doc_id}")).await {
                    Ok((StatusCode::OK, body)) if body["_source"] == expected => {}
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "acknowledged documents did not return with exact values within {READY_TIMEOUT:?}\n{}",
                    self.logs_summary()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn assert_expected_shard_dirs_exist(
        &self,
        cluster_state: &Value,
        index_uuid: &str,
    ) -> Result<()> {
        let shard_routing = cluster_state["indices"][INDEX_NAME]["shard_routing"]
            .as_object()
            .context("cluster state missing shard routing")?;

        for node in &self.nodes {
            let mut expected = Vec::new();
            for (shard_id, routing) in shard_routing {
                let assigned_here = routing["primary"].as_str() == Some(node.config.name.as_str())
                    || routing["replicas"]
                        .as_array()
                        .map(|replicas| {
                            replicas
                                .iter()
                                .any(|replica| replica.as_str() == Some(node.config.name.as_str()))
                        })
                        .unwrap_or(false);
                if assigned_here {
                    expected.push(shard_id.parse::<u32>()?);
                }
            }

            for shard_id in expected {
                let shard_dir = node
                    .config
                    .data_dir
                    .join(index_uuid)
                    .join(format!("shard_{shard_id}"));
                assert!(
                    shard_dir.exists(),
                    "expected shard dir {:?} to exist for {}\n{}",
                    shard_dir,
                    node.config.name,
                    self.logs_summary()
                );
            }
        }

        Ok(())
    }

    fn assert_no_delete_reasons_in_logs(&self) {
        for node in &self.nodes {
            let log_text = fs::read_to_string(&node.config.log_path).unwrap_or_default();
            for forbidden in [
                "reason=api_delete_index",
                "reason=transport_delete_index_rpc",
                "reason=orphan_cleanup_unknown_uuid",
            ] {
                assert!(
                    !log_text.contains(forbidden),
                    "unexpected destructive delete reason {} in {} log\n{}",
                    forbidden,
                    node.config.name,
                    node.log_tail(120)
                );
            }
        }
    }

    fn logs_summary(&self) -> String {
        self.nodes
            .iter()
            .map(|node| format!("===== {} =====\n{}", node.config.name, node.log_tail(80)))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for RestartClusterHarness {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.stop();
        }
    }
}

fn process_test_lock() -> &'static Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
}

fn routing_snapshot(
    cluster_state: &Value,
    index_name: &str,
) -> Result<BTreeMap<u32, RoutingSnapshot>> {
    let routing = cluster_state["indices"][index_name]["shard_routing"]
        .as_object()
        .with_context(|| format!("cluster state is missing routing for {index_name}"))?;
    routing
        .iter()
        .map(|(shard_id, entry)| {
            let shard_id = shard_id
                .parse::<u32>()
                .with_context(|| format!("invalid shard id {shard_id}"))?;
            let primary = entry["primary"]
                .as_str()
                .with_context(|| format!("shard {shard_id} is missing primary"))?
                .to_string();
            let replicas = entry["replicas"]
                .as_array()
                .with_context(|| format!("shard {shard_id} is missing replicas"))?
                .iter()
                .map(|node| {
                    node.as_str()
                        .with_context(|| format!("shard {shard_id} has a non-string replica"))
                        .map(str::to_string)
                })
                .collect::<Result<Vec<_>>>()?;
            let in_sync_replicas = entry["in_sync_replicas"]
                .as_array()
                .with_context(|| format!("shard {shard_id} is missing in_sync_replicas"))?
                .iter()
                .map(|node| {
                    node.as_str()
                        .with_context(|| {
                            format!("shard {shard_id} has a non-string in-sync replica")
                        })
                        .map(str::to_string)
                })
                .collect::<Result<Vec<_>>>()?;
            let unassigned_replicas = entry["unassigned_replicas"]
                .as_u64()
                .with_context(|| format!("shard {shard_id} is missing unassigned_replicas"))?
                as u32;
            Ok((
                shard_id,
                RoutingSnapshot {
                    primary,
                    replicas,
                    in_sync_replicas,
                    unassigned_replicas,
                },
            ))
        })
        .collect()
}

fn expected_document(doc_id: usize) -> Value {
    json!({
        "title": format!("restart regression doc {doc_id}"),
        "author": format!("author-{}", doc_id % 17),
        "payload": DOC_BODY,
        "n": doc_id,
    })
}

async fn get_local_document(node: &NodeConfig, doc_id: &str) -> Result<Option<Value>> {
    let mut client =
        InternalTransportClient::connect(format!("http://127.0.0.1:{}", node.transport_port))
            .await?;
    let response = client
        .get_doc(tonic::Request::new(ShardGetRequest {
            index_name: INDEX_NAME.into(),
            shard_id: 0,
            doc_id: doc_id.into(),
        }))
        .await?
        .into_inner();
    if !response.found {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&response.source_json)?))
}

fn document_id_for_shard(prefix: &str, shard_id: u32) -> String {
    for suffix in 0..10_000 {
        let candidate = format!("{prefix}-{suffix}");
        if calculate_shard(&candidate, 3) == shard_id {
            return candidate;
        }
    }
    panic!("could not find a document ID for shard {shard_id}");
}

async fn wait_for_http_ready(client: &Client, node: &mut NodeProcess) -> Result<()> {
    let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    loop {
        node.ensure_running()?;

        if let Ok(response) = client
            .get(format!("{}/", node.config.base_url()))
            .send()
            .await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            bail!(
                "node {} did not become ready\n{}",
                node.config.name,
                node.log_tail(80)
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_single_node_bootstrap(client: &Client, node: &mut NodeProcess) -> Result<()> {
    let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    loop {
        node.ensure_running()?;

        if let Ok(response) = client
            .get(format!("{}/_cluster/state", node.config.base_url()))
            .send()
            .await
            && response.status() == StatusCode::OK
        {
            let state: Value = response.json().await?;
            let master = state["master_node"].as_str();
            let has_local = state["nodes"].get(node.config.name.as_str()).is_some();
            if master == Some(node.config.name.as_str()) && has_local {
                return Ok(());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            bail!(
                "node {} did not finish single-node bootstrap\n{}",
                node.config.name,
                node.log_tail(80)
            );
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn three_node_flush_restart_preserves_uuid_dirs_and_document_count() -> Result<()> {
    let mut harness = RestartClusterHarness::start().await?;
    let index_uuid = harness.create_index(0).await?;

    harness.bulk_index_documents(DOC_COUNT).await?;
    harness.flush_index().await?;

    let cluster_state_before_restart = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    assert_eq!(
        cluster_state_before_restart["indices"][INDEX_NAME]["uuid"],
        json!(index_uuid)
    );
    harness.assert_expected_shard_dirs_exist(&cluster_state_before_restart, &index_uuid)?;

    harness.restart_all().await?;

    let cluster_state_after_restart = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    assert_eq!(
        cluster_state_after_restart["indices"][INDEX_NAME]["uuid"],
        json!(index_uuid)
    );
    harness.assert_expected_shard_dirs_exist(&cluster_state_after_restart, &index_uuid)?;
    harness.wait_for_count(DOC_COUNT).await?;
    harness.assert_no_delete_reasons_in_logs();

    Ok(())
}

#[tokio::test]
async fn added_replica_recovers_files_and_survives_primary_loss() -> Result<()> {
    let mut harness = RestartClusterHarness::start().await?;
    harness.create_index_with_shards(1, 0).await?;
    harness
        .wait_for_index_shards(INDEX_NAME, 1, 0, READY_TIMEOUT)
        .await?;

    harness.bulk_index_document_range(0, 20).await?;
    harness.refresh_index().await?;
    harness.flush_index_copies(1).await?;
    harness.bulk_index_document_range(20, 25).await?;
    harness.refresh_index().await?;

    let before_replica = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    let primary = routing_snapshot(&before_replica, INDEX_NAME)?[&0]
        .primary
        .clone();
    harness.update_replica_count(1).await?;
    let (_assigned_state, assigned) = harness.wait_for_out_of_sync_replica().await?;
    let replica = assigned.replicas[0].clone();
    assert_ne!(primary, replica);

    let mut next_doc = 25usize;
    let recovery_deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
    loop {
        let state = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
        let routing = routing_snapshot(&state, INDEX_NAME)?;
        if routing[&0]
            .in_sync_replicas
            .iter()
            .any(|node| node == &replica)
        {
            break;
        }
        harness
            .put_document(&format!("doc-{next_doc}"), expected_document(next_doc))
            .await?;
        next_doc += 1;
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "peer recovery did not finish while concurrent writes continued\n{}",
            harness.logs_summary()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        next_doc > 25,
        "the test must acknowledge a write during recovery"
    );
    harness.wait_for_in_sync_replicas(1).await?;
    harness.wait_for_green().await?;

    let _stopped_primary = harness.stop_node(&primary)?;
    let promoted = harness.wait_for_primary_promotion(&primary).await?;
    assert_eq!(promoted.primary, replica);
    harness.wait_for_exact_documents(next_doc).await?;

    harness
        .put_document(&format!("doc-{next_doc}"), expected_document(next_doc))
        .await?;
    harness.wait_for_exact_documents(next_doc + 1).await?;
    Ok(())
}

#[tokio::test]
async fn peer_recovery_disabled_replica_is_not_promoted_and_primary_rejoin_restores_data()
-> Result<()> {
    let mut harness = RestartClusterHarness::start_with_peer_recovery_limit(Some(0)).await?;
    harness.create_index_with_shards(1, 0).await?;
    harness
        .wait_for_index_shards(INDEX_NAME, 1, 0, READY_TIMEOUT)
        .await?;

    let initial_state = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    let primary = routing_snapshot(&initial_state, INDEX_NAME)?[&0]
        .primary
        .clone();
    let mut primary_process = harness.stop_node(&primary)?;
    primary_process.config.max_concurrent_peer_recoveries = Some(2);
    harness.restart_node(primary_process).await?;

    harness.bulk_index_document_range(0, 20).await?;
    harness.refresh_index().await?;
    harness.flush_index_copies(1).await?;
    harness.bulk_index_document_range(20, 25).await?;
    harness.refresh_index().await?;

    let before_replica = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    assert_eq!(
        routing_snapshot(&before_replica, INDEX_NAME)?[&0].primary,
        primary
    );

    harness.update_replica_count(1).await?;
    let (_assigned_state, assigned) = harness.wait_for_out_of_sync_replica().await?;
    let replica = assigned.replicas[0].clone();
    assert_ne!(primary, replica);

    let cat_deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    loop {
        let (status, text) = harness.request_text("/_cat/shards?v").await?;
        if status == StatusCode::OK {
            let rows = text
                .lines()
                .skip(1)
                .filter_map(|line| {
                    let columns = line.split_whitespace().collect::<Vec<_>>();
                    (columns.first().copied() == Some(INDEX_NAME)).then_some(columns)
                })
                .collect::<Vec<_>>();
            let primary_started = rows.iter().any(|columns| {
                columns.get(2).copied() == Some("p") && columns.get(3).copied() == Some("STARTED")
            });
            let replica_initializing = rows.iter().any(|columns| {
                columns.get(2).copied() == Some("r")
                    && columns.get(3).copied() == Some("INITIALIZING")
                    && columns.get(5).copied() == Some(replica.as_str())
            });
            let replica_started = rows.iter().any(|columns| {
                columns.get(2).copied() == Some("r") && columns.get(3).copied() == Some("STARTED")
            });
            if rows.len() == 2 && primary_started && replica_initializing && !replica_started {
                break;
            }
        }

        if tokio::time::Instant::now() >= cat_deadline {
            bail!(
                "assigned out-of-sync replica did not remain INITIALIZING\n{}",
                harness.logs_summary()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (health_status, health) = harness
        .request_json(Method::GET, "/_cluster/health", None)
        .await?;
    assert_eq!(health_status, StatusCode::OK, "{health}");
    assert_eq!(health["status"], json!("yellow"));
    assert_eq!(health["unassigned_shards"], json!(1));

    harness
        .put_document("doc-25", expected_document(25))
        .await?;

    let stopped_primary = harness.stop_node(&primary)?;
    let unavailable_state = harness
        .wait_for_primary_removal_without_promotion(&primary, &replica)
        .await?;
    let unavailable_routing = routing_snapshot(&unavailable_state, INDEX_NAME)?;
    assert_eq!(unavailable_routing[&0].primary, primary);
    assert_eq!(
        unavailable_routing[&0].replicas.as_slice(),
        std::slice::from_ref(&replica)
    );
    assert!(unavailable_routing[&0].in_sync_replicas.is_empty());

    let (health_status, health) = harness
        .request_json(Method::GET, "/_cluster/health", None)
        .await?;
    assert_eq!(health_status, StatusCode::OK, "{health}");
    assert_eq!(health["status"], json!("red"));

    let (get_status, get_body) = harness.get_document("doc-0").await?;
    assert_eq!(get_status, StatusCode::INTERNAL_SERVER_ERROR, "{get_body}");
    assert_eq!(
        get_body["error"]["type"],
        json!("node_not_found_exception"),
        "{get_body}"
    );

    harness.restart_node(stopped_primary).await?;
    let rejoined_state = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    assert_eq!(
        routing_snapshot(&rejoined_state, INDEX_NAME)?[&0].primary,
        primary
    );
    harness.wait_for_exact_documents(26).await?;

    let (health_status, health) = harness
        .request_json(Method::GET, "/_cluster/health", None)
        .await?;
    assert_eq!(health_status, StatusCode::OK, "{health}");
    assert_eq!(health["status"], json!("yellow"));

    Ok(())
}

#[tokio::test]
async fn rejoining_stale_replica_is_recovered_before_primary_failover() -> Result<()> {
    let mut harness = RestartClusterHarness::start().await?;
    harness.create_index_with_shards(1, 2).await?;
    harness
        .wait_for_index_shards(INDEX_NAME, 3, 0, READY_TIMEOUT)
        .await?;
    harness.bulk_index_document_range(0, 20).await?;
    harness.refresh_index().await?;
    harness.flush_index_copies(3).await?;

    let before_loss = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    let routing_before = routing_snapshot(&before_loss, INDEX_NAME)?;
    let primary = routing_before[&0].primary.clone();
    let stale_replica = routing_before[&0].replicas[0].clone();
    let stopped_replica = harness.stop_node(&stale_replica)?;

    let removal_deadline = tokio::time::Instant::now() + FAILOVER_TIMEOUT;
    loop {
        let state = harness.wait_for_cluster_state(2, Some(INDEX_NAME)).await?;
        let routing = routing_snapshot(&state, INDEX_NAME)?;
        if routing[&0].replicas.len() == 1
            && routing[&0].in_sync_replicas.len() == 1
            && routing[&0].unassigned_replicas == 1
        {
            break;
        }
        if tokio::time::Instant::now() >= removal_deadline {
            bail!(
                "stopped replica was not removed from routing\n{}",
                harness.logs_summary()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    harness.bulk_index_document_range(20, 25).await?;
    harness.refresh_index().await?;
    let stale_config = stopped_replica.config.clone();
    harness.restart_node(stopped_replica).await?;
    harness.wait_for_in_sync_replicas(2).await?;
    harness.wait_for_green().await?;
    harness.refresh_index().await?;

    for doc_id in 0..25 {
        assert_eq!(
            get_local_document(&stale_config, &format!("doc-{doc_id}")).await?,
            Some(expected_document(doc_id)),
            "rejoined replica is missing doc-{doc_id}"
        );
    }

    let _stopped_primary = harness.stop_node(&primary)?;
    harness.wait_for_primary_promotion(&primary).await?;
    harness.wait_for_exact_documents(25).await?;
    harness
        .put_document("doc-25", expected_document(25))
        .await?;
    harness.wait_for_exact_documents(26).await?;
    Ok(())
}

#[tokio::test]
async fn mixed_role_node_loss_accounts_every_shard_and_preserves_acknowledged_data() -> Result<()> {
    let mut harness = RestartClusterHarness::start().await?;
    let index_uuid = harness.create_index(2).await?;
    harness
        .wait_for_index_shards(INDEX_NAME, 9, 0, READY_TIMEOUT)
        .await?;

    let cluster_state_before = harness.wait_for_cluster_state(3, Some(INDEX_NAME)).await?;
    let routing_before = routing_snapshot(&cluster_state_before, INDEX_NAME)?;
    assert_eq!(routing_before.len(), 3);
    assert_eq!(
        cluster_state_before["indices"][INDEX_NAME]["number_of_replicas"],
        json!(2)
    );
    assert_eq!(
        cluster_state_before["indices"][INDEX_NAME]["uuid"],
        json!(index_uuid)
    );
    for routing in routing_before.values() {
        assert_eq!(routing.replicas.len(), 2);
        assert_eq!(routing.in_sync_replicas.len(), 2);
        assert_eq!(routing.unassigned_replicas, 0);
    }

    let dead_node = cluster_state_before["master_node"]
        .as_str()
        .context("cluster state is missing the elected master")?
        .to_string();
    let promoted_shards = routing_before
        .iter()
        .filter_map(|(shard_id, routing)| (routing.primary == dead_node).then_some(*shard_id))
        .collect::<Vec<_>>();
    let replica_shards = routing_before
        .iter()
        .filter_map(|(shard_id, routing)| {
            routing
                .replicas
                .iter()
                .any(|node| node == &dead_node)
                .then_some(*shard_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        promoted_shards.len(),
        1,
        "each node must own one primary in the 3-shard round-robin routing"
    );
    assert_eq!(
        replica_shards.len(),
        2,
        "the failed node must be a replica for the other two shards"
    );
    let promoted_shard = promoted_shards[0];

    let mut expected_documents = BTreeMap::new();
    for shard_id in 0..3 {
        let doc_id = document_id_for_shard("acknowledged", shard_id);
        let source = json!({
            "title": format!("acknowledged shard {shard_id}"),
            "author": format!("author-{shard_id}"),
            "payload": format!("preserved-value-{shard_id}"),
            "n": shard_id,
        });
        harness.put_document(&doc_id, source.clone()).await?;
        expected_documents.insert(doc_id, source);
    }

    let deleted_doc_id = document_id_for_shard("acknowledged-delete", promoted_shard);
    harness
        .put_document(
            &deleted_doc_id,
            json!({
                "title": "delete before failover",
                "author": "delete-test",
                "payload": "must remain deleted",
                "n": 99,
            }),
        )
        .await?;
    harness.delete_document(&deleted_doc_id).await?;
    harness.refresh_index().await?;

    for (doc_id, expected_source) in &expected_documents {
        let (status, body) = harness.get_document(doc_id).await?;
        assert_eq!(status, StatusCode::OK, "get {doc_id}: {body}");
        assert_eq!(&body["_source"], expected_source, "get {doc_id}: {body}");
    }
    let (deleted_status, deleted_body) = harness.get_document(&deleted_doc_id).await?;
    assert_eq!(deleted_status, StatusCode::NOT_FOUND, "{deleted_body}");
    assert_eq!(deleted_body["found"], json!(false));

    let _stopped_node = harness.stop_node(&dead_node)?;
    let cluster_state_after = harness
        .wait_for_node_loss_accounting(INDEX_NAME, &dead_node)
        .await?;
    harness
        .wait_for_index_shards(INDEX_NAME, 6, 3, READY_TIMEOUT)
        .await?;

    assert_eq!(
        cluster_state_after["indices"][INDEX_NAME]["uuid"],
        json!(index_uuid)
    );
    assert_eq!(
        cluster_state_after["indices"][INDEX_NAME]["number_of_replicas"],
        json!(2)
    );
    let routing_after = routing_snapshot(&cluster_state_after, INDEX_NAME)?;
    let mut observed_promotions = 0;
    for (shard_id, before) in &routing_before {
        let after = &routing_after[shard_id];
        assert_ne!(after.primary, dead_node);
        assert!(!after.replicas.iter().any(|node| node == &dead_node));
        assert!(!after.in_sync_replicas.iter().any(|node| node == &dead_node));
        assert_eq!(after.replicas.len(), 1);
        assert_eq!(after.in_sync_replicas, after.replicas);
        assert_eq!(after.unassigned_replicas, 1);

        let expected_survivors = std::iter::once(&before.primary)
            .chain(before.replicas.iter())
            .filter(|node| node.as_str() != dead_node)
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_survivors = std::iter::once(&after.primary)
            .chain(after.replicas.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_survivors, expected_survivors,
            "shard {shard_id} must retain exactly its two surviving copies"
        );

        if before.primary == dead_node {
            observed_promotions += 1;
            assert!(
                before.replicas.contains(&after.primary),
                "shard {shard_id} primary must be promoted from a surviving replica"
            );
        } else {
            assert_eq!(
                after.primary, before.primary,
                "replica-only loss must not replace shard {shard_id}'s primary"
            );
        }
    }
    assert_eq!(observed_promotions, 1);

    let (health_status, health) = harness
        .request_json(Method::GET, "/_cluster/health", None)
        .await?;
    assert_eq!(health_status, StatusCode::OK, "{health}");
    assert_eq!(health["number_of_nodes"], json!(2));
    assert_eq!(health["status"], json!("yellow"));
    assert_eq!(health["unassigned_shards"], json!(3));

    for (doc_id, expected_source) in &expected_documents {
        let (status, body) = harness.get_document(doc_id).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "get after failover {doc_id}: {body}"
        );
        assert_eq!(
            &body["_source"], expected_source,
            "get after failover {doc_id}: {body}"
        );
    }
    let (deleted_status, deleted_body) = harness.get_document(&deleted_doc_id).await?;
    assert_eq!(deleted_status, StatusCode::NOT_FOUND, "{deleted_body}");
    assert_eq!(deleted_body["found"], json!(false));

    let post_failover_id = document_id_for_shard("post-promotion-write", promoted_shard);
    let post_failover_source = json!({
        "title": "write through promoted primary",
        "author": "failover-test",
        "payload": "post-promotion-value",
        "n": 100,
    });
    harness
        .put_document(&post_failover_id, post_failover_source.clone())
        .await?;
    harness.refresh_index().await?;
    let (status, body) = harness.get_document(&post_failover_id).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_shard"], json!(promoted_shard));
    assert_eq!(body["_source"], post_failover_source);

    Ok(())
}
