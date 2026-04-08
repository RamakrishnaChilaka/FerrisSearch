use anyhow::{Context, Result, bail};
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

const INDEX_NAME: &str = "restart-regression";
const CLUSTER_NAME: &str = "restart-regression-cluster";
const DOC_COUNT: usize = 12_000;
const BATCH_SIZE: usize = 500;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const DOC_BODY: &str = "restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload restart regression payload";

#[derive(Clone)]
struct NodeConfig {
    name: String,
    data_dir: PathBuf,
    log_path: PathBuf,
    http_port: u16,
    transport_port: u16,
    raft_node_id: u64,
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
    _temp_dir: TempDir,
    client: Client,
    seed_hosts: String,
    nodes: Vec<NodeProcess>,
}

impl RestartClusterHarness {
    async fn start() -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let node_configs: Vec<NodeConfig> = (1..=3)
            .map(|idx| NodeConfig {
                name: format!("node-{idx}"),
                data_dir: temp_dir.path().join(format!("node-{idx}")),
                log_path: temp_dir.path().join(format!("node-{idx}.log")),
                http_port: reserve_port(),
                transport_port: reserve_port(),
                raft_node_id: idx as u64,
            })
            .collect();

        let seed_hosts = node_configs
            .iter()
            .map(|cfg| format!("127.0.0.1:{}", cfg.transport_port))
            .collect::<Vec<_>>()
            .join(",");

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

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value)> {
        let url = format!("{}{}", self.nodes[0].config.base_url(), path);
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
        let url = format!("{}{}", self.nodes[0].config.base_url(), path);
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
                if nodes_len >= expected_nodes && index_ready {
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

    async fn create_index(&mut self) -> Result<String> {
        let (status, body) = self
            .request_json(
                Method::PUT,
                &format!("/{INDEX_NAME}"),
                Some(json!({
                    "settings": {
                        "number_of_shards": 3,
                        "number_of_replicas": 0,
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

    async fn bulk_index_documents(&self, doc_count: usize) -> Result<()> {
        for batch_start in (0..doc_count).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, doc_count);
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
            json!(3),
            "expected all shard copies to flush: {body}"
        );
        Ok(())
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

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    listener.local_addr().expect("port addr").port()
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
    let index_uuid = harness.create_index().await?;

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
