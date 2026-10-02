use anyhow::{Context, Result, bail};
use ferrissearch::cluster::ClusterManager;
use ferrissearch::cluster::state::ClusterState;
use ferrissearch::engine::{HotEngine, SearchEngine};
use ferrissearch::transport::TransportClient;
use ferrissearch::worker::WorkerPools;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use std::fs::{self, File};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::OwnedMutexGuard;

const INDEX: &str = "crash";
const READY_TIMEOUT: Duration = Duration::from_secs(60);

struct NodeProcess {
    name: String,
    base_url: String,
    log_path: PathBuf,
    child: Child,
}

impl NodeProcess {
    fn ensure_running(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "{} (PID {}) exited with {status}\n{}",
                self.name,
                self.child.id(),
                fs::read_to_string(&self.log_path)?
            );
        }
        Ok(())
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                if let Err(error) = self.child.kill() {
                    eprintln!("failed to kill test node {}: {error}", self.child.id());
                }
                if let Err(error) = self.child.wait() {
                    eprintln!("failed to reap test node {}: {error}", self.child.id());
                }
            }
        }
    }
}

struct Cluster {
    nodes: Vec<NodeProcess>,
    client: Client,
    _directory: TempDir,
    _guard: OwnedMutexGuard<()>,
}

fn process_test_lock() -> Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

impl Cluster {
    async fn start(node_count: usize, test_name: &str) -> Result<Self> {
        let guard = process_test_lock().lock_owned().await;
        let directory = tempfile::tempdir()?;
        let reservations = (0..node_count * 2)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let ports = reservations
            .iter()
            .map(|listener| listener.local_addr().map(|address| address.port()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let seeds = (0..node_count)
            .map(|i| format!("127.0.0.1:{}", ports[i * 2 + 1]))
            .collect::<Vec<_>>()
            .join(",");
        let log_dir = match std::env::var_os("FERRIS_CRASH_TEST_LOG_DIR") {
            Some(path) => PathBuf::from(path),
            None => directory.path().to_path_buf(),
        };
        fs::create_dir_all(&log_dir)?;
        drop(reservations);

        let mut cluster = Self {
            nodes: Vec::new(),
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
            _directory: directory,
            _guard: guard,
        };
        for i in 0..node_count {
            let name = format!("crash-node-{}", i + 1);
            let log_path = log_dir.join(format!("{test_name}-{}-{name}.log", std::process::id()));
            let stdout = File::create(&log_path)?;
            let stderr = stdout.try_clone()?;
            let mut command = Command::new(env!("CARGO_BIN_EXE_ferrissearch"));
            for (key, _) in std::env::vars() {
                if key.starts_with("FERRISSEARCH_") {
                    command.env_remove(key);
                }
            }
            let child = command
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .env("RUST_LOG", "info")
                .env("FERRISSEARCH_NODE_NAME", &name)
                .env("FERRISSEARCH_CLUSTER_NAME", "request-crash-regression")
                .env("FERRISSEARCH_HTTP_PORT", ports[i * 2].to_string())
                .env("FERRISSEARCH_TRANSPORT_PORT", ports[i * 2 + 1].to_string())
                .env("FERRISSEARCH_RAFT_NODE_ID", (i + 1).to_string())
                .env(
                    "FERRISSEARCH_DATA_DIR",
                    cluster._directory.path().join(&name),
                )
                .env("FERRISSEARCH_SEED_HOSTS", &seeds)
                .env("FERRISSEARCH_COLUMN_CACHE_SIZE_PERCENT", "0")
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .spawn()?;
            eprintln!(
                "started {name} PID {}, log {}",
                child.id(),
                log_path.display()
            );
            cluster.nodes.push(NodeProcess {
                name,
                base_url: format!("http://127.0.0.1:{}", ports[i * 2]),
                log_path,
                child,
            });
            cluster.wait_for_membership(i + 1).await?;
        }
        Ok(cluster)
    }

    async fn wait_for_membership(&mut self, count: usize) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        tokio::time::timeout(READY_TIMEOUT, async {
            loop {
                interval.tick().await;
                self.ensure_running()?;
                let mut ready = true;
                for node in &self.nodes {
                    let response = self
                        .client
                        .get(format!("{}/_cluster/state", node.base_url))
                        .send()
                        .await;
                    match response {
                        Ok(response) if response.status() == StatusCode::OK => {
                            let state: Value = response.json().await?;
                            ready &= state["nodes"].as_object().map(|nodes| nodes.len())
                                == Some(count)
                                && state["master_node"].as_str().is_some();
                        }
                        _ => ready = false,
                    }
                }
                if ready {
                    return Ok(());
                }
            }
        })
        .await
        .context("cluster membership readiness timed out")?
    }

    fn ensure_running(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            node.ensure_running()?;
        }
        Ok(())
    }

    async fn request(
        &mut self,
        node: usize,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value)> {
        self.ensure_running()?;
        let request = self
            .client
            .request(method, format!("{}{path}", self.nodes[node].base_url));
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        let response = request.send().await;
        self.ensure_running()?;
        let response = response.context("request to crash-regression node failed")?;
        let status = response.status();
        let text = response.text().await?;
        self.ensure_running()?;
        let value = serde_json::from_str(&text).with_context(|| format!("{status}: {text}"))?;
        Ok((status, value))
    }

    async fn seed(&mut self) -> Result<()> {
        self.seed_values(&[1, 2, 3]).await
    }

    async fn seed_values(&mut self, values: &[i64]) -> Result<()> {
        let (status, body) = self
            .request(
                0,
                Method::PUT,
                &format!("/{INDEX}"),
                Some(json!({
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "refresh_interval_ms": 60000
                    },
                    "mappings": {"properties": {
                        "title": {"type": "text"},
                        "tag": {"type": "keyword"},
                        "n": {"type": "integer"},
                        "f": {"type": "float"},
                        "d": {"type": "date"},
                        "embedding": {"type": "knn_vector", "dimension": 2}
                    }}
                })),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        for (position, &n) in values.iter().enumerate() {
            let (status, body) = self
                .request(
                    0,
                    Method::PUT,
                    &format!("/{INDEX}/_doc/{n}"),
                    Some(json!({
                        "title": format!("document {n}"),
                        "tag": format!("tag-{n}"),
                        "n": n,
                        "f": n as f64 + 0.5,
                        "d": format!("2026-01-{:02}T00:00:00Z", position + 1),
                        "embedding": [n as f32, 1.0]
                    })),
                )
                .await?;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
        let (status, body) = self
            .request(0, Method::POST, &format!("/{INDEX}/_refresh"), None)
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        self.normal_search_count(0, values.len()).await
    }

    async fn normal_search(&mut self, node: usize) -> Result<()> {
        self.normal_search_count(node, 3).await
    }

    async fn normal_search_count(&mut self, node: usize, count: usize) -> Result<()> {
        let (status, body) = self
            .request(
                node,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"query": {"match_all": {}}})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["hits"]["total"]["value"], count, "{body}");
        assert_eq!(body["hits"]["hits"].as_array().map(Vec::len), Some(count));
        assert_eq!(body["_shards"]["failed"], 0, "{body}");
        Ok(())
    }

    async fn assert_bad_query(&mut self, node: usize, query: Value, field: &str) -> Result<()> {
        let (status, body) = self
            .request(
                node,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"query": query})),
            )
            .await?;
        assert_bad_typed_value(status, &body, field);
        self.normal_search(node).await
    }

    async fn assert_search_values(
        &mut self,
        node: usize,
        query: Value,
        expected: &[i64],
    ) -> Result<()> {
        let (status, body) = self
            .request(
                node,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"query": query, "sort": [{"n": "asc"}]})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body["_shards"]["failed"], 0, "{body}");
        assert_eq!(body["hits"]["total"]["value"], expected.len(), "{body}");
        let values: Vec<_> = body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["_source"]["n"].as_i64().unwrap())
            .collect();
        assert_eq!(values, expected, "{query}: {body}");
        Ok(())
    }

    async fn assert_sql_values(
        &mut self,
        node: usize,
        predicate: &str,
        expected: &[i64],
    ) -> Result<()> {
        let query = format!("SELECT n FROM {INDEX} WHERE {predicate} ORDER BY n");
        for endpoint in ["_sql", "_sql/stream"] {
            self.ensure_running()?;
            let response = self
                .client
                .post(format!("{}/{INDEX}/{endpoint}", self.nodes[node].base_url))
                .json(&json!({"query": query}))
                .send()
                .await?;
            let status = response.status();
            let text = response.text().await?;
            self.ensure_running()?;
            assert_eq!(
                status,
                StatusCode::OK,
                "coordinator {node}, {endpoint}, {query}: {text}"
            );
            let rows: Vec<Value> = if endpoint == "_sql" {
                let body: Value = serde_json::from_str(&text)?;
                body["rows"].as_array().unwrap().clone()
            } else {
                let frames = text
                    .lines()
                    .map(serde_json::from_str::<Value>)
                    .collect::<serde_json::Result<Vec<_>>>()?;
                assert_eq!(frames[0]["type"], "meta", "{text}");
                assert!(
                    !frames.iter().any(|frame| frame["type"] == "error"),
                    "{text}"
                );
                frames
                    .iter()
                    .filter(|frame| frame["type"] == "rows")
                    .flat_map(|frame| frame["rows"].as_array().unwrap().iter().cloned())
                    .collect()
            };
            let values: Vec<_> = rows.iter().map(|row| row["n"].as_i64().unwrap()).collect();
            assert_eq!(
                values, expected,
                "coordinator {node}, {endpoint}, {query}: {text}"
            );
        }
        Ok(())
    }
}

fn assert_bad_typed_value(status: StatusCode, body: &Value, field: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "search_phase_execution_exception");
    assert_eq!(body["error"]["reason"], "all shards failed");
    let failures = body["error"]["failed_shards"].as_array().unwrap();
    assert_eq!(failures.len(), 1, "{body}");
    let reason = &failures[0]["reason"];
    assert_eq!(reason["type"], "query_shard_exception", "{body}");
    assert_eq!(reason["caused_by"]["type"], "parse_exception", "{body}");
    let text = reason["reason"].as_str().unwrap();
    assert!(text.contains(field) && text.contains("abc"), "{body}");
    assert!(
        reason["caused_by"]["reason"]
            .as_str()
            .is_some_and(|text| !text.is_empty()),
        "{body}"
    );
}

macro_rules! bad_value_test {
    ($name:ident, $field:literal, $kind:literal) => {
        #[tokio::test]
        async fn $name() -> Result<()> {
            let mut cluster = Cluster::start(1, stringify!($name)).await?;
            cluster.seed().await?;
            let value = match $kind {
                "range" => json!({"gte": "abc"}),
                "terms" => json!(["abc"]),
                _ => json!("abc"),
            };
            cluster
                .assert_bad_query(0, json!({$kind: {$field: value}}), $field)
                .await
        }
    };
}

bad_value_test!(integer_range_bad_value_returns_400, "n", "range");
bad_value_test!(float_range_bad_value_returns_400, "f", "range");
bad_value_test!(date_range_bad_value_returns_400, "d", "range");
bad_value_test!(integer_term_bad_value_returns_400, "n", "term");
bad_value_test!(float_term_bad_value_returns_400, "f", "term");
bad_value_test!(date_term_bad_value_returns_400, "d", "term");
bad_value_test!(integer_terms_bad_value_returns_400, "n", "terms");
bad_value_test!(float_terms_bad_value_returns_400, "f", "terms");
bad_value_test!(date_terms_bad_value_returns_400, "d", "terms");

#[tokio::test]
async fn sql_bad_typed_predicates_return_400() -> Result<()> {
    let mut cluster = Cluster::start(1, "sql_bad_typed_predicates").await?;
    cluster.seed().await?;
    for field in ["n", "f", "d"] {
        for predicate in [
            format!("{field} >= 'abc'"),
            format!("{field} = 'abc'"),
            format!("{field} IN ('abc')"),
        ] {
            for endpoint in ["_sql", "_sql/stream"] {
                let (status, body) = cluster
                    .request(
                        0,
                        Method::POST,
                        &format!("/{INDEX}/{endpoint}"),
                        Some(json!({"query": format!("SELECT title FROM {INDEX} WHERE {predicate}")})),
                    )
                    .await?;
                assert_bad_typed_value(status, &body, field);
                cluster.normal_search(0).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn residual_sql_bad_typed_predicates_return_400() -> Result<()> {
    let mut cluster = Cluster::start(1, "residual_sql_bad_typed_predicates").await?;
    cluster.seed().await?;
    for field in ["n", "f", "d"] {
        for endpoint in ["_sql", "_sql/stream"] {
            let query =
                format!("SELECT n FROM {INDEX} WHERE {field} >= 'abc' OR title LIKE '%document%'");
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    &format!("/{INDEX}/{endpoint}"),
                    Some(json!({"query": query})),
                )
                .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
            assert_eq!(body["error"]["type"], "query_shard_exception", "{body}");
            assert_eq!(
                body["error"]["caused_by"]["type"], "parse_exception",
                "{body}"
            );
            assert!(
                body["error"]["reason"].as_str().unwrap().contains("abc"),
                "{body}"
            );
            cluster.normal_search(0).await?;
        }
    }
    Ok(())
}

async fn assert_review_residual_sql(predicate: &str, expected: &[i64]) -> Result<()> {
    let mut cluster = Cluster::start(2, predicate).await?;
    cluster.seed_values(&[1, 2, 3, 4, 5]).await?;
    for node in 0..cluster.nodes.len() {
        cluster.assert_sql_values(node, predicate, expected).await?;
        cluster.normal_search_count(node, 5).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_residual_sql_fractional_gte_keeps_datafusion_results() -> Result<()> {
    assert_review_residual_sql("n >= 1.5 OR title LIKE '%zzz%'", &[2, 3, 4, 5]).await
}

#[tokio::test]
async fn review_residual_sql_fractional_gt_keeps_datafusion_results() -> Result<()> {
    assert_review_residual_sql("n > 1.5 OR title LIKE '%zzz%'", &[2, 3, 4, 5]).await
}

#[tokio::test]
async fn review_residual_sql_timestamps_keep_datafusion_results() -> Result<()> {
    for (predicate, expected) in [
        (
            "d >= '2026-01-03 00:00:00' OR title LIKE '%zzz%'",
            vec![3, 4, 5],
        ),
        (
            "d >= '2026-01-03 00:00:00.5' OR title LIKE '%zzz%'",
            vec![4, 5],
        ),
    ] {
        assert_review_residual_sql(predicate, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_residual_sql_unsigned_literal_keeps_datafusion_results() -> Result<()> {
    assert_review_residual_sql(
        "n < 18446744073709551615 OR title LIKE '%zzz%'",
        &[1, 2, 3, 4, 5],
    )
    .await
}

#[tokio::test]
async fn review_fractional_integer_terms_match_opensearch_results() -> Result<()> {
    let mut cluster = Cluster::start(2, "review_fractional_terms").await?;
    cluster.seed_values(&[1, 2, 3, 4, 5]).await?;
    for node in 0..cluster.nodes.len() {
        for value in [json!(1.5), json!(-1.5), json!("1.5"), json!("-1.5")] {
            cluster
                .assert_search_values(node, json!({"term": {"n": value}}), &[])
                .await?;
        }
        for (query, expected) in [
            (
                json!({"terms": {"n": [1, 1.5, "2", "2.5", 3.0]}}),
                vec![1, 2, 3],
            ),
            (json!({"terms": {"n": [1.5, -1.5]}}), vec![]),
            (json!({"term": {"n": 2.0}}), vec![2]),
        ] {
            cluster.assert_search_values(node, query, &expected).await?;
        }
        cluster.normal_search_count(node, 5).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_fractional_integer_ranges_match_opensearch_results() -> Result<()> {
    let values = [-3, -2, -1, 0, 1, 2, 3, 4, 5];
    let mut cluster = Cluster::start(2, "review_fractional_ranges").await?;
    cluster.seed_values(&values).await?;
    for node in 0..cluster.nodes.len() {
        for (bound, lower, upper) in [(1.5, 2, 1), (-1.5, -1, -2), (0.5, 1, 0), (-0.5, 0, -1)] {
            for (operator, expected) in [
                (
                    "gte",
                    values
                        .into_iter()
                        .filter(|n| *n >= lower)
                        .collect::<Vec<_>>(),
                ),
                ("gt", values.into_iter().filter(|n| *n >= lower).collect()),
                ("lte", values.into_iter().filter(|n| *n <= upper).collect()),
                ("lt", values.into_iter().filter(|n| *n <= upper).collect()),
            ] {
                for value in [json!(bound), json!(bound.to_string())] {
                    cluster
                        .assert_search_values(
                            node,
                            json!({"range": {"n": {operator: value}}}),
                            &expected,
                        )
                        .await?;
                }
            }
        }
        for (condition, expected) in [
            (json!({"gte": 1.5, "lte": 3.5}), vec![2, 3]),
            (json!({"gte": 3.5, "lte": 1.5}), vec![]),
            (json!({"gt": 1.0, "lt": 3.0}), vec![2]),
            (json!({"gt": -2.0, "lt": 0.0}), vec![-1]),
        ] {
            cluster
                .assert_search_values(node, json!({"range": {"n": condition}}), &expected)
                .await?;
        }
        cluster.normal_search_count(node, values.len()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_fractional_sql_pushdown_and_residual_results_agree() -> Result<()> {
    let mut cluster = Cluster::start(2, "review_fractional_sql").await?;
    cluster.seed_values(&[-3, -2, -1, 0, 1, 2, 3, 4, 5]).await?;
    for node in 0..cluster.nodes.len() {
        for (predicate, expected) in [
            ("n >= 1.5", vec![2, 3, 4, 5]),
            ("n > 1.5", vec![2, 3, 4, 5]),
            ("n <= -1.5", vec![-3, -2]),
            ("n < -1.5", vec![-3, -2]),
            ("n = 1.5", vec![]),
            ("n = -1.5", vec![]),
            ("n IN (1, 2.5)", vec![1]),
            ("n IN (-1, -2.5)", vec![-1]),
            ("n BETWEEN 1.5 AND 3.5", vec![2, 3]),
            ("n BETWEEN -2.5 AND -0.5", vec![-2, -1]),
        ] {
            for predicate in [
                predicate.to_string(),
                format!("({predicate}) OR title LIKE '%zzz%'"),
            ] {
                cluster
                    .assert_sql_values(node, &predicate, &expected)
                    .await?;
            }
        }
        cluster.normal_search_count(node, 9).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_fractional_date_epoch_millis_preserve_truncation() -> Result<()> {
    let mut cluster = Cluster::start(2, "review_fractional_dates").await?;
    cluster.seed_values(&[1, 2, 3, 4, 5]).await?;
    for node in 0..cluster.nodes.len() {
        for value in [json!(1_767_398_400_000.5), json!("1767398400000.5")] {
            for (query, expected) in [
                (json!({"term": {"d": value}}), vec![3]),
                (json!({"terms": {"d": [value]}}), vec![3]),
                (json!({"range": {"d": {"gte": value}}}), vec![3, 4, 5]),
            ] {
                cluster.assert_search_values(node, query, &expected).await?;
            }
        }
        cluster.normal_search_count(node, 5).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_empty_range_is_a_client_error_locally_and_remotely() -> Result<()> {
    let mut cluster = Cluster::start(2, "review_empty_range").await?;
    cluster.seed().await?;
    let (_, state) = cluster
        .request(0, Method::GET, "/_cluster/state", None)
        .await?;
    let primary = state["indices"][INDEX]["shard_routing"]["0"]["primary"]
        .as_str()
        .context("missing primary routing")?;
    for node in 0..cluster.nodes.len() {
        let (status, body) = cluster
            .request(
                node,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"query": {"range": {"n": {}}}})),
            )
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        let failures = body["error"]["failed_shards"].as_array().unwrap();
        assert_eq!(failures.len(), 1, "{body}");
        assert_eq!(failures[0]["node"], primary, "{body}");
        assert_eq!(
            failures[0]["reason"]["type"], "query_shard_exception",
            "{body}"
        );
        let reason = failures[0]["reason"]["reason"].as_str().unwrap();
        assert!(reason.contains('n') && reason.contains("bound"), "{body}");
        cluster.normal_search(node).await?;
    }
    Ok(())
}

#[tokio::test]
async fn review_invalid_knn_dimensions_are_client_errors_on_both_coordinators() -> Result<()> {
    let mut cluster = Cluster::start(2, "review_knn_dimensions").await?;
    cluster.seed().await?;
    let (_, state) = cluster
        .request(0, Method::GET, "/_cluster/state", None)
        .await?;
    let primary = state["indices"][INDEX]["shard_routing"]["0"]["primary"]
        .as_str()
        .context("missing primary routing")?;
    for node in 0..cluster.nodes.len() {
        for vector in [json!([1.0, 1.0, 1.0]), json!([])] {
            for k in [0, 3, usize::MAX] {
                let (status, body) = cluster
                    .request(
                        node,
                        Method::POST,
                        &format!("/{INDEX}/_search"),
                        Some(json!({"knn": {"embedding": {"vector": vector, "k": k}}})),
                    )
                    .await?;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
                assert_eq!(body["error"]["type"], "search_phase_execution_exception");
                let failures = body["error"]["failed_shards"].as_array().unwrap();
                assert_eq!(failures.len(), 1, "{body}");
                assert_eq!(failures[0]["node"], primary, "{body}");
                let reason = failures[0]["reason"]["reason"].as_str().unwrap();
                assert!(
                    reason.contains("dimension") && reason.contains("expected 2"),
                    "{body}"
                );
                cluster.normal_search(node).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn review_empty_remote_store_rejects_empty_ranges() -> Result<()> {
    let mut cluster = Cluster::start(1, "review_empty_remote_range").await?;
    cluster.seed().await?;
    let (status, body) = cluster
        .request(
            0,
            Method::PUT,
            "/empty",
            Some(json!({
                "engine": "remote_store",
                "mappings": {"properties": {"n": {"type": "integer"}}}
            })),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = cluster
        .request(
            0,
            Method::POST,
            "/empty/_search",
            Some(json!({"query": {"range": {"n": {}}}})),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "query_shard_exception", "{body}");
    assert!(
        body["error"]["reason"].as_str().unwrap().contains("bound"),
        "{body}"
    );
    cluster.normal_search(0).await
}

#[tokio::test]
async fn review_integer_extrema_and_json_precision_remain_exact() -> Result<()> {
    let mut cluster = Cluster::start(1, "review_integer_precision").await?;
    let values = [
        i64::MIN,
        0,
        1,
        9_007_199_254_740_992,
        9_007_199_254_740_993,
        i64::MAX,
    ];
    cluster.seed_values(&values).await?;
    for n in values {
        for value in [json!(n), json!(n.to_string())] {
            cluster
                .assert_search_values(0, json!({"term": {"n": value}}), &[n])
                .await?;
        }
    }
    for (condition, expected) in [
        (json!({"gt": i64::MAX}), vec![]),
        (json!({"lt": i64::MIN}), vec![]),
        (json!({"gte": i64::MAX}), vec![i64::MAX]),
        (json!({"lte": i64::MIN}), vec![i64::MIN]),
        (
            json!({"gt": 9_007_199_254_740_992i64, "lt": i64::MAX}),
            vec![9_007_199_254_740_993],
        ),
    ] {
        cluster
            .assert_search_values(0, json!({"range": {"n": condition}}), &expected)
            .await?;
    }
    cluster.normal_search_count(0, values.len()).await
}

#[tokio::test]
async fn sql_null_literals_retain_three_valued_predicate_semantics() -> Result<()> {
    let mut cluster = Cluster::start(1, "sql_null_literals").await?;
    cluster.seed().await?;
    for (predicate, expected) in [
        ("n = NULL", vec![]),
        ("n >= NULL", vec![]),
        ("n BETWEEN NULL AND 3", vec![]),
        ("n IN (NULL, 1)", vec![1]),
        ("n = NULL OR n = 2", vec![2]),
    ] {
        let query = format!("SELECT n FROM {INDEX} WHERE {predicate} ORDER BY n");
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_sql"),
                Some(json!({"query": query})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        let values: Vec<_> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["n"].as_i64().unwrap())
            .collect();
        assert_eq!(values, expected, "{body}");
        cluster.normal_search(0).await?;
    }
    Ok(())
}

async fn assert_huge_sql_limit(limit: &str, test_name: &str) -> Result<()> {
    let mut cluster = Cluster::start(1, test_name).await?;
    cluster.seed().await?;
    for query in [
        format!("SELECT title FROM {INDEX} LIMIT {limit}"),
        format!("SELECT title FROM {INDEX} ORDER BY n LIMIT {limit}"),
        format!("SELECT title FROM {INDEX} ORDER BY f DESC LIMIT {limit}"),
        format!("SELECT _id, _score FROM {INDEX} LIMIT {limit}"),
        format!("SELECT * FROM {INDEX} LIMIT {limit}"),
    ] {
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_sql"),
                Some(json!({"query": query})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body["rows"].as_array().map(Vec::len), Some(3), "{body}");
        let column = if query.contains("SELECT _id") {
            "_id"
        } else {
            "title"
        };
        let mut values: Vec<_> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row[column].as_str().unwrap())
            .collect();
        values.sort_unstable();
        let expected = if column == "_id" {
            vec!["1", "2", "3"]
        } else {
            vec!["document 1", "document 2", "document 3"]
        };
        assert_eq!(values, expected, "{body}");
        assert_eq!(body["truncated"], false, "{body}");
        cluster.normal_search(0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn sql_capacity_overflow_limit_returns_all_rows() -> Result<()> {
    assert_huge_sql_limit("4611686018427387904", "sql_capacity_overflow_limit").await
}

#[tokio::test]
async fn sql_multiply_overflow_limit_returns_all_rows() -> Result<()> {
    assert_huge_sql_limit("18446744073709551615", "sql_multiply_overflow_limit").await
}

#[tokio::test]
async fn grouped_sql_huge_and_zero_windows_preserve_results() -> Result<()> {
    let mut cluster = Cluster::start(1, "grouped_sql_windows").await?;
    cluster.seed().await?;
    for (suffix, expected) in [
        ("LIMIT 18446744073709551615", 3),
        ("LIMIT 18446744073709551615 OFFSET 1", 2),
        ("LIMIT 1 OFFSET 18446744073709551615", 0),
        ("LIMIT 0", 0),
    ] {
        let query =
            format!("SELECT tag, count(*) AS c FROM {INDEX} GROUP BY tag ORDER BY c DESC {suffix}");
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_sql"),
                Some(json!({"query": query})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(
            body["rows"].as_array().map(Vec::len),
            Some(expected),
            "{body}"
        );
        cluster.normal_search(0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn huge_search_windows_and_aggregation_sizes_do_not_crash() -> Result<()> {
    let mut cluster = Cluster::start(1, "huge_search_windows").await?;
    cluster.seed().await?;
    for (from, size) in [(0, usize::MAX), (usize::MAX, 0), (usize::MAX, 1)] {
        for sort in [json!([]), json!([{"n": "asc"}])] {
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    &format!("/{INDEX}/_search"),
                    Some(json!({"from": from, "size": size, "sort": sort})),
                )
                .await?;
            assert!(
                matches!(status, StatusCode::OK | StatusCode::BAD_REQUEST),
                "{body}"
            );
            if status == StatusCode::OK {
                assert_eq!(body["hits"]["total"]["value"], 3, "{body}");
                assert_eq!(
                    body["hits"]["hits"].as_array().map(Vec::len),
                    Some(if from == 0 { 3 } else { 0 }),
                    "{body}"
                );
            }
            cluster.normal_search(0).await?;
        }
    }
    let (status, body) = cluster
        .request(
            0,
            Method::POST,
            &format!("/{INDEX}/_search"),
            Some(json!({"size": 0, "aggs": {"tags": {"terms": {"field": "tag", "size": usize::MAX}}}})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["aggregations"]["tags"]["buckets"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );
    cluster.normal_search(0).await
}

#[tokio::test]
async fn huge_sort_width_is_rejected_before_cursor_expansion() -> Result<()> {
    let mut cluster = Cluster::start(1, "huge_sort_width").await?;
    cluster.seed().await?;
    for width in [64, 65] {
        for cursor in [false, true] {
            let mut request = json!({"sort": vec![json!({"n": "asc"}); width]});
            if cursor {
                request["search_after"] = json!(vec![0; width]);
            }
            let (status, body) = cluster
                .request(0, Method::POST, &format!("/{INDEX}/_search"), Some(request))
                .await?;
            if width == 64 {
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(body["hits"]["total"]["value"], 3, "{body}");
                assert_eq!(body["hits"]["hits"].as_array().map(Vec::len), Some(3));
            } else {
                assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
                assert!(
                    body["error"]["reason"].as_str().unwrap().contains("64"),
                    "{body}"
                );
            }
            cluster.normal_search(0).await?;
        }
    }
    let order = vec!["n"; 65].join(", ");
    let (status, body) = cluster
        .request(
            0,
            Method::POST,
            &format!("/{INDEX}/_sql"),
            Some(json!({"query": format!("SELECT n FROM {INDEX} ORDER BY {order} LIMIT 1")})),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["error"]["reason"].as_str().unwrap().contains("64"));
    cluster.normal_search(0).await
}

async fn assert_huge_knn(filtered: bool, test_name: &str) -> Result<()> {
    let mut cluster = Cluster::start(1, test_name).await?;
    cluster.seed().await?;
    let mut params = json!({"vector": [1.0, 1.0], "k": usize::MAX});
    if filtered {
        params["filter"] = json!({"range": {"n": {"gte": 1}}});
    }
    let (status, body) = cluster
        .request(
            0,
            Method::POST,
            &format!("/{INDEX}/_search"),
            Some(json!({"size": 10, "knn": {"embedding": params}})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_shards"]["failed"], 0, "{body}");
    let hits = body["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3, "{body}");
    assert!(
        hits.iter().any(|hit| hit["_knn_field"] == "embedding"),
        "kNN must not silently fail and return only text hits: {body}"
    );
    cluster.normal_search(0).await
}

#[tokio::test]
async fn huge_knn_k_returns_available_neighbors() -> Result<()> {
    assert_huge_knn(false, "huge_knn_k").await
}

#[tokio::test]
async fn huge_filtered_knn_k_returns_available_neighbors() -> Result<()> {
    assert_huge_knn(true, "huge_filtered_knn_k").await
}

#[tokio::test]
async fn forwarded_bad_numeric_bound_returns_400_and_both_nodes_survive() -> Result<()> {
    let mut cluster = Cluster::start(2, "forwarded_bad_bound").await?;
    cluster.seed().await?;
    let (_, state) = cluster
        .request(0, Method::GET, "/_cluster/state", None)
        .await?;
    let primary = state["indices"][INDEX]["shard_routing"]["0"]["primary"]
        .as_str()
        .context("missing primary routing")?
        .to_string();
    let coordinator = cluster
        .nodes
        .iter()
        .position(|node| node.name != primary)
        .context("no remote coordinator")?;
    let snapshot: ClusterState = serde_json::from_value(state)?;
    let target = snapshot.nodes[&primary].clone();
    let manager = Arc::new(ClusterManager::new(snapshot.cluster_name.clone()));
    manager.update_state(snapshot);
    let transport = TransportClient::new().with_cluster_manager(manager);
    let wide_req = serde_json::from_value(json!({"sort": vec![json!({"n": "asc"}); 65]}))?;
    let error = transport
        .forward_search_dsl_to_shard(&target, INDEX, 0, &wide_req)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<tonic::Status>().unwrap().code(),
        tonic::Code::InvalidArgument,
        "{error:#}"
    );
    for field in ["n", "f", "d"] {
        for query in [
            json!({"range": {field: {"gte": "abc"}}}),
            json!({"term": {field: "abc"}}),
            json!({"terms": {field: ["abc"]}}),
        ] {
            let (status, body) = cluster
                .request(
                    coordinator,
                    Method::POST,
                    &format!("/{INDEX}/_search"),
                    Some(json!({"query": query})),
                )
                .await?;
            assert_bad_typed_value(status, &body, field);
            assert_eq!(body["error"]["failed_shards"][0]["node"], primary);

            let req = serde_json::from_value(json!({"query": query, "size": usize::MAX}))?;
            let columns = vec![field.to_string()];
            let unary = transport
                .forward_sql_batch_to_shard(&target, INDEX, 0, &req, &columns, false, false)
                .await
                .map(|_| ());
            let streamed = transport
                .open_sql_batch_stream_to_shard(&target, INDEX, 0, &req, &columns, false, false, 0)
                .await
                .map(|_| ());
            for result in [unary, streamed] {
                let error = result.unwrap_err();
                let status = error
                    .downcast_ref::<tonic::Status>()
                    .context("query error lost its transport status")?;
                assert_eq!(status.code(), tonic::Code::InvalidArgument, "{error:#}");
                let reason: Value = serde_json::from_slice(status.details())?;
                assert_eq!(reason["type"], "query_shard_exception", "{reason}");
                assert_eq!(reason["caused_by"]["type"], "parse_exception", "{reason}");
                let text = reason["reason"].as_str().unwrap();
                assert!(text.contains(field) && text.contains("abc"), "{reason}");
            }
        }
    }
    for node in 0..cluster.nodes.len() {
        cluster.normal_search(node).await?;
    }
    Ok(())
}

#[tokio::test]
async fn huge_sql_limit_streams_all_rows_through_a_remote_coordinator() -> Result<()> {
    let mut cluster = Cluster::start(2, "huge_sql_stream").await?;
    cluster.seed().await?;
    let (_, state) = cluster
        .request(0, Method::GET, "/_cluster/state", None)
        .await?;
    let primary = state["indices"][INDEX]["shard_routing"]["0"]["primary"]
        .as_str()
        .context("missing primary routing")?;
    let coordinator = cluster
        .nodes
        .iter()
        .position(|node| node.name != primary)
        .unwrap();
    for projection in ["title", "n"] {
        let response = cluster.client
            .post(format!("{}/{INDEX}/_sql/stream", cluster.nodes[coordinator].base_url))
            .json(&json!({"query": format!("SELECT {projection} FROM {INDEX} ORDER BY n LIMIT 18446744073709551615")}))
            .send().await?;
        let status = response.status();
        let text = response.text().await?;
        cluster.ensure_running()?;
        assert_eq!(status, StatusCode::OK, "{text}");
        let frames = text
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<serde_json::Result<Vec<_>>>()?;
        assert_eq!(frames[0]["type"], "meta", "{text}");
        assert!(
            !frames.iter().any(|frame| frame["type"] == "error"),
            "{text}"
        );
        let rows: Vec<_> = frames
            .iter()
            .filter(|frame| frame["type"] == "rows")
            .flat_map(|frame| frame["rows"].as_array().unwrap())
            .collect();
        assert_eq!(rows.len(), 3, "{text}");
        for (position, row) in rows.iter().enumerate() {
            if projection == "title" {
                assert_eq!(row["title"], format!("document {}", position + 1));
            } else {
                assert_eq!(row["n"], position + 1);
            }
        }
        for node in 0..cluster.nodes.len() {
            cluster.normal_search(node).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn additional_sql_limit_offset_arithmetic_preserves_rows() -> Result<()> {
    let mut cluster = Cluster::start(1, "sql_limit_offset_arithmetic").await?;
    cluster.seed().await?;
    for (suffix, expected) in [
        ("LIMIT 18446744073709551615 OFFSET 1", 2),
        ("LIMIT 1 OFFSET 18446744073709551615", 0),
        ("LIMIT 0", 0),
    ] {
        for projection in ["title", "*"] {
            let query = format!("SELECT {projection} FROM {INDEX} {suffix}");
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    &format!("/{INDEX}/_sql"),
                    Some(json!({"query": query})),
                )
                .await?;
            assert_eq!(status, StatusCode::OK, "{query}: {body}");
            assert_eq!(
                body["rows"].as_array().map(Vec::len),
                Some(expected),
                "{body}"
            );
            cluster.normal_search(0).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn additional_bad_numeric_search_after_returns_400() -> Result<()> {
    let mut cluster = Cluster::start(1, "bad_numeric_search_after").await?;
    cluster.seed().await?;
    for field in ["n", "f", "d"] {
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"sort": [{field: "asc"}], "search_after": ["abc"]})),
            )
            .await?;
        assert_bad_typed_value(status, &body, field);
        cluster.normal_search(0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn additional_bad_knn_filter_fails_the_shard_locally_and_remotely() -> Result<()> {
    let mut cluster = Cluster::start(2, "bad_knn_filter").await?;
    cluster.seed().await?;
    for node in 0..cluster.nodes.len() {
        let (status, body) = cluster
            .request(
                node,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({
                    "knn": {"embedding": {
                        "vector": [1.0, 1.0],
                        "k": 3,
                        "filter": {"term": {"n": "abc"}}
                    }}
                })),
            )
            .await?;
        assert_bad_typed_value(status, &body, "n");
        cluster.normal_search(node).await?;
    }
    Ok(())
}

#[tokio::test]
async fn additional_empty_remote_store_validates_typed_values() -> Result<()> {
    let mut cluster = Cluster::start(1, "empty_remote_store_typed_values").await?;
    cluster.seed().await?;
    let (status, body) = cluster
        .request(
            0,
            Method::PUT,
            "/empty",
            Some(json!({
                "engine": "remote_store",
                "mappings": {"properties": {
                    "n": {"type": "integer"},
                    "f": {"type": "float"},
                    "d": {"type": "date"}
                }}
            })),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    for field in ["n", "f", "d"] {
        for query in [
            json!({"range": {field: {"gte": "abc"}}}),
            json!({"term": {field: "abc"}}),
            json!({"terms": {field: ["abc"]}}),
        ] {
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    "/empty/_search",
                    Some(json!({"query": query})),
                )
                .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
            assert_eq!(body["error"]["type"], "query_shard_exception", "{body}");
            assert_eq!(body["error"]["caused_by"]["type"], "parse_exception");
            let reason = body["error"]["reason"].as_str().unwrap();
            assert!(reason.contains(field) && reason.contains("abc"), "{body}");
            cluster.normal_search(0).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn additional_zero_grouped_collector_limit_returns_empty_buckets() -> Result<()> {
    let mut cluster = Cluster::start(1, "zero_grouped_collector_limit").await?;
    cluster.seed().await?;
    for query in [
        json!({"match_all": {}}),
        json!({"range": {"n": {"gte": 1}}}),
    ] {
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({
                    "query": query,
                    "size": 0,
                    "aggs": {"groups": {"grouped_metrics": {
                        "group_by": ["tag"],
                        "metrics": [{"output_name": "c", "function": "count"}],
                        "shard_top_k": {
                            "limit": 0,
                            "sort_by": "c",
                            "sort_function": "count",
                            "descending": true
                        }
                    }}}
                })),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["hits"]["total"]["value"], 3);
        assert_eq!(
            body["aggregations"]["groups"]["buckets"]
                .as_array()
                .map(Vec::len),
            Some(0),
            "{body}"
        );
        cluster.normal_search(0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn additional_invalid_numeric_representations_return_400() -> Result<()> {
    let mut cluster = Cluster::start(1, "invalid_numeric_representations").await?;
    cluster.seed().await?;
    for (field, values) in [
        (
            "n",
            vec![
                json!(u64::MAX),
                json!("9223372036854775808"),
                json!(1.5),
                json!(true),
                json!([]),
            ],
        ),
        (
            "f",
            vec![
                json!("NaN"),
                json!("inf"),
                json!("1e999"),
                json!(true),
                json!({}),
            ],
        ),
        (
            "d",
            vec![json!(u64::MAX), json!("not-a-date"), json!(true), json!([])],
        ),
    ] {
        for value in values {
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    &format!("/{INDEX}/_search"),
                    Some(json!({"query": {"term": {field: value}}})),
                )
                .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {value}: {body}");
            assert_eq!(
                body["error"]["failed_shards"][0]["reason"]["type"],
                "query_shard_exception"
            );
            let reason = body["error"]["failed_shards"][0]["reason"]["reason"]
                .as_str()
                .unwrap();
            assert!(
                reason.contains(field) && reason.contains(&value.to_string()),
                "{body}"
            );
            cluster.normal_search(0).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn additional_valid_typed_terms_preserve_exact_integer_identity() -> Result<()> {
    let mut cluster = Cluster::start(1, "valid_typed_terms").await?;
    cluster.seed().await?;
    for (query, id) in [
        (json!({"term": {"n": "2"}}), "2"),
        (json!({"terms": {"n": ["2"]}}), "2"),
        (json!({"term": {"f": "2.5"}}), "2"),
        (json!({"terms": {"f": ["2.5"]}}), "2"),
        (json!({"term": {"d": "2026-01-02T00:00:00Z"}}), "2"),
        (json!({"terms": {"d": ["2026-01-02T00:00:00Z"]}}), "2"),
    ] {
        let (status, body) = cluster
            .request(
                0,
                Method::POST,
                &format!("/{INDEX}/_search"),
                Some(json!({"query": query})),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body["hits"]["total"]["value"], 1, "{body}");
        assert_eq!(body["hits"]["hits"][0]["_id"], id);
    }
    for (id, n) in [
        ("large-even", 9_007_199_254_740_992i64),
        ("large-odd", 9_007_199_254_740_993i64),
    ] {
        let (status, body) = cluster
            .request(
                0,
                Method::PUT,
                &format!("/{INDEX}/_doc/{id}"),
                Some(json!({"n": n})),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    let (status, body) = cluster
        .request(0, Method::POST, &format!("/{INDEX}/_refresh"), None)
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    for value in [json!(9_007_199_254_740_993i64), json!("9007199254740993")] {
        for query in [
            json!({"term": {"n": value}}),
            json!({"terms": {"n": [value]}}),
            json!({"range": {"n": {"gte": value, "lte": value}}}),
        ] {
            let (status, body) = cluster
                .request(
                    0,
                    Method::POST,
                    &format!("/{INDEX}/_search"),
                    Some(json!({"query": query})),
                )
                .await?;
            assert_eq!(status, StatusCode::OK, "{query}: {body}");
            assert_eq!(body["hits"]["total"]["value"], 1, "{body}");
            assert_eq!(body["hits"]["hits"][0]["_id"], "large-odd");
        }
    }
    Ok(())
}

fn worker_probe(kind: &str) -> Result<Output> {
    Command::new(std::env::current_exe()?)
        .args(["--exact", "worker_panic_probe", "--ignored", "--nocapture"])
        .env("FERRIS_CRASH_WORKER_PROBE", kind)
        .output()
        .context("spawn crash-isolated worker probe")
}

#[test]
fn search_worker_contains_panics_and_keeps_serving() -> Result<()> {
    let output = worker_probe("search")?;
    assert!(
        output.status.success(),
        "search worker probe exited with {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn additional_huge_sql_stream_batch_size_is_bounded() -> Result<()> {
    let output = worker_probe("batch")?;
    assert!(
        output.status.success(),
        "SQL batch probe exited with {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn write_worker_panic_is_explicit_and_fail_stop() -> Result<()> {
    use std::os::unix::process::ExitStatusExt;

    let output = worker_probe("write")?;
    assert_eq!(output.status.signal(), Some(6), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("write worker task panicked") && stderr.contains("write panic marker"),
        "write panic must identify the operation before aborting: {stderr}"
    );
    Ok(())
}

#[test]
#[ignore = "subprocess-only panic probe; the parent observes any process abort"]
fn worker_panic_probe() -> Result<()> {
    let kind = std::env::var("FERRIS_CRASH_WORKER_PROBE")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let pools = WorkerPools::new(2, 1);
        if kind == "batch" {
            let directory = tempfile::tempdir()?;
            let engine = HotEngine::new(directory.path(), Duration::from_secs(60))?;
            engine.add_document("one", json!({"body": "one"}))?;
            engine.refresh()?;
            let req = serde_json::from_value(json!({"size": usize::MAX}))?;
            pools
                .spawn_search(move || -> Result<()> {
                    let mut handle =
                        engine.sql_streaming_batch_handle(&req, &[], true, false, usize::MAX)?;
                    assert_eq!(handle.next_batch()?.unwrap().num_rows(), 1);
                    assert!(handle.next_batch()?.is_none());
                    Ok(())
                })
                .await??;
            return Ok(());
        }
        if kind == "write" {
            pools.spawn_write(|| panic!("write panic marker")).await?;
            bail!("a panicking write must fail-stop");
        }
        let error = pools
            .spawn_search(|| panic!("search panic marker"))
            .await
            .unwrap_err();
        let reason = error.to_string();
        assert!(reason.contains("search") && reason.contains("search panic marker"));
        let error = pools
            .spawn_search(|| std::panic::panic_any(String::from("owned panic marker")))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("owned panic marker"));
        let error = pools
            .spawn_search(|| std::panic::panic_any(7u8))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-string panic payload"));
        let results = futures::future::join_all((0..12).map(|n| {
            pools.spawn_search(move || {
                if n % 2 == 0 {
                    panic!("concurrent panic {n}");
                }
                n
            })
        }))
        .await;
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 6);
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 6);
        for n in 0..12 {
            assert_eq!(pools.spawn_search(move || n).await?, n);
        }
        assert_eq!(pools.spawn_write(|| 42).await?, 42);
        Ok(())
    })
}
