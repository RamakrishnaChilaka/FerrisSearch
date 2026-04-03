use anyhow::Result;
use ferrissearch::api::{AppState, create_router};
use ferrissearch::cluster::ClusterManager;
use ferrissearch::cluster::state::{ClusterState, NodeInfo, NodeRole};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::server::create_transport_service;
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
    http_handle: JoinHandle<()>,
    transport_handle: JoinHandle<()>,
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
            raft_node_id: 0,
        };

        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(local_node);
        cluster_state.master_node = Some("node-1".into());

        let manager = ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);

        let shard_manager = Arc::new(ShardManager::new(temp_dir.path(), Duration::from_secs(60)));
        let cluster_manager = Arc::new(manager);
        let transport_client = TransportClient::new();
        let app_state = AppState {
            cluster_manager: cluster_manager.clone(),
            shard_manager: shard_manager.clone(),
            transport_client: transport_client.clone(),
            local_node_id: "node-1".into(),
            raft: None,
            worker_pools: ferrissearch::worker::WorkerPools::new(2, 2),
            sql_group_by_scan_limit: 1_000_000,
        };

        let transport_service = create_transport_service(
            cluster_manager,
            shard_manager,
            transport_client,
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
            http_handle,
            transport_handle,
        };

        harness.wait_until_ready().await?;
        Ok(harness)
    }

    async fn wait_until_ready(&self) -> Result<()> {
        for _ in 0..50 {
            if let Ok(response) = self.client.get(format!("{}/", self.base_url)).send().await
                && response.status() == StatusCode::OK
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("test HTTP server did not become ready in time");
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

impl Drop for RestTestHarness {
    fn drop(&mut self) {
        self.http_handle.abort();
        self.transport_handle.abort();
    }
}

async fn create_products_index(harness: &RestTestHarness) -> Result<()> {
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
            .put_json(&format!("/products/_doc/{}?refresh=true", doc_id), payload)
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
    assert_eq!(transfer_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        transfer_body["error"]["type"],
        json!("raft_not_enabled_exception")
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
    assert_eq!(body["truncated"], json!(false));

    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2, "should have 2 groups: apple and samsung");

    Ok(())
}
