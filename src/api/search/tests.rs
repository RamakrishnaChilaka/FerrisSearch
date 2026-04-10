use super::*;
use crate::cluster::state::{
    ClusterState, FieldMapping, FieldType, IndexMetadata, IndexSettings, NodeInfo, NodeRole,
    ShardRoutingEntry,
};
use axum::extract::{Path, State};
use datafusion::arrow::array::{Float32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct DropSignal(Option<std::sync::mpsc::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

fn sorted_remote_targets(
    cluster_state: &ClusterState,
    metadata: &IndexMetadata,
    local_shard_ids: &HashSet<u32>,
) -> Vec<(String, Vec<u32>)> {
    let mut targets: Vec<_> = super::remote_count_targets(cluster_state, metadata, local_shard_ids)
        .into_iter()
        .map(|(node, mut shard_ids)| {
            shard_ids.sort_unstable();
            (node.id, shard_ids)
        })
        .collect();
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    targets
}

fn make_test_node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        name: id.into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Data],
        raft_node_id: 1,
    }
}

fn make_sql_metadata(index: &str) -> IndexMetadata {
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".to_string(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let mut mappings = HashMap::new();
    mappings.insert(
        "title".to_string(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );
    mappings.insert(
        "description".to_string(),
        FieldMapping {
            field_type: FieldType::Text,
            dimension: None,
        },
    );
    mappings.insert(
        "brand".to_string(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );
    mappings.insert(
        "price".to_string(),
        FieldMapping {
            field_type: FieldType::Float,
            dimension: None,
        },
    );

    IndexMetadata {
        name: index.to_string(),
        uuid: crate::cluster::state::IndexUuid::new(format!("{}-uuid", index)),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        settings: IndexSettings::default(),
    }
}

async fn make_test_app_state(index_name: &str) -> (tempfile::TempDir, AppState) {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    cluster_state.add_index(make_sql_metadata(index_name));

    let temp_dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
        .await
        .unwrap();
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
    manager.update_state(cluster_state.clone());
    let state = AppState {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(crate::shard::ShardManager::new(
            temp_dir.path(),
            Duration::from_secs(60),
        )),
        transport_client: crate::transport::TransportClient::new(),
        local_node_id: "node-1".into(),
        raft,
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        sql_group_by_scan_limit: 1_000_000,
        sql_approximate_top_k: false,
    };

    let metadata = cluster_state.indices.get(index_name).unwrap().clone();
    assert!(
        state
            .shard_manager
            .open_shard_with_settings(
                index_name,
                0,
                &metadata.mappings,
                &metadata.settings,
                &metadata.uuid,
            )
            .is_ok()
    );

    let shard = state.shard_manager.get_shard(index_name, 0).unwrap();
    shard
        .add_document(
            "1",
            json!({"title": "iPhone Pro", "description": "iphone flagship", "brand": "Apple", "price": 999.0}),
        )
        .unwrap();
    shard
        .add_document(
            "2",
            json!({"title": "iPhone", "description": "iphone standard", "brand": "Apple", "price": 899.0}),
        )
        .unwrap();
    shard
        .add_document(
            "3",
            json!({"title": "Galaxy", "description": "iphone competitor", "brand": "Samsung", "price": 799.0}),
        )
        .unwrap();
    shard.refresh().unwrap();

    (temp_dir, state)
}

#[tokio::test]
async fn search_sql_uses_tantivy_fast_fields_for_local_non_wildcard_query() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC, brand ASC".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
    assert_eq!(body["planner"]["group_by_columns"], json!(["brand"]));
    assert_eq!(body["planner"]["has_residual_predicates"], json!(false));
    assert_eq!(body["matched_hits"], 3);
}

#[tokio::test]
async fn search_sql_uses_materialized_hits_fallback_for_select_star() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "materialized_hits_fallback");
    assert!(body["rows"].as_array().is_some());
    assert_eq!(body["matched_hits"], 3);
}

#[tokio::test]
async fn explain_sql_returns_plan_without_executing() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["index"], "products");
    assert_eq!(body["execution_strategy"], "tantivy_grouped_partials");
    assert_eq!(
        body["pushdown_summary"]["text_match"]["field"],
        "description"
    );
    assert_eq!(body["pushdown_summary"]["pushed_filter_count"], 1);
    assert_eq!(body["columns"]["group_by"], json!(["brand"]));

    let pipeline = body["pipeline"].as_array().unwrap();
    assert_eq!(pipeline.len(), 4);
    assert_eq!(pipeline[0]["name"], "tantivy_search");
    assert_eq!(pipeline[1]["name"], "grouped_partial_collect");
    assert_eq!(pipeline[2]["name"], "grouped_partial_merge");
    assert_eq!(pipeline[3]["name"], "final_grouped_sql_shape");

    // Should NOT have rows/matched_hits — explain doesn't execute
    assert!(body.get("rows").is_none());
    assert!(body.get("matched_hits").is_none());
}

#[tokio::test]
async fn explain_sql_returns_error_for_invalid_sql() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "NOT A VALID SQL".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "parsing_exception");
}

#[tokio::test]
async fn explain_sql_returns_error_for_nonexistent_index() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("nonexistent").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT * FROM nonexistent".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
}

#[tokio::test]
async fn explain_sql_shows_materialized_fallback_for_select_star() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_strategy"], "materialized_hits_fallback");
    assert!(body["columns"]["selects_all"].as_bool().unwrap());

    let pipeline = body["pipeline"].as_array().unwrap();
    assert_eq!(pipeline[1]["name"], "source_materialization");
}

#[tokio::test]
async fn search_sql_marks_expression_group_by_fast_field_streaming() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT LOWER(brand) AS brand_bucket, COUNT(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY LOWER(brand) ORDER BY brand_bucket ASC".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_fast_fields");
    assert_eq!(body["streaming_used"], true);
    assert_eq!(body["matched_hits"], 3);

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand_bucket"], json!("apple"));
    assert_eq!(rows[0]["total"], json!(2));
    assert_eq!(rows[1]["brand_bucket"], json!("samsung"));
    assert_eq!(rows[1]["total"], json!(1));
}

#[tokio::test]
async fn explain_analyze_returns_timings_and_rows() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY brand".to_string(),
            analyze: true,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    // Plan fields should still be present
    assert_eq!(body["execution_strategy"], "tantivy_grouped_partials");
    assert!(body["pipeline"].as_array().is_some());

    // ANALYZE-specific fields: timings, rows, matched_hits
    let timings = &body["timings"];
    assert!(timings["planning_ms"].as_f64().unwrap() >= 0.0);
    assert!(timings["search_ms"].as_f64().unwrap() >= 0.0);
    assert!(timings["total_ms"].as_f64().unwrap() > 0.0);

    assert_eq!(body["matched_hits"], 3);
    assert!(body["row_count"].as_u64().unwrap() > 0);
    let rows = body["rows"].as_array().unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn search_sql_semijoin_inner_hidden_order_by_metric_executes() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT title, brand FROM products WHERE brand IN (SELECT brand FROM products GROUP BY brand ORDER BY SUM(price) DESC LIMIT 1) ORDER BY title ASC".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
    assert_eq!(body["matched_hits"], json!(2));
    assert_eq!(body["planner"]["semijoin"]["outer_key"], json!("brand"));

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["title"], json!("iPhone"));
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[1]["title"], json!("iPhone Pro"));
    assert_eq!(rows[1]["brand"], json!("Apple"));
}

#[test]
fn collect_semijoin_key_allows_exact_limit_and_rejects_next_distinct_key() {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for key in 0..crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS {
        collect_semijoin_key(&mut keys, &mut seen, json!(key)).unwrap();
    }

    let err = collect_semijoin_key(
        &mut keys,
        &mut seen,
        json!(crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS),
    )
    .unwrap_err();

    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert_eq!(keys.len(), crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS);
}

#[tokio::test]
async fn explain_without_analyze_has_no_timings_or_rows() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand FROM products WHERE text_match(description, 'iphone')".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    // Plan-only mode: no timings, no rows, no matched_hits
    assert!(body.get("timings").is_none());
    assert!(body.get("rows").is_none());
    assert!(body.get("matched_hits").is_none());
}

#[tokio::test]
async fn explain_analyze_fast_fields_returns_timings() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = explain_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, price FROM products WHERE text_match(description, 'iphone')"
                .to_string(),
            analyze: true,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_fast_fields");
    assert_eq!(body["streaming_used"], false);
    assert!(body["timings"]["total_ms"].as_f64().unwrap() > 0.0);
    assert_eq!(body["matched_hits"], 3);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn search_sql_order_by_aggregate_expr_without_alias_uses_grouped_partials() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, SUM(price) FROM products WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY SUM(price) DESC LIMIT 2".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
    assert_eq!(body["matched_hits"], 3);

    let columns = body["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 2);
    let metric_column = columns[1].as_str().unwrap().to_string();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["brand"], json!("Apple"));
    assert_eq!(rows[0].get(&metric_column), Some(&json!(1898.0)));
    assert_eq!(rows[1]["brand"], json!("Samsung"));
    assert_eq!(rows[1].get(&metric_column), Some(&json!(799.0)));
}

// -------- Tests for global SQL helper functions --------

#[test]
fn matches_command_case_insensitive() {
    assert!(super::matches_command("SHOW TABLES", "SHOW TABLES"));
    assert!(super::matches_command("show tables", "SHOW TABLES"));
    assert!(super::matches_command("Show Tables", "SHOW TABLES"));
    assert!(super::matches_command("SHOW TABLES;", "SHOW TABLES"));
    assert!(super::matches_command("  SHOW TABLES  ;  ", "SHOW TABLES"));
    assert!(!super::matches_command("SHOW", "SHOW TABLES"));
    assert!(!super::matches_command("SELECT * FROM foo", "SHOW TABLES"));
}

#[test]
fn strip_command_extracts_argument() {
    assert_eq!(
        super::strip_command("DESCRIBE hackernews", "DESCRIBE"),
        Some("hackernews")
    );
    assert_eq!(
        super::strip_command("describe hackernews;", "DESCRIBE"),
        Some("hackernews")
    );
    assert_eq!(
        super::strip_command("DESC \"hackernews\"", "DESC"),
        Some("\"hackernews\"")
    );
    assert_eq!(super::strip_command("DESCRIBE", "DESCRIBE"), None);
    assert_eq!(
        super::strip_command("SHOW CREATE TABLE hackernews", "SHOW CREATE TABLE"),
        Some("hackernews")
    );
}

#[test]
fn unquote_identifier_handles_all_formats() {
    assert_eq!(super::unquote_identifier("hackernews"), "hackernews");
    assert_eq!(super::unquote_identifier("\"hackernews\""), "hackernews");
    assert_eq!(super::unquote_identifier("`hackernews`"), "hackernews");
    assert_eq!(super::unquote_identifier("  hackernews  "), "hackernews");
    assert_eq!(super::unquote_identifier("\"hackernews\";"), "hackernews");
}

#[test]
fn extract_index_from_sql_finds_table_name() {
    assert_eq!(
        super::extract_index_from_sql(r#"SELECT * FROM "hackernews" WHERE x > 1"#),
        Some("hackernews".to_string())
    );
    assert_eq!(
        super::extract_index_from_sql("SELECT count(*) FROM hackernews"),
        Some("hackernews".to_string())
    );
    assert_eq!(
        super::extract_index_from_sql(r#"SELECT count(*) FROM "benchmark-1gb""#),
        Some("benchmark-1gb".to_string())
    );
    assert_eq!(
        super::extract_index_from_sql(r#"SELECT count(*) from "benchmark-1gb""#),
        Some("benchmark-1gb".to_string())
    );
    assert_eq!(super::extract_index_from_sql("SHOW TABLES"), None);
    assert_eq!(super::extract_index_from_sql("not valid sql at all"), None);
}

#[tokio::test]
async fn canonicalize_sql_plan_fields_rewrites_case_insensitive_columns() {
    let (_tmp, state) = make_test_app_state("products").await;
    let cluster_state = state.cluster_manager.get_state();
    let mappings = &cluster_state.indices.get("products").unwrap().mappings;

    let plan = crate::hybrid::planner::plan_sql(
        "products",
        "SELECT BRAND AS brand, AVG(PRICE) AS avg_price FROM products WHERE text_match(DESCRIPTION, 'iphone') AND PRICE > 800 GROUP BY BRAND ORDER BY avg_price DESC",
    )
    .unwrap();

    let plan = super::canonicalize_sql_plan_fields(plan, mappings).unwrap();

    assert_eq!(plan.text_matches[0].field, "description");
    assert_eq!(plan.required_columns, vec!["brand", "price"]);
    assert_eq!(plan.group_by_columns, vec!["brand"]);

    match &plan.pushed_filters[0] {
        crate::search::QueryClause::Range(fields) => {
            assert!(fields.contains_key("price"));
        }
        other => panic!("expected range filter, got {other:?}"),
    }

    let grouped_sql = plan.grouped_sql.as_ref().unwrap();
    assert_eq!(grouped_sql.group_columns[0].source_name, "brand");
    assert_eq!(grouped_sql.group_columns[0].output_name, "brand");
    assert_eq!(grouped_sql.metrics[0].field.as_deref(), Some("price"));
    assert!(plan.rewritten_sql.contains("AVG(\"price\")"));
    assert!(plan.rewritten_sql.contains("GROUP BY \"brand\""));
    assert!(plan.rewritten_sql.contains("ORDER BY avg_price DESC"));
}

#[tokio::test]
async fn canonicalize_sql_plan_fields_quotes_mixed_case_source_identifiers_for_datafusion() {
    use crate::cluster::state::{FieldMapping, FieldType};

    let mappings = HashMap::from([
        (
            "PULocationID".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        ),
        (
            "DOLocationID".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        ),
    ]);

    let plan = crate::hybrid::planner::plan_sql(
        "rides",
        "SELECT CAST(PULocationID / 100 AS INT) AS bucket, COUNT(*) AS rides FROM rides GROUP BY CAST(PULocationID / 100 AS INT) HAVING COUNT(*) > 0 ORDER BY bucket",
    )
    .unwrap();

    let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();

    assert!(
        plan.rewritten_sql
            .contains("CAST(\"PULocationID\" / 100 AS INT)")
    );
    assert!(plan.rewritten_sql.contains("HAVING COUNT(*) > 0"));
    assert!(plan.rewritten_sql.contains("ORDER BY bucket"));
}

#[tokio::test]
async fn canonicalize_sql_plan_fields_preserves_quoted_mixed_case_identifiers() {
    use crate::cluster::state::{FieldMapping, FieldType};

    let mappings = HashMap::from([(
        "PULocationID".to_string(),
        FieldMapping {
            field_type: FieldType::Integer,
            dimension: None,
        },
    )]);

    let plan = crate::hybrid::planner::plan_sql(
        "rides",
        "SELECT \"PULocationID\" FROM rides ORDER BY \"PULocationID\"",
    )
    .unwrap();

    let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();

    assert!(plan.rewritten_sql.contains("\"PULocationID\""));
    assert!(!plan.rewritten_sql.contains("\"pulocationid\""));
}

#[tokio::test]
async fn direct_sql_partition_from_local_handle_stops_after_consumer_drop() {
    let plan =
        Arc::new(crate::hybrid::planner::plan_sql("test", "SELECT brand FROM test").unwrap());
    let target_schema = Arc::new(Schema::new(vec![Field::new("brand", DataType::Utf8, true)]));
    let worker_pools = crate::worker::WorkerPools::new(1, 1);
    let batch_calls = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    let handle = crate::engine::SqlStreamingBatchHandle::new(100, 100, {
        let batch_calls = batch_calls.clone();
        let drop_signal = DropSignal(Some(done_tx));
        move || {
            let _keep_drop_signal_alive = &drop_signal;
            let batch_no = batch_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if batch_no > 100 {
                return Ok(None);
            }

            if batch_no > 1 {
                std::thread::sleep(Duration::from_millis(50));
            }

            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("_id", DataType::Utf8, false),
                    Field::new("_score", DataType::Float32, false),
                    Field::new("brand", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![format!("id-{batch_no}")])),
                    Arc::new(Float32Array::from(vec![0.0f32])),
                    Arc::new(StringArray::from(vec![format!("brand-{batch_no}")])),
                ],
            )?;
            Ok(Some(batch))
        }
    });

    let partition =
        super::direct_sql_partition_from_local_handle(target_schema, plan, worker_pools, handle);

    let mut stream = partition.execute(SessionContext::new().task_ctx());
    let first_batch = stream
        .next()
        .await
        .expect("stream should yield first batch")
        .expect("first batch should be valid");
    assert_eq!(first_batch.num_rows(), 1);
    drop(stream);

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("producer should stop after consumer drop");
    assert!(
        batch_calls.load(Ordering::SeqCst) <= 2,
        "producer should stop shortly after the receiver drops"
    );
}

#[tokio::test]
async fn canonicalize_sql_plan_fields_rewrites_semijoin_keys_case_insensitively() {
    let (_tmp, state) = make_test_app_state("products").await;
    let cluster_state = state.cluster_manager.get_state();
    let mappings = &cluster_state.indices.get("products").unwrap().mappings;

    let plan = crate::hybrid::planner::plan_sql(
        "products",
        "SELECT TITLE AS title FROM products WHERE BRAND IN (SELECT BRAND FROM products WHERE PRICE > 800)",
    )
    .unwrap();

    let plan = super::canonicalize_sql_plan_fields(plan, mappings).unwrap();
    let semijoin = plan.semijoin.as_ref().unwrap();

    assert_eq!(semijoin.outer_key, "brand");
    assert_eq!(semijoin.inner_key, "brand");
    assert_eq!(semijoin.inner_plan.required_columns, vec!["brand"]);

    match &semijoin.inner_plan.pushed_filters[0] {
        crate::search::QueryClause::Range(fields) => {
            assert!(fields.contains_key("price"));
        }
        other => panic!("expected range filter, got {other:?}"),
    }
}

#[test]
fn canonicalize_sql_plan_fields_disables_derived_grouped_partials_for_numeric_source() {
    use crate::cluster::state::{FieldMapping, FieldType};

    let mappings = HashMap::from([(
        "PULocationID".to_string(),
        FieldMapping {
            field_type: FieldType::Integer,
            dimension: None,
        },
    )]);

    let sql = "SELECT CASE
            WHEN PULocationID >= '100' AND PULocationID < '200' THEN '100s'
            WHEN PULocationID >= '200' AND PULocationID < '300' THEN '200s'
        END AS bucket,
        count(*) AS rides
    FROM rides
    GROUP BY CASE
            WHEN PULocationID >= '100' AND PULocationID < '200' THEN '100s'
            WHEN PULocationID >= '200' AND PULocationID < '300' THEN '200s'
        END
    ORDER BY bucket";

    let plan = crate::hybrid::planner::plan_sql("rides", sql).unwrap();
    assert!(plan.uses_grouped_partials());

    let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();
    assert!(!plan.uses_grouped_partials());
    assert!(plan.has_group_by_fallback);
}

#[test]
fn canonicalize_rejects_ambiguous_case_only_mappings() {
    use crate::cluster::state::{FieldMapping, FieldType};

    let mut mappings = HashMap::new();
    mappings.insert(
        "Price".to_string(),
        FieldMapping {
            field_type: FieldType::Float,
            dimension: None,
        },
    );
    mappings.insert(
        "PRICE".to_string(),
        FieldMapping {
            field_type: FieldType::Float,
            dimension: None,
        },
    );

    let result = super::resolve_case_insensitive_mapping_name("price", &mappings);
    let err = result.unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    let body = err.1.0;
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("ambiguous"),
        "Expected ambiguous error, got: {body}"
    );
}

#[tokio::test]
async fn global_sql_show_tables_returns_index_list() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "SHOW TABLES".to_string(),
        analyze: false,
    };
    let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::OK);
    let columns = body["columns"].as_array().unwrap();
    assert!(columns.contains(&json!("index")));
    assert!(columns.contains(&json!("docs")));
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["index"], "products");
}

#[tokio::test]
async fn global_sql_show_tables_case_insensitive() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "show tables;".to_string(),
        analyze: false,
    };
    let (status, _) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn global_sql_describe_returns_field_mappings() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "DESCRIBE products".to_string(),
        analyze: false,
    };
    let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["index"], "products");
    let rows = body["rows"].as_array().unwrap();
    assert!(!rows.is_empty());
    // Check that known fields are present
    let field_names: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("field").and_then(|v| v.as_str()))
        .collect();
    assert!(field_names.contains(&"title"));
    assert!(field_names.contains(&"price"));
}

#[tokio::test]
async fn global_sql_describe_nonexistent_index_returns_404() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "DESCRIBE nonexistent".to_string(),
        analyze: false,
    };
    let (status, _) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn global_sql_show_create_table_returns_index_json() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "SHOW CREATE TABLE products".to_string(),
        analyze: false,
    };
    let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["index"], "products");
    let create = &body["create_statement"];
    assert!(create.get("settings").is_some());
    assert!(create.get("mappings").is_some());
    let props = &create["mappings"]["properties"];
    assert!(props.get("title").is_some());
}

#[tokio::test]
async fn global_sql_select_routes_to_correct_index() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: r#"SELECT count(*) AS total FROM "products""#.to_string(),
        analyze: false,
    };
    let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("columns").is_some());
    assert!(body.get("rows").is_some());
}

#[tokio::test]
async fn global_sql_invalid_query_returns_error() {
    let (_tmp, state) = make_test_app_state("products").await;
    let req = SqlQueryRequest {
        query: "THIS IS NOT SQL".to_string(),
        analyze: false,
    };
    let (status, _) = super::global_sql(State(state), Json(req)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_text_field_returns_error() {
    // Create metadata with a Text field (not Keyword)
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".to_string(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let mut mappings = HashMap::new();
    mappings.insert(
        "description".to_string(),
        FieldMapping {
            field_type: FieldType::Text,
            dimension: None,
        },
    );
    mappings.insert(
        "category".to_string(),
        FieldMapping {
            field_type: FieldType::Keyword,
            dimension: None,
        },
    );

    cluster_state.add_index(IndexMetadata {
        name: "texttest".to_string(),
        uuid: crate::cluster::state::IndexUuid::new("texttest-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings,
        settings: IndexSettings::default(),
    });

    let temp_dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
        .await
        .unwrap();
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
    manager.update_state(cluster_state.clone());
    let state = AppState {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(crate::shard::ShardManager::new(
            temp_dir.path(),
            Duration::from_secs(60),
        )),
        transport_client: crate::transport::TransportClient::new(),
        local_node_id: "node-1".into(),
        raft,
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        sql_group_by_scan_limit: 1_000_000,
        sql_approximate_top_k: false,
    };

    // GROUP BY on text field should return error
    let (status, Json(body)) = search_sql(
        State(state.clone()),
        Path(crate::common::IndexName::new("texttest").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT description, count(*) AS cnt FROM texttest GROUP BY description"
                .to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"]["type"], "group_by_text_field_exception",
        "should reject GROUP BY on text fields"
    );

    // GROUP BY on keyword field should succeed (not error)
    let (status2, _) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("texttest").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT category, count(*) AS cnt FROM texttest GROUP BY category".to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status2, StatusCode::OK, "keyword fields should work");
}

#[tokio::test]
async fn non_streamable_expression_group_by_with_tiny_scan_limit_returns_error() {
    // Set sql_group_by_scan_limit to 1 so even 3 docs triggers the error.
    let (_tmp, mut state) = make_test_app_state("products").await;
    state.sql_group_by_scan_limit = 1;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT LOWER(description) AS d, count(*) AS cnt FROM products GROUP BY LOWER(description)"
                .to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"]["type"], "group_by_scan_limit_exceeded",
        "should error when GROUP BY fallback hits scan limit"
    );
    let reason = body["error"]["reason"].as_str().unwrap();
    assert!(
        reason.contains("only") && reason.contains("were collected"),
        "error should mention collected vs matched: {reason}"
    );
}

#[tokio::test]
async fn streamable_expression_group_by_ignores_tiny_scan_limit() {
    let (_tmp, mut state) = make_test_app_state("products").await;
    state.sql_group_by_scan_limit = 1;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT LOWER(brand) AS b, count(*) AS cnt FROM products GROUP BY LOWER(brand) ORDER BY b"
                .to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_fast_fields");
    assert_eq!(body["streaming_used"], true);
    assert_eq!(body["truncated"], false);

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["b"], "apple");
    assert_eq!(rows[0]["cnt"], 2);
    assert_eq!(rows[1]["b"], "samsung");
    assert_eq!(rows[1]["cnt"], 1);
}

#[tokio::test]
async fn having_aggregate_source_field_survives_alias_shadowing() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, count(*) AS price FROM products GROUP BY brand HAVING AVG(price) > 850 ORDER BY brand"
                .to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
    assert_eq!(body["streaming_used"], false);

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["brand"], "Apple");
    assert_eq!(rows[0]["price"], 2);
}

#[tokio::test]
async fn wrapped_having_aggregate_source_field_survives_alias_shadowing() {
    let (_tmp, state) = make_test_app_state("products").await;

    let (status, Json(body)) = search_sql(
        State(state),
        Path(crate::common::IndexName::new("products").unwrap()),
        Json(SqlQueryRequest {
            query: "SELECT brand, count(*) AS price FROM products GROUP BY brand HAVING COALESCE(AVG(price), 0) > 850 ORDER BY brand"
                .to_string(),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_mode"], "tantivy_fast_fields");
    assert_eq!(body["streaming_used"], true);

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["brand"], "Apple");
    assert_eq!(rows[0]["price"], 2);
}

#[test]
fn remote_count_targets_groups_shards_per_node() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    cluster_state.add_node(make_test_node("node-2"));
    cluster_state.add_node(make_test_node("node-3"));

    let mut metadata = make_sql_metadata("products");
    metadata.shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".to_string(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    metadata.shard_routing.insert(
        2,
        ShardRoutingEntry {
            primary: "node-2".to_string(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    metadata.shard_routing.insert(
        3,
        ShardRoutingEntry {
            primary: "node-3".to_string(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );

    let local_shard_ids = HashSet::from([0u32]);
    let targets = sorted_remote_targets(&cluster_state, &metadata, &local_shard_ids);

    assert_eq!(
        targets,
        vec![
            ("node-2".to_string(), vec![1, 2]),
            ("node-3".to_string(), vec![3]),
        ]
    );
}
