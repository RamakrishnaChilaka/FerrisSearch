use crate::api::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use futures::future::join_all;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(default = "default_query")]
    q: String,
    #[serde(default = "default_size")]
    size: usize,
    #[serde(default)]
    from: usize,
}

fn default_query() -> String {
    "*".to_string()
}

fn default_size() -> usize {
    10
}

#[derive(Deserialize)]
pub struct SqlQueryRequest {
    pub query: String,
}

/// GET /{index}/_search?q=... — query-string search across all shards (local + remote) for this index.
pub async fn search_documents(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(params): Query<SearchParams>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let mut all_hits = Vec::new();
    let mut successful_shards = 0u32;
    let mut failed_shards = 0u32;

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        &state,
        &index_name,
        &metadata,
        "GET search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    for (shard_id, engine) in &local_shards {
        match engine.search(&params.q) {
            Ok(hits) => {
                successful_shards += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name,
                        "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Err(e) => {
                tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e);
                failed_shards += 1;
            }
        }
    }

    // Scatter to remote shards
    let mut remote_futures = Vec::new();
    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }
        if let Some(node_info) = cluster_state.nodes.get(&routing.primary) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let index = index_name.clone();
            let sid = *shard_id;
            let query = params.q.clone();
            remote_futures.push(tokio::spawn(async move {
                (
                    sid,
                    client
                        .forward_search_to_shard(&node_info, &index, sid, &query)
                        .await,
                )
            }));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for result in remote_results {
        match result {
            Ok((shard_id, Ok(hits))) => {
                successful_shards += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Ok((shard_id, Err(e))) => {
                tracing::error!(
                    "Remote shard {}/{} search failed: {}",
                    index_name,
                    shard_id,
                    e
                );
                failed_shards += 1;
            }
            Err(e) => {
                tracing::error!("Remote shard search task panicked: {}", e);
                failed_shards += 1;
            }
        }
    }

    // Sort by _score descending, then apply from/size pagination
    all_hits.sort_by(|a, b| {
        let sa = a.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sb = b.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });

    let total = all_hits.len();
    let from = params.from;
    let size = params.size;
    let paginated: Vec<_> = all_hits.into_iter().skip(from).take(size).collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": {
                "total": successful_shards + failed_shards,
                "successful": successful_shards,
                "failed": failed_shards
            },
            "hits": {
                "total": { "value": total, "relation": "eq" },
                "hits": paginated
            }
        })),
    )
}

/// POST /{index}/_sql/explain — Explain the SQL query plan without executing it.
pub async fn explain_sql(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    if !cluster_state.indices.contains_key(&index_name) {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    }

    let plan = match crate::hybrid::planner::plan_sql(&index_name, &req.query) {
        Ok(plan) => plan,
        Err(e) => {
            return crate::api::error_response(StatusCode::BAD_REQUEST, "parsing_exception", e);
        }
    };

    (StatusCode::OK, Json(plan.to_explain_json()))
}

/// POST /{index}/_sql — SQL projection/aggregation over distributed search hits.
pub async fn search_sql(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let plan = match crate::hybrid::planner::plan_sql(&index_name, &req.query) {
        Ok(plan) => plan,
        Err(e) => {
            return crate::api::error_response(StatusCode::BAD_REQUEST, "parsing_exception", e);
        }
    };

    let search_req = plan.to_search_request();
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    if plan.uses_grouped_partials() {
        let distributed = match crate::api::index::execute_distributed_dsl_search(
            &state,
            &index_name,
            &search_req,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => return err,
        };

        let sql_result =
            match crate::hybrid::execute_grouped_partial_sql(&plan, &distributed.partial_aggs) {
                Ok(result) => result,
                Err(error) => {
                    return crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sql_execution_exception",
                        error,
                    );
                }
            };

        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "execution_mode": "tantivy_grouped_partials",
                "planner": {
                    "text_match": plan.text_match.as_ref().map(|text_match| serde_json::json!({
                        "field": text_match.field,
                        "query": text_match.query,
                    })),
                    "pushed_down_filters": plan.pushed_filters,
                    "group_by_columns": plan.group_by_columns,
                    "required_columns": plan.required_columns,
                    "has_residual_predicates": plan.has_residual_predicates,
                    "uses_grouped_partials": true,
                },
                "_shards": {
                    "total": distributed.successful_shards + distributed.failed_shards,
                    "successful": distributed.successful_shards,
                    "failed": distributed.failed_shards
                },
                "matched_hits": distributed.total_hits,
                "columns": sql_result.columns,
                "rows": sql_result.rows
            })),
        );
    }

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        &state,
        &index_name,
        &metadata,
        "SQL search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    // Distributed fast-field SQL: scatter sql_record_batch to all shards (local + remote)
    let direct_sql = if !plan.selects_all_columns {
        let mut batches = Vec::new();
        let mut total_hits = 0usize;
        let mut successful_shards = 0u32;
        let mut direct_error: Option<anyhow::Error> = None;

        // Local shards: call engine.sql_record_batch directly
        for (_shard_id, engine) in &local_shards {
            match engine.sql_record_batch(
                &search_req,
                &plan.required_columns,
                plan.needs_id,
                plan.needs_score,
            ) {
                Ok(Some(batch_result)) => {
                    successful_shards += 1;
                    total_hits += batch_result.total_hits;
                    batches.push(batch_result.batch);
                }
                Ok(None) => {
                    direct_error = Some(anyhow::anyhow!(
                        "local shard does not support direct SQL batches"
                    ));
                    break;
                }
                Err(error) => {
                    direct_error = Some(error);
                    break;
                }
            }
        }

        // Remote shards: scatter Arrow IPC requests via gRPC
        if direct_error.is_none() {
            let cs = state.cluster_manager.get_state();
            let mut remote_futures = Vec::new();

            for (shard_id, routing) in &metadata.shard_routing {
                if local_shard_ids.contains(shard_id) {
                    continue; // already handled locally
                }
                let primary_node_id = &routing.primary;
                if let Some(node) = cs.nodes.get(primary_node_id) {
                    let client = state.transport_client.clone();
                    let node = node.clone();
                    let idx = index_name.clone();
                    let sid = *shard_id;
                    let req = search_req.clone();
                    let cols = plan.required_columns.clone();
                    let nid = plan.needs_id;
                    let nsc = plan.needs_score;
                    remote_futures.push(tokio::spawn(async move {
                        client
                            .forward_sql_batch_to_shard(&node, &idx, sid, &req, &cols, nid, nsc)
                            .await
                    }));
                }
            }

            let remote_results = futures::future::join_all(remote_futures).await;
            for result in remote_results {
                match result {
                    Ok(Ok((batch, hits))) => {
                        successful_shards += 1;
                        total_hits += hits;
                        batches.push(batch);
                    }
                    Ok(Err(e)) => {
                        direct_error = Some(e);
                        break;
                    }
                    Err(e) => {
                        direct_error = Some(anyhow::anyhow!("remote shard task failed: {}", e));
                        break;
                    }
                }
            }
        }

        match direct_error {
            None => Some((batches, total_hits, successful_shards, 0u32)),
            Some(error) => {
                tracing::warn!(
                    "Falling back to materialized SQL execution for [{}]: {}",
                    index_name,
                    error
                );
                None
            }
        }
    } else {
        None
    };

    let (sql_result, matched_hits, successful_shards, failed_shards, execution_mode) =
        if let Some((batches, total_hits, successful_shards, failed_shards)) = direct_sql {
            let sql_result = match crate::hybrid::execute_planned_sql_batches(&plan, batches).await
            {
                Ok(result) => result,
                Err(e) => {
                    return crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sql_execution_exception",
                        e,
                    );
                }
            };
            (
                sql_result,
                total_hits,
                successful_shards,
                failed_shards,
                "tantivy_fast_fields",
            )
        } else {
            let distributed = match crate::api::index::execute_distributed_dsl_search(
                &state,
                &index_name,
                &search_req,
            )
            .await
            {
                Ok(result) => result,
                Err(err) => return err,
            };

            let sql_result =
                match crate::hybrid::execute_planned_sql(&plan, &distributed.all_hits).await {
                    Ok(result) => result,
                    Err(e) => {
                        return crate::api::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "sql_execution_exception",
                            e,
                        );
                    }
                };
            (
                sql_result,
                distributed.total_hits,
                distributed.successful_shards,
                distributed.failed_shards,
                "materialized_hits_fallback",
            )
        };

    // Determine if results were truncated by the internal collection ceiling.
    // If the user specified an explicit LIMIT, they got what they asked for — that's not truncation.
    // Truncation only occurs when the internal 100K ceiling silently drops results.
    let truncated = !plan.limit_pushed_down
        && plan.limit.is_none()
        && matched_hits > search_req.size
        && execution_mode != "tantivy_grouped_partials";

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "execution_mode": execution_mode,
            "truncated": truncated,
            "planner": {
                "text_match": plan.text_match.as_ref().map(|text_match| serde_json::json!({
                    "field": text_match.field,
                    "query": text_match.query,
                })),
                "pushed_down_filters": plan.pushed_filters,
                "group_by_columns": plan.group_by_columns,
                "required_columns": plan.required_columns,
                "has_residual_predicates": plan.has_residual_predicates,
            },
            "_shards": {
                "total": successful_shards + failed_shards,
                "successful": successful_shards,
                "failed": failed_shards
            },
            "matched_hits": matched_hits,
            "columns": sql_result.columns,
            "rows": sql_result.rows
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        ClusterState, FieldMapping, FieldType, IndexMetadata, IndexSettings, NodeInfo, NodeRole,
        ShardRoutingEntry,
    };
    use axum::extract::{Path, State};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

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
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        }
    }

    fn make_test_app_state(index_name: &str) -> (tempfile::TempDir, AppState) {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));
        cluster_state.add_index(make_sql_metadata(index_name));

        let temp_dir = tempfile::tempdir().unwrap();
        let manager = crate::cluster::ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state.clone());
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft: None,
        };

        let metadata = cluster_state.indices.get(index_name).unwrap().clone();
        assert!(
            crate::api::index::ensure_local_index_shards_open(
                &state, index_name, &metadata, "SQL test"
            )
            .len()
                == 1
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
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC, brand ASC".to_string(),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
        assert_eq!(body["planner"]["group_by_columns"], json!(["brand"]));
        assert_eq!(body["planner"]["has_residual_predicates"], json!(false));
        assert_eq!(body["planner"]["uses_grouped_partials"], json!(true));
        assert_eq!(body["matched_hits"], 3);
    }

    #[tokio::test]
    async fn search_sql_uses_materialized_hits_fallback_for_select_star() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
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
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC".to_string(),
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
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "NOT A VALID SQL".to_string(),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
    }

    #[tokio::test]
    async fn explain_sql_returns_error_for_nonexistent_index() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("nonexistent".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM nonexistent".to_string(),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
    }

    #[tokio::test]
    async fn explain_sql_shows_materialized_fallback_for_select_star() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_strategy"], "materialized_hits_fallback");
        assert!(body["columns"]["selects_all"].as_bool().unwrap());

        let pipeline = body["pipeline"].as_array().unwrap();
        assert_eq!(pipeline[1]["name"], "source_materialization");
    }
}
