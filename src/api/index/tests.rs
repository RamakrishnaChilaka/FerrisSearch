use super::*;
use crate::cluster::state::{
    ClusterState, IndexEngine, IndexSettings, NodeInfo, NodeRole, ShardRoutingEntry,
};
use crate::engine::{CompositeEngine, SearchEngine};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn parse_opensearch_ndjson_format() {
    let input = r#"{"index":{"_index":"my-index","_id":"1"}}
{"title":"Hello","year":2024}
{"index":{"_index":"my-index","_id":"2"}}
{"title":"World","year":2025}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].doc_id, "1");
    assert_eq!(docs[0].index.as_deref(), Some("my-index"));
    assert_eq!(docs[0].payload["title"], "Hello");
    assert_eq!(docs[1].doc_id, "2");
    assert_eq!(docs[1].payload["year"], 2025);
}

#[test]
fn parse_opensearch_create_action() {
    let input = r#"{"create":{"_index":"logs","_id":"abc"}}
{"msg":"test log"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "abc");
    assert_eq!(docs[0].index.as_deref(), Some("logs"));
    assert_eq!(docs[0].payload["msg"], "test log");
}

#[test]
fn parse_legacy_ferrissearch_format() {
    let input = r#"{}
{"_doc_id":"d1","_source":{"name":"Alice"}}
{}
{"_doc_id":"d2","_source":{"name":"Bob"}}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].doc_id, "d1");
    assert_eq!(docs[0].payload["name"], "Alice");
    assert_eq!(docs[1].doc_id, "d2");
    assert_eq!(docs[1].payload["name"], "Bob");
}

#[test]
fn parse_id_in_doc_body_fallback() {
    let input = r#"{"index":{}}
{"_id":"from-body","title":"test"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "from-body");
    assert!(docs[0].payload.get("_id").is_none());
    assert_eq!(docs[0].payload["title"], "test");
}

#[test]
fn parse_action_id_takes_precedence_over_body_id() {
    let input = r#"{"index":{"_id":"action-id"}}
{"_id":"body-id","title":"test"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "action-id");
}

#[test]
fn parse_auto_generates_id_when_missing() {
    let input = r#"{"index":{}}
{"title":"no id"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert!(!docs[0].doc_id.is_empty());
    assert_eq!(docs[0].payload["title"], "no id");
}

#[test]
fn parse_empty_body() {
    let docs = parse_bulk_ndjson("");
    assert!(docs.is_empty());
}

#[test]
fn parse_blank_lines_are_skipped() {
    let input = r#"{"index":{"_id":"1"}}

{"title":"Hello"}

{"index":{"_id":"2"}}

{"title":"World"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 2);
}

#[test]
fn parse_source_wrapper_unwrapped() {
    let input = r#"{"index":{"_id":"1"}}
{"_source":{"name":"Alice"},"_doc_id":"ignored"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "1");
    assert_eq!(docs[0].payload["name"], "Alice");
}

#[test]
fn parse_index_extracted_from_action() {
    let input = r#"{"index":{"_index":"idx-a","_id":"1"}}
{"f":"v1"}
{"index":{"_index":"idx-b","_id":"2"}}
{"f":"v2"}
{"index":{"_id":"3"}}
{"f":"v3"}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0].index.as_deref(), Some("idx-a"));
    assert_eq!(docs[1].index.as_deref(), Some("idx-b"));
    assert!(docs[2].index.is_none());
}

#[test]
fn parse_odd_number_of_lines_ignores_trailing() {
    let input = r#"{"index":{"_id":"1"}}
{"title":"complete"}
{"index":{"_id":"2"}}
"#;
    let docs = parse_bulk_ndjson(input);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "1");
}

fn make_test_node(id: &str) -> NodeInfo {
    make_test_node_with_roles(id, vec![NodeRole::Data])
}

fn make_test_node_with_roles(id: &str, roles: Vec<NodeRole>) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        name: id.into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles,
        raft_node_id: 1,
    }
}

fn make_test_metadata(primary: Option<&str>) -> IndexMetadata {
    let mut shard_routing = HashMap::new();
    if let Some(primary) = primary {
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: primary.to_string(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
    }
    IndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    }
}

async fn make_test_app_state(cluster_state: ClusterState) -> (tempfile::TempDir, AppState) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
        .await
        .unwrap();
    // Wait for leader election
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
    manager.update_state(cluster_state);
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
    (temp_dir, state)
}

#[test]
fn route_bulk_doc_reports_missing_primary() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let err = route_bulk_doc(
        0,
        "idx".into(),
        "doc-1".into(),
        serde_json::json!({"title": "hello"}),
        &make_test_metadata(None),
        &cluster_state,
    )
    .unwrap_err();

    assert_eq!(err["index"]["status"], 500);
    assert_eq!(
        err["index"]["error"]["type"],
        "shard_not_available_exception"
    );
}

#[tokio::test]
async fn bulk_index_reports_missing_primary_node_as_item_error() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    cluster_state.add_index(make_test_metadata(Some("missing-node")));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let input = axum::body::Bytes::from("{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n");

    let (status, Json(body)) = bulk_index(
        State(state),
        Path(crate::common::IndexName::new("idx").unwrap()),
        Query(RefreshParam { refresh: None }),
        input,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["index"]["status"], 500);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "node_not_found_exception"
    );
}

#[tokio::test]
async fn bulk_index_global_reports_missing_action_index() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let input = axum::body::Bytes::from("{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n");

    let (status, Json(body)) =
        bulk_index_global(State(state), Query(RefreshParam { refresh: None }), input).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["index"]["status"], 400);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "action_request_validation_exception"
    );
}

#[test]
fn finalize_bulk_items_preserves_shard_error_reason() {
    let routed_docs = vec![RoutedBulkDoc {
        position: 0,
        index_name: "idx".into(),
        doc_id: "doc-1".into(),
        payload: serde_json::json!({"title": "hello"}),
        shard_id: 0,
        node_id: "node-1".into(),
    }];
    let failed_targets = HashMap::from([(
        ("idx".to_string(), "node-1".to_string(), 0),
        "Shard bulk index failed: Replication failed: replica node-2 timed out".to_string(),
    )]);

    let items = finalize_bulk_items(vec![None], routed_docs, &failed_targets);

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["index"]["status"], 500);
    assert_eq!(items[0]["index"]["error"]["type"], "shard_failure");
    assert_eq!(
        items[0]["index"]["error"]["reason"],
        "Shard bulk index failed: Replication failed: replica node-2 timed out"
    );
}

#[tokio::test]
async fn create_index_applies_flush_threshold_setting() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    let body = axum::body::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "flush_threshold_bytes": 4096
            }
        }))
        .unwrap(),
    );

    let (status, _) = create_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let created = state.cluster_manager.get_state();
    assert_eq!(
        created.indices["idx"].settings.flush_threshold_bytes,
        Some(4096)
    );
}

#[tokio::test]
async fn create_index_returns_no_data_nodes_exception_when_no_data_nodes_are_available() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node_with_roles("node-1", vec![NodeRole::Master]));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    let body = axum::body::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }))
        .unwrap(),
    );

    let (status, Json(response)) = create_index(
        State(state),
        Path(crate::common::IndexName::new("idx").unwrap()),
        body,
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response["error"]["type"], "no_data_nodes_exception");
    assert_eq!(
        response["error"]["reason"],
        "No data nodes available to assign shards"
    );
}

#[test]
fn forwarded_create_index_error_response_preserves_create_index_statuses() {
    let invalid_argument = anyhow::Error::from(tonic::Status::invalid_argument("unknown engine"));
    let (status, Json(body)) =
        forwarded_create_index_error_response(&invalid_argument).expect("status should map");
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert_eq!(body["error"]["reason"], "unknown engine");

    let unimplemented = anyhow::Error::from(tonic::Status::unimplemented(
        "remote_store engine is not implemented yet",
    ));
    let (status, Json(body)) =
        forwarded_create_index_error_response(&unimplemented).expect("status should map");
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert_eq!(
        body["error"]["reason"],
        "remote_store engine is not implemented yet"
    );

    let no_data_nodes = anyhow::Error::from(tonic::Status::internal(
        "No data nodes available to assign shards",
    ));
    let (status, Json(body)) =
        forwarded_create_index_error_response(&no_data_nodes).expect("status should map");
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"]["type"], "no_data_nodes_exception");
    assert_eq!(
        body["error"]["reason"],
        "No data nodes available to assign shards"
    );

    let unrelated_internal = anyhow::Error::from(tonic::Status::internal("boom"));
    assert!(forwarded_create_index_error_response(&unrelated_internal).is_none());
}

#[tokio::test]
async fn create_index_accepts_explicit_local_shards_engine() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    let body = axum::body::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "engine": "local_shards",
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }))
        .unwrap(),
    );

    let (status, _) = create_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state.cluster_manager.get_state().indices["idx"]
            .settings
            .engine,
        IndexEngine::LocalShards
    );
}

#[tokio::test]
async fn create_index_rejects_unimplemented_remote_store_engine() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    let body = axum::body::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "engine": "remote_store"
        }))
        .unwrap(),
    );

    let (status, Json(response)) = create_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        body,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(
        response["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("remote_store")
    );
    assert!(
        !state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("idx")
    );
}

#[tokio::test]
async fn fan_out_maintenance_keeps_local_target_without_node_entry() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (temp_dir, state) = make_test_app_state(cluster_state).await;
    let shard_dir = temp_dir.path().join("idx-uuid").join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
    engine
        .add_document("doc-1", serde_json::json!({"title": "maintenance reopen"}))
        .unwrap();
    engine.refresh().unwrap();
    drop(engine);

    let (successful, failed) =
        fan_out_maintenance(&state, "idx", MaintenanceDispatchOp::Flush).await;

    assert_eq!((successful, failed), (1, 0));
    assert!(state.shard_manager.get_shard("idx", 0).is_some());
}

#[tokio::test]
async fn enqueue_force_merge_tasks_keeps_local_target_without_node_entry() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (temp_dir, state) = make_test_app_state(cluster_state).await;
    let shard_dir = temp_dir.path().join("idx-uuid").join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
    engine
        .add_document("doc-1", serde_json::json!({"title": "maintenance reopen"}))
        .unwrap();
    engine.refresh().unwrap();
    drop(engine);

    let dispatch = enqueue_force_merge_tasks(&state, "idx", 1).await;

    assert_eq!(dispatch.total_nodes, 1);
    assert_eq!(dispatch.node_tasks.len(), 1);
    assert!(dispatch.dispatch_failures.is_empty());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state.shard_manager.get_shard("idx", 0).is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background force-merge should reopen the local shard");
}

#[tokio::test]
async fn force_merge_http_returns_accepted_response() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let (status, Json(body)) = force_merge_index(
        State(state),
        Path(crate::common::IndexName::new("idx").unwrap()),
        Query(HashMap::from([(
            "max_num_segments".to_string(),
            "3".to_string(),
        )])),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["index"], "idx");
    assert_eq!(body["max_num_segments"], 3);
    assert_eq!(body["task"]["action"], "indices:admin/forcemerge");
    assert!(body["task"]["id"].as_str().is_some());
    assert_eq!(body["_nodes"]["total"], 1);
    assert_eq!(body["_nodes"]["started"], 1);
    assert_eq!(body["_nodes"]["failed"], 0);
}

#[test]
fn maintenance_fanout_concurrency_keeps_flush_parallel() {
    assert_eq!(maintenance_fanout_concurrency(3), 3);
    assert_eq!(maintenance_fanout_concurrency(1), 1);
}

#[test]
fn maintenance_fanout_concurrency_uses_target_count_floor() {
    assert_eq!(maintenance_fanout_concurrency(3), 3);
    assert_eq!(maintenance_fanout_concurrency(0), 1);
}

#[tokio::test]
async fn maintenance_job_catches_panics() {
    let err = spawn_maintenance_job(async move {
        panic!("boom");
        #[allow(unreachable_code)]
        Ok::<(u32, u32), anyhow::Error>((0, 0))
    })
    .await
    .err()
    .unwrap();

    assert!(err.to_string().contains("panicked"));
}

#[tokio::test]
async fn auto_create_index_opens_local_shard_with_cluster_uuid() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let (_tmp, state) = make_test_app_state(cluster_state.clone()).await;

    let metadata = auto_create_index(&state, "auto-idx", &cluster_state)
        .await
        .unwrap();

    assert_eq!(
        state.shard_manager.index_uuid("auto-idx"),
        Some(metadata.uuid.to_string())
    );
    assert!(state.shard_manager.get_shard("auto-idx", 0).is_some());
}

#[tokio::test]
async fn get_index_settings_includes_flush_threshold_setting() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let mut metadata = make_test_metadata(Some("node-1"));
    metadata.settings.engine = IndexEngine::LocalShards;
    metadata.settings.flush_threshold_bytes = Some(8192);
    cluster_state.add_index(metadata);

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let (status, Json(body)) = get_index_settings(
        State(state),
        Path(crate::common::IndexName::new("idx").unwrap()),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["idx"]["settings"]["index"]["flush_threshold_bytes"],
        8192
    );
    assert_eq!(body["idx"]["settings"]["index"]["engine"], "local_shards");
}

#[tokio::test]
async fn update_index_settings_rejects_engine_changes() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let (status, Json(body)) = update_index_settings(
        State(state),
        Path(crate::common::IndexName::new("idx").unwrap()),
        Json(serde_json::json!({
            "index": {
                "engine": "remote_store"
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("index.engine is immutable")
    );
}

#[tokio::test]
async fn update_index_settings_updates_flush_threshold_setting() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let (status, Json(body)) = update_index_settings(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        Json(serde_json::json!({
            "index": {
                "flush_threshold_bytes": 16384
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], true);
    let updated = state.cluster_manager.get_state();
    assert_eq!(
        updated.indices["idx"].settings.flush_threshold_bytes,
        Some(16384)
    );
}
