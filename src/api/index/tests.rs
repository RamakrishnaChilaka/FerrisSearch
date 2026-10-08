use super::*;
use crate::cluster::state::{
    ClusterState, IndexEngine, IndexSettings, NodeInfo, NodeRole, ShardRoutingEntry,
};
use crate::shard::ShardManager;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn forwarding_metadata_timeout_is_retryable_but_ambiguous_rpc_timeouts_are_not() {
    for status in [
        tonic::Status::unavailable("connection lost after write"),
        tonic::Status::deadline_exceeded("request timed out"),
    ] {
        assert_eq!(
            forwarded_write_error_classification(&anyhow::Error::new(status)),
            (StatusCode::INTERNAL_SERVER_ERROR, "forward_exception")
        );
    }
    let error = anyhow::Error::new(tonic::Status::unavailable(format!(
        "{}timed out waiting for local cluster state version 7; applied version is 6",
        crate::transport::state_wait::STATE_WAIT_STATUS_PREFIX,
    )))
    .context("IndexDoc RPC");
    assert_eq!(
        forwarded_write_error_classification(&error),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "shard_not_available_exception"
        )
    );
    let (_, Json(body)) = document_write_error_response("Forward", error);
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("applied version is 6")
    );
}
#[test]
fn d1_commit3_version_map_capacity_is_retryable_429() {
    let unrelated = anyhow::Error::new(tonic::Status::resource_exhausted("worker queue full"));
    assert_eq!(
        forwarded_write_error_classification(&unrelated),
        (StatusCode::INTERNAL_SERVER_ERROR, "forward_exception")
    );

    let error = anyhow::Error::new(tonic::Status::resource_exhausted(format!(
        "{}version map capacity exceeded",
        crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX
    )));

    assert_eq!(
        forwarded_write_error_classification(&error),
        (
            StatusCode::TOO_MANY_REQUESTS,
            "version_map_capacity_exceeded"
        )
    );
}

#[test]
fn remote_store_search_stats_response_json_contains_pruning_counters() {
    let stats = RemoteStoreSearchStats {
        published_splits: 5,
        candidate_splits: 2,
        pruned_splits: 3,
        assigned_splits: 2,
    };

    assert_eq!(
        stats.to_response_json(),
        serde_json::json!({
            "pruning": {
                "published_splits": 5,
                "candidate_splits": 2,
                "pruned_splits": 3,
                "assigned_splits": 2,
            }
        })
    );
}

#[test]
fn parse_opensearch_ndjson_format() {
    let input = r#"{"index":{"_index":"my-index","_id":"1"}}
{"title":"Hello","year":2024}
{"index":{"_index":"my-index","_id":"2"}}
{"title":"World","year":2025}
"#;
    let docs = parse_bulk_ndjson(input).unwrap();
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
    let docs = parse_bulk_ndjson(input).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "abc");
    assert_eq!(docs[0].index.as_deref(), Some("logs"));
    assert_eq!(docs[0].payload["msg"], "test log");
}

#[test]
fn parse_action_id_takes_precedence_over_body_id() {
    let input = r#"{"index":{"_id":"action-id"}}
{"_id":"body-id","title":"test"}
"#;
    let docs = parse_bulk_ndjson(input).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, "action-id");
}

#[test]
fn parse_auto_generates_id_when_missing() {
    let input = r#"{"index":{}}
{"title":"no id"}
"#;
    let docs = parse_bulk_ndjson(input).unwrap();
    assert_eq!(docs.len(), 1);
    assert!(!docs[0].doc_id.is_empty());
    assert_eq!(docs[0].payload["title"], "no id");
}

#[test]
fn parse_empty_body() {
    let docs = parse_bulk_ndjson("").unwrap();
    assert!(docs.is_empty());
}

#[test]
fn parse_blank_lines_are_skipped() {
    let input = r#"{"index":{"_id":"1"}}

{"title":"Hello"}

{"index":{"_id":"2"}}

{"title":"World"}
"#;
    let docs = parse_bulk_ndjson(input).unwrap();
    assert_eq!(docs.len(), 2);
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
    let docs = parse_bulk_ndjson(input).unwrap();
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0].index.as_deref(), Some("idx-a"));
    assert_eq!(docs[1].index.as_deref(), Some("idx-b"));
    assert!(docs[2].index.is_none());
}

#[test]
fn parse_trailing_action_rejects_missing_source() {
    let input = r#"{"index":{"_id":"1"}}
{"title":"complete"}
{"index":{"_id":"2"}}
"#;
    let error = parse_bulk_ndjson(input).unwrap_err();
    assert!(error.contains("line [3]"), "{error}");
    assert!(error.contains("requires a source line"), "{error}");
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
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
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
        storage_manager: Arc::new(
            crate::storage::StorageManager::new_in_path(temp_dir.path()).unwrap(),
        ),
        security_manager: Arc::new(crate::security::SecurityManager::disabled()),
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
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

    assert_eq!(err["index"]["status"], 503);
    assert_eq!(
        err["index"]["error"]["type"],
        "shard_not_available_exception"
    );
}

#[tokio::test]
async fn empty_bulk_does_not_create_an_index() {
    let (_temporary, state) = make_test_app_state(ClusterState::new("empty-bulk".into())).await;
    let (status, Json(body)) = bulk_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("empty").unwrap()),
        None,
        Query(RefreshParam::default()),
        axum::body::Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["errors"], false);
    assert!(state.cluster_manager.get_state().indices.is_empty());
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
        None,
        Query(RefreshParam::default()),
        input,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["index"]["status"], 503);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "shard_not_available_exception"
    );
}

#[tokio::test]
async fn bulk_index_global_reports_missing_action_index() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let input = axum::body::Bytes::from("{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n");

    let (status, Json(body)) =
        bulk_index_global(State(state), None, Query(RefreshParam::default()), input).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["index"]["status"], 400);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "action_request_validation_exception"
    );
}

#[tokio::test]
async fn bulk_index_global_rejects_protected_security_index() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let input = axum::body::Bytes::from(
        "{\"index\":{\"_index\":\".ferris_security\",\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n",
    );

    let (status, Json(body)) =
        bulk_index_global(State(state), None, Query(RefreshParam::default()), input).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"][0]["index"]["status"], 403);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "security_exception"
    );
}

#[tokio::test]
async fn bulk_index_global_validates_raw_action_index_name() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let (_tmp, state) = make_test_app_state(cluster_state).await;
    let input = axum::body::Bytes::from(
        "{\"index\":{\"_index\":\"BadName\",\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n",
    );

    let (status, Json(body)) =
        bulk_index_global(State(state), None, Query(RefreshParam::default()), input).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"][0]["index"]["status"], 400);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "invalid_index_name_exception"
    );
}

#[tokio::test]
async fn bulk_index_global_enforces_principal_index_permissions() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));

    let (_tmp, mut state) = make_test_app_state(cluster_state).await;
    state.security_manager = Arc::new(
        crate::security::SecurityManager::new(crate::security::SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![],
        })
        .unwrap(),
    );
    let principal = crate::security::Principal {
        name: "writer".into(),
        key_id: "writer-key".into(),
        roles: vec!["write".into()],
        indices: vec!["logs-*".into()],
    };
    let input = axum::body::Bytes::from(
        "{\"index\":{\"_index\":\"metrics\",\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n",
    );

    let (status, Json(body)) = bulk_index_global(
        State(state),
        Some(axum::extract::Extension(principal)),
        Query(RefreshParam::default()),
        input,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"][0]["index"]["status"], 403);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "security_exception"
    );
}

#[tokio::test]
async fn index_scoped_bulk_authorizes_the_action_index_override() {
    let (_temporary, mut state) =
        make_test_app_state(ClusterState::new("bulk-override".into())).await;
    state.security_manager = Arc::new(
        crate::security::SecurityManager::new(crate::security::SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![],
        })
        .unwrap(),
    );
    let principal = crate::security::Principal {
        name: "writer".into(),
        key_id: "writer-key".into(),
        roles: vec!["write".into()],
        indices: vec!["logs-*".into()],
    };
    let (status, Json(body)) = bulk_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("logs-2026").unwrap()),
        Some(axum::extract::Extension(principal)),
        Query(RefreshParam::default()),
        axum::body::Bytes::from(
            "{\"index\":{\"_index\":\"metrics\",\"_id\":\"1\"}}\n{\"value\":1}\n",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"][0]["index"]["status"], 403);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "security_exception"
    );
    assert!(state.cluster_manager.get_state().indices.is_empty());
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
        action: "index".into(),
        condition: crate::engine::WriteCondition::Unconditional,
        retry_on_conflict: 0,
    }];
    let failed_targets = HashMap::from([(
        ("idx".to_string(), "node-1".to_string(), 0),
        Err(bulk::BulkTargetFailure::internal(
            "Shard bulk index failed: Replication failed: replica node-2 timed out".to_string(),
        )),
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

#[test]
fn retryable_aborted_write_maps_to_service_unavailable() {
    let error = anyhow::Error::from(tonic::Status::aborted(
        "reopen shard after dynamic mapping: shard UUID changed; retry the write",
    ));
    let (status, Json(body)) = document_write_error_response("Forward", error);

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["type"], "shard_not_available_exception");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("shard UUID changed")
    );
}

#[test]
fn typed_write_failure_bulk_finalizer_uses_submitted_count_and_duplicate_id_offsets() {
    use crate::transport::write_failure::WriteFailure;
    let routed = |count| {
        (0..count)
            .map(|position| RoutedBulkDoc {
                position,
                index_name: "idx".into(),
                node_id: "node-1".into(),
                shard_id: 0,
                doc_id: "duplicate".into(),
                payload: Value::Null,
                action: "index".into(),
                condition: crate::engine::WriteCondition::Unconditional,
                retry_on_conflict: 0,
            })
            .collect()
    };
    let failed = bulk::BulkTargetFailure::from_forward_error(
        WriteFailure::indeterminate("replica failed", Some(0), Some(7), Some(2)).into(),
    );
    let outcomes = HashMap::from([(("idx".into(), "node-1".into(), 0), Err(failed))]);
    let items = finalize_bulk_items(vec![None; 3], routed(3), &outcomes);
    for (offset, item) in items.iter().enumerate() {
        assert_eq!(item["index"]["status"], 500);
        assert_eq!(item["index"]["error"]["type"], "write_outcome_unknown");
        assert_eq!(item["index"]["_id"], "duplicate");
        assert_eq!(item["index"]["_seq_no"], offset as u64);
        assert_eq!(item["index"]["_primary_term"], 7);
    }
    let invalid = finalize_bulk_items(vec![None; 2], routed(2), &outcomes);
    for item in invalid {
        assert!(item["index"].get("_seq_no").is_none(), "{item}");
        assert!(
            item["index"]["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("does not match the submitted batch"),
            "{item}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_get_update_and_bulk_update_fail_closed_after_partial_replay() {
    use crate::transport::proto::internal_transport_client::InternalTransportClient;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut cluster_state = ClusterState::new("realtime-replay-failure".into());
    let mut node = make_test_node("node-1");
    node.transport_port = address.port();
    cluster_state.add_node(node);
    let mut metadata = make_test_metadata(Some("node-1"));
    metadata.settings.refresh_interval_ms = Some(3_600_000);
    metadata.settings.flush_threshold_bytes = Some(0);
    let settings = metadata.settings.clone();
    cluster_state.add_index(metadata);
    let allocation_id = cluster_state.primary_allocation_id("idx", 0).unwrap();
    let (_temporary, state) = make_test_app_state(cluster_state).await;
    let shard_manager = state.shard_manager.clone();
    let engine = tokio::task::spawn_blocking(move || {
        shard_manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &settings,
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
    })
    .await
    .unwrap()
    .unwrap();
    let failure_engine = engine.clone();
    tokio::task::spawn_blocking(move || {
        failure_engine
            .add_document("old", serde_json::json!({"value": 1}))
            .unwrap();
        failure_engine.refresh().unwrap();
        let mut suffix = (0..3_000)
            .map(|i| (format!("filler-{i}"), serde_json::json!({"value": i})))
            .collect::<Vec<_>>();
        suffix.push(("old".into(), serde_json::json!({"value": 2})));
        suffix.push(("fresh".into(), serde_json::json!({"value": 3})));
        failure_engine.bulk_add_documents(suffix).unwrap();
        failure_engine.inject_engine_apply_failures_for_test(28, 1);
        assert!(
            failure_engine
                .add_document("failed", serde_json::json!({"value": 4}))
                .is_err()
        );
        failure_engine.inject_replay_commit_failure_for_test(28, 1);
        let error = failure_engine.refresh().unwrap_err();
        assert!(
            error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.raw_os_error() == Some(28))
            }),
            "{error:#}"
        );
    })
    .await
    .unwrap();
    let service = crate::transport::server::create_transport_service_for_test(
        state.cluster_manager.clone(),
        state.shard_manager.clone(),
        state.transport_client.clone(),
        state.task_manager.clone(),
        "node-1".into(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async { shutdown_rx.await.unwrap() },
            )
            .await
    });
    let mut client = InternalTransportClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    client
        .ping(crate::transport::proto::PingRequest {
            source_node_id: "node-1".into(),
        })
        .await
        .unwrap();
    let get = get_document(
        State(state.clone()),
        Path((crate::common::IndexName::new("idx").unwrap(), "old".into())),
        Query(GetParams::default()),
    )
    .await;
    let update = update_document(
        State(state.clone()),
        Path((crate::common::IndexName::new("idx").unwrap(), "old".into())),
        Query(UpdateParams::default()),
        Json(serde_json::json!({"doc": {"value": 5}})),
    )
    .await;
    let bulk = bulk_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        None,
        Query(RefreshParam::default()),
        axum::body::Bytes::from(
            "{\"update\":{\"_id\":\"fresh\"}}\n{\"doc\":{\"value\":6},\"doc_as_upsert\":true}\n",
        ),
    )
    .await;
    let reader = get_document(
        State(state.clone()),
        Path((crate::common::IndexName::new("idx").unwrap(), "old".into())),
        Query(GetParams {
            realtime: Some(false),
        }),
    )
    .await;
    drop(client);
    drop(state);
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();

    for (status, Json(body)) in [get, update] {
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["error"]["type"], "shard_not_available_exception");
        let reason = body["error"]["reason"].as_str().unwrap();
        assert!(reason.contains("incomplete"), "{reason}");
        assert!(reason.contains("No space left"), "{reason}");
    }
    let (status, Json(body)) = bulk;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], true, "{body}");
    let item = &body["items"][0]["update"];
    assert_eq!(item["status"], 503, "{item}");
    assert_eq!(item["error"]["type"], "shard_not_available_exception");
    assert!(
        item["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("No space left")
    );
    assert!(item.get("_seq_no").is_none());
    let (status, Json(body)) = reader;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_source"], serde_json::json!({"value": 1}));
    assert_eq!(body["_index_uuid"], "idx-uuid");
    assert!(engine.writer_is_failed_for_test());
}

#[test]
fn bulk_aborted_failure_remains_attributable_and_retryable() {
    let routed_docs = vec![RoutedBulkDoc {
        position: 0,
        index_name: "idx".into(),
        doc_id: "doc-1".into(),
        payload: serde_json::json!({"title": "hello"}),
        shard_id: 0,
        node_id: "node-1".into(),
        action: "index".into(),
        condition: crate::engine::WriteCondition::Unconditional,
        retry_on_conflict: 0,
    }];
    let failure = bulk::BulkTargetFailure::from_forward_error(anyhow::Error::from(
        tonic::Status::aborted("stale shard reopen; retry the write"),
    ));
    let failed_targets =
        HashMap::from([(("idx".to_string(), "node-1".to_string(), 0), Err(failure))]);

    let items = finalize_bulk_items(vec![None], routed_docs, &failed_targets);

    assert_eq!(items[0]["index"]["_index"], "idx");
    assert_eq!(items[0]["index"]["_id"], "doc-1");
    assert_eq!(items[0]["index"]["status"], 503);
    assert_eq!(
        items[0]["index"]["error"]["type"],
        "shard_not_available_exception"
    );
    assert!(
        items[0]["index"]["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("stale shard reopen")
    );
}

#[test]
fn finalize_bulk_items_preserves_receipts_across_targets_and_duplicate_ids() {
    let documents = [
        (0, "a", 0, "same"),
        (1, "b", 1, "other"),
        (2, "a", 0, "same"),
    ];
    let routed = documents
        .into_iter()
        .map(|(position, index, shard_id, doc_id)| RoutedBulkDoc {
            position,
            index_name: index.into(),
            doc_id: doc_id.into(),
            payload: serde_json::json!({}),
            shard_id,
            node_id: "node-1".into(),
            action: "index".into(),
            condition: crate::engine::WriteCondition::Unconditional,
            retry_on_conflict: 0,
        })
        .collect();
    let outcomes = HashMap::from([
        (
            ("a".to_string(), "node-1".to_string(), 0),
            Ok(vec![
                serde_json::json!({"_id": "same", "_seq_no": 10, "_primary_term": 7, "status": 201}),
                serde_json::json!({"_id": "same", "_seq_no": 11, "_primary_term": 7, "status": 200}),
            ]),
        ),
        (
            ("b".to_string(), "node-1".to_string(), 1),
            Ok(vec![
                serde_json::json!({"_id": "other", "_seq_no": 20, "_primary_term": 8, "status": 201}),
            ]),
        ),
    ]);
    let items = finalize_bulk_items(vec![None, None, None], routed, &outcomes);
    assert_eq!(
        items
            .iter()
            .map(|item| item["index"]["_seq_no"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![10, 20, 11]
    );
    assert_eq!(items[0]["index"]["_primary_term"], 7);
    assert_eq!(items[1]["index"]["_primary_term"], 8);
    assert_eq!(items[0]["index"]["status"], 201);
    assert_eq!(items[1]["index"]["status"], 201);
    assert_eq!(items[2]["index"]["status"], 200);
}

#[test]
fn finalize_bulk_items_fails_when_primary_receipt_is_missing() {
    let routed = vec![RoutedBulkDoc {
        position: 0,
        index_name: "idx".into(),
        doc_id: "doc".into(),
        payload: serde_json::json!({}),
        shard_id: 0,
        node_id: "node-1".into(),
        action: "index".into(),
        condition: crate::engine::WriteCondition::Unconditional,
        retry_on_conflict: 0,
    }];
    let items = finalize_bulk_items(vec![None], routed, &HashMap::new());
    assert_eq!(items[0]["index"]["status"], 500);
    assert_eq!(
        items[0]["index"]["error"]["reason"],
        "missing primary bulk write receipt"
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
        Query(UnsupportedWriteParams::default()),
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
        Query(UnsupportedWriteParams::default()),
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
        Query(UnsupportedWriteParams::default()),
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
async fn create_index_with_remote_store_engine_succeeds_but_rejects_writes() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_node(make_test_node("node-1"));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    let body = axum::body::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "engine": "remote_store"
        }))
        .unwrap(),
    );

    let (status, Json(_response)) = create_index(
        State(state.clone()),
        Path(crate::common::IndexName::new("idx").unwrap()),
        Query(UnsupportedWriteParams::default()),
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let snapshot = state.cluster_manager.get_state();
    let metadata = snapshot
        .indices
        .get("idx")
        .expect("remote_store index should be present in cluster state");
    assert_eq!(metadata.settings.engine, IndexEngine::RemoteStore);
    assert_eq!(metadata.number_of_shards, 0);
    assert!(metadata.shard_routing.is_empty());
    assert!(!metadata.settings.engine.supports_writes());
}

#[tokio::test]
async fn fan_out_maintenance_keeps_local_target_without_node_entry() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_index(make_test_metadata(Some("node-1")));

    let (temp_dir, state) = make_test_app_state(cluster_state).await;
    {
        let persisted = ShardManager::new(temp_dir.path(), Duration::from_secs(60));
        persisted.register_index_uuid("idx", "idx-uuid");
        let engine = persisted
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
            )
            .unwrap();
        engine
            .add_document("doc-1", serde_json::json!({"title": "maintenance reopen"}))
            .unwrap();
        engine.refresh().unwrap();
    }

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
    {
        let persisted = ShardManager::new(temp_dir.path(), Duration::from_secs(60));
        persisted.register_index_uuid("idx", "idx-uuid");
        let engine = persisted
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
            )
            .unwrap();
        engine
            .add_document("doc-1", serde_json::json!({"title": "maintenance reopen"}))
            .unwrap();
        engine.refresh().unwrap();
    }

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

#[tokio::test]
async fn force_merge_http_rejects_invalid_segment_targets() {
    let mut cluster_state = ClusterState::new("test-cluster".into());
    cluster_state.add_index(make_test_metadata(Some("node-1")));
    let (_tmp, state) = make_test_app_state(cluster_state).await;

    for invalid in ["0", "not-a-number", "4294967296"] {
        let (status, Json(body)) = force_merge_index(
            State(state.clone()),
            Path(crate::common::IndexName::new("idx").unwrap()),
            Query(HashMap::from([(
                "max_num_segments".to_string(),
                invalid.to_string(),
            )])),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert!(
            body["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("between 1 and 4294967295")
        );
    }
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
