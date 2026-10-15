use super::*;
use crate::transport::proto::{ShardBulkOpKind, ShardBulkOperation, WriteFailureOutcome};
use crate::transport::write_failure::WriteFailure;

fn primary_node(harness: &RefreshCluster) -> usize {
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary = &state.indices[INDEX].shard_routing[&0].primary;
    harness
        .cluster
        .nodes
        .iter()
        .position(|node| node.state.local_node_id == *primary)
        .unwrap()
}

pub(super) fn fail_replica_once(harness: &RefreshCluster) {
    let replica = harness.replica(0);
    harness.cluster.nodes[replica]
        .state
        .shard_manager
        .set_copy_retry_policy_for_test(100, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    harness.engines[&(replica, 0)].inject_wal_write_failures_for_test(28, 1);
}

async fn failure_cluster() -> RefreshCluster {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    harness.set_minimum_durable_copies(2).await;
    harness
}

fn assert_unknown(body: &Value, seq_no: u64, term: u64) {
    assert_eq!(body["error"]["type"], "write_outcome_unknown", "{body}");
    assert_eq!(body["_seq_no"], seq_no, "{body}");
    assert_eq!(body["_primary_term"], term, "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("minimum_durable_copies"),
        "{body}"
    );
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("No space left"),
        "{body}"
    );
    assert!(body.get("_shards").is_none(), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_write_failure_replication_receipts_survive_grpc_rest_and_ordered_bulk() {
    for (method, path, payload) in [
        (
            reqwest::Method::PUT,
            format!("/{INDEX}/_doc/doc"),
            Some(json!({"value": 1})),
        ),
        (
            reqwest::Method::PUT,
            format!("/{INDEX}/_create/created"),
            Some(json!({"value": 2})),
        ),
        (
            reqwest::Method::POST,
            format!("/{INDEX}/_update/doc?retry_on_conflict=3"),
            Some(json!({"doc": {"value": 3}})),
        ),
        (reqwest::Method::DELETE, format!("/{INDEX}/_doc/doc"), None),
    ] {
        let harness = failure_cluster().await;
        let primary = primary_node(&harness);
        let (status, seeded) = harness
            .cluster
            .request(
                0,
                reqwest::Method::PUT,
                &format!("/{INDEX}/_doc/doc"),
                Some(json!({"value": 0})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{seeded}");
        fail_replica_once(&harness);
        let (status, body) = harness.cluster.request(0, method, &path, payload).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        let retained = harness.engines[&(primary, 0)]
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap();
        let operation = retained
            .operations
            .iter()
            .find(|operation| operation.seq_no == 1)
            .unwrap();
        assert_unknown(&body, 1, operation.primary_term);
        assert_eq!(
            harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
            Some(1),
            "an indeterminate update must not consume automatic retry sequences"
        );
    }
    for grpc in [true, false] {
        let harness = failure_cluster().await;
        let primary = primary_node(&harness);
        let coordinator = &harness.cluster.nodes[0].state;
        let state = coordinator.cluster_manager.get_state();
        let node = state.nodes[&harness.cluster.nodes[primary].state.local_node_id].clone();
        let term = state.indices[INDEX].shard_routing[&0].primary_term;
        fail_replica_once(&harness);
        if grpc {
            let docs = vec![
                ("duplicate".to_string(), json!({"value": 4})),
                ("duplicate".to_string(), json!({"value": 5})),
                ("other".to_string(), json!({"value": 6})),
            ];
            let error = coordinator
                .transport_client
                .forward_bulk_to_shard(&node, INDEX, 0, &docs)
                .await
                .unwrap_err();
            let failure = error.downcast_ref::<WriteFailure>().unwrap();
            assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
            assert_eq!(failure.seq_no, Some(0));
            assert_eq!(failure.last_seq_no, Some(2));
            assert_eq!(failure.primary_term, Some(term));
            assert!(failure.reason.contains("No space left"), "{failure:?}");
        } else {
            let body = harness
                .bulk(
                    0,
                    false,
                    "",
                    "{\"index\":{\"_id\":\"duplicate\"}}\n{\"value\":7}\n\
                 {\"index\":{\"_id\":\"duplicate\"}}\n{\"value\":8}\n\
                 {\"index\":{\"_id\":\"other\"}}\n{\"value\":9}\n"
                        .to_string(),
                )
                .await;
            assert_eq!(body["errors"], true, "{body}");
            for (offset, id) in ["duplicate", "duplicate", "other"].iter().enumerate() {
                let item = &body["items"][offset]["index"];
                assert_eq!(item["_id"], *id, "{body}");
                assert_eq!(item["status"], 500, "{body}");
                assert_unknown(item, offset as u64, term);
            }
        }
        assert_eq!(
            harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
            Some(2)
        );
    }

    let harness = failure_cluster().await;
    let primary = primary_node(&harness);
    let term = harness.cluster.nodes[0]
        .state
        .cluster_manager
        .get_state()
        .indices[INDEX]
        .shard_routing[&0]
        .primary_term;
    fail_replica_once(&harness);
    let body = harness
        .bulk(
            0,
            false,
            "",
            "{\"index\":{\"_id\":\"mixed\"}}\n{\"value\":10}\n\
             {\"create\":{\"_id\":\"neighbor\"}}\n{\"value\":11}\n\
             {\"delete\":{\"_id\":\"never-existed\"}}\n\
             {\"create\":{\"_id\":\"created\"}}\n{\"value\":12}\n"
                .to_string(),
        )
        .await;
    assert_unknown(&body["items"][0]["index"], 0, term);
    for item in body["items"].as_array().unwrap().iter().skip(1) {
        let item = item.as_object().unwrap().values().next().unwrap();
        assert_eq!(item["status"], 503, "{body}");
        assert_eq!(
            item["error"]["type"], "shard_not_available_exception",
            "{body}"
        );
        assert!(item.get("_seq_no").is_none(), "{body}");
    }
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(0)
    );

    let harness = failure_cluster().await;
    let primary = primary_node(&harness);
    let (status, seeded) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/mixed"),
            Some(json!({"value": 0})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{seeded}");
    let term = seeded["_primary_term"].as_u64().unwrap();
    fail_replica_once(&harness);
    let body = harness
        .bulk(
            0,
            false,
            "",
            "{\"update\":{\"_id\":\"mixed\",\"retry_on_conflict\":3}}\n\
             {\"doc\":{\"value\":13}}\n"
                .to_string(),
        )
        .await;
    assert_unknown(&body["items"][0]["update"], 1, term);
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_runtime_mixed_bulk_reports_each_captured_set_after_exclusion() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let primary = primary_node(&harness);
    let replica = harness.replica(0);
    let replica_id = harness.cluster.nodes[replica].state.local_node_id.clone();
    let (status, seed) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/created"),
            Some(json!({"value": 0})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{seed}");
    fail_replica_once(&harness);
    let body = harness
        .bulk(
            0,
            false,
            "?refresh=true",
            "{\"index\":{\"_id\":\"mixed\"}}\n{\"value\":10}\n\
         {\"create\":{\"_id\":\"neighbor\"}}\n{\"value\":11}\n\
         {\"delete\":{\"_id\":\"never-existed\"}}\n\
         {\"create\":{\"_id\":\"created\"}}\n{\"value\":12}\n"
                .to_string(),
        )
        .await;
    assert_eq!(body["errors"], true, "{body}");
    let first = &body["items"][0]["index"];
    assert_eq!(first["status"], 201, "{body}");
    assert_eq!(first["_seq_no"], 1, "{body}");
    assert_eq!(first["_shards"]["total"], 2, "{body}");
    assert_eq!(first["_shards"]["successful"], 1, "{body}");
    assert_eq!(first["_shards"]["failed"], 1, "{body}");
    assert_eq!(
        first["_shards"]["failures"][0]["node"], replica_id,
        "{body}"
    );
    for (offset, action, status) in [(1, "create", 201), (2, "delete", 404)] {
        let item = &body["items"][offset][action];
        assert_eq!(item["status"], status, "{body}");
        assert_eq!(item["_seq_no"], offset + 1, "{body}");
        assert_eq!(
            item["_shards"],
            json!({"total": 1, "successful": 1, "failed": 0}),
            "{body}"
        );
        assert!(item.get("error").is_none(), "{body}");
    }
    for (offset, action) in [(0, "index"), (1, "create"), (2, "delete")] {
        assert_eq!(
            body["items"][offset][action]["_refresh"],
            json!({"total": 1, "successful": 1, "failed": 0}),
            "{body}"
        );
    }
    assert_eq!(body["items"][3]["create"]["status"], 409, "{body}");
    assert!(
        body["items"][3]["create"].get("_seq_no").is_none(),
        "{body}"
    );
    assert!(
        body["items"][3]["create"].get("_shards").is_none(),
        "{body}"
    );
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(3)
    );
    assert!(
        harness.engines[&(replica, 0)]
            .ensure_write_health()
            .is_err()
    );
    let (status, noop) = harness
        .cluster
        .request(
            0,
            reqwest::Method::POST,
            &format!("/{INDEX}/_update/mixed?operation_id=noop-after-exclusion"),
            Some(json!({"doc": {"value": 10}, "detect_noop": true})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{noop}");
    assert_eq!(noop["result"], "noop", "{noop}");
    assert_eq!(noop["_seq_no"], 1, "{noop}");
    assert_eq!(
        noop["_shards"],
        json!({"total": 1, "successful": 1, "failed": 0}),
        "{noop}"
    );
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(4)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_write_failure_pre_wal_rejections_preserve_identity_and_neighbors() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let primary = primary_node(&harness);
    let coordinator = &harness.cluster.nodes[0].state;
    let state = coordinator.cluster_manager.get_state();
    let node = state.nodes[&harness.cluster.nodes[primary].state.local_node_id].clone();
    let uuid = state.indices[INDEX].uuid.to_string();
    let (status, receipt) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc"),
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    let before = harness.engines[&(primary, 0)].sequence_stats();

    for (source, condition, expected) in [
        (
            json!({"_seq_no": 9}),
            crate::engine::WriteCondition::Unconditional,
            400,
        ),
        (
            json!({"value": 2}),
            crate::engine::WriteCondition::Create,
            409,
        ),
    ] {
        let error = coordinator
            .transport_client
            .forward_index_with_options_to_shard(
                &node,
                INDEX,
                0,
                "doc",
                &source,
                crate::transport::WriteOptions {
                    condition,
                    refresh: false,
                },
            )
            .await
            .unwrap_err();
        let failure = error.downcast_ref::<WriteFailure>().unwrap();
        assert_eq!(failure.outcome, WriteFailureOutcome::Rejected);
        assert_eq!(failure.status, expected);
        assert_eq!(failure.seq_no, None);
        assert_eq!(failure.primary_term, None);
        assert_eq!(harness.engines[&(primary, 0)].sequence_stats(), before);
    }

    let error = coordinator
        .transport_client
        .forward_index_request_to_shard(
            &node,
            crate::transport::proto::ShardDocRequest {
                index_name: INDEX.into(),
                shard_id: 0,
                doc_id: "doc".into(),
                index_uuid: Some(format!("retired-{uuid}")),
                payload_json: serde_json::to_vec(&json!({"value": 2})).unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let failure = error.downcast_ref::<WriteFailure>().unwrap();
    assert_eq!(failure.outcome, WriteFailureOutcome::Rejected);
    assert_eq!(failure.status, 404);
    assert_eq!(failure.seq_no, None);
    assert_eq!(harness.engines[&(primary, 0)].sequence_stats(), before);

    let docs = vec![
        (
            "doc".into(),
            json!({"value": 2}),
            ShardBulkOperation {
                kind: ShardBulkOpKind::Create as i32,
                ..Default::default()
            },
        ),
        (
            "neighbor".into(),
            json!({"value": 3}),
            ShardBulkOperation {
                kind: ShardBulkOpKind::Index as i32,
                ..Default::default()
            },
        ),
    ];
    let items = coordinator
        .transport_client
        .forward_bulk_operations_to_shard(&node, INDEX, 0, &docs, false)
        .await
        .unwrap();
    assert_eq!(items[0].status, 409);
    assert_eq!(items[0].seq_no, None);
    assert_eq!(items[1].status, 201);
    assert_eq!(items[1].seq_no, Some(1));
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_write_failure_primary_post_wal_apply_fails_closed_without_same_process_healing() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        0,
    )
    .await;
    let primary = primary_node(&harness);
    harness.engines[&(primary, 0)].inject_engine_apply_failures_for_test(28, 1);
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc"),
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"]["type"], "write_outcome_unknown", "{body}");
    assert_eq!(body["_seq_no"], 0, "{body}");
    let term = harness.cluster.nodes[primary]
        .state
        .cluster_manager
        .get_state()
        .indices[INDEX]
        .shard_routing[&0]
        .primary_term;
    assert_eq!(body["_primary_term"], term, "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("No space left"),
        "{body}"
    );
    let (status, refresh) = harness
        .cluster
        .request(
            0,
            reqwest::Method::POST,
            &format!("/{INDEX}/_refresh"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{refresh}");
    let (status, document) = harness
        .cluster
        .request(
            0,
            reqwest::Method::GET,
            &format!("/{INDEX}/_doc/doc?realtime=true"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{document}");
    assert_eq!(
        document["error"]["type"], "shard_not_available_exception",
        "{document}"
    );
    assert!(
        document["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("fail-stopped"),
        "{document}"
    );
    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        let source = (method == reqwest::Method::PUT).then(|| json!({"value": 2}));
        let (status, rejected) = harness
            .cluster
            .request(0, method, &format!("/{INDEX}/_doc/next"), source)
            .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{rejected}");
        assert_eq!(
            rejected["error"]["type"], "shard_not_available_exception",
            "{rejected}"
        );
        assert!(rejected.get("_seq_no").is_none(), "{rejected}");
    }
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(0)
    );
}
