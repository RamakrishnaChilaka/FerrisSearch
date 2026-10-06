use super::*;
use crate::transport::proto::{ShardBulkOpKind, ShardBulkOperation, WriteFailureOutcome};
use crate::transport::write_failure::WriteFailure;

fn primary(harness: &RefreshCluster) -> usize {
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary = &state.indices[INDEX].shard_routing[&0].primary;
    harness
        .cluster
        .nodes
        .iter()
        .position(|node| node.state.local_node_id == *primary)
        .unwrap()
}

fn fail_replica_once(harness: &RefreshCluster) {
    let replica = harness.replica(0);
    harness.cluster.nodes[replica]
        .state
        .shard_manager
        .set_copy_retry_policy_for_test(100, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    harness.engines[&(replica, 0)].inject_wal_write_failures_for_test(28, 1);
}

fn assert_unknown(body: &Value, seq_no: u64, term: u64) {
    assert_eq!(body["error"]["type"], "write_outcome_unknown", "{body}");
    assert_eq!(body["_seq_no"], seq_no, "{body}");
    assert_eq!(body["_primary_term"], term, "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("Replication failed"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_write_failure_replication_receipts_survive_grpc_rest_and_ordered_bulk() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let primary = primary(&harness);
    let coordinator = &harness.cluster.nodes[0].state;
    let state = coordinator.cluster_manager.get_state();
    let node = state.nodes[&harness.cluster.nodes[primary].state.local_node_id].clone();

    for (seq_no, method, path, payload) in [
        (
            0,
            reqwest::Method::PUT,
            format!("/{INDEX}/_doc/doc"),
            Some(json!({"value": 1})),
        ),
        (
            1,
            reqwest::Method::PUT,
            format!("/{INDEX}/_create/created"),
            Some(json!({"value": 2})),
        ),
        (
            2,
            reqwest::Method::POST,
            format!("/{INDEX}/_update/doc?retry_on_conflict=3"),
            Some(json!({"doc": {"value": 3}})),
        ),
        (
            3,
            reqwest::Method::DELETE,
            format!("/{INDEX}/_doc/doc"),
            None,
        ),
    ] {
        fail_replica_once(&harness);
        let (status, body) = harness.cluster.request(0, method, &path, payload).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        let retained = harness.engines[&(primary, 0)]
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap();
        let operation = retained
            .operations
            .iter()
            .find(|operation| operation.seq_no == seq_no)
            .unwrap();
        assert_unknown(&body, seq_no, operation.primary_term);
        assert_eq!(
            harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
            Some(seq_no),
            "an indeterminate update must not consume automatic retry sequences"
        );
    }
    let term = harness.cluster.nodes[primary]
        .state
        .cluster_manager
        .get_state()
        .indices[INDEX]
        .shard_routing[&0]
        .primary_term;

    let docs = vec![
        ("duplicate".to_string(), json!({"value": 4})),
        ("duplicate".to_string(), json!({"value": 5})),
        ("other".to_string(), json!({"value": 6})),
    ];
    fail_replica_once(&harness);
    let error = coordinator
        .transport_client
        .forward_bulk_to_shard(&node, INDEX, 0, &docs)
        .await
        .unwrap_err();
    let failure = error.downcast_ref::<WriteFailure>().unwrap();
    assert_eq!(failure.outcome, WriteFailureOutcome::Indeterminate);
    assert_eq!(failure.seq_no, Some(4));
    assert_eq!(failure.last_seq_no, Some(6));
    assert_eq!(failure.primary_term, Some(term));

    fail_replica_once(&harness);
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
        assert_unknown(item, 7 + offset as u64, term);
    }

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
    assert_unknown(&body["items"][0]["index"], 10, term);
    assert_eq!(body["items"][1]["create"]["status"], 201, "{body}");
    assert_eq!(body["items"][1]["create"]["_seq_no"], 11, "{body}");
    assert_eq!(body["items"][2]["delete"]["status"], 404, "{body}");
    assert_eq!(body["items"][2]["delete"]["result"], "not_found", "{body}");
    assert_eq!(body["items"][2]["delete"]["_seq_no"], 12, "{body}");
    assert!(body["items"][2]["delete"].get("error").is_none(), "{body}");
    assert_eq!(body["items"][3]["create"]["status"], 409, "{body}");
    assert!(
        body["items"][3]["create"].get("_seq_no").is_none(),
        "{body}"
    );

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
    assert_unknown(&body["items"][0]["update"], 13, term);
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats().max_seq_no,
        Some(13)
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
    let primary = primary(&harness);
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
async fn typed_write_failure_primary_post_wal_apply_survives_http_and_later_replay() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        0,
    )
    .await;
    let primary = primary(&harness);
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
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_source"], json!({"value": 1}));
    assert_eq!(document["_seq_no"], body["_seq_no"]);
    assert_eq!(document["_primary_term"], body["_primary_term"]);
}
