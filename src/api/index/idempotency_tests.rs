use super::*;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_retry_after_ambiguous_commit_returns_original_receipt() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    harness.set_minimum_durable_copies(2).await;
    let primary = primary(&harness);
    let replica = harness.replica(0);
    harness.cluster.nodes[replica]
        .state
        .shard_manager
        .set_copy_retry_policy_for_test(100, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    harness.engines[&(replica, 0)].inject_wal_write_failures_for_test(28, 1);

    let path = format!("/{INDEX}/_doc/retry?operation_id=ambiguous-retry");
    let payload = json!({"value": 7});
    let (status, failed) = harness
        .cluster
        .request(0, reqwest::Method::PUT, &path, Some(payload.clone()))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{failed}");
    assert_eq!(failed["error"]["type"], "write_outcome_unknown", "{failed}");
    let seq_no = failed["_seq_no"].as_u64().unwrap();
    let primary_term = failed["_primary_term"].as_u64().unwrap();
    let before_retry = harness.engines[&(primary, 0)].sequence_stats();
    assert!(
        harness.engines[&(replica, 0)]
            .ensure_write_health()
            .is_err()
    );
    harness.set_minimum_durable_copies(1).await;

    let (status, retried) = harness
        .cluster
        .request(0, reqwest::Method::PUT, &path, Some(payload))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{retried}");
    assert_eq!(retried["_seq_no"], seq_no, "{retried}");
    assert_eq!(retried["_primary_term"], primary_term, "{retried}");
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats(),
        before_retry
    );
    assert_eq!(
        harness.engines[&(replica, 0)].sequence_stats().max_seq_no,
        None,
        "a retry cannot heal or write to the failed, excluded replica"
    );
    assert_eq!(
        retried["_shards"],
        json!({"total": 1, "successful": 1, "failed": 0})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_operation_ids_deduplicate_auto_index_update_and_delete() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let primary = primary(&harness);

    let auto_path = format!("/{INDEX}/_doc?operation_id=auto-retry");
    let auto_payload = json!({"value": 1});
    let (first_status, first) = harness
        .cluster
        .request(
            0,
            reqwest::Method::POST,
            &auto_path,
            Some(auto_payload.clone()),
        )
        .await;
    assert_eq!(first_status, StatusCode::CREATED, "{first}");
    let after_first = harness.engines[&(primary, 0)].sequence_stats();
    let (retry_status, retry) = harness
        .cluster
        .request(
            1,
            reqwest::Method::POST,
            &auto_path,
            Some(auto_payload.clone()),
        )
        .await;
    assert_eq!(retry_status, StatusCode::CREATED, "{retry}");
    assert_eq!(retry["_id"], first["_id"], "{retry}");
    assert_eq!(retry["_seq_no"], first["_seq_no"], "{retry}");
    assert_eq!(retry["_version"], first["_version"], "{retry}");
    assert_eq!(harness.engines[&(primary, 0)].sequence_stats(), after_first);

    let (conflict_status, conflict) = harness
        .cluster
        .request(
            0,
            reqwest::Method::POST,
            &auto_path,
            Some(json!({"value": 2})),
        )
        .await;
    assert_eq!(conflict_status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(
        conflict["error"]["type"], "operation_id_conflict",
        "{conflict}"
    );

    let doc_id = first["_id"].as_str().unwrap();
    let update_path = format!("/{INDEX}/_update/{doc_id}?operation_id=update-noop");
    let update_payload = json!({"doc": {"value": 1}, "detect_noop": true});
    let (update_status, update) = harness
        .cluster
        .request(
            0,
            reqwest::Method::POST,
            &update_path,
            Some(update_payload.clone()),
        )
        .await;
    assert_eq!(update_status, StatusCode::OK, "{update}");
    assert_eq!(update["result"], "noop", "{update}");
    let after_update = harness.engines[&(primary, 0)].sequence_stats();
    let (retry_status, retry) = harness
        .cluster
        .request(1, reqwest::Method::POST, &update_path, Some(update_payload))
        .await;
    assert_eq!(retry_status, StatusCode::OK, "{retry}");
    assert_eq!(retry["_seq_no"], update["_seq_no"], "{retry}");
    assert_eq!(retry["_primary_term"], update["_primary_term"], "{retry}");
    assert_eq!(retry["_version"], update["_version"], "{retry}");
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats(),
        after_update
    );

    let delete_path = format!("/{INDEX}/_doc/{doc_id}?operation_id=delete-retry");
    let (delete_status, deleted) = harness
        .cluster
        .request(0, reqwest::Method::DELETE, &delete_path, None)
        .await;
    assert_eq!(delete_status, StatusCode::OK, "{deleted}");
    let after_delete = harness.engines[&(primary, 0)].sequence_stats();
    let (retry_status, retry) = harness
        .cluster
        .request(1, reqwest::Method::DELETE, &delete_path, None)
        .await;
    assert_eq!(retry_status, StatusCode::OK, "{retry}");
    assert_eq!(retry["_seq_no"], deleted["_seq_no"], "{retry}");
    assert_eq!(retry["_primary_term"], deleted["_primary_term"], "{retry}");
    assert_eq!(retry["_version"], deleted["_version"], "{retry}");
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats(),
        after_delete
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_operation_ids_return_original_results_and_conflicts_per_item() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let primary = primary(&harness);
    let first = harness
        .bulk(
            0,
            false,
            "",
            "{\"index\":{\"_operation_id\":\"bulk-retry\"}}\n{\"value\":1}\n".into(),
        )
        .await;
    assert_eq!(first["errors"], false, "{first}");
    let original = &first["items"][0]["index"];
    let before_retry = harness.engines[&(primary, 0)].sequence_stats();

    let retry = harness
        .bulk(
            1,
            false,
            "",
            "{\"index\":{\"_operation_id\":\"bulk-retry\"}}\n{\"value\":1}\n\
             {\"index\":{\"_operation_id\":\"bulk-retry\"}}\n{\"value\":2}\n"
                .into(),
        )
        .await;
    assert_eq!(retry["errors"], true, "{retry}");
    let duplicate = &retry["items"][0]["index"];
    assert_eq!(duplicate["status"], original["status"], "{retry}");
    assert_eq!(duplicate["_id"], original["_id"], "{retry}");
    assert_eq!(duplicate["_seq_no"], original["_seq_no"], "{retry}");
    assert_eq!(
        duplicate["_primary_term"], original["_primary_term"],
        "{retry}"
    );
    assert_eq!(duplicate["_version"], original["_version"], "{retry}");
    let conflict = &retry["items"][1]["index"];
    assert_eq!(conflict["status"], 409, "{retry}");
    assert_eq!(
        conflict["error"]["type"], "operation_id_conflict",
        "{retry}"
    );
    assert_eq!(
        harness.engines[&(primary, 0)].sequence_stats(),
        before_retry
    );
}
