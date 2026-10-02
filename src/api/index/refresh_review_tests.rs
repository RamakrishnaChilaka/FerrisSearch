use super::*;
use crate::transport::proto::{
    ShardBulkOpKind, ShardBulkOperation, ShardCopyRefreshRequest, ShardDocRequest,
    shard_copy_refresh_result::Outcome,
};

fn refresh_rpcs(harness: &RefreshCluster) -> usize {
    harness
        .cluster
        .nodes
        .iter()
        .map(|node| node.refresh_requests.load(Ordering::Relaxed))
        .sum()
}

fn bulk_refresh_rounds(harness: &RefreshCluster) -> usize {
    harness
        .cluster
        .nodes
        .iter()
        .map(|node| node.bulk_refresh_requests.load(Ordering::Relaxed))
        .sum()
}

fn bulk_documents(action: &str, count: usize, offset: usize) -> String {
    (0..count)
        .map(|item| {
            format!(
                "{}\n{}\n",
                json!({(action): {"_index": INDEX, "_id": format!("review-{item}")}}),
                if action == "update" {
                    json!({"doc": {"value": item + offset}})
                } else {
                    json!({"value": item + offset})
                }
            )
        })
        .collect()
}

async fn assert_all_documents_visible(harness: &RefreshCluster, count: usize, offset: usize) {
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let query: SearchRequest = serde_json::from_value(json!({"size": count})).unwrap();
    let routing = &state.indices[INDEX].shard_routing[&0];
    for node_id in std::iter::once(&routing.primary).chain(&routing.in_sync_replicas) {
        let (hits, total, _) = harness.cluster.nodes[0]
            .state
            .transport_client
            .forward_search_dsl_to_shard(&state.nodes[node_id], INDEX, 0, &query)
            .await
            .unwrap();
        assert_eq!(total, count, "{node_id}");
        let values = hits
            .into_iter()
            .map(|hit| {
                (
                    hit["_id"].as_str().unwrap().to_string(),
                    hit["_source"]["value"].as_u64().unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();
        for item in 0..count {
            assert_eq!(
                values[&format!("review-{item}")],
                (item + offset) as u64,
                "{node_id}"
            );
        }
    }
}

async fn large_bulk(action: &str) {
    const COUNT: usize = 100;
    let harness = RefreshCluster::start(1).await;
    if action == "update" {
        let seed = harness
            .bulk(
                0,
                false,
                "?refresh=false",
                bulk_documents("index", COUNT, 0),
            )
            .await;
        assert_eq!(seed["errors"], false, "{seed}");
        assert_eq!(refresh_rpcs(&harness), 0);
    }
    let before = refresh_rpcs(&harness);
    let offset = if action == "update" { COUNT } else { 0 };
    let (body, elapsed) = harness
        .bulk_measured(
            0,
            false,
            "?refresh=true",
            bulk_documents(action, COUNT, offset),
        )
        .await;
    let items = body["items"].as_array().unwrap();
    let acknowledged = items
        .iter()
        .filter(|item| item[action].get("error").is_none())
        .count();
    let calls = refresh_rpcs(&harness) - before;
    eprintln!(
        "REFRESH_REVIEW_BULK action={action} items={COUNT} elapsed_ms={} acknowledged={acknowledged} refresh_rpcs={calls} errors={}",
        elapsed.as_millis(),
        body["errors"],
    );
    assert_eq!(items.len(), COUNT, "{body}");
    assert_eq!(body["errors"], false, "{body}");
    assert_eq!(acknowledged, COUNT, "{body}");
    for item in items {
        let result = &item[action];
        assert_eq!(
            result["status"],
            if action == "create" { 201 } else { 200 },
            "{item}"
        );
        assert!(result["_seq_no"].is_u64(), "{item}");
        assert!(result["_primary_term"].as_u64().unwrap() > 0, "{item}");
        harness.assert_refresh_response(result);
    }
    assert_eq!(
        calls, 2,
        "bulk refresh must be O(touched shards), not O(items)"
    );
    assert_eq!(bulk_refresh_rounds(&harness), 1);
    assert_all_documents_visible(&harness, COUNT, offset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_create_bulk_100_is_acknowledged_with_one_round() {
    large_bulk("create").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_update_bulk_100_is_acknowledged_with_one_round() {
    large_bulk("update").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_slow_replica_uses_remaining_deadline_and_preserves_ack() {
    let harness = RefreshCluster::start(1).await;
    // Keep both primary and replica waits deadline-limited, not production-cap-limited.
    for node in &harness.cluster.nodes {
        node.refresh_service
            .set_copy_refresh_limit_for_test(Duration::from_secs(20));
    }
    let replica = harness.replica(0);
    let (committed_tx, committed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    harness.engines[&(replica, 0)]
        .text_engine()
        .pause_after_refresh_commit_for_test(committed_tx, release_rx);
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary_id = &state.indices[INDEX].shard_routing[&0].primary;
    let primary = &state.nodes[primary_id];
    let mut client = harness.cluster.nodes[0]
        .state
        .transport_client
        .connect(&primary.host, primary.transport_port)
        .await
        .unwrap();
    let mut request = crate::transport::request_with_cluster_state_version(
        ShardDocRequest {
            index_name: INDEX.into(),
            shard_id: 0,
            doc_id: "slow".into(),
            payload_json: serde_json::to_vec(&json!({"value": 42})).unwrap(),
            index_uuid: Some(state.indices[INDEX].uuid.to_string()),
            refresh: true,
            ..Default::default()
        },
        state.version,
    );
    request.set_timeout(Duration::from_secs(10));
    let started = std::time::Instant::now();
    let response = tokio::spawn(async move { client.index_doc(request).await });
    let paused =
        tokio::task::spawn_blocking(move || committed_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    if let Err(error) = paused {
        let _ = release_tx.send(());
        panic!("replica refresh did not reach the deterministic pause: {error}");
    }

    let received = response.await.unwrap();
    let _ = release_tx.send(());
    eprintln!(
        "REFRESH_REVIEW_SLOW elapsed_ms={} acknowledged={} result={received:?}",
        started.elapsed().as_millis(),
        received
            .as_ref()
            .is_ok_and(|response| response.get_ref().success),
    );
    let response = received
        .expect("refresh budget must expire before the enclosing request deadline")
        .into_inner();
    assert!(response.success, "{response:?}");
    assert_eq!(response.seq_no, Some(0));
    let report = response.write_refresh.expect("requested refresh report");
    crate::transport::write_refresh::validate_write_refresh_response(
        Some(&report),
        true,
        primary_id,
    )
    .unwrap();
    let failures = report
        .copies
        .iter()
        .filter(|copy| matches!(copy.outcome, Some(Outcome::Error(_))))
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1, "{report:?}");
    assert_eq!(
        failures[0].node_id,
        harness.cluster.nodes[replica].state.local_node_id
    );
    assert!(
        matches!(&failures[0].outcome, Some(Outcome::Error(reason)) if reason.contains("refresh timed out")),
        "{report:?}"
    );
    let mut body = json!({"status": 201, "result": "created", "_seq_no": response.seq_no, "_primary_term": response.primary_term});
    crate::transport::write_refresh::add_write_refresh_to_response(&mut body, INDEX, 0, &report);
    assert_eq!(body["status"], 201);
    assert_eq!(body["_shards"]["failed"], 1);
    assert_eq!(body["_shards"]["successful"], 2);
    assert_eq!(body["forced_refresh"], true);
    for node in 1..4 {
        let engine = harness.engines[&(node, 0)].clone();
        let document =
            tokio::task::spawn_blocking(move || engine.get_document_with_metadata("slow", true))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(document.source["value"], 42);
        assert_eq!(document.seq_no, 0);
    }
}

async fn slow_http_copy(primary_copy: bool) {
    let harness = RefreshCluster::start(1).await;
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary_id = &state.indices[INDEX].shard_routing[&0].primary;
    let primary = harness
        .cluster
        .nodes
        .iter()
        .position(|node| node.state.local_node_id == *primary_id)
        .unwrap();
    let paused_copy = if primary_copy {
        primary
    } else {
        harness.replica(0)
    };
    harness.cluster.nodes[primary]
        .refresh_service
        .set_copy_refresh_limit_for_test(Duration::from_secs(1));
    let (committed_tx, committed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    harness.engines[&(paused_copy, 0)]
        .text_engine()
        .pause_after_refresh_commit_for_test(committed_tx, release_rx);
    let client = harness.cluster.client.clone();
    let url = format!(
        "{}/{INDEX}/_doc/http-slow?refresh=true",
        harness.cluster.nodes[0].url
    );
    let response = tokio::spawn(async move {
        let response = client
            .put(url)
            .json(&json!({"value": 43}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        (status, response.json::<Value>().await.unwrap())
    });
    let paused =
        tokio::task::spawn_blocking(move || committed_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    if let Err(error) = paused {
        let _ = release_tx.send(());
        panic!("configured refresh did not reach pause: {error}");
    }
    let received = tokio::time::timeout(Duration::from_secs(5), response).await;
    let _ = release_tx.send(());
    let (status, body) = received
        .expect("configured copy budget must bound the HTTP response")
        .unwrap();
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created", "{body}");
    assert_eq!(body["_seq_no"], 0, "{body}");
    assert_eq!(body["_shards"]["total"], 3, "{body}");
    assert_eq!(body["_shards"]["successful"], 2, "{body}");
    assert_eq!(body["_shards"]["failed"], 1, "{body}");
    assert_eq!(
        body["_shards"]["failures"][0]["node"],
        harness.cluster.nodes[paused_copy].state.local_node_id
    );
    assert!(
        body["_shards"]["failures"][0]["reason"]["reason"]
            .as_str()
            .unwrap()
            .contains("refresh timed out"),
        "{body}"
    );
    assert!(body.get("error").is_none(), "{body}");
    if primary_copy {
        assert!(body.get("forced_refresh").is_none(), "{body}");
    } else {
        assert_eq!(body["forced_refresh"], true, "{body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_configurable_replica_budget_preserves_http_ack() {
    slow_http_copy(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_configurable_local_budget_preserves_http_ack() {
    slow_http_copy(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_ordered_bulk_rpc_coalesces_acknowledged_items() {
    let harness = RefreshCluster::start(1).await;
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary = &state.nodes[&state.indices[INDEX].shard_routing[&0].primary];
    let operations = [
        ("direct-a", json!({"value": 10}), ShardBulkOpKind::Create),
        ("direct-a", json!({"value": 11}), ShardBulkOpKind::Create),
        ("direct-b", json!({"value": 12}), ShardBulkOpKind::Create),
        ("missing", Value::Null, ShardBulkOpKind::Delete),
    ]
    .into_iter()
    .map(|(id, source, kind)| {
        (
            id.to_string(),
            source,
            ShardBulkOperation {
                kind: kind as i32,
                ..Default::default()
            },
        )
    })
    .collect::<Vec<_>>();
    let items = harness.cluster.nodes[0]
        .state
        .transport_client
        .forward_bulk_operations_to_shard(primary, INDEX, 0, &operations, true)
        .await
        .unwrap();
    assert_eq!(
        items.iter().map(|item| item.status).collect::<Vec<_>>(),
        [201, 409, 201, 404]
    );
    assert_eq!(items[1].error_type, "version_conflict_engine_exception");
    assert!(items[1].write_refresh.is_none());
    let report = items[0].write_refresh.as_ref().unwrap();
    assert_eq!(report, items[2].write_refresh.as_ref().unwrap());
    assert_eq!(report, items[3].write_refresh.as_ref().unwrap());
    assert_eq!(refresh_rpcs(&harness), 2);
    assert_eq!(
        bulk_refresh_rounds(&harness),
        0,
        "the direct bulk handler owns its round"
    );
    harness
        .assert_visible(&[
            (0, "direct-a".into(), Some(10)),
            (0, "direct-b".into(), Some(12)),
            (0, "missing".into(), None),
        ])
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_bulk_shares_report_without_changing_item_outcomes() {
    let harness = RefreshCluster::start(1).await;
    let seed = harness
        .bulk(0, false, "?refresh=false", bulk_documents("index", 1, 0))
        .await;
    assert_eq!(seed["errors"], false, "{seed}");
    let body = harness
        .bulk(
            0,
            true,
            "?refresh=true",
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
                json!({"update": {"_index": INDEX, "_id": "review-0"}}),
                json!({"doc": {"value": 0}}),
                json!({"create": {"_index": INDEX, "_id": "review-0"}}),
                json!({"value": 999}),
                json!({"update": {"_index": INDEX, "_id": "review-0"}}),
                json!({"doc": {"value": 1}}),
                json!({"delete": {"_index": INDEX, "_id": "missing"}}),
            ),
        )
        .await;
    assert_eq!(body["errors"], true, "{body}");
    assert_eq!(body["items"][0]["update"]["result"], "noop", "{body}");
    assert_eq!(body["items"][1]["create"]["status"], 409, "{body}");
    assert_eq!(
        body["items"][1]["create"]["error"]["type"], "version_conflict_engine_exception",
        "{body}"
    );
    assert!(
        body["items"][1]["create"].get("_shards").is_none(),
        "{body}"
    );
    assert_eq!(body["items"][2]["update"]["result"], "updated", "{body}");
    assert_eq!(body["items"][3]["delete"]["status"], 404, "{body}");
    assert_eq!(body["items"][3]["delete"]["result"], "not_found", "{body}");
    for (position, action) in [(0, "update"), (2, "update"), (3, "delete")] {
        harness.assert_refresh_response(&body["items"][position][action]);
    }
    assert_eq!(refresh_rpcs(&harness), 2);
    assert_eq!(bulk_refresh_rounds(&harness), 1);
    harness
        .assert_visible(&[(0, "review-0".into(), Some(1)), (0, "missing".into(), None)])
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_empty_noop_and_all_error_bulk_have_no_refresh_round() {
    let harness = RefreshCluster::start(1).await;
    let seed = harness
        .bulk(0, false, "?refresh=false", bulk_documents("index", 1, 0))
        .await;
    assert_eq!(seed["errors"], false, "{seed}");
    let mut notifications = Vec::new();
    for engine in harness.engines.values() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        engine
            .text_engine()
            .notify_before_refresh_writer_for_test(sender);
        notifications.push(receiver);
    }
    let empty = harness.bulk(0, false, "?refresh=true", String::new()).await;
    assert_eq!(empty["items"], json!([]), "{empty}");
    let noop = harness
        .bulk(0, false, "?refresh=true", bulk_documents("update", 1, 0))
        .await;
    assert_eq!(noop["items"][0]["update"]["result"], "noop", "{noop}");
    assert!(
        noop["items"][0]["update"].get("forced_refresh").is_none(),
        "{noop}"
    );
    let conflict = harness
        .bulk(0, false, "?refresh=true", bulk_documents("create", 1, 1))
        .await;
    assert_eq!(conflict["errors"], true, "{conflict}");
    assert_eq!(conflict["items"][0]["create"]["status"], 409, "{conflict}");
    assert_eq!(refresh_rpcs(&harness), 0);
    assert_eq!(bulk_refresh_rounds(&harness), 0);
    for mut receiver in notifications {
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_bulk_phase_rpc_failure_keeps_known_acknowledgements() {
    let harness = RefreshCluster::start(1).await;
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary = &state.indices[INDEX].shard_routing[&0].primary;
    harness
        .cluster
        .nodes
        .iter()
        .find(|node| node.state.local_node_id == *primary)
        .unwrap()
        .reject_refresh_requests
        .store(true, Ordering::Relaxed);
    let body = harness
        .bulk(0, false, "?refresh=true", bulk_documents("create", 3, 0))
        .await;
    assert_eq!(body["errors"], false, "{body}");
    for item in body["items"].as_array().unwrap() {
        let item = &item["create"];
        assert_eq!(item["status"], 201, "{item}");
        assert!(item["_seq_no"].is_u64(), "{item}");
        assert!(item["_primary_term"].as_u64().unwrap() > 0, "{item}");
        assert_eq!(item["_shards"]["failed"], 1, "{item}");
        assert_eq!(item["_shards"]["failures"][0]["node"], *primary, "{item}");
        assert!(
            item["_shards"]["failures"][0]["reason"]["reason"]
                .as_str()
                .unwrap()
                .contains(&tonic::Status::unimplemented("").to_string()),
            "{item}"
        );
        assert!(item.get("error").is_none(), "{item}");
        assert!(item.get("forced_refresh").is_none(), "{item}");
    }
    assert_eq!(bulk_refresh_rounds(&harness), 1);
    assert_eq!(refresh_rpcs(&harness), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_review_primary_bulk_rpc_rejects_malformed_and_stale_fences() {
    let harness = RefreshCluster::start(1).await;
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/rpc-fence?refresh=false"),
            Some(json!({"value": 99})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary = &state.nodes[&state.indices[INDEX].shard_routing[&0].primary];
    let mut client = harness.cluster.nodes[0]
        .state
        .transport_client
        .connect(&primary.host, primary.transport_port)
        .await
        .unwrap();
    let valid = ShardCopyRefreshRequest {
        index_name: INDEX.into(),
        index_uuid: state.indices[INDEX].uuid.to_string(),
        shard_id: 0,
        primary_node_id: primary.id.clone(),
        primary_term: body["_primary_term"].as_u64(),
        target_allocation_id: state.primary_allocation_id(INDEX, 0),
    };
    for request in [
        ShardCopyRefreshRequest {
            primary_term: None,
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            target_allocation_id: Some(0),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            index_uuid: String::new(),
            ..valid.clone()
        },
    ] {
        let error = client
            .refresh_shard_writes(crate::transport::request_with_cluster_state_version(
                request,
                state.version,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
    }
    for request in [
        ShardCopyRefreshRequest {
            primary_term: valid.primary_term.map(|term| term + 1),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            target_allocation_id: valid.target_allocation_id.map(|id| id + 1),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            index_uuid: "obsolete-index-uuid".into(),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            primary_node_id: "node-1".into(),
            ..valid.clone()
        },
    ] {
        let error = client
            .refresh_shard_writes(crate::transport::request_with_cluster_state_version(
                request,
                state.version,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    }
    assert_eq!(
        refresh_rpcs(&harness),
        0,
        "invalid identity must not reach copy maintenance"
    );
    let report = client
        .refresh_shard_writes(crate::transport::request_with_cluster_state_version(
            valid,
            state.version,
        ))
        .await
        .unwrap()
        .into_inner();
    crate::transport::write_refresh::validate_write_refresh_response(
        Some(&report),
        true,
        &primary.id,
    )
    .unwrap();
    assert_eq!(report.copies.len(), 3);
    assert!(
        report
            .copies
            .iter()
            .all(|copy| matches!(copy.outcome, Some(Outcome::Refreshed(_))))
    );
    assert_eq!(refresh_rpcs(&harness), 2);
    harness
        .assert_visible(&[(0, "rpc-fence".into(), Some(99))])
        .await;
}
