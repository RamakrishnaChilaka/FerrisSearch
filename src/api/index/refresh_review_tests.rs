use super::*;
use crate::transport::proto::{ShardDocRequest, shard_copy_refresh_result::Outcome};

fn refresh_rpcs(harness: &RefreshCluster) -> usize {
    harness
        .cluster
        .nodes
        .iter()
        .map(|node| node.refresh_requests.load(Ordering::Relaxed))
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
    request.set_timeout(Duration::from_secs(2));
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
