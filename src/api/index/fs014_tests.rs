use super::*;

fn documents(count: usize) -> String {
    (0..count)
        .map(|value| {
            format!(
                "{}\n{}\n",
                json!({"index":{"_index":INDEX,"_id":format!("fs014-{value}")}}),
                json!({"value":value})
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs014_many_batches_refresh_all_copies_once_and_preserve_exact_operation_sequences() {
    let harness = RefreshCluster::start(1).await;
    let response = harness.bulk(0, true, "?refresh=true", documents(600)).await;
    assert_eq!(response["errors"], false, "{response}");
    let items = response["items"].as_array().unwrap();
    assert_eq!(items.len(), 600);
    for (value, item) in items.iter().enumerate() {
        let item = &item["index"];
        assert_eq!(item["_id"], format!("fs014-{value}"), "{item}");
        assert_eq!(item["_seq_no"], value, "{item}");
        assert_eq!(item["_version"], 1, "{item}");
        assert_eq!(item["_shards"]["total"], 3, "{item}");
        assert_eq!(item["_shards"]["successful"], 3, "{item}");
        harness.assert_refresh_response(item);
    }
    assert_eq!(
        harness
            .cluster
            .nodes
            .iter()
            .map(|node| node.bulk_refresh_requests.load(Ordering::Relaxed))
            .sum::<usize>(),
        1
    );
    assert_eq!(
        harness
            .cluster
            .nodes
            .iter()
            .map(|node| node.refresh_requests.load(Ordering::Relaxed))
            .sum::<usize>(),
        2
    );
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let routing = &state.indices[INDEX].shard_routing[&0];
    let query: SearchRequest = serde_json::from_value(json!({"size":600})).unwrap();
    for node in std::iter::once(&routing.primary).chain(&routing.in_sync_replicas) {
        let (hits, total, _) = harness.cluster.nodes[0]
            .state
            .transport_client
            .forward_search_dsl_to_shard(&state.nodes[node], INDEX, 0, &query)
            .await
            .unwrap();
        assert_eq!(total, 600);
        assert_eq!(hits.len(), 600);
        for hit in hits {
            let value = hit["_source"]["value"].as_u64().unwrap();
            assert_eq!(hit["_id"], format!("fs014-{value}"));
        }
    }
    for engine in harness.engines.values() {
        assert_eq!(engine.local_checkpoint(), Some(599));
        let document = engine
            .get_document_with_metadata("fs014-599", true)
            .unwrap()
            .unwrap();
        assert_eq!(document.seq_no, 599);
        assert_eq!(document.source["value"], 599);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs014_replica_failure_preserves_failed_range_and_fences_later_batches_at_copy_floor() {
    let harness = RefreshCluster::start(1).await;
    harness.set_minimum_durable_copies(3).await;
    let (status, seed) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/seed"),
            Some(json!({"value":-1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{seed}");
    let replica = harness.replica(0);
    super::write_failures::fail_replica_once(&harness);
    let response = harness.bulk(0, true, "", documents(600)).await;
    assert_eq!(response["errors"], true, "{response}");
    let items = response["items"].as_array().unwrap();
    assert_eq!(items.len(), 600);
    for (position, item) in items.iter().enumerate() {
        let item = &item["index"];
        assert_eq!(item["_id"], format!("fs014-{position}"));
        assert!(item.get("error").is_some(), "{position}: {item}");
        assert!(item.get("_shards").is_none(), "{position}: {item}");
        if position < 64 {
            assert_eq!(item["error"]["type"], "write_outcome_unknown", "{item}");
            assert_eq!(item["_seq_no"], position + 1, "{item}");
            assert!(item["_primary_term"].as_u64().is_some_and(|term| term > 0));
        } else {
            assert!(item.get("_seq_no").is_none(), "{position}: {item}");
            assert!(item.get("_primary_term").is_none(), "{position}: {item}");
        }
    }
    for ((node, _), engine) in &harness.engines {
        if *node != replica {
            assert_eq!(engine.local_checkpoint(), Some(64));
            assert!(
                engine
                    .get_document_with_metadata("fs014-63", true)
                    .unwrap()
                    .is_some()
            );
            assert!(
                engine
                    .get_document_with_metadata("fs014-64", true)
                    .unwrap()
                    .is_none()
            );
            assert!(
                engine
                    .get_document_with_metadata("fs014-599", true)
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[cfg(feature = "protocol-trace")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs014_cancelled_request_retains_issued_rpc_admission_and_stops_later_units() {
    cancellation(false).await;
}

#[cfg(feature = "protocol-trace")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs014_http_disconnect_retains_issued_rpc_admission_and_stops_later_units() {
    cancellation(true).await;
}

#[cfg(feature = "protocol-trace")]
async fn cancellation(http: bool) {
    use crate::api::index::RefreshParam;
    use axum::extract::{Query, State};

    let harness = RefreshCluster::start(1).await;
    harness.set_minimum_durable_copies(3).await;
    let (status, seed) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/seed"),
            Some(json!({"value":-1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{seed}");
    let current = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let metadata = &current.indices[INDEX];
    let primary = harness
        .cluster
        .nodes
        .iter()
        .find(|node| node.state.local_node_id == metadata.shard_routing[&0].primary)
        .unwrap();
    let mut pause = primary
        .state
        .transport_client
        .arm_primary_replication_pause_for_test(crate::protocol_trace::OperationKey::new(
            metadata.uuid.as_str(),
            0,
            seed["_primary_term"].as_u64().unwrap(),
            1,
        ))
        .unwrap();
    let mut state = harness.cluster.nodes[0].state.clone();
    state.transport_client = state
        .transport_client
        .with_cluster_manager(state.cluster_manager.clone());
    if !http {
        state.bulk_runtime = Arc::new(
            crate::api::index::BulkRuntime::new(&crate::config::AppConfig {
                bulk_max_concurrent_requests: 1,
                ..Default::default()
            })
            .unwrap(),
        );
    }
    let runtime = state.bulk_runtime.clone();
    let available = runtime.available_permits_for_test();
    let request = if http {
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(90))
            .build()
            .unwrap();
        let url = format!("{}/_bulk", harness.cluster.nodes[0].url);
        tokio::spawn(async move {
            client
                .post(url)
                .header("content-type", "application/x-ndjson")
                .body(documents(600))
                .send()
                .await
                .unwrap();
        })
    } else {
        tokio::spawn(async move {
            crate::api::index::bulk::bulk_index_global(
                State(state),
                None,
                Query(RefreshParam::default()),
                axum::extract::RawQuery(None),
                axum::body::Body::from(documents(600)),
            )
            .await;
        })
    };
    tokio::time::timeout(Duration::from_secs(30), pause.wait_until_reached())
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(10), async {
        while runtime.reserved_spool_bytes_for_test() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        runtime.available_permits_for_test(),
        available - 1,
        "issued RPC owns admission after all request files close"
    );
    pause.release().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while runtime.available_permits_for_test() != available {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for engine in harness.engines.values() {
        assert_eq!(engine.local_checkpoint(), Some(64));
        let first = engine
            .get_document_with_metadata("fs014-0", true)
            .unwrap()
            .unwrap();
        assert_eq!(first.seq_no, 1);
        let last = engine
            .get_document_with_metadata("fs014-63", true)
            .unwrap()
            .unwrap();
        assert_eq!(last.seq_no, 64);
        assert_eq!(last.source["value"], 63);
        assert!(
            engine
                .get_document_with_metadata("fs014-64", true)
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .get_document_with_metadata("fs014-599", true)
                .unwrap()
                .is_none()
        );
    }
}
