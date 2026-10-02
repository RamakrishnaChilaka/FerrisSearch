use super::*;
use crate::cluster::state::IndexMetadata;
use crate::engine::{CompositeEngine, SearchEngine};
use crate::search::SearchRequest;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

const INDEX: &str = "refresh-copies";

#[path = "refresh_review_tests.rs"]
mod review;

struct RefreshCluster {
    cluster: ForwardingCluster,
    engines: HashMap<(usize, u32), Arc<CompositeEngine>>,
}

impl RefreshCluster {
    async fn start(shards: u32) -> Self {
        Self::start_with_topology(
            shards,
            &["node-2".into(), "node-3".into(), "node-4".into()],
            2,
        )
        .await
    }

    async fn start_with_topology(shards: u32, data_nodes: &[String], in_sync: usize) -> Self {
        let cluster = ForwardingCluster::start_with_roles(&[
            vec![NodeRole::Master],
            vec![NodeRole::Data],
            vec![NodeRole::Data],
            vec![NodeRole::Data],
        ])
        .await;
        let metadata = IndexMetadata::from_create_request_body(
            INDEX,
            &json!({
                "settings": {
                    "number_of_shards": shards, "number_of_replicas": 2,
                    "refresh_interval_ms": 3_600_000, "flush_threshold_bytes": 0
                },
                "mappings": {"properties": {"value": {"type": "integer"}}}
            }),
            data_nodes,
        )
        .unwrap();
        let leader = &cluster.nodes[0].state;
        leader
            .raft
            .client_write(ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data
            .into_result()
            .unwrap();
        for shard in 0..shards {
            let state = leader.cluster_manager.get_state();
            let metadata = &state.indices[INDEX];
            let routing = &metadata.shard_routing[&shard];
            leader
                .raft
                .client_write(ClusterCommand::ActivatePrimary {
                    index_name: INDEX.into(),
                    index_uuid: metadata.uuid.to_string(),
                    shard_id: shard,
                    primary: routing.primary.clone(),
                    allocation_id: state.primary_allocation_id(INDEX, shard).unwrap(),
                    expected_term: routing.primary_term,
                })
                .await
                .unwrap()
                .data
                .into_result()
                .unwrap();
        }
        let state = leader.cluster_manager.get_state();
        let mut engines = HashMap::new();
        for shard in 0..shards {
            let metadata = &state.indices[INDEX];
            let routing = &metadata.shard_routing[&shard];
            for node_id in std::iter::once(&routing.primary).chain(&routing.replicas) {
                let node = cluster
                    .nodes
                    .iter()
                    .position(|node| node.state.local_node_id == *node_id)
                    .unwrap();
                let manager = cluster.nodes[node].state.shard_manager.clone();
                let directory = cluster.nodes[node]
                    ._data
                    .path()
                    .join(metadata.uuid.as_str())
                    .join(format!("shard_{shard}"));
                let uuid = metadata.uuid.to_string();
                let mappings = metadata.mappings.clone();
                let allocation = state.shard_allocation_id(INDEX, shard, node_id).unwrap();
                let term = routing.primary_term;
                let engine = tokio::task::spawn_blocking(move || {
                    manager
                        .initialize_copy_identity_for_test(INDEX, shard, &uuid, allocation, term)
                        .unwrap();
                    let engine = Arc::new(
                        CompositeEngine::new_with_mappings(
                            directory,
                            Duration::from_secs(3600),
                            &mappings,
                            crate::wal::TranslogDurability::Request,
                            Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
                        )
                        .unwrap(),
                    );
                    manager.insert_shard_for_test(INDEX, shard, engine.clone());
                    engine
                })
                .await
                .unwrap();
                engines.insert((node, shard), engine);
            }
            // All fixture copies are identically empty before their Raft admission.
            for replica in routing.replicas.iter().take(in_sync) {
                leader
                    .raft
                    .client_write(ClusterCommand::MarkReplicaInSync {
                        index_name: INDEX.into(),
                        index_uuid: metadata.uuid.to_string(),
                        shard_id: shard,
                        replica: replica.clone(),
                        allocation_id: state.shard_allocation_id(INDEX, shard, replica).unwrap(),
                        primary: routing.primary.clone(),
                        primary_term: routing.primary_term,
                    })
                    .await
                    .unwrap()
                    .data
                    .into_result()
                    .unwrap();
            }
        }
        let version = leader.cluster_manager.version();
        for node in &cluster.nodes {
            node.state
                .cluster_manager
                .wait_for_version(version)
                .await
                .unwrap();
        }
        assert!(cluster.nodes[0].state.shard_manager.all_shards().is_empty());
        for shard in 0..shards {
            let routing = &leader.cluster_manager.get_state().indices[INDEX].shard_routing[&shard];
            let primary = cluster
                .nodes
                .iter()
                .find(|node| node.state.local_node_id == routing.primary)
                .unwrap();
            assert!(
                !primary.state.raft.is_leader(),
                "primary must be a follower"
            );
            assert_eq!(routing.in_sync_replicas.len(), in_sync);
        }
        Self { cluster, engines }
    }

    fn replica(&self, shard: u32) -> usize {
        let state = self.cluster.nodes[0].state.cluster_manager.get_state();
        let replica = &state.indices[INDEX].shard_routing[&shard].in_sync_replicas[0];
        self.cluster
            .nodes
            .iter()
            .position(|node| node.state.local_node_id == *replica)
            .unwrap()
    }

    fn doc_id(&self, shard: u32, label: &str) -> String {
        let count = self.cluster.nodes[0]
            .state
            .cluster_manager
            .get_state()
            .indices[INDEX]
            .number_of_shards;
        (0..10_000)
            .map(|suffix| format!("{label}-{suffix}"))
            .find(|id| crate::engine::routing::calculate_shard(id, count) == shard)
            .unwrap()
    }

    async fn fixture_refresh(&self) {
        for engine in self.engines.values() {
            let engine = engine.clone();
            crate::worker::spawn_engine_maintenance("refresh test fixture", move || {
                engine.refresh()
            })
            .await
            .unwrap();
        }
    }

    async fn assert_visible(&self, expected: &[(u32, String, Option<i64>)]) {
        let state = self.cluster.nodes[0].state.cluster_manager.get_state();
        let request: SearchRequest = serde_json::from_value(json!({"size": 100})).unwrap();
        let mut failures = Vec::new();
        for (shard, doc_id, value) in expected {
            let routing = &state.indices[INDEX].shard_routing[shard];
            for node_id in std::iter::once(&routing.primary).chain(&routing.in_sync_replicas) {
                let (hits, _, _) = self.cluster.nodes[0]
                    .state
                    .transport_client
                    .forward_search_dsl_to_shard(&state.nodes[node_id], INDEX, *shard, &request)
                    .await
                    .unwrap();
                let found = hits.iter().find(|hit| hit["_id"] == *doc_id);
                let actual = found.map(|hit| hit["_source"]["value"].as_i64().unwrap());
                if actual != *value {
                    failures.push(format!(
                        "{node_id}/{INDEX}/{shard}/{doc_id}: expected {value:?}, searched {actual:?}"
                    ));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "copies were not refreshed before the response:\n{}",
            failures.join("\n")
        );
    }

    fn assert_refresh_response(&self, body: &Value) {
        assert_eq!(body["_shards"]["total"], 3, "{body}");
        assert_eq!(body["_shards"]["successful"], 3, "{body}");
        assert_eq!(body["_shards"]["failed"], 0, "{body}");
        assert_eq!(body["forced_refresh"], true, "{body}");
    }

    async fn bulk(&self, node: usize, global: bool, refresh: &str, body: String) -> Value {
        self.bulk_measured(node, global, refresh, body).await.0
    }

    async fn bulk_measured(
        &self,
        node: usize,
        global: bool,
        refresh: &str,
        body: String,
    ) -> (Value, Duration) {
        let route = if global {
            "/_bulk".to_string()
        } else {
            format!("/{INDEX}/_bulk")
        };
        let started = std::time::Instant::now();
        let response = self
            .cluster
            .client
            .post(format!("{}{route}{refresh}", self.cluster.nodes[node].url))
            .timeout(Duration::from_secs(90))
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        (response.json().await.unwrap(), started.elapsed())
    }
}

#[derive(Clone, Copy)]
enum Mutation {
    Index,
    AutoIndex,
    Create,
    Update,
    Delete,
}

async fn single_visibility(mutation: Mutation, replica_coordinator: bool) {
    let harness = RefreshCluster::start(1).await;
    let coordinator = if replica_coordinator {
        harness.replica(0)
    } else {
        0
    };
    if matches!(mutation, Mutation::Update | Mutation::Delete) {
        let (status, body) = harness
            .cluster
            .request(
                0,
                reqwest::Method::PUT,
                &format!("/{INDEX}/_doc/doc"),
                Some(json!({"value": 1})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        harness.fixture_refresh().await;
    }
    let (method, route, source, status) = match mutation {
        Mutation::Index => (
            reqwest::Method::PUT,
            "_doc/doc?refresh=true",
            Some(json!({"value": 2})),
            StatusCode::CREATED,
        ),
        Mutation::AutoIndex => (
            reqwest::Method::POST,
            "_doc?refresh",
            Some(json!({"value": 2})),
            StatusCode::CREATED,
        ),
        Mutation::Create => (
            reqwest::Method::PUT,
            "_create/doc?refresh=",
            Some(json!({"value": 2})),
            StatusCode::CREATED,
        ),
        Mutation::Update => (
            reqwest::Method::POST,
            "_update/doc?refresh=true",
            Some(json!({"doc": {"value": 2}})),
            StatusCode::OK,
        ),
        Mutation::Delete => (
            reqwest::Method::DELETE,
            "_doc/doc?refresh=true",
            None,
            StatusCode::OK,
        ),
    };
    let (actual_status, body) = harness
        .cluster
        .request(coordinator, method, &format!("/{INDEX}/{route}"), source)
        .await;
    assert_eq!(actual_status, status, "{body}");
    assert!(body["_seq_no"].is_u64(), "{body}");
    assert!(body["_primary_term"].as_u64().unwrap() > 0, "{body}");
    harness
        .assert_visible(&[(
            0,
            body["_id"].as_str().unwrap().to_string(),
            (!matches!(mutation, Mutation::Delete)).then_some(2),
        )])
        .await;
    harness.assert_refresh_response(&body);
    assert_eq!(
        harness
            .cluster
            .nodes
            .iter()
            .map(|node| node.refresh_requests.load(Ordering::Relaxed))
            .sum::<usize>(),
        2,
        "one dedicated refresh RPC per in-sync replica"
    );
}

macro_rules! visibility_test {
    ($name:ident, $kind:ident, $replica:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            single_visibility(Mutation::$kind, $replica).await;
        }
    };
}

visibility_test!(refresh_regression_index_shardless_coordinator, Index, false);
visibility_test!(refresh_regression_index_replica_coordinator, Index, true);
visibility_test!(
    refresh_regression_auto_index_shardless_coordinator,
    AutoIndex,
    false
);
visibility_test!(
    refresh_regression_auto_index_replica_coordinator,
    AutoIndex,
    true
);
visibility_test!(
    refresh_regression_create_shardless_coordinator,
    Create,
    false
);
visibility_test!(refresh_regression_create_replica_coordinator, Create, true);
visibility_test!(
    refresh_regression_update_shardless_coordinator,
    Update,
    false
);
visibility_test!(refresh_regression_update_replica_coordinator, Update, true);
visibility_test!(
    refresh_regression_delete_shardless_coordinator,
    Delete,
    false
);
visibility_test!(refresh_regression_delete_replica_coordinator, Delete, true);

async fn bulk_visibility(global: bool, replica_coordinator: bool, mixed: bool) {
    let harness = RefreshCluster::start(3).await;
    let coordinator = if replica_coordinator {
        harness.replica(0)
    } else {
        0
    };
    let mut ndjson = String::new();
    let mut expected = Vec::new();
    for shard in 0..3 {
        let first = harness.doc_id(shard, "first");
        ndjson.push_str(&format!(
            "{}\n{}\n",
            json!({"index": {"_index": INDEX, "_id": first}}),
            json!({"value": 1})
        ));
        if mixed {
            let second = harness.doc_id(shard, "second");
            ndjson.push_str(&format!(
                "{}\n{}\n{}\n{}\n{}\n",
                json!({"create": {"_index": INDEX, "_id": second}}),
                json!({"value": 2}),
                json!({"update": {"_index": INDEX, "_id": first}}),
                json!({"doc": {"value": 3}}),
                json!({"delete": {"_index": INDEX, "_id": second}})
            ));
            expected.push((shard, second, None));
        }
        expected.push((shard, first, Some(if mixed { 3 } else { 1 })));
    }
    let body = harness
        .bulk(coordinator, global, "?refresh=true", ndjson)
        .await;
    assert_eq!(body["errors"], false, "{body}");
    assert_eq!(
        body["items"].as_array().unwrap().len(),
        if mixed { 12 } else { 3 }
    );
    harness.assert_visible(&expected).await;
    for item in body["items"].as_array().unwrap() {
        let result = item.as_object().unwrap().values().next().unwrap();
        assert!(result["status"].as_u64().unwrap() < 400, "{item}");
        harness.assert_refresh_response(result);
    }
    assert_eq!(
        harness
            .cluster
            .nodes
            .iter()
            .map(|node| node.refresh_requests.load(Ordering::Relaxed))
            .sum::<usize>(),
        6,
        "one refresh round for each touched shard, independent of bulk actions"
    );
}

macro_rules! bulk_visibility_test {
    ($name:ident, $global:literal, $replica:literal, $mixed:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            bulk_visibility($global, $replica, $mixed).await;
        }
    };
}

bulk_visibility_test!(refresh_regression_global_bulk_shardless, true, false, false);
bulk_visibility_test!(refresh_regression_global_bulk_replica, true, true, false);
bulk_visibility_test!(refresh_regression_index_bulk_shardless, false, false, false);
bulk_visibility_test!(refresh_regression_index_bulk_replica, false, true, false);
bulk_visibility_test!(
    refresh_regression_global_mixed_bulk_shardless,
    true,
    false,
    true
);
bulk_visibility_test!(
    refresh_regression_global_mixed_bulk_replica,
    true,
    true,
    true
);
bulk_visibility_test!(
    refresh_regression_index_mixed_bulk_shardless,
    false,
    false,
    true
);
bulk_visibility_test!(
    refresh_regression_index_mixed_bulk_replica,
    false,
    true,
    true
);

fn assert_copy_failure(body: &Value, node_id: &str, shard: u32, primary: bool) {
    assert_eq!(body["_shards"]["total"], 3, "{body}");
    assert_eq!(body["_shards"]["successful"], 2, "{body}");
    assert_eq!(body["_shards"]["failed"], 1, "{body}");
    let failures = body["_shards"]["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 1, "{body}");
    let failure = &failures[0];
    assert_eq!(failure["node"], node_id, "{body}");
    assert_eq!(failure["index"], INDEX, "{body}");
    assert_eq!(failure["shard"], shard, "{body}");
    assert_eq!(failure["primary"], primary, "{body}");
    assert!(failure["allocation_id"].as_u64().unwrap() > 0, "{body}");
    assert_eq!(failure["reason"]["type"], "refresh_exception", "{body}");
    assert!(
        failure["reason"]["reason"]
            .as_str()
            .unwrap()
            .contains("injected refresh commit failure"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_replica_failure_preserves_single_acknowledgement() {
    let harness = RefreshCluster::start(1).await;
    let replica = harness.replica(0);
    harness.engines[&(replica, 0)]
        .text_engine()
        .inject_refresh_commit_failures_for_test(1);
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 8})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created", "{body}");
    assert!(body.get("error").is_none(), "{body}");
    assert!(body["_seq_no"].is_u64(), "{body}");
    assert_copy_failure(
        &body,
        &harness.cluster.nodes[replica].state.local_node_id,
        0,
        false,
    );
    assert_eq!(body["forced_refresh"], true, "{body}");
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let query: SearchRequest = serde_json::from_value(json!({"size": 100})).unwrap();
    for node in 1..4 {
        let engine = harness.engines[&(node, 0)].clone();
        let document =
            tokio::task::spawn_blocking(move || engine.get_document_with_metadata("doc", true))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(document.source["value"], 8);
        assert_eq!(document.seq_no, body["_seq_no"].as_u64().unwrap());
        let (hits, total, _) = harness.cluster.nodes[0]
            .state
            .transport_client
            .forward_search_dsl_to_shard(
                &state.nodes[&harness.cluster.nodes[node].state.local_node_id],
                INDEX,
                0,
                &query,
            )
            .await
            .unwrap();
        if node == replica {
            assert_eq!(
                total, 0,
                "failed refresh must not claim new search visibility"
            );
        } else {
            assert_eq!(total, 1);
            assert_eq!(hits[0]["_source"]["value"], 8);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_primary_failure_preserves_acknowledgement() {
    let harness = RefreshCluster::start(1).await;
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let primary_id = &state.indices[INDEX].shard_routing[&0].primary;
    let primary = harness
        .cluster
        .nodes
        .iter()
        .position(|node| node.state.local_node_id == *primary_id)
        .unwrap();
    harness.engines[&(primary, 0)]
        .text_engine()
        .inject_refresh_commit_failures_for_test(1);
    let (status, body) = harness
        .cluster
        .request(
            harness.replica(0),
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 9})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created", "{body}");
    assert!(body.get("error").is_none(), "{body}");
    assert_copy_failure(&body, primary_id, 0, true);
    assert!(body.get("forced_refresh").is_none(), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_replica_failure_preserves_bulk_items() {
    let harness = RefreshCluster::start(3).await;
    let replica = harness.replica(1);
    harness.engines[&(replica, 1)]
        .text_engine()
        .inject_refresh_commit_failures_for_test(1);
    let mut ndjson = String::new();
    for shard in 0..3 {
        for label in ["one", "two"] {
            ndjson.push_str(&format!(
                "{}\n{}\n",
                json!({"index": {"_index": INDEX, "_id": harness.doc_id(shard, label)}}),
                json!({"value": 7})
            ));
        }
    }
    let body = harness.bulk(0, true, "?refresh=true", ndjson).await;
    assert_eq!(body["errors"], false, "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 6);
    for (position, item) in body["items"].as_array().unwrap().iter().enumerate() {
        let result = &item["index"];
        assert_eq!(result["status"], 201, "{item}");
        assert_eq!(result["result"], "created", "{item}");
        assert!(result.get("error").is_none(), "{item}");
        assert!(result["_seq_no"].is_u64(), "{item}");
        if position / 2 == 1 {
            assert_copy_failure(
                result,
                &harness.cluster.nodes[replica].state.local_node_id,
                1,
                false,
            );
        } else {
            harness.assert_refresh_response(result);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_false_and_absent_do_not_refresh_single_writes() {
    let harness = RefreshCluster::start(1).await;
    let mut notifications = Vec::new();
    for engine in harness.engines.values() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        engine
            .text_engine()
            .notify_before_refresh_writer_for_test(sender);
        notifications.push(receiver);
    }
    for suffix in ["", "?refresh=false"] {
        let (status, body) = harness
            .cluster
            .request(
                harness.replica(0),
                reqwest::Method::PUT,
                &format!("/{INDEX}/_doc/doc{suffix}"),
                Some(json!({"value": 11})),
            )
            .await;
        assert!(status.is_success(), "{body}");
        assert!(body.get("forced_refresh").is_none(), "{body}");
        for (method, route, source) in [
            (
                reqwest::Method::POST,
                "_update/doc",
                Some(json!({"doc": {"value": 12}})),
            ),
            (
                reqwest::Method::PUT,
                "_create/created",
                Some(json!({"value": 13})),
            ),
            (reqwest::Method::DELETE, "_doc/created", None),
        ] {
            let (status, body) = harness
                .cluster
                .request(
                    harness.replica(0),
                    method,
                    &format!("/{INDEX}/{route}{suffix}"),
                    source,
                )
                .await;
            assert!(status.is_success(), "{body}");
            assert!(body.get("forced_refresh").is_none(), "{body}");
        }
    }
    harness.assert_visible(&[(0, "doc".into(), None)]).await;
    for mut receiver in notifications {
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    }
    assert!(
        harness
            .cluster
            .nodes
            .iter()
            .all(|node| node.refresh_requests.load(Ordering::Relaxed) == 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_false_and_absent_do_not_refresh_bulk() {
    let harness = RefreshCluster::start(3).await;
    let mut notifications = Vec::new();
    for engine in harness.engines.values() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        engine
            .text_engine()
            .notify_before_refresh_writer_for_test(sender);
        notifications.push(receiver);
    }
    for (global, refresh) in [(true, ""), (false, "?refresh=false")] {
        let body = harness
            .bulk(
                harness.replica(0),
                global,
                refresh,
                format!(
                    "{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
                    json!({"index": {"_index": INDEX, "_id": "doc"}}),
                    json!({"value": 12}),
                    json!({"create": {"_index": INDEX, "_id": "created"}}),
                    json!({"value": 13}),
                    json!({"update": {"_index": INDEX, "_id": "doc"}}),
                    json!({"doc": {"value": 14}}),
                    json!({"delete": {"_index": INDEX, "_id": "created"}}),
                ),
            )
            .await;
        assert_eq!(body["errors"], false, "{body}");
        assert!(
            body["items"][0]["index"].get("forced_refresh").is_none(),
            "{body}"
        );
    }
    for mut receiver in notifications {
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    }
    assert!(
        harness
            .cluster
            .nodes
            .iter()
            .all(|node| node.refresh_requests.load(Ordering::Relaxed) == 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_primary_uses_authority_newer_than_replica_coordinator() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-3".into(), "node-2".into(), "node-4".into()],
        1,
    )
    .await;
    let coordinator = harness.replica(0);
    assert_eq!(
        coordinator, 1,
        "gate the replica coordinator, not the primary"
    );
    harness.cluster.gate.pause();
    let leader = &harness.cluster.nodes[0].state;
    let state = leader.cluster_manager.get_state();
    let metadata = &state.indices[INDEX];
    let routing = &metadata.shard_routing[&0];
    leader
        .raft
        .client_write(ClusterCommand::MarkReplicaInSync {
            index_name: INDEX.into(),
            index_uuid: metadata.uuid.to_string(),
            shard_id: 0,
            replica: "node-4".into(),
            allocation_id: state.shard_allocation_id(INDEX, 0, "node-4").unwrap(),
            primary: routing.primary.clone(),
            primary_term: routing.primary_term,
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    harness.cluster.gate.wait_until_entered().await;
    let version = leader.cluster_manager.version();
    for (offset, node) in harness.cluster.nodes.iter().enumerate() {
        if offset != coordinator {
            node.state
                .cluster_manager
                .wait_for_version(version)
                .await
                .unwrap();
        }
    }
    let started = harness.cluster.nodes[coordinator]
        .refresh_request_started
        .clone();
    let manager = harness.cluster.nodes[coordinator]
        .state
        .cluster_manager
        .clone();
    let gate = harness.cluster.gate.clone();
    let release = tokio::spawn(async move {
        let arrived = tokio::time::timeout(Duration::from_secs(10), started.notified()).await;
        let count = manager.get_state().indices[INDEX].shard_routing[&0]
            .in_sync_replicas
            .len();
        gate.resume();
        arrived.expect("the primary must dispatch refresh to the stale coordinator");
        count
    });
    let (status, body) = harness
        .cluster
        .request(
            coordinator,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 19})),
        )
        .await;
    assert_eq!(
        release.await.unwrap(),
        1,
        "coordinator must route from an older in-sync view"
    );
    assert_eq!(status, StatusCode::CREATED, "{body}");
    harness.assert_refresh_response(&body);
    harness.assert_visible(&[(0, "doc".into(), Some(19))]).await;
    assert_eq!(
        harness.cluster.nodes[3]
            .refresh_requests
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_assigned_out_of_sync_copy_is_not_refreshed() {
    let harness = RefreshCluster::start_with_topology(
        1,
        &["node-2".into(), "node-3".into(), "node-4".into()],
        1,
    )
    .await;
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    harness.engines[&(3, 0)]
        .text_engine()
        .notify_before_refresh_writer_for_test(sender);
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 20})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["_shards"],
        json!({"total": 2, "successful": 2, "failed": 0}),
        "{body}"
    );
    harness.assert_visible(&[(0, "doc".into(), Some(20))]).await;
    assert_eq!(
        harness.cluster.nodes[3]
            .refresh_requests
            .load(Ordering::Relaxed),
        0
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let engine = harness.engines[&(3, 0)].clone();
    assert!(
        tokio::task::spawn_blocking(move || engine.get_document_with_metadata("doc", true))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_grpc_refresh_failure_preserves_acknowledgement() {
    let harness = RefreshCluster::start(1).await;
    let replica = harness.replica(0);
    harness.cluster.nodes[replica]
        .reject_refresh_requests
        .store(true, Ordering::Relaxed);
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 21})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created", "{body}");
    assert_eq!(body["_shards"]["failed"], 1, "{body}");
    let failure = &body["_shards"]["failures"][0];
    assert_eq!(
        failure["node"],
        harness.cluster.nodes[replica].state.local_node_id
    );
    assert_eq!(
        failure["reason"]["reason"],
        tonic::Status::unimplemented("").to_string(),
        "{body}"
    );
    assert!(body.get("error").is_none(), "{body}");
    let engine = harness.engines[&(replica, 0)].clone();
    let document =
        tokio::task::spawn_blocking(move || engine.get_document_with_metadata("doc", true))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert_eq!(document.source["value"], 21);
    assert_eq!(document.seq_no, body["_seq_no"].as_u64().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_response_waits_for_replica_reader_publication() {
    let harness = RefreshCluster::start(1).await;
    let replica = harness.replica(0);
    let (committed_tx, committed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    harness.engines[&(replica, 0)]
        .text_engine()
        .pause_after_refresh_commit_for_test(committed_tx, release_rx);
    let client = harness.cluster.client.clone();
    let url = format!(
        "{}/{INDEX}/_doc/doc?refresh=true",
        harness.cluster.nodes[0].url
    );
    let response = tokio::spawn(async move {
        client
            .put(url)
            .json(&json!({"value": 22}))
            .send()
            .await
            .unwrap()
    });
    tokio::task::spawn_blocking(move || committed_rx.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("replica refresh must reach the pre-publication barrier");
    assert!(
        !response.is_finished(),
        "HTTP acknowledgement must wait for reader publication"
    );
    let engine = harness.engines[&(replica, 0)].clone();
    let unrefreshed =
        tokio::task::spawn_blocking(move || engine.get_document_with_metadata("doc", false))
            .await
            .unwrap()
            .unwrap();
    assert!(unrefreshed.is_none());
    release_tx.send(()).unwrap();
    let response = response.await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    harness.assert_refresh_response(&response.json().await.unwrap());
    harness.assert_visible(&[(0, "doc".into(), Some(22))]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_reports_every_failed_copy_without_unacknowledging() {
    let harness = RefreshCluster::start(1).await;
    for engine in harness.engines.values() {
        engine
            .text_engine()
            .inject_refresh_commit_failures_for_test(1);
    }
    let (status, body) = harness
        .cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{INDEX}/_doc/doc?refresh=true"),
            Some(json!({"value": 23})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created", "{body}");
    assert!(body["_seq_no"].is_u64(), "{body}");
    assert_eq!(body["_shards"]["total"], 3, "{body}");
    assert_eq!(body["_shards"]["successful"], 0, "{body}");
    assert_eq!(body["_shards"]["failed"], 3, "{body}");
    let failures = body["_shards"]["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 3);
    let nodes = failures
        .iter()
        .map(|failure| failure["node"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        nodes,
        std::collections::HashSet::from(["node-2", "node-3", "node-4"])
    );
    assert!(body.get("forced_refresh").is_none(), "{body}");
    assert!(body.get("error").is_none(), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_copy_rpc_rejects_malformed_and_obsolete_authority() {
    use crate::transport::proto::{ShardCopyRefreshRequest, shard_copy_refresh_result::Outcome};

    let harness = RefreshCluster::start(1).await;
    let replica = harness.replica(0);
    let state = harness.cluster.nodes[0].state.cluster_manager.get_state();
    let node = &state.nodes[&harness.cluster.nodes[replica].state.local_node_id];
    let mut client = harness.cluster.nodes[0]
        .state
        .transport_client
        .connect(&node.host, node.transport_port)
        .await
        .unwrap();
    let routing = &state.indices[INDEX].shard_routing[&0];
    let valid = ShardCopyRefreshRequest {
        index_name: INDEX.into(),
        index_uuid: state.indices[INDEX].uuid.to_string(),
        shard_id: 0,
        primary_node_id: routing.primary.clone(),
        primary_term: Some(routing.primary_term),
        target_allocation_id: state.shard_allocation_id(INDEX, 0, &node.id),
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
            .refresh_shard_copy(crate::transport::request_with_cluster_state_version(
                request,
                state.version,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
    }
    for request in [
        ShardCopyRefreshRequest {
            primary_term: Some(routing.primary_term + 1),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            target_allocation_id: valid.target_allocation_id.map(|id| id + 1),
            ..valid.clone()
        },
        ShardCopyRefreshRequest {
            primary_node_id: node.id.clone(),
            ..valid.clone()
        },
    ] {
        let result = client
            .refresh_shard_copy(crate::transport::request_with_cluster_state_version(
                request,
                state.version,
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(
            matches!(result.outcome, Some(Outcome::Error(ref reason)) if reason.contains("authority changed")),
            "{result:?}"
        );
    }
    assert!(
        harness
            .engines
            .values()
            .all(|engine| engine.wal_max_seq_no().is_none())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_regression_wait_for_is_rejected_before_mutation() {
    let harness = RefreshCluster::start(1).await;
    for (method, route, body) in [
        (reqwest::Method::PUT, "_doc/doc", Some(json!({"value": 1}))),
        (
            reqwest::Method::PUT,
            "_create/doc",
            Some(json!({"value": 1})),
        ),
        (
            reqwest::Method::POST,
            "_update/doc",
            Some(json!({"doc": {"value": 1}, "doc_as_upsert": true})),
        ),
        (reqwest::Method::DELETE, "_doc/doc", None),
    ] {
        let (status, body) = harness
            .cluster
            .request(
                0,
                method,
                &format!("/{INDEX}/{route}?refresh=wait_for"),
                body,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            body["error"]["type"], "illegal_argument_exception",
            "{body}"
        );
        assert!(
            body["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("refresh")
        );
    }
    for route in ["/_bulk".to_string(), format!("/{INDEX}/_bulk")] {
        let response = harness
            .cluster
            .client
            .post(format!(
                "{}{route}?refresh=wait_for",
                harness.cluster.nodes[0].url
            ))
            .header("content-type", "application/x-ndjson")
            .body(format!(
                "{}\n{}\n",
                json!({"index": {"_index": INDEX, "_id": "doc"}}),
                json!({"value": 1})
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(
        harness
            .engines
            .values()
            .all(|engine| engine.wal_max_seq_no().is_none())
    );
}
