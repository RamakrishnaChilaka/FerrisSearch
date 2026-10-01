use super::*;
use crate::cluster::state::{DynamicMapping, IndexMetadata, NodeRole};
use std::sync::atomic::Ordering;

async fn enable_leader_data_role(cluster: &ForwardingCluster) {
    let mut leader = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-1"].clone();
    leader.roles = vec![NodeRole::Master, NodeRole::Data];
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::AddNode { node: leader })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
}

async fn add_unreachable_data_node(cluster: &ForwardingCluster) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::AddNode {
            node: NodeInfo {
                id: "unreachable".into(),
                name: "unreachable".into(),
                host: "127.0.0.1".into(),
                transport_port: port,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 99,
            },
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
}

async fn assert_auto_create_maps_and_aggregates(coordinator: usize) {
    let cluster = ForwardingCluster::start().await;
    enable_leader_data_role(&cluster).await;
    let index = format!("review-auto-{coordinator}");
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::PUT,
            &format!("/{index}/_doc/a"),
            Some(json!({"count": 5, "level": "error"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let metadata = cluster.nodes[0].state.cluster_manager.get_state().indices[&index].clone();
    assert_eq!(metadata.dynamic, DynamicMapping::True);
    assert!(metadata.mappings.contains_key("count"));
    assert!(metadata.mappings.contains_key("level"));
    if coordinator == 0 {
        assert_eq!(metadata.primary_node(0).map(String::as_str), Some("node-1"));
    }
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::POST,
            &format!("/{index}/_refresh"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_shards"]["failed"], 0, "{body}");
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": format!("SELECT count FROM \"{index}\" WHERE count >= 1")})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["execution_mode"], "tantivy_fast_fields", "{body}");
    assert_eq!(body["rows"][0]["count"], 5, "{body}");
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::POST,
            "/_sql",
            Some(json!({"query": format!("SELECT max(count) AS peak FROM \"{index}\"")})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["execution_mode"], "tantivy_grouped_partials", "{body}");
    assert_eq!(body["rows"][0]["peak"].as_f64(), Some(5.0), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_leader_auto_create_preserves_dynamic_mapping_and_placement() {
    assert_auto_create_maps_and_aggregates(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_follower_auto_create_maps_fields_and_aggregates() {
    assert_auto_create_maps_and_aggregates(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_committed_create_with_unreachable_primary_is_acknowledged() {
    let cluster = ForwardingCluster::start().await;
    add_unreachable_data_node(&cluster).await;
    for (coordinator, index) in [
        (0, "review-unreachable-leader"),
        (1, "review-unreachable-follower"),
    ] {
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::PUT,
                &format!("/{index}"),
                Some(json!({"settings": {"number_of_shards": 2, "number_of_replicas": 0}})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["acknowledged"], true, "{body}");
        assert_eq!(body["shards_acknowledged"], false, "{body}");
        assert!(
            cluster.nodes[0]
                .state
                .cluster_manager
                .get_state()
                .indices
                .contains_key(index)
        );
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::PUT,
                &format!("/{index}"),
                Some(json!({})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            body["error"]["type"], "resource_already_exists_exception",
            "{body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_many_slow_primary_opens_are_not_failed_creates() {
    let cluster = ForwardingCluster::start().await;
    for node in &cluster.nodes {
        node.state
            .cluster_manager
            .forwarding_wait_millis
            .store(80, Ordering::Relaxed);
        node.state
            .cluster_manager
            .primary_open_wait_millis
            .store(100, Ordering::Relaxed);
    }
    cluster.nodes[1]
        .state
        .cluster_manager
        .primary_open_delay_millis
        .store(350, Ordering::Relaxed);
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/review-slow-open",
            Some(json!({"settings": {"number_of_shards": 16, "number_of_replicas": 0}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["shards_acknowledged"], false, "{body}");
    cluster.nodes[1]
        .state
        .cluster_manager
        .primary_open_delay_millis
        .store(0, Ordering::Relaxed);
    for node in &cluster.nodes {
        node.state
            .cluster_manager
            .forwarding_wait_millis
            .store(5_000, Ordering::Relaxed);
    }
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/review-slow-open/_doc/a",
            Some(json!({"body": "a later write must succeed"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_primary_open_budget_is_independent_and_concurrent() {
    let cluster = ForwardingCluster::start().await;
    let index = "review-concurrent-open";
    let metadata = IndexMetadata::from_create_request_body(
        index,
        &json!({"settings": {"number_of_shards": 8, "number_of_replicas": 0}}),
        &["node-2".to_string()],
    )
    .unwrap();
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::CreateIndex { metadata })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
    for node in &cluster.nodes {
        node.state
            .cluster_manager
            .forwarding_wait_millis
            .store(40, Ordering::Relaxed);
        node.state
            .cluster_manager
            .primary_open_wait_millis
            .store(2_500, Ordering::Relaxed);
    }
    let primary = &cluster.nodes[1].state;
    primary
        .cluster_manager
        .primary_open_delay_millis
        .store(200, Ordering::Relaxed);
    let coordinator = &cluster.nodes[0].state;
    assert!(
        crate::transport::primary_open::wait_for_index_primaries(
            &coordinator.cluster_manager,
            &coordinator.shard_manager,
            &coordinator.transport_client,
            &coordinator.local_node_id,
            index,
        )
        .await
    );
    assert_eq!(
        primary
            .cluster_manager
            .primary_open_peak
            .load(Ordering::Relaxed),
        4
    );
    for shard in 0..8 {
        assert!(
            primary.shard_manager.get_shard(index, shard).is_some(),
            "shard {shard}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_lagging_primary_create_is_acknowledged() {
    let cluster = ForwardingCluster::start().await;
    cluster.nodes[1]
        .state
        .cluster_manager
        .forwarding_wait_millis
        .store(100, Ordering::Relaxed);
    cluster.gate.pause();
    let (status, body) = cluster.create("review-lag-create").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["shards_acknowledged"], false, "{body}");
    cluster.gate.resume();
    cluster.nodes[1]
        .state
        .cluster_manager
        .forwarding_wait_millis
        .store(5_000, Ordering::Relaxed);
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/review-lag-create/_doc/a",
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_create_reply_does_not_fail_for_coordinator_apply_lag() {
    let cluster = ForwardingCluster::start().await;
    enable_leader_data_role(&cluster).await;
    let mut follower_info =
        cluster.nodes[0].state.cluster_manager.get_state().nodes["node-2"].clone();
    follower_info.roles = vec![NodeRole::Client];
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::AddNode {
            node: follower_info,
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
    cluster.nodes[1]
        .state
        .cluster_manager
        .forwarding_wait_millis
        .store(100, Ordering::Relaxed);
    cluster.gate.pause();
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            "/review-coordinator-lag",
            Some(json!({"settings": {"number_of_replicas": 0}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["shards_acknowledged"], false, "{body}");
    assert!(
        cluster.nodes[0]
            .state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("review-coordinator-lag")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_delete_then_recreate_is_not_a_failed_create() {
    let cluster = ForwardingCluster::start().await;
    let (status, body) = cluster.create("review-recreated").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/review-recreated/_doc/a",
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let old_uuid = cluster.nodes[0].state.cluster_manager.get_state().indices["review-recreated"]
        .uuid
        .clone();
    let (status, body) = cluster
        .request(0, reqwest::Method::DELETE, "/review-recreated", None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = cluster.create("review-recreated").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_ne!(
        cluster.nodes[0].state.cluster_manager.get_state().indices["review-recreated"].uuid,
        old_uuid
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_review_unrelated_acknowledgement_and_apply_lag_do_not_stall_reads() {
    let cluster = ForwardingCluster::start().await;
    for index in ["review-stable", "review-other"] {
        let (status, body) = cluster.create(index).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/review-stable/_doc/a",
            Some(json!({"value": 7})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = cluster
        .request(0, reqwest::Method::POST, "/review-stable/_refresh", None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for node in &cluster.nodes {
        node.state
            .cluster_manager
            .forwarding_wait_millis
            .store(150, Ordering::Relaxed);
    }
    cluster.gate.pause();
    let master = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-1"].clone();
    cluster.nodes[1]
        .state
        .transport_client
        .forward_add_mappings(
            &master,
            "review-other",
            &Default::default(),
            &DynamicMapping::Strict,
        )
        .await
        .unwrap();
    cluster.gate.wait_until_entered().await;
    let applied = cluster.nodes[1].state.cluster_manager.version();
    assert!(applied < cluster.nodes[0].state.cluster_manager.version());
    for coordinator in [0, 1] {
        for (method, path, payload) in [
            (reqwest::Method::GET, "/review-stable/_doc/a", None),
            (reqwest::Method::GET, "/review-stable/_search", None),
            (
                reqwest::Method::POST,
                "/review-stable/_search",
                Some(json!({"query": {"match_all": {}}})),
            ),
            (reqwest::Method::GET, "/review-stable/_count", None),
            (
                reqwest::Method::POST,
                "/_sql",
                Some(json!({"query": "SELECT value FROM \"review-stable\""})),
            ),
            (
                reqwest::Method::POST,
                "/_sql",
                Some(json!({"query": "SELECT count(*) FROM \"review-stable\""})),
            ),
        ] {
            let (status, body) = cluster.request(coordinator, method, path, payload).await;
            assert_eq!(status, StatusCode::OK, "node {coordinator}: {body}");
            if path.ends_with("_doc/a") {
                assert_eq!(body["_source"]["value"], 7, "{body}");
            } else {
                assert_eq!(body["_shards"]["failed"], 0, "{body}");
                assert_eq!(body["_shards"]["successful"], 1, "{body}");
            }
            assert_eq!(cluster.nodes[1].state.cluster_manager.version(), applied);
        }
    }
}

#[test]
fn forwarding_review_generic_create_unavailable_is_not_retryable_503() {
    let error = anyhow::Error::new(tonic::Status::unavailable(
        "connection lost after possible commit",
    ));
    assert!(super::super::forwarded_create_index_error_response(&error).is_none());
}
