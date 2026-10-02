use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_ci158_preentered_gate_cannot_autocreate_on_master() {
    let cluster = ForwardingCluster::start().await;
    cluster.nodes[1]
        .state
        .cluster_manager
        .forwarding_wait_millis
        .store(100, std::sync::atomic::Ordering::Relaxed);
    cluster.gate.pause();
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::SetMaster {
            node_id: "node-1".into(),
        })
        .await
        .unwrap();
    cluster.gate.wait_until_entered().await;
    let create = cluster
        .client
        .put(format!("{}/ci158-paused", cluster.nodes[0].url))
        .json(&json!({"settings": {"number_of_replicas": 0}}))
        .send();
    tokio::pin!(create);
    tokio::select! {
        biased;
        () = cluster.gate.wait_until_entered() => {}
        response = &mut create => panic!("create unexpectedly completed: {response:?}"),
    }
    assert!(
        !cluster.nodes[0]
            .state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("ci158-paused")
    );
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/ci158-paused/_doc/a",
            Some(json!({"body": "must not be served"})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let metadata =
        cluster.nodes[0].state.cluster_manager.get_state().indices["ci158-paused"].clone();
    assert_eq!(metadata.primary_node(0).map(String::as_str), Some("node-2"));
    for node in &cluster.nodes {
        assert!(
            node.state
                .shard_manager
                .get_shard("ci158-paused", 0)
                .is_none()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_ci158_autocreate_uses_data_node_not_master_only_leader() {
    let cluster = ForwardingCluster::start().await;
    let source = json!({"body": "data-node copy"});
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/ci158-placement/_doc/a",
            Some(source.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let metadata =
        cluster.nodes[0].state.cluster_manager.get_state().indices["ci158-placement"].clone();
    assert_eq!(metadata.primary_node(0).map(String::as_str), Some("node-2"));
    assert!(
        cluster.nodes[0]
            .state
            .shard_manager
            .get_shard("ci158-placement", 0)
            .is_none()
    );
    assert!(
        cluster.nodes[1]
            .state
            .shard_manager
            .get_shard("ci158-placement", 0)
            .is_some()
    );
    for coordinator in 0..2 {
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::GET,
                "/ci158-placement/_doc/a",
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_source"], source, "{body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_ci158_autocreate_without_data_nodes_rejects_before_metadata() {
    let cluster = ForwardingCluster::start().await;
    let mut node = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-2"].clone();
    node.roles = vec![NodeRole::Client];
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::AddNode { node })
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
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            "/ci158-no-data/_doc/a",
            Some(json!({"body": "must not be stored"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "no_data_nodes_exception", "{body}");
    for node in &cluster.nodes {
        assert!(
            !node
                .state
                .cluster_manager
                .get_state()
                .indices
                .contains_key("ci158-no-data")
        );
        assert!(
            node.state
                .shard_manager
                .get_shard("ci158-no-data", 0)
                .is_none()
        );
        assert!(
            node.state
                .shard_manager
                .index_uuid("ci158-no-data")
                .is_none()
        );
    }
}
