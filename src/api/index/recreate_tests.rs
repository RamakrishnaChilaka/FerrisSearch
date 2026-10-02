use super::*;

const CYCLES: usize = 20;

async fn wait_for_applied_metadata(cluster: &ForwardingCluster) {
    cluster.nodes[1]
        .state
        .cluster_manager
        .wait_for_version(cluster.nodes[0].state.cluster_manager.version())
        .await
        .unwrap();
}

async fn use_leader_primary(cluster: &ForwardingCluster) {
    for (node, roles) in [
        (0, vec![NodeRole::Master, NodeRole::Data]),
        (1, vec![NodeRole::Client]),
    ] {
        let mut info = cluster.nodes[0].state.cluster_manager.get_state().nodes
            [&cluster.nodes[node].state.local_node_id]
            .clone();
        info.roles = roles;
        cluster.nodes[0]
            .state
            .raft
            .client_write(ClusterCommand::AddNode { node: info })
            .await
            .unwrap()
            .data
            .into_result()
            .unwrap();
    }
    wait_for_applied_metadata(cluster).await;
}

async fn create_on(cluster: &ForwardingCluster, coordinator: usize, index: &str) -> Value {
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::PUT,
            &format!("/{index}"),
            Some(json!({
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"value": {"type": "integer"}}}
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    wait_for_applied_metadata(cluster).await;
    body
}

async fn read_file(path: std::path::PathBuf) -> Vec<u8> {
    tokio::task::spawn_blocking(move || std::fs::read(path).unwrap())
        .await
        .unwrap()
}

async fn assert_recreate_cycles(coordinator: usize, primary: usize) {
    let cluster = ForwardingCluster::start().await;
    if primary == 0 {
        use_leader_primary(&cluster).await;
    }
    let primary_node = &cluster.nodes[primary];
    let primary_id = &primary_node.state.local_node_id;
    let protected = "recreate-protected";
    create_on(&cluster, coordinator, protected).await;
    let protected_source = json!({"value": 987654});
    let (status, body) = cluster
        .request(
            coordinator,
            reqwest::Method::PUT,
            &format!("/{protected}/_doc/keep"),
            Some(protected_source.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    wait_for_applied_metadata(&cluster).await;
    let protected_dir = primary_node
        .state
        .shard_manager
        .shard_data_dir(protected, 0)
        .unwrap();
    let protected_identity_path = protected_dir.join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let protected_identity = read_file(protected_identity_path.clone()).await;
    let marker = protected_dir.join("unrelated-allocation-evidence");
    let marker_for_write = marker.clone();
    tokio::task::spawn_blocking(move || std::fs::write(marker_for_write, b"must survive").unwrap())
        .await
        .unwrap();

    let index = "recreate-cycle";
    let mut failures = Vec::new();
    for iteration in 0..CYCLES {
        let first_create = create_on(&cluster, coordinator, index).await;
        assert_eq!(first_create["shards_acknowledged"], true, "{first_create}");
        let old_state = cluster.nodes[0].state.cluster_manager.get_state();
        let old_metadata = &old_state.indices[index];
        assert_eq!(old_metadata.primary_node(0), Some(primary_id));
        let old_uuid = old_metadata.uuid.clone();
        let old_allocation = old_state.shard_allocation_id(index, 0, primary_id).unwrap();
        let old_source = json!({"value": -(iteration as i64) - 1});
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::PUT,
                &format!("/{index}/_doc/old"),
                Some(old_source.clone()),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::GET,
                &format!("/{index}/_doc/old?realtime=true"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_source"], old_source, "{body}");

        let (status, body) = cluster
            .request(0, reqwest::Method::DELETE, &format!("/{index}"), None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        wait_for_applied_metadata(&cluster).await;
        let recreated = create_on(&cluster, coordinator, index).await;
        let new_state = cluster.nodes[0].state.cluster_manager.get_state();
        let new_metadata = &new_state.indices[index];
        assert_ne!(new_metadata.uuid, old_uuid);
        assert_eq!(new_metadata.primary_node(0), Some(primary_id));
        let new_allocation = new_state.shard_allocation_id(index, 0, primary_id).unwrap();
        assert_ne!(new_allocation, old_allocation);
        let expected_dir = primary_node
            .state
            .shard_manager
            .data_dir()
            .join(&new_metadata.uuid)
            .join("shard_0");
        assert_ne!(expected_dir, protected_dir);
        let new_source = json!({"value": iteration});
        let (status, receipt) = cluster
            .request(
                coordinator,
                reqwest::Method::PUT,
                &format!("/{index}/_doc/new"),
                Some(new_source.clone()),
            )
            .await;
        if status != StatusCode::CREATED {
            failures.push(format!(
                "cycle {iteration}: readiness={recreated}; write={status}: {receipt}"
            ));
        } else {
            assert_eq!(recreated["shards_acknowledged"], true, "{recreated}");
            let identity = primary_node
                .state
                .shard_manager
                .copy_identity(index, 0)
                .unwrap();
            assert_eq!(identity.index_uuid, new_metadata.uuid.as_str());
            assert_eq!(identity.allocation_id, new_allocation);
            assert_eq!(
                primary_node.state.shard_manager.shard_data_dir(index, 0),
                Some(expected_dir.clone())
            );
            let durable: crate::shard::ShardCopyIdentity = serde_json::from_slice(
                &read_file(expected_dir.join(crate::shard::SHARD_COPY_IDENTITY_FILE)).await,
            )
            .unwrap();
            assert_eq!(durable.index_uuid, identity.index_uuid);
            assert_eq!(durable.allocation_id, identity.allocation_id);
            for node in 0..2 {
                let (status, body) = cluster
                    .request(
                        node,
                        reqwest::Method::GET,
                        &format!("/{index}/_doc/new?realtime=true"),
                        None,
                    )
                    .await;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(body["_source"], new_source, "{body}");
                assert_eq!(body["_seq_no"], receipt["_seq_no"], "{body}");
                assert_eq!(body["_primary_term"], receipt["_primary_term"], "{body}");
                let (status, body) = cluster
                    .request(
                        node,
                        reqwest::Method::GET,
                        &format!("/{index}/_doc/old?realtime=true"),
                        None,
                    )
                    .await;
                assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
                assert_eq!(body["found"], false, "{body}");
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
            for node in 0..2 {
                let (status, body) = cluster
                    .request(
                        node,
                        reqwest::Method::POST,
                        &format!("/{index}/_search"),
                        Some(json!({"query": {"match_all": {}}})),
                    )
                    .await;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(body["_shards"]["failed"], 0, "{body}");
                assert_eq!(body["hits"]["total"]["value"], 1, "{body}");
                assert_eq!(body["hits"]["hits"][0]["_id"], "new", "{body}");
                assert_eq!(body["hits"]["hits"][0]["_source"], new_source, "{body}");
                let (status, body) = cluster
                    .request(
                        node,
                        reqwest::Method::POST,
                        "/_sql",
                        Some(json!({"query": format!("SELECT value FROM \"{index}\"")})),
                    )
                    .await;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(body["_shards"]["failed"], 0, "{body}");
                assert_eq!(body["rows"], json!([{"value": iteration}]), "{body}");
            }
        }
        assert_eq!(
            read_file(protected_identity_path.clone()).await,
            protected_identity
        );
        assert_eq!(read_file(marker.clone()).await, b"must survive");
        let (status, body) = cluster
            .request(
                coordinator,
                reqwest::Method::GET,
                &format!("/{protected}/_doc/keep?realtime=true"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_source"], protected_source, "{body}");

        // Retire the test index even after a failed cycle so all cycles run on main.
        primary_node
            .state
            .shard_manager
            .close_index_shards_blocking(index.to_string())
            .await
            .unwrap();
        let (status, body) = cluster
            .request(0, reqwest::Method::DELETE, &format!("/{index}"), None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        wait_for_applied_metadata(&cluster).await;
    }
    println!(
        "issue-152 recreate: primary={primary_id}, coordinator=node-{}: {}/{CYCLES} failed cycles",
        coordinator + 1,
        failures.len()
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_recreate_follower_primary_from_leader_20_cycles() {
    assert_recreate_cycles(0, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_recreate_follower_primary_from_follower_20_cycles() {
    assert_recreate_cycles(1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_recreate_leader_primary_from_follower_20_cycles() {
    assert_recreate_cycles(1, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_recreate_while_old_mapping_write_is_in_flight() {
    let cluster = ForwardingCluster::start().await;
    let index = "recreate-in-flight";
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{index}"),
            Some(json!({
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "dynamic": true,
                    "properties": {"value": {"type": "integer"}}
                }
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    wait_for_applied_metadata(&cluster).await;
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{index}/_doc/old"),
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let old_uuid = cluster.nodes[0].state.cluster_manager.get_state().indices[index]
        .uuid
        .clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    cluster.nodes[1]
        .state
        .shard_manager
        .set_reopen_after_cleanup_gate(entered_tx, release_rx);
    let client = cluster.client.clone();
    let old_url = format!("{}/{index}/_doc/in-flight", cluster.nodes[0].url);
    let old_write = tokio::spawn(async move {
        let response = client
            .put(old_url)
            .json(&json!({"value": 2, "new_field": "old incarnation"}))
            .send()
            .await
            .unwrap();
        (response.status(), response.json::<Value>().await.unwrap())
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let (status, body) = cluster
        .request(0, reqwest::Method::DELETE, &format!("/{index}"), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let client = cluster.client.clone();
    let create_url = format!("{}/{index}", cluster.nodes[0].url);
    let create = tokio::spawn(async move {
        let response = client
            .put(create_url)
            .json(&json!({
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"value": {"type": "integer"}}}
            }))
            .send()
            .await
            .unwrap();
        (response.status(), response.json::<Value>().await.unwrap())
    });
    let replaced = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = cluster.nodes[1].state.cluster_manager.get_state();
            if let Some(metadata) = state.indices.get(index)
                && metadata.uuid != old_uuid
            {
                break metadata.uuid.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    release_tx.send(()).unwrap();
    let new_uuid = replaced.expect("recreate must apply while the old reopen is blocked");
    assert_ne!(new_uuid, old_uuid);
    let (status, body) = old_write.await.unwrap();
    assert!(
        !status.is_success(),
        "old-incarnation write was acknowledged: {body}"
    );
    let (status, body) = create.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shards_acknowledged"], true, "{body}");
    let (status, body) = cluster
        .request(
            1,
            reqwest::Method::PUT,
            &format!("/{index}/_doc/new"),
            Some(json!({"value": 3})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for (id, expected) in [("old", None), ("in-flight", None), ("new", Some(3))] {
        let (status, body) = cluster
            .request(
                0,
                reqwest::Method::GET,
                &format!("/{index}/_doc/{id}?realtime=true"),
                None,
            )
            .await;
        if let Some(value) = expected {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["_source"], json!({"value": value}), "{body}");
        } else {
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["found"], false, "{body}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_applied_delete_releases_idle_follower_engine_and_wal() {
    let cluster = ForwardingCluster::start().await;
    let index = "recreate-idle";
    create_on(&cluster, 0, index).await;
    let (status, body) = cluster
        .request(
            0,
            reqwest::Method::PUT,
            &format!("/{index}/_doc/old"),
            Some(json!({"value": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let manager = &cluster.nodes[1].state.shard_manager;
    let old = manager.get_shard(index, 0).unwrap();
    let weak = Arc::downgrade(&old);
    drop(old);
    let old_dir = manager.shard_data_dir(index, 0).unwrap();
    let identity_path = old_dir.join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let identity = read_file(identity_path.clone()).await;
    let (status, body) = cluster
        .request(0, reqwest::Method::DELETE, &format!("/{index}"), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    wait_for_applied_metadata(&cluster).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some()
            || manager.index_uuid(index).is_some()
            || manager.copy_identity(index, 0).is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("applied deletion must drop the idle engine/WAL without a subsequent client request");
    assert!(manager.get_index_shards(index).is_empty());
    assert_eq!(read_file(identity_path).await, identity);
}
