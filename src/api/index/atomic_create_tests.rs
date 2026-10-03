use super::*;
use crate::cluster::state::IndexMetadata;
use crate::consensus::types::ClusterResponse;
use openraft::type_config::async_runtime::WatchReceiver;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

const WRITERS: usize = 8;
const BULK_INDICES: usize = 3;
const TEST_WAIT: Duration = Duration::from_secs(60);

async fn catch_up(cluster: &ForwardingCluster) {
    let version = cluster.nodes[0].state.cluster_manager.version();
    let index = cluster.nodes[0]
        .state
        .raft
        .metrics()
        .borrow_watched()
        .last_applied
        .unwrap()
        .index;
    tokio::time::timeout(TEST_WAIT, async {
        for node in &cluster.nodes {
            loop {
                if node.state.cluster_manager.version() >= version
                    && node
                        .state
                        .raft
                        .metrics()
                        .borrow_watched()
                        .last_applied
                        .is_some_and(|applied| applied.index >= index)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("all real Raft state machines must apply the committed log");
}

async fn all_roles(cluster: &ForwardingCluster) {
    for node in &cluster.nodes {
        let mut info = cluster.nodes[0].state.cluster_manager.get_state().nodes
            [&node.state.local_node_id]
            .clone();
        info.roles = vec![NodeRole::Master, NodeRole::Data];
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
    catch_up(cluster).await;
}

async fn wait_for_proposals(cluster: &ForwardingCluster, last_index: u64) {
    tokio::time::timeout(TEST_WAIT, async {
        loop {
            if cluster.nodes[0]
                .state
                .raft
                .metrics()
                .borrow_watched()
                .last_log_index
                .is_some_and(|index| index >= last_index)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all requests must propose before leader application is released");
}

fn candidate(index: &str, primary: &str) -> IndexMetadata {
    IndexMetadata::from_create_request_body(
        index,
        &json!({
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {"dynamic": true}
        }),
        &[primary.to_string()],
    )
    .unwrap()
}

async fn request(
    cluster: &ForwardingCluster,
    node: usize,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    cluster
        .request_with_timeout(node, method, path, body, Some(TEST_WAIT))
        .await
}

async fn bulk_request(
    cluster: &ForwardingCluster,
    node: usize,
    path: &str,
    body: String,
) -> (StatusCode, Value) {
    let response = cluster
        .client
        .post(format!("{}{path}", cluster.nodes[node].url))
        .timeout(TEST_WAIT)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .send()
        .await
        .unwrap();
    (response.status(), response.json().await.unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_leader_apply_gate_does_not_pause_third_node() {
    let cluster = ForwardingCluster::start_with_roles(&[
        vec![NodeRole::Master],
        vec![NodeRole::Data],
        vec![NodeRole::Data],
    ])
    .await;
    catch_up(&cluster).await;
    let before = cluster.nodes[0].state.cluster_manager.version();
    let raft = cluster.nodes[0].state.raft.clone();
    cluster.leader_gate.pause();
    let write = tokio::spawn(async move {
        raft.client_write(ClusterCommand::SetMaster {
            node_id: "node-1".into(),
        })
        .await
        .unwrap()
    });
    tokio::time::timeout(TEST_WAIT, cluster.leader_gate.wait_until_entered())
        .await
        .unwrap();
    tokio::time::timeout(TEST_WAIT, async {
        while cluster.nodes[2].state.cluster_manager.version() < before + 1 {
            assert_eq!(
                cluster.nodes[0].state.cluster_manager.version(),
                before,
                "the leader gate must not be consumed by another node",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the third node must apply while only leader application is paused");
    assert_eq!(cluster.nodes[0].state.cluster_manager.version(), before);
    assert!(!write.is_finished());
    cluster.leader_gate.resume();
    write.await.unwrap().data.into_result().unwrap();
    catch_up(&cluster).await;
    assert_eq!(cluster.nodes[0].state.cluster_manager.version(), before + 1);
    assert_eq!(cluster.nodes[2].state.cluster_manager.version(), before + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_real_raft_queued_autocreates_use_winning_metadata() {
    let cluster = ForwardingCluster::start().await;
    all_roles(&cluster).await;
    let index = "atomic-queued";
    let before = cluster.nodes[0].state.cluster_manager.get_state();
    assert!(!before.indices.contains_key(index));
    let winner = candidate(index, "node-2");
    let winner_uuid = winner.uuid.clone();
    let raft = cluster.nodes[0].state.raft.clone();
    cluster.leader_gate.pause();
    let first = tokio::spawn(async move {
        raft.client_write(ClusterCommand::CreateIndex { metadata: winner })
            .await
            .unwrap()
    });
    tokio::time::timeout(TEST_WAIT, cluster.leader_gate.wait_until_entered())
        .await
        .unwrap();
    let first_index = cluster.nodes[0]
        .state
        .raft
        .metrics()
        .borrow_watched()
        .last_log_index
        .unwrap();
    let mut creates = Vec::new();
    for writer in 0..WRITERS {
        let state = cluster.nodes[writer % 2].state.clone();
        let before = before.clone();
        creates.push(tokio::spawn(async move {
            super::super::auto_create_index(&state, index, &before).await
        }));
    }
    wait_for_proposals(&cluster, first_index + WRITERS as u64).await;
    assert!(
        !cluster.nodes[0]
            .state
            .cluster_manager
            .get_state()
            .indices
            .contains_key(index),
    );
    cluster.leader_gate.resume();
    assert_eq!(first.await.unwrap().data, ClusterResponse::Ok);
    let mut failures = Vec::new();
    for create in creates {
        match create.await.unwrap() {
            Ok(metadata) if metadata.uuid == winner_uuid => {}
            result => failures.push(format!("{result:?}")),
        }
    }
    catch_up(&cluster).await;
    for node in &cluster.nodes {
        let state = node.state.cluster_manager.get_state();
        if state.indices[index].uuid != winner_uuid {
            failures.push(format!(
                "{} replaced the winning UUID with {}",
                node.state.local_node_id, state.indices[index].uuid,
            ));
        }
        if state.version != before.version + 1 {
            failures.push(format!(
                "duplicate creates bumped version to {}",
                state.version
            ));
        }
    }
    if cluster.nodes[0]
        .state
        .shard_manager
        .get_shard(index, 0)
        .is_some()
    {
        failures.push("leader opened a losing local-primary candidate".into());
    }
    let source = json!({"body": "write belongs to the winning incarnation"});
    let (status, body) = request(
        &cluster,
        0,
        reqwest::Method::PUT,
        &format!("/{index}/_doc/a"),
        Some(source.clone()),
    )
    .await;
    if status != StatusCode::CREATED {
        failures.push(format!("write: {status} {body}"));
    }
    for node in 0..2 {
        let (status, body) = request(
            &cluster,
            node,
            reqwest::Method::GET,
            &format!("/{index}/_doc/a?realtime=true"),
            None,
        )
        .await;
        if status != StatusCode::OK || body["_source"] != source {
            failures.push(format!("GET through {node}: {status} {body}"));
        }
    }
    println!(
        "ATOMIC_QUEUED auto_creates={WRITERS} winner_uuid={winner_uuid} failures={failures:?}"
    );
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_real_raft_late_duplicate_preserves_acknowledged_writes() {
    let cluster = ForwardingCluster::start().await;
    catch_up(&cluster).await;
    let index = "atomic-acked";
    let absent = cluster.nodes[0].state.cluster_manager.get_state();
    assert!(!absent.indices.contains_key(index));
    let first = candidate(index, "node-2");
    let delayed = candidate(index, "node-2");
    let first_uuid = first.uuid.clone();
    assert_ne!(first_uuid, delayed.uuid);
    let raft = &cluster.nodes[0].state.raft;
    raft.client_write(ClusterCommand::CreateIndex { metadata: first })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let source = json!({"body": "acknowledged before the delayed CreateIndex"});
    let (status, body) = request(
        &cluster,
        0,
        reqwest::Method::PUT,
        &format!("/{index}/_create/acked"),
        Some(source.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    catch_up(&cluster).await;
    let before = cluster.nodes[0].state.cluster_manager.get_state();
    assert!(before.primary_initialized(index, 0));
    let duplicate = raft
        .client_write(ClusterCommand::CreateIndex { metadata: delayed })
        .await
        .unwrap();
    catch_up(&cluster).await;
    let after = cluster.nodes[0].state.cluster_manager.get_state();
    let later_source = json!({"body": "write after the delayed proposal"});
    let (status, body) = request(
        &cluster,
        1,
        reqwest::Method::PUT,
        &format!("/{index}/_doc/later"),
        Some(later_source.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (get_status, get_body) = request(
        &cluster,
        0,
        reqwest::Method::GET,
        &format!("/{index}/_doc/acked?realtime=true"),
        None,
    )
    .await;
    let (refresh_status, refresh_body) = request(
        &cluster,
        0,
        reqwest::Method::POST,
        &format!("/{index}/_refresh"),
        None,
    )
    .await;
    assert_eq!(refresh_status, StatusCode::OK, "{refresh_body}");
    let (count_status, count_body) = request(
        &cluster,
        0,
        reqwest::Method::GET,
        &format!("/{index}/_count"),
        None,
    )
    .await;
    println!(
        "ATOMIC_ACKED duplicate={:?} first_uuid={first_uuid} surviving_uuid={} acknowledged=2 count={} GET_acked={get_status} GET_body={get_body}",
        duplicate.data, after.indices[index].uuid, count_body["count"],
    );
    assert_eq!(get_status, StatusCode::OK, "{get_body}");
    assert_eq!(get_body["_source"], source, "{get_body}");
    assert_eq!(count_status, StatusCode::OK, "{count_body}");
    assert_eq!(count_body["count"], 2, "{count_body}");
    assert!(duplicate.data.into_result().is_err());
    assert_eq!(after.version, before.version);
    assert_eq!(after.indices[index].uuid, first_uuid);
    assert_eq!(after.shard_allocations, before.shard_allocations);
    assert_eq!(
        serde_json::to_value(&after.indices).unwrap(),
        serde_json::to_value(&before.indices).unwrap(),
    );
    for node in 0..2 {
        for (id, expected) in [("acked", &source), ("later", &later_source)] {
            let (status, body) = request(
                &cluster,
                node,
                reqwest::Method::GET,
                &format!("/{index}/_doc/{id}?realtime=true"),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(&body["_source"], expected, "{body}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_explicit_create_race_has_one_winner() {
    let cluster = ForwardingCluster::start().await;
    catch_up(&cluster).await;
    let before = cluster.nodes[0].state.cluster_manager.get_state();
    let last_index = cluster.nodes[0]
        .state
        .raft
        .metrics()
        .borrow_watched()
        .last_log_index
        .unwrap();
    cluster.leader_gate.pause();
    let mut creates = Vec::new();
    for writer in 0..WRITERS {
        let client = cluster.client.clone();
        let url = format!("{}/atomic-explicit", cluster.nodes[writer % 2].url);
        creates.push(tokio::spawn(async move {
            let response = client
                .put(url)
                .timeout(TEST_WAIT)
                .json(&json!({"settings": {"number_of_replicas": 0}}))
                .send()
                .await
                .unwrap();
            (response.status(), response.json::<Value>().await.unwrap())
        }));
    }
    wait_for_proposals(&cluster, last_index + WRITERS as u64).await;
    cluster.leader_gate.resume();
    let mut winners = 0;
    let mut rejected = 0;
    let mut failures = Vec::new();
    for create in creates {
        let (status, body) = create.await.unwrap();
        if status == StatusCode::OK && body["acknowledged"] == true {
            winners += 1;
        } else if status == StatusCode::BAD_REQUEST
            && body["error"]["type"] == "resource_already_exists_exception"
        {
            rejected += 1;
        } else {
            failures.push(format!("{status} {body}"));
        }
    }
    catch_up(&cluster).await;
    let committed = cluster.nodes[0].state.cluster_manager.get_state();
    println!(
        "ATOMIC_EXPLICIT winners={winners} rejected={rejected} version_delta={} failures={failures:?}",
        committed.version - before.version,
    );
    assert_eq!(winners, 1);
    assert_eq!(rejected, WRITERS - 1);
    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(committed.version, before.version + 1);
    let uuid = committed.indices["atomic-explicit"].uuid.clone();
    for node in 0..2 {
        let (status, body) = request(
            &cluster,
            node,
            reqwest::Method::PUT,
            "/atomic-explicit",
            Some(json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        assert_eq!(
            cluster.nodes[node]
                .state
                .cluster_manager
                .get_state()
                .indices["atomic-explicit"]
                .uuid,
            uuid,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_lagging_coordinator_classifies_existing_create_and_autocreate() {
    let cluster = ForwardingCluster::start().await;
    all_roles(&cluster).await;
    let follower = &cluster.nodes[1].state;
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(100, Ordering::Relaxed);
    cluster.gate.pause();
    let index = "atomic-lag-existing";
    cluster.nodes[0]
        .state
        .raft
        .client_write(ClusterCommand::CreateIndex {
            metadata: candidate(index, "node-1"),
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    tokio::time::timeout(TEST_WAIT, cluster.gate.wait_until_entered())
        .await
        .unwrap();
    let before = cluster.nodes[0].state.cluster_manager.get_state();
    assert!(
        !follower
            .cluster_manager
            .get_state()
            .indices
            .contains_key(index)
    );
    let (status, body) = request(
        &cluster,
        1,
        reqwest::Method::PUT,
        &format!("/{index}"),
        Some(json!({})),
    )
    .await;
    let stale = follower.cluster_manager.get_state();
    let auto = super::super::auto_create_index(follower, index, &stale).await;
    println!("ATOMIC_LAG explicit={status} {body} auto={auto:?}");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "resource_already_exists_exception");
    assert_eq!(
        follower.transport_client.required_state_version(index),
        before.version,
    );
    let (status, body) = auto.unwrap_err();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    assert!(
        body.0["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("cluster state")
    );
    assert_eq!(
        cluster.nodes[0].state.cluster_manager.version(),
        before.version
    );
    assert!(follower.shard_manager.get_shard(index, 0).is_none());
    cluster.gate.resume();
    follower
        .cluster_manager
        .forwarding_wait_millis
        .store(5_000, Ordering::Relaxed);
    catch_up(&cluster).await;
    let metadata = super::super::auto_create_index(follower, index, &stale)
        .await
        .unwrap();
    assert_eq!(metadata.uuid, before.indices[index].uuid);
    let source = json!({"body": "write after the winning metadata applies"});
    let (status, body) = request(
        &cluster,
        1,
        reqwest::Method::PUT,
        &format!("/{index}/_doc/a"),
        Some(source.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = request(
        &cluster,
        0,
        reqwest::Method::GET,
        &format!("/{index}/_doc/a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"], source);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_lagging_create_uses_leader_allocation_view() {
    let cluster = ForwardingCluster::start().await;
    let raft = &cluster.nodes[0].state.raft;
    let mut data_node = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-2"].clone();
    data_node.roles = vec![NodeRole::Client];
    raft.client_write(ClusterCommand::AddNode { node: data_node })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    catch_up(&cluster).await;
    cluster.gate.pause();
    let mut leader = cluster.nodes[0].state.cluster_manager.get_state().nodes["node-1"].clone();
    leader.roles = vec![NodeRole::Master, NodeRole::Data];
    raft.client_write(ClusterCommand::AddNode { node: leader })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let index = "atomic-lag-allocation";
    raft.client_write(ClusterCommand::CreateIndex {
        metadata: candidate(index, "node-1"),
    })
    .await
    .unwrap()
    .data
    .into_result()
    .unwrap();
    tokio::time::timeout(TEST_WAIT, cluster.gate.wait_until_entered())
        .await
        .unwrap();
    let follower = cluster.nodes[1].state.cluster_manager.get_state();
    assert!(
        !follower
            .nodes
            .values()
            .any(|node| node.roles.contains(&NodeRole::Data)),
    );
    assert!(!follower.indices.contains_key(index));
    let before = cluster.nodes[0].state.cluster_manager.get_state();
    let (status, body) = request(
        &cluster,
        1,
        reqwest::Method::PUT,
        &format!("/{index}"),
        Some(json!({"settings": {"number_of_replicas": 0}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "resource_already_exists_exception");
    assert_eq!(
        cluster.nodes[0].state.cluster_manager.get_state().version,
        before.version,
    );
    assert_eq!(
        cluster.nodes[0].state.cluster_manager.get_state().indices[index].uuid,
        before.indices[index].uuid,
    );
    cluster.gate.resume();
    catch_up(&cluster).await;
    let source = json!({"body": "leader metadata, not the lagging allocation view"});
    let (status, body) = request(
        &cluster,
        1,
        reqwest::Method::PUT,
        &format!("/{index}/_doc/a"),
        Some(source.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = request(
        &cluster,
        0,
        reqwest::Method::GET,
        &format!("/{index}/_doc/a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_source"], source);
}

#[derive(Default, serde::Serialize)]
struct RaceStats {
    created: usize,
    bad_request: usize,
    internal_error: usize,
    unavailable: usize,
    other: usize,
    count_mismatches: usize,
    source_mismatches: usize,
    uuid_changes: usize,
    excess_mutations: usize,
    exact_gets: usize,
}

fn record_outcome(stats: &mut RaceStats, status: u16, body: &Value, failures: &mut Vec<String>) {
    match status {
        201 => stats.created += 1,
        400 => stats.bad_request += 1,
        500 => stats.internal_error += 1,
        503 => stats.unavailable += 1,
        _ => stats.other += 1,
    }
    if status != 201 && failures.len() < 12 {
        failures.push(format!("{status} {body}"));
    }
}

async fn concurrent_first_writes(all_data: bool, coordinator: usize) {
    let cluster = ForwardingCluster::start().await;
    if all_data {
        all_roles(&cluster).await;
    } else {
        catch_up(&cluster).await;
    }
    let rounds = std::env::var("FERRIS_ATOMIC_ROUNDS")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("FERRIS_ATOMIC_ROUNDS must be an integer")
        })
        .unwrap_or(2);
    assert!(rounds > 0);
    let mut stats = RaceStats::default();
    let mut failures = Vec::new();
    for round in 0..rounds {
        let before = cluster.nodes[0].state.cluster_manager.version();
        let single_index = format!("atomic-single-{round}");
        let bulk_indices = (0..BULK_INDICES)
            .map(|offset| format!("atomic-bulk-{round}-{offset}"))
            .collect::<Vec<_>>();
        let mut acknowledged = BTreeMap::<String, Vec<(String, Value)>>::new();
        let mut observed = BTreeMap::<String, BTreeSet<String>>::new();
        let results = futures::future::join_all((0..WRITERS).map(|writer| {
            let index = &single_index;
            let cluster = &cluster;
            async move {
                let source = json!({"body": format!("single round {round} writer {writer}")});
                let (method, path) = match writer % 3 {
                    0 => (reqwest::Method::POST, format!("/{index}/_doc")),
                    1 => (reqwest::Method::PUT, format!("/{index}/_doc/{writer}")),
                    _ => (reqwest::Method::PUT, format!("/{index}/_create/{writer}")),
                };
                let (status, body) =
                    request(cluster, coordinator, method, &path, Some(source.clone())).await;
                let uuid = cluster.nodes[0]
                    .state
                    .cluster_manager
                    .get_state()
                    .indices
                    .get(index)
                    .map(|metadata| metadata.uuid.to_string());
                (status, body, source, uuid)
            }
        }))
        .await;
        for (status, body, source, uuid) in results {
            record_outcome(&mut stats, status.as_u16(), &body, &mut failures);
            if let Some(uuid) = uuid {
                observed
                    .entry(single_index.clone())
                    .or_default()
                    .insert(uuid);
            }
            if status == StatusCode::CREATED {
                acknowledged
                    .entry(single_index.clone())
                    .or_default()
                    .push((body["_id"].as_str().unwrap().to_string(), source));
            }
        }
        let results = futures::future::join_all((0..WRITERS).map(|writer| {
            let cluster = &cluster;
            let indices = &bulk_indices;
            async move {
                let mut ndjson = String::new();
                let mut sources = Vec::new();
                for (offset, index) in indices.iter().enumerate() {
                    let action = if writer % 2 == 0 { "index" } else { "create" };
                    let id = writer.to_string();
                    let source = json!({"body": format!("bulk round {round} index {offset} writer {writer}")});
                    ndjson.push_str(&format!("{}\n{source}\n", json!({(action): {"_index": index, "_id": id}})));
                    sources.push((index.clone(), id, source));
                }
                let path = if writer % 2 == 0 {
                    "/_bulk".to_string()
                } else {
                    format!("/{}/_bulk", indices[0])
                };
                let (status, body) = bulk_request(cluster, coordinator, &path, ndjson).await;
                let state = cluster.nodes[0].state.cluster_manager.get_state();
                let uuids = indices.iter().filter_map(|index| {
                    state.indices.get(index).map(|metadata| (index.clone(), metadata.uuid.to_string()))
                }).collect::<Vec<_>>();
                (status, body, sources, uuids)
            }
        }))
        .await;
        for (status, body, sources, uuids) in results {
            assert_eq!(status, StatusCode::OK, "{body}");
            let items = body["items"].as_array().unwrap();
            assert_eq!(items.len(), BULK_INDICES, "{body}");
            for (item, (index, id, source)) in items.iter().zip(sources) {
                let result = item.as_object().unwrap().values().next().unwrap();
                let status = result["status"].as_u64().unwrap() as u16;
                record_outcome(&mut stats, status, result, &mut failures);
                if status == 201 {
                    acknowledged.entry(index).or_default().push((id, source));
                }
            }
            for (index, uuid) in uuids {
                observed.entry(index).or_default().insert(uuid);
            }
            assert_eq!(
                body["errors"],
                items.iter().any(|item| {
                    item.as_object()
                        .unwrap()
                        .values()
                        .any(|result| result.get("error").is_some())
                })
            );
        }
        catch_up(&cluster).await;
        let committed = cluster.nodes[0].state.cluster_manager.get_state();
        if committed.version != before + 2 * (1 + BULK_INDICES) as u64 {
            stats.excess_mutations += 1;
            if failures.len() < 12 {
                failures.push(format!(
                    "round {round}: version delta {}, expected one create/activation per index",
                    committed.version - before
                ));
            }
        }
        for index in std::iter::once(&single_index).chain(&bulk_indices) {
            let uuid = committed.indices[index].uuid.to_string();
            observed
                .entry(index.clone())
                .or_default()
                .insert(uuid.clone());
            for node in &cluster.nodes {
                let state = node.state.cluster_manager.get_state();
                observed
                    .entry(index.clone())
                    .or_default()
                    .insert(state.indices[index].uuid.to_string());
                assert_eq!(
                    state.shard_allocations[index],
                    committed.shard_allocations[index]
                );
            }
            if observed[index].len() != 1 {
                stats.uuid_changes += 1;
            }
            let docs = acknowledged.get(index).map(Vec::as_slice).unwrap_or(&[]);
            let (status, body) = request(
                &cluster,
                0,
                reqwest::Method::POST,
                &format!("/{index}/_refresh"),
                None,
            )
            .await;
            if status != StatusCode::OK || body["_shards"]["failed"] != 0 {
                failures.push(format!("refresh {index}: {status} {body}"));
            }
            let (status, body) = request(
                &cluster,
                0,
                reqwest::Method::GET,
                &format!("/{index}/_count"),
                None,
            )
            .await;
            if status != StatusCode::OK || body["count"].as_u64() != Some(docs.len() as u64) {
                stats.count_mismatches += 1;
                failures.push(format!(
                    "{index}: acknowledged={} count: {status} {body}",
                    docs.len()
                ));
            }
            for (id, source) in docs {
                for node in 0..2 {
                    let (status, body) = request(
                        &cluster,
                        node,
                        reqwest::Method::GET,
                        &format!("/{index}/_doc/{id}?realtime=true"),
                        None,
                    )
                    .await;
                    if status == StatusCode::OK && &body["_source"] == source {
                        stats.exact_gets += 1;
                    } else {
                        stats.source_mismatches += 1;
                        if failures.len() < 12 {
                            failures
                                .push(format!("GET {index}/{id} through {node}: {status} {body}"));
                        }
                    }
                }
            }
        }
        for index in std::iter::once(&single_index).chain(&bulk_indices) {
            let (status, body) = request(
                &cluster,
                0,
                reqwest::Method::DELETE,
                &format!("/{index}"),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        catch_up(&cluster).await;
    }
    println!(
        "ATOMIC_CONCURRENCY {}",
        json!({
            "all_roles": all_data,
            "coordinator": coordinator,
            "rounds": rounds,
            "writers": WRITERS,
            "expected_created": rounds * WRITERS * (1 + BULK_INDICES),
            "stats": stats,
            "failures": failures,
        })
    );
    assert_eq!(stats.created, rounds * WRITERS * (1 + BULK_INDICES));
    assert_eq!(stats.count_mismatches, 0);
    assert_eq!(stats.source_mismatches, 0);
    assert_eq!(stats.uuid_changes, 0);
    assert_eq!(stats.excess_mutations, 0);
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_concurrent_first_writes_master_only_leader() {
    concurrent_first_writes(false, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_concurrent_first_writes_master_only_follower() {
    concurrent_first_writes(false, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_concurrent_first_writes_all_roles_leader() {
    concurrent_first_writes(true, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_concurrent_first_writes_all_roles_follower() {
    concurrent_first_writes(true, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_bulk_index_failures_are_independent() {
    let cluster = ForwardingCluster::start().await;
    catch_up(&cluster).await;
    let ndjson = concat!(
        "{\"index\":{\"_index\":\"atomic-independent-a\",\"_id\":\"a\"}}\n",
        "{\"body\":\"first independent success\"}\n",
        "{\"create\":{\"_index\":\"INVALID\",\"_id\":\"bad\"}}\n",
        "{\"body\":\"must not prevent other indices\"}\n",
        "{\"create\":{\"_index\":\"atomic-independent-b\",\"_id\":\"b\"}}\n",
        "{\"body\":\"second independent success\"}\n",
        "{\"index\":{\"_index\":\"INVALID\",\"_id\":\"bad-again\"}}\n",
        "{\"body\":\"same failed index\"}\n",
    );
    for node in 0..2 {
        let body = ndjson.replace("independent", &format!("independent-{node}"));
        let (status, body) = bulk_request(&cluster, node, "/_bulk", body).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["errors"], true, "{body}");
        assert_eq!(body["items"][0]["index"]["status"], 201, "{body}");
        assert_eq!(body["items"][2]["create"]["status"], 201, "{body}");
        for (position, action) in [(1, "create"), (3, "index")] {
            assert_eq!(body["items"][position][action]["status"], 400, "{body}");
            assert_eq!(
                body["items"][position][action]["error"]["type"], "invalid_index_name_exception",
                "{body}"
            );
        }
        for (suffix, id, source) in [
            ("a", "a", format!("first independent-{node} success")),
            ("b", "b", format!("second independent-{node} success")),
        ] {
            let (status, body) = request(
                &cluster,
                node,
                reqwest::Method::GET,
                &format!("/atomic-independent-{node}-{suffix}/_doc/{id}"),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["_source"], json!({"body": source}), "{body}");
        }
    }
    assert!(
        !cluster.nodes[0]
            .state
            .cluster_manager
            .get_state()
            .indices
            .contains_key("INVALID")
    );
}
