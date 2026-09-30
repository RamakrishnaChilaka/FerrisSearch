#![cfg(feature = "protocol-trace")]

use anyhow::{Context, Result};
use ferrissearch::cluster::manager::ClusterManager;
use ferrissearch::cluster::state::{
    DynamicMapping, FieldMapping, FieldType, IndexMetadata, IndexSettings, IndexUuid,
    NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use ferrissearch::consensus::state_machine::ClusterStateMachine;
use ferrissearch::consensus::types::{ClusterCommand, ClusterResponse};
use ferrissearch::protocol_trace::{
    self, FaultAction, FaultRule, MutationMode, TraceConfig, TraceNode, TraceStartCopy,
    TraceStartShard,
};
use ferrissearch::shard::ShardManager;
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::proto::{ShardBulkRequest, ShardDocRequest, ShardGetRequest};
use ferrissearch::transport::server::{
    TransportService, create_transport_service_for_test_with_handle,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{Barrier, RwLock};

const INDEX: &str = "d1-trace";
const INDEX_UUID: &str = "d1-trace-uuid";
const SHARD: u32 = 0;

struct RunningNode {
    address: std::net::SocketAddr,
    service: TransportService,
    server: tokio::task::JoinHandle<()>,
}

async fn start_node(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    node_id: &str,
    port: Option<u16>,
) -> Result<RunningNode> {
    let bind = std::net::SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let (server, service) = create_transport_service_for_test_with_handle(
        cluster_manager,
        shard_manager,
        TransportClient::new(),
        Arc::new(ferrissearch::tasks::TaskManager::new()),
        node_id.to_string(),
    );
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
            .expect("protocol trace gRPC server failed");
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    Ok(RunningNode {
        address,
        service,
        server,
    })
}

async fn connect(
    address: std::net::SocketAddr,
) -> Result<InternalTransportClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect()
        .await?;
    Ok(InternalTransportClient::new(channel))
}

fn node_info(node: &str, address: std::net::SocketAddr) -> DomainNodeInfo {
    DomainNodeInfo {
        id: node.to_string(),
        name: node.to_string(),
        host: "127.0.0.1".to_string(),
        transport_port: address.port(),
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    }
}

fn initial_cluster_state(
    p: std::net::SocketAddr,
    q: std::net::SocketAddr,
    r: std::net::SocketAddr,
) -> ferrissearch::cluster::state::ClusterState {
    let mut state = ferrissearch::cluster::state::ClusterState::new("d1-trace".to_string());
    state.add_node(node_info("p", p));
    state.add_node(node_info("q", q));
    state.add_node(node_info("r", r));
    state.add_index(IndexMetadata {
        name: INDEX.to_string(),
        uuid: IndexUuid::new(INDEX_UUID),
        number_of_shards: 1,
        number_of_replicas: 2,
        shard_routing: HashMap::from([(
            SHARD,
            ShardRoutingEntry {
                primary: "p".to_string(),
                primary_term: 1,
                replicas: vec!["q".to_string(), "r".to_string()],
                in_sync_replicas: vec!["q".to_string(), "r".to_string()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::from([(
            "value".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        )]),
        dynamic: DynamicMapping::Strict,
        settings: IndexSettings {
            refresh_interval_ms: Some(600_000),
            flush_threshold_bytes: Some(u64::MAX),
            ..IndexSettings::default()
        },
    });
    state
}

fn install_empty_replica(
    cluster_manager: &ClusterManager,
    shard_manager: &ShardManager,
) -> Result<()> {
    let state = cluster_manager.get_state();
    let metadata = state
        .indices
        .get(INDEX)
        .context("trace index metadata is missing")?;
    shard_manager.open_shard_with_settings(
        INDEX,
        SHARD,
        &metadata.mappings,
        &metadata.settings,
        metadata.uuid.as_str(),
    )?;
    Ok(())
}

fn parse_seed() -> Result<u64> {
    match std::env::var("D1_TRACE_SEED") {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid D1_TRACE_SEED '{value}'")),
        Err(_) => Ok(0xD1_5EED),
    }
}

fn mutation_mode() -> Result<MutationMode> {
    match std::env::var("D1_TRACE_MUTATION")
        .unwrap_or_else(|_| "none".to_string())
        .as_str()
    {
        "none" => Ok(MutationMode::None),
        "arrival-order" => Ok(MutationMode::ArrivalOrderApply),
        "seq-only-redelivery" => Ok(MutationMode::SeqOnlyRedelivery),
        value => anyhow::bail!("unknown D1_TRACE_MUTATION '{value}'"),
    }
}

fn trace_output(trace_dir: &TempDir, seed: u64, mutation: MutationMode) -> PathBuf {
    std::env::var_os("D1_TRACE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mode = match mutation {
                MutationMode::None => "correct",
                MutationMode::ArrivalOrderApply => "arrival-order",
                MutationMode::SeqOnlyRedelivery => "seq-only-redelivery",
            };
            trace_dir.path().join(format!("d1-{mode}-{seed}.jsonl"))
        })
}

async fn index_document(
    client: &mut InternalTransportClient<tonic::transport::Channel>,
    doc_id: &str,
    value: i64,
) -> Result<ferrissearch::transport::proto::ShardDocResponse> {
    Ok(client
        .index_doc(tonic::Request::new(ShardDocRequest {
            index_name: INDEX.to_string(),
            shard_id: SHARD,
            doc_id: doc_id.to_string(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": value}))?,
        }))
        .await?
        .into_inner())
}

async fn wait_for_primary_sequence(shard_manager: &ShardManager, expected: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if shard_manager
                .get_shard(INDEX, SHARD)
                .and_then(|engine| engine.sequence_stats().max_seq_no)
                == Some(expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("timed out waiting for the primary sequence")?;
    Ok(())
}

fn assert_command_ok(response: ClusterResponse, action: &str) -> Result<()> {
    match response {
        ClusterResponse::Ok => Ok(()),
        ClusterResponse::Error(error) => anyhow::bail!("{action} failed: {error}"),
    }
}

fn current_authoritative_state(
    state_machine: &ClusterStateMachine,
) -> ferrissearch::cluster::state::ClusterState {
    state_machine
        .state_handle()
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
}

async fn stop_node(node: RunningNode) {
    node.server.abort();
    let _ = node.server.await;
    drop(node.service);
}

fn capture_final_copy_state(
    shard_manager: &ShardManager,
) -> Result<Option<ferrissearch::protocol_trace::TraceCopySnapshot>> {
    if shard_manager.get_shard(INDEX, SHARD).is_some() {
        return Ok(Some(
            shard_manager.capture_protocol_trace_copy_state(INDEX, SHARD)?,
        ));
    }
    Ok(None)
}

fn assert_trace_completeness(trace_path: &Path) -> Result<()> {
    let actual_path = trace_path.with_extension("actual.json");
    let checker =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/tla/check_d1_trace_invariants.py");
    let output = Command::new("python3")
        .arg(checker)
        .arg(trace_path)
        .arg("--actual")
        .arg(&actual_path)
        .arg("--completeness-only")
        .output()
        .context("run D1 trace completeness checker")?;
    anyhow::ensure!(
        output.status.success(),
        "D1 trace completeness check failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_three_node_fault_trace() -> Result<()> {
    let seed = parse_seed()?;
    let mutation = mutation_mode()?;
    let trace_dir = tempfile::tempdir()?;
    let output = trace_output(&trace_dir, seed, mutation);

    let p_dir = tempfile::tempdir()?;
    let q_dir = tempfile::tempdir()?;
    let r_dir = tempfile::tempdir()?;
    let p_cm = Arc::new(ClusterManager::new("d1-trace".to_string()));
    let q_cm = Arc::new(ClusterManager::new("d1-trace".to_string()));
    let r_cm = Arc::new(ClusterManager::new("d1-trace".to_string()));
    let p_sm = Arc::new(ShardManager::new(p_dir.path(), Duration::from_secs(60)));
    let q_sm = Arc::new(ShardManager::new(q_dir.path(), Duration::from_secs(60)));
    let r_sm = Arc::new(ShardManager::new(r_dir.path(), Duration::from_secs(60)));

    let p = start_node(p_cm.clone(), p_sm.clone(), "p", None).await?;
    let q = start_node(q_cm.clone(), q_sm.clone(), "q", None).await?;
    let r = start_node(r_cm.clone(), r_sm.clone(), "r", None).await?;

    let initial = initial_cluster_state(p.address, q.address, r.address);
    for manager in [&p_cm, &q_cm, &r_cm] {
        manager.update_state(initial.clone());
    }
    install_empty_replica(&q_cm, &q_sm)?;
    install_empty_replica(&r_cm, &r_sm)?;
    p.service
        .protocol_trace_activate_primary_for_test(INDEX, SHARD)
        .await
        .map_err(anyhow::Error::msg)?;

    let mut initialized = initial;
    initialized
        .shard_allocations
        .get_mut(INDEX)
        .and_then(|shards| shards.get_mut(&SHARD))
        .context("trace shard allocation metadata is missing")?
        .primary_initialized = true;
    initialized.version += 1;
    for manager in [&p_cm, &q_cm, &r_cm] {
        manager.update_state(initialized.clone());
    }
    let state_machine =
        ClusterStateMachine::from_state_for_protocol_trace_test(initialized.clone());

    let trace = protocol_trace::start(TraceConfig {
        output: output.clone(),
        run_id: format!("d1-seeded-{seed}-{mutation:?}"),
        test: "seeded_three_node_fault_trace".to_string(),
        durability: "request",
        nodes: ["p", "q", "r"]
            .into_iter()
            .map(|node| TraceNode {
                node: node.to_string(),
                incarnation: 0,
            })
            .collect(),
        shard_state: TraceStartShard {
            index_uuid: INDEX_UUID.to_string(),
            shard: SHARD,
            primary: "p".to_string(),
            term: 1,
            activated: true,
            in_sync: vec!["q".to_string(), "r".to_string()],
            copies: ["p", "q", "r"]
                .into_iter()
                .map(|node| TraceStartCopy {
                    node: node.to_string(),
                    allocation: 1,
                    exists: true,
                    fence_term: 1,
                })
                .collect(),
        },
        mutation,
        faults: vec![
            FaultRule {
                target: "q".to_string(),
                seq_no: 0,
                action: FaultAction::DelayRequest {
                    millis: 150 + seed % 31,
                },
            },
            FaultRule {
                target: "q".to_string(),
                seq_no: 5,
                action: FaultAction::DropRequest,
            },
        ],
    })?;
    for manager in [&p_cm, &q_cm, &r_cm] {
        manager.record_protocol_trace_routing_views()?;
    }

    let operation_gate = Arc::new(RwLock::new(()));
    let mut first_client = connect(p.address).await?;
    let first_gate = operation_gate.clone();
    let first = tokio::spawn(async move {
        let _operation = first_gate.read_owned().await;
        index_document(&mut first_client, "ordered", 10).await
    });
    wait_for_primary_sequence(&p_sm, 0).await?;
    let mut second_client = connect(p.address).await?;
    let second_gate = operation_gate.clone();
    let second = tokio::spawn(async move {
        let _operation = second_gate.read_owned().await;
        index_document(&mut second_client, "ordered", 20).await
    });
    let first = first.await??;
    let second = second.await??;
    assert!(first.success, "first ordered write failed: {}", first.error);
    assert!(
        second.success,
        "second ordered write failed: {}",
        second.error
    );
    assert_eq!(first.seq_no, Some(0));
    assert_eq!(second.seq_no, Some(1));

    let start = Arc::new(Barrier::new(3));
    let mut bulk_client = connect(p.address).await?;
    let bulk_start = start.clone();
    let bulk_gate = operation_gate.clone();
    let bulk = tokio::spawn(async move {
        bulk_start.wait().await;
        if seed & 1 == 0 {
            tokio::task::yield_now().await;
        }
        let _operation = bulk_gate.read_owned().await;
        Ok::<_, anyhow::Error>(
            bulk_client
                .bulk_index(tonic::Request::new(ShardBulkRequest {
                    index_name: INDEX.to_string(),
                    shard_id: SHARD,
                    documents_json: vec![
                        serde_json::to_vec(&serde_json::json!({
                            "_doc_id": "bulk-a",
                            "_source": {"value": 30}
                        }))?,
                        serde_json::to_vec(&serde_json::json!({
                            "_doc_id": "bulk-b",
                            "_source": {"value": 40}
                        }))?,
                    ],
                }))
                .await?
                .into_inner(),
        )
    });
    let mut concurrent_client = connect(p.address).await?;
    let concurrent_start = start.clone();
    let concurrent_gate = operation_gate.clone();
    let concurrent = tokio::spawn(async move {
        concurrent_start.wait().await;
        if seed & 1 != 0 {
            tokio::task::yield_now().await;
        }
        let _operation = concurrent_gate.read_owned().await;
        index_document(&mut concurrent_client, "concurrent", 50).await
    });
    start.wait().await;
    let bulk = bulk.await??;
    let concurrent = concurrent.await??;
    assert!(bulk.success, "bulk write failed: {}", bulk.error);
    assert!(
        concurrent.success,
        "concurrent write failed: {}",
        concurrent.error
    );
    let mut concurrent_sequences = vec![concurrent.seq_no.context("single sequence missing")?];
    let bulk_start = bulk.start_seq_no.context("bulk sequence missing")?;
    concurrent_sequences.extend([bulk_start, bulk_start + 1]);
    concurrent_sequences.sort_unstable();
    assert_eq!(concurrent_sequences, vec![2, 3, 4]);

    let mut primary_client = connect(p.address).await?;
    let dropped = {
        let _operation = operation_gate.read().await;
        index_document(&mut primary_client, "collision", 60).await?
    };
    assert!(
        !dropped.success,
        "injected replica drop must fail the request"
    );
    assert_eq!(dropped.seq_no, Some(5));
    let after_gap = {
        let _operation = operation_gate.read().await;
        index_document(&mut primary_client, "after-gap", 70).await?
    };
    assert!(
        after_gap.success,
        "post-gap write failed: {}",
        after_gap.error
    );
    assert_eq!(after_gap.seq_no, Some(6));
    drop(primary_client);

    {
        let _exclusive = operation_gate.write().await;
        stop_node(p).await;
        protocol_trace::record_node_crashed("p", "unclean")?;
        assert_command_ok(
            state_machine.apply_command_for_protocol_trace_test(
                &ClusterCommand::FailShardCopy {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    node: "p".to_string(),
                    allocation_id: 1,
                    expected_primary_term: 1,
                    promote_only: true,
                    promotion_candidate: Some("q".to_string()),
                },
                2,
            ),
            "primary promotion",
        )?;
        let promoted = current_authoritative_state(&state_machine);
        q_cm.update_state(promoted.clone());
        r_cm.update_state(promoted);
        assert_command_ok(
            state_machine.apply_command_for_protocol_trace_test(
                &ClusterCommand::ActivatePrimary {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    primary: "q".to_string(),
                    allocation_id: 1,
                    expected_term: 2,
                },
                3,
            ),
            "primary activation",
        )?;
        let activated = current_authoritative_state(&state_machine);
        q_cm.update_state(activated.clone());
        r_cm.update_state(activated);
    }

    {
        let _operation = operation_gate.read().await;
        q.service
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    let collision_quarantined = r_sm.get_shard(INDEX, SHARD).is_none();
    assert_eq!(
        collision_quarantined,
        mutation != MutationMode::SeqOnlyRedelivery,
        "term-aware collision behavior did not match the selected mutation"
    );
    if collision_quarantined {
        {
            let _operation = operation_gate.read().await;
            q.service
                .protocol_trace_activate_primary_for_test(INDEX, SHARD)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        let _exclusive = operation_gate.write().await;
        assert_command_ok(
            state_machine.apply_command_for_protocol_trace_test(
                &ClusterCommand::FailShardCopy {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    node: "r".to_string(),
                    allocation_id: 1,
                    expected_primary_term: 3,
                    promote_only: false,
                    promotion_candidate: None,
                },
                4,
            ),
            "colliding replica removal",
        )?;
        let without_collision = current_authoritative_state(&state_machine);
        q_cm.update_state(without_collision.clone());
        r_cm.update_state(without_collision);
    }

    {
        let _operation = operation_gate.read().await;
        q.service
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    let mut promoted_client = connect(q.address).await?;
    let promoted_write = {
        let _operation = operation_gate.read().await;
        index_document(&mut promoted_client, "post-failover", 80).await?
    };
    assert!(
        promoted_write.success,
        "promoted-primary write failed: {}",
        promoted_write.error
    );
    assert_eq!(promoted_write.seq_no, Some(7));
    drop(promoted_client);

    let q_address = q.address;
    {
        let _exclusive = operation_gate.write().await;
        q_sm.close_protocol_trace_shard_for_restart(INDEX, SHARD);
        stop_node(q).await;
        protocol_trace::record_node_crashed("q", "unclean")?;
        protocol_trace::prepare_node_restart("q")?;
    }
    drop(q_sm);
    let q_sm = Arc::new(ShardManager::new(q_dir.path(), Duration::from_secs(60)));
    let q = start_node(q_cm.clone(), q_sm.clone(), "q", Some(q_address.port())).await?;
    let mut restart_probe = connect(q.address).await?;
    {
        let _operation = operation_gate.read().await;
        let response = restart_probe
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                doc_id: "after-gap".to_string(),
            }))
            .await?
            .into_inner();
        assert!(response.found, "restarted primary did not replay after-gap");
    }
    drop(restart_probe);
    {
        let _exclusive = operation_gate.write().await;
        assert_command_ok(
            state_machine.apply_command_for_protocol_trace_test(
                &ClusterCommand::ActivatePrimary {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    primary: "q".to_string(),
                    allocation_id: 1,
                    expected_term: 3,
                },
                5,
            ),
            "post-restart primary activation",
        )?;
        let reactivated = current_authoritative_state(&state_machine);
        q_cm.update_state(reactivated.clone());
        r_cm.update_state(reactivated);
    }
    {
        let _operation = operation_gate.read().await;
        q.service
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    let mut restarted_client = connect(q.address).await?;
    let restarted_write = {
        let _operation = operation_gate.read().await;
        index_document(&mut restarted_client, "post-restart", 90).await?
    };
    assert!(
        restarted_write.success,
        "post-restart write failed: {}",
        restarted_write.error
    );
    assert_eq!(restarted_write.seq_no, Some(8));
    drop(restarted_client);

    {
        let _exclusive = operation_gate.write().await;
        let snapshots = [
            capture_final_copy_state(&q_sm)?,
            capture_final_copy_state(&r_sm)?,
        ];
        for snapshot in snapshots.into_iter().flatten() {
            protocol_trace::record_copy_snapshot(snapshot, "trace_end")?;
        }
    }
    let output = trace.finish(true)?;
    assert_trace_completeness(&output)?;
    assert!(Path::new(&output).is_file());
    println!("D1 protocol trace: {}", output.display());

    stop_node(q).await;
    stop_node(r).await;
    Ok(())
}
