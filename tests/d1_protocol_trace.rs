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
use ferrissearch::shard::{PeerRecoveryTargetState, ShardManager};
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::internal_transport_client::InternalTransportClient;
use ferrissearch::transport::proto::{
    ShardBulkRequest, ShardDeleteRequest, ShardDocRequest, ShardGetRequest,
};
use ferrissearch::transport::server::{
    TransportService, create_transport_service_for_test_with_handle,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
            ..Default::default()
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

const RANDOM_DOC_IDS: [&str; 6] = ["d0", "d1", "d2", "d3", "d4", "d5"];

#[derive(Clone, Debug)]
enum PlannedOperation {
    Index { doc_id: String, value: i64 },
    Delete { doc_id: String },
    Bulk { documents: Vec<(String, i64)> },
}

impl PlannedOperation {
    fn sequence_count(&self) -> u64 {
        match self {
            Self::Index { .. } | Self::Delete { .. } => 1,
            Self::Bulk { documents } => documents.len() as u64,
        }
    }

    fn schedule_json(&self) -> serde_json::Value {
        match self {
            Self::Index { doc_id, value } => serde_json::json!({
                "op": "index",
                "doc": doc_id,
                "value": value,
            }),
            Self::Delete { doc_id } => serde_json::json!({
                "op": "delete",
                "doc": doc_id,
            }),
            Self::Bulk { documents } => serde_json::json!({
                "op": "bulk",
                "documents": documents
                    .iter()
                    .map(|(doc_id, value)| serde_json::json!({
                        "doc": doc_id,
                        "value": value,
                    }))
                    .collect::<Vec<_>>(),
            }),
        }
    }
}

#[derive(Clone, Debug)]
struct PlannedRequest {
    ordinal: usize,
    operation: PlannedOperation,
}

impl PlannedRequest {
    fn schedule_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ordinal": self.ordinal,
            "operation": self.operation.schedule_json(),
        })
    }
}

#[derive(Clone, Debug)]
struct RandomSchedule {
    seed: u64,
    clients: usize,
    random_requests: Vec<PlannedRequest>,
    final_requests: Vec<PlannedRequest>,
    restart_after: usize,
    failover_after: usize,
    gap_request: PlannedRequest,
    after_gap_request: PlannedRequest,
    gap_seq_no: u64,
    faults: Vec<FaultRule>,
}

impl RandomSchedule {
    fn request_count(&self) -> usize {
        self.random_requests.len() + self.final_requests.len() + 2
    }

    fn operation_count(&self) -> u64 {
        self.random_requests
            .iter()
            .chain([&self.gap_request, &self.after_gap_request])
            .chain(self.final_requests.iter())
            .map(|request| request.operation.sequence_count())
            .sum()
    }

    fn dropped_sequences(&self) -> BTreeSet<u64> {
        self.faults
            .iter()
            .filter_map(|fault| {
                matches!(
                    fault.action,
                    FaultAction::DropRequest | FaultAction::DropResponse
                )
                .then_some(fault.seq_no)
            })
            .collect()
    }

    fn fault_counts(&self) -> BTreeMap<&'static str, usize> {
        let mut counts = BTreeMap::from([
            ("delay", 0usize),
            ("drop_request", 0usize),
            ("drop_response", 0usize),
            ("hold_until_applied", 0usize),
        ]);
        for fault in &self.faults {
            let label = match fault.action {
                FaultAction::DelayRequest { .. } => "delay",
                FaultAction::HoldRequestUntilApplied { .. } => "hold_until_applied",
                FaultAction::DropRequest => "drop_request",
                FaultAction::DropResponse => "drop_response",
            };
            *counts.get_mut(label).expect("fault counter exists") += 1;
        }
        counts
    }

    fn schedule_json(&self) -> serde_json::Value {
        let faults = self
            .faults
            .iter()
            .map(|fault| {
                let action = match fault.action {
                    FaultAction::DelayRequest { millis } => {
                        serde_json::json!({"kind": "delay", "millis": millis})
                    }
                    FaultAction::HoldRequestUntilApplied { seq_no } => {
                        serde_json::json!({
                            "kind": "hold_until_applied",
                            "seq_no": seq_no,
                        })
                    }
                    FaultAction::DropRequest => serde_json::json!({"kind": "drop_request"}),
                    FaultAction::DropResponse => serde_json::json!({"kind": "drop_response"}),
                };
                serde_json::json!({
                    "target": fault.target,
                    "seq_no": fault.seq_no,
                    "action": action,
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "seed": self.seed,
            "clients": self.clients,
            "request_count": self.request_count(),
            "operation_count": self.operation_count(),
            "restart_after_random_request": self.restart_after,
            "failover_after_random_request": self.failover_after,
            "gap_seq_no": self.gap_seq_no,
            "random_requests": self
                .random_requests
                .iter()
                .map(PlannedRequest::schedule_json)
                .collect::<Vec<_>>(),
            "gap_request": self.gap_request.schedule_json(),
            "after_gap_request": self.after_gap_request.schedule_json(),
            "final_requests": self
                .final_requests
                .iter()
                .map(PlannedRequest::schedule_json)
                .collect::<Vec<_>>(),
            "faults": faults,
            "control_faults": [
                "replica_crash_restart",
                "primary_crash_promotion",
                "collision_removal",
                "peer_recovery",
            ],
        })
    }
}

struct SeedRng {
    state: u64,
}

impl SeedRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0xA076_1D64_78BD_642F,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn usize(&mut self, upper_exclusive: usize) -> usize {
        assert!(upper_exclusive > 0);
        (self.next_u64() % upper_exclusive as u64) as usize
    }

    fn inclusive(&mut self, minimum: usize, maximum: usize) -> usize {
        assert!(minimum <= maximum);
        minimum + self.usize(maximum - minimum + 1)
    }

    fn percent(&mut self, threshold: u64) -> bool {
        self.next_u64() % 100 < threshold
    }
}

fn planned_value(seed: u64, ordinal: usize, offset: usize) -> i64 {
    ((seed % 1_000_000) * 1_000 + (ordinal * 10 + offset) as u64) as i64
}

fn random_doc_id(rng: &mut SeedRng) -> String {
    RANDOM_DOC_IDS[rng.usize(RANDOM_DOC_IDS.len())].to_string()
}

fn random_operation(
    rng: &mut SeedRng,
    seed: u64,
    ordinal: usize,
    max_sequences: usize,
) -> PlannedOperation {
    let max_sequences = max_sequences.clamp(1, 5);
    let roll = rng.usize(100);
    if roll < 45 {
        PlannedOperation::Index {
            doc_id: random_doc_id(rng),
            value: planned_value(seed, ordinal, 0),
        }
    } else if roll < 70 {
        PlannedOperation::Delete {
            doc_id: random_doc_id(rng),
        }
    } else {
        let count = rng.inclusive(1, max_sequences);
        PlannedOperation::Bulk {
            documents: (0..count)
                .map(|offset| (random_doc_id(rng), planned_value(seed, ordinal, offset)))
                .collect(),
        }
    }
}

fn random_fault(
    rng: &mut SeedRng,
    target: &str,
    seq_no: u64,
    force: Option<FaultAction>,
) -> Option<FaultRule> {
    let action = match force {
        Some(action) => action,
        None => {
            let roll = rng.usize(100);
            if roll < 28 {
                FaultAction::DelayRequest {
                    millis: rng.inclusive(0, 200) as u64,
                }
            } else if roll < 36 {
                FaultAction::DropResponse
            } else {
                return None;
            }
        }
    };
    Some(FaultRule {
        target: target.to_string(),
        seq_no,
        action,
    })
}

fn build_random_schedule(seed: u64) -> RandomSchedule {
    let mut rng = SeedRng::new(seed);
    let operation_count = rng.inclusive(20, 60) as u64;
    let clients = rng.inclusive(2, 4);
    let fixed_operation_count = RANDOM_DOC_IDS.len() as u64 + 2;
    let random_operation_budget = operation_count - fixed_operation_count;
    let mut random_requests = vec![
        PlannedRequest {
            ordinal: 0,
            operation: PlannedOperation::Index {
                doc_id: "d0".to_string(),
                value: planned_value(seed, 0, 0),
            },
        },
        PlannedRequest {
            ordinal: 1,
            operation: PlannedOperation::Index {
                doc_id: "d0".to_string(),
                value: planned_value(seed, 1, 0),
            },
        },
        PlannedRequest {
            ordinal: 2,
            operation: PlannedOperation::Delete {
                doc_id: random_doc_id(&mut rng),
            },
        },
        PlannedRequest {
            ordinal: 3,
            operation: PlannedOperation::Bulk {
                documents: (0..rng
                    .inclusive(1, usize::min(5, (random_operation_budget - 7) as usize)))
                    .map(|offset| (random_doc_id(&mut rng), planned_value(seed, 3, offset)))
                    .collect(),
            },
        },
    ];
    let mut planned_random_operations = random_requests
        .iter()
        .map(|request| request.operation.sequence_count())
        .sum::<u64>();
    while random_requests.len() < 8 {
        let ordinal = random_requests.len();
        let operation = random_operation(&mut rng, seed, ordinal, 1);
        planned_random_operations += operation.sequence_count();
        random_requests.push(PlannedRequest { ordinal, operation });
    }
    while planned_random_operations < random_operation_budget {
        let ordinal = random_requests.len();
        let remaining = (random_operation_budget - planned_random_operations) as usize;
        let operation = random_operation(&mut rng, seed, ordinal, remaining);
        planned_random_operations += operation.sequence_count();
        random_requests.push(PlannedRequest { ordinal, operation });
    }
    debug_assert_eq!(planned_random_operations, random_operation_budget);

    let random_count = random_requests.len();
    let failover_after = rng.inclusive(6, random_count - 2);
    let restart_after = rng.inclusive(2, failover_after - 1);
    let mut request_starts = Vec::with_capacity(random_count);
    let mut next_seq_no = 0u64;
    for request in &random_requests {
        request_starts.push(next_seq_no);
        next_seq_no += request.operation.sequence_count();
    }
    let gap_seq_no = random_requests[..failover_after]
        .iter()
        .map(|request| request.operation.sequence_count())
        .sum();
    let gap_doc = random_doc_id(&mut rng);
    let after_gap_doc = random_doc_id(&mut rng);
    let gap_request = PlannedRequest {
        ordinal: random_count,
        operation: PlannedOperation::Index {
            doc_id: gap_doc,
            value: planned_value(seed, random_count, 0),
        },
    };
    let after_gap_request = PlannedRequest {
        ordinal: random_count + 1,
        operation: PlannedOperation::Index {
            doc_id: after_gap_doc,
            value: planned_value(seed, random_count + 1, 0),
        },
    };
    let final_requests = RANDOM_DOC_IDS
        .iter()
        .enumerate()
        .map(|(offset, doc_id)| PlannedRequest {
            ordinal: random_count + 2 + offset,
            operation: PlannedOperation::Index {
                doc_id: (*doc_id).to_string(),
                value: planned_value(seed, random_count + 2 + offset, 0),
            },
        })
        .collect::<Vec<_>>();

    let mut faults = vec![FaultRule {
        target: "q".to_string(),
        seq_no: 0,
        action: FaultAction::HoldRequestUntilApplied { seq_no: 1 },
    }];
    for (index, seq_no) in request_starts
        .iter()
        .copied()
        .enumerate()
        .take(failover_after)
        .skip(2)
    {
        let target = if rng.percent(50) { "q" } else { "r" };
        let force = match index {
            2 => Some(FaultAction::DropResponse),
            3 => Some(FaultAction::DelayRequest {
                millis: rng.inclusive(1, 200) as u64,
            }),
            _ => None,
        };
        if let Some(fault) = random_fault(&mut rng, target, seq_no, force) {
            faults.push(fault);
        }
    }
    faults.push(FaultRule {
        target: "q".to_string(),
        seq_no: gap_seq_no,
        action: FaultAction::DropRequest,
    });
    if rng.percent(50) {
        faults.push(FaultRule {
            target: "r".to_string(),
            seq_no: gap_seq_no + 1,
            action: FaultAction::DelayRequest {
                millis: rng.inclusive(0, 200) as u64,
            },
        });
    }

    let mut post_seq_no = gap_seq_no + 2;
    for request in random_requests.iter().skip(failover_after) {
        if let Some(fault) = random_fault(&mut rng, "r", post_seq_no, None) {
            faults.push(fault);
        }
        post_seq_no += request.operation.sequence_count();
    }
    for request in &final_requests {
        if rng.percent(30) {
            faults.push(FaultRule {
                target: "r".to_string(),
                seq_no: post_seq_no,
                action: FaultAction::DelayRequest {
                    millis: rng.inclusive(0, 200) as u64,
                },
            });
        }
        post_seq_no += request.operation.sequence_count();
    }

    RandomSchedule {
        seed,
        clients,
        random_requests,
        final_requests,
        restart_after,
        failover_after,
        gap_request,
        after_gap_request,
        gap_seq_no,
        faults,
    }
}

#[test]
fn randomized_schedule_is_reproducible_and_bounded() {
    for seed in 0..1_000 {
        let schedule = build_random_schedule(seed);
        assert_eq!(
            schedule.schedule_json(),
            build_random_schedule(seed).schedule_json()
        );
        assert!((2..=4).contains(&schedule.clients));
        assert!((20..=60).contains(&schedule.operation_count()));
        assert!(schedule.restart_after >= 2);
        assert!(schedule.restart_after < schedule.failover_after);
        assert!(schedule.failover_after <= schedule.random_requests.len() - 2);

        let mut kinds = BTreeSet::new();
        for request in schedule
            .random_requests
            .iter()
            .chain([&schedule.gap_request, &schedule.after_gap_request])
            .chain(schedule.final_requests.iter())
        {
            match &request.operation {
                PlannedOperation::Index { .. } => {
                    kinds.insert("index");
                }
                PlannedOperation::Delete { .. } => {
                    kinds.insert("delete");
                }
                PlannedOperation::Bulk { documents } => {
                    assert!((1..=5).contains(&documents.len()));
                    kinds.insert("bulk");
                }
            }
        }
        assert_eq!(kinds, BTreeSet::from(["bulk", "delete", "index"]));
        for fault in &schedule.faults {
            if let FaultAction::DelayRequest { millis } = fault.action {
                assert!(millis <= 200);
            }
        }
    }
}

fn parse_random_seeds() -> Result<Vec<u64>> {
    let Ok(value) = std::env::var("D1_TRACE_SEEDS") else {
        return Ok(vec![parse_seed()?]);
    };
    let mut seeds = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        if let Some((start, end)) = token.split_once("..=") {
            let start = start
                .parse::<u64>()
                .with_context(|| format!("invalid D1_TRACE_SEEDS range start '{start}'"))?;
            let end = end
                .parse::<u64>()
                .with_context(|| format!("invalid D1_TRACE_SEEDS range end '{end}'"))?;
            anyhow::ensure!(start <= end, "D1_TRACE_SEEDS range is reversed");
            seeds.extend(start..=end);
        } else {
            seeds.push(
                token
                    .parse()
                    .with_context(|| format!("invalid D1_TRACE_SEEDS value '{token}'"))?,
            );
        }
    }
    anyhow::ensure!(!seeds.is_empty(), "D1_TRACE_SEEDS is empty");
    Ok(seeds)
}

fn mutation_label(mutation: MutationMode) -> &'static str {
    match mutation {
        MutationMode::None => "correct",
        MutationMode::ArrivalOrderApply => "arrival-order",
        MutationMode::SeqOnlyRedelivery => "seq-only-redelivery",
    }
}

fn random_trace_output(
    trace_dir: &TempDir,
    seed: u64,
    mutation: MutationMode,
    multiple: bool,
) -> Result<PathBuf> {
    if let Some(output) = std::env::var_os("D1_TRACE_OUTPUT") {
        anyhow::ensure!(
            !multiple,
            "D1_TRACE_OUTPUT cannot be used with more than one randomized seed"
        );
        return Ok(PathBuf::from(output));
    }
    let directory = std::env::var_os("D1_TRACE_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| trace_dir.path().to_path_buf());
    Ok(directory.join(format!(
        "d1-random-{}-{seed}.jsonl",
        mutation_label(mutation)
    )))
}

struct RandomNode {
    _data_dir: TempDir,
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    running: Option<RunningNode>,
}

impl RandomNode {
    async fn start(node_id: &str) -> Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let cluster_manager = Arc::new(ClusterManager::new("d1-trace".to_string()));
        let shard_manager = Arc::new(ShardManager::new(data_dir.path(), Duration::from_secs(60)));
        let running = start_node(
            cluster_manager.clone(),
            shard_manager.clone(),
            node_id,
            None,
        )
        .await?;
        Ok(Self {
            _data_dir: data_dir,
            cluster_manager,
            shard_manager,
            running: Some(running),
        })
    }
}

struct RandomTraceCluster {
    nodes: BTreeMap<String, RandomNode>,
    state_machine: ClusterStateMachine,
    next_log_index: u64,
    current_primary: String,
    operation_gate: Arc<RwLock<()>>,
}

impl RandomTraceCluster {
    async fn start() -> Result<Self> {
        let mut nodes = BTreeMap::new();
        for node_id in ["p", "q", "r"] {
            nodes.insert(node_id.to_string(), RandomNode::start(node_id).await?);
        }
        let initial = initial_cluster_state(
            nodes["p"].running.as_ref().unwrap().address,
            nodes["q"].running.as_ref().unwrap().address,
            nodes["r"].running.as_ref().unwrap().address,
        );
        for node in nodes.values() {
            node.cluster_manager.update_state(initial.clone());
        }
        for node_id in ["q", "r"] {
            let node = &nodes[node_id];
            install_empty_replica(&node.cluster_manager, &node.shard_manager)?;
        }
        nodes["p"]
            .running
            .as_ref()
            .unwrap()
            .service
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;

        let mut initialized = initial;
        initialized
            .shard_allocations
            .get_mut(INDEX)
            .and_then(|shards| shards.get_mut(&SHARD))
            .context("random trace shard allocation metadata is missing")?
            .primary_initialized = true;
        initialized.version += 1;
        for node in nodes.values() {
            node.cluster_manager.update_state(initialized.clone());
        }
        Ok(Self {
            nodes,
            state_machine: ClusterStateMachine::from_state_for_protocol_trace_test(initialized),
            next_log_index: 2,
            current_primary: "p".to_string(),
            operation_gate: Arc::new(RwLock::new(())),
        })
    }

    fn node(&self, node_id: &str) -> Result<&RandomNode> {
        self.nodes
            .get(node_id)
            .with_context(|| format!("random trace node '{node_id}' is missing"))
    }

    fn address(&self, node_id: &str) -> Result<std::net::SocketAddr> {
        self.node(node_id)?
            .running
            .as_ref()
            .map(|node| node.address)
            .with_context(|| format!("random trace node '{node_id}' is stopped"))
    }

    fn service(&self, node_id: &str) -> Result<TransportService> {
        self.node(node_id)?
            .running
            .as_ref()
            .map(|node| node.service.clone())
            .with_context(|| format!("random trace node '{node_id}' is stopped"))
    }

    fn authoritative_state(&self) -> ferrissearch::cluster::state::ClusterState {
        current_authoritative_state(&self.state_machine)
    }

    fn publish_state(&self, state: &ferrissearch::cluster::state::ClusterState) {
        for node in self.nodes.values() {
            if node.running.is_some() {
                node.cluster_manager.update_state(state.clone());
            }
        }
    }

    fn apply_command(
        &mut self,
        command: ClusterCommand,
        action: &str,
    ) -> Result<(ferrissearch::cluster::state::ClusterState, u64)> {
        let log_index = self.next_log_index;
        self.next_log_index += 1;
        assert_command_ok(
            self.state_machine
                .apply_command_for_protocol_trace_test(&command, log_index),
            action,
        )?;
        let state = self.authoritative_state();
        self.publish_state(&state);
        Ok((state, log_index))
    }

    fn record_initial_routing_views(&self) -> Result<()> {
        for node in self.nodes.values() {
            node.cluster_manager.record_protocol_trace_routing_views()?;
        }
        Ok(())
    }

    async fn restart_replica(&mut self, node_id: &str) -> Result<()> {
        let operation_gate = self.operation_gate.clone();
        let _exclusive = operation_gate.write_owned().await;
        let port = self.address(node_id)?.port();
        for node in self.nodes.values() {
            if let Some(running) = node.running.as_ref() {
                running
                    .service
                    .transport_client
                    .evict_protocol_trace_channel("127.0.0.1", port);
            }
        }
        let node = self
            .nodes
            .get_mut(node_id)
            .with_context(|| format!("restart node '{node_id}' is missing"))?;
        node.shard_manager
            .close_protocol_trace_shard_for_restart(INDEX, SHARD);
        let running = node
            .running
            .take()
            .with_context(|| format!("restart node '{node_id}' is already stopped"))?;
        stop_node(running).await;
        protocol_trace::record_node_crashed(node_id, "unclean")?;
        protocol_trace::prepare_node_restart(node_id)?;
        let shard_manager = Arc::new(ShardManager::new(
            node._data_dir.path(),
            Duration::from_secs(60),
        ));
        let previous_shard_manager =
            std::mem::replace(&mut node.shard_manager, shard_manager.clone());
        drop(previous_shard_manager);
        let running = start_node(
            node.cluster_manager.clone(),
            shard_manager.clone(),
            node_id,
            Some(port),
        )
        .await?;
        node.running = Some(running);

        let mut probe = connect(node.running.as_ref().unwrap().address).await?;
        let response = probe
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                doc_id: "d0".to_string(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        anyhow::ensure!(
            response.error.is_empty(),
            "restarted replica probe failed: {}",
            response.error
        );
        anyhow::ensure!(
            Arc::ptr_eq(
                &node.shard_manager,
                &node.running.as_ref().unwrap().service.shard_manager
            ),
            "replacement service is using a different shard manager"
        );
        anyhow::ensure!(
            node.shard_manager.get_shard(INDEX, SHARD).is_some(),
            "restarted replica probe did not retain the replayed engine"
        );
        Ok(())
    }

    async fn failover_and_recover(&mut self) -> Result<()> {
        {
            let operation_gate = self.operation_gate.clone();
            let _exclusive = operation_gate.write_owned().await;
            let primary = self
                .nodes
                .get_mut("p")
                .context("random trace primary node is missing")?
                .running
                .take()
                .context("random trace primary is already stopped")?;
            stop_node(primary).await;
            protocol_trace::record_node_crashed("p", "unclean")?;

            let state = self.authoritative_state();
            let term = state.indices[INDEX].shard_routing[&SHARD].primary_term;
            let allocation = state
                .shard_allocation_id(INDEX, SHARD, "p")
                .context("failed primary allocation is missing")?;
            self.apply_command(
                ClusterCommand::FailShardCopy {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    node: "p".to_string(),
                    allocation_id: allocation,
                    expected_primary_term: term,
                    promote_only: true,
                    promotion_candidate: Some("q".to_string()),
                },
                "random primary promotion",
            )?;
            let promoted = self.authoritative_state();
            let promoted_term = promoted.indices[INDEX].shard_routing[&SHARD].primary_term;
            let promoted_allocation = promoted
                .shard_allocation_id(INDEX, SHARD, "q")
                .context("promoted allocation is missing")?;
            self.apply_command(
                ClusterCommand::ActivatePrimary {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    primary: "q".to_string(),
                    allocation_id: promoted_allocation,
                    expected_term: promoted_term,
                },
                "random primary activation",
            )?;
            self.current_primary = "q".to_string();
        }

        self.service("q")?
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;

        {
            let operation_gate = self.operation_gate.clone();
            let _exclusive = operation_gate.write_owned().await;
            let state = self.authoritative_state();
            let term = state.indices[INDEX].shard_routing[&SHARD].primary_term;
            let allocation = state
                .shard_allocation_id(INDEX, SHARD, "r")
                .context("colliding replica allocation is missing")?;
            self.apply_command(
                ClusterCommand::FailShardCopy {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    node: "r".to_string(),
                    allocation_id: allocation,
                    expected_primary_term: term,
                    promote_only: false,
                    promotion_candidate: None,
                },
                "random colliding replica removal",
            )?;
        }

        self.service("q")?
            .protocol_trace_activate_primary_for_test(INDEX, SHARD)
            .await
            .map_err(anyhow::Error::msg)?;

        let target_allocation;
        {
            let operation_gate = self.operation_gate.clone();
            let _exclusive = operation_gate.write_owned().await;
            let state = self.authoritative_state();
            let mut metadata = state.indices[INDEX].clone();
            let routing = metadata
                .shard_routing
                .get_mut(&SHARD)
                .context("random routing is missing during reassignment")?;
            anyhow::ensure!(
                !routing.replicas.iter().any(|replica| replica == "r"),
                "removed replica is still assigned"
            );
            anyhow::ensure!(
                routing.unassigned_replicas > 0,
                "no unassigned replica slot remains for recovery"
            );
            routing.replicas.push("r".to_string());
            routing.unassigned_replicas -= 1;
            let (assigned, log_index) = self.apply_command(
                ClusterCommand::UpdateIndex { metadata },
                "random replica reassignment",
            )?;
            target_allocation = assigned
                .shard_allocation_id(INDEX, SHARD, "r")
                .context("reassigned replica allocation is missing")?;
            anyhow::ensure!(
                target_allocation == log_index,
                "reassigned allocation does not match the assigning log index"
            );
        }

        let target_manager = self.node("r")?.cluster_manager.clone();
        let target_shards = self.node("r")?.shard_manager.clone();
        let recovery_state = target_manager.get_state();
        ferrissearch::node::start_peer_recovery_for_protocol_trace_test(
            &recovery_state,
            "r",
            target_manager.clone(),
            target_shards.clone(),
        );

        let pending = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(pending) = target_shards
                    .peer_recovery_target_states()
                    .into_iter()
                    .find_map(|(_, state)| match state {
                        PeerRecoveryTargetState::FinalizedAwaitingMembership(pending) => {
                            Some(pending)
                        }
                        PeerRecoveryTargetState::Recovering { .. } => None,
                    })
                {
                    break pending;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("timed out waiting for randomized peer recovery finalization")?;
        anyhow::ensure!(
            pending.allocation_id == target_allocation,
            "peer recovery finalized the wrong allocation"
        );

        {
            let operation_gate = self.operation_gate.clone();
            let _exclusive = operation_gate.write_owned().await;
            let state = self.authoritative_state();
            let routing = &state.indices[INDEX].shard_routing[&SHARD];
            self.apply_command(
                ClusterCommand::MarkReplicaInSync {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    replica: "r".to_string(),
                    allocation_id: target_allocation,
                    primary: routing.primary.clone(),
                    primary_term: routing.primary_term,
                },
                "random peer recovery admission",
            )?;
        }

        tokio::time::timeout(Duration::from_secs(20), async {
            while target_shards.is_peer_recovery_target(INDEX, SHARD) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("timed out waiting for randomized peer recovery settlement")?;
        anyhow::ensure!(
            target_shards.get_shard(INDEX, SHARD).is_some(),
            "peer-recovered replica is unavailable"
        );
        Ok(())
    }

    async fn stop_all(&mut self) {
        for node in self.nodes.values_mut() {
            if let Some(running) = node.running.take() {
                stop_node(running).await;
            }
        }
    }
}

#[derive(Clone, Debug)]
struct AckedMutation {
    doc_id: String,
    seq_no: u64,
    value: Option<i64>,
}

#[derive(Debug)]
struct ExecutedRequest {
    ordinal: usize,
    success: bool,
    error: String,
    start_seq_no: Option<u64>,
    sequence_count: u64,
    acknowledged: Vec<AckedMutation>,
}

async fn execute_planned_request(
    client: &mut InternalTransportClient<tonic::transport::Channel>,
    request: PlannedRequest,
) -> Result<ExecutedRequest> {
    let sequence_count = request.operation.sequence_count();
    match request.operation {
        PlannedOperation::Index { doc_id, value } => {
            let response = index_document(client, &doc_id, value).await?;
            Ok(ExecutedRequest {
                ordinal: request.ordinal,
                success: response.success,
                error: response.error,
                start_seq_no: response.seq_no,
                sequence_count,
                acknowledged: response
                    .success
                    .then(|| {
                        response.seq_no.map(|seq_no| AckedMutation {
                            doc_id,
                            seq_no,
                            value: Some(value),
                        })
                    })
                    .flatten()
                    .into_iter()
                    .collect(),
            })
        }
        PlannedOperation::Delete { doc_id } => {
            let response = client
                .delete_doc(tonic::Request::new(ShardDeleteRequest {
                    index_name: INDEX.to_string(),
                    shard_id: SHARD,
                    doc_id: doc_id.clone(),
                    ..Default::default()
                }))
                .await?
                .into_inner();
            Ok(ExecutedRequest {
                ordinal: request.ordinal,
                success: response.success,
                error: response.error,
                start_seq_no: response.seq_no,
                sequence_count,
                acknowledged: response
                    .success
                    .then(|| {
                        response.seq_no.map(|seq_no| AckedMutation {
                            doc_id,
                            seq_no,
                            value: None,
                        })
                    })
                    .flatten()
                    .into_iter()
                    .collect(),
            })
        }
        PlannedOperation::Bulk { documents } => {
            let response = client
                .bulk_index(tonic::Request::new(ShardBulkRequest {
                    index_name: INDEX.to_string(),
                    shard_id: SHARD,
                    documents_json: documents
                        .iter()
                        .map(|(doc_id, value)| {
                            serde_json::to_vec(&serde_json::json!({
                                "_doc_id": doc_id,
                                "_source": {"value": value},
                            }))
                        })
                        .collect::<serde_json::Result<Vec<_>>>()?,
                    ..Default::default()
                }))
                .await?
                .into_inner();
            let acknowledged = if response.success {
                let start_seq_no = response
                    .start_seq_no
                    .context("successful randomized bulk response has no sequence")?;
                documents
                    .into_iter()
                    .enumerate()
                    .map(|(offset, (doc_id, value))| AckedMutation {
                        doc_id,
                        seq_no: start_seq_no + offset as u64,
                        value: Some(value),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            Ok(ExecutedRequest {
                ordinal: request.ordinal,
                success: response.success,
                error: response.error,
                start_seq_no: response.start_seq_no,
                sequence_count,
                acknowledged,
            })
        }
    }
}

async fn execute_wave(
    cluster: &RandomTraceCluster,
    requests: &[PlannedRequest],
) -> Result<Vec<ExecutedRequest>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let address = cluster.address(&cluster.current_primary)?;
    let start = Arc::new(Barrier::new(requests.len() + 1));
    let mut tasks = Vec::with_capacity(requests.len());
    for request in requests.iter().cloned() {
        let start = start.clone();
        let gate = cluster.operation_gate.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = connect(address).await?;
            start.wait().await;
            let _operation = gate.read_owned().await;
            execute_planned_request(&mut client, request).await
        }));
    }
    start.wait().await;
    let mut executed = Vec::with_capacity(tasks.len());
    for task in tasks {
        executed.push(task.await.context("randomized client task panicked")??);
    }
    Ok(executed)
}

fn collect_executed_requests(
    executed: Vec<ExecutedRequest>,
    dropped_sequences: &BTreeSet<u64>,
    acknowledged: &mut Vec<AckedMutation>,
) -> Result<Vec<ExecutedRequest>> {
    for request in &executed {
        if !request.success {
            eprintln!(
                "D1 randomized request failed: ordinal={} start_seq_no={:?} count={} error={}",
                request.ordinal, request.start_seq_no, request.sequence_count, request.error
            );
        }
    }
    for request in &executed {
        let start_seq_no = request
            .start_seq_no
            .with_context(|| format!("request {} has no sequence", request.ordinal))?;
        if request.success {
            acknowledged.extend(request.acknowledged.iter().cloned());
            continue;
        }
        let end_seq_no = start_seq_no
            .checked_add(request.sequence_count)
            .context("randomized request sequence range overflow")?;
        anyhow::ensure!(
            dropped_sequences
                .range(start_seq_no..end_seq_no)
                .next()
                .is_some(),
            "request {} failed without a scheduled drop at seq {}: {}",
            request.ordinal,
            start_seq_no,
            request.error
        );
        anyhow::ensure!(
            request.error.starts_with("Replication failed:"),
            "request {} failed outside replication: {}",
            request.ordinal,
            request.error
        );
    }
    Ok(executed)
}

async fn execute_request_range(
    cluster: &RandomTraceCluster,
    requests: &[PlannedRequest],
    clients: usize,
    dropped_sequences: &BTreeSet<u64>,
    acknowledged: &mut Vec<AckedMutation>,
) -> Result<()> {
    for wave in requests.chunks(clients) {
        let executed = execute_wave(cluster, wave).await?;
        collect_executed_requests(executed, dropped_sequences, acknowledged)?;
    }
    Ok(())
}

fn expected_acknowledged_documents(
    acknowledged: &[AckedMutation],
) -> BTreeMap<String, AckedMutation> {
    let mut latest = BTreeMap::<String, AckedMutation>::new();
    for operation in acknowledged {
        match latest.get(&operation.doc_id) {
            Some(existing) if existing.seq_no > operation.seq_no => {}
            _ => {
                latest.insert(operation.doc_id.clone(), operation.clone());
            }
        }
    }
    latest
}

fn assert_final_convergence(
    cluster: &RandomTraceCluster,
    acknowledged: &[AckedMutation],
) -> Result<()> {
    let state = cluster.authoritative_state();
    let routing = &state.indices[INDEX].shard_routing[&SHARD];
    let mut in_sync_nodes = vec![routing.primary.clone()];
    in_sync_nodes.extend(routing.in_sync_replicas.iter().cloned());
    in_sync_nodes.sort();
    in_sync_nodes.dedup();
    anyhow::ensure!(
        in_sync_nodes == ["q".to_string(), "r".to_string()],
        "unexpected final in-sync copies: {in_sync_nodes:?}"
    );

    let mut snapshots = Vec::new();
    for node_id in &in_sync_nodes {
        let snapshot = cluster
            .node(node_id)?
            .shard_manager
            .capture_protocol_trace_copy_state(INDEX, SHARD)?;
        snapshots.push((node_id.clone(), snapshot));
    }
    let expected = expected_acknowledged_documents(acknowledged);
    anyhow::ensure!(
        expected.len() == RANDOM_DOC_IDS.len()
            && RANDOM_DOC_IDS
                .iter()
                .all(|doc_id| expected.contains_key(*doc_id)),
        "final acknowledged cleanup did not cover every document"
    );
    let baseline = snapshots
        .first()
        .context("no in-sync copy snapshots were captured")?
        .1
        .live_documents
        .iter()
        .map(|(doc_id, seq_no, term, content_hash)| {
            (doc_id.clone(), (*seq_no, *term, content_hash.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    for (node_id, snapshot) in &snapshots {
        let actual_documents = snapshot
            .live_documents
            .iter()
            .map(|(doc_id, seq_no, term, content_hash)| {
                (doc_id.clone(), (*seq_no, *term, content_hash.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            actual_documents == baseline,
            "in-sync copy {node_id} did not converge on documents and per-document sequences"
        );
        let actual_sequences = snapshot
            .live_documents
            .iter()
            .map(|(doc_id, seq_no, _, _)| (doc_id.as_str(), *seq_no))
            .collect::<BTreeMap<_, _>>();
        for (doc_id, operation) in &expected {
            anyhow::ensure!(
                operation.value.is_some(),
                "latest acknowledged cleanup unexpectedly deleted {doc_id}"
            );
            anyhow::ensure!(
                actual_sequences.get(doc_id.as_str()) == Some(&operation.seq_no),
                "copy {node_id} has the wrong sequence for {doc_id}"
            );
            let actual = cluster
                .node(node_id)?
                .shard_manager
                .get_shard(INDEX, SHARD)
                .context("final in-sync engine is missing")?
                .get_document(doc_id)?;
            anyhow::ensure!(
                actual == Some(serde_json::json!({"value": operation.value.unwrap()})),
                "copy {node_id} has the wrong value for acknowledged document {doc_id}"
            );
        }
    }
    for (_, snapshot) in snapshots {
        protocol_trace::record_copy_snapshot(snapshot, "trace_end")?;
    }
    Ok(())
}

async fn execute_random_schedule(
    cluster: &mut RandomTraceCluster,
    schedule: &RandomSchedule,
) -> Result<()> {
    cluster.record_initial_routing_views()?;
    let dropped_sequences = schedule.dropped_sequences();
    let mut acknowledged = Vec::new();

    let first_pair = execute_wave(cluster, &schedule.random_requests[..2]).await?;
    let first_pair = collect_executed_requests(first_pair, &dropped_sequences, &mut acknowledged)?;
    anyhow::ensure!(
        first_pair.iter().all(|request| request.success),
        "the deterministic arrival-order pair did not succeed"
    );

    execute_request_range(
        cluster,
        &schedule.random_requests[2..schedule.restart_after],
        schedule.clients,
        &dropped_sequences,
        &mut acknowledged,
    )
    .await?;
    cluster.restart_replica("r").await?;
    execute_request_range(
        cluster,
        &schedule.random_requests[schedule.restart_after..schedule.failover_after],
        schedule.clients,
        &dropped_sequences,
        &mut acknowledged,
    )
    .await?;

    let gap = execute_wave(cluster, std::slice::from_ref(&schedule.gap_request)).await?;
    let gap = collect_executed_requests(gap, &dropped_sequences, &mut acknowledged)?;
    anyhow::ensure!(
        gap.len() == 1 && !gap[0].success && gap[0].start_seq_no == Some(schedule.gap_seq_no),
        "the scheduled promotion gap did not fail at seq {}",
        schedule.gap_seq_no
    );
    let after_gap =
        execute_wave(cluster, std::slice::from_ref(&schedule.after_gap_request)).await?;
    let after_gap = collect_executed_requests(after_gap, &dropped_sequences, &mut acknowledged)?;
    anyhow::ensure!(
        after_gap.len() == 1
            && after_gap[0].success
            && after_gap[0].start_seq_no == Some(schedule.gap_seq_no + 1),
        "the post-gap operation did not establish the promotion fill range"
    );

    cluster.failover_and_recover().await?;
    execute_request_range(
        cluster,
        &schedule.random_requests[schedule.failover_after..],
        schedule.clients,
        &dropped_sequences,
        &mut acknowledged,
    )
    .await?;
    for wave in schedule.final_requests.chunks(schedule.clients) {
        let executed = execute_wave(cluster, wave).await?;
        let executed = collect_executed_requests(executed, &dropped_sequences, &mut acknowledged)?;
        anyhow::ensure!(
            executed.iter().all(|request| request.success),
            "a final convergence write failed"
        );
    }

    assert_final_convergence(cluster, &acknowledged)?;
    Ok(())
}

async fn run_randomized_seed(seed: u64, mutation: MutationMode, output: PathBuf) -> Result<()> {
    let schedule = build_random_schedule(seed);
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let schedule_path = output.with_extension("schedule.json");
    std::fs::write(
        &schedule_path,
        serde_json::to_vec_pretty(&schedule.schedule_json())?,
    )?;

    let mut cluster = RandomTraceCluster::start().await?;
    let trace = protocol_trace::start(TraceConfig {
        output: output.clone(),
        run_id: format!("d1-random-{seed}-{mutation:?}"),
        test: "randomized_three_node_fault_trace".to_string(),
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
        faults: schedule.faults.clone(),
    })?;

    let execution = execute_random_schedule(&mut cluster, &schedule).await;
    let trace_result = trace.finish(execution.is_ok());
    cluster.stop_all().await;
    let trace_path = trace_result?;
    execution.with_context(|| {
        format!(
            "randomized D1 trace failed; seed={seed}, trace={}, schedule={}",
            trace_path.display(),
            schedule_path.display()
        )
    })?;
    assert_trace_completeness(&trace_path)?;

    let events = std::fs::read_to_string(&trace_path)?.lines().count();
    println!(
        "D1 randomized trace: seed={seed} mutation={} requests={} operations={} events={} faults={:?} trace={} schedule={}",
        mutation_label(mutation),
        schedule.request_count(),
        schedule.operation_count(),
        events,
        schedule.fault_counts(),
        trace_path.display(),
        schedule_path.display(),
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_and_mixed_bulk_write_protocol_trace() -> Result<()> {
    let trace_dir = tempfile::tempdir()?;
    let output = std::env::var_os("D1_WRITES_TRACE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| trace_dir.path().join("conditional-bulk.jsonl"));
    let mut cluster = RandomTraceCluster::start().await?;
    let trace = protocol_trace::start(TraceConfig {
        output: output.clone(),
        run_id: "d1-conditional-bulk".to_string(),
        test: "conditional_and_mixed_bulk_write_protocol_trace".to_string(),
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
        mutation: MutationMode::None,
        faults: Vec::new(),
    })?;
    let execution = async {
        cluster.record_initial_routing_views()?;
        let mut client = connect(cluster.address("p")?).await?;
        let created = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                doc_id: "a".to_string(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 1}))?,
                create_only: true,
                ..Default::default()
            }))
            .await?
            .into_inner();
        anyhow::ensure!(
            created.success && created.created && created.seq_no == Some(0),
            "{created:?}"
        );
        let wrong_incarnation = client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                doc_id: "a".to_string(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": -1}))?,
                if_seq_no: created.seq_no,
                if_primary_term: created.primary_term,
                index_uuid: Some(format!("{INDEX_UUID}-obsolete")),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        anyhow::ensure!(wrong_incarnation.code() == tonic::Code::NotFound);
        let conditional = ShardDocRequest {
            index_name: INDEX.to_string(),
            shard_id: SHARD,
            doc_id: "a".to_string(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": 2}))?,
            if_seq_no: Some(0),
            if_primary_term: Some(1),
            create_only: false,
            index_uuid: Some(INDEX_UUID.to_string()),
        };
        let updated = client
            .index_doc(tonic::Request::new(conditional.clone()))
            .await?
            .into_inner();
        anyhow::ensure!(
            updated.success && !updated.created && updated.seq_no == Some(1),
            "{updated:?}"
        );
        let conflict = client
            .index_doc(tonic::Request::new(conditional))
            .await
            .unwrap_err();
        anyhow::ensure!(conflict.code() == tonic::Code::AlreadyExists, "{conflict}");
        let document = client
            .get_doc(tonic::Request::new(ShardGetRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                doc_id: "a".to_string(),
                realtime: Some(true),
            }))
            .await?
            .into_inner();
        anyhow::ensure!(
            document.found && document.seq_no == Some(1) && document.primary_term == Some(1)
        );
        anyhow::ensure!(document.index_uuid == INDEX_UUID);
        anyhow::ensure!(
            serde_json::from_slice::<serde_json::Value>(&document.source_json)?["value"] == 2
        );
        let bulk = client
            .bulk_index(tonic::Request::new(ShardBulkRequest {
                index_name: INDEX.to_string(),
                shard_id: SHARD,
                documents_json: vec![
                    serde_json::to_vec(&serde_json::json!({"_doc_id": "a"}))?,
                    serde_json::to_vec(
                        &serde_json::json!({"_doc_id": "a", "_source": {"value": 3}}),
                    )?,
                    serde_json::to_vec(
                        &serde_json::json!({"_doc_id": "a", "_source": {"value": 4}}),
                    )?,
                    serde_json::to_vec(&serde_json::json!({"_doc_id": "b"}))?,
                ],
                operations: [
                    ferrissearch::transport::proto::ShardBulkOpKind::Delete,
                    ferrissearch::transport::proto::ShardBulkOpKind::Create,
                    ferrissearch::transport::proto::ShardBulkOpKind::Create,
                    ferrissearch::transport::proto::ShardBulkOpKind::Delete,
                ]
                .into_iter()
                .map(|kind| ferrissearch::transport::proto::ShardBulkOperation {
                    kind: kind as i32,
                    ..Default::default()
                })
                .collect(),
            }))
            .await?
            .into_inner();
        anyhow::ensure!(bulk.success && bulk.results.len() == 4, "{bulk:?}");
        anyhow::ensure!(bulk.results[0].seq_no == Some(2) && bulk.results[0].result == "deleted");
        anyhow::ensure!(bulk.results[1].seq_no == Some(3) && bulk.results[1].result == "created");
        anyhow::ensure!(bulk.results[2].status == 409 && bulk.results[2].seq_no.is_none());
        anyhow::ensure!(
            bulk.results[3].status == 404
                && bulk.results[3].seq_no == Some(4)
                && bulk.results[3].error.is_empty()
        );
        let snapshots = cluster
            .nodes
            .values()
            .map(|node| {
                capture_final_copy_state(&node.shard_manager)?
                    .context("conditional/bulk trace copy is unavailable")
            })
            .collect::<Result<Vec<_>>>()?;
        for snapshot in snapshots {
            protocol_trace::record_copy_snapshot(snapshot, "trace_end")?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let result = trace.finish(execution.is_ok());
    cluster.stop_all().await;
    let output = result?;
    execution?;
    assert_trace_completeness(&output)?;
    println!("D1 conditional/bulk write trace: {}", output.display());
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
                action: FaultAction::HoldRequestUntilApplied { seq_no: 1 },
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
                    ..Default::default()
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
                ..Default::default()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_noop_retry_emits_every_transport_attempt() -> Result<()> {
    let trace_dir = tempfile::tempdir()?;
    let output = std::env::var_os("D1_TRACE_RETRY_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| trace_dir.path().join("promotion-noop-retry.jsonl"));
    let mut cluster = RandomTraceCluster::start().await?;
    let trace = protocol_trace::start(TraceConfig {
        output,
        run_id: "d1-promotion-noop-retry".to_string(),
        test: "promotion_noop_retry_emits_every_transport_attempt".to_string(),
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
        mutation: MutationMode::None,
        faults: ["q", "r", "r"]
            .into_iter()
            .map(|target| FaultRule {
                target: target.to_string(),
                seq_no: 1,
                action: FaultAction::DropRequest,
            })
            .collect(),
    })?;

    let execution: Result<()> = async {
        cluster.record_initial_routing_views()?;
        let mut client = connect(cluster.address("p")?).await?;
        for value in 0..=2 {
            let _operation = cluster.operation_gate.read().await;
            let response = index_document(&mut client, "retry-doc", value).await?;
            anyhow::ensure!(
                response.seq_no == Some(value as u64) && response.success == (value != 1),
                "unexpected promotion-gap write result: {response:?}"
            );
        }
        drop(client);

        {
            let operation_gate = cluster.operation_gate.clone();
            let _exclusive = operation_gate.write_owned().await;
            let primary = cluster
                .nodes
                .get_mut("p")
                .context("primary node is missing")?
                .running
                .take()
                .context("primary is already stopped")?;
            stop_node(primary).await;
            protocol_trace::record_node_crashed("p", "unclean")?;
            cluster.apply_command(
                ClusterCommand::FailShardCopy {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    node: "p".to_string(),
                    allocation_id: 1,
                    expected_primary_term: 1,
                    promote_only: true,
                    promotion_candidate: Some("q".to_string()),
                },
                "retry probe primary promotion",
            )?;
            let promoted = cluster.authoritative_state();
            cluster.apply_command(
                ClusterCommand::ActivatePrimary {
                    index_name: INDEX.to_string(),
                    index_uuid: INDEX_UUID.to_string(),
                    shard_id: SHARD,
                    primary: "q".to_string(),
                    allocation_id: 1,
                    expected_term: promoted.indices[INDEX].shard_routing[&SHARD].primary_term,
                },
                "retry probe primary activation",
            )?;
            cluster.current_primary = "q".to_string();
        }

        let service = cluster.service("q")?;
        for (expected_pending, expected_checkpoint) in [(true, Some(0)), (false, Some(2))] {
            let _operation = cluster.operation_gate.read().await;
            service
                .protocol_trace_activate_primary_for_test(INDEX, SHARD)
                .await
                .map_err(anyhow::Error::msg)?;
            let has_pending = !service
                .shard_manager
                .isr_tracker
                .expired_gap_observations(Duration::ZERO)
                .is_empty();
            anyhow::ensure!(
                has_pending == expected_pending,
                "promotion NoOp retry did not update the replica gap"
            );
            let replica = cluster
                .node("r")?
                .shard_manager
                .get_shard(INDEX, SHARD)
                .context("retry target is not open")?;
            let sequence = replica.sequence_stats();
            anyhow::ensure!(
                sequence.processed_checkpoint == expected_checkpoint
                    && sequence.persisted_checkpoint == expected_checkpoint,
                "retry target has incorrect checkpoints: {sequence:?}"
            );
        }

        {
            let _exclusive = cluster.operation_gate.write().await;
            let snapshots = ["q", "r"]
                .into_iter()
                .map(|node| {
                    let manager = &cluster.nodes[node].shard_manager;
                    let engine = manager
                        .get_shard(INDEX, SHARD)
                        .context("copy is not open")?;
                    anyhow::ensure!(
                        engine.sequence_stats().processed_checkpoint == Some(2)
                            && engine.sequence_stats().persisted_checkpoint == Some(2),
                        "copy {node} did not durably close the promotion gap"
                    );
                    let snapshot = manager.capture_protocol_trace_copy_state(INDEX, SHARD)?;
                    anyhow::ensure!(
                        engine.get_document("retry-doc")? == Some(serde_json::json!({"value": 2})),
                        "copy {node} lost the acknowledged document"
                    );
                    Ok(snapshot)
                })
                .collect::<Result<Vec<_>>>()?;
            for snapshot in snapshots {
                protocol_trace::record_copy_snapshot(snapshot, "trace_end")?;
            }
        }
        Ok(())
    }
    .await;
    let trace_result = trace.finish(execution.is_ok());
    cluster.stop_all().await;
    let output = trace_result?;
    execution
        .with_context(|| format!("promotion retry probe failed; trace={}", output.display()))?;
    assert_trace_completeness(&output)?;

    let events = std::fs::read_to_string(&output)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let sends = events
        .iter()
        .filter(|event| event["event"] == "promotion_noop_replication_started")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        sends.len() == 2,
        "both promotion transport attempts must be emitted"
    );
    anyhow::ensure!(
        sends[0]["message_id"] != sends[1]["message_id"]
            && sends[0]["receipt_id"] == sends[1]["receipt_id"]
            && sends[0]["batch_id"] == sends[1]["batch_id"],
        "retry must preserve the NoOp identity but allocate a fresh message"
    );
    for (send, expected_outcome) in sends.iter().zip(["dropped", "acknowledged"]) {
        let result = events
            .iter()
            .find(|event| {
                event["event"] == "promotion_noop_result"
                    && event["message_id"] == send["message_id"]
            })
            .context("promotion transport attempt has no result")?;
        anyhow::ensure!(
            result["outcome"] == expected_outcome,
            "incorrect retry result"
        );
    }
    let promotion_appends = events
        .iter()
        .filter(|event| event["event"] == "wal_appended" && event["origin"] == "promotion")
        .count();
    anyhow::ensure!(
        promotion_appends == 1,
        "retry must not invent another local WAL append"
    );
    println!("D1 promotion retry trace: {}", output.display());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_three_node_fault_trace() -> Result<()> {
    let seeds = parse_random_seeds()?;
    let mutation = mutation_mode()?;
    let trace_dir = tempfile::tempdir()?;
    let multiple = seeds.len() > 1;
    for seed in seeds {
        let output = random_trace_output(&trace_dir, seed, mutation, multiple)?;
        run_randomized_seed(seed, mutation, output).await?;
    }
    Ok(())
}
