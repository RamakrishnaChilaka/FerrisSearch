use crate::api::AppState;
use crate::cluster::state::NodeRole;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write;

type ShardCopyDocCounts = HashMap<(String, String, u32), u64>;
type ShardCopySegments = HashMap<(String, String, u32), Vec<crate::engine::SegmentInfo>>;

#[derive(Deserialize, Default)]
pub struct CatParams {
    #[serde(default)]
    pub v: Option<String>,
    /// When present (`?local`), only show doc counts from local shards (no fan-out).
    #[serde(default)]
    pub local: Option<String>,
}

/// Returns true when the `v` (verbose/header) flag is present in the query string.
/// Matches OpenSearch behaviour: `?v`, `?v=`, and `?v=true` all enable headers.
fn wants_headers(params: &CatParams) -> bool {
    params.v.is_some()
}

fn wants_local(params: &CatParams) -> bool {
    params.local.is_some()
}

/// Collect doc counts for all shards across the cluster.
/// Fans out to ALL nodes (including self) via gRPC `GetShardStats` concurrently.
/// This ensures consistent doc counts regardless of which node is queried.
/// Returns a map of (node_id, index_name, shard_id) → doc_count.
async fn collect_shard_doc_counts(state: &AppState) -> ShardCopyDocCounts {
    let mut counts = ShardCopyDocCounts::new();

    let cs = state.cluster_manager.get_state();
    let mut handles = Vec::new();
    for node in cs.nodes.values() {
        let client = state.transport_client.clone();
        let node = node.clone();
        handles.push(tokio::spawn(async move {
            let node_id = node.id.clone();
            (node_id, client.get_shard_stats(&node).await)
        }));
    }
    for handle in handles {
        if let Ok((node_id, Ok(remote_counts))) = handle.await {
            for ((index_name, shard_id), doc_count) in remote_counts {
                counts.insert((node_id.clone(), index_name, shard_id), doc_count);
            }
        }
    }

    counts
}

/// Collect segment lists for all shard copies across the cluster.
/// Fans out to ALL nodes (including self) via gRPC `GetSegmentStats` concurrently.
/// Returns a map of (node_id, index_name, shard_id) -> segment list.
async fn collect_shard_segments(state: &AppState) -> ShardCopySegments {
    let mut segments = ShardCopySegments::new();

    let cs = state.cluster_manager.get_state();
    let mut handles = Vec::new();
    for node in cs.nodes.values() {
        let client = state.transport_client.clone();
        let node = node.clone();
        handles.push(tokio::spawn(async move {
            let node_id = node.id.clone();
            (node_id, client.get_segment_stats(&node).await)
        }));
    }
    for handle in handles {
        if let Ok((node_id, Ok(remote_segments))) = handle.await {
            for (index_name, shard_id, segment_info) in remote_segments {
                segments
                    .entry((node_id.clone(), index_name, shard_id))
                    .or_default()
                    .push(segment_info);
            }
        }
    }

    for shard_segments in segments.values_mut() {
        shard_segments.sort_by(|left, right| left.segment_id.cmp(&right.segment_id));
    }

    segments
}

/// Local-only doc count lookup: returns doc count string or "-" for remote shards.
fn local_doc_count(state: &AppState, node_id: &str, index: &str, shard_id: u32) -> String {
    if node_id != state.local_node_id {
        return "-".into();
    }

    state
        .shard_manager
        .get_shard(index, shard_id)
        .map(|e| e.doc_count().to_string())
        .unwrap_or_else(|| "-".into())
}

fn distributed_doc_count(
    doc_counts: &ShardCopyDocCounts,
    node_id: &str,
    index: &str,
    shard_id: u32,
) -> String {
    doc_counts
        .get(&(node_id.to_string(), index.to_string(), shard_id))
        .map(|count| count.to_string())
        .unwrap_or_else(|| "0".into())
}

fn text_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// GET /_cat/nodes — tabular node listing
pub async fn cat_nodes(State(state): State<AppState>, params: Query<CatParams>) -> Response {
    let cs = state.cluster_manager.get_state();

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(
            out,
            "{:<40} {:<20} {:<15} {:<8} {:<10} {:<6}",
            "id", "name", "host", "http", "transport", "roles"
        )
        .unwrap();
    }

    let mut nodes: Vec<_> = cs.nodes.values().collect();
    nodes.sort_by(|a, b| a.name.cmp(&b.name));

    for n in &nodes {
        let roles: String = n
            .roles
            .iter()
            .map(|r| match r {
                NodeRole::Master => 'm',
                NodeRole::Data => 'd',
                NodeRole::Client => 'c',
            })
            .collect();
        let is_master = cs.master_node.as_ref() == Some(&n.id);
        let roles_display = if is_master {
            format!("{}*", roles)
        } else {
            roles
        };
        writeln!(
            out,
            "{:<40} {:<20} {:<15} {:<8} {:<10} {:<6}",
            n.id, n.name, n.host, n.http_port, n.transport_port, roles_display
        )
        .unwrap();
    }

    text_response(out)
}

/// Determine the display state for a shard assigned to `node_id`.
/// - `UNASSIGNED` — the assigned node doesn't exist in the cluster
/// - `INITIALIZING` — the node exists but the engine isn't open yet (shard not in doc_counts)
/// - `STARTED` — the node exists and the engine is serving docs
fn shard_display_state(
    node_id: &str,
    index: &str,
    shard_id: u32,
    cs: &crate::cluster::state::ClusterState,
    doc_counts: &Option<ShardCopyDocCounts>,
    state: &AppState,
) -> &'static str {
    if !cs.nodes.contains_key(node_id) {
        return "UNASSIGNED";
    }
    match doc_counts {
        Some(m) => {
            if m.contains_key(&(node_id.to_string(), index.to_string(), shard_id)) {
                "STARTED"
            } else {
                "INITIALIZING"
            }
        }
        None => {
            // Local-only mode
            if node_id == state.local_node_id {
                if state.shard_manager.get_shard(index, shard_id).is_some() {
                    "STARTED"
                } else {
                    "INITIALIZING"
                }
            } else {
                "STARTED" // remote shard, can't determine from local-only view
            }
        }
    }
}

/// GET /_cat/shards — tabular shard listing
/// By default, fans out to all nodes to collect real doc counts.
/// Pass `?local` to only show counts for shards on this node.
pub async fn cat_shards(State(state): State<AppState>, params: Query<CatParams>) -> Response {
    let cs = state.cluster_manager.get_state();
    let local_only = wants_local(&params);
    let doc_counts = if local_only {
        None
    } else {
        Some(collect_shard_doc_counts(&state).await)
    };

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(
            out,
            "{:<25} {:<8} {:<10} {:<14} {:<10} {:<40}",
            "index", "shard", "prirep", "state", "docs", "node"
        )
        .unwrap();
    }

    let mut index_names: Vec<&String> = cs.indices.keys().collect();
    index_names.sort();

    for idx_name in index_names {
        let meta = &cs.indices[idx_name];
        let mut shard_ids: Vec<u32> = meta.shard_routing.keys().copied().collect();
        shard_ids.sort();

        for shard_id in shard_ids {
            let routing = &meta.shard_routing[&shard_id];
            let node_name = cs
                .nodes
                .get(&routing.primary)
                .map(|n| n.name.as_str())
                .unwrap_or("UNASSIGNED");
            let primary_state = shard_display_state(
                &routing.primary,
                idx_name,
                shard_id,
                &cs,
                &doc_counts,
                &state,
            );

            let docs = match &doc_counts {
                Some(m) => distributed_doc_count(m, &routing.primary, idx_name, shard_id),
                None => local_doc_count(&state, &routing.primary, idx_name, shard_id),
            };

            writeln!(
                out,
                "{:<25} {:<8} {:<10} {:<14} {:<10} {:<40}",
                idx_name, shard_id, "p", primary_state, docs, node_name
            )
            .unwrap();

            // List assigned replica shards
            for replica_node_id in &routing.replicas {
                let replica_name = cs
                    .nodes
                    .get(replica_node_id)
                    .map(|n| n.name.as_str())
                    .unwrap_or("UNASSIGNED");
                let replica_state = shard_display_state(
                    replica_node_id,
                    idx_name,
                    shard_id,
                    &cs,
                    &doc_counts,
                    &state,
                );
                let replica_docs = match &doc_counts {
                    Some(m) => distributed_doc_count(m, replica_node_id, idx_name, shard_id),
                    None => local_doc_count(&state, replica_node_id, idx_name, shard_id),
                };
                writeln!(
                    out,
                    "{:<25} {:<8} {:<10} {:<14} {:<10} {:<40}",
                    idx_name, shard_id, "r", replica_state, replica_docs, replica_name
                )
                .unwrap();
            }

            // List unassigned replica shards (couldn't be placed)
            for _ in 0..routing.unassigned_replicas {
                writeln!(
                    out,
                    "{:<25} {:<8} {:<10} {:<14} {:<10} {:<40}",
                    idx_name, shard_id, "r", "UNASSIGNED", "-", ""
                )
                .unwrap();
            }
        }
    }

    text_response(out)
}

/// GET /_cat/indices — tabular index listing
/// By default, fans out to all nodes to collect real doc counts.
/// Pass `?local` to only sum counts from local shards.
pub async fn cat_indices(State(state): State<AppState>, params: Query<CatParams>) -> Response {
    let cs = state.cluster_manager.get_state();
    let local_only = wants_local(&params);
    let doc_counts = if local_only {
        None
    } else {
        Some(collect_shard_doc_counts(&state).await)
    };

    let health_fn = |idx_name: &str| -> &'static str {
        let meta = match cs.indices.get(idx_name) {
            Some(m) => m,
            None => return "red",
        };
        let data_node_ids: std::collections::HashSet<&String> = cs
            .nodes
            .values()
            .filter(|n| n.roles.contains(&NodeRole::Data))
            .map(|n| &n.id)
            .collect();
        for routing in meta.shard_routing.values() {
            if !data_node_ids.contains(&routing.primary) {
                return "yellow";
            }
            for replica in &routing.replicas {
                if !data_node_ids.contains(replica) {
                    return "yellow";
                }
            }
        }
        "green"
    };

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(
            out,
            "{:<8} {:<25} {:<8} {:<10} {:<10}",
            "health", "index", "shards", "docs", "status"
        )
        .unwrap();
    }

    let mut index_names: Vec<&String> = cs.indices.keys().collect();
    index_names.sort();

    for idx_name in index_names {
        let meta = &cs.indices[idx_name];
        let health = health_fn(idx_name);

        let total_docs: u64 = match &doc_counts {
            Some(m) => (0..meta.number_of_shards)
                .map(|sid| {
                    meta.shard_routing
                        .get(&sid)
                        .and_then(|routing| {
                            m.get(&(routing.primary.clone(), idx_name.clone(), sid))
                                .copied()
                        })
                        .unwrap_or(0)
                })
                .sum(),
            None => (0..meta.number_of_shards)
                .map(|sid| {
                    state
                        .shard_manager
                        .get_shard(idx_name, sid)
                        .map(|e| e.doc_count())
                        .unwrap_or(0)
                })
                .sum(),
        };

        writeln!(
            out,
            "{:<8} {:<25} {:<8} {:<10} {:<10}",
            health, idx_name, meta.number_of_shards, total_docs, "open"
        )
        .unwrap();
    }

    text_response(out)
}

/// GET /_cat/master — show the current master node
pub async fn cat_master(State(state): State<AppState>, params: Query<CatParams>) -> Response {
    let cs = state.cluster_manager.get_state();

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(
            out,
            "{:<40} {:<20} {:<15} {:<6}",
            "id", "host", "ip", "node"
        )
        .unwrap();
    }

    if let Some(master_id) = &cs.master_node {
        if let Some(master) = cs.nodes.get(master_id) {
            writeln!(
                out,
                "{:<40} {:<20} {:<15} {:<6}",
                master.id, master.host, master.host, master.name
            )
            .unwrap();
        } else {
            writeln!(out, "{:<40} {:<20} {:<15} {:<6}", master_id, "-", "-", "-").unwrap();
        }
    }

    text_response(out)
}

/// GET /_cat/segments — tabular listing of per-shard segments.
/// By default, fans out to all nodes and lists segments for every started shard
/// copy in the cluster. Pass `?local` to show only segments from this node.
pub async fn cat_segments(State(state): State<AppState>, params: Query<CatParams>) -> Response {
    let cs = state.cluster_manager.get_state();
    let local_only = wants_local(&params);
    let shard_segments = if local_only {
        None
    } else {
        Some(collect_shard_segments(&state).await)
    };

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(
            out,
            "{:<25} {:<8} {:<36} {:<12} {:<12}",
            "index", "shard", "segment", "num_docs", "deleted_docs"
        )
        .unwrap();
    }

    let mut index_names: Vec<&String> = cs.indices.keys().collect();
    index_names.sort();

    for idx_name in index_names {
        let meta = &cs.indices[idx_name];
        let mut shard_ids: Vec<u32> = meta.shard_routing.keys().copied().collect();
        shard_ids.sort();

        for shard_id in shard_ids {
            let routing = &meta.shard_routing[&shard_id];
            let mut copy_node_ids = Vec::with_capacity(1 + routing.replicas.len());
            copy_node_ids.push(routing.primary.clone());
            copy_node_ids.extend(routing.replicas.iter().cloned());

            for node_id in copy_node_ids {
                if local_only {
                    if node_id != state.local_node_id {
                        continue;
                    }

                    let Some(engine) = state.shard_manager.get_shard(idx_name, shard_id) else {
                        continue;
                    };

                    for seg in engine.segment_infos() {
                        writeln!(
                            out,
                            "{:<25} {:<8} {:<36} {:<12} {:<12}",
                            idx_name, shard_id, seg.segment_id, seg.num_docs, seg.deleted_docs
                        )
                        .unwrap();
                    }
                    continue;
                }

                let Some(segments) = shard_segments.as_ref().and_then(|segments| {
                    segments.get(&(node_id.clone(), idx_name.clone(), shard_id))
                }) else {
                    continue;
                };

                for seg in segments {
                    writeln!(
                        out,
                        "{:<25} {:<8} {:<36} {:<12} {:<12}",
                        idx_name, shard_id, seg.segment_id, seg.num_docs, seg.deleted_docs
                    )
                    .unwrap();
                }
            }
        }
    }

    text_response(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::manager::ClusterManager;
    use crate::cluster::state::{
        IndexMetadata, IndexSettings, IndexUuid, NodeInfo, NodeRole, ShardRoutingEntry,
    };
    use crate::shard::ShardManager;
    use crate::transport::TransportClient;
    use crate::worker::WorkerPools;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    async fn make_app_state(local_node_id: &str) -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, "test-cluster".into())
                .await
                .unwrap();
        crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
            .await
            .unwrap();
        for _ in 0..50 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let manager = ClusterManager::with_shared_state(shared_state);
        (
            dir,
            AppState {
                cluster_manager: Arc::new(manager),
                shard_manager,
                transport_client: TransportClient::new(),
                local_node_id: local_node_id.to_string(),
                raft,
                worker_pools: WorkerPools::default_for_system(),
                task_manager: Arc::new(crate::tasks::TaskManager::new()),
                sql_group_by_scan_limit: 0,
                sql_approximate_top_k: false,
            },
        )
    }

    fn make_cluster_state() -> crate::cluster::state::ClusterState {
        let mut state = crate::cluster::state::ClusterState::new("test-cluster".into());
        state.add_node(NodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 1,
        });
        state.add_node(NodeInfo {
            id: "node-2".into(),
            name: "node-2".into(),
            host: "127.0.0.1".into(),
            transport_port: 9301,
            http_port: 9201,
            roles: vec![NodeRole::Data],
            raft_node_id: 2,
        });

        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        );

        state.add_index(IndexMetadata {
            name: "idx".into(),
            uuid: IndexUuid::new("idx-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing,
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });

        state
    }

    #[test]
    fn wants_headers_returns_true_when_v_present() {
        let params = CatParams {
            v: Some(String::new()),
            local: None,
        };
        assert!(wants_headers(&params));
    }

    #[test]
    fn wants_headers_returns_false_when_v_absent() {
        let params = CatParams {
            v: None,
            local: None,
        };
        assert!(!wants_headers(&params));
    }

    #[test]
    fn wants_local_returns_true_when_local_present() {
        let params = CatParams {
            v: None,
            local: Some(String::new()),
        };
        assert!(wants_local(&params));
    }

    #[test]
    fn wants_local_returns_false_when_local_absent() {
        let params = CatParams {
            v: None,
            local: None,
        };
        assert!(!wants_local(&params));
    }

    #[test]
    fn cat_params_default_has_no_local() {
        let params = CatParams::default();
        assert!(params.v.is_none());
        assert!(params.local.is_none());
    }

    #[tokio::test]
    async fn shard_display_state_requires_report_from_assigned_node() {
        let (_dir, app_state) = make_app_state("node-1").await;
        let cluster_state = make_cluster_state();
        let mut doc_counts = ShardCopyDocCounts::new();
        doc_counts.insert(("node-1".into(), "idx".into(), 0), 12);

        assert_eq!(
            shard_display_state(
                "node-1",
                "idx",
                0,
                &cluster_state,
                &Some(doc_counts.clone()),
                &app_state,
            ),
            "STARTED"
        );
        assert_eq!(
            shard_display_state(
                "node-2",
                "idx",
                0,
                &cluster_state,
                &Some(doc_counts),
                &app_state,
            ),
            "INITIALIZING"
        );
    }

    #[tokio::test]
    async fn local_doc_count_returns_dash_for_remote_rows() {
        let (_dir, app_state) = make_app_state("node-1").await;
        app_state.shard_manager.open_shard("idx", 0).unwrap();

        assert_eq!(local_doc_count(&app_state, "node-1", "idx", 0), "0");
        assert_eq!(local_doc_count(&app_state, "node-2", "idx", 0), "-");
    }

    #[tokio::test]
    async fn cat_segments_local_only_skips_remote_shards() {
        let (_dir, app_state) = make_app_state("node-1").await;
        let mut cluster_state = make_cluster_state();
        cluster_state.add_index(IndexMetadata {
            name: "segments".into(),
            uuid: IndexUuid::new("segments-uuid"),
            number_of_shards: 2,
            number_of_replicas: 0,
            shard_routing: HashMap::from([
                (
                    0,
                    ShardRoutingEntry {
                        primary: "node-1".into(),
                        replicas: vec![],
                        unassigned_replicas: 0,
                    },
                ),
                (
                    1,
                    ShardRoutingEntry {
                        primary: "node-2".into(),
                        replicas: vec![],
                        unassigned_replicas: 0,
                    },
                ),
            ]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        app_state.cluster_manager.update_state(cluster_state);

        app_state.shard_manager.open_shard("segments", 0).unwrap();
        let engine = app_state
            .shard_manager
            .get_shard("segments", 0)
            .expect("local shard should be open");
        engine
            .add_document("doc-1", serde_json::json!({"title": "local segment"}))
            .unwrap();
        engine.refresh().unwrap();

        let response = cat_segments(
            State(app_state),
            Query(CatParams {
                v: Some(String::new()),
                local: Some(String::new()),
            }),
        )
        .await;

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);

        let fields: Vec<&str> = lines[1].split_whitespace().collect();
        assert_eq!(fields[0], "segments");
        assert_eq!(fields[1], "0");
    }
}
