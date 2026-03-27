use crate::api::AppState;
use crate::cluster::state::NodeRole;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write;

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
/// Fans out to remote nodes via gRPC `GetShardStats` concurrently.
/// Local shards are read directly from the ShardManager.
/// Returns a map of (index_name, shard_id) → doc_count.
async fn collect_shard_doc_counts(state: &AppState) -> HashMap<(String, u32), u64> {
    let mut counts: HashMap<(String, u32), u64> = HashMap::new();

    // Local shards
    for (key, engine) in state.shard_manager.all_shards() {
        counts.insert((key.index.clone(), key.shard_id), engine.doc_count());
    }

    // Remote nodes — fan out concurrently
    let cs = state.cluster_manager.get_state();
    let mut handles = Vec::new();
    for node in cs.nodes.values() {
        if node.id == state.local_node_id {
            continue;
        }
        let client = state.transport_client.clone();
        let node = node.clone();
        handles.push(tokio::spawn(
            async move { client.get_shard_stats(&node).await },
        ));
    }
    for handle in handles {
        if let Ok(Ok(remote_counts)) = handle.await {
            counts.extend(remote_counts);
        }
    }

    counts
}

/// Local-only doc count lookup: returns doc count string or "-" for remote shards.
fn local_doc_count(state: &AppState, index: &str, shard_id: u32) -> String {
    state
        .shard_manager
        .get_shard(index, shard_id)
        .map(|e| e.doc_count().to_string())
        .unwrap_or_else(|| "-".into())
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
    doc_counts: &Option<HashMap<(String, u32), u64>>,
    state: &AppState,
) -> &'static str {
    if !cs.nodes.contains_key(node_id) {
        return "UNASSIGNED";
    }
    match doc_counts {
        Some(m) => {
            if m.contains_key(&(index.to_string(), shard_id)) {
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
                Some(m) => m
                    .get(&(idx_name.clone(), shard_id))
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "0".into()),
                None => local_doc_count(&state, idx_name, shard_id),
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
                    Some(m) => m
                        .get(&(idx_name.clone(), shard_id))
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "0".into()),
                    None => local_doc_count(&state, idx_name, shard_id),
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
                .map(|sid| m.get(&(idx_name.clone(), sid)).copied().unwrap_or(0))
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
