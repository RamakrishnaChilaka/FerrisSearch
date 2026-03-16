use crate::api::AppState;
use crate::cluster::state::NodeRole;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::fmt::Write;

#[derive(Deserialize, Default)]
pub struct CatParams {
    #[serde(default)]
    pub v: Option<String>,
}

/// Returns true when the `v` (verbose/header) flag is present in the query string.
/// Matches OpenSearch behaviour: `?v`, `?v=`, and `?v=true` all enable headers.
fn wants_headers(params: &CatParams) -> bool {
    params.v.is_some()
}

fn text_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// GET /_cat/nodes — tabular node listing
pub async fn cat_nodes(
    State(state): State<AppState>,
    params: Query<CatParams>,
) -> Response {
    let cs = state.cluster_manager.get_state();

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(out, "{:<40} {:<20} {:<15} {:<8} {:<10} {:<6}",
            "id", "name", "host", "http", "transport", "roles"
        ).unwrap();
    }

    let mut nodes: Vec<_> = cs.nodes.values().collect();
    nodes.sort_by(|a, b| a.name.cmp(&b.name));

    for n in &nodes {
        let roles: String = n.roles.iter().map(|r| match r {
            NodeRole::Master => 'm',
            NodeRole::Data   => 'd',
            NodeRole::Client => 'c',
        }).collect();
        let is_master = cs.master_node.as_ref() == Some(&n.id);
        let roles_display = if is_master {
            format!("{}*", roles)
        } else {
            roles
        };
        writeln!(out, "{:<40} {:<20} {:<15} {:<8} {:<10} {:<6}",
            n.id, n.name, n.host, n.http_port, n.transport_port, roles_display
        ).unwrap();
    }

    text_response(out)
}

/// GET /_cat/shards — tabular shard listing
pub async fn cat_shards(
    State(state): State<AppState>,
    params: Query<CatParams>,
) -> Response {
    let cs = state.cluster_manager.get_state();

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(out, "{:<25} {:<8} {:<10} {:<10} {:<40}",
            "index", "shard", "prirep", "docs", "node"
        ).unwrap();
    }

    let mut index_names: Vec<&String> = cs.indices.keys().collect();
    index_names.sort();

    for idx_name in index_names {
        let meta = &cs.indices[idx_name];
        let mut shard_ids: Vec<u32> = meta.shards.keys().copied().collect();
        shard_ids.sort();

        for shard_id in shard_ids {
            let node_id = &meta.shards[&shard_id];
            let node_name = cs.nodes.get(node_id)
                .map(|n| n.name.as_str())
                .unwrap_or("UNASSIGNED");

            // If this shard is local, grab doc count from the engine
            let docs = state.shard_manager
                .get_shard(idx_name, shard_id)
                .map(|e| e.doc_count().to_string())
                .unwrap_or_else(|| "-".into());

            writeln!(out, "{:<25} {:<8} {:<10} {:<10} {:<40}",
                idx_name, shard_id, "p", docs, node_name
            ).unwrap();
        }
    }

    text_response(out)
}

/// GET /_cat/indices — tabular index listing
pub async fn cat_indices(
    State(state): State<AppState>,
    params: Query<CatParams>,
) -> Response {
    let cs = state.cluster_manager.get_state();
    let health_fn = |idx_name: &str| -> &'static str {
        let meta = match cs.indices.get(idx_name) {
            Some(m) => m,
            None => return "red",
        };
        let data_node_ids: std::collections::HashSet<&String> = cs.nodes.values()
            .filter(|n| n.roles.contains(&NodeRole::Data))
            .map(|n| &n.id)
            .collect();
        for node_id in meta.shards.values() {
            if !data_node_ids.contains(node_id) {
                return "yellow";
            }
        }
        "green"
    };

    let mut out = String::new();
    if wants_headers(&params) {
        writeln!(out, "{:<8} {:<25} {:<8} {:<10} {:<10}",
            "health", "index", "shards", "docs", "status"
        ).unwrap();
    }

    let mut index_names: Vec<&String> = cs.indices.keys().collect();
    index_names.sort();

    for idx_name in index_names {
        let meta = &cs.indices[idx_name];
        let health = health_fn(idx_name);

        // Sum doc counts from local shards; remote shards show as 0 here
        let total_docs: u64 = (0..meta.number_of_shards)
            .map(|sid| {
                state.shard_manager
                    .get_shard(idx_name, sid)
                    .map(|e| e.doc_count())
                    .unwrap_or(0)
            })
            .sum();

        writeln!(out, "{:<8} {:<25} {:<8} {:<10} {:<10}",
            health, idx_name, meta.number_of_shards, total_docs, "open"
        ).unwrap();
    }

    text_response(out)
}
