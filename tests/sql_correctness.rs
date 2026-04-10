//! SQL correctness tests using sqllogictest.
//!
//! These tests verify that FerrisSearch's SQL layer produces correct results,
//! not just correct execution modes. Uses the industry-standard `.slt` format
//! (same approach as DataFusion, CockroachDB, DuckDB, RisingWave).

use ferrissearch::api::AppState;
use ferrissearch::cluster::state::*;
use ferrissearch::worker::WorkerPools;
use serde_json::json;
use sqllogictest::{DBOutput, DefaultColumnType};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// FerrisSearch adapter for the sqllogictest `DB` trait.
struct FerrisDB {
    state: AppState,
    index_name: String,
    _tmp: tempfile::TempDir,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
#[error("{0}")]
struct FerrisError(String);

#[derive(Clone, Copy)]
struct SampleStory {
    title: &'static str,
    author: &'static str,
    upvotes: i64,
    comments: i64,
    category: &'static str,
}

fn sample_story_docs() -> [SampleStory; 10] {
    [
        SampleStory {
            title: "Show HN: Rust search engine",
            author: "alice",
            upvotes: 250,
            comments: 45,
            category: "rust",
        },
        SampleStory {
            title: "Rust vs Go performance",
            author: "alice",
            upvotes: 180,
            comments: 92,
            category: "rust",
        },
        SampleStory {
            title: "Why Rust is great for systems",
            author: "alice",
            upvotes: 120,
            comments: 30,
            category: "rust",
        },
        SampleStory {
            title: "Python 4.0 released",
            author: "bob",
            upvotes: 500,
            comments: 200,
            category: "python",
        },
        SampleStory {
            title: "Machine learning with Python",
            author: "bob",
            upvotes: 300,
            comments: 150,
            category: "python",
        },
        SampleStory {
            title: "Deep learning frameworks compared",
            author: "carol",
            upvotes: 400,
            comments: 80,
            category: "ml",
        },
        SampleStory {
            title: "GPT-5 announcement",
            author: "carol",
            upvotes: 1000,
            comments: 500,
            category: "ml",
        },
        SampleStory {
            title: "Startup funding trends 2026",
            author: "dave",
            upvotes: 50,
            comments: 20,
            category: "startup",
        },
        SampleStory {
            title: "Building a startup in Rust",
            author: "dave",
            upvotes: 150,
            comments: 35,
            category: "rust",
        },
        SampleStory {
            title: "Remote work is dead",
            author: "eve",
            upvotes: 800,
            comments: 600,
            category: "culture",
        },
    ]
}

impl FerrisDB {
    fn new_with_hackernews_sample() -> Self {
        let index_name = "stories";

        let handle = tokio::runtime::Handle::current();

        // Build cluster state
        let mut cluster_state = ClusterState::new("slt-cluster".into());
        cluster_state.add_node(NodeInfo {
            id: "node-1".into(),
            name: "node-1".into(),
            host: "127.0.0.1".into(),
            transport_port: 19300,
            http_port: 19200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: 0,
        });

        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );

        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "author".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "upvotes".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "comments".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        cluster_state.add_index(IndexMetadata {
            name: index_name.to_string(),
            uuid: ferrissearch::cluster::state::IndexUuid::new(format!("{}-uuid", index_name)),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        });

        let tmp = tempfile::tempdir().unwrap();
        let (raft, shared_state) = tokio::task::block_in_place(|| {
            handle.block_on(async {
                let (r, s) = ferrissearch::consensus::create_raft_instance_mem(
                    1,
                    cluster_state.cluster_name.clone(),
                )
                .await
                .unwrap();
                ferrissearch::consensus::bootstrap_single_node(&r, 1, "127.0.0.1:19300".into())
                    .await
                    .unwrap();
                for _ in 0..50 {
                    if r.current_leader().await.is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                (r, s)
            })
        });
        let manager = ferrissearch::cluster::ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state.clone());
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(ferrissearch::shard::ShardManager::new(
                tmp.path(),
                Duration::from_secs(60),
            )),
            transport_client: ferrissearch::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft,
            worker_pools: WorkerPools::new(2, 2),
            task_manager: Arc::new(ferrissearch::tasks::TaskManager::new()),
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
        };

        // Open the shard (CompositeEngine spawns refresh loop on current runtime)
        let metadata = cluster_state.indices.get(index_name).unwrap();
        state
            .shard_manager
            .open_shard_with_settings(
                index_name,
                0,
                &metadata.mappings,
                &metadata.settings,
                &metadata.uuid,
            )
            .expect("open shard");

        let shard = state.shard_manager.get_shard(index_name, 0).unwrap();

        // Sample data: 10 HN-style posts
        for (i, doc) in sample_story_docs().iter().enumerate() {
            shard
                .add_document(
                    &format!("doc-{}", i + 1),
                    json!({
                        "title": doc.title,
                        "author": doc.author,
                        "upvotes": doc.upvotes,
                        "comments": doc.comments,
                        "category": doc.category,
                    }),
                )
                .unwrap();
        }
        shard.refresh().unwrap();

        FerrisDB {
            state,
            index_name: index_name.to_string(),
            _tmp: tmp,
        }
    }
}

impl sqllogictest::DB for FerrisDB {
    type Error = FerrisError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let state = self.state.clone();
        let index = self.index_name.clone();
        let sql = sql.to_string();

        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::block_in_place(|| {
            handle.block_on(async {
                ferrissearch::api::search::execute_sql_for_testing(&state, &index, &sql).await
            })
        });

        match result {
            Ok(query_result) => {
                let types: Vec<DefaultColumnType> = query_result
                    .columns
                    .iter()
                    .map(|_| DefaultColumnType::Any)
                    .collect();

                let rows: Vec<Vec<String>> = query_result
                    .rows
                    .iter()
                    .map(|row| {
                        query_result
                            .columns
                            .iter()
                            .map(|col| match row.get(col) {
                                Some(serde_json::Value::Null) => "NULL".to_string(),
                                Some(serde_json::Value::String(s)) => s.clone(),
                                Some(serde_json::Value::Number(n)) => {
                                    if let Some(i) = n.as_i64() {
                                        i.to_string()
                                    } else {
                                        format!("{:.1}", n.as_f64().unwrap_or(0.0))
                                    }
                                }
                                Some(serde_json::Value::Bool(b)) => b.to_string(),
                                Some(v) => v.to_string(),
                                None => "NULL".to_string(),
                            })
                            .collect()
                    })
                    .collect();

                Ok(DBOutput::Rows { types, rows })
            }
            Err(e) => Err(FerrisError(e.to_string())),
        }
    }
}

fn json_number(row: &serde_json::Value, column: &str) -> f64 {
    row.get(column)
        .and_then(|value| value.as_f64().or_else(|| value.as_i64().map(|n| n as f64)))
        .unwrap_or_else(|| panic!("expected numeric column {} in row {:?}", column, row))
}

fn expected_average_by_author<F>(docs: &[SampleStory], value_fn: F) -> BTreeMap<&'static str, f64>
where
    F: Fn(&SampleStory) -> f64,
{
    let mut totals: BTreeMap<&'static str, (f64, usize)> = BTreeMap::new();
    for doc in docs {
        let entry = totals.entry(doc.author).or_insert((0.0, 0));
        entry.0 += value_fn(doc);
        entry.1 += 1;
    }

    totals
        .into_iter()
        .map(|(author, (sum, count))| (author, sum / count as f64))
        .collect()
}

fn assert_grouped_numeric_results(
    rows: &[serde_json::Value],
    key_column: &str,
    value_column: &str,
    expected_by_key: &BTreeMap<&'static str, f64>,
) {
    assert_eq!(rows.len(), expected_by_key.len());
    for row in rows {
        let key = row
            .get(key_column)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("{} column should be present", key_column));
        let value = json_number(row, value_column);
        let expected = expected_by_key
            .get(key)
            .unwrap_or_else(|| panic!("unexpected key {} in grouped rows", key));
        assert!(
            (value - expected).abs() < 1e-9,
            "{} {}: got {}, expected {}",
            key_column,
            key,
            value,
            expected
        );
    }
}

async fn assert_exact_expression_numeric_results() {
    let db = FerrisDB::new_with_hackernews_sample();
    let docs = sample_story_docs();

    let overall = ferrissearch::api::search::execute_sql_for_testing(
        &db.state,
        &db.index_name,
        "SELECT AVG(CAST(upvotes AS FLOAT) / comments) AS avg_ratio, SUM(upvotes) * 1.0 / SUM(comments) AS weighted_ratio FROM stories WHERE comments > 0",
    )
    .await
    .expect("overall ratio query should succeed");

    assert_eq!(overall.rows.len(), 1);
    let overall_row = &overall.rows[0];
    let avg_ratio = json_number(overall_row, "avg_ratio");
    let weighted_ratio = json_number(overall_row, "weighted_ratio");

    let expected_avg_ratio = docs
        .iter()
        .map(|doc| doc.upvotes as f64 / doc.comments as f64)
        .sum::<f64>()
        / docs.len() as f64;
    let expected_weighted_ratio = docs.iter().map(|doc| doc.upvotes as f64).sum::<f64>()
        / docs.iter().map(|doc| doc.comments as f64).sum::<f64>();

    assert!((avg_ratio - expected_avg_ratio).abs() < 1e-9);
    assert!((weighted_ratio - expected_weighted_ratio).abs() < 1e-9);

    let grouped_ratio = ferrissearch::api::search::execute_sql_for_testing(
        &db.state,
        &db.index_name,
        "SELECT author, AVG(CAST(upvotes AS FLOAT) / comments) AS avg_ratio FROM stories WHERE comments > 0 GROUP BY author ORDER BY author",
    )
    .await
    .expect("grouped ratio query should succeed");

    let expected_ratio_by_author =
        expected_average_by_author(&docs, |doc| doc.upvotes as f64 / doc.comments as f64);
    assert_grouped_numeric_results(
        &grouped_ratio.rows,
        "author",
        "avg_ratio",
        &expected_ratio_by_author,
    );

    let grouped = ferrissearch::api::search::execute_sql_for_testing(
        &db.state,
        &db.index_name,
        "SELECT author, AVG((CAST(upvotes AS FLOAT) - comments) / upvotes) AS margin FROM stories WHERE comments > 0 GROUP BY author ORDER BY author",
    )
    .await
    .expect("grouped nested expression query should succeed");

    let expected_margin_by_author = expected_average_by_author(&docs, |doc| {
        (doc.upvotes as f64 - doc.comments as f64) / doc.upvotes as f64
    });
    assert_grouped_numeric_results(
        &grouped.rows,
        "author",
        "margin",
        &expected_margin_by_author,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_correctness_basic() {
    let slt_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    for entry in std::fs::read_dir(&slt_dir).expect("tests/slt directory") {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "slt") {
            let mut tester =
                sqllogictest::Runner::new(|| async { Ok(FerrisDB::new_with_hackernews_sample()) });
            tester
                .run_file_async(&path)
                .await
                .unwrap_or_else(|e| panic!("SLT test {} failed: {}", path.display(), e));
        }
    }

    // The SLT file rounds user-visible arithmetic results explicitly in SQL.
    // Keep exact-value regressions here so grouped arithmetic semantics do not
    // drift behind that presentation layer.
    assert_exact_expression_numeric_results().await;
}
