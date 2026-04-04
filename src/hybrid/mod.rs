pub mod arrow_bridge;
pub mod column_store;
pub mod datafusion_exec;
pub mod merge;
pub mod planner;

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

pub use planner::QueryPlan;

/// Per-stage wall-clock timings for SQL execution, in fractional milliseconds.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SqlTimings {
    pub planning_ms: f64,
    pub search_ms: f64,
    pub collect_ms: f64,
    pub merge_ms: f64,
    pub datafusion_ms: f64,
    pub total_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
}

pub async fn execute_planned_sql(plan: &QueryPlan, hits: &[Value]) -> Result<SqlQueryResult> {
    execute_planned_sql_with_mappings(plan, hits, &std::collections::HashMap::new()).await
}

/// Execute SQL over materialized hits with schema-derived type hints.
/// Ensures integer fields become Int64 and float fields become Float64
/// regardless of the JSON number representation.
pub async fn execute_planned_sql_with_mappings(
    plan: &QueryPlan,
    hits: &[Value],
    mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<SqlQueryResult> {
    let column_store = column_store::ColumnStore::from_hits(hits, &plan.required_columns);
    let type_hints: std::collections::HashMap<String, arrow_bridge::ColumnKind> = mappings
        .iter()
        .map(|(name, fm)| {
            (
                name.clone(),
                arrow_bridge::column_kind_from_field_type(&fm.field_type),
            )
        })
        .collect();
    let batch = arrow_bridge::build_record_batch_with_hints(&column_store, &type_hints)?;
    datafusion_exec::execute_sql(plan, batch).await
}

pub async fn execute_planned_sql_batches(
    plan: &QueryPlan,
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> Result<SqlQueryResult> {
    datafusion_exec::execute_sql_batches(plan, batches).await
}

pub async fn execute_planned_sql_batches_stream(
    plan: &QueryPlan,
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> Result<(
    Vec<String>,
    datafusion::physical_plan::SendableRecordBatchStream,
)> {
    datafusion_exec::execute_sql_batches_stream(plan, batches).await
}

pub async fn execute_planned_sql_partition_streams(
    plan: &QueryPlan,
    schema: std::sync::Arc<datafusion::arrow::datatypes::Schema>,
    partitions: Vec<std::sync::Arc<dyn datafusion::physical_plan::streaming::PartitionStream>>,
) -> Result<(
    Vec<String>,
    datafusion::physical_plan::SendableRecordBatchStream,
)> {
    datafusion_exec::execute_sql_partition_streams(plan, schema, partitions).await
}

pub fn direct_sql_input_schema(
    plan: &QueryPlan,
    mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<std::sync::Arc<datafusion::arrow::datatypes::Schema>> {
    datafusion_exec::direct_sql_input_schema(plan, mappings)
}

pub fn normalize_sql_batch_for_schema(
    batch: datafusion::arrow::record_batch::RecordBatch,
    plan: &QueryPlan,
    target_schema: &std::sync::Arc<datafusion::arrow::datatypes::Schema>,
) -> Result<datafusion::arrow::record_batch::RecordBatch> {
    datafusion_exec::normalize_sql_batch_for_schema(batch, plan, target_schema)
}

pub fn one_shot_partition_stream(
    schema: std::sync::Arc<datafusion::arrow::datatypes::Schema>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> std::sync::Arc<dyn datafusion::physical_plan::streaming::PartitionStream> {
    datafusion_exec::one_shot_partition_stream(schema, stream)
}

pub fn execute_grouped_partial_sql(
    plan: &QueryPlan,
    partials: &[std::collections::HashMap<String, crate::search::PartialAggResult>],
) -> Result<SqlQueryResult> {
    let grouped_sql = plan
        .grouped_sql
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("query plan is not eligible for grouped partial SQL"))?;

    let params = crate::search::GroupedMetricsAggParams {
        group_by: grouped_sql
            .group_columns
            .iter()
            .map(|column| column.source_name.clone())
            .collect(),
        metrics: grouped_sql
            .metrics
            .iter()
            .map(|metric| crate::search::GroupedMetricAgg {
                output_name: metric.output_name.clone(),
                function: metric.function.to_search_function(),
                field: metric.field.clone(),
            })
            .collect(),
    };

    let merged = crate::search::merge_grouped_metrics_partials(
        partials,
        crate::hybrid::planner::INTERNAL_SQL_GROUPED_AGG,
        &params,
    );

    let columns: Vec<String> = grouped_sql
        .group_columns
        .iter()
        .map(|column| column.output_name.clone())
        .chain(
            grouped_sql
                .metrics
                .iter()
                .map(|metric| metric.output_name.clone()),
        )
        .collect();

    let mut rows = merged
        .into_iter()
        .map(|bucket| {
            let mut row = serde_json::Map::new();
            for (index, column) in grouped_sql.group_columns.iter().enumerate() {
                row.insert(
                    column.output_name.clone(),
                    bucket
                        .group_values
                        .get(index)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                );
            }

            for metric in &grouped_sql.metrics {
                row.insert(
                    metric.output_name.clone(),
                    grouped_metric_value(bucket.metrics.get(&metric.output_name), metric),
                );
            }

            serde_json::Value::Object(row)
        })
        .collect::<Vec<_>>();

    // Apply HAVING filters before sorting
    if !grouped_sql.having.is_empty() {
        rows.retain(|row| {
            grouped_sql.having.iter().all(|filter| {
                let val = row.get(&filter.output_name).and_then(json_to_f64);
                match val {
                    Some(v) => match filter.op {
                        planner::HavingOp::Gt => v > filter.value,
                        planner::HavingOp::GtEq => v >= filter.value,
                        planner::HavingOp::Lt => v < filter.value,
                        planner::HavingOp::LtEq => v <= filter.value,
                        planner::HavingOp::Eq => (v - filter.value).abs() < f64::EPSILON,
                    },
                    None => false, // NULL doesn't pass any filter
                }
            })
        });
    }

    // Top-K selection: when LIMIT is small relative to total rows and ORDER BY
    // is present, use partial sort (select_nth_unstable_by) instead of full sort.
    // This is O(N) average vs O(N log N) for full sort.
    let needed = plan.offset.unwrap_or(0) + plan.limit.unwrap_or(usize::MAX);
    let has_order = !grouped_sql.order_by.is_empty();

    if has_order && needed < rows.len() {
        // select_nth_unstable_by partitions: elements [0..n] are <= nth, [n+1..] are >=
        // This gives us the top-K without fully sorting.
        let n = needed.min(rows.len()) - 1;
        rows.select_nth_unstable_by(n, |left, right| {
            compare_grouped_rows(left, right, &grouped_sql.order_by)
        });
        rows.truncate(needed);
        // Now sort only the top-K subset (much smaller)
        rows.sort_by(|left, right| compare_grouped_rows(left, right, &grouped_sql.order_by));
    } else if has_order {
        rows.sort_by(|left, right| compare_grouped_rows(left, right, &grouped_sql.order_by));
    }

    // Apply LIMIT and OFFSET after sorting
    let offset = plan.offset.unwrap_or(0);
    if offset > 0 || plan.limit.is_some() {
        let start = offset.min(rows.len());
        let end = plan
            .limit
            .map(|limit| (start + limit).min(rows.len()))
            .unwrap_or(rows.len());
        rows = rows[start..end].to_vec();
    }

    Ok(SqlQueryResult { columns, rows })
}

fn grouped_metric_value(
    partial: Option<&crate::search::GroupedMetricPartial>,
    metric: &planner::SqlGroupedMetric,
) -> serde_json::Value {
    match (partial, &metric.function) {
        (
            Some(crate::search::GroupedMetricPartial::Count { count }),
            planner::SqlGroupedMetricFunction::Count,
        ) => serde_json::json!(count),
        (
            Some(crate::search::GroupedMetricPartial::Stats {
                count,
                sum,
                min: _,
                max: _,
            }),
            planner::SqlGroupedMetricFunction::Sum,
        ) => {
            if *count > 0 {
                serde_json::json!(sum)
            } else {
                serde_json::Value::Null
            }
        }
        (
            Some(crate::search::GroupedMetricPartial::Stats { count, sum, .. }),
            planner::SqlGroupedMetricFunction::Avg,
        ) => {
            if *count > 0 {
                serde_json::json!(sum / *count as f64)
            } else {
                serde_json::Value::Null
            }
        }
        (
            Some(crate::search::GroupedMetricPartial::Stats { count, min, .. }),
            planner::SqlGroupedMetricFunction::Min,
        ) => {
            if *count > 0 {
                serde_json::json!(min)
            } else {
                serde_json::Value::Null
            }
        }
        (
            Some(crate::search::GroupedMetricPartial::Stats { count, max, .. }),
            planner::SqlGroupedMetricFunction::Max,
        ) => {
            if *count > 0 {
                serde_json::json!(max)
            } else {
                serde_json::Value::Null
            }
        }
        _ => serde_json::Value::Null,
    }
}

fn compare_grouped_rows(
    left: &serde_json::Value,
    right: &serde_json::Value,
    order_by: &[planner::SqlOrderBy],
) -> std::cmp::Ordering {
    for order in order_by {
        let left_value = left
            .get(&order.output_name)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let right_value = right
            .get(&order.output_name)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let ordering = compare_json_values(&left_value, &right_value);
        let ordering = if order.desc {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_json_values(left: &serde_json::Value, right: &serde_json::Value) -> std::cmp::Ordering {
    match (left, right) {
        (serde_json::Value::Null, serde_json::Value::Null) => std::cmp::Ordering::Equal,
        (serde_json::Value::Null, _) => std::cmp::Ordering::Greater,
        (_, serde_json::Value::Null) => std::cmp::Ordering::Less,
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => left
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&right.as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal),
        (serde_json::Value::String(left), serde_json::Value::String(right)) => left.cmp(right),
        (serde_json::Value::Bool(left), serde_json::Value::Bool(right)) => left.cmp(right),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn json_to_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn planner_extracts_text_match_and_rewrites_sql() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price, _score FROM products WHERE text_match(description, 'iphone') AND price > 500 ORDER BY _score DESC",
        )
        .unwrap();

        assert_eq!(plan.index_name, "products");
        assert_eq!(plan.primary_text_match().unwrap().field, "description");
        assert_eq!(plan.primary_text_match().unwrap().query, "iphone");
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(plan.required_columns.contains(&"title".to_string()));
        assert!(plan.required_columns.contains(&"price".to_string()));
        assert!(plan.rewritten_sql.contains("FROM matched_rows"));
        assert!(!plan.rewritten_sql.contains("text_match"));
        assert!(!plan.rewritten_sql.contains("price > 500"));
    }

    #[test]
    fn planner_accepts_quoted_hyphenated_index_names() {
        let plan = planner::plan_sql(
            "my-index",
            "SELECT count(*) AS total FROM \"my-index\" WHERE text_match(description, 'iphone')",
        )
        .unwrap();

        assert_eq!(plan.index_name, "my-index");
        assert_eq!(plan.primary_text_match().unwrap().field, "description");
        assert!(plan.rewritten_sql.contains("FROM matched_rows"));
    }

    #[test]
    fn planner_tracks_group_by_columns() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC",
        )
        .unwrap();

        assert_eq!(plan.group_by_columns, vec!["brand"]);
        assert!(plan.required_columns.contains(&"brand".to_string()));
        // `total` is a computed alias for count(*) — must NOT be in required_columns
        assert!(
            !plan.required_columns.contains(&"total".to_string()),
            "SELECT aliases like 'total' must not appear in required_columns"
        );
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        assert!(plan.uses_grouped_partials());
    }

    #[test]
    fn planner_excludes_all_select_aliases_from_required_columns() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total, avg(price) AS avg_price FROM products WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY total DESC",
        )
        .unwrap();

        assert!(plan.required_columns.contains(&"brand".to_string()));
        assert!(plan.required_columns.contains(&"price".to_string()));
        assert!(
            !plan.required_columns.contains(&"total".to_string()),
            "alias 'total' must be excluded from required_columns"
        );
        assert!(
            !plan.required_columns.contains(&"avg_price".to_string()),
            "alias 'avg_price' must be excluded from required_columns"
        );
    }

    #[test]
    fn planner_marks_select_star_as_fallback_shape() {
        let plan = planner::plan_sql(
            "products",
            "SELECT * FROM products WHERE text_match(description, 'iphone')",
        )
        .unwrap();

        assert!(plan.selects_all_columns);
        assert!(plan.required_columns.is_empty());
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn planner_tracks_residual_predicates_that_cannot_be_pushed_down() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, _score FROM products WHERE text_match(description, 'iphone') AND price > 500 AND _score > 1.0 ORDER BY _score DESC",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(plan.has_residual_predicates);
        assert!(plan.rewritten_sql.contains("_score > 1"));
    }

    // ── IN / BETWEEN pushdown ───────────────────────────────────────────

    #[test]
    fn between_pushes_down_as_range() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'iphone') AND price BETWEEN 100 AND 500",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Range(fields) => {
                let range = fields.get("price").expect("price range");
                assert_eq!(range.gte, Some(json!(100)));
                assert_eq!(range.lte, Some(json!(500)));
                assert!(range.gt.is_none());
                assert!(range.lt.is_none());
            }
            other => panic!("expected Range, got {other:?}"),
        }
    }

    #[test]
    fn in_list_pushes_down_as_bool_should() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND brand IN ('Apple', 'Samsung')",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.should.len(), 2);
                assert!(bool_query.must.is_empty());
                assert!(bool_query.must_not.is_empty());
                // Each should clause is a Term for brand
                for clause in &bool_query.should {
                    match clause {
                        crate::search::QueryClause::Term(fields) => {
                            assert!(fields.contains_key("brand"));
                        }
                        other => panic!("expected Term in should, got {other:?}"),
                    }
                }
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn in_list_with_numbers_pushes_down() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND price IN (100, 200, 300)",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.should.len(), 3);
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn not_in_stays_as_residual() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND brand NOT IN ('Apple')",
        )
        .unwrap();

        // NOT IN cannot be pushed — stays as residual for DataFusion
        assert_eq!(plan.pushed_filters.len(), 0);
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn not_between_stays_as_residual() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND price NOT BETWEEN 100 AND 500",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 0);
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn between_and_in_combine_with_existing_pushdown() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND price BETWEEN 100 AND 500 AND brand IN ('Apple', 'Samsung') AND rating >= 4.0",
        )
        .unwrap();

        // 3 pushed filters: BETWEEN (Range), IN (Bool), >= (Range)
        assert_eq!(plan.pushed_filters.len(), 3);
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn in_on_score_stays_as_residual() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND _score IN (1.0, 2.0)",
        )
        .unwrap();

        // _score is explicitly blocked from pushdown
        assert_eq!(plan.pushed_filters.len(), 0);
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn to_search_request_uses_match_all_when_no_pushdowns_exist() {
        let plan =
            planner::plan_sql("products", "SELECT title FROM products ORDER BY title ASC").unwrap();

        match plan.to_search_request().query {
            crate::search::QueryClause::MatchAll(_) => {}
            other => panic!("expected MatchAll query, got {other:?}"),
        }
    }

    #[test]
    fn to_search_request_preserves_text_match_and_filters() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND price >= 500",
        )
        .unwrap();

        let req = plan.to_search_request();
        match req.query {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.must.len(), 1);
                assert_eq!(bool_query.filter.len(), 1);
            }
            other => panic!("expected Bool query, got {other:?}"),
        }
    }

    #[test]
    fn to_search_request_emits_internal_grouped_agg_for_group_by_query() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total, avg(price) AS avg_price FROM products WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY total DESC",
        )
        .unwrap();

        let req = plan.to_search_request();
        assert_eq!(req.size, 0);
        assert!(req.aggs.contains_key(planner::INTERNAL_SQL_GROUPED_AGG));
    }

    #[test]
    fn planner_supports_multiple_text_match_predicates() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND text_match(title, 'pro')",
        )
        .unwrap();

        assert_eq!(plan.text_matches.len(), 2);
        let req = plan.to_search_request();
        match req.query {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.must.len(), 2);
                assert!(bool_query.filter.is_empty());
            }
            other => panic!("expected Bool query, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_planned_sql_orders_by_underscore_score() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price, _score FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC",
        )
        .unwrap();

        let hits = vec![
            json!({
                "_id": "2",
                "_score": 1.25,
                "_source": {"title": "Budget phone", "price": 499.0, "description": "iphone style"}
            }),
            json!({
                "_id": "1",
                "_score": 2.75,
                "_source": {"title": "Pro phone", "price": 999.0, "description": "iphone pro"}
            }),
        ];

        let result = execute_planned_sql(&plan, &hits).await.unwrap();
        assert_eq!(result.columns, vec!["title", "price", "_score"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["title"], "Pro phone");
        assert_eq!(result.rows[1]["title"], "Budget phone");
    }

    #[tokio::test]
    async fn execute_planned_sql_supports_avg_and_count() {
        let plan = planner::plan_sql(
            "products",
            "SELECT avg(price) AS avg_price, count(*) AS total FROM products WHERE text_match(description, 'iphone')",
        )
        .unwrap();

        let hits = vec![
            json!({
                "_id": "1",
                "_score": 3.0,
                "_source": {"price": 900.0, "description": "iphone"}
            }),
            json!({
                "_id": "2",
                "_score": 2.0,
                "_source": {"price": 1100.0, "description": "iphone max"}
            }),
        ];

        let result = execute_planned_sql(&plan, &hits).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["total"], 2);
        assert_eq!(result.rows[0]["avg_price"], 1000.0);
    }

    #[tokio::test]
    async fn execute_planned_sql_supports_group_by() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total, avg(price) AS avg_price FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC, brand ASC",
        )
        .unwrap();

        let hits = vec![
            json!({
                "_id": "1",
                "_score": 3.2,
                "_source": {"brand": "Apple", "price": 999.0, "description": "iphone pro"}
            }),
            json!({
                "_id": "2",
                "_score": 2.4,
                "_source": {"brand": "Apple", "price": 899.0, "description": "iphone plus"}
            }),
            json!({
                "_id": "3",
                "_score": 1.8,
                "_source": {"brand": "Samsung", "price": 799.0, "description": "iphone competitor"}
            }),
        ];

        let result = execute_planned_sql(&plan, &hits).await.unwrap();
        assert_eq!(result.columns, vec!["brand", "total", "avg_price"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["brand"], "Apple");
        assert_eq!(result.rows[0]["total"], 2);
        assert_eq!(result.rows[0]["avg_price"], 949.0);
        assert_eq!(result.rows[1]["brand"], "Samsung");
        assert_eq!(result.rows[1]["total"], 1);
    }

    #[test]
    fn execute_grouped_partial_sql_merges_and_orders_group_rows() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total, avg(price) AS avg_price FROM products WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY total DESC, brand ASC",
        )
        .unwrap();

        let partials = vec![
            std::collections::HashMap::from([(
                planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
                crate::search::PartialAggResult::GroupedMetrics {
                    buckets: vec![
                        crate::search::GroupedMetricsBucket {
                            group_values: vec![json!("Apple")],
                            metrics: std::collections::HashMap::from([
                                (
                                    "total".to_string(),
                                    crate::search::GroupedMetricPartial::Count { count: 1 },
                                ),
                                (
                                    "avg_price".to_string(),
                                    crate::search::GroupedMetricPartial::Stats {
                                        count: 1,
                                        sum: 999.0,
                                        min: 999.0,
                                        max: 999.0,
                                    },
                                ),
                            ]),
                        },
                        crate::search::GroupedMetricsBucket {
                            group_values: vec![json!("Samsung")],
                            metrics: std::collections::HashMap::from([
                                (
                                    "total".to_string(),
                                    crate::search::GroupedMetricPartial::Count { count: 1 },
                                ),
                                (
                                    "avg_price".to_string(),
                                    crate::search::GroupedMetricPartial::Stats {
                                        count: 1,
                                        sum: 799.0,
                                        min: 799.0,
                                        max: 799.0,
                                    },
                                ),
                            ]),
                        },
                    ],
                },
            )]),
            std::collections::HashMap::from([(
                planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
                crate::search::PartialAggResult::GroupedMetrics {
                    buckets: vec![crate::search::GroupedMetricsBucket {
                        group_values: vec![json!("Apple")],
                        metrics: std::collections::HashMap::from([
                            (
                                "total".to_string(),
                                crate::search::GroupedMetricPartial::Count { count: 1 },
                            ),
                            (
                                "avg_price".to_string(),
                                crate::search::GroupedMetricPartial::Stats {
                                    count: 1,
                                    sum: 899.0,
                                    min: 899.0,
                                    max: 899.0,
                                },
                            ),
                        ]),
                    }],
                },
            )]),
        ];

        let result = execute_grouped_partial_sql(&plan, &partials).unwrap();
        assert_eq!(result.columns, vec!["brand", "total", "avg_price"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["brand"], "Apple");
        assert_eq!(result.rows[0]["total"], 2);
        assert_eq!(result.rows[0]["avg_price"], 949.0);
        assert_eq!(result.rows[1]["brand"], "Samsung");
        assert_eq!(result.rows[1]["total"], 1);
        assert_eq!(result.rows[1]["avg_price"], 799.0);
    }

    #[test]
    fn explain_shows_fast_field_strategy_for_projected_query() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC",
        )
        .unwrap();

        let explain = plan.to_explain_json();
        assert_eq!(explain["index"], "products");
        assert_eq!(explain["execution_strategy"], "tantivy_grouped_partials");
        assert_eq!(explain["pushdown_summary"]["pushed_filter_count"], 1);
        assert_eq!(
            explain["pushdown_summary"]["has_residual_predicates"],
            false
        );
        assert_eq!(
            explain["pushdown_summary"]["text_match"]["field"],
            "description"
        );
        assert_eq!(explain["pushdown_summary"]["text_match"]["query"], "iphone");
        assert_eq!(
            explain["pushdown_summary"]["text_matches"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(explain["columns"]["group_by"], json!(["brand"]));
        assert!(!explain["columns"]["selects_all"].as_bool().unwrap());
        assert_eq!(explain["columns"]["uses_grouped_partials"], true);

        let pipeline = explain["pipeline"].as_array().unwrap();
        assert_eq!(pipeline.len(), 4);
        assert_eq!(pipeline[0]["name"], "tantivy_search");
        assert_eq!(pipeline[1]["name"], "grouped_partial_collect");
        assert_eq!(pipeline[2]["name"], "grouped_partial_merge");
        assert_eq!(pipeline[3]["name"], "final_grouped_sql_shape");
    }

    #[test]
    fn explain_shows_materialized_fallback_for_select_star() {
        let plan = planner::plan_sql(
            "products",
            "SELECT * FROM products WHERE text_match(description, 'iphone')",
        )
        .unwrap();

        let explain = plan.to_explain_json();
        assert_eq!(explain["execution_strategy"], "materialized_hits_fallback");
        assert!(
            explain["strategy_reason"]
                .as_str()
                .unwrap()
                .contains("SELECT *")
        );
        assert!(explain["columns"]["selects_all"].as_bool().unwrap());

        let pipeline = explain["pipeline"].as_array().unwrap();
        assert_eq!(pipeline[1]["name"], "source_materialization");
    }

    #[test]
    fn explain_shows_residual_predicates_in_datafusion_stage() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, _score FROM products WHERE text_match(description, 'iphone') AND _score > 1.0 ORDER BY _score DESC",
        )
        .unwrap();

        let explain = plan.to_explain_json();
        assert_eq!(explain["pushdown_summary"]["has_residual_predicates"], true);

        let pipeline = explain["pipeline"].as_array().unwrap();
        let df_stage = &pipeline[3];
        assert_eq!(df_stage["residual_predicates"], true);
        assert!(
            df_stage["residual_note"]
                .as_str()
                .unwrap()
                .contains("could not be pushed")
        );
    }

    #[test]
    fn explain_match_all_when_no_text_match() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price FROM products ORDER BY price DESC",
        )
        .unwrap();

        let explain = plan.to_explain_json();
        assert!(explain["pushdown_summary"]["text_match"].is_null());
        assert!(
            explain["pushdown_summary"]["text_matches"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(explain["pushdown_summary"]["pushed_filter_count"], 0);

        let pipeline = explain["pipeline"].as_array().unwrap();
        let tantivy_stage = &pipeline[0];
        assert!(tantivy_stage.get("text_match").is_none());
    }

    #[test]
    fn explain_preserves_original_and_rewritten_sql() {
        let original = "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY brand";
        let plan = planner::plan_sql("products", original).unwrap();

        let explain = plan.to_explain_json();
        assert_eq!(explain["original_sql"], original);
        assert!(
            explain["rewritten_sql"]
                .as_str()
                .unwrap()
                .contains("matched_rows")
        );
        assert!(
            !explain["rewritten_sql"]
                .as_str()
                .unwrap()
                .contains("text_match")
        );
    }

    #[test]
    fn build_record_batch_with_hints_uses_float64_for_empty_numeric_column() {
        use crate::hybrid::arrow_bridge::{ColumnKind, build_record_batch_with_hints};
        use crate::hybrid::column_store::ColumnStore;
        use datafusion::arrow::datatypes::DataType;
        use std::collections::{BTreeMap, HashMap};

        // Zero rows — simulates a query that matched 0 documents
        let mut columns = BTreeMap::new();
        columns.insert("brand".to_string(), vec![]);
        columns.insert("price".to_string(), vec![]);
        let store = ColumnStore::new(vec![], vec![], columns);

        // Without hints: both columns would be Utf8 (the default for empty)
        let batch_no_hints = crate::hybrid::arrow_bridge::build_record_batch(&store).unwrap();
        // _id, _score, brand, price
        assert_eq!(
            batch_no_hints
                .schema()
                .field_with_name("price")
                .unwrap()
                .data_type(),
            &DataType::Utf8,
            "Without hints, empty column should default to Utf8"
        );

        // With hints: price should be Float64
        let mut hints = HashMap::new();
        hints.insert("price".to_string(), ColumnKind::Float64);
        let batch_with_hints = build_record_batch_with_hints(&store, &hints).unwrap();
        assert_eq!(
            batch_with_hints
                .schema()
                .field_with_name("price")
                .unwrap()
                .data_type(),
            &DataType::Float64,
            "With Float64 hint, empty column should be Float64"
        );
        assert_eq!(
            batch_with_hints
                .schema()
                .field_with_name("brand")
                .unwrap()
                .data_type(),
            &DataType::Utf8,
            "Unhinted column should still infer (Utf8 for empty)"
        );
    }

    #[test]
    fn limit_pushdown_with_order_by_score() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price, _score FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC LIMIT 25",
        ).unwrap();
        assert_eq!(plan.limit, Some(25));
        assert_eq!(plan.offset, None);
        assert!(plan.limit_pushed_down);
        let req = plan.to_search_request();
        assert_eq!(req.size, 25);
    }

    #[test]
    fn limit_pushdown_with_offset() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price, _score FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC LIMIT 10 OFFSET 20",
        ).unwrap();
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.offset, Some(20));
        assert!(plan.limit_pushed_down);
        let req = plan.to_search_request();
        assert_eq!(req.size, 30); // limit + offset
    }

    #[test]
    fn limit_pushed_down_for_single_fast_field_order_by() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'iphone') ORDER BY price DESC LIMIT 10",
        ).unwrap();
        assert_eq!(plan.limit, Some(10));
        assert!(
            plan.limit_pushed_down,
            "single fast-field ORDER BY should push down LIMIT"
        );
        assert_eq!(plan.sort_pushdown, Some(("price".to_string(), true)));
        let req = plan.to_search_request();
        assert_eq!(req.size, 10); // pushed down, not 100K
        assert_eq!(req.sort.len(), 1); // sort clause populated
    }

    #[test]
    fn sort_pushdown_not_applied_for_multi_column_order_by() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') ORDER BY price DESC, title ASC LIMIT 10",
        ).unwrap();
        assert!(
            plan.sort_pushdown.is_none(),
            "multi-column ORDER BY cannot push to Tantivy"
        );
        assert!(!plan.limit_pushed_down);
    }

    #[test]
    fn sort_pushdown_asc_direction() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products ORDER BY price ASC LIMIT 5",
        )
        .unwrap();
        assert_eq!(plan.sort_pushdown, Some(("price".to_string(), false)));
        assert!(plan.limit_pushed_down);
    }

    #[test]
    fn sort_pushdown_no_limit_still_captured() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products ORDER BY price DESC",
        )
        .unwrap();
        // sort_pushdown is captured even without LIMIT
        assert_eq!(plan.sort_pushdown, Some(("price".to_string(), true)));
        // but limit_pushed_down requires a LIMIT clause
        assert!(!plan.limit_pushed_down);
    }

    #[test]
    fn limit_not_pushed_down_when_residual_predicates() {
        // _score > 1.0 is a residual predicate (not pushed into Tantivy)
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, _score FROM products WHERE text_match(description, 'iphone') AND _score > 1.0 ORDER BY _score DESC LIMIT 5",
        ).unwrap();
        assert_eq!(plan.limit, Some(5));
        assert!(plan.has_residual_predicates);
        assert!(!plan.limit_pushed_down);
        let req = plan.to_search_request();
        assert_eq!(req.size, 100_000);
    }

    #[test]
    fn no_limit_uses_sql_match_limit() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC",
        ).unwrap();
        assert_eq!(plan.limit, None);
        assert!(!plan.limit_pushed_down);
        let req = plan.to_search_request();
        assert_eq!(req.size, 100_000);
    }

    #[test]
    fn limit_pushdown_no_order_by() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'iphone') LIMIT 50",
        )
        .unwrap();
        assert_eq!(plan.limit, Some(50));
        assert!(plan.limit_pushed_down); // no ORDER BY = default _score sort, safe
        let req = plan.to_search_request();
        assert_eq!(req.size, 50);
    }

    #[test]
    fn limit_pushed_down_for_select_star() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT * FROM products WHERE text_match(description, 'iphone') LIMIT 10",
        )
        .unwrap();
        assert_eq!(plan.limit, Some(10));
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        // Search request should collect only 10 docs, not 100K
        let req = plan.to_search_request();
        assert_eq!(req.size, 10);
    }

    #[test]
    fn grouped_query_with_limit_uses_grouped_partials() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT category, count(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY category LIMIT 5",
        ).unwrap();
        assert_eq!(plan.limit, Some(5));
        assert!(plan.grouped_sql.is_some()); // LIMIT + GROUP BY uses grouped partials
        assert!(!plan.limit_pushed_down); // LIMIT is applied after merge, not pushed to Tantivy
    }

    #[test]
    fn explain_shows_limit_pushdown_info() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price, _score FROM products WHERE text_match(description, 'iphone') ORDER BY _score DESC LIMIT 25",
        ).unwrap();
        let explain = plan.to_explain_json();
        assert_eq!(explain["columns"]["limit"], 25);
        assert_eq!(explain["columns"]["limit_pushed_down"], true);
        let pipeline = explain["pipeline"].as_array().unwrap();
        assert_eq!(pipeline[0]["max_hits"], 25);
        assert_eq!(pipeline[0]["limit_pushed_down"], true);
    }

    #[test]
    fn rewritten_sql_preserves_limit_clause() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'test') LIMIT 10",
        )
        .unwrap();
        assert!(plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(10));
        // CRITICAL: the rewritten SQL must still contain the LIMIT clause
        // so DataFusion re-applies it after multi-shard concatenation
        let upper = plan.rewritten_sql.to_uppercase();
        assert!(
            upper.contains("LIMIT"),
            "rewritten SQL must contain LIMIT: {}",
            plan.rewritten_sql
        );
    }

    // ── needs_id / needs_score detection tests ──────────────────────────

    #[test]
    fn plan_detects_needs_id_when_projected() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT _id, price FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(plan.needs_id);
        assert!(!plan.needs_score);
    }

    #[test]
    fn plan_detects_needs_score_when_projected() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, _score FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(!plan.needs_id);
        assert!(plan.needs_score);
    }

    #[test]
    fn plan_detects_needs_score_in_order_by() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'test') ORDER BY _score DESC",
        )
        .unwrap();
        assert!(!plan.needs_id);
        assert!(plan.needs_score);
    }

    #[test]
    fn plan_neither_id_nor_score_when_not_referenced() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, price FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(!plan.needs_id);
        assert!(!plan.needs_score);
    }

    #[test]
    fn plan_detects_both_id_and_score() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT _id, _score, title FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(plan.needs_id);
        assert!(plan.needs_score);
    }

    #[test]
    fn plan_select_star_needs_both() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT * FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        // SELECT * doesn't go through required_columns since selects_all_columns=true
        // but needs_id/needs_score should be false (they're only set from explicit references)
        assert!(!plan.needs_id);
        assert!(!plan.needs_score);
        assert!(plan.selects_all_columns);
    }

    // ── Truncation flag logic ───────────────────────────────────────────

    #[test]
    fn explicit_limit_should_not_be_truncated() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'test') LIMIT 5",
        )
        .unwrap();
        // User explicitly requested LIMIT 5 → truncated should never be true
        assert!(plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(5));
        // The truncation check in search_sql: !limit_pushed_down && limit.is_none() && ...
        // With limit_pushed_down=true, truncated is always false regardless of matched_hits
        let would_truncate = !plan.limit_pushed_down && plan.limit.is_none();
        assert!(
            !would_truncate,
            "explicit LIMIT must not trigger truncation"
        );
    }

    #[test]
    fn no_limit_can_truncate_at_ceiling() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(!plan.limit_pushed_down);
        assert!(plan.limit.is_none());
        // With no LIMIT, if matched_hits > 100K, truncated should be true
        let would_truncate = !plan.limit_pushed_down && plan.limit.is_none();
        assert!(
            would_truncate,
            "no LIMIT should allow truncation at ceiling"
        );
    }

    #[test]
    fn grouped_partials_never_truncated() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title, count(*) AS cnt FROM products WHERE text_match(description, 'test') GROUP BY title",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        // Grouped partials scan all docs (size=0), never truncated
    }

    #[test]
    fn limit_with_residual_predicate_not_pushed_down() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'test') AND title = 'Foo' LIMIT 5",
        )
        .unwrap();
        // title = 'Foo' is a residual predicate (not pushed into Tantivy for keyword match on text_match)
        // Wait — title = 'Foo' may or may not be pushed. Let's check:
        // has_residual_predicates means selection was NOT fully consumed by pushdowns
        if plan.has_residual_predicates {
            assert!(!plan.limit_pushed_down);
            assert_eq!(plan.limit, Some(5));
            // With residual predicates + explicit LIMIT, limit is NOT pushed
            // but truncated should still be false (user asked for LIMIT)
            let would_truncate = !plan.limit_pushed_down && plan.limit.is_none();
            assert!(!would_truncate);
        }
    }

    // ── Ungrouped aggregate pushdown ───────────────────────────────────

    #[test]
    fn count_star_without_group_by_uses_grouped_partials() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT count(*) AS total FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        // count(*) without GROUP BY should use the grouped partial path
        // with zero group-by columns (one global bucket)
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert!(
            grouped.group_columns.is_empty(),
            "ungrouped aggregate should have zero group-by columns"
        );
        assert_eq!(grouped.metrics.len(), 1);
        assert_eq!(
            grouped.metrics[0].function,
            crate::hybrid::planner::SqlGroupedMetricFunction::Count
        );
    }

    #[test]
    fn ungrouped_multi_agg_uses_grouped_partials() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT count(*) AS cnt, avg(price) AS avg_price, sum(price) AS total FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert!(grouped.group_columns.is_empty());
        assert_eq!(grouped.metrics.len(), 3);
    }

    #[test]
    fn ungrouped_with_bare_column_falls_back() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT price, count(*) AS cnt FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        // `price` without GROUP BY is not a valid aggregate → falls back
        assert!(!plan.uses_grouped_partials());
    }

    #[test]
    fn select_literal_does_not_force_needs_score() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT 1 AS one FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        // SELECT 1 has no data column references
        assert!(plan.required_columns.is_empty());
        assert!(!plan.needs_score);
    }

    #[test]
    fn select_with_columns_does_not_force_needs_score() {
        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT count(*), avg(price) FROM products WHERE text_match(description, 'test')",
        )
        .unwrap();
        // avg(price) references price → required_columns is not empty
        assert!(!plan.required_columns.is_empty());
        // needs_score should NOT be forced
        assert!(!plan.needs_score);
    }

    #[test]
    fn grouped_partial_sql_applies_limit() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 2",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, Some(2));

        // Simulate 4 grouped buckets from a single shard
        let mut partial_map = HashMap::new();
        let buckets = vec![
            GroupedMetricsBucket {
                group_values: vec![json!("alice")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 100 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("bob")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 50 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("carol")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 30 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("dave")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 10 },
                )]),
            },
        ];
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["author"], "alice");
        assert_eq!(result.rows[0]["posts"], 100);
        assert_eq!(result.rows[1]["author"], "bob");
        assert_eq!(result.rows[1]["posts"], 50);
    }

    #[test]
    fn grouped_partial_sql_applies_offset_and_limit() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 2 OFFSET 1",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, Some(2));
        assert_eq!(plan.offset, Some(1));

        let mut partial_map = HashMap::new();
        let buckets = vec![
            GroupedMetricsBucket {
                group_values: vec![json!("alice")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 100 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("bob")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 50 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("carol")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 30 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("dave")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 10 },
                )]),
            },
        ];
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        // OFFSET 1 skips alice, LIMIT 2 takes bob and carol
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["author"], "bob");
        assert_eq!(result.rows[0]["posts"], 50);
        assert_eq!(result.rows[1]["author"], "carol");
        assert_eq!(result.rows[1]["posts"], 30);
    }

    #[test]
    fn grouped_partial_sql_no_limit_returns_all() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, None);

        let mut partial_map = HashMap::new();
        let buckets = vec![
            GroupedMetricsBucket {
                group_values: vec![json!("alice")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 100 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("bob")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 50 },
                )]),
            },
        ];
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn grouped_partial_sql_applies_having_filter() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author HAVING posts > 30 ORDER BY posts DESC",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);

        let mut partial_map = HashMap::new();
        let buckets = vec![
            GroupedMetricsBucket {
                group_values: vec![json!("alice")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 100 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("bob")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 20 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("carol")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 50 },
                )]),
            },
        ];
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        // bob (20) filtered out by HAVING posts > 30
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["author"], "alice");
        assert_eq!(result.rows[0]["posts"], 100);
        assert_eq!(result.rows[1]["author"], "carol");
        assert_eq!(result.rows[1]["posts"], 50);
    }

    #[test]
    fn grouped_partial_sql_having_with_limit() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author HAVING posts > 20 ORDER BY posts DESC LIMIT 1",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, Some(1));

        let mut partial_map = HashMap::new();
        let buckets = vec![
            GroupedMetricsBucket {
                group_values: vec![json!("alice")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 100 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("bob")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 50 },
                )]),
            },
            GroupedMetricsBucket {
                group_values: vec![json!("carol")],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count { count: 10 },
                )]),
            },
        ];
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        // HAVING filters carol (10), LIMIT 1 takes only alice (100)
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["author"], "alice");
        assert_eq!(result.rows[0]["posts"], 100);
    }

    #[test]
    fn top_k_selection_produces_correct_results() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        // 100 authors with sequential post counts — top-K should find the highest
        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 3",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, Some(3));

        let mut partial_map = HashMap::new();
        let mut buckets = Vec::new();
        for i in 0..100 {
            buckets.push(GroupedMetricsBucket {
                group_values: vec![json!(format!("author_{}", i))],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count {
                        count: i as u64 + 1,
                    },
                )]),
            });
        }
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        // Top 3 by posts DESC should be author_99 (100), author_98 (99), author_97 (98)
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0]["author"], "author_99");
        assert_eq!(result.rows[0]["posts"], 100);
        assert_eq!(result.rows[1]["author"], "author_98");
        assert_eq!(result.rows[1]["posts"], 99);
        assert_eq!(result.rows[2]["author"], "author_97");
        assert_eq!(result.rows[2]["posts"], 98);
    }

    #[test]
    fn top_k_with_offset_produces_correct_results() {
        use crate::search::{GroupedMetricPartial, GroupedMetricsBucket, PartialAggResult};
        use std::collections::HashMap;

        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 2 OFFSET 2",
        )
        .unwrap();

        let mut partial_map = HashMap::new();
        let mut buckets = Vec::new();
        for i in 0..50 {
            buckets.push(GroupedMetricsBucket {
                group_values: vec![json!(format!("author_{}", i))],
                metrics: HashMap::from([(
                    "posts".to_string(),
                    GroupedMetricPartial::Count {
                        count: i as u64 + 1,
                    },
                )]),
            });
        }
        partial_map.insert(
            planner::INTERNAL_SQL_GROUPED_AGG.to_string(),
            PartialAggResult::GroupedMetrics { buckets },
        );

        let result = execute_grouped_partial_sql(&plan, &[partial_map]).unwrap();
        // OFFSET 2 skips author_49 and author_48, gives author_47 and author_46
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["author"], "author_47");
        assert_eq!(result.rows[0]["posts"], 48);
        assert_eq!(result.rows[1]["author"], "author_46");
        assert_eq!(result.rows[1]["posts"], 47);
    }

    #[test]
    fn planner_detects_top_k_eligibility() {
        let plan = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 10",
        )
        .unwrap();
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert!(grouped.uses_top_k(plan.limit));

        // No LIMIT = no top-K
        let plan2 = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC",
        )
        .unwrap();
        let grouped2 = plan2.grouped_sql.as_ref().unwrap();
        assert!(!grouped2.uses_top_k(plan2.limit));

        // No ORDER BY = no top-K
        let plan3 = planner::plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author LIMIT 10",
        )
        .unwrap();
        let grouped3 = plan3.grouped_sql.as_ref().unwrap();
        assert!(!grouped3.uses_top_k(plan3.limit));
    }
}
