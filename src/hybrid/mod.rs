pub mod arrow_bridge;
pub mod column_store;
pub mod datafusion_exec;
pub mod merge;
pub mod planner;

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

pub use planner::QueryPlan;

#[derive(Debug, Clone, Serialize)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
}

pub async fn execute_planned_sql(plan: &QueryPlan, hits: &[Value]) -> Result<SqlQueryResult> {
    let column_store = column_store::ColumnStore::from_hits(hits, &plan.required_columns);
    let batch = arrow_bridge::build_record_batch(&column_store)?;
    datafusion_exec::execute_sql(plan, batch).await
}

pub async fn execute_planned_sql_batches(
    plan: &QueryPlan,
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> Result<SqlQueryResult> {
    datafusion_exec::execute_sql_batches(plan, batches).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn planner_extracts_text_match_and_rewrites_sql() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price, score FROM products WHERE text_match(description, 'iphone') AND price > 500 ORDER BY score DESC",
        )
        .unwrap();

        assert_eq!(plan.index_name, "products");
        assert_eq!(plan.text_match.as_ref().unwrap().field, "description");
        assert_eq!(plan.text_match.as_ref().unwrap().query, "iphone");
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
        assert_eq!(plan.text_match.as_ref().unwrap().field, "description");
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
            "SELECT title, score FROM products WHERE text_match(description, 'iphone') AND price > 500 AND score > 1.0 ORDER BY score DESC",
        )
        .unwrap();

        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(plan.has_residual_predicates);
        assert!(plan.rewritten_sql.contains("score > 1"));
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
    fn planner_rejects_multiple_text_match_predicates() {
        let err = planner::plan_sql(
            "products",
            "SELECT title FROM products WHERE text_match(description, 'iphone') AND text_match(title, 'pro')",
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Only one text_match(field, query) predicate is supported")
        );
    }

    #[tokio::test]
    async fn execute_planned_sql_orders_by_score() {
        let plan = planner::plan_sql(
            "products",
            "SELECT title, price, score FROM products WHERE text_match(description, 'iphone') ORDER BY score DESC",
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
        assert_eq!(result.columns, vec!["title", "price", "score"]);
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
    fn explain_shows_fast_field_strategy_for_projected_query() {
        let plan = planner::plan_sql(
            "products",
            "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC",
        )
        .unwrap();

        let explain = plan.to_explain_json();
        assert_eq!(explain["index"], "products");
        assert_eq!(explain["execution_strategy"], "tantivy_fast_fields");
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
        assert_eq!(explain["columns"]["group_by"], json!(["brand"]));
        assert!(!explain["columns"]["selects_all"].as_bool().unwrap());

        let pipeline = explain["pipeline"].as_array().unwrap();
        assert_eq!(pipeline.len(), 4);
        assert_eq!(pipeline[0]["name"], "tantivy_search");
        assert_eq!(pipeline[1]["name"], "fast_field_read");
        assert_eq!(pipeline[2]["name"], "arrow_batch");
        assert_eq!(pipeline[3]["name"], "datafusion_sql");
        assert_eq!(pipeline[3]["group_by"], json!(["brand"]));
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
            "SELECT title, score FROM products WHERE text_match(description, 'iphone') AND score > 1.0 ORDER BY score DESC",
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
        // _id, score, brand, price
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
}
