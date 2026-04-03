use super::SqlQueryResult;
use super::merge::record_batches_to_json_rows;
use super::planner::QueryPlan;
use anyhow::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub async fn execute_sql(plan: &QueryPlan, batch: RecordBatch) -> Result<SqlQueryResult> {
    execute_sql_batches(plan, vec![batch]).await
}

pub async fn execute_sql_batches(
    plan: &QueryPlan,
    batches: Vec<RecordBatch>,
) -> Result<SqlQueryResult> {
    let schema = if let Some(batch) = batches.first() {
        batch.schema()
    } else {
        Arc::new(Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "_id",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            ),
            datafusion::arrow::datatypes::Field::new(
                "score",
                datafusion::arrow::datatypes::DataType::Float32,
                false,
            ),
        ]))
    };

    // Unify all batches to the same schema (handles nullability mismatches
    // between local and Arrow-IPC-deserialized remote batches).
    let unified: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| {
            if b.schema() == schema {
                b
            } else {
                let columns: Vec<datafusion::arrow::array::ArrayRef> = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if let Some(col) = b.column_by_name(field.name()) {
                            col.clone()
                        } else {
                            datafusion::arrow::array::new_null_array(
                                field.data_type(),
                                b.num_rows(),
                            )
                        }
                    })
                    .collect();
                RecordBatch::try_new(schema.clone(), columns).unwrap_or(b)
            }
        })
        .collect();

    // Concatenate all batches into a single batch so DataFusion sees one
    // partition and applies LIMIT/ORDER BY across all shards correctly.
    let unified_batch = if unified.len() > 1 {
        match datafusion::arrow::compute::concat_batches(&schema, &unified) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    batch_count = unified.len(),
                    "concat_batches failed, falling back to separate partitions"
                );
                let table = MemTable::try_new(schema.clone(), vec![unified])?;
                let ctx = SessionContext::new();
                ctx.register_table("matched_rows", Arc::new(table))?;
                let dataframe = ctx.sql(&plan.rewritten_sql).await?;
                let batches = dataframe.collect().await?;
                let (columns, rows) = record_batches_to_json_rows(&batches)?;
                return Ok(SqlQueryResult { columns, rows });
            }
        }
    } else if let Some(b) = unified.into_iter().next() {
        b
    } else {
        RecordBatch::new_empty(schema.clone())
    };

    // Work around DataFusion 53 bug: when a SELECT projects columns in a
    // different order than the table schema, the LIMIT fetch-pushdown into
    // MemTable's TableScan silently ignores the limit. To avoid this, we
    // project the RecordBatch down to only the columns referenced in the SQL
    // (in schema order) so that DataFusion's projection is either identity
    // or a simple prefix — preventing the reorder that triggers the bug.
    let (table_batch, table_schema) = project_batch_to_sql_columns(&unified_batch, plan);

    let table = MemTable::try_new(table_schema, vec![vec![table_batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("matched_rows", Arc::new(table))?;
    let dataframe = ctx.sql(&plan.rewritten_sql).await?;
    let result_batches = dataframe.collect().await?;
    let (columns, rows) = record_batches_to_json_rows(&result_batches)?;

    Ok(SqlQueryResult { columns, rows })
}

/// Reorder a RecordBatch so its columns match the SELECT list order from the SQL.
/// This prevents DataFusion 53's projection-reorder LIMIT bug where
/// fetch-pushdown into MemTable's TableScan ignores LIMIT when projection
/// reorders columns relative to the table schema.
fn project_batch_to_sql_columns(
    batch: &RecordBatch,
    plan: &QueryPlan,
) -> (RecordBatch, Arc<Schema>) {
    let schema = batch.schema();

    let select_order = extract_select_column_order(&plan.rewritten_sql);
    if select_order.is_empty() {
        return (batch.clone(), schema);
    }

    // Build new column order: first the SELECT'd columns in SELECT order,
    // then any remaining columns not in the SELECT list (for WHERE/ORDER BY refs)
    let mut ordered_indices: Vec<usize> = Vec::new();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    for col_name in &select_order {
        if let Some(idx) = field_names.iter().position(|&n| n == col_name)
            && !ordered_indices.contains(&idx)
        {
            ordered_indices.push(idx);
        }
    }

    // Add any remaining referenced columns (ORDER BY, WHERE) that aren't in SELECT
    let referenced_columns = referenced_columns_from_plan(plan);
    for (idx, field) in schema.fields().iter().enumerate() {
        if referenced_columns.contains(field.name().as_str()) && !ordered_indices.contains(&idx) {
            ordered_indices.push(idx);
        }
    }

    // If the order already matches or we couldn't determine, return as-is
    if ordered_indices.len() == schema.fields().len()
        && ordered_indices.iter().enumerate().all(|(i, &v)| i == v)
    {
        return (batch.clone(), schema);
    }

    if ordered_indices.is_empty() {
        return (batch.clone(), schema);
    }

    let new_fields: Vec<_> = ordered_indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect();
    let new_columns: Vec<_> = ordered_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();
    let new_schema = Arc::new(Schema::new(new_fields));
    match RecordBatch::try_new(new_schema.clone(), new_columns) {
        Ok(projected) => (projected, new_schema),
        Err(_) => (batch.clone(), schema),
    }
}

fn referenced_columns_from_plan(plan: &QueryPlan) -> std::collections::HashSet<String> {
    let mut names: std::collections::HashSet<String> =
        plan.required_columns.iter().cloned().collect();
    if plan.needs_id {
        names.insert("_id".to_string());
    }
    if plan.needs_score {
        names.insert("score".to_string());
    }
    names
}

/// Extract the ordered list of column names from the SELECT clause.
fn extract_select_column_order(sql: &str) -> Vec<String> {
    use sqlparser::ast::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return Vec::new();
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if let Some(name) = expr_to_simple_name(expr) {
                    cols.push(name);
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Vec::new(); // SELECT * — can't optimize
            }
        }
    }
    cols
}

fn expr_to_simple_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => {
            parts.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));
        let ids: Vec<String> = (0..n).map(|i| format!("id-{}", i)).collect();
        let scores: Vec<f32> = vec![1.0f32; n];
        let names: Vec<String> = (0..n).map(|i| format!("item-{}", i)).collect();
        let prices: Vec<f64> = (0..n).map(|i| i as f64 * 10.0).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(datafusion::arrow::array::Float32Array::from(scores)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(prices)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn datafusion_limit_applied_on_single_batch() {
        let batch = make_test_batch(20);
        let plan = super::super::planner::plan_sql("test", "SELECT name, price FROM test LIMIT 5")
            .unwrap();
        let result = execute_sql_batches(&plan, vec![batch]).await.unwrap();
        assert_eq!(
            result.rows.len(),
            5,
            "LIMIT 5 must return 5 rows, got {}",
            result.rows.len()
        );
    }

    #[tokio::test]
    async fn datafusion_limit_applied_on_concatenated_batches() {
        let b1 = make_test_batch(10);
        let b2 = make_test_batch(10);
        let b3 = make_test_batch(10);
        // 30 total rows across 3 batches
        let plan = super::super::planner::plan_sql("test", "SELECT name, price FROM test LIMIT 7")
            .unwrap();
        let result = execute_sql_batches(&plan, vec![b1, b2, b3]).await.unwrap();
        assert_eq!(
            result.rows.len(),
            7,
            "LIMIT 7 must return 7 rows from 30, got {}",
            result.rows.len()
        );
    }

    #[tokio::test]
    async fn datafusion_limit_with_skip_id_score() {
        // Simulate needs_id=false, needs_score=false: _id is empty, score is 0
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]));
        let make_batch = |n: usize| -> RecordBatch {
            let ids: Vec<String> = vec!["".to_string(); n];
            let scores: Vec<f32> = vec![0.0f32; n];
            let cats: Vec<String> = (0..n).map(|i| format!("cat-{}", i % 3)).collect();
            let amounts: Vec<f64> = (0..n).map(|i| i as f64).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(datafusion::arrow::array::Float32Array::from(scores)),
                    Arc::new(StringArray::from(cats)),
                    Arc::new(Float64Array::from(amounts)),
                ],
            )
            .unwrap()
        };

        let b1 = make_batch(5);
        let b2 = make_batch(5);
        let b3 = make_batch(5);
        let plan =
            super::super::planner::plan_sql("test", "SELECT category, amount FROM test LIMIT 3")
                .unwrap();
        let result = execute_sql_batches(&plan, vec![b1, b2, b3]).await.unwrap();
        assert_eq!(
            result.rows.len(),
            3,
            "LIMIT 3 on 15 rows must return 3, got {}",
            result.rows.len()
        );
    }

    #[tokio::test]
    async fn datafusion_limit_with_ipc_round_trip() {
        // Simulate distributed path: build batches, IPC serialize, deserialize, then run SQL
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        let make_batch = |n: usize| -> RecordBatch {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![""; n])),
                    Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                        0.0f32;
                        n
                    ])),
                    Arc::new(StringArray::from(
                        (0..n).map(|i| format!("n-{}", i)).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        // Simulate 3 shards: build, IPC round-trip, then SQL
        let mut batches = Vec::new();
        for _ in 0..3 {
            let batch = make_batch(5);
            let ipc = super::super::arrow_bridge::record_batch_to_ipc(&batch).unwrap();
            let restored = super::super::arrow_bridge::record_batch_from_ipc(&ipc).unwrap();
            batches.push(restored);
        }
        let plan =
            super::super::planner::plan_sql("test", "SELECT name, val FROM test LIMIT 4").unwrap();
        let result = execute_sql_batches(&plan, batches).await.unwrap();
        assert_eq!(
            result.rows.len(),
            4,
            "LIMIT 4 after IPC round-trip must return 4, got {}",
            result.rows.len()
        );
    }

    #[tokio::test]
    async fn pure_datafusion_limit_nullability_isolation() {
        // Proves that nullability (non-null _id/score + nullable data cols) is NOT
        // the trigger for the DataFusion 53 LIMIT bug. The actual trigger is
        // projection column reorder vs schema order (see exact_taxi test below).
        let make_batch = |schema: Arc<Schema>| -> RecordBatch {
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(
                        (0..15).map(|i| format!("id-{i}")).collect::<Vec<_>>(),
                    )),
                    Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                        0.0f32;
                        15
                    ])),
                    Arc::new(Float64Array::from(
                        (0..15).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        (0..15).map(|i| format!("H{i}")).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let run_limit = |schema: Arc<Schema>, label: &str| {
            let label = label.to_string();
            async move {
                let batch = make_batch(schema.clone());
                let table =
                    datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
                let ctx = datafusion::prelude::SessionContext::new();
                ctx.register_table("t", Arc::new(table)).unwrap();
                let rows: usize = ctx
                    .sql("SELECT c, d FROM t LIMIT 5")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum();
                eprintln!("{label}: {rows} rows");
                rows
            }
        };

        // Case 1: all columns nullable
        let s1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Float32, true),
            Field::new("c", DataType::Float64, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let r1 = run_limit(s1, "all-nullable").await;

        // Case 2: first two NOT nullable (mirrors our _id/score schema)
        let s2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Float64, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let r2 = run_limit(s2, "mixed-nullable").await;

        // Case 3: all NOT nullable
        let s3 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        let r3 = run_limit(s3, "all-not-nullable").await;

        assert_eq!(r1, 5, "all-nullable: expected 5, got {r1}");
        assert_eq!(r2, 5, "mixed-nullable: expected 5, got {r2}");
        assert_eq!(r3, 5, "all-not-nullable: expected 5, got {r3}");
    }

    #[tokio::test]
    async fn pure_datafusion_limit_exact_taxi_schema_no_order_by() {
        // Reproduces DataFusion 53 bug: LIMIT is silently ignored when SELECT
        // projects columns in a different order than the MemTable schema.
        // Test A proves schema-order SELECT works; Test B proves our workaround
        // (project_batch_to_sql_columns) correctly handles reverse-order SELECT.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("base_passenger_fare", DataType::Float64, true),
            Field::new("hvfhs_license_num", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..15).map(|i| format!("id-{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                    0.0f32;
                    15
                ])),
                Arc::new(Float64Array::from(
                    (0..15).map(|i| i as f64 * 10.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..15)
                        .map(|i| format!("HV{:04}", i % 3))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        // Test A: SELECT in schema order (pos2, pos3) — should work
        {
            let table = datafusion::datasource::MemTable::try_new(
                schema.clone(),
                vec![vec![batch.clone()]],
            )
            .unwrap();
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.register_table("matched_rows", Arc::new(table)).unwrap();
            let rows: usize = ctx
                .sql("SELECT base_passenger_fare, hvfhs_license_num FROM matched_rows LIMIT 5")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            eprintln!("Schema-order SELECT (fare,lic): {rows}");
            assert_eq!(rows, 5, "Schema-order: expected 5, got {rows}");
        }

        // Test B: SELECT in REVERSE schema order via execute_sql_batches
        // which uses project_batch_to_sql_columns to work around DataFusion bug
        {
            let plan = super::super::planner::plan_sql(
                "test",
                "SELECT hvfhs_license_num, base_passenger_fare FROM test LIMIT 5",
            )
            .unwrap();
            let result = execute_sql_batches(&plan, vec![batch.clone()])
                .await
                .unwrap();
            eprintln!(
                "Reverse-order via execute_sql_batches: {} rows",
                result.rows.len()
            );
            assert_eq!(
                result.rows.len(),
                5,
                "Reverse-order: expected 5, got {}",
                result.rows.len()
            );
        }

        // Test C: SELECT single column — should always work
        {
            let table =
                datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]])
                    .unwrap();
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.register_table("matched_rows", Arc::new(table)).unwrap();
            let rows: usize = ctx
                .sql("SELECT hvfhs_license_num FROM matched_rows LIMIT 5")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            eprintln!("Single column: {rows}");
            assert_eq!(rows, 5, "Single column: expected 5, got {rows}");
        }
    }

    #[tokio::test]
    async fn datafusion_limit_offset_with_projection_order_different_from_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("base_passenger_fare", DataType::Float64, true),
            Field::new("hvfhs_license_num", DataType::Utf8, true),
        ]));

        let make_batch = |start: usize, n: usize| -> RecordBatch {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        (start..start + n)
                            .map(|i| format!("id-{i}"))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                        0.0f32;
                        n
                    ])),
                    Arc::new(Float64Array::from(
                        (start..start + n).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        (start..start + n)
                            .map(|i| format!("lic-{i:02}"))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let plan = super::super::planner::plan_sql(
            "test",
            "SELECT hvfhs_license_num, base_passenger_fare FROM test ORDER BY base_passenger_fare ASC LIMIT 4 OFFSET 2",
        )
        .unwrap();
        let result = execute_sql_batches(
            &plan,
            vec![make_batch(0, 3), make_batch(3, 3), make_batch(6, 3)],
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.rows[0]["hvfhs_license_num"], json!("lic-02"));
        assert_eq!(result.rows[0]["base_passenger_fare"], json!(2.0));
        assert_eq!(result.rows[3]["hvfhs_license_num"], json!("lic-05"));
        assert_eq!(result.rows[3]["base_passenger_fare"], json!(5.0));
    }

    // ── DataFusion 53 upstream bug regression tests ─────────────────────
    //
    // These tests document a confirmed DataFusion 53.0.0 bug where LIMIT
    // fetch-pushdown into MemTable's TableScan is silently ignored when
    // the SQL SELECT projects columns in a different order than the table
    // schema. When DataFusion fixes this upstream, these tests should still
    // pass (the workaround becomes a harmless no-op).

    /// Standalone reproduction for upstream bug report.
    /// Run with: cargo test df53_limit_bug_full_repro --lib -- --nocapture
    #[tokio::test]
    async fn df53_limit_bug_full_repro() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Float64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("a-{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..20).map(|i| i as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("c-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        // Case 1: Schema-order SELECT
        let t1 =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .unwrap();
        let ctx1 = datafusion::prelude::SessionContext::new();
        ctx1.register_table("t", Arc::new(t1)).unwrap();
        let r1: usize = ctx1
            .sql("SELECT col_b, col_c FROM t LIMIT 5")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        eprintln!("Schema-order SELECT (col_b, col_c): {r1} rows");

        // Case 2: Reverse-order SELECT (BUG)
        let t2 =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .unwrap();
        let ctx2 = datafusion::prelude::SessionContext::new();
        ctx2.register_table("t", Arc::new(t2)).unwrap();
        let df2 = ctx2
            .sql("SELECT col_c, col_b FROM t LIMIT 5")
            .await
            .unwrap();
        let plan = df2.clone().into_optimized_plan().unwrap();
        eprintln!("\nOptimized plan for reverse-order SELECT:\n{plan}\n");
        let r2: usize = df2
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        eprintln!("Reverse-order SELECT (col_c, col_b): {r2} rows");

        // Case 3: Single column
        let t3 =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .unwrap();
        let ctx3 = datafusion::prelude::SessionContext::new();
        ctx3.register_table("t", Arc::new(t3)).unwrap();
        let r3: usize = ctx3
            .sql("SELECT col_c FROM t LIMIT 5")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        eprintln!("Single column SELECT (col_c): {r3} rows");

        // Case 4: Reverse + ORDER BY
        let t4 =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let ctx4 = datafusion::prelude::SessionContext::new();
        ctx4.register_table("t", Arc::new(t4)).unwrap();
        let r4: usize = ctx4
            .sql("SELECT col_c, col_b FROM t ORDER BY col_b LIMIT 5")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        eprintln!("Reverse + ORDER BY: {r4} rows");

        eprintln!("\n--- Summary ---");
        eprintln!(
            "Schema-order (col_b, col_c) LIMIT 5: {r1} rows {}",
            if r1 == 5 { "✓" } else { "✗ BUG" }
        );
        eprintln!(
            "Reverse-order (col_c, col_b) LIMIT 5: {r2} rows {}",
            if r2 == 5 { "✓" } else { "✗ BUG" }
        );
        eprintln!(
            "Single column (col_c) LIMIT 5: {r3} rows {}",
            if r3 == 5 { "✓" } else { "✗ BUG" }
        );
        eprintln!(
            "Reverse + ORDER BY LIMIT 5: {r4} rows {}",
            if r4 == 5 { "✓" } else { "✗ BUG" }
        );

        assert_eq!(r1, 5);
        assert_eq!(
            r2, 20,
            "If DataFusion fixes this, update the assertion to 5 and remove the workaround"
        );
        assert_eq!(r3, 5);
        assert_eq!(r4, 5);
    }

    #[tokio::test]
    async fn datafusion53_bug_repro_schema_order_limit_works() {
        // SELECT columns in the SAME order as the schema → LIMIT works.
        // This is the control case proving DataFusion's LIMIT works normally.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Float64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("a-{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..20).map(|i| i as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("c-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("t", Arc::new(table)).unwrap();

        // Schema order: col_b then col_c (indices 1, 2)
        let rows: usize = ctx
            .sql("SELECT col_b, col_c FROM t LIMIT 7")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 7, "Schema-order SELECT LIMIT must work: got {rows}");
    }

    #[tokio::test]
    async fn datafusion53_bug_repro_reverse_order_limit_broken() {
        // SELECT columns in REVERSE order vs schema → LIMIT is ignored
        // in raw DataFusion 53. Our workaround (project_batch_to_sql_columns)
        // fixes this by reordering the MemTable schema before registration.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Float64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("a-{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..20).map(|i| i as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("c-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        // Raw DataFusion (no workaround) with reverse-order SELECT
        let table_raw =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .unwrap();
        let ctx_raw = datafusion::prelude::SessionContext::new();
        ctx_raw.register_table("t", Arc::new(table_raw)).unwrap();
        let raw_rows: usize = ctx_raw
            .sql("SELECT col_c, col_b FROM t LIMIT 7")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        // This documents the bug: raw DataFusion returns all 20 rows
        assert_eq!(
            raw_rows, 20,
            "DataFusion 53 bug: reverse-order SELECT ignores LIMIT, got {raw_rows} (expected 20 to prove bug exists)"
        );

        // With our workaround via execute_sql_batches
        let plan = super::super::planner::plan_sql("test", "SELECT col_c, col_b FROM test LIMIT 7")
            .unwrap();
        // Re-create batch with proper schema for execute_sql_batches
        // (it expects _id and score columns)
        let workaround_schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("col_b", DataType::Float64, true),
            Field::new("col_c", DataType::Utf8, true),
        ]));
        let workaround_batch = RecordBatch::try_new(
            workaround_schema,
            vec![
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("id-{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                    0.0f32;
                    20
                ])),
                Arc::new(Float64Array::from(
                    (0..20).map(|i| i as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("c-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let result = execute_sql_batches(&plan, vec![workaround_batch])
            .await
            .unwrap();
        assert_eq!(
            result.rows.len(),
            7,
            "Workaround must fix LIMIT: expected 7 rows, got {}",
            result.rows.len()
        );
    }

    #[test]
    fn project_batch_to_sql_columns_reorders_correctly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("fare", DataType::Float64, true),
            Field::new("lic", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id1"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![1.0f32])),
                Arc::new(Float64Array::from(vec![42.0])),
                Arc::new(StringArray::from(vec!["HV"])),
            ],
        )
        .unwrap();

        // SQL selects lic, fare (reverse of schema positions 2, 3)
        let plan =
            super::super::planner::plan_sql("test", "SELECT lic, fare FROM test LIMIT 5").unwrap();
        let (projected, new_schema) = project_batch_to_sql_columns(&batch, &plan);

        // Should have columns in SELECT order: lic, fare (not _id, score)
        assert_eq!(new_schema.fields().len(), 2);
        assert_eq!(new_schema.field(0).name(), "lic");
        assert_eq!(new_schema.field(1).name(), "fare");
        assert_eq!(projected.num_rows(), 1);
    }

    #[test]
    fn project_batch_to_sql_columns_includes_order_by_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("fare", DataType::Float64, true),
            Field::new("lic", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id1"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![1.0f32])),
                Arc::new(Float64Array::from(vec![42.0])),
                Arc::new(StringArray::from(vec!["HV"])),
            ],
        )
        .unwrap();

        // SQL selects lic but orders by fare — fare must be included
        let plan = super::super::planner::plan_sql(
            "test",
            "SELECT lic FROM test ORDER BY fare DESC LIMIT 5",
        )
        .unwrap();
        let (_, new_schema) = project_batch_to_sql_columns(&batch, &plan);

        assert_eq!(new_schema.fields().len(), 2);
        assert_eq!(new_schema.field(0).name(), "lic"); // SELECT column first
        assert_eq!(new_schema.field(1).name(), "fare"); // ORDER BY column after
    }

    #[test]
    fn project_batch_preserves_all_for_select_star() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();

        let plan = super::super::planner::plan_sql("test", "SELECT * FROM test LIMIT 5").unwrap();
        let (_, new_schema) = project_batch_to_sql_columns(&batch, &plan);
        // SELECT * should return the original schema unchanged
        assert_eq!(new_schema.fields().len(), 2);
        assert_eq!(new_schema.field(0).name(), "a");
        assert_eq!(new_schema.field(1).name(), "b");
    }

    #[test]
    fn project_batch_to_sql_columns_keeps_case_dependencies() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("author", DataType::Utf8, true),
            Field::new("upvotes", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id1"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![0.0f32])),
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
        )
        .unwrap();

        let plan = super::super::planner::plan_sql(
            "test",
            "SELECT author, SUM(CASE WHEN upvotes < 300 THEN upvotes ELSE 0 END) AS nonviral_upvotes FROM test GROUP BY author ORDER BY nonviral_upvotes DESC LIMIT 5",
        )
        .unwrap();
        let (_, new_schema) = project_batch_to_sql_columns(&batch, &plan);

        assert_eq!(new_schema.fields().len(), 2);
        assert_eq!(new_schema.field(0).name(), "author");
        assert_eq!(new_schema.field(1).name(), "upvotes");
    }

    #[tokio::test]
    async fn execute_sql_batches_handles_case_aggregate_dependencies() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("author", DataType::Utf8, true),
            Field::new("upvotes", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id1", "id2", "id3", "id4", "id5"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![
                    0.0f32;
                    5
                ])),
                Arc::new(StringArray::from(vec![
                    "alice", "alice", "bob", "bob", "carol",
                ])),
                Arc::new(Float64Array::from(vec![100.0, 400.0, 50.0, 200.0, 500.0])),
            ],
        )
        .unwrap();

        let plan = super::super::planner::plan_sql(
            "test",
            "SELECT author, COUNT(*) AS posts, SUM(CASE WHEN upvotes < 300 THEN upvotes ELSE 0 END) AS nonviral_upvotes, SUM(CASE WHEN upvotes < 300 THEN 1 ELSE 0 END) AS nonviral_posts, ROUND(SUM(CASE WHEN upvotes < 300 THEN upvotes ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN upvotes < 300 THEN 1 ELSE 0 END), 0), 1) AS avg_nonviral FROM test GROUP BY author HAVING COUNT(*) >= 2 ORDER BY avg_nonviral DESC, author ASC LIMIT 2",
        )
        .unwrap();

        let result = execute_sql_batches(&plan, vec![batch]).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["author"], json!("bob"));
        assert_eq!(result.rows[0]["posts"], json!(2));
        assert_eq!(result.rows[0]["nonviral_upvotes"], json!(250.0));
        assert_eq!(result.rows[0]["nonviral_posts"], json!(2));
        assert_eq!(result.rows[0]["avg_nonviral"], json!(125.0));
        assert_eq!(result.rows[1]["author"], json!("alice"));
        assert_eq!(result.rows[1]["avg_nonviral"], json!(100.0));
    }
}
