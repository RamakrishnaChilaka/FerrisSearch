use super::SqlQueryResult;
use super::merge::record_batches_to_json_rows;
use super::planner::QueryPlan;
use anyhow::Result;
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
        Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
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
    let table = MemTable::try_new(schema, vec![batches])?;

    let ctx = SessionContext::new();
    ctx.register_table("matched_rows", Arc::new(table))?;

    let dataframe = ctx.sql(&plan.rewritten_sql).await?;
    let batches = dataframe.collect().await?;
    let (columns, rows) = record_batches_to_json_rows(&batches)?;

    Ok(SqlQueryResult { columns, rows })
}
