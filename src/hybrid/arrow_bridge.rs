use super::column_store::ColumnStore;
use anyhow::{Result, bail};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Array, Float64Array, Float64Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Float64,
    Boolean,
    Utf8,
}

pub fn build_float64_array(values: &[Value]) -> Result<Float64Array> {
    let mut builder = Float64Builder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Number(number) => builder.append_value(number.as_f64().unwrap_or(0.0)),
            Value::Null => builder.append_null(),
            _ => bail!("Non-numeric value found in numeric column"),
        }
    }
    Ok(builder.finish())
}

pub fn build_score_array(scores: &[f32]) -> Float32Array {
    Float32Array::from(scores.to_vec())
}

pub fn build_record_batch(column_store: &ColumnStore) -> Result<RecordBatch> {
    build_record_batch_with_hints(column_store, &HashMap::new())
}

pub fn build_record_batch_with_hints(
    column_store: &ColumnStore,
    type_hints: &HashMap<String, ColumnKind>,
) -> Result<RecordBatch> {
    let mut fields = vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        build_id_array(column_store),
        Arc::new(build_score_array(column_store.scores())),
    ];

    for (name, values) in column_store.columns() {
        let kind = type_hints
            .get(name)
            .copied()
            .unwrap_or_else(|| infer_column_kind(values));
        fields.push(Field::new(name, data_type_for(kind), true));
        arrays.push(build_array(values, kind)?);
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_id_array(column_store: &ColumnStore) -> ArrayRef {
    let mut builder =
        StringBuilder::with_capacity(column_store.row_count(), column_store.row_count() * 8);
    for value in column_store.ids() {
        builder.append_value(value);
    }
    Arc::new(builder.finish())
}

fn infer_column_kind(values: &[Value]) -> ColumnKind {
    let mut kind = None;
    for value in values {
        match value {
            Value::Null => continue,
            Value::Bool(_) => {
                kind = Some(ColumnKind::Boolean);
                break;
            }
            Value::Number(_) => {
                kind = Some(ColumnKind::Float64);
                break;
            }
            _ => {
                kind = Some(ColumnKind::Utf8);
                break;
            }
        }
    }
    kind.unwrap_or(ColumnKind::Utf8)
}

fn data_type_for(kind: ColumnKind) -> DataType {
    match kind {
        ColumnKind::Float64 => DataType::Float64,
        ColumnKind::Boolean => DataType::Boolean,
        ColumnKind::Utf8 => DataType::Utf8,
    }
}

fn build_array(values: &[Value], kind: ColumnKind) -> Result<ArrayRef> {
    match kind {
        ColumnKind::Float64 => Ok(Arc::new(build_float64_array(values)?)),
        ColumnKind::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    Value::Bool(boolean) => builder.append_value(*boolean),
                    Value::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ColumnKind::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    Value::String(text) => builder.append_value(text),
                    Value::Null => builder.append_null(),
                    other => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}
