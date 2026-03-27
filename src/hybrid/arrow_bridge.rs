use super::column_store::ColumnStore;
use anyhow::{Result, bail};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Array, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Float64,
    Int64,
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

pub fn build_int64_array(values: &[Value]) -> Result<Int64Array> {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Number(number) => {
                if let Some(i) = number.as_i64() {
                    builder.append_value(i);
                } else {
                    builder.append_value(number.as_f64().unwrap_or(0.0) as i64);
                }
            }
            Value::Null => builder.append_null(),
            _ => bail!("Non-numeric value found in integer column"),
        }
    }
    Ok(builder.finish())
}

/// Derive Arrow column type from the index field mapping.
/// This ensures both fast-field and fallback paths agree on column types.
pub fn column_kind_from_field_type(ft: &crate::cluster::state::FieldType) -> ColumnKind {
    match ft {
        crate::cluster::state::FieldType::Float => ColumnKind::Float64,
        crate::cluster::state::FieldType::Integer => ColumnKind::Int64,
        crate::cluster::state::FieldType::Boolean => ColumnKind::Boolean,
        _ => ColumnKind::Utf8,
    }
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
    let row_count = column_store.row_count();
    let mut fields = vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ];

    let mut arrays: Vec<ArrayRef> = if column_store.ids().is_empty() && row_count > 0 {
        // _id not needed: emit a column of empty strings
        let mut builder = StringBuilder::with_capacity(row_count, 0);
        for _ in 0..row_count {
            builder.append_value("");
        }
        vec![Arc::new(builder.finish())]
    } else {
        vec![build_id_array(column_store)]
    };

    if column_store.scores().is_empty() && row_count > 0 {
        // score not needed: emit a column of zeros
        arrays.push(Arc::new(Float32Array::from(vec![0.0f32; row_count])));
    } else {
        arrays.push(Arc::new(build_score_array(column_store.scores())));
    };

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
            Value::Number(n) => {
                // Distinguish integers from floats based on the JSON number type
                if n.is_f64() && n.as_i64().is_none() {
                    kind = Some(ColumnKind::Float64);
                } else {
                    kind = Some(ColumnKind::Int64);
                }
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
        ColumnKind::Int64 => DataType::Int64,
        ColumnKind::Boolean => DataType::Boolean,
        ColumnKind::Utf8 => DataType::Utf8,
    }
}

fn build_array(values: &[Value], kind: ColumnKind) -> Result<ArrayRef> {
    match kind {
        ColumnKind::Float64 => Ok(Arc::new(build_float64_array(values)?)),
        ColumnKind::Int64 => Ok(Arc::new(build_int64_array(values)?)),
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

/// Serialize a RecordBatch to Arrow IPC stream format bytes.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer =
            datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize a RecordBatch from Arrow IPC stream format bytes.
pub fn record_batch_from_ipc(bytes: &[u8]) -> Result<RecordBatch> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("Arrow IPC stream contained no batches"))?
        .map_err(|e| anyhow::anyhow!("Arrow IPC deserialization error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid::column_store::ColumnStore;
    use std::collections::BTreeMap;

    #[test]
    fn arrow_ipc_round_trip_preserves_data() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let scores = vec![1.5f32, 2.0];
        let mut cols = BTreeMap::new();
        cols.insert(
            "price".to_string(),
            vec![Value::from(10.5), Value::from(20.0)],
        );
        cols.insert(
            "name".to_string(),
            vec![
                Value::String("widget".to_string()),
                Value::String("gadget".to_string()),
            ],
        );
        let store = ColumnStore::new(ids, scores, cols);
        let batch = build_record_batch(&store).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        assert!(!ipc_bytes.is_empty());

        let restored = record_batch_from_ipc(&ipc_bytes).unwrap();
        assert_eq!(restored.num_rows(), 2);
        assert_eq!(restored.num_columns(), batch.num_columns());

        // Verify column names match
        let original_schema = batch.schema();
        let restored_schema = restored.schema();
        let original_names: Vec<&str> = original_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let restored_names: Vec<&str> = restored_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(original_names, restored_names);
    }

    #[test]
    fn arrow_ipc_round_trip_empty_batch() {
        let store = ColumnStore::new(vec![], vec![], BTreeMap::new());
        let batch = build_record_batch(&store).unwrap();
        assert_eq!(batch.num_rows(), 0);

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let restored = record_batch_from_ipc(&ipc_bytes).unwrap();
        assert_eq!(restored.num_rows(), 0);
    }

    #[test]
    fn arrow_ipc_round_trip_with_type_hints() {
        let ids = vec!["x".to_string()];
        let scores = vec![0.0f32];
        let mut cols = BTreeMap::new();
        cols.insert("amount".to_string(), vec![Value::from(42.5)]);
        let store = ColumnStore::new(ids, scores, cols);

        let mut hints = HashMap::new();
        hints.insert("amount".to_string(), ColumnKind::Float64);
        let batch = build_record_batch_with_hints(&store, &hints).unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let restored = record_batch_from_ipc(&ipc_bytes).unwrap();

        // Verify Float64 type is preserved
        let restored_schema = restored.schema();
        let field = restored_schema.field_with_name("amount").unwrap();
        assert_eq!(*field.data_type(), DataType::Float64);
    }

    #[test]
    fn arrow_ipc_round_trip_skip_id_score() {
        // Empty ids/scores but data present — simulates needs_id=false, needs_score=false
        let mut cols = BTreeMap::new();
        cols.insert(
            "category".to_string(),
            vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ],
        );
        let store = ColumnStore::new(vec![], vec![], cols);
        let batch = build_record_batch(&store).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let restored = record_batch_from_ipc(&ipc_bytes).unwrap();
        assert_eq!(restored.num_rows(), 2);
    }

    #[test]
    fn int64_hint_produces_int64_arrow_column() {
        let ids = vec!["a".to_string()];
        let scores = vec![0.0f32];
        let mut cols = BTreeMap::new();
        cols.insert("count".to_string(), vec![Value::from(42)]);
        let store = ColumnStore::new(ids, scores, cols);

        let mut hints = HashMap::new();
        hints.insert("count".to_string(), ColumnKind::Int64);
        let batch = build_record_batch_with_hints(&store, &hints).unwrap();

        let schema = batch.schema();
        let field = schema.field_with_name("count").unwrap();
        assert_eq!(
            *field.data_type(),
            DataType::Int64,
            "integer column should be Int64"
        );

        let array = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array.value(0), 42);
    }

    #[test]
    fn float64_hint_produces_float64_arrow_column() {
        let ids = vec!["a".to_string()];
        let scores = vec![0.0f32];
        let mut cols = BTreeMap::new();
        cols.insert("price".to_string(), vec![Value::from(42.5)]);
        let store = ColumnStore::new(ids, scores, cols);

        let mut hints = HashMap::new();
        hints.insert("price".to_string(), ColumnKind::Float64);
        let batch = build_record_batch_with_hints(&store, &hints).unwrap();

        let schema = batch.schema();
        let field = schema.field_with_name("price").unwrap();
        assert_eq!(*field.data_type(), DataType::Float64);
    }

    #[test]
    fn infer_integer_json_as_int64() {
        // JSON integer `42` should infer as Int64, not Float64
        let values = vec![Value::from(42), Value::from(100)];
        let kind = infer_column_kind(&values);
        assert_eq!(kind, ColumnKind::Int64);
    }

    #[test]
    fn infer_float_json_as_float64() {
        // JSON float `42.5` should infer as Float64
        let values = vec![Value::from(42.5), Value::from(1.0)];
        let kind = infer_column_kind(&values);
        assert_eq!(kind, ColumnKind::Float64);
    }

    #[test]
    fn column_kind_from_field_type_maps_correctly() {
        use crate::cluster::state::FieldType;
        assert_eq!(
            column_kind_from_field_type(&FieldType::Integer),
            ColumnKind::Int64
        );
        assert_eq!(
            column_kind_from_field_type(&FieldType::Float),
            ColumnKind::Float64
        );
        assert_eq!(
            column_kind_from_field_type(&FieldType::Boolean),
            ColumnKind::Boolean
        );
        assert_eq!(
            column_kind_from_field_type(&FieldType::Text),
            ColumnKind::Utf8
        );
        assert_eq!(
            column_kind_from_field_type(&FieldType::Keyword),
            ColumnKind::Utf8
        );
    }

    #[test]
    fn int64_round_trip_preserves_values() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let scores = vec![0.0f32, 0.0];
        let mut cols = BTreeMap::new();
        cols.insert("age".to_string(), vec![Value::from(25), Value::from(30)]);
        let store = ColumnStore::new(ids, scores, cols);

        let mut hints = HashMap::new();
        hints.insert("age".to_string(), ColumnKind::Int64);
        let batch = build_record_batch_with_hints(&store, &hints).unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let restored = record_batch_from_ipc(&ipc_bytes).unwrap();

        let schema = restored.schema();
        let field = schema.field_with_name("age").unwrap();
        assert_eq!(*field.data_type(), DataType::Int64);

        let array = restored
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array.value(0), 25);
        assert_eq!(array.value(1), 30);
    }
}
