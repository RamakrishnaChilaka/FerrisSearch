use anyhow::Result;
use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

pub fn record_batches_to_json_rows(batches: &[RecordBatch]) -> Result<(Vec<String>, Vec<Value>)> {
    if batches.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let columns = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Map::new();
            for (column_idx, field) in batch.schema().fields().iter().enumerate() {
                let value = array_value_to_json(
                    batch.column(column_idx).as_ref(),
                    field.data_type(),
                    row_idx,
                )?;
                row.insert(field.name().to_string(), value);
            }
            rows.push(Value::Object(row));
        }
    }

    Ok((columns, rows))
}

fn downcast_array<'a, T: 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
    expected: &'static str,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "failed to downcast Arrow array for {:?}: expected {} backing array, got {:?}",
            data_type,
            expected,
            array.data_type()
        )
    })
}

fn array_value_to_json(array: &dyn Array, data_type: &DataType, row_idx: usize) -> Result<Value> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }

    let value = match data_type {
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, data_type, "StringArray")?;
            Value::String(array.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, data_type, "LargeStringArray")?;
            Value::String(array.value(row_idx).to_string())
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, data_type, "Float64Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, data_type, "Float32Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array, data_type, "Int64Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array, data_type, "Int32Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array, data_type, "UInt64Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array, data_type, "UInt32Array")?;
            serde_json::json!(array.value(row_idx))
        }
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array, data_type, "BooleanArray")?;
            serde_json::json!(array.value(row_idx))
        }
        _ => Value::String(format!("{:?}", data_type)),
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn record_batches_to_json_rows_serializes_supported_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42, 7])) as ArrayRef,
            ],
        )
        .unwrap();

        let (columns, rows) = record_batches_to_json_rows(&[batch]).unwrap();

        assert_eq!(columns, vec!["name", "age"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], Value::String("alice".into()));
        assert_eq!(rows[1]["age"], serde_json::json!(7));
    }

    #[test]
    fn array_value_to_json_returns_error_for_mismatched_backing_array() {
        let array = Int64Array::from(vec![1]);
        let err = array_value_to_json(&array, &DataType::Utf8, 0).unwrap_err();

        assert!(err.to_string().contains("failed to downcast Arrow array"));
        assert!(err.to_string().contains("Utf8"));
    }
}
