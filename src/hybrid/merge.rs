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
                );
                row.insert(field.name().to_string(), value);
            }
            rows.push(Value::Object(row));
        }
    }

    Ok((columns, rows))
}

fn array_value_to_json(array: &dyn Array, data_type: &DataType, row_idx: usize) -> Value {
    if array.is_null(row_idx) {
        return Value::Null;
    }

    match data_type {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 array");
            Value::String(array.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 array");
            Value::String(array.value(row_idx).to_string())
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::Int32 => {
            let array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::UInt32 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("uint32 array");
            serde_json::json!(array.value(row_idx))
        }
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool array");
            serde_json::json!(array.value(row_idx))
        }
        _ => Value::String(format!("{:?}", data_type)),
    }
}
