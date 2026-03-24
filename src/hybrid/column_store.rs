use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct ColumnStore {
    ids: Vec<String>,
    scores: Vec<f32>,
    columns: BTreeMap<String, Vec<Value>>,
}

impl ColumnStore {
    pub fn new(ids: Vec<String>, scores: Vec<f32>, columns: BTreeMap<String, Vec<Value>>) -> Self {
        Self {
            ids,
            scores,
            columns,
        }
    }

    pub fn from_hits(hits: &[Value], selected_columns: &[String]) -> Self {
        let field_names: BTreeSet<String> = if selected_columns.is_empty() {
            let mut inferred = BTreeSet::new();
            for hit in hits {
                if let Some(source) = hit.get("_source").and_then(|value| value.as_object()) {
                    inferred.extend(source.keys().cloned());
                }
            }
            inferred
        } else {
            selected_columns.iter().cloned().collect()
        };

        let row_count = hits.len();
        let mut columns = BTreeMap::new();
        for field in &field_names {
            columns.insert(field.clone(), Vec::with_capacity(row_count));
        }

        let mut ids = Vec::with_capacity(row_count);
        let mut scores = Vec::with_capacity(row_count);

        // CRITICAL DESIGN:
        // row_idx is the direct index in every materialized column vector
        // for the matched result set. After materialization, access remains
        // direct: column[row_idx] with no extra mapping or indirection.
        for hit in hits {
            ids.push(
                hit.get("_id")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            scores.push(
                hit.get("_score")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0) as f32,
            );

            let source = hit.get("_source").and_then(|value| value.as_object());
            for field in &field_names {
                let value = source
                    .and_then(|object| object.get(field))
                    .cloned()
                    .unwrap_or(Value::Null);
                columns
                    .get_mut(field)
                    .expect("column should exist")
                    .push(value);
            }
        }
        Self::new(ids, scores, columns)
    }

    pub fn row_count(&self) -> usize {
        self.ids.len()
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    pub fn scores(&self) -> &[f32] {
        &self.scores
    }

    pub fn columns(&self) -> &BTreeMap<String, Vec<Value>> {
        &self.columns
    }
}
