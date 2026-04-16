//! Shared types and utilities.
//! Long-term goal: Shared types, utilities, and error handling for the entire node.

pub mod date;
pub mod sql_parse;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// Validates that an index name is safe and well-formed.
/// Prevents path traversal attacks and rejects names that would cause filesystem issues.
fn validate_index_name(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("Index name must not be empty");
    }
    if name.len() > 255 {
        return Err("Index name must not exceed 255 characters");
    }
    if name.starts_with('.') || name.starts_with('_') {
        return Err("Index name must not start with '.' or '_'");
    }
    // Only allow lowercase alphanumeric, hyphens, and underscores
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(
            "Index name must only contain lowercase letters, digits, hyphens, or underscores",
        );
    }
    Ok(())
}

/// A validated index name.
///
/// Guarantees at construction time that the name is non-empty, <= 255 chars,
/// doesn't start with `.` or `_`, and contains only lowercase alphanumeric,
/// hyphens, or underscores. Invalid names cannot exist.
///
/// Use [`IndexName::new()`] for programmatic construction or extract directly
/// from Axum `Path<IndexName>` for HTTP handlers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexName(String);

impl IndexName {
    /// Create a validated index name.
    ///
    /// Returns `Err` with a human-readable message if the name is invalid.
    pub fn new(name: impl Into<String>) -> std::result::Result<Self, &'static str> {
        let name = name.into();
        validate_index_name(&name)?;
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IndexName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for IndexName {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for IndexName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<IndexName> for String {
    fn from(name: IndexName) -> String {
        name.0
    }
}

impl<'de> serde::Deserialize<'de> for IndexName {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        IndexName::new(s).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for IndexName {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

// ─── Dynamic Mapping: Type Inference ────────────────────────────────────────

use crate::cluster::state::{FieldMapping, FieldType};
use std::collections::{BTreeSet, HashMap};

/// Infer the `FieldMapping` for a single JSON value.
///
/// Returns `None` for null, arrays, and objects (nested types are not supported).
/// Strings are tested for ISO 8601 date format first; if the parse succeeds the
/// field is mapped as `Date`, otherwise `Text`.
pub fn infer_field_type(value: &serde_json::Value) -> Option<FieldMapping> {
    match value {
        serde_json::Value::Bool(_) => Some(FieldMapping {
            field_type: FieldType::Boolean,
            dimension: None,
        }),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                Some(FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                })
            } else {
                Some(FieldMapping {
                    field_type: FieldType::Float,
                    dimension: None,
                })
            }
        }
        serde_json::Value::String(s) => {
            if date::parse_iso8601_to_epoch_millis(s).is_some() {
                Some(FieldMapping {
                    field_type: FieldType::Date,
                    dimension: None,
                })
            } else {
                Some(FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                })
            }
        }
        _ => None, // null, array, object → skip
    }
}

/// Scan a JSON document object and return inferred mappings for every top-level
/// field. Fields named `_id` are excluded (reserved).
pub fn infer_field_mappings(payload: &serde_json::Value) -> HashMap<String, FieldMapping> {
    let mut mappings = HashMap::new();
    if let Some(obj) = payload.as_object() {
        for (key, value) in obj {
            if key == "_id" {
                continue;
            }
            if let Some(mapping) = infer_field_type(value) {
                mappings.insert(key.clone(), mapping);
            }
        }
    }
    mappings
}

/// Return only the fields from `payload` that are not already present in
/// `existing_mappings`. This is the set of new fields that dynamic mapping
/// needs to register.
pub fn detect_new_fields(
    payload: &serde_json::Value,
    existing_mappings: &HashMap<String, FieldMapping>,
) -> HashMap<String, FieldMapping> {
    infer_field_mappings(payload)
        .into_iter()
        .filter(|(name, _)| !existing_mappings.contains_key(name))
        .collect()
}

/// Batch version of [`detect_new_fields`] — scans multiple documents and
/// returns the union of all newly discovered fields (first-seen type wins).
pub fn detect_new_fields_batch(
    payloads: &[(String, serde_json::Value)],
    existing_mappings: &HashMap<String, FieldMapping>,
) -> HashMap<String, FieldMapping> {
    let mut new_fields: HashMap<String, FieldMapping> = HashMap::new();
    for (_, payload) in payloads {
        if let Some(obj) = payload.as_object() {
            for (key, value) in obj {
                if key == "_id"
                    || existing_mappings.contains_key(key)
                    || new_fields.contains_key(key)
                {
                    continue;
                }
                if let Some(mapping) = infer_field_type(value) {
                    new_fields.insert(key.clone(), mapping);
                }
            }
        }
    }
    new_fields
}

/// Return the set of top-level field names that are not already present in
/// `existing_mappings`, regardless of whether their values are inferable.
///
/// Used by `dynamic: strict` so arrays/objects/nulls are still rejected as
/// unknown fields instead of being silently skipped by type inference.
pub fn detect_unknown_fields(
    payload: &serde_json::Value,
    existing_mappings: &HashMap<String, FieldMapping>,
) -> Vec<String> {
    let Some(obj) = payload.as_object() else {
        return Vec::new();
    };

    let mut unknown: Vec<String> = obj
        .keys()
        .filter(|key| key.as_str() != "_id" && !existing_mappings.contains_key(*key))
        .cloned()
        .collect();
    unknown.sort();
    unknown
}

/// Batch version of [`detect_unknown_fields`]. Returns the sorted union of all
/// unknown top-level field names across the batch.
pub fn detect_unknown_fields_batch(
    payloads: &[(String, serde_json::Value)],
    existing_mappings: &HashMap<String, FieldMapping>,
) -> Vec<String> {
    let mut unknown = BTreeSet::new();
    for (_, payload) in payloads {
        if let Some(obj) = payload.as_object() {
            for key in obj.keys() {
                if key != "_id" && !existing_mappings.contains_key(key) {
                    unknown.insert(key.clone());
                }
            }
        }
    }
    unknown.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_index_names() {
        assert!(IndexName::new("my-index").is_ok());
        assert!(IndexName::new("logs-2024").is_ok());
        assert!(IndexName::new("a").is_ok());
        assert!(IndexName::new("test_index_123").is_ok());
    }

    #[test]
    fn empty_name_rejected() {
        assert!(IndexName::new("").is_err());
    }

    #[test]
    fn too_long_name_rejected() {
        let long_name = "a".repeat(256);
        assert!(IndexName::new(&long_name).is_err());
    }

    #[test]
    fn dot_prefix_rejected() {
        assert!(IndexName::new(".hidden").is_err());
    }

    #[test]
    fn underscore_prefix_rejected() {
        assert!(IndexName::new("_internal").is_err());
    }

    #[test]
    fn uppercase_rejected() {
        assert!(IndexName::new("MyIndex").is_err());
    }

    #[test]
    fn spaces_rejected() {
        assert!(IndexName::new("my index").is_err());
    }

    #[test]
    fn special_chars_rejected() {
        assert!(IndexName::new("index/name").is_err());
        assert!(IndexName::new("index..name").is_err());
        assert!(IndexName::new("index@name").is_err());
    }

    #[test]
    fn max_length_accepted() {
        let name = "a".repeat(255);
        assert!(IndexName::new(&name).is_ok());
    }

    #[test]
    fn index_name_deref_and_display() {
        let name = IndexName::new("test-idx").unwrap();
        assert_eq!(name.as_str(), "test-idx");
        assert_eq!(format!("{}", name), "test-idx");
        let s: &str = &name;
        assert_eq!(s, "test-idx");
    }

    #[test]
    fn index_name_into_string() {
        let name = IndexName::new("test-idx").unwrap();
        let s: String = name.into();
        assert_eq!(s, "test-idx");
    }

    #[test]
    fn index_name_serde_roundtrip() {
        let name = IndexName::new("test-idx").unwrap();
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"test-idx\"");
        let deserialized: IndexName = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.as_str(), "test-idx");
    }

    #[test]
    fn index_name_serde_rejects_invalid() {
        let result: std::result::Result<IndexName, _> = serde_json::from_str("\"MyIndex\"");
        assert!(result.is_err());
    }

    // ── Type inference tests ───────────────────────────────────────────

    use crate::cluster::state::{FieldMapping, FieldType};
    use serde_json::json;

    #[test]
    fn infer_bool_value() {
        let m = infer_field_type(&json!(true)).unwrap();
        assert_eq!(m.field_type, FieldType::Boolean);
        assert!(m.dimension.is_none());

        let m = infer_field_type(&json!(false)).unwrap();
        assert_eq!(m.field_type, FieldType::Boolean);
    }

    #[test]
    fn infer_integer_value() {
        let m = infer_field_type(&json!(42)).unwrap();
        assert_eq!(m.field_type, FieldType::Integer);

        let m = infer_field_type(&json!(-100)).unwrap();
        assert_eq!(m.field_type, FieldType::Integer);

        let m = infer_field_type(&json!(0)).unwrap();
        assert_eq!(m.field_type, FieldType::Integer);
    }

    #[test]
    fn infer_float_value() {
        let m = infer_field_type(&json!(3.15)).unwrap();
        assert_eq!(m.field_type, FieldType::Float);

        let m = infer_field_type(&json!(-0.001)).unwrap();
        assert_eq!(m.field_type, FieldType::Float);
    }

    #[test]
    fn infer_text_string() {
        let m = infer_field_type(&json!("hello world")).unwrap();
        assert_eq!(m.field_type, FieldType::Text);
    }

    #[test]
    fn infer_date_string() {
        let m = infer_field_type(&json!("2024-01-15T10:30:00Z")).unwrap();
        assert_eq!(m.field_type, FieldType::Date);

        let m = infer_field_type(&json!("2024-01-15")).unwrap();
        assert_eq!(m.field_type, FieldType::Date);
    }

    #[test]
    fn infer_null_returns_none() {
        assert!(infer_field_type(&json!(null)).is_none());
    }

    #[test]
    fn infer_array_returns_none() {
        assert!(infer_field_type(&json!([1, 2, 3])).is_none());
    }

    #[test]
    fn infer_object_returns_none() {
        assert!(infer_field_type(&json!({"nested": "value"})).is_none());
    }

    #[test]
    fn infer_field_mappings_full_doc() {
        let doc = json!({
            "title": "hello",
            "count": 42,
            "price": 9.99,
            "active": true,
            "created": "2024-06-01T00:00:00Z",
            "_id": "should-be-skipped",
            "tags": ["a", "b"],
            "nested": {"x": 1}
        });
        let m = infer_field_mappings(&doc);
        assert_eq!(m.len(), 5);
        assert_eq!(m["title"].field_type, FieldType::Text);
        assert_eq!(m["count"].field_type, FieldType::Integer);
        assert_eq!(m["price"].field_type, FieldType::Float);
        assert_eq!(m["active"].field_type, FieldType::Boolean);
        assert_eq!(m["created"].field_type, FieldType::Date);
        assert!(!m.contains_key("_id"));
        assert!(!m.contains_key("tags"));
        assert!(!m.contains_key("nested"));
    }

    #[test]
    fn infer_field_mappings_empty_doc() {
        assert!(infer_field_mappings(&json!({})).is_empty());
    }

    #[test]
    fn infer_field_mappings_non_object() {
        assert!(infer_field_mappings(&json!("not an object")).is_empty());
    }

    #[test]
    fn detect_new_fields_filters_existing() {
        let existing = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let doc = json!({"title": "old", "count": 5, "status": true});
        let new = detect_new_fields(&doc, &existing);
        assert_eq!(new.len(), 2);
        assert!(new.contains_key("count"));
        assert!(new.contains_key("status"));
        assert!(!new.contains_key("title"));
    }

    #[test]
    fn detect_new_fields_returns_empty_when_all_known() {
        let existing = HashMap::from([
            (
                "a".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            ),
            (
                "b".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
        ]);
        let doc = json!({"a": "x", "b": 1});
        assert!(detect_new_fields(&doc, &existing).is_empty());
    }

    #[test]
    fn detect_new_fields_batch_union_of_docs() {
        let existing = HashMap::new();
        let docs = vec![
            ("1".into(), json!({"title": "hello", "count": 1})),
            ("2".into(), json!({"title": "world", "price": 9.99})),
        ];
        let new = detect_new_fields_batch(&docs, &existing);
        assert_eq!(new.len(), 3);
        assert_eq!(new["title"].field_type, FieldType::Text);
        assert_eq!(new["count"].field_type, FieldType::Integer);
        assert_eq!(new["price"].field_type, FieldType::Float);
    }

    #[test]
    fn detect_new_fields_batch_first_type_wins() {
        let existing = HashMap::new();
        // First doc: "value" is an integer. Second doc: "value" is a float.
        // First-seen type (integer) should win.
        let docs = vec![
            ("1".into(), json!({"value": 42})),
            ("2".into(), json!({"value": 3.15})),
        ];
        let new = detect_new_fields_batch(&docs, &existing);
        assert_eq!(new["value"].field_type, FieldType::Integer);
    }

    #[test]
    fn detect_new_fields_batch_skips_existing() {
        let existing = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        let docs = vec![("1".into(), json!({"title": "a", "new_field": 1}))];
        let new = detect_new_fields_batch(&docs, &existing);
        assert_eq!(new.len(), 1);
        assert!(new.contains_key("new_field"));
        assert!(!new.contains_key("title"));
    }

    #[test]
    fn detect_new_fields_batch_skips_id_field() {
        let docs = vec![("1".into(), json!({"_id": "custom", "name": "test"}))];
        let new = detect_new_fields_batch(&docs, &HashMap::new());
        assert!(!new.contains_key("_id"));
        assert!(new.contains_key("name"));
    }

    #[test]
    fn detect_unknown_fields_includes_non_inferable_values() {
        let existing = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let doc = json!({
            "title": "known",
            "metadata": {"nested": true},
            "tags": ["a", "b"],
            "missing_scalar": 42,
            "_id": "reserved"
        });

        let unknown = detect_unknown_fields(&doc, &existing);
        assert_eq!(unknown, vec!["metadata", "missing_scalar", "tags"]);
    }

    #[test]
    fn detect_unknown_fields_batch_unions_and_sorts_names() {
        let docs = vec![
            ("1".into(), json!({"payload": {"x": 1}, "title": "a"})),
            ("2".into(), json!({"tags": [1, 2], "title": "b"})),
            ("3".into(), json!({"payload": {"y": 2}, "count": 7})),
        ];

        let unknown = detect_unknown_fields_batch(&docs, &HashMap::new());
        assert_eq!(unknown, vec!["count", "payload", "tags", "title"]);
    }
}
