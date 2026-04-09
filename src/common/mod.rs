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
}
