//! Shared types and utilities.
//! Long-term goal: Shared types, utilities, and error handling for the entire node.

pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// Validates that an index name is safe and well-formed.
/// Prevents path traversal attacks and rejects names that would cause filesystem issues.
pub fn validate_index_name(name: &str) -> std::result::Result<(), &'static str> {
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
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_') {
        return Err("Index name must only contain lowercase letters, digits, hyphens, or underscores");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_index_names() {
        assert!(validate_index_name("my-index").is_ok());
        assert!(validate_index_name("logs-2024").is_ok());
        assert!(validate_index_name("a").is_ok());
        assert!(validate_index_name("test_index_123").is_ok());
    }

    #[test]
    fn empty_name_rejected() {
        assert!(validate_index_name("").is_err());
    }

    #[test]
    fn too_long_name_rejected() {
        let long_name = "a".repeat(256);
        assert!(validate_index_name(&long_name).is_err());
    }

    #[test]
    fn dot_prefix_rejected() {
        assert!(validate_index_name(".hidden").is_err());
    }

    #[test]
    fn underscore_prefix_rejected() {
        assert!(validate_index_name("_internal").is_err());
    }

    #[test]
    fn uppercase_rejected() {
        assert!(validate_index_name("MyIndex").is_err());
    }

    #[test]
    fn spaces_rejected() {
        assert!(validate_index_name("my index").is_err());
    }

    #[test]
    fn special_chars_rejected() {
        assert!(validate_index_name("index/name").is_err());
        assert!(validate_index_name("index..name").is_err());
        assert!(validate_index_name("index@name").is_err());
    }

    #[test]
    fn max_length_accepted() {
        let name = "a".repeat(255);
        assert!(validate_index_name(&name).is_ok());
    }
}
