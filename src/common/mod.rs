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
