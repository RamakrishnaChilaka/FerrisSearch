//! Search query DSL types.
//! Supports a subset of the OpenSearch Query DSL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level search request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: QueryClause,
    #[serde(default = "default_size")]
    pub size: usize,
}

fn default_size() -> usize {
    10
}

/// A query clause: term, match, or match_all.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryClause {
    /// Exact term match: `{ "term": { "field": "value" } }`
    Term(HashMap<String, serde_json::Value>),
    /// Full-text match: `{ "match": { "field": "text" } }`
    Match(HashMap<String, serde_json::Value>),
    /// Match all documents.
    MatchAll(serde_json::Value),
}
