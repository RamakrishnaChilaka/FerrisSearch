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
    #[serde(default)]
    pub from: usize,
}

fn default_size() -> usize {
    10
}

/// A query clause: term, match, match_all, bool, or range.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryClause {
    /// Exact term match: `{ "term": { "field": "value" } }`
    Term(HashMap<String, serde_json::Value>),
    /// Full-text match: `{ "match": { "field": "text" } }`
    Match(HashMap<String, serde_json::Value>),
    /// Match all documents.
    MatchAll(serde_json::Value),
    /// Boolean query: `{ "bool": { "must": [...], "should": [...], "must_not": [...], "filter": [...] } }`
    Bool(BoolQuery),
    /// Range query: `{ "range": { "field": { "gte": 10, "lt": 100 } } }`
    Range(HashMap<String, RangeCondition>),
}

/// Range condition with optional gt/gte/lt/lte bounds.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RangeCondition {
    #[serde(default)]
    pub gt: Option<serde_json::Value>,
    #[serde(default)]
    pub gte: Option<serde_json::Value>,
    #[serde(default)]
    pub lt: Option<serde_json::Value>,
    #[serde(default)]
    pub lte: Option<serde_json::Value>,
}

/// Boolean query with must/should/must_not/filter clauses.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BoolQuery {
    #[serde(default)]
    pub must: Vec<QueryClause>,
    #[serde(default)]
    pub should: Vec<QueryClause>,
    #[serde(default)]
    pub must_not: Vec<QueryClause>,
    #[serde(default)]
    pub filter: Vec<QueryClause>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_match_all_query() {
        let body = json!({
            "query": { "match_all": {} },
            "size": 5
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(matches!(req.query, QueryClause::MatchAll(_)));
        assert_eq!(req.size, 5);
    }

    #[test]
    fn deserialize_match_query() {
        let body = json!({
            "query": { "match": { "title": "search engine" } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Match(fields) => {
                assert!(fields.contains_key("title"));
                assert_eq!(fields["title"], "search engine");
            }
            _ => panic!("expected Match query"),
        }
    }

    #[test]
    fn deserialize_term_query() {
        let body = json!({
            "query": { "term": { "status": "published" } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Term(fields) => {
                assert_eq!(fields["status"], "published");
            }
            _ => panic!("expected Term query"),
        }
    }

    #[test]
    fn default_size_is_10() {
        let body = json!({
            "query": { "match_all": {} }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.size, 10);
        assert_eq!(req.from, 0);
    }

    #[test]
    fn from_and_size_from_json() {
        let body = json!({
            "query": { "match_all": {} },
            "from": 20,
            "size": 5
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.from, 20);
        assert_eq!(req.size, 5);
    }

    #[test]
    fn roundtrip_serialize_deserialize() {
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 20,
            from: 5,
        };
        let json_str = serde_json::to_string(&req).unwrap();
        let req2: SearchRequest = serde_json::from_str(&json_str).unwrap();
        assert_eq!(req2.size, 20);
        assert_eq!(req2.from, 5);
        assert!(matches!(req2.query, QueryClause::MatchAll(_)));
    }

    #[test]
    fn deserialize_bool_query_must() {
        let body = json!({
            "query": {
                "bool": {
                    "must": [
                        { "match": { "title": "search" } }
                    ]
                }
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert_eq!(bq.must.len(), 1);
                assert!(bq.should.is_empty());
                assert!(bq.must_not.is_empty());
                assert!(bq.filter.is_empty());
            }
            _ => panic!("expected Bool query"),
        }
    }

    #[test]
    fn deserialize_bool_query_all_clauses() {
        let body = json!({
            "query": {
                "bool": {
                    "must": [{ "match": { "title": "rust" } }],
                    "should": [{ "match": { "body": "search" } }],
                    "must_not": [{ "term": { "status": "draft" } }],
                    "filter": [{ "term": { "lang": "en" } }]
                }
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert_eq!(bq.must.len(), 1);
                assert_eq!(bq.should.len(), 1);
                assert_eq!(bq.must_not.len(), 1);
                assert_eq!(bq.filter.len(), 1);
            }
            _ => panic!("expected Bool query"),
        }
    }

    #[test]
    fn deserialize_bool_query_empty_clauses() {
        let body = json!({
            "query": { "bool": {} }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert!(bq.must.is_empty());
                assert!(bq.should.is_empty());
                assert!(bq.must_not.is_empty());
                assert!(bq.filter.is_empty());
            }
            _ => panic!("expected Bool query"),
        }
    }

    #[test]
    fn deserialize_nested_bool_query() {
        let body = json!({
            "query": {
                "bool": {
                    "must": [
                        { "bool": {
                            "should": [
                                { "match": { "title": "rust" } },
                                { "match": { "title": "python" } }
                            ]
                        }}
                    ],
                    "must_not": [{ "term": { "status": "draft" } }]
                }
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert_eq!(bq.must.len(), 1);
                assert_eq!(bq.must_not.len(), 1);
                match &bq.must[0] {
                    QueryClause::Bool(inner) => assert_eq!(inner.should.len(), 2),
                    _ => panic!("expected nested Bool"),
                }
            }
            _ => panic!("expected Bool query"),
        }
    }

    #[test]
    fn bool_query_roundtrip_serde() {
        let req = SearchRequest {
            query: QueryClause::Bool(BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                should: vec![],
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("status".into(), json!("draft"));
                    m
                })],
                filter: vec![],
            }),
            size: 5,
            from: 10,
        };
        let json_str = serde_json::to_string(&req).unwrap();
        let req2: SearchRequest = serde_json::from_str(&json_str).unwrap();
        assert_eq!(req2.size, 5);
        assert_eq!(req2.from, 10);
        match &req2.query {
            QueryClause::Bool(bq) => {
                assert_eq!(bq.must.len(), 1);
                assert_eq!(bq.must_not.len(), 1);
            }
            _ => panic!("expected Bool query after roundtrip"),
        }
    }

    #[test]
    fn deserialize_range_query_gte_lt() {
        let body = json!({
            "query": { "range": { "age": { "gte": 18, "lt": 65 } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Range(fields) => {
                let cond = fields.get("age").unwrap();
                assert_eq!(cond.gte, Some(json!(18)));
                assert_eq!(cond.lt, Some(json!(65)));
                assert!(cond.gt.is_none());
                assert!(cond.lte.is_none());
            }
            _ => panic!("expected Range query"),
        }
    }

    #[test]
    fn deserialize_range_query_gt_lte() {
        let body = json!({
            "query": { "range": { "price": { "gt": 0, "lte": 100 } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Range(fields) => {
                let cond = fields.get("price").unwrap();
                assert_eq!(cond.gt, Some(json!(0)));
                assert_eq!(cond.lte, Some(json!(100)));
            }
            _ => panic!("expected Range query"),
        }
    }

    #[test]
    fn deserialize_range_query_string_bounds() {
        let body = json!({
            "query": { "range": { "name": { "gte": "a", "lt": "m" } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Range(fields) => {
                let cond = fields.get("name").unwrap();
                assert_eq!(cond.gte, Some(json!("a")));
                assert_eq!(cond.lt, Some(json!("m")));
            }
            _ => panic!("expected Range query"),
        }
    }

    #[test]
    fn range_inside_bool_filter() {
        let body = json!({
            "query": {
                "bool": {
                    "must": [{ "match": { "title": "rust" } }],
                    "filter": [{ "range": { "year": { "gte": 2020, "lte": 2026 } } }]
                }
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert_eq!(bq.must.len(), 1);
                assert_eq!(bq.filter.len(), 1);
                assert!(matches!(&bq.filter[0], QueryClause::Range(_)));
            }
            _ => panic!("expected Bool query"),
        }
    }
}
