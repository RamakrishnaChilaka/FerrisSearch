//! Search query DSL types.
//! Supports a subset of the OpenSearch Query DSL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level search request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    #[serde(default = "default_query")]
    pub query: QueryClause,
    #[serde(default = "default_size")]
    pub size: usize,
    #[serde(default)]
    pub from: usize,
    /// Optional k-NN vector search clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knn: Option<KnnQuery>,
    /// Optional sort clauses. Default: sort by `_score` descending.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<SortClause>,
    /// Optional aggregations. Maps agg name → agg definition.
    #[serde(
        default,
        alias = "aggregations",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub aggs: HashMap<String, AggregationRequest>,
}

/// A single sort clause. Supports field sort and `_score` sort.
///
/// OpenSearch-compatible formats:
/// - `"_score"` — sort by relevance score descending
/// - `{"year": "desc"}` — sort by field value
/// - `{"year": {"order": "asc"}}` — explicit order object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortClause {
    /// Simple string: `"_score"` or `"field_name"`
    Simple(String),
    /// Field with order: `{"year": "desc"}` or `{"year": {"order": "asc"}}`
    Field(HashMap<String, SortOrder>),
}

/// Sort order for a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortOrder {
    /// Simple string: `"asc"` or `"desc"`
    Direction(SortDirection),
    /// Object with order: `{"order": "asc"}`
    Object { order: SortDirection },
}

/// Sort direction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

fn default_query() -> QueryClause {
    QueryClause::MatchAll(serde_json::Value::Object(Default::default()))
}

fn default_size() -> usize {
    10
}

/// k-NN search clause: `{ "knn": { "field_name": { "vector": [...], "k": 10 } } }`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnnQuery {
    /// The field name containing the vector (flattened from the map).
    #[serde(flatten)]
    pub fields: HashMap<String, KnnParams>,
}

/// Parameters for a k-NN search on a single field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnnParams {
    /// Query vector.
    pub vector: Vec<f32>,
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// Optional pre-filter: only return neighbors matching this query clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<QueryClause>,
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
    /// Wildcard query: `{ "wildcard": { "field": "ru*t" } }` — `*` matches any chars, `?` matches one char
    Wildcard(HashMap<String, serde_json::Value>),
    /// Prefix query: `{ "prefix": { "field": "sea" } }` — matches terms starting with the value
    Prefix(HashMap<String, serde_json::Value>),
    /// Fuzzy query: `{ "fuzzy": { "field": { "value": "rsut", "fuzziness": 1 } } }`
    Fuzzy(HashMap<String, FuzzyParams>),
}

/// Fuzzy query parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuzzyParams {
    /// The term to search for (possibly misspelled).
    pub value: String,
    /// Maximum edit distance (default: 1). Valid values: 0, 1, 2.
    #[serde(default = "default_fuzziness")]
    pub fuzziness: u8,
}

fn default_fuzziness() -> u8 {
    1
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

// ── Aggregation types ───────────────────────────────────────────────────────

/// An aggregation request. Each variant maps to an OpenSearch aggregation type.
/// Designed so that each engine (shard, split, or segment) computes a partial
/// result, which is then merged at the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationRequest {
    /// Terms aggregation: top-N buckets by keyword value.
    /// `{ "terms": { "field": "genre", "size": 10 } }`
    Terms(TermsAggParams),
    /// Stats aggregation: min, max, sum, count, avg.
    /// `{ "stats": { "field": "rating" } }`
    Stats(MetricAggParams),
    /// Min aggregation.
    Min(MetricAggParams),
    /// Max aggregation.
    Max(MetricAggParams),
    /// Avg aggregation.
    Avg(MetricAggParams),
    /// Sum aggregation.
    Sum(MetricAggParams),
    /// Value count: count of non-null values.
    ValueCount(MetricAggParams),
    /// Histogram: fixed-interval numeric buckets.
    /// `{ "histogram": { "field": "year", "interval": 10 } }`
    Histogram(HistogramAggParams),
}

/// Parameters for a terms aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermsAggParams {
    pub field: String,
    #[serde(default = "default_terms_size")]
    pub size: usize,
}

fn default_terms_size() -> usize {
    10
}

/// Parameters for a metric aggregation (stats, min, max, avg, sum, value_count).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricAggParams {
    pub field: String,
}

/// Parameters for a histogram aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramAggParams {
    pub field: String,
    pub interval: f64,
}

/// A partial aggregation result from a single shard/split.
/// Designed to be mergeable: the coordinator combines partials from all shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PartialAggResult {
    /// Terms: map of value → doc_count (partial counts per shard)
    Terms { buckets: Vec<TermsBucket> },
    /// Stats: partial stats that can be combined (sum, count, min, max)
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    /// Single metric value
    Metric { value: Option<f64> },
    /// Histogram buckets: key → doc_count
    Histogram { buckets: Vec<HistogramBucket> },
}

/// A single bucket in a terms aggregation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermsBucket {
    pub key: String,
    pub doc_count: u64,
}

/// A single bucket in a histogram aggregation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub key: f64,
    pub doc_count: u64,
}

/// Compute aggregations from a list of search hits (JSON objects with `_source`).
/// This runs per-shard/per-split and produces partial results for coordinator merge.
pub fn compute_aggregations(
    hits: &[serde_json::Value],
    aggs: &HashMap<String, AggregationRequest>,
) -> HashMap<String, PartialAggResult> {
    let mut results = HashMap::new();

    for (name, agg_req) in aggs {
        let result = match agg_req {
            AggregationRequest::Terms(params) => compute_terms(hits, &params.field, params.size),
            AggregationRequest::Stats(params) => compute_stats(hits, &params.field),
            AggregationRequest::Min(params) => {
                let stats = compute_stats_raw(hits, &params.field);
                PartialAggResult::Metric {
                    value: if stats.count > 0 {
                        Some(stats.min)
                    } else {
                        None
                    },
                }
            }
            AggregationRequest::Max(params) => {
                let stats = compute_stats_raw(hits, &params.field);
                PartialAggResult::Metric {
                    value: if stats.count > 0 {
                        Some(stats.max)
                    } else {
                        None
                    },
                }
            }
            AggregationRequest::Avg(params) => {
                let stats = compute_stats_raw(hits, &params.field);
                PartialAggResult::Stats {
                    count: stats.count,
                    sum: stats.sum,
                    min: stats.min,
                    max: stats.max,
                }
            }
            AggregationRequest::Sum(params) => {
                let stats = compute_stats_raw(hits, &params.field);
                PartialAggResult::Metric {
                    value: Some(stats.sum),
                }
            }
            AggregationRequest::ValueCount(params) => {
                let count = hits
                    .iter()
                    .filter(|h| {
                        h.get("_source")
                            .and_then(|s| s.get(&params.field))
                            .is_some()
                    })
                    .count();
                PartialAggResult::Metric {
                    value: Some(count as f64),
                }
            }
            AggregationRequest::Histogram(params) => {
                compute_histogram(hits, &params.field, params.interval)
            }
        };
        results.insert(name.clone(), result);
    }

    results
}

struct RawStats {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

fn compute_stats_raw(hits: &[serde_json::Value], field: &str) -> RawStats {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;

    for hit in hits {
        if let Some(val) = hit
            .get("_source")
            .and_then(|s| s.get(field))
            .and_then(|v| v.as_f64())
        {
            count += 1;
            sum += val;
            if val < min {
                min = val;
            }
            if val > max {
                max = val;
            }
        }
    }

    RawStats {
        count,
        sum,
        min,
        max,
    }
}

fn compute_stats(hits: &[serde_json::Value], field: &str) -> PartialAggResult {
    let s = compute_stats_raw(hits, field);
    PartialAggResult::Stats {
        count: s.count,
        sum: s.sum,
        min: s.min,
        max: s.max,
    }
}

fn compute_terms(hits: &[serde_json::Value], field: &str, size: usize) -> PartialAggResult {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for hit in hits {
        if let Some(val) = hit.get("_source").and_then(|s| s.get(field)) {
            let key = match val {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            *counts.entry(key).or_insert(0) += 1;
        }
    }

    let mut buckets: Vec<TermsBucket> = counts
        .into_iter()
        .map(|(key, doc_count)| TermsBucket { key, doc_count })
        .collect();
    buckets.sort_by(|a, b| b.doc_count.cmp(&a.doc_count));
    buckets.truncate(size);

    PartialAggResult::Terms { buckets }
}

fn compute_histogram(hits: &[serde_json::Value], field: &str, interval: f64) -> PartialAggResult {
    let mut bucket_counts: HashMap<i64, u64> = HashMap::new();
    for hit in hits {
        if let Some(val) = hit
            .get("_source")
            .and_then(|s| s.get(field))
            .and_then(|v| v.as_f64())
        {
            let bucket_key = (val / interval).floor() as i64;
            *bucket_counts.entry(bucket_key).or_insert(0) += 1;
        }
    }

    let mut buckets: Vec<HistogramBucket> = bucket_counts
        .into_iter()
        .map(|(k, doc_count)| HistogramBucket {
            key: k as f64 * interval,
            doc_count,
        })
        .collect();
    buckets.sort_by(|a, b| {
        a.key
            .partial_cmp(&b.key)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    PartialAggResult::Histogram { buckets }
}

/// Merge partial aggregation results from multiple shards/splits.
/// This is the coordinator-level merge that combines partial agg results.
pub fn merge_aggregations(
    partials: Vec<HashMap<String, PartialAggResult>>,
    aggs: &HashMap<String, AggregationRequest>,
) -> HashMap<String, serde_json::Value> {
    let mut merged = HashMap::new();

    for (name, agg_req) in aggs {
        let parts: Vec<&PartialAggResult> = partials.iter().filter_map(|p| p.get(name)).collect();

        let result = match agg_req {
            AggregationRequest::Terms(params) => merge_terms(&parts, params.size),
            AggregationRequest::Stats(_) => merge_stats(&parts),
            AggregationRequest::Min(_) => merge_min(&parts),
            AggregationRequest::Max(_) => merge_max(&parts),
            AggregationRequest::Avg(_) => merge_avg(&parts),
            AggregationRequest::Sum(_) => merge_sum(&parts),
            AggregationRequest::ValueCount(_) => merge_value_count(&parts),
            AggregationRequest::Histogram(_) => merge_histogram(&parts),
        };

        merged.insert(name.clone(), result);
    }

    merged
}

fn merge_terms(parts: &[&PartialAggResult], size: usize) -> serde_json::Value {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for part in parts {
        if let PartialAggResult::Terms { buckets } = part {
            for b in buckets {
                *counts.entry(b.key.clone()).or_insert(0) += b.doc_count;
            }
        }
    }
    let mut buckets: Vec<serde_json::Value> = counts
        .into_iter()
        .map(|(key, doc_count)| serde_json::json!({"key": key, "doc_count": doc_count}))
        .collect();
    buckets.sort_by(|a, b| {
        let ca = a["doc_count"].as_u64().unwrap_or(0);
        let cb = b["doc_count"].as_u64().unwrap_or(0);
        cb.cmp(&ca)
    });
    buckets.truncate(size);
    serde_json::json!({"buckets": buckets})
}

fn merge_stats(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;

    for part in parts {
        if let PartialAggResult::Stats {
            count: c,
            sum: s,
            min: mn,
            max: mx,
        } = part
        {
            count += c;
            sum += s;
            if *mn < min {
                min = *mn;
            }
            if *mx > max {
                max = *mx;
            }
        }
    }

    let avg = if count > 0 { sum / count as f64 } else { 0.0 };
    serde_json::json!({
        "count": count,
        "min": if count > 0 { serde_json::json!(min) } else { serde_json::Value::Null },
        "max": if count > 0 { serde_json::json!(max) } else { serde_json::Value::Null },
        "avg": if count > 0 { serde_json::json!(avg) } else { serde_json::Value::Null },
        "sum": sum
    })
}

fn merge_min(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut min = f64::INFINITY;
    let mut found = false;
    for part in parts {
        match part {
            PartialAggResult::Metric { value: Some(v) } => {
                if *v < min {
                    min = *v;
                }
                found = true;
            }
            PartialAggResult::Stats { min: v, count, .. } if *count > 0 => {
                if *v < min {
                    min = *v;
                }
                found = true;
            }
            _ => {}
        }
    }
    serde_json::json!({"value": if found { serde_json::json!(min) } else { serde_json::Value::Null }})
}

fn merge_max(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut max = f64::NEG_INFINITY;
    let mut found = false;
    for part in parts {
        match part {
            PartialAggResult::Metric { value: Some(v) } => {
                if *v > max {
                    max = *v;
                }
                found = true;
            }
            PartialAggResult::Stats { max: v, count, .. } if *count > 0 => {
                if *v > max {
                    max = *v;
                }
                found = true;
            }
            _ => {}
        }
    }
    serde_json::json!({"value": if found { serde_json::json!(max) } else { serde_json::Value::Null }})
}

fn merge_avg(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    for part in parts {
        if let PartialAggResult::Stats {
            count: c, sum: s, ..
        } = part
        {
            count += c;
            sum += s;
        }
    }
    let avg = if count > 0 { sum / count as f64 } else { 0.0 };
    serde_json::json!({"value": if count > 0 { serde_json::json!(avg) } else { serde_json::Value::Null }})
}

fn merge_sum(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut sum = 0.0f64;
    for part in parts {
        if let PartialAggResult::Metric { value: Some(v) } = part {
            sum += v;
        }
    }
    serde_json::json!({"value": sum})
}

fn merge_value_count(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut count = 0.0f64;
    for part in parts {
        if let PartialAggResult::Metric { value: Some(v) } = part {
            count += v;
        }
    }
    serde_json::json!({"value": count as u64})
}

fn merge_histogram(parts: &[&PartialAggResult]) -> serde_json::Value {
    let mut bucket_counts: HashMap<i64, u64> = HashMap::new();
    for part in parts {
        if let PartialAggResult::Histogram { buckets } = part {
            for b in buckets {
                // Use integer key for exact bucketing
                let k = (b.key * 1000.0).round() as i64;
                *bucket_counts.entry(k).or_insert(0) += b.doc_count;
            }
        }
    }
    let mut buckets: Vec<serde_json::Value> = bucket_counts
        .into_iter()
        .map(|(k, doc_count)| serde_json::json!({"key": k as f64 / 1000.0, "doc_count": doc_count}))
        .collect();
    buckets.sort_by(|a, b| {
        let ka = a["key"].as_f64().unwrap_or(0.0);
        let kb = b["key"].as_f64().unwrap_or(0.0);
        ka.partial_cmp(&kb).unwrap_or(std::cmp::Ordering::Equal)
    });
    serde_json::json!({"buckets": buckets})
}

/// Sort a list of search hits according to the given sort clauses.
/// Each hit is a JSON object with `_score` and `_source` fields.
/// Sort clauses are applied in order (primary sort first).
/// If no sort clauses are given, sorts by `_score` descending (default).
pub fn sort_hits(hits: &mut Vec<serde_json::Value>, sort_clauses: &[SortClause]) {
    if sort_clauses.is_empty() {
        // Default: sort by _score descending
        hits.sort_by(|a, b| {
            let sa = a.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let sb = b.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });
        return;
    }

    hits.sort_by(|a, b| {
        for clause in sort_clauses {
            let (field_name, direction) = match clause {
                SortClause::Simple(name) => {
                    if name == "_score" {
                        (name.as_str(), SortDirection::Desc)
                    } else {
                        (name.as_str(), SortDirection::Asc)
                    }
                }
                SortClause::Field(map) => {
                    if let Some((name, order)) = map.iter().next() {
                        let dir = match order {
                            SortOrder::Direction(d) => d.clone(),
                            SortOrder::Object { order } => order.clone(),
                        };
                        (name.as_str(), dir)
                    } else {
                        continue;
                    }
                }
            };

            let (va, vb) = if field_name == "_score" {
                (
                    a.get("_score").cloned().unwrap_or(serde_json::Value::Null),
                    b.get("_score").cloned().unwrap_or(serde_json::Value::Null),
                )
            } else {
                (
                    a.get("_source")
                        .and_then(|s| s.get(field_name))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    b.get("_source")
                        .and_then(|s| s.get(field_name))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                )
            };

            let cmp = compare_json_values(&va, &vb);
            let ordered = match direction {
                SortDirection::Asc => cmp,
                SortDirection::Desc => cmp.reverse(),
            };
            if ordered != std::cmp::Ordering::Equal {
                return ordered;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Compare two JSON values for sorting. Numbers are compared numerically,
/// strings lexicographically, nulls sort last.
fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater, // nulls last
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Number(na), Value::Number(nb)) => {
            let fa = na.as_f64().unwrap_or(0.0);
            let fb = nb.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
        (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Reciprocal Rank Fusion (RRF) constant. Higher values smooth out rank differences.
/// OpenSearch uses k=60 by default.
const RRF_K: f64 = 60.0;

/// Merge text and kNN hit lists using Reciprocal Rank Fusion (RRF).
///
/// Each hit is a JSON object with at least `_id` (string) and `_score` (number).
/// Hits from text search and kNN search are ranked independently, then
/// combined into a single score: `rrf_score = 1/(k + rank_text) + 1/(k + rank_knn)`.
///
/// Duplicate `_id`s are collapsed into a single hit. The `_source` is taken from
/// whichever list provided it. kNN metadata (`_knn_distance`, `_knn_field`) is
/// preserved from the kNN hit when present.
///
/// Returns hits sorted by RRF score descending.
pub fn merge_hybrid_hits(
    text_hits: Vec<serde_json::Value>,
    knn_hits: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    // If only one list has hits, skip the merge entirely
    if knn_hits.is_empty() {
        return text_hits;
    }
    if text_hits.is_empty() {
        return knn_hits;
    }

    // Build rank maps (1-based ranking by position, which is already score-sorted)
    let mut doc_map: HashMap<String, MergedHit> = HashMap::new();

    for (rank, hit) in text_hits.iter().enumerate() {
        let id = hit
            .get("_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if id.is_empty() {
            continue;
        }
        let entry = doc_map.entry(id).or_insert_with(|| MergedHit::new(hit));
        entry.text_rank = Some(rank + 1);
        if entry.source.is_none() {
            entry.source = hit.get("_source").cloned();
        }
    }

    for (rank, hit) in knn_hits.iter().enumerate() {
        let id = hit
            .get("_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if id.is_empty() {
            continue;
        }
        let entry = doc_map.entry(id).or_insert_with(|| MergedHit::new(hit));
        entry.knn_rank = Some(rank + 1);
        // Prefer kNN hit's _source (it may include vector fields)
        if let Some(src) = hit.get("_source") {
            entry.source = Some(src.clone());
        }
        entry.knn_distance = hit.get("_knn_distance").and_then(|v| v.as_f64());
        entry.knn_field = hit
            .get("_knn_field")
            .and_then(|v| v.as_str())
            .map(String::from);
    }

    // Compute RRF scores and build result hits
    let mut results: Vec<serde_json::Value> = doc_map
        .into_iter()
        .map(|(id, merged)| {
            let rrf_score = merged.rrf_score();
            let mut hit = serde_json::json!({
                "_id": id,
                "_score": rrf_score,
                "_source": merged.source,
            });
            // Preserve envelope fields from the original hit
            if let Some(idx) = merged.index {
                hit["_index"] = serde_json::Value::String(idx);
            }
            if let Some(shard) = merged.shard {
                hit["_shard"] = serde_json::json!(shard);
            }
            if let Some(dist) = merged.knn_distance {
                hit["_knn_distance"] = serde_json::json!(dist);
            }
            if let Some(ref field) = merged.knn_field {
                hit["_knn_field"] = serde_json::Value::String(field.clone());
            }
            hit
        })
        .collect();

    results.sort_by(|a, b| {
        let sa = a.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sb = b.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

/// Internal helper for tracking a doc across text and kNN result sets.
struct MergedHit {
    text_rank: Option<usize>,
    knn_rank: Option<usize>,
    source: Option<serde_json::Value>,
    index: Option<String>,
    shard: Option<u32>,
    knn_distance: Option<f64>,
    knn_field: Option<String>,
}

impl MergedHit {
    fn new(hit: &serde_json::Value) -> Self {
        Self {
            text_rank: None,
            knn_rank: None,
            source: hit.get("_source").cloned(),
            index: hit.get("_index").and_then(|v| v.as_str()).map(String::from),
            shard: hit.get("_shard").and_then(|v| v.as_u64()).map(|v| v as u32),
            knn_distance: None,
            knn_field: None,
        }
    }

    fn rrf_score(&self) -> f64 {
        let text_component = self
            .text_rank
            .map(|r| 1.0 / (RRF_K + r as f64))
            .unwrap_or(0.0);
        let knn_component = self
            .knn_rank
            .map(|r| 1.0 / (RRF_K + r as f64))
            .unwrap_or(0.0);
        text_component + knn_component
    }
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
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
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
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
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

    // ── k-NN deserialization tests ──────────────────────────────────────

    #[test]
    fn deserialize_knn_query() {
        let body = json!({
            "knn": { "embedding": { "vector": [0.1, 0.2, 0.3], "k": 10 } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        let knn = req.knn.unwrap();
        let params = knn.fields.get("embedding").unwrap();
        assert_eq!(params.vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(params.k, 10);
    }

    #[test]
    fn deserialize_knn_with_text_query() {
        let body = json!({
            "query": { "match": { "title": "search" } },
            "knn": { "embedding": { "vector": [0.1, 0.2], "k": 5 } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(matches!(req.query, QueryClause::Match(_)));
        let knn = req.knn.unwrap();
        assert_eq!(knn.fields.get("embedding").unwrap().k, 5);
    }

    #[test]
    fn deserialize_no_knn_defaults_to_none() {
        let body = json!({
            "query": { "match_all": {} }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(req.knn.is_none());
    }

    #[test]
    fn deserialize_knn_only_no_query() {
        let body = json!({
            "knn": { "vec_field": { "vector": [1.0, 2.0, 3.0, 4.0], "k": 3 } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        // query should default to match_all
        assert!(matches!(req.query, QueryClause::MatchAll(_)));
        assert!(req.knn.is_some());
    }

    #[test]
    fn deserialize_knn_with_filter() {
        let body = json!({
            "knn": { "embedding": { "vector": [0.1, 0.2, 0.3], "k": 5, "filter": { "match": { "genre": "action" } } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        let knn = req.knn.unwrap();
        let params = knn.fields.get("embedding").unwrap();
        assert_eq!(params.k, 5);
        assert!(params.filter.is_some());
        assert!(matches!(
            params.filter.as_ref().unwrap(),
            QueryClause::Match(_)
        ));
    }

    #[test]
    fn deserialize_knn_without_filter_defaults_to_none() {
        let body = json!({
            "knn": { "embedding": { "vector": [0.1, 0.2], "k": 3 } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        let params = req.knn.unwrap().fields.into_values().next().unwrap();
        assert!(params.filter.is_none());
    }

    #[test]
    fn deserialize_knn_filter_with_bool_query() {
        let body = json!({
            "knn": { "emb": { "vector": [0.5], "k": 10, "filter": {
                "bool": {
                    "must": [{ "match": { "status": "active" } }],
                    "filter": [{ "range": { "year": { "gte": 2020 } } }]
                }
            }}}
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        let params = req.knn.unwrap().fields.get("emb").unwrap().clone();
        assert!(matches!(params.filter.unwrap(), QueryClause::Bool(_)));
    }

    #[test]
    fn knn_filter_roundtrip_serde() {
        let filter = QueryClause::Match({
            let mut m = HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        let params = KnnParams {
            vector: vec![1.0, 2.0],
            k: 5,
            filter: Some(filter),
        };
        let json_str = serde_json::to_string(&params).unwrap();
        let params2: KnnParams = serde_json::from_str(&json_str).unwrap();
        assert_eq!(params2.k, 5);
        assert!(params2.filter.is_some());
    }

    // ── Wildcard / Prefix deserialization ────────────────────────────────

    #[test]
    fn deserialize_wildcard_query() {
        let body = json!({
            "query": { "wildcard": { "title": "rust*" } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Wildcard(fields) => {
                assert_eq!(fields["title"], "rust*");
            }
            _ => panic!("expected Wildcard query"),
        }
    }

    #[test]
    fn deserialize_prefix_query() {
        let body = json!({
            "query": { "prefix": { "title": "sea" } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Prefix(fields) => {
                assert_eq!(fields["title"], "sea");
            }
            _ => panic!("expected Prefix query"),
        }
    }

    #[test]
    fn wildcard_inside_bool_filter() {
        let body = json!({
            "query": {
                "bool": {
                    "must": [{ "match": { "body": "search" } }],
                    "filter": [{ "wildcard": { "title": "r?st" } }]
                }
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Bool(bq) => {
                assert!(matches!(&bq.filter[0], QueryClause::Wildcard(_)));
            }
            _ => panic!("expected Bool"),
        }
    }

    #[test]
    fn deserialize_fuzzy_query() {
        let body = json!({
            "query": { "fuzzy": { "title": { "value": "rsut", "fuzziness": 2 } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Fuzzy(fields) => {
                let params = fields.get("title").unwrap();
                assert_eq!(params.value, "rsut");
                assert_eq!(params.fuzziness, 2);
            }
            _ => panic!("expected Fuzzy query"),
        }
    }

    #[test]
    fn deserialize_fuzzy_default_fuzziness() {
        let body = json!({
            "query": { "fuzzy": { "title": { "value": "serch" } } }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        match &req.query {
            QueryClause::Fuzzy(fields) => {
                assert_eq!(
                    fields["title"].fuzziness, 1,
                    "default fuzziness should be 1"
                );
            }
            _ => panic!("expected Fuzzy query"),
        }
    }

    // ── RRF merge tests ─────────────────────────────────────────────────

    #[test]
    fn merge_deduplicates_by_id() {
        let text = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"title": "hello"}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"title": "world"}}),
        ];
        let knn = vec![
            json!({"_id": "d1", "_score": 0.9, "_source": {"title": "hello"}, "_knn_field": "emb", "_knn_distance": 0.01}),
            json!({"_id": "d3", "_score": 0.8, "_source": {"title": "new"}, "_knn_field": "emb", "_knn_distance": 0.1}),
        ];
        let merged = merge_hybrid_hits(text, knn);

        let ids: Vec<&str> = merged.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        // d1 appears once (deduplicated), d2 and d3 each once = 3 total
        assert_eq!(ids.len(), 3, "should have 3 unique docs");
        assert_eq!(
            ids.iter().filter(|&&id| id == "d1").count(),
            1,
            "d1 should appear exactly once"
        );
    }

    #[test]
    fn merge_rrf_score_both_lists_ranks_higher() {
        // d1 appears in both lists → higher RRF score than d2 (text only) or d3 (knn only)
        let text = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"t": "a"}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"t": "b"}}),
        ];
        let knn = vec![
            json!({"_id": "d1", "_score": 0.9, "_source": {"t": "a"}, "_knn_field": "emb", "_knn_distance": 0.01}),
            json!({"_id": "d3", "_score": 0.8, "_source": {"t": "c"}, "_knn_field": "emb", "_knn_distance": 0.1}),
        ];
        let merged = merge_hybrid_hits(text, knn);

        // d1 should be ranked first (RRF from rank 1 in both lists)
        assert_eq!(
            merged[0]["_id"], "d1",
            "d1 should rank first (appears in both lists)"
        );

        let d1_score = merged[0]["_score"].as_f64().unwrap();
        let d2_score = merged.iter().find(|h| h["_id"] == "d2").unwrap()["_score"]
            .as_f64()
            .unwrap();
        let d3_score = merged.iter().find(|h| h["_id"] == "d3").unwrap()["_score"]
            .as_f64()
            .unwrap();

        assert!(
            d1_score > d2_score,
            "d1 (both lists) should score higher than d2 (text only)"
        );
        assert!(
            d1_score > d3_score,
            "d1 (both lists) should score higher than d3 (knn only)"
        );
    }

    #[test]
    fn merge_preserves_knn_metadata() {
        let text = vec![json!({"_id": "d1", "_score": 1.0, "_source": {"title": "hello"}})];
        let knn = vec![
            json!({"_id": "d1", "_score": 0.9, "_source": {"title": "hello"}, "_knn_field": "emb", "_knn_distance": 0.05}),
        ];
        let merged = merge_hybrid_hits(text, knn);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["_knn_field"], "emb");
        let dist = merged[0]["_knn_distance"].as_f64().unwrap();
        assert!((dist - 0.05).abs() < 1e-9);
    }

    #[test]
    fn merge_preserves_index_and_shard() {
        let text = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {}, "_index": "movies", "_shard": 2}),
        ];
        let knn = vec![];
        let merged = merge_hybrid_hits(text, knn);
        assert_eq!(merged[0]["_index"], "movies");
        assert_eq!(merged[0]["_shard"], 2);
    }

    #[test]
    fn merge_empty_knn_returns_text_as_is() {
        let text = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"t": "a"}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"t": "b"}}),
        ];
        let merged = merge_hybrid_hits(text.clone(), vec![]);
        assert_eq!(merged.len(), 2);
        // Should return text hits unchanged (no RRF applied)
        assert_eq!(merged[0]["_id"], text[0]["_id"]);
        assert_eq!(merged[1]["_id"], text[1]["_id"]);
    }

    #[test]
    fn merge_empty_text_returns_knn_as_is() {
        let knn = vec![
            json!({"_id": "d1", "_score": 0.9, "_source": {"t": "a"}, "_knn_field": "emb", "_knn_distance": 0.01}),
        ];
        let merged = merge_hybrid_hits(vec![], knn.clone());
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["_id"], knn[0]["_id"]);
    }

    #[test]
    fn merge_both_empty_returns_empty() {
        let merged = merge_hybrid_hits(vec![], vec![]);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_rrf_scores_are_descending() {
        let text = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {}}),
            json!({"_id": "d2", "_score": 0.8, "_source": {}}),
            json!({"_id": "d3", "_score": 0.5, "_source": {}}),
        ];
        let knn = vec![
            json!({"_id": "d3", "_score": 0.9, "_source": {}, "_knn_field": "e", "_knn_distance": 0.01}),
            json!({"_id": "d4", "_score": 0.7, "_source": {}, "_knn_field": "e", "_knn_distance": 0.1}),
        ];
        let merged = merge_hybrid_hits(text, knn);
        for w in merged.windows(2) {
            let s0 = w[0]["_score"].as_f64().unwrap();
            let s1 = w[1]["_score"].as_f64().unwrap();
            assert!(s0 >= s1, "scores should be descending: {} >= {}", s0, s1);
        }
    }

    #[test]
    fn merge_no_overlap_returns_all() {
        let text = vec![json!({"_id": "d1", "_score": 1.0, "_source": {"t": "a"}})];
        let knn = vec![
            json!({"_id": "d2", "_score": 0.9, "_source": {"t": "b"}, "_knn_field": "e", "_knn_distance": 0.01}),
        ];
        let merged = merge_hybrid_hits(text, knn);
        assert_eq!(
            merged.len(),
            2,
            "non-overlapping union should produce 2 hits"
        );
        let ids: Vec<&str> = merged.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d2"));
    }

    #[test]
    fn merge_skips_hits_without_id() {
        let text = vec![
            json!({"_score": 1.0, "_source": {}}), // no _id
            json!({"_id": "d1", "_score": 0.8, "_source": {}}),
        ];
        let knn = vec![
            json!({"_id": "", "_score": 0.9, "_source": {}, "_knn_field": "e"}), // empty _id
        ];
        let merged = merge_hybrid_hits(text, knn);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["_id"], "d1");
    }

    // ── Sort tests ──────────────────────────────────────────────────────

    #[test]
    fn sort_default_by_score_desc() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 0.5, "_source": {}}),
            json!({"_id": "d2", "_score": 0.9, "_source": {}}),
            json!({"_id": "d3", "_score": 0.1, "_source": {}}),
        ];
        sort_hits(&mut hits, &[]);
        assert_eq!(hits[0]["_id"], "d2");
        assert_eq!(hits[1]["_id"], "d1");
        assert_eq!(hits[2]["_id"], "d3");
    }

    #[test]
    fn sort_by_numeric_field_asc() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"year": 2010}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"year": 1999}}),
            json!({"_id": "d3", "_score": 0.8, "_source": {"year": 2024}}),
        ];
        let sort = vec![SortClause::Field({
            let mut m = HashMap::new();
            m.insert("year".to_string(), SortOrder::Direction(SortDirection::Asc));
            m
        })];
        sort_hits(&mut hits, &sort);
        assert_eq!(hits[0]["_id"], "d2", "1999 first");
        assert_eq!(hits[1]["_id"], "d1", "2010 second");
        assert_eq!(hits[2]["_id"], "d3", "2024 third");
    }

    #[test]
    fn sort_by_numeric_field_desc() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"rating": 8.5}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"rating": 9.2}}),
            json!({"_id": "d3", "_score": 0.8, "_source": {"rating": 7.0}}),
        ];
        let sort = vec![SortClause::Field({
            let mut m = HashMap::new();
            m.insert(
                "rating".to_string(),
                SortOrder::Direction(SortDirection::Desc),
            );
            m
        })];
        sort_hits(&mut hits, &sort);
        assert_eq!(hits[0]["_id"], "d2", "9.2 first");
        assert_eq!(hits[1]["_id"], "d1", "8.5 second");
        assert_eq!(hits[2]["_id"], "d3", "7.0 third");
    }

    #[test]
    fn sort_by_string_field_asc() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"title": "Inception"}}),
            json!({"_id": "d2", "_score": 0.5, "_source": {"title": "Avatar"}}),
            json!({"_id": "d3", "_score": 0.8, "_source": {"title": "Matrix"}}),
        ];
        let sort = vec![SortClause::Field({
            let mut m = HashMap::new();
            m.insert(
                "title".to_string(),
                SortOrder::Direction(SortDirection::Asc),
            );
            m
        })];
        sort_hits(&mut hits, &sort);
        assert_eq!(hits[0]["_id"], "d2", "Avatar first");
        assert_eq!(hits[1]["_id"], "d1", "Inception second");
        assert_eq!(hits[2]["_id"], "d3", "Matrix third");
    }

    #[test]
    fn sort_by_score_explicit() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 0.3, "_source": {}}),
            json!({"_id": "d2", "_score": 0.9, "_source": {}}),
        ];
        let sort = vec![SortClause::Simple("_score".to_string())];
        sort_hits(&mut hits, &sort);
        assert_eq!(
            hits[0]["_id"], "d2",
            "_score 0.9 first (desc default for _score)"
        );
    }

    #[test]
    fn sort_by_field_then_score_tiebreaker() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 0.5, "_source": {"year": 2020}}),
            json!({"_id": "d2", "_score": 0.9, "_source": {"year": 2020}}),
            json!({"_id": "d3", "_score": 0.1, "_source": {"year": 2010}}),
        ];
        let sort = vec![
            SortClause::Field({
                let mut m = HashMap::new();
                m.insert(
                    "year".to_string(),
                    SortOrder::Direction(SortDirection::Desc),
                );
                m
            }),
            SortClause::Simple("_score".to_string()),
        ];
        sort_hits(&mut hits, &sort);
        // year 2020 first (d1 and d2), then tie-broken by _score desc
        assert_eq!(hits[0]["_id"], "d2", "year=2020, score=0.9");
        assert_eq!(hits[1]["_id"], "d1", "year=2020, score=0.5");
        assert_eq!(hits[2]["_id"], "d3", "year=2010");
    }

    #[test]
    fn sort_nulls_last() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"year": 2020}}),
            json!({"_id": "d2", "_score": 1.0, "_source": {}}), // no year field
            json!({"_id": "d3", "_score": 1.0, "_source": {"year": 2010}}),
        ];
        let sort = vec![SortClause::Field({
            let mut m = HashMap::new();
            m.insert("year".to_string(), SortOrder::Direction(SortDirection::Asc));
            m
        })];
        sort_hits(&mut hits, &sort);
        assert_eq!(hits[0]["_id"], "d3", "2010 first");
        assert_eq!(hits[1]["_id"], "d1", "2020 second");
        assert_eq!(hits[2]["_id"], "d2", "null last");
    }

    #[test]
    fn sort_with_object_order_syntax() {
        let mut hits = vec![
            json!({"_id": "d1", "_score": 1.0, "_source": {"year": 2020}}),
            json!({"_id": "d2", "_score": 1.0, "_source": {"year": 1999}}),
        ];
        let sort = vec![SortClause::Field({
            let mut m = HashMap::new();
            m.insert(
                "year".to_string(),
                SortOrder::Object {
                    order: SortDirection::Asc,
                },
            );
            m
        })];
        sort_hits(&mut hits, &sort);
        assert_eq!(hits[0]["_id"], "d2", "1999 first");
    }

    #[test]
    fn deserialize_sort_simple_string() {
        let body = json!({"query": {"match_all": {}}, "sort": ["_score"]});
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.sort.len(), 1);
        assert!(matches!(&req.sort[0], SortClause::Simple(s) if s == "_score"));
    }

    #[test]
    fn deserialize_sort_field_with_direction() {
        let body = json!({"query": {"match_all": {}}, "sort": [{"year": "desc"}]});
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.sort.len(), 1);
        match &req.sort[0] {
            SortClause::Field(m) => {
                assert!(m.contains_key("year"));
                match m.get("year").unwrap() {
                    SortOrder::Direction(d) => assert_eq!(*d, SortDirection::Desc),
                    _ => panic!("expected Direction"),
                }
            }
            _ => panic!("expected Field sort clause"),
        }
    }

    #[test]
    fn deserialize_sort_field_with_object() {
        let body = json!({"query": {"match_all": {}}, "sort": [{"year": {"order": "asc"}}]});
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.sort.len(), 1);
    }

    #[test]
    fn deserialize_sort_mixed() {
        let body = json!({
            "query": {"match_all": {}},
            "sort": [{"year": "desc"}, "_score"]
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.sort.len(), 2);
    }

    #[test]
    fn deserialize_no_sort_defaults_to_empty() {
        let body = json!({"query": {"match_all": {}}});
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(req.sort.is_empty());
    }

    // ── Aggregation tests ───────────────────────────────────────────────

    fn make_hits(sources: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        sources
            .into_iter()
            .enumerate()
            .map(|(i, src)| json!({"_id": format!("d{}", i), "_score": 1.0, "_source": src}))
            .collect()
    }

    #[test]
    fn terms_aggregation_counts_correctly() {
        let hits = make_hits(vec![
            json!({"genre": "scifi"}),
            json!({"genre": "action"}),
            json!({"genre": "scifi"}),
            json!({"genre": "scifi"}),
            json!({"genre": "action"}),
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "genres".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "genre".into(),
                size: 10,
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        let buckets = merged["genres"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0]["key"], "scifi");
        assert_eq!(buckets[0]["doc_count"], 3);
        assert_eq!(buckets[1]["key"], "action");
        assert_eq!(buckets[1]["doc_count"], 2);
    }

    #[test]
    fn terms_aggregation_respects_size() {
        let hits = make_hits(vec![
            json!({"color": "red"}),
            json!({"color": "blue"}),
            json!({"color": "green"}),
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "colors".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "color".into(),
                size: 2,
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        let buckets = merged["colors"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2, "should be limited to size=2");
    }

    #[test]
    fn stats_aggregation() {
        let hits = make_hits(vec![
            json!({"rating": 8.0}),
            json!({"rating": 9.0}),
            json!({"rating": 7.0}),
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "rating_stats".into(),
            AggregationRequest::Stats(MetricAggParams {
                field: "rating".into(),
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        let stats = &merged["rating_stats"];
        assert_eq!(stats["count"], 3);
        assert_eq!(stats["min"], 7.0);
        assert_eq!(stats["max"], 9.0);
        assert_eq!(stats["sum"], 24.0);
        assert_eq!(stats["avg"], 8.0);
    }

    #[test]
    fn min_max_avg_sum_aggregations() {
        let hits = make_hits(vec![
            json!({"price": 10.0}),
            json!({"price": 30.0}),
            json!({"price": 20.0}),
        ]);
        for (name, agg) in [
            (
                "min_price",
                AggregationRequest::Min(MetricAggParams {
                    field: "price".into(),
                }),
            ),
            (
                "max_price",
                AggregationRequest::Max(MetricAggParams {
                    field: "price".into(),
                }),
            ),
            (
                "avg_price",
                AggregationRequest::Avg(MetricAggParams {
                    field: "price".into(),
                }),
            ),
            (
                "sum_price",
                AggregationRequest::Sum(MetricAggParams {
                    field: "price".into(),
                }),
            ),
        ] {
            let mut aggs = HashMap::new();
            aggs.insert(name.into(), agg);
            let result = compute_aggregations(&hits, &aggs);
            let merged = merge_aggregations(vec![result], &aggs);
            match name {
                "min_price" => assert_eq!(merged[name]["value"], 10.0),
                "max_price" => assert_eq!(merged[name]["value"], 30.0),
                "avg_price" => assert_eq!(merged[name]["value"], 20.0),
                "sum_price" => assert_eq!(merged[name]["value"], 60.0),
                _ => {}
            }
        }
    }

    #[test]
    fn value_count_aggregation() {
        let hits = make_hits(vec![
            json!({"title": "a"}),
            json!({"title": "b"}),
            json!({}), // 3rd has no title
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "title_count".into(),
            AggregationRequest::ValueCount(MetricAggParams {
                field: "title".into(),
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        assert_eq!(merged["title_count"]["value"], 2);
    }

    #[test]
    fn histogram_aggregation() {
        let hits = make_hits(vec![
            json!({"year": 1999}),
            json!({"year": 2003}),
            json!({"year": 2010}),
            json!({"year": 2014}),
            json!({"year": 2008}),
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "decade".into(),
            AggregationRequest::Histogram(HistogramAggParams {
                field: "year".into(),
                interval: 10.0,
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        let buckets = merged["decade"]["buckets"].as_array().unwrap();
        // 1990s: 1 (1999), 2000s: 2 (2003, 2008), 2010s: 2 (2010, 2014)
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0]["key"], 1990.0);
        assert_eq!(buckets[0]["doc_count"], 1);
        assert_eq!(buckets[1]["key"], 2000.0);
        assert_eq!(buckets[1]["doc_count"], 2);
        assert_eq!(buckets[2]["key"], 2010.0);
        assert_eq!(buckets[2]["doc_count"], 2);
    }

    #[test]
    fn merge_terms_across_shards() {
        let hits1 = make_hits(vec![json!({"genre": "scifi"}), json!({"genre": "scifi"})]);
        let hits2 = make_hits(vec![json!({"genre": "action"}), json!({"genre": "scifi"})]);

        let mut aggs = HashMap::new();
        aggs.insert(
            "genres".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "genre".into(),
                size: 10,
            }),
        );

        let r1 = compute_aggregations(&hits1, &aggs);
        let r2 = compute_aggregations(&hits2, &aggs);
        let merged = merge_aggregations(vec![r1, r2], &aggs);
        let buckets = merged["genres"]["buckets"].as_array().unwrap();
        assert_eq!(buckets[0]["key"], "scifi");
        assert_eq!(buckets[0]["doc_count"], 3); // 2 + 1
        assert_eq!(buckets[1]["key"], "action");
        assert_eq!(buckets[1]["doc_count"], 1);
    }

    #[test]
    fn merge_stats_across_shards() {
        let hits1 = make_hits(vec![json!({"v": 1.0}), json!({"v": 5.0})]);
        let hits2 = make_hits(vec![json!({"v": 3.0}), json!({"v": 10.0})]);

        let mut aggs = HashMap::new();
        aggs.insert(
            "s".into(),
            AggregationRequest::Stats(MetricAggParams { field: "v".into() }),
        );

        let r1 = compute_aggregations(&hits1, &aggs);
        let r2 = compute_aggregations(&hits2, &aggs);
        let merged = merge_aggregations(vec![r1, r2], &aggs);
        assert_eq!(merged["s"]["count"], 4);
        assert_eq!(merged["s"]["min"], 1.0);
        assert_eq!(merged["s"]["max"], 10.0);
        assert_eq!(merged["s"]["sum"], 19.0);
        assert_eq!(merged["s"]["avg"], 4.75);
    }

    #[test]
    fn empty_hits_produce_null_stats() {
        let hits: Vec<serde_json::Value> = vec![];
        let mut aggs = HashMap::new();
        aggs.insert(
            "s".into(),
            AggregationRequest::Stats(MetricAggParams { field: "v".into() }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        assert_eq!(merged["s"]["count"], 0);
        assert!(merged["s"]["min"].is_null());
        assert!(merged["s"]["max"].is_null());
    }

    #[test]
    fn deserialize_aggs_terms() {
        let body = json!({
            "query": {"match_all": {}},
            "aggs": {
                "genres": {"terms": {"field": "genre", "size": 5}}
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.aggs.len(), 1);
        assert!(matches!(
            req.aggs.get("genres"),
            Some(AggregationRequest::Terms(_))
        ));
    }

    #[test]
    fn deserialize_aggs_stats() {
        let body = json!({
            "query": {"match_all": {}},
            "aggs": {
                "rating_stats": {"stats": {"field": "rating"}}
            }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(matches!(
            req.aggs.get("rating_stats"),
            Some(AggregationRequest::Stats(_))
        ));
    }

    #[test]
    fn deserialize_aggs_histogram() {
        let body = json!({
            "query": {"match_all": {}},
            "aggs": { "by_decade": {"histogram": {"field": "year", "interval": 10}} }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(matches!(
            req.aggs.get("by_decade"),
            Some(AggregationRequest::Histogram(_))
        ));
    }

    #[test]
    fn deserialize_aggregations_alias() {
        let body = json!({
            "query": {"match_all": {}},
            "aggregations": { "g": {"terms": {"field": "genre"}} }
        });
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.aggs.len(), 1, "'aggregations' should alias to 'aggs'");
    }

    #[test]
    fn no_aggs_defaults_to_empty() {
        let body = json!({"query": {"match_all": {}}});
        let req: SearchRequest = serde_json::from_value(body).unwrap();
        assert!(req.aggs.is_empty());
    }

    #[test]
    fn multiple_aggregations_in_one_request() {
        let hits = make_hits(vec![
            json!({"genre": "scifi", "rating": 8.7}),
            json!({"genre": "action", "rating": 9.0}),
            json!({"genre": "scifi", "rating": 8.4}),
        ]);
        let mut aggs = HashMap::new();
        aggs.insert(
            "genres".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "genre".into(),
                size: 10,
            }),
        );
        aggs.insert(
            "rating_stats".into(),
            AggregationRequest::Stats(MetricAggParams {
                field: "rating".into(),
            }),
        );

        let result = compute_aggregations(&hits, &aggs);
        let merged = merge_aggregations(vec![result], &aggs);
        assert!(merged.contains_key("genres"));
        assert!(merged.contains_key("rating_stats"));
        assert_eq!(merged["rating_stats"]["count"], 3);
    }
}
