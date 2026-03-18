use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Field, STORED, STRING, Schema, TEXT, Value};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use super::SearchEngine;
use crate::wal::{HotTranslog, WriteAheadLog};

/// Dynamic field registry — maps user-facing field names to Tantivy Field handles.
/// New fields are added on first encounter (dynamic mapping, like OpenSearch).
struct FieldRegistry {
    /// _id: unique document identifier (indexed, not tokenized)
    id_field: Field,
    /// _source: stores the raw JSON document (STORED only, not indexed)
    source_field: Field,
    /// Named text fields created dynamically from document keys
    fields: HashMap<String, Field>,
}

/// Hot engine — Tantivy-backed search engine where all data lives in
/// memory-mapped segments for maximum query performance.
pub struct HotEngine {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    field_registry: RwLock<FieldRegistry>,
    /// The per-index refresh interval (e.g. 5s default, matches OpenSearch's index.refresh_interval)
    pub refresh_interval: Duration,
    /// Write-ahead log for crash durability
    translog: Arc<Mutex<dyn WriteAheadLog>>,
}

impl HotEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, refresh_interval: Duration) -> Result<Self> {
        Self::new_with_mappings(data_dir, refresh_interval, &HashMap::new())
    }

    /// Create a new HotEngine with explicit field mappings.
    /// When mappings are provided, named Tantivy fields are created for each mapped field.
    /// The "body" catch-all is always created for backward compatibility with `?q=` queries.
    pub fn new_with_mappings<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let index_path = data_dir.join("index");
        std::fs::create_dir_all(&index_path)?;

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("_id", STRING | STORED);
        let source_field = schema_builder.add_text_field("_source", STORED);
        let body_field = schema_builder.add_text_field("body", TEXT | STORED);

        // Create typed fields from mappings
        let mut mapped_fields: HashMap<String, Field> = HashMap::new();
        for (name, mapping) in mappings {
            use crate::cluster::state::FieldType;
            let field = match mapping.field_type {
                FieldType::Text => schema_builder.add_text_field(name, TEXT | STORED),
                FieldType::Keyword => schema_builder.add_text_field(name, STRING | STORED),
                FieldType::Integer => {
                    schema_builder.add_i64_field(name, tantivy::schema::INDEXED | STORED)
                }
                FieldType::Float => {
                    schema_builder.add_f64_field(name, tantivy::schema::INDEXED | STORED)
                }
                FieldType::Boolean => schema_builder.add_text_field(name, STRING | STORED),
                FieldType::KnnVector => continue, // vectors are in USearch, not Tantivy
            };
            mapped_fields.insert(name.clone(), field);
        }

        let schema = schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_path)?;
        let index = Index::open_or_create(mmap_dir, schema.clone())?;

        // Rebuild field registry from persisted schema (handles restart)
        let mut fields = HashMap::new();
        fields.insert("body".to_string(), body_field);
        // Merge in the mapped fields
        for (name, field) in &mapped_fields {
            fields.insert(name.clone(), *field);
        }
        // Also pick up any fields from the persisted schema (restart case)
        for (field, entry) in schema.fields() {
            let name = entry.name().to_string();
            if name != "_source" && name != "_id" && !fields.contains_key(&name) {
                fields.insert(name, field);
            }
        }

        let writer = index.writer(50_000_000)?; // 50MB heap
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        // Open or create the translog in the data directory (not index_path)
        let translog = HotTranslog::open(data_dir)?;

        let field_registry = FieldRegistry {
            id_field,
            source_field,
            fields,
        };

        let engine = Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            field_registry: RwLock::new(field_registry),
            refresh_interval,
            translog: Arc::new(Mutex::new(translog)),
        };

        // Replay any uncommitted translog entries from before a crash
        engine.replay_translog()?;

        Ok(engine)
    }

    /// Get (or lazily register) a field by name.
    /// With Tantivy, once an index is created, the schema is fixed — so we look up
    /// pre-existing fields. If a field doesn't exist, we fall back to the "body" field.
    fn resolve_field(&self, field_name: &str) -> Field {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(f) = registry.fields.get(field_name) {
            return *f;
        }
        // Fall back to "body" for unknown fields
        *registry.fields.get("body").expect("body field must exist")
    }

    /// Build a Tantivy document from a JSON object.
    /// When typed fields exist in the registry, values are indexed into their
    /// proper field types. All text values also go into the "body" catch-all
    /// for backward-compatible `?q=` query string searches.
    fn build_tantivy_doc(&self, doc_id: &str, payload: &serde_json::Value) -> TantivyDocument {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        Self::build_tantivy_doc_inner(&registry, doc_id, payload)
    }

    /// Build a Tantivy document using an already-acquired registry reference.
    /// Used by bulk paths to avoid per-doc RwLock acquisition.
    fn build_tantivy_doc_inner(
        registry: &FieldRegistry,
        doc_id: &str,
        payload: &serde_json::Value,
    ) -> TantivyDocument {
        let mut doc = TantivyDocument::new();

        // Store the document ID
        doc.add_text(registry.id_field, doc_id);

        // Store the raw JSON in _source (serde_json::to_string is faster than Display)
        if let Ok(json_str) = serde_json::to_string(payload) {
            doc.add_text(registry.source_field, json_str);
        }

        let body_field = *registry.fields.get("body").expect("body field must exist");

        if let Some(obj) = payload.as_object() {
            // Build body catch-all with a single String buffer (avoids Vec<String> + join)
            let mut body_buf = String::new();

            for (key, value) in obj {
                // If this field has a named Tantivy field, index into it by type
                if let Some(&field) = registry.fields.get(key.as_str()) {
                    if field != body_field {
                        match value {
                            serde_json::Value::String(s) => {
                                doc.add_text(field, s);
                            }
                            serde_json::Value::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    doc.add_i64(field, i);
                                } else if let Some(f) = n.as_f64() {
                                    doc.add_f64(field, f);
                                }
                            }
                            serde_json::Value::Bool(b) => {
                                doc.add_text(field, if *b { "true" } else { "false" });
                            }
                            _ => {}
                        }
                    }
                }

                // Append text representation to body catch-all buffer
                match value {
                    serde_json::Value::String(s) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        body_buf.push_str(s);
                    }
                    serde_json::Value::Number(n) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        use std::fmt::Write;
                        let _ = write!(body_buf, "{n}");
                    }
                    serde_json::Value::Bool(b) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        body_buf.push_str(if *b { "true" } else { "false" });
                    }
                    _ => {}
                }
            }

            if !body_buf.is_empty() {
                doc.add_text(body_field, body_buf);
            }
        } else if let Ok(s) = serde_json::to_string(payload) {
            doc.add_text(body_field, s);
        }

        doc
    }

    /// Replays all pending translog entries into the Tantivy buffer.
    /// Called on startup to recover from an unclean shutdown.
    fn replay_translog(&self) -> Result<()> {
        let entries = {
            let tl = self.translog.lock().unwrap();
            tl.read_all()?
        };

        if entries.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            "Replaying {} translog entries from unclean shutdown...",
            entries.len()
        );

        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        for entry in &entries {
            let doc_id = entry
                .payload
                .get("_doc_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let source = entry.payload.get("_source").unwrap_or(&entry.payload);
            let doc = self.build_tantivy_doc(doc_id, source);
            writer.add_document(doc)?;
        }
        writer.commit()?;
        self.reader.reload()?;

        tracing::info!(
            "Translog replay complete. {} documents recovered.",
            entries.len()
        );
        Ok(())
    }

    /// Starts the per-index background refresh loop.
    /// Called by the Node after wrapping the engine in an Arc.
    pub fn start_refresh_loop(engine: Arc<Self>) {
        let interval = engine.refresh_interval;
        tokio::spawn(async move {
            tracing::info!("Index refresh loop started (interval: {:?})", interval);
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = engine.refresh() {
                    tracing::error!("Background refresh failed: {}", e);
                }
            }
        });
    }

    /// Shared search execution helper — returns _id + _source from each hit.
    /// `limit` controls how many top docs Tantivy collects.
    fn execute_search(
        &self,
        searcher: tantivy::Searcher,
        query: &dyn tantivy::query::Query,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let effective_limit = if limit == 0 { 1 } else { limit };
        let top_docs = searcher.search(query, &TopDocs::with_limit(effective_limit))?;
        self.collect_hits(&searcher, top_docs)
    }

    /// Extract _id, _score, _source from pre-collected top docs.
    fn collect_hits(
        &self,
        searcher: &tantivy::Searcher,
        top_docs: Vec<(f32, tantivy::DocAddress)>,
    ) -> Result<Vec<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            // Get _id
            let doc_id = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // Get _source
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str() {
                    if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                        results.push(serde_json::json!({
                            "_id": doc_id,
                            "_score": score,
                            "_source": json_val
                        }));
                    }
                }
            }
        }
        Ok(results)
    }

    /// Return the set of document IDs matching a query clause.
    /// Used by CompositeEngine for pre-filtering kNN results.
    pub fn matching_doc_ids(
        &self,
        clause: &crate::search::QueryClause,
    ) -> Result<std::collections::HashSet<String>> {
        let query = self.build_query(clause)?;
        let searcher = self.reader.searcher();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        // Collect up to 100k matching docs — a reasonable ceiling for filter sets
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(100_000))?;
        let mut ids = std::collections::HashSet::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            if let Some(doc_id) = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
            {
                ids.insert(doc_id.to_string());
            }
        }
        Ok(ids)
    }

    /// Recursively convert a QueryClause into a Tantivy Query.
    fn build_query(
        &self,
        clause: &crate::search::QueryClause,
    ) -> Result<Box<dyn tantivy::query::Query>> {
        use crate::search::QueryClause;
        use tantivy::Term;
        use tantivy::query::{AllQuery, BooleanQuery, Occur, TermQuery};
        use tantivy::schema::IndexRecordOption;

        match clause {
            QueryClause::MatchAll(_) => Ok(Box::new(AllQuery)),
            QueryClause::Match(fields) => {
                if let Some((field_name, value)) = fields.iter().next() {
                    let query_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let target_field = self.resolve_field(field_name);
                    let query_parser = QueryParser::for_index(&self.index, vec![target_field]);
                    let query = query_parser.parse_query(&query_str)?;
                    Ok(query)
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Term(fields) => {
                if let Some((field_name, value)) = fields.iter().next() {
                    let term_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let target_field = self.resolve_field(field_name);
                    let term = Term::from_field_text(target_field, &term_str);
                    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Bool(bq) => {
                let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

                for clause in &bq.must {
                    subqueries.push((Occur::Must, self.build_query(clause)?));
                }
                for clause in &bq.should {
                    subqueries.push((Occur::Should, self.build_query(clause)?));
                }
                for clause in &bq.must_not {
                    subqueries.push((Occur::MustNot, self.build_query(clause)?));
                }
                // filter = must without scoring (Tantivy doesn't distinguish, so treat as Must)
                for clause in &bq.filter {
                    subqueries.push((Occur::Must, self.build_query(clause)?));
                }

                if subqueries.is_empty() {
                    // Empty bool matches all
                    Ok(Box::new(AllQuery))
                } else {
                    Ok(Box::new(BooleanQuery::new(subqueries)))
                }
            }
            QueryClause::Range(fields) => {
                use std::ops::Bound;
                use tantivy::query::RangeQuery;

                if let Some((field_name, condition)) = fields.iter().next() {
                    let target_field = self.resolve_field(field_name);

                    let to_term = |v: &serde_json::Value| -> Term {
                        match v {
                            serde_json::Value::String(s) => Term::from_field_text(target_field, s),
                            serde_json::Value::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    Term::from_field_i64(target_field, i)
                                } else if let Some(f) = n.as_f64() {
                                    Term::from_field_f64(target_field, f)
                                } else {
                                    Term::from_field_text(target_field, &n.to_string())
                                }
                            }
                            other => Term::from_field_text(target_field, &other.to_string()),
                        }
                    };

                    let lower = if let Some(ref v) = condition.gt {
                        Bound::Excluded(to_term(v))
                    } else if let Some(ref v) = condition.gte {
                        Bound::Included(to_term(v))
                    } else {
                        Bound::Unbounded
                    };

                    let upper = if let Some(ref v) = condition.lt {
                        Bound::Excluded(to_term(v))
                    } else if let Some(ref v) = condition.lte {
                        Bound::Included(to_term(v))
                    } else {
                        Bound::Unbounded
                    };

                    Ok(Box::new(RangeQuery::new(lower, upper)))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Wildcard(fields) => {
                use tantivy::query::RegexQuery;
                if let Some((field_name, value)) = fields.iter().next() {
                    let pattern = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // Convert OpenSearch wildcard syntax to regex:
                    // Escape regex special chars first, then convert * → .* and ? → .
                    let mut regex_pattern = String::new();
                    for ch in pattern.chars() {
                        match ch {
                            '*' => regex_pattern.push_str(".*"),
                            '?' => regex_pattern.push('.'),
                            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|'
                            | '\\' => {
                                regex_pattern.push('\\');
                                regex_pattern.push(ch);
                            }
                            _ => regex_pattern.push(ch),
                        }
                    }
                    let target_field = self.resolve_field(field_name);
                    let query = RegexQuery::from_pattern(&regex_pattern, target_field)
                        .map_err(|e| anyhow::anyhow!("Invalid wildcard pattern: {}", e))?;
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Prefix(fields) => {
                use tantivy::query::RegexQuery;
                if let Some((field_name, value)) = fields.iter().next() {
                    let prefix = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // Escape the prefix for regex safety, then append .*
                    let mut escaped = String::new();
                    for ch in prefix.chars() {
                        match ch {
                            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^'
                            | '$' | '|' | '\\' => {
                                escaped.push('\\');
                                escaped.push(ch);
                            }
                            _ => escaped.push(ch),
                        }
                    }
                    let regex_pattern = format!("{}.*", escaped);
                    let target_field = self.resolve_field(field_name);
                    let query = RegexQuery::from_pattern(&regex_pattern, target_field)
                        .map_err(|e| anyhow::anyhow!("Invalid prefix pattern: {}", e))?;
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Fuzzy(fields) => {
                use tantivy::query::FuzzyTermQuery;
                if let Some((field_name, params)) = fields.iter().next() {
                    let target_field = self.resolve_field(field_name);
                    let term = Term::from_field_text(target_field, &params.value);
                    let query = FuzzyTermQuery::new(term, params.fuzziness, true);
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
        }
    }

    /// Flush with translog retention: commit to disk and truncate WAL entries
    /// only up to the given global checkpoint. Entries above the checkpoint
    /// are retained for replica recovery via translog replay.
    /// Returns the highest seq_no written to the WAL.
    pub fn last_seq_no(&self) -> u64 {
        let tl = self.translog.lock().unwrap();
        tl.last_seq_no()
    }

    pub fn flush_with_global_checkpoint(&self, global_checkpoint: u64) -> Result<()> {
        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.commit()?;
        drop(writer);
        self.reader.reload()?;
        let tl = self.translog.lock().unwrap();
        if global_checkpoint > 0 {
            tl.truncate_below(global_checkpoint)?;
        } else {
            tl.truncate()?;
        }
        Ok(())
    }
}

impl super::SearchEngine for HotEngine {
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String> {
        // 1. Write to translog — durable before anything else
        {
            let tl = self.translog.lock().unwrap();
            let wal_entry = serde_json::json!({
                "_doc_id": doc_id,
                "_source": payload
            });
            tl.append("index", wal_entry)?;
        }

        // 2. Delete any existing doc with same _id (upsert semantics)
        let id_field = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.delete_term(Term::from_field_text(id_field, doc_id));

        // 3. Write to Tantivy in-memory buffer
        let doc = self.build_tantivy_doc(doc_id, &payload);
        writer.add_document(doc)?;

        Ok(doc_id.to_string())
    }

    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>> {
        // 1. Write all ops to translog with a single fsync (write_bulk skips entry construction)
        let ops: Vec<(&str, serde_json::Value)> = docs
            .iter()
            .map(|(id, p)| ("index", serde_json::json!({ "_doc_id": id, "_source": p })))
            .collect();
        {
            let tl = self.translog.lock().unwrap();
            tl.write_bulk(&ops)?;
        }

        // 2. Write all docs to Tantivy in-memory buffer under one lock
        // Acquire registry once for the entire batch (not per-doc)
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let mut doc_ids = Vec::with_capacity(docs.len());
        for (doc_id, payload) in &docs {
            writer.delete_term(Term::from_field_text(registry.id_field, doc_id));
            let doc = Self::build_tantivy_doc_inner(&registry, doc_id, payload);
            writer.add_document(doc)?;
            doc_ids.push(doc_id.clone());
        }

        Ok(doc_ids)
    }

    fn delete_document(&self, doc_id: &str) -> Result<u64> {
        // 1. Write to translog
        {
            let tl = self.translog.lock().unwrap();
            tl.append("delete", serde_json::json!({ "_doc_id": doc_id }))?;
        }

        // 2. Delete from Tantivy
        let id_field = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let opstamp = writer.delete_term(Term::from_field_text(id_field, doc_id));
        // delete_term returns an OpStamp, not a count — we report 1 optimistically
        let _ = opstamp;
        Ok(1)
    }

    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let searcher = self.reader.searcher();
        let term = Term::from_field_text(registry.id_field, doc_id);
        let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        if let Some((_score, doc_address)) = top_docs.first() {
            let retrieved_doc = searcher.doc::<TantivyDocument>(*doc_address)?;
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str() {
                    if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                        return Ok(Some(json_val));
                    }
                }
            }
        }
        Ok(None)
    }

    fn refresh(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.commit()?;
        drop(writer); // release lock before translog truncate
        self.reader.reload()?;
        let tl = self.translog.lock().unwrap();
        tl.truncate()?;
        Ok(())
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        let body_field = self.resolve_field("body");
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![body_field]);
        let query = query_parser.parse_query(query_str)?;
        self.execute_search(searcher, &*query, 100)
    }

    fn search_query(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let limit = std::cmp::max(req.from + req.size, 100);
        let searcher = self.reader.searcher();
        let query = self.build_query(&req.query)?;
        let effective_limit = if limit == 0 { 1 } else { limit };
        let (top_docs, total) =
            searcher.search(&*query, &(TopDocs::with_limit(effective_limit), Count))?;
        let hits = self.collect_hits(&searcher, top_docs)?;
        Ok((hits, total))
    }

    fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{QueryClause, SearchRequest};
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::Duration;

    /// Helper: create a HotEngine backed by a temp directory.
    fn create_engine() -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        (dir, engine)
    }

    // ── basic CRUD ──────────────────────────────────────────────────────

    #[test]
    fn add_and_get_document() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let doc = engine.get_document("doc1").unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["title"], "hello world");
    }

    #[test]
    fn get_nonexistent_document_returns_none() {
        let (_dir, engine) = create_engine();
        let doc = engine.get_document("no-such-doc").unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn add_document_upsert_semantics() {
        let (_dir, engine) = create_engine();
        engine.add_document("doc1", json!({"version": 1})).unwrap();
        engine.refresh().unwrap();

        // Overwrite with new payload
        engine.add_document("doc1", json!({"version": 2})).unwrap();
        engine.refresh().unwrap();

        let doc = engine.get_document("doc1").unwrap().unwrap();
        assert_eq!(doc["version"], 2);
        // Should still be 1 doc, not 2
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn delete_document() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc1", json!({"title": "delete me"}))
            .unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        engine.delete_document("doc1").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 0);
        assert!(engine.get_document("doc1").unwrap().is_none());
    }

    // ── bulk operations ─────────────────────────────────────────────────

    #[test]
    fn bulk_add_documents() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..10)
            .map(|i| (format!("doc-{}", i), json!({"num": i})))
            .collect();
        let ids = engine.bulk_add_documents(docs).unwrap();
        engine.refresh().unwrap();

        assert_eq!(ids.len(), 10);
        assert_eq!(engine.doc_count(), 10);

        let doc = engine.get_document("doc-5").unwrap().unwrap();
        assert_eq!(doc["num"], 5);
    }

    // ── search ──────────────────────────────────────────────────────────

    #[test]
    fn simple_query_string_search() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust programming language"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python programming language"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "cooking recipes"}))
            .unwrap();
        engine.refresh().unwrap();

        let results = engine.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn search_match_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("a", json!({"x": 1})).unwrap();
        engine.add_document("b", json!({"x": 2})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, total) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(total, 2);
    }

    #[test]
    fn search_query_total_count_exceeds_limit() {
        let (_dir, engine) = create_engine();
        // Insert 200 docs — more than the default limit of 100
        for i in 0..200 {
            engine
                .add_document(&format!("doc-{}", i), json!({"val": i}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // With size=0, only 100 hits are collected, but total should be 200
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, total) = engine.search_query(&req).unwrap();
        assert_eq!(total, 200, "total should count all matching docs");
        assert!(
            results.len() <= 100,
            "hits should be limited by max(from+size, 100)"
        );
    }

    #[test]
    fn search_query_total_with_filter() {
        let (_dir, engine) = create_engine();
        for i in 0..50 {
            engine
                .add_document(&format!("d{}", i), json!({"body": "matching term"}))
                .unwrap();
        }
        for i in 50..100 {
            engine
                .add_document(&format!("d{}", i), json!({"body": "other content"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let mut fields = HashMap::new();
        fields.insert("body".to_string(), json!("matching"));
        let req = SearchRequest {
            query: QueryClause::Match(fields),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, total) = engine.search_query(&req).unwrap();
        assert_eq!(total, 50, "total should count all matching docs");
        assert!(results.len() <= 100);
    }

    #[test]
    fn search_match_query() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "database internals"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "web development"}))
            .unwrap();
        engine.refresh().unwrap();

        let mut fields = HashMap::new();
        fields.insert("body".to_string(), json!("database"));
        let req = SearchRequest {
            query: QueryClause::Match(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── refresh / flush ─────────────────────────────────────────────────

    #[test]
    fn documents_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "invisible"}))
            .unwrap();
        // doc_count uses the reader which hasn't been reloaded yet
        assert_eq!(engine.doc_count(), 0);

        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn flush_truncates_translog() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.flush().unwrap();

        // After flush the translog should be empty
        let tl = engine.translog.lock().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    // ── translog replay / crash recovery ────────────────────────────────

    #[test]
    fn translog_replay_recovers_documents_after_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate: write docs but never flush (simulating crash before commit)
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("crash-doc", json!({"recovered": true}))
                .unwrap();
            // intentionally do NOT flush — translog has the entry, Tantivy segments may not
        }

        // Reopen — replay should recover the document
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        // After replay, the engine commits and reloads
        let doc = engine2.get_document("crash-doc").unwrap();
        assert!(
            doc.is_some(),
            "document should be recovered from translog replay"
        );
        assert_eq!(doc.unwrap()["recovered"], true);
    }

    #[test]
    fn flush_then_reopen_has_empty_translog() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("safe", json!({"flushed": true}))
                .unwrap();
            engine.flush().unwrap();
        }
        // Reopen
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let tl = engine2.translog.lock().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(
            entries.is_empty(),
            "translog should be empty after flush + reopen"
        );
    }

    // ── doc_count ───────────────────────────────────────────────────────

    #[test]
    fn doc_count_reflects_operations() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.doc_count(), 0);

        engine.add_document("a", json!({"x": 1})).unwrap();
        engine.add_document("b", json!({"x": 2})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);

        engine.delete_document("a").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    // ── from/size pagination ────────────────────────────────────────────

    #[test]
    fn search_query_respects_size() {
        let (_dir, engine) = create_engine();
        for i in 0..20 {
            engine
                .add_document(&format!("doc-{}", i), json!({"title": "hello world"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Engine fetches max(from+size, 100) hits; coordinator does the slicing.
        // With 20 docs and limit=max(5,100)=100, engine returns all 20.
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            20,
            "engine returns all matching docs for coordinator to slice"
        );
    }

    #[test]
    fn search_query_from_skips_results() {
        let (_dir, engine) = create_engine();
        for i in 0..10 {
            engine
                .add_document(&format!("doc-{}", i), json!({"title": "hello world"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Engine always fetches max(from+size, 100) — returns all 10
        let req_all = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (all_results, _) = engine.search_query(&req_all).unwrap();
        assert_eq!(all_results.len(), 10);

        // from=7, size=10 → engine fetches max(17,100)=100, returns all 10
        let req_paged = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 7,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (paged_results, _) = engine.search_query(&req_paged).unwrap();
        assert_eq!(
            paged_results.len(),
            10,
            "engine returns all available hits; coordinator slices"
        );
    }

    #[test]
    fn search_query_from_beyond_total_returns_all_available() {
        let (_dir, engine) = create_engine();
        for i in 0..5 {
            engine
                .add_document(&format!("doc-{}", i), json!({"title": "test"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 100,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            5,
            "engine returns all 5 hits; coordinator will slice to empty"
        );
    }

    #[test]
    fn pagination_total_is_accurate_after_coordinator_slice() {
        // Simulates what the API coordinator does: collect all engine hits,
        // report total from full set, then slice with from/size.
        let (_dir, engine) = create_engine();
        for i in 0..15 {
            engine
                .add_document(&format!("doc-{}", i), json!({"title": "hello"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (all_hits, _) = engine.search_query(&req).unwrap();
        let total = all_hits.len(); // This is what hits.total.value should be
        let paginated: Vec<_> = all_hits.into_iter().skip(req.from).take(req.size).collect();
        assert_eq!(total, 15, "total should reflect all matching docs");
        assert_eq!(paginated.len(), 5, "paginated should have size hits");
    }

    // ── Bool query tests ────────────────────────────────────────────────

    #[test]
    fn bool_must_filters_documents() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "must:rust should match d1 and d3");
    }

    #[test]
    fn bool_must_not_excludes_documents() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::MatchAll(json!({}))],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("python"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "must_not:python should exclude d2");
    }

    #[test]
    fn bool_should_with_no_must_matches_any() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python search"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "java build"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                should: vec![
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("body".into(), json!("rust"));
                        m
                    }),
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("body".into(), json!("python"));
                        m
                    }),
                ],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "should match d1 (rust) and d2 (python), not d3"
        );
    }

    #[test]
    fn bool_filter_acts_like_must() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                filter: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1, "filter:rust should match only d1");
    }

    #[test]
    fn bool_empty_matches_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "one"})).unwrap();
        engine.add_document("d2", json!({"title": "two"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery::default()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "empty bool should match all docs");
    }

    #[test]
    fn bool_combined_must_and_must_not() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust web server"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "python search tool"}))
            .unwrap();
        engine.refresh().unwrap();

        // must: rust, must_not: web → should only match d1
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("web"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "must:rust + must_not:web should only match d1"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn bool_nested_bool_inside_must() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine
            .add_document("d4", json!({"title": "java build tool"}))
            .unwrap();
        engine.refresh().unwrap();

        // Nested: must[ bool{ should[rust, python] } ], must_not[web]
        // Should match: d1 (rust, no web) — d2 (python, has web→excluded), d3 (rust, has web→excluded)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Bool(crate::search::BoolQuery {
                    should: vec![
                        QueryClause::Match({
                            let mut m = HashMap::new();
                            m.insert("body".into(), json!("rust"));
                            m
                        }),
                        QueryClause::Match({
                            let mut m = HashMap::new();
                            m.insert("body".into(), json!("python"));
                            m
                        }),
                    ],
                    ..Default::default()
                })],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("web"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "nested bool + must_not should match only d1"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn build_query_match_empty_fields_returns_all() {
        // Match with empty HashMap should return AllQuery (match all)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "empty Match should fall back to match all"
        );
    }

    #[test]
    fn build_query_term_empty_fields_returns_all() {
        // Term with empty HashMap should return AllQuery (match all)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "empty Term should fall back to match all");
    }

    #[test]
    fn build_query_match_with_numeric_value() {
        // Match with a non-string value should stringify it
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "document 42"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "other text"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match({
                let mut m = HashMap::new();
                m.insert("body".into(), json!(42));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "match with numeric value should find doc with '42'"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn build_query_term_via_search_query() {
        // Verify term query works through search_query (build_query path)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"status": "published"}))
            .unwrap();
        engine
            .add_document("d2", json!({"status": "draft"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("published"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── Range query tests ───────────────────────────────────────────────

    #[test]
    fn range_query_gte_lt_on_text() {
        // Range on text field uses lexicographic ordering
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"name": "alice"})).unwrap();
        engine.add_document("d2", json!({"name": "bob"})).unwrap();
        engine
            .add_document("d3", json!({"name": "charlie"}))
            .unwrap();
        engine.add_document("d4", json!({"name": "dave"})).unwrap();
        engine.refresh().unwrap();

        // gte "b", lt "d" → should match "bob" and "charlie"
        let mut fields = HashMap::new();
        fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gte: Some(json!("b")),
                lt: Some(json!("d")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Range(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"), "bob should match");
        assert!(ids.contains(&"d3"), "charlie should match");
    }

    #[test]
    fn range_query_gt_lte_on_text() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"name": "alice"})).unwrap();
        engine.add_document("d2", json!({"name": "bob"})).unwrap();
        engine
            .add_document("d3", json!({"name": "charlie"}))
            .unwrap();
        engine.refresh().unwrap();

        // gt "alice", lte "charlie" → bob and charlie
        let mut fields = HashMap::new();
        fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gt: Some(json!("alice")),
                lte: Some(json!("charlie")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Range(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"), "bob should match");
        assert!(ids.contains(&"d3"), "charlie should match");
        assert!(
            !ids.contains(&"d1"),
            "alice should be excluded (gt, not gte)"
        );
    }

    #[test]
    fn range_query_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": "a"})).unwrap();
        engine.add_document("d2", json!({"x": "b"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "empty Range should fall back to match all"
        );
    }

    #[test]
    fn range_inside_bool_filter_engine() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust alpha"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust beta"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "python gamma"}))
            .unwrap();
        engine.refresh().unwrap();

        // must: match "rust", filter: range body >= "alpha" and < "beta"
        // "alpha" matches d1, "beta" is excluded → only d1
        let mut range_fields = HashMap::new();
        range_fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gte: Some(json!("alpha")),
                lt: Some(json!("beta")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                filter: vec![QueryClause::Range(range_fields)],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── Wildcard / Prefix query tests ───────────────────────────────────

    #[test]
    fn wildcard_star_matches_suffix() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rustacean"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rusty"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("rust*"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn wildcard_question_mark_matches_single_char() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine.add_document("d2", json!({"title": "rest"})).unwrap();
        engine
            .add_document("d3", json!({"title": "roast"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("r?st"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"), "rust should match r?st");
        assert!(ids.contains(&"d2"), "rest should match r?st");
        assert!(!ids.contains(&"d3"), "roast should NOT match r?st");
    }

    #[test]
    fn prefix_query_matches_beginning() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "sea turtle"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "mountain"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Prefix({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("sea"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"), "search should match prefix 'sea'");
        assert!(ids.contains(&"d2"), "sea should match prefix 'sea'");
        assert!(
            !ids.contains(&"d3"),
            "mountain should NOT match prefix 'sea'"
        );
    }

    #[test]
    fn wildcard_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn prefix_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Prefix(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Fuzzy query tests ───────────────────────────────────────────────

    #[test]
    fn fuzzy_matches_typo() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine
            .add_document("d2", json!({"title": "python"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy({
                let mut m = HashMap::new();
                m.insert(
                    "body".into(),
                    crate::search::FuzzyParams {
                        value: "rsut".into(),
                        fuzziness: 2,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]["_id"], "d1",
            "fuzzy should match 'rust' for 'rsut'"
        );
    }

    #[test]
    fn fuzzy_fuzziness_0_is_exact() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy({
                let mut m = HashMap::new();
                m.insert(
                    "body".into(),
                    crate::search::FuzzyParams {
                        value: "rsut".into(),
                        fuzziness: 0,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert!(results.is_empty(), "fuzziness 0 should be exact match only");
    }

    #[test]
    fn fuzzy_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Field mappings tests ────────────────────────────────────────────

    fn create_engine_with_mappings(
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            HotEngine::new_with_mappings(dir.path(), Duration::from_secs(60), &mappings).unwrap();
        (dir, engine)
    }

    #[test]
    fn mapped_text_field_is_searchable_by_name() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "rust programming"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python scripting"}))
            .unwrap();
        engine.refresh().unwrap();

        // Match query on "title" field should hit the named text field, not just body
        let req = SearchRequest {
            query: QueryClause::Match({
                let mut m = HashMap::new();
                m.insert("title".to_string(), json!("rust"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn mapped_keyword_field_supports_term_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "status".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"status": "published", "title": "a"}))
            .unwrap();
        engine
            .add_document("d2", json!({"status": "draft", "title": "b"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("status".to_string(), json!("published"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn mapped_integer_field_supports_range_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "old", "year": 1999}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "new", "year": 2024}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "mid", "year": 2010}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "year".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(2010)),
                        lt: None,
                        lte: None,
                        gt: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 2, "year >= 2010 should match d2 and d3");
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn mapped_float_field_supports_range_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "cheap", "price": 9.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "expensive", "price": 99.99}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gt: Some(json!(50.0)),
                        gte: None,
                        lt: None,
                        lte: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn unmapped_fields_still_searchable_via_body() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // "description" is not mapped — should still be searchable via body catch-all
        engine
            .add_document(
                "d1",
                json!({"title": "test", "description": "rust programming"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("rust").unwrap();
        assert_eq!(
            hits.len(),
            1,
            "unmapped field should be searchable via body"
        );
    }

    #[test]
    fn empty_mappings_behave_like_default_engine() {
        let (_dir, engine) = create_engine_with_mappings(HashMap::new());
        engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("hello").unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn knn_vector_mapping_is_skipped_in_tantivy_schema() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(3),
            },
        );
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // knn_vector should be ignored by Tantivy — no field created for it
        engine
            .add_document("d1", json!({"title": "test", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("test").unwrap();
        assert_eq!(
            hits.len(),
            1,
            "doc should be searchable despite knn_vector field"
        );
    }

    // ── last_seq_no and flush_with_global_checkpoint ────────────────────

    #[test]
    fn last_seq_no_returns_zero_on_empty_engine() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.last_seq_no(), 0);
    }

    #[test]
    fn last_seq_no_tracks_writes() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        assert_eq!(engine.last_seq_no(), 0);

        engine.add_document("d2", json!({"x": 2})).unwrap();
        assert_eq!(engine.last_seq_no(), 1);
    }

    #[test]
    fn last_seq_no_after_bulk() {
        let (_dir, engine) = create_engine();
        engine
            .bulk_add_documents(vec![
                ("a".into(), json!({"x": 1})),
                ("b".into(), json!({"x": 2})),
                ("c".into(), json!({"x": 3})),
            ])
            .unwrap();
        assert_eq!(engine.last_seq_no(), 2, "3 entries → seq_nos 0,1,2");
    }

    #[test]
    fn flush_with_global_checkpoint_retains_above() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap(); // seq_no=0
        engine.add_document("d2", json!({"x": 2})).unwrap(); // seq_no=1
        engine.add_document("d3", json!({"x": 3})).unwrap(); // seq_no=2

        // Flush retaining entries above global_checkpoint=1
        engine.flush_with_global_checkpoint(1).unwrap();

        // Seq_no should be preserved
        assert_eq!(engine.last_seq_no(), 2);

        // All docs should still be searchable
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 3);
    }

    #[test]
    fn flush_with_global_checkpoint_zero_truncates_all() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();

        // global_checkpoint=0 → truncate() (discard all)
        engine.flush_with_global_checkpoint(0).unwrap();

        // Seq_no preserved
        assert_eq!(engine.last_seq_no(), 1);

        // Docs still searchable (committed)
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);
    }

    // ── refresh visibility ──────────────────────────────────────────────

    #[test]
    fn docs_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "invisible"}))
            .unwrap();

        // No refresh — doc should NOT be searchable
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "docs must not be visible before refresh");
        assert!(hits.is_empty());

        // get_document should also return None (not committed)
        let doc = engine.get_document("d1").unwrap();
        assert!(doc.is_none(), "get_document must not find uncommitted doc");
    }

    #[test]
    fn docs_visible_after_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "now visible"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "also visible"}))
            .unwrap();

        // Refresh commits + reloads reader
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total) = engine.search_query(&req).unwrap();
        assert_eq!(total, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(engine.doc_count(), 2);
    }

    #[test]
    fn bulk_docs_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..50)
            .map(|i| (format!("b{}", i), json!({"val": i})))
            .collect();
        engine.bulk_add_documents(docs).unwrap();

        // No refresh — none should be searchable
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "bulk docs must not be visible before refresh");
        assert!(hits.is_empty());
    }

    #[test]
    fn bulk_docs_visible_after_refresh() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..50)
            .map(|i| (format!("b{}", i), json!({"val": i})))
            .collect();
        engine.bulk_add_documents(docs).unwrap();

        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 50,
            "all 50 bulk docs should be visible after refresh"
        );
        assert_eq!(hits.len(), 50, "all 50 returned since under internal limit");
        assert_eq!(engine.doc_count(), 50);
    }

    #[test]
    fn incremental_refresh_visibility() {
        let (_dir, engine) = create_engine();

        // Batch 1: add + refresh
        engine.add_document("a1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        // Batch 2: add without refresh — old docs still visible, new ones not
        engine.add_document("a2", json!({"x": 2})).unwrap();
        assert_eq!(engine.doc_count(), 1, "a2 not visible until refresh");

        // Refresh again — both visible
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);
    }

    #[test]
    fn refresh_idempotent() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        // Multiple refreshes with no new writes should be fine
        engine.refresh().unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }
}
