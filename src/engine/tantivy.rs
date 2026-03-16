use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, STRING, STORED, TEXT};
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
        let data_dir = data_dir.as_ref();
        let index_path = data_dir.join("index");
        std::fs::create_dir_all(&index_path)?;

        let mut schema_builder = Schema::builder();
        // _id stores the document identifier (indexed for exact lookup/delete, not tokenized)
        let id_field = schema_builder.add_text_field("_id", STRING | STORED);
        // _source stores the original JSON verbatim (STORED only — not searchable)
        let source_field = schema_builder.add_text_field("_source", STORED);
        // Pre-create a catch-all "body" field for backward compat with simple query strings
        let body_field = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_path)?;
        let index = Index::open_or_create(mmap_dir, schema.clone())?;

        // Rebuild field registry from persisted schema (handles restart)
        let mut fields = HashMap::new();
        fields.insert("body".to_string(), body_field);
        for (field, entry) in schema.fields() {
            let name = entry.name().to_string();
            if name != "_source" && name != "_id" {
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
        let registry = self.field_registry.read().unwrap_or_else(|e| e.into_inner());
        if let Some(f) = registry.fields.get(field_name) {
            return *f;
        }
        // Fall back to "body" for unknown fields
        *registry.fields.get("body").expect("body field must exist")
    }

    /// Build a Tantivy document from a JSON object.
    /// Stores _id, _source(raw JSON), and indexes text into the "body" catch-all.
    fn build_tantivy_doc(&self, doc_id: &str, payload: &serde_json::Value) -> TantivyDocument {
        let registry = self.field_registry.read().unwrap_or_else(|e| e.into_inner());
        let mut doc = TantivyDocument::new();

        // Store the document ID
        doc.add_text(registry.id_field, doc_id);

        // Store the raw JSON in _source
        doc.add_text(registry.source_field, payload.to_string());

        // Index each top-level string/number field into the "body" catch-all
        let body_field = *registry.fields.get("body").expect("body field must exist");
        if let Some(obj) = payload.as_object() {
            let mut body_parts = Vec::new();
            for (_key, value) in obj {
                match value {
                    serde_json::Value::String(s) => body_parts.push(s.clone()),
                    serde_json::Value::Number(n) => body_parts.push(n.to_string()),
                    serde_json::Value::Bool(b) => body_parts.push(b.to_string()),
                    _ => {}
                }
            }
            if !body_parts.is_empty() {
                doc.add_text(body_field, body_parts.join(" "));
            }
        } else {
            // Not an object — store the whole thing as body
            doc.add_text(body_field, payload.to_string());
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
            let doc_id = entry.payload.get("_doc_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let source = entry.payload.get("_source")
                .unwrap_or(&entry.payload);
            let doc = self.build_tantivy_doc(doc_id, source);
            writer.add_document(doc)?;
        }
        writer.commit()?;
        self.reader.reload()?;

        tracing::info!("Translog replay complete. {} documents recovered.", entries.len());
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

    /// Shared search execution helper — returns _id + _source from each hit
    fn execute_search(&self, searcher: tantivy::Searcher, query: &dyn tantivy::query::Query) -> Result<Vec<serde_json::Value>> {
        let top_docs = searcher.search(query, &TopDocs::with_limit(10))?;
        let registry = self.field_registry.read()
            .unwrap_or_else(|e| e.into_inner());

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            // Get _id
            let doc_id = retrieved_doc.get_all(registry.id_field)
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
        let id_field = self.field_registry.read().unwrap_or_else(|e| e.into_inner()).id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.delete_term(Term::from_field_text(id_field, doc_id));

        // 3. Write to Tantivy in-memory buffer
        let doc = self.build_tantivy_doc(doc_id, &payload);
        writer.add_document(doc)?;

        Ok(doc_id.to_string())
    }

    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>> {
        // 1. Write all ops to translog with a single fsync
        let ops: Vec<(&str, serde_json::Value)> = docs
            .iter()
            .map(|(id, p)| ("index", serde_json::json!({ "_doc_id": id, "_source": p })))
            .collect();
        {
            let tl = self.translog.lock().unwrap();
            tl.append_bulk(&ops)?;
        }

        // 2. Write all docs to Tantivy in-memory buffer under one lock
        let id_field = self.field_registry.read().unwrap_or_else(|e| e.into_inner()).id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let mut doc_ids = Vec::with_capacity(docs.len());
        for (doc_id, payload) in &docs {
            writer.delete_term(Term::from_field_text(id_field, doc_id));
            let doc = self.build_tantivy_doc(doc_id, payload);
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
        let id_field = self.field_registry.read().unwrap_or_else(|e| e.into_inner()).id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let opstamp = writer.delete_term(Term::from_field_text(id_field, doc_id));
        // delete_term returns an OpStamp, not a count — we report 1 optimistically
        let _ = opstamp;
        Ok(1)
    }

    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>> {
        let registry = self.field_registry.read().unwrap_or_else(|e| e.into_inner());
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
        let tl = self.translog.lock().unwrap();
        tl.truncate()?;
        Ok(())
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        let body_field = self.resolve_field("body");
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![body_field]);
        let query = query_parser.parse_query(query_str)?;
        self.execute_search(searcher, &*query)
    }

    fn search_query(&self, req: &crate::search::SearchRequest) -> Result<Vec<serde_json::Value>> {
        use crate::search::QueryClause;
        use tantivy::query::{AllQuery, TermQuery};
        use tantivy::schema::IndexRecordOption;
        use tantivy::Term;

        let searcher = self.reader.searcher();

        match &req.query {
            QueryClause::MatchAll(_) => {
                self.execute_search(searcher, &AllQuery)
            }
            QueryClause::Match(fields) => {
                if let Some((field_name, value)) = fields.iter().next() {
                    let query_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let target_field = self.resolve_field(field_name);
                    let query_parser = QueryParser::for_index(&self.index, vec![target_field]);
                    let query = query_parser.parse_query(&query_str)?;
                    self.execute_search(searcher, &*query)
                } else {
                    Ok(vec![])
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
                    let query = TermQuery::new(term, IndexRecordOption::Basic);
                    self.execute_search(searcher, &query)
                } else {
                    Ok(vec![])
                }
            }
        }
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
        engine.add_document("doc1", json!({"title": "hello world"})).unwrap();
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
        engine.add_document("doc1", json!({"title": "delete me"})).unwrap();
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
        engine.add_document("d1", json!({"title": "rust programming language"})).unwrap();
        engine.add_document("d2", json!({"title": "python programming language"})).unwrap();
        engine.add_document("d3", json!({"title": "cooking recipes"})).unwrap();
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
        };
        let results = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn search_match_query() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "database internals"})).unwrap();
        engine.add_document("d2", json!({"title": "web development"})).unwrap();
        engine.refresh().unwrap();

        let mut fields = HashMap::new();
        fields.insert("body".to_string(), json!("database"));
        let req = SearchRequest {
            query: QueryClause::Match(fields),
            size: 10,
        };
        let results = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── refresh / flush ─────────────────────────────────────────────────

    #[test]
    fn documents_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "invisible"})).unwrap();
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
            engine.add_document("crash-doc", json!({"recovered": true})).unwrap();
            // intentionally do NOT flush — translog has the entry, Tantivy segments may not
        }

        // Reopen — replay should recover the document
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        // After replay, the engine commits and reloads
        let doc = engine2.get_document("crash-doc").unwrap();
        assert!(doc.is_some(), "document should be recovered from translog replay");
        assert_eq!(doc.unwrap()["recovered"], true);
    }

    #[test]
    fn flush_then_reopen_has_empty_translog() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine.add_document("safe", json!({"flushed": true})).unwrap();
            engine.flush().unwrap();
        }
        // Reopen
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let tl = engine2.translog.lock().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty(), "translog should be empty after flush + reopen");
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
}
