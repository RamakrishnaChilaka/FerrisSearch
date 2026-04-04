use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, Field, STORED, STRING, Schema, TEXT, Value};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use super::SearchEngine;
use crate::wal::{HotTranslog, TranslogDurability, WriteAheadLog};

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
    /// Highest committed translog seq_no, stored as the next seq_no after commit.
    committed_seq_no_path: PathBuf,
    /// Shared column cache for fast-field Arrow arrays.
    column_cache: Arc<super::column_cache::ColumnCache>,
}

impl HotEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, refresh_interval: Duration) -> Result<Self> {
        Self::new_with_mappings(
            data_dir,
            refresh_interval,
            &HashMap::new(),
            TranslogDurability::Request,
            Arc::new(super::column_cache::ColumnCache::new(0, 0)),
        )
    }

    /// Create a new HotEngine with explicit field mappings.
    /// When mappings are provided, named Tantivy fields are created for each mapped field.
    /// The "body" catch-all is always created for backward compatibility with `?q=` queries.
    pub fn new_with_mappings<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let index_path = data_dir.join("index");
        std::fs::create_dir_all(&index_path)?;

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("_id", (STRING | STORED).set_fast(None));
        let source_field = schema_builder.add_text_field("_source", STORED);
        let body_field = schema_builder.add_text_field("body", TEXT | STORED);

        // Create typed fields from mappings
        let mut mapped_fields: HashMap<String, Field> = HashMap::new();
        let mut mapping_names: Vec<_> = mappings.keys().cloned().collect();
        mapping_names.sort();
        for name in mapping_names {
            let mapping = &mappings[&name];
            use crate::cluster::state::FieldType;
            let field =
                match mapping.field_type {
                    FieldType::Text => schema_builder.add_text_field(&name, TEXT | STORED),
                    FieldType::Keyword => {
                        schema_builder.add_text_field(&name, (STRING | STORED).set_fast(None))
                    }
                    FieldType::Integer => schema_builder
                        .add_i64_field(&name, tantivy::schema::INDEXED | STORED | FAST),
                    FieldType::Float => schema_builder
                        .add_f64_field(&name, tantivy::schema::INDEXED | STORED | FAST),
                    FieldType::Boolean => {
                        schema_builder.add_text_field(&name, (STRING | STORED).set_fast(None))
                    }
                    FieldType::KnnVector => continue, // vectors are in USearch, not Tantivy
                };
            mapped_fields.insert(name, field);
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

        let writer = index.writer(512_000_000)?; // 512MB heap — matches OpenSearch's recommended minimum
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        // Open or create the translog in the data directory (not index_path)
        let translog = HotTranslog::open_with_durability(data_dir, durability)?;
        if matches!(durability, TranslogDurability::Async { .. }) {
            translog.start_sync_task();
        }

        let field_registry = FieldRegistry {
            id_field,
            source_field,
            fields,
        };

        let committed_seq_no_path = data_dir.join("translog.committed");

        let engine = Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            field_registry: RwLock::new(field_registry),
            refresh_interval,
            translog: Arc::new(Mutex::new(translog)),
            committed_seq_no_path,
            column_cache,
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

    /// Create a Tantivy Term that matches the schema type of the target field.
    /// This prevents type mismatches (e.g., i64 term on an f64 field) that cause
    /// silent 0-hit results.
    fn typed_term(&self, field: Field, value: &serde_json::Value) -> Term {
        use tantivy::schema::FieldType;
        let schema = self.index.schema();
        let field_type = schema.get_field_entry(field).field_type();
        match value {
            serde_json::Value::String(s) => Term::from_field_text(field, s),
            serde_json::Value::Number(n) => match field_type {
                FieldType::I64(_) => {
                    let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                    Term::from_field_i64(field, i)
                }
                FieldType::F64(_) => {
                    let f = n.as_f64().unwrap_or(n.as_i64().unwrap_or(0) as f64);
                    Term::from_field_f64(field, f)
                }
                FieldType::U64(_) => {
                    let u = n.as_u64().unwrap_or(n.as_f64().unwrap_or(0.0) as u64);
                    Term::from_field_u64(field, u)
                }
                _ => Term::from_field_text(field, &n.to_string()),
            },
            serde_json::Value::Bool(b) => {
                Term::from_field_text(field, if *b { "true" } else { "false" })
            }
            other => Term::from_field_text(field, &other.to_string()),
        }
    }

    pub fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<super::SqlBatchResult> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&req.query)?;
        let limit = std::cmp::max(req.size, 1);

        // Use fast-field sort when the SearchRequest includes a sortable field,
        // otherwise fall back to score-based collection.
        let (top_docs, total_hits) =
            if let Some((sort_field, order)) = self.extract_fast_field_sort(req) {
                let schema = self.index.schema();
                let field = self.resolve_field(&sort_field);
                match schema.get_field_entry(field).field_type() {
                    tantivy::schema::FieldType::F64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<f64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    tantivy::schema::FieldType::I64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<i64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    tantivy::schema::FieldType::U64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<u64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    _ => searcher.search(&*query, &(TopDocs::with_limit(limit), Count))?,
                }
            } else {
                searcher.search(&*query, &(TopDocs::with_limit(limit), Count))?
            };

        let schema = self.index.schema();
        let segment_readers = searcher.segment_readers();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        // Build per-segment field readers for requested columns
        let mut field_plans = Vec::with_capacity(segment_readers.len());
        // Also open fast-field reader for _id per segment (only if needed)
        let mut id_readers: Vec<Option<StringFastFieldReader>> =
            Vec::with_capacity(segment_readers.len());
        let mut needs_stored_doc = false;

        for segment_reader in segment_readers {
            let fast_fields = segment_reader.fast_fields();
            let mut segment_fields = Vec::with_capacity(columns.len());
            for column in columns {
                let reader = open_sql_field_reader(&schema, fast_fields, column);
                if matches!(reader, SqlFieldReader::SourceFallback) {
                    needs_stored_doc = true;
                }
                segment_fields.push(reader);
            }
            field_plans.push(segment_fields);

            // Only open _id fast-field reader if we actually need _id
            let id_reader = if needs_id {
                StringFastFieldReader::open(fast_fields, "_id")
            } else {
                None
            };
            id_readers.push(id_reader);
        }

        let mut ids = Vec::with_capacity(if needs_id { top_docs.len() } else { 0 });
        let mut scores = Vec::with_capacity(if needs_score { top_docs.len() } else { 0 });

        // Fast path: use column cache when no SourceFallback columns are needed.
        // This avoids per-doc serde_json::Value allocation by working directly with Arrow arrays,
        // including zero-column queries like `SELECT 1 ...` that still need one output row per hit.
        let use_cache = !needs_stored_doc;

        if use_cache {
            // Group matching doc IDs by segment ordinal
            let mut seg_docs: Vec<Vec<(u32, f32)>> = vec![Vec::new(); segment_readers.len()];
            for (score, doc_address) in &top_docs {
                let seg_ord = doc_address.segment_ord as usize;
                seg_docs[seg_ord].push((doc_address.doc_id, *score));
            }

            // Collect scores in doc order. _id now uses the same per-segment
            // projection path as other fast-field columns.
            for (score, _) in &top_docs {
                if needs_score {
                    scores.push(*score);
                }
            }

            use datafusion::arrow::array::UInt32Array;

            let id_array = if needs_id {
                let mut segment_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();

                for (seg_ord, docs) in seg_docs.iter().enumerate() {
                    if docs.is_empty() {
                        continue;
                    }

                    let taken = if let Some(id_reader) = id_readers[seg_ord].as_ref() {
                        let reader = SqlFieldReader::Str(id_reader.clone());
                        build_projected_fast_field_array(
                            self.column_cache.as_ref(),
                            segment_readers[seg_ord].segment_id(),
                            segment_readers[seg_ord].max_doc(),
                            "_id",
                            &reader,
                            docs,
                        )?
                    } else {
                        std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![
                            "";
                            docs.len()
                        ]))
                    };

                    segment_arrays.push(taken);
                }

                let refs: Vec<&dyn datafusion::arrow::array::Array> =
                    segment_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated: datafusion::arrow::array::ArrayRef = if refs.is_empty() {
                    std::sync::Arc::new(datafusion::arrow::array::StringArray::from(
                        Vec::<&str>::new(),
                    ))
                } else {
                    datafusion::arrow::compute::concat(&refs)?
                };
                Some(concatenated)
            } else {
                None
            };

            // For each column, build per-segment Arrow arrays via cache, then take() matching rows

            let mut result_columns: Vec<(String, datafusion::arrow::array::ArrayRef)> = Vec::new();

            for (col_idx, column) in columns.iter().enumerate() {
                let mut segment_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();

                for (seg_ord, docs) in seg_docs.iter().enumerate() {
                    if docs.is_empty() {
                        continue;
                    }
                    let seg_id = segment_readers[seg_ord].segment_id();
                    let max_doc = segment_readers[seg_ord].max_doc();
                    let reader = &field_plans[seg_ord][col_idx];

                    let taken = build_projected_fast_field_array(
                        self.column_cache.as_ref(),
                        seg_id,
                        max_doc,
                        column,
                        reader,
                        docs,
                    )?;
                    segment_arrays.push(taken);
                }

                // Concatenate across segments (preserving top_docs order within each segment)
                let refs: Vec<&dyn datafusion::arrow::array::Array> =
                    segment_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = if refs.is_empty() {
                    // Empty result — build typed empty array from schema
                    let kind = type_hint_from_schema(&schema, column);
                    empty_typed_array(kind)
                } else {
                    datafusion::arrow::compute::concat(&refs)?
                };
                result_columns.push((column.clone(), concatenated));
            }

            // Build the RecordBatch from the cached/taken columns + ids + scores
            // We need to reorder rows to match the original top_docs order since we grouped by segment.
            // Build a mapping: for each (seg_ord, position_in_seg_docs) → position in top_docs
            let mut seg_positions: Vec<usize> = vec![0; segment_readers.len()];
            let mut reorder_indices: Vec<u32> = Vec::with_capacity(top_docs.len());

            // First pass: compute output offset per segment
            let mut seg_offsets: Vec<usize> = Vec::with_capacity(segment_readers.len());
            let mut offset = 0;
            for docs in &seg_docs {
                seg_offsets.push(offset);
                offset += docs.len();
            }

            // For each doc in original top_docs order, find its position in the concatenated output
            for (_, doc_address) in &top_docs {
                let seg_ord = doc_address.segment_ord as usize;
                let pos = seg_offsets[seg_ord] + seg_positions[seg_ord];
                reorder_indices.push(pos as u32);
                seg_positions[seg_ord] += 1;
            }
            let reorder_array = UInt32Array::from(reorder_indices);

            // Reorder all columns to match original top_docs order
            let mut schema_fields = Vec::new();
            let mut ordered_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();
            let num_rows = top_docs.len();

            // _id column — empty strings if not needed
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                "_id",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            ));
            if needs_id {
                let reordered = if let Some(arr) = &id_array {
                    datafusion::arrow::compute::take(arr.as_ref(), &reorder_array, None)?
                } else {
                    let empty_ids: datafusion::arrow::array::ArrayRef = std::sync::Arc::new(
                        datafusion::arrow::array::StringArray::from(vec![""; num_rows]),
                    );
                    empty_ids
                };
                ordered_arrays.push(reordered);
            } else {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::StringArray::from(vec![""; num_rows]),
                ));
            }

            // score column — zeros if not needed
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                "_score",
                datafusion::arrow::datatypes::DataType::Float32,
                false,
            ));
            if needs_score {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::Float32Array::from(scores),
                ));
            } else {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::Float32Array::from(vec![0.0f32; num_rows]),
                ));
            }

            // Data columns — reorder each to match top_docs order
            for (name, arr) in &result_columns {
                let reordered =
                    datafusion::arrow::compute::take(arr.as_ref(), &reorder_array, None)?;
                let dt = reordered.data_type().clone();
                schema_fields.push(datafusion::arrow::datatypes::Field::new(name, dt, true));
                ordered_arrays.push(reordered);
            }

            let schema =
                std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(schema_fields));
            let batch =
                datafusion::arrow::record_batch::RecordBatch::try_new(schema, ordered_arrays)?;
            return Ok(super::SqlBatchResult { batch, total_hits });
        }

        // Fallback: per-doc reading (used when SourceFallback columns are needed)
        let mut projected_columns = std::collections::BTreeMap::new();
        for column in columns {
            projected_columns.insert(column.clone(), Vec::with_capacity(top_docs.len()));
        }

        for (score, doc_address) in top_docs {
            let seg_ord = doc_address.segment_ord as usize;
            let doc_id = doc_address.doc_id;

            // Load stored doc only when needed for SourceFallback columns
            let retrieved_doc = if needs_stored_doc {
                Some(searcher.doc::<TantivyDocument>(doc_address)?)
            } else {
                None
            };

            // Read _id only if the SQL query references it
            if needs_id {
                let id_str = if needs_stored_doc {
                    retrieved_doc
                        .as_ref()
                        .unwrap()
                        .get_all(registry.id_field)
                        .next()
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else if let Some(ref id_reader) = id_readers[seg_ord] {
                    let mut text = String::new();
                    if id_reader.first_text(doc_id, &mut text) {
                        text
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                ids.push(id_str);
            }

            // Read score only if the SQL query references it
            if needs_score {
                scores.push(score);
            }

            let mut source_json = None;
            for (index, column) in columns.iter().enumerate() {
                let value = match &field_plans[seg_ord][index] {
                    SqlFieldReader::F64(reader) => reader
                        .first(doc_id)
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                    SqlFieldReader::I64(reader) => reader
                        .first(doc_id)
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                    SqlFieldReader::Str(reader) => {
                        let mut text = String::new();
                        if reader.first_text(doc_id, &mut text) {
                            serde_json::Value::String(text)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    SqlFieldReader::SourceFallback => {
                        let source = source_json.get_or_insert_with(|| {
                            retrieved_doc
                                .as_ref()
                                .unwrap()
                                .get_all(registry.source_field)
                                .next()
                                .and_then(|value| value.as_str())
                                .and_then(|text| {
                                    serde_json::from_str::<serde_json::Value>(text).ok()
                                })
                                .and_then(|value| value.as_object().cloned())
                                .unwrap_or_default()
                        });
                        source
                            .get(column)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    }
                };
                projected_columns
                    .get_mut(column)
                    .expect("projected SQL column should exist")
                    .push(value);
            }
        }

        let column_store =
            crate::hybrid::column_store::ColumnStore::new(ids, scores, projected_columns);

        // Build type hints from the SqlFieldReader variants so that zero-result
        // queries still produce correctly-typed Arrow columns (e.g. Float64 for
        // price) instead of defaulting to Utf8.
        let mut type_hints = std::collections::HashMap::new();
        if let Some(first_segment) = field_plans.first() {
            for (i, column) in columns.iter().enumerate() {
                let kind = match &first_segment[i] {
                    SqlFieldReader::F64(_) => crate::hybrid::arrow_bridge::ColumnKind::Float64,
                    SqlFieldReader::I64(_) => crate::hybrid::arrow_bridge::ColumnKind::Int64,
                    SqlFieldReader::Str(_) => crate::hybrid::arrow_bridge::ColumnKind::Utf8,
                    SqlFieldReader::SourceFallback => continue,
                };
                type_hints.insert(column.clone(), kind);
            }
        } else {
            // No segments — derive types from the Tantivy schema directly
            for column in columns {
                if let Ok(field) = schema.get_field(column) {
                    let kind = match schema.get_field_entry(field).field_type() {
                        tantivy::schema::FieldType::F64(_) => {
                            crate::hybrid::arrow_bridge::ColumnKind::Float64
                        }
                        tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {
                            crate::hybrid::arrow_bridge::ColumnKind::Int64
                        }
                        tantivy::schema::FieldType::Bool(_) => {
                            crate::hybrid::arrow_bridge::ColumnKind::Boolean
                        }
                        _ => crate::hybrid::arrow_bridge::ColumnKind::Utf8,
                    };
                    type_hints.insert(column.clone(), kind);
                }
            }
        }
        let batch =
            crate::hybrid::arrow_bridge::build_record_batch_with_hints(&column_store, &type_hints)?;
        Ok(super::SqlBatchResult { batch, total_hits })
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
        Self::build_tantivy_doc_inner(&registry, &self.index.schema(), doc_id, payload)
    }

    /// Build a Tantivy document using an already-acquired registry reference.
    /// Used by bulk paths to avoid per-doc RwLock acquisition.
    fn build_tantivy_doc_inner(
        registry: &FieldRegistry,
        schema: &Schema,
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
                if let Some(&field) = registry.fields.get(key.as_str())
                    && field != body_field
                {
                    match value {
                        serde_json::Value::String(s) => {
                            doc.add_text(field, s);
                        }
                        serde_json::Value::Number(n) => {
                            use tantivy::schema::FieldType;
                            match schema.get_field_entry(field).field_type() {
                                FieldType::F64(_) => {
                                    let f = n.as_f64().unwrap_or(n.as_i64().unwrap_or(0) as f64);
                                    doc.add_f64(field, f);
                                }
                                FieldType::I64(_) => {
                                    let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                                    doc.add_i64(field, i);
                                }
                                FieldType::U64(_) => {
                                    let u = n.as_u64().unwrap_or(n.as_f64().unwrap_or(0.0) as u64);
                                    doc.add_u64(field, u);
                                }
                                _ => {}
                            }
                        }
                        serde_json::Value::Bool(b) => {
                            doc.add_text(field, if *b { "true" } else { "false" });
                        }
                        _ => {}
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
        let committed_next_seq = self.load_committed_next_seq_no()?;
        let entries = {
            let tl = self.translog.lock().unwrap();
            tl.read_all()?
                .into_iter()
                .filter(|entry| entry.seq_no >= committed_next_seq)
                .collect::<Vec<_>>()
        };

        if entries.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            "Replaying {} translog entries from seq_no {} after restart...",
            entries.len(),
            committed_next_seq
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
        self.persist_committed_next_seq_no(
            entries
                .last()
                .map(|entry| entry.seq_no + 1)
                .unwrap_or(committed_next_seq),
        )?;

        tracing::info!(
            "Translog replay complete. {} documents recovered.",
            entries.len()
        );
        Ok(())
    }

    fn load_committed_next_seq_no(&self) -> Result<u64> {
        if !self.committed_seq_no_path.exists() {
            return Ok(0);
        }
        let s = std::fs::read_to_string(&self.committed_seq_no_path)?;
        Ok(s.trim().parse::<u64>().unwrap_or(0))
    }

    fn persist_committed_next_seq_no(&self, next_seq_no: u64) -> Result<()> {
        std::fs::write(&self.committed_seq_no_path, next_seq_no.to_string())?;
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
                if let Some(text) = value.as_str()
                    && let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text)
                {
                    results.push(serde_json::json!({
                        "_id": doc_id,
                        "_score": score,
                        "_source": json_val
                    }));
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
        use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Occur, TermQuery};
        use tantivy::schema::IndexRecordOption;

        match clause {
            QueryClause::MatchAll(_) => Ok(Box::new(AllQuery)),
            QueryClause::MatchNone(_) => Ok(Box::new(EmptyQuery)),
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
                    let target_field = self.resolve_field(field_name);
                    let term = self.typed_term(target_field, value);
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

                    let to_term =
                        |v: &serde_json::Value| -> Term { self.typed_term(target_field, v) };

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
                    let term = self.typed_term(
                        target_field,
                        &serde_json::Value::String(params.value.clone()),
                    );
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
        let tl = self.translog.lock().unwrap();
        let committed_next_seq = tl.next_seq_no();
        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.commit()?;
        drop(writer);
        self.reader.reload()?;
        self.persist_committed_next_seq_no(committed_next_seq)?;
        if global_checkpoint > 0 {
            tl.truncate_below(global_checkpoint)?;
        } else {
            tl.truncate()?;
        }
        Ok(())
    }

    /// Extract the primary sort field and direction from a SearchRequest,
    /// if eligible for fast-field optimization (numeric FAST field, not _score).
    fn extract_fast_field_sort(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Option<(String, tantivy::Order)> {
        use crate::search::{SortClause, SortDirection, SortOrder};
        if req.sort.is_empty() {
            return None;
        }
        let clause = &req.sort[0];
        match clause {
            SortClause::Simple(name) if name != "_score" => {
                let field = self.resolve_field(name);
                let schema = self.index.schema();
                let entry = schema.get_field_entry(field);
                match entry.field_type() {
                    tantivy::schema::FieldType::I64(opts)
                    | tantivy::schema::FieldType::F64(opts)
                    | tantivy::schema::FieldType::U64(opts)
                        if opts.is_fast() =>
                    {
                        Some((name.clone(), tantivy::Order::Asc))
                    }
                    _ => None,
                }
            }
            SortClause::Field(map) => {
                if let Some((name, order)) = map.iter().next() {
                    if name == "_score" {
                        return None;
                    }
                    let field = self.resolve_field(name);
                    let schema = self.index.schema();
                    let entry = schema.get_field_entry(field);
                    let is_fast = match entry.field_type() {
                        tantivy::schema::FieldType::I64(opts)
                        | tantivy::schema::FieldType::F64(opts)
                        | tantivy::schema::FieldType::U64(opts) => opts.is_fast(),
                        _ => false,
                    };
                    if !is_fast {
                        return None;
                    }
                    let dir = match order {
                        SortOrder::Direction(d) => d.clone(),
                        SortOrder::Object { order } => order.clone(),
                    };
                    let tantivy_order = match dir {
                        SortDirection::Asc => tantivy::Order::Asc,
                        SortDirection::Desc => tantivy::Order::Desc,
                    };
                    Some((name.clone(), tantivy_order))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Collect hits from fast-field-sorted results (score is not meaningful).
    fn collect_hits_sorted<T>(
        &self,
        searcher: &tantivy::Searcher,
        top_docs: Vec<(T, tantivy::DocAddress)>,
    ) -> Result<Vec<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let mut results = Vec::new();
        for (_sort_value, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            let doc_id = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str()
                    && let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text)
                {
                    results.push(serde_json::json!({
                        "_id": doc_id,
                        "_score": 0.0,
                        "_source": json_val
                    }));
                }
            }
        }
        Ok(results)
    }

    /// Direct columnar scan for match_all + grouped_partials queries.
    /// Bypasses Tantivy's search/collector machinery entirely — iterates segment
    /// fast-field columns in batches of BATCH_SIZE without scoring or posting-list
    /// traversal. For single-column keyword GROUP BY, uses flat arrays indexed by
    /// ordinal instead of HashMap — eliminates hash computation, collision handling,
    /// and per-group heap allocation. Returns the same PartialAggResult as the collector path.
    fn grouped_partials_direct_scan(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<std::collections::HashMap<String, crate::search::PartialAggResult>> {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let segment_readers = searcher.segment_readers();

        // Extract shard_top_k from the first grouped metrics agg (if any).
        let shard_top_k: Option<&crate::search::ShardTopK> =
            req.aggs.values().find_map(|agg| match agg {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    params.shard_top_k.as_ref()
                }
                _ => None,
            });

        let specs: Vec<ResolvedGroupedAggSpec> = req
            .aggs
            .iter()
            .filter_map(|(name, agg)| match agg {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    Some(ResolvedGroupedAggSpec {
                        name: name.clone(),
                        group_by: params.group_by.clone(),
                        metrics: params
                            .metrics
                            .iter()
                            .map(|m| ResolvedGroupedMetricSpec {
                                output_name: m.output_name.clone(),
                                function: m.function.clone(),
                                field_name: m.field.clone(),
                            })
                            .collect(),
                    })
                }
                _ => None,
            })
            .collect();

        // Parallel segment scan: each segment is scanned independently using scoped threads.
        // Uses std::thread::scope instead of rayon to avoid nested-pool deadlocks
        // (this method is already called from the search rayon pool).
        let all_segment_fruits: Vec<Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>> =
            std::thread::scope(|s| {
                let handles: Vec<_> = segment_readers
                    .iter()
                    .map(|segment_reader| {
                        let schema = &schema;
                        let specs = &specs;
                        s.spawn(move || {
                            let ff = segment_reader.fast_fields();
                            let max_doc = segment_reader.max_doc();

                            let mut segment_results: Vec<(
                                String,
                                Vec<crate::search::GroupedMetricsBucket>,
                            )> = Vec::with_capacity(specs.len());

                            for spec in specs.iter() {
                                // Open key readers
                                let mut key_readers = Vec::with_capacity(spec.group_by.len());
                                let mut unsupported = false;
                                for field_name in &spec.group_by {
                                    match open_group_key_reader(schema, ff, field_name) {
                                        Some(reader) => key_readers.push(reader),
                                        None => {
                                            unsupported = true;
                                            break;
                                        }
                                    }
                                }
                                if unsupported {
                                    continue;
                                }

                                // Open metric readers + build layout
                                let mut metric_entries = Vec::with_capacity(spec.metrics.len());
                                for metric in &spec.metrics {
                                    let source = match metric.function {
                                        crate::search::GroupedMetricFunction::Count => {
                                            match &metric.field_name {
                                                None => GroupedMetricSource::CountAll,
                                                Some(field_name) => {
                                                    let reader = open_sql_field_reader(
                                                        schema, ff, field_name,
                                                    );
                                                    if matches!(
                                                        reader,
                                                        SqlFieldReader::SourceFallback
                                                    ) {
                                                        unsupported = true;
                                                        break;
                                                    }
                                                    GroupedMetricSource::CountField(reader)
                                                }
                                            }
                                        }
                                        crate::search::GroupedMetricFunction::Sum
                                        | crate::search::GroupedMetricFunction::Avg
                                        | crate::search::GroupedMetricFunction::Min
                                        | crate::search::GroupedMetricFunction::Max => {
                                            let Some(field_name) = &metric.field_name else {
                                                unsupported = true;
                                                break;
                                            };
                                            let Some(column) = open_num_col(schema, ff, field_name)
                                            else {
                                                unsupported = true;
                                                break;
                                            };
                                            GroupedMetricSource::Numeric(column)
                                        }
                                    };
                                    if unsupported {
                                        break;
                                    }
                                    metric_entries.push(GroupedMetricEntry {
                                        output_name: metric.output_name.clone(),
                                        function: metric.function.clone(),
                                        source,
                                    });
                                }
                                if unsupported {
                                    continue;
                                }

                                // Determine if we can use flat array path:
                                // single-column keyword GROUP BY with known dictionary size < 2M
                                let use_flat = key_readers.len() == 1
                                    && matches!(key_readers[0], GroupKeyReader::Str(_))
                                    && key_readers[0].num_terms() < 2_000_000;

                                if use_flat {
                                    let num_groups = key_readers[0].num_terms();
                                    // Do NOT pass shard_top_k here — per-segment pruning
                                    // is incorrect because a group below the cutoff in every
                                    // segment can still be top-K after segment totals are
                                    // merged. Shard-level pruning happens after segment merge
                                    // at the end of grouped_partials_direct_scan.
                                    let buckets = flat_scan_segment(
                                        &key_readers[0],
                                        &metric_entries,
                                        max_doc,
                                        num_groups,
                                        None,
                                    );
                                    segment_results.push((spec.name.clone(), buckets));
                                } else {
                                    // Fallback: HashMap-based accumulation (multi-column or huge cardinality)
                                    let approx_groups =
                                        key_readers.first().map(|r| r.num_terms()).unwrap_or(256);
                                    let multi_col = key_readers.len() > 1;
                                    let buckets = if key_readers.is_empty() {
                                        GroupedBuckets::Global(None)
                                    } else if multi_col {
                                        GroupedBuckets::Multi(
                                            std::collections::HashMap::with_capacity(approx_groups),
                                        )
                                    } else {
                                        GroupedBuckets::Single(
                                            OrdHashMap::with_capacity_and_hasher(
                                                approx_groups,
                                                OrdBuildHasher::default(),
                                            ),
                                        )
                                    };

                                    let mut numeric_buf_map: Vec<Option<usize>> =
                                        Vec::with_capacity(metric_entries.len());
                                    let mut num_numeric = 0usize;
                                    for me in &metric_entries {
                                        if matches!(me.source, GroupedMetricSource::Numeric(_)) {
                                            numeric_buf_map.push(Some(num_numeric));
                                            num_numeric += 1;
                                        } else {
                                            numeric_buf_map.push(None);
                                        }
                                    }
                                    let mut accum_template =
                                        Vec::with_capacity(metric_entries.len());
                                    for me in &metric_entries {
                                        accum_template.push(match &me.source {
                                            GroupedMetricSource::CountAll
                                            | GroupedMetricSource::CountField(_) => {
                                                CompactMetricAccum::Count(0)
                                            }
                                            GroupedMetricSource::Numeric(_) => {
                                                CompactMetricAccum::Stats {
                                                    count: 0,
                                                    sum: 0.0,
                                                    min: f64::INFINITY,
                                                    max: f64::NEG_INFINITY,
                                                }
                                            }
                                        });
                                    }
                                    let numeric_buffers: Vec<Vec<Option<f64>>> =
                                        (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();

                                    let mut entry = GroupedAggSegmentEntry {
                                        key_readers,
                                        metric_entries,
                                        buckets,
                                        accum_template,
                                        doc_buffer: Vec::with_capacity(BATCH_SIZE),
                                        ord_buffer: vec![None; BATCH_SIZE],
                                        numeric_buffers,
                                        numeric_buf_map,
                                    };

                                    // Batched accumulation: buffer docs in chunks of BATCH_SIZE,
                                    // batch-read ordinals and numerics, then accumulate.
                                    // Avoids per-doc fast-field reads and Vec allocations.
                                    for doc in 0..max_doc {
                                        entry.doc_buffer.push(doc);
                                        if entry.doc_buffer.len() >= BATCH_SIZE {
                                            flush_batch_multi(&mut entry);
                                        }
                                    }
                                    if !entry.doc_buffer.is_empty() {
                                        flush_batch_multi(&mut entry);
                                    }

                                    let raw_buckets: Vec<OrdGroupedBucket> = match entry.buckets {
                                        GroupedBuckets::Single(map) => map.into_values().collect(),
                                        GroupedBuckets::Multi(map) => map.into_values().collect(),
                                        GroupedBuckets::Global(opt) => opt.into_iter().collect(),
                                    };
                                    let resolved: Vec<crate::search::GroupedMetricsBucket> =
                                        raw_buckets
                                            .into_iter()
                                            .map(|b| {
                                                resolve_bucket(
                                                    b,
                                                    &entry.key_readers,
                                                    &entry.metric_entries,
                                                )
                                            })
                                            .collect();
                                    segment_results.push((spec.name.clone(), resolved));
                                }
                            }
                            segment_results
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("segment scan thread panicked"))
                    .collect()
            });

        // Merge across segments
        let mut merged: std::collections::HashMap<
            String,
            std::collections::HashMap<String, crate::search::GroupedMetricsBucket>,
        > = std::collections::HashMap::new();

        for fruit in all_segment_fruits {
            for (name, buckets) in fruit {
                let agg_buckets = merged.entry(name).or_default();
                for bucket in buckets {
                    let key = compact_group_key(&bucket.group_values);
                    let target = agg_buckets.entry(key).or_insert_with(|| {
                        crate::search::GroupedMetricsBucket {
                            group_values: bucket.group_values.clone(),
                            metrics: std::collections::HashMap::new(),
                        }
                    });
                    merge_grouped_bucket_metrics(target, &bucket);
                }
            }
        }

        let mut results = std::collections::HashMap::new();
        for spec in &specs {
            let mut buckets: Vec<_> = merged
                .remove(&spec.name)
                .unwrap_or_default()
                .into_values()
                .collect();

            // Shard-level top-K pruning after segment merge.
            if let Some(top_k) = shard_top_k {
                apply_shard_top_k(&mut buckets, top_k);
            }

            results.insert(
                spec.name.clone(),
                crate::search::PartialAggResult::GroupedMetrics { buckets },
            );
        }
        Ok(results)
    }
}

// -- Single-pass Aggregation Collector --
// Implements tantivy::collector::Collector to compute aggregations in the same
// pass as TopDocs hit collection, mirroring OpenSearch's aggregation architecture.

enum NumCol {
    F64(tantivy::columnar::Column<f64>),
    I64(tantivy::columnar::Column<i64>),
}

impl NumCol {
    #[inline]
    fn first_f64(&self, doc: u32) -> Option<f64> {
        match self {
            NumCol::F64(c) => c.first(doc),
            NumCol::I64(c) => c.first(doc).map(|v| v as f64),
        }
    }

    /// Batch-read f64 values for a slice of doc IDs.
    /// Much faster than per-doc `first_f64()` due to sequential memory access patterns.
    #[inline]
    fn first_vals_f64(&self, docs: &[tantivy::DocId], output: &mut [Option<f64>]) {
        match self {
            NumCol::F64(col) => {
                col.first_vals(docs, output);
            }
            NumCol::I64(col) => {
                let mut i64_buf: Vec<Option<i64>> = vec![None; docs.len()];
                col.first_vals(docs, &mut i64_buf);
                for (i, val) in i64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v as f64);
                }
            }
        }
    }
}

fn open_num_col(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
) -> Option<NumCol> {
    let field = schema.get_field(field_name).ok()?;
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => fast_fields.f64(entry.name()).ok().map(NumCol::F64),
        tantivy::schema::FieldType::I64(_) => fast_fields.i64(entry.name()).ok().map(NumCol::I64),
        tantivy::schema::FieldType::U64(_) => fast_fields.i64(entry.name()).ok().map(NumCol::I64),
        _ => None,
    }
}

/// Identity hasher for u64 ordinal keys — ordinals are already well-distributed
/// dictionary indices, so hashing them is wasted work. This eliminates hash
/// computation overhead in the per-doc GROUP BY hot path.
#[derive(Default)]
struct OrdHasher(u64);

impl std::hash::Hasher for OrdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _bytes: &[u8]) {}
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type OrdBuildHasher = std::hash::BuildHasherDefault<OrdHasher>;
type OrdHashMap<V> = std::collections::HashMap<u64, V, OrdBuildHasher>;

#[derive(Clone)]
struct StringFastFieldReader {
    str_col: tantivy::columnar::StrColumn,
    ord_col: tantivy::columnar::Column<u64>,
}

impl StringFastFieldReader {
    fn open(fast_fields: &tantivy::fastfield::FastFieldReaders, field_name: &str) -> Option<Self> {
        let str_col = fast_fields.str(field_name).ok().flatten()?;
        let ord_col = str_col.ords().clone();
        Some(Self { str_col, ord_col })
    }

    #[inline]
    fn first_ord(&self, doc: tantivy::DocId) -> Option<u64> {
        self.ord_col.first(doc)
    }

    #[inline]
    fn first_ords_batch(&self, docs: &[tantivy::DocId], output: &mut [Option<u64>]) {
        self.ord_col.first_vals(docs, output);
    }

    #[inline]
    fn first_text(&self, doc: tantivy::DocId, buf: &mut String) -> bool {
        let Some(ord) = self.first_ord(doc) else {
            return false;
        };
        buf.clear();
        self.ord_to_str(ord, buf)
    }

    #[inline]
    fn ord_to_str(&self, ord: u64, buf: &mut String) -> bool {
        self.str_col.ord_to_str(ord, buf).unwrap_or(false)
    }

    fn num_terms(&self) -> usize {
        self.str_col.num_terms()
    }
}

enum SqlFieldReader {
    F64(tantivy::columnar::Column<f64>),
    I64(tantivy::columnar::Column<i64>),
    Str(StringFastFieldReader),
    SourceFallback,
}

impl SqlFieldReader {
    fn type_name(&self) -> &'static str {
        match self {
            SqlFieldReader::F64(_) => "F64",
            SqlFieldReader::I64(_) => "I64",
            SqlFieldReader::Str(_) => "Str",
            SqlFieldReader::SourceFallback => "SourceFallback",
        }
    }

    /// Read the first value for a doc as JSON. Used for count(field) null checks.
    #[allow(dead_code)]
    fn first_json(&self, doc: tantivy::DocId) -> serde_json::Value {
        match self {
            SqlFieldReader::F64(reader) => reader
                .first(doc)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            SqlFieldReader::I64(reader) => reader
                .first(doc)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            SqlFieldReader::Str(reader) => {
                let mut text = String::new();
                if reader.first_text(doc, &mut text) {
                    serde_json::Value::String(text)
                } else {
                    serde_json::Value::Null
                }
            }
            SqlFieldReader::SourceFallback => serde_json::Value::Null,
        }
    }
}

fn open_sql_field_reader(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
) -> SqlFieldReader {
    let Ok(field) = schema.get_field(field_name) else {
        return SqlFieldReader::SourceFallback;
    };
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => fast_fields
            .f64(entry.name())
            .map(SqlFieldReader::F64)
            .unwrap_or(SqlFieldReader::SourceFallback),
        tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => fast_fields
            .i64(entry.name())
            .map(SqlFieldReader::I64)
            .unwrap_or(SqlFieldReader::SourceFallback),
        _ => StringFastFieldReader::open(fast_fields, entry.name())
            .map(SqlFieldReader::Str)
            .unwrap_or(SqlFieldReader::SourceFallback),
    }
}

/// Build a full-segment Arrow array from a fast-field reader.
/// Reads every doc in the segment (0..max_doc) to produce a complete column.
fn build_full_segment_array(
    reader: &SqlFieldReader,
    max_doc: u32,
) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
    use std::sync::Arc;

    match reader {
        SqlFieldReader::F64(col) => {
            let mut builder = Float64Builder::with_capacity(max_doc as usize);
            for doc in 0..max_doc {
                match col.first(doc) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::I64(col) => {
            let mut builder = Int64Builder::with_capacity(max_doc as usize);
            for doc in 0..max_doc {
                match col.first(doc) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::Str(col) => {
            let mut builder = StringBuilder::with_capacity(max_doc as usize, 0);
            let mut buf = String::new();
            for doc in 0..max_doc {
                if col.first_text(doc, &mut buf) {
                    builder.append_value(&buf);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::SourceFallback => {
            // Should never be called for SourceFallback — pre-checked by caller
            Arc::new(datafusion::arrow::array::StringBuilder::new().finish())
        }
    }
}

fn should_cache_full_segment_array(reader: &SqlFieldReader, max_doc: u32, cache_max: u64) -> bool {
    cache_max > 0 && estimate_full_segment_array_bytes(reader, max_doc) <= cache_max / 4
}

fn estimate_full_segment_array_bytes(reader: &SqlFieldReader, max_doc: u32) -> u64 {
    let doc_count = u64::from(max_doc);
    let null_bitmap_bytes = doc_count.saturating_add(7) / 8;

    match reader {
        SqlFieldReader::F64(_) | SqlFieldReader::I64(_) => doc_count
            .saturating_mul(std::mem::size_of::<f64>() as u64)
            .saturating_add(null_bitmap_bytes),
        SqlFieldReader::Str(col) => {
            let offset_bytes = doc_count
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<i32>() as u64);
            let avg_term_len = estimate_string_array_value_bytes(col);
            offset_bytes
                .saturating_add(null_bitmap_bytes)
                .saturating_add(doc_count.saturating_mul(avg_term_len))
        }
        SqlFieldReader::SourceFallback => 0,
    }
}

fn estimate_string_array_value_bytes(col: &StringFastFieldReader) -> u64 {
    const MAX_SAMPLES: usize = 32;

    let num_terms = col.num_terms();
    if num_terms == 0 {
        return 16;
    }

    let sample_count = num_terms.min(MAX_SAMPLES);
    let step = (num_terms.saturating_add(sample_count - 1) / sample_count).max(1);
    let mut total_len = 0u64;
    let mut sampled = 0u64;
    let mut buf = String::new();
    let mut ord = 0usize;

    while ord < num_terms && sampled < sample_count as u64 {
        buf.clear();
        if col.ord_to_str(ord as u64, &mut buf) {
            total_len = total_len.saturating_add(buf.len() as u64);
            sampled += 1;
        }
        ord = ord.saturating_add(step);
    }

    if sampled == 0 {
        return 16;
    }

    // 2× safety margin: dictionary term lengths don't account for multi-valued docs,
    // null bitmap overhead, or Arrow offset array padding. Over-estimating is safe
    // (it just falls back to build_selective_array), under-estimating causes a large
    // allocation that gets rejected by the cache insert guard.
    total_len.saturating_mul(2).saturating_div(sampled).max(16)
}

/// Build an Arrow array containing only the values at specific doc IDs.
/// Used when the segment is too large to cache the full column.
fn build_selective_array(
    reader: &SqlFieldReader,
    docs: &[(u32, f32)],
) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
    use std::sync::Arc;

    match reader {
        SqlFieldReader::F64(col) => {
            let mut builder = Float64Builder::with_capacity(docs.len());
            for (doc_id, _) in docs {
                match col.first(*doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::I64(col) => {
            let mut builder = Int64Builder::with_capacity(docs.len());
            for (doc_id, _) in docs {
                match col.first(*doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::Str(col) => {
            let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 16);
            let doc_ids: Vec<tantivy::DocId> = docs.iter().map(|(doc_id, _)| *doc_id).collect();
            let mut ords = vec![None; docs.len()];
            col.first_ords_batch(&doc_ids, &mut ords);
            let mut buf = String::new();
            for ord in ords {
                if let Some(ord) = ord {
                    buf.clear();
                    if col.ord_to_str(ord, &mut buf) {
                        builder.append_value(&buf);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::SourceFallback => Arc::new(StringBuilder::new().finish()),
    }
}

fn build_projected_fast_field_array(
    column_cache: &super::column_cache::ColumnCache,
    segment_id: tantivy::index::SegmentId,
    max_doc: u32,
    column_name: &str,
    reader: &SqlFieldReader,
    docs: &[(u32, f32)],
) -> Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::UInt32Array;

    let cache_max = column_cache.max_capacity();
    let use_segment_cache = should_cache_full_segment_array(reader, max_doc, cache_max);

    if use_segment_cache {
        let full_array = if let Some(cached) = column_cache.get(segment_id, column_name) {
            cached
        } else if column_cache.should_populate(docs.len(), max_doc) {
            let array = build_full_segment_array(reader, max_doc);
            column_cache.insert(segment_id, column_name, array.clone());
            array
        } else {
            return Ok(build_selective_array(reader, docs));
        };

        let indices = UInt32Array::from(docs.iter().map(|(d, _)| *d).collect::<Vec<u32>>());
        return Ok(datafusion::arrow::compute::take(
            &full_array,
            &indices,
            None,
        )?);
    }

    Ok(build_selective_array(reader, docs))
}

/// Determine column type from Tantivy schema (for building typed empty arrays).
fn type_hint_from_schema(schema: &Schema, column: &str) -> crate::hybrid::arrow_bridge::ColumnKind {
    if let Ok(field) = schema.get_field(column) {
        match schema.get_field_entry(field).field_type() {
            tantivy::schema::FieldType::F64(_) => crate::hybrid::arrow_bridge::ColumnKind::Float64,
            tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {
                crate::hybrid::arrow_bridge::ColumnKind::Int64
            }
            _ => crate::hybrid::arrow_bridge::ColumnKind::Utf8,
        }
    } else {
        crate::hybrid::arrow_bridge::ColumnKind::Utf8
    }
}

/// Create an empty Arrow array with the correct type.
fn empty_typed_array(
    kind: crate::hybrid::arrow_bridge::ColumnKind,
) -> datafusion::arrow::array::ArrayRef {
    use std::sync::Arc;
    match kind {
        crate::hybrid::arrow_bridge::ColumnKind::Float64 => Arc::new(
            datafusion::arrow::array::Float64Array::from(Vec::<f64>::new()),
        ),
        crate::hybrid::arrow_bridge::ColumnKind::Int64 => {
            Arc::new(datafusion::arrow::array::Int64Array::from(Vec::<i64>::new()))
        }
        crate::hybrid::arrow_bridge::ColumnKind::Boolean => Arc::new(
            datafusion::arrow::array::BooleanArray::from(Vec::<bool>::new()),
        ),
        crate::hybrid::arrow_bridge::ColumnKind::Utf8 => Arc::new(
            datafusion::arrow::array::StringArray::from(Vec::<&str>::new()),
        ),
    }
}

fn open_group_key_reader(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
) -> Option<GroupKeyReader> {
    let field = schema.get_field(field_name).ok()?;
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => {
            fast_fields.f64(entry.name()).ok().map(GroupKeyReader::F64)
        }
        tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {
            fast_fields.i64(entry.name()).ok().map(GroupKeyReader::I64)
        }
        tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::Bytes(_) => {
            StringFastFieldReader::open(fast_fields, entry.name()).map(GroupKeyReader::Str)
        }
        _ => None,
    }
}

struct ResolvedGroupedAggSpec {
    name: String,
    group_by: Vec<String>,
    metrics: Vec<ResolvedGroupedMetricSpec>,
}

struct ResolvedGroupedMetricSpec {
    output_name: String,
    function: crate::search::GroupedMetricFunction,
    field_name: Option<String>,
}

#[allow(dead_code)] // CountField reader will be used when null-aware count(field) is added
enum GroupedMetricSource {
    CountAll,
    CountField(SqlFieldReader),
    Numeric(NumCol),
}

struct GroupedMetricEntry {
    output_name: String,
    function: crate::search::GroupedMetricFunction,
    source: GroupedMetricSource,
}

/// Compact per-doc key reader. Extracts a u64 ordinal per group-by column
/// without allocating Strings or serde_json::Values in the hot path.
enum GroupKeyReader {
    /// String column — reads ordinals from the underlying Column<u64> directly,
    /// bypassing the iterator-based `term_ords()` path.
    Str(StringFastFieldReader),
    I64(tantivy::columnar::Column<i64>),
    F64(tantivy::columnar::Column<f64>),
}

impl GroupKeyReader {
    /// Batch-read ordinal keys for a slice of doc IDs into the output buffer.
    /// Much faster than per-doc reads due to sequential memory access.
    #[inline]
    fn keys_batch(&self, docs: &[tantivy::DocId], output: &mut [Option<u64>]) {
        match self {
            GroupKeyReader::Str(reader) => {
                reader.first_ords_batch(docs, output);
            }
            GroupKeyReader::I64(reader) => {
                // Reinterpret: read i64s into a temp buffer, convert to u64
                let mut i64_buf: Vec<Option<i64>> = vec![None; docs.len()];
                reader.first_vals(docs, &mut i64_buf);
                for (i, val) in i64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v as u64);
                }
            }
            GroupKeyReader::F64(reader) => {
                let mut f64_buf: Vec<Option<f64>> = vec![None; docs.len()];
                reader.first_vals(docs, &mut f64_buf);
                for (i, val) in f64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v.to_bits());
                }
            }
        }
    }

    /// Number of unique terms (for string columns), used for HashMap pre-sizing.
    fn num_terms(&self) -> usize {
        match self {
            GroupKeyReader::Str(reader) => reader.num_terms(),
            _ => 256,
        }
    }

    /// Resolve an ordinal key back to a serde_json::Value (called once per unique group).
    fn resolve(&self, key: u64) -> serde_json::Value {
        if key == u64::MAX {
            return serde_json::Value::Null;
        }
        match self {
            GroupKeyReader::Str(reader) => {
                let mut text = String::new();
                if reader.ord_to_str(key, &mut text) {
                    serde_json::Value::String(text)
                } else {
                    serde_json::Value::Null
                }
            }
            GroupKeyReader::I64(_) => serde_json::Value::from(key as i64),
            GroupKeyReader::F64(_) => serde_json::Value::from(f64::from_bits(key)),
        }
    }
}

/// Compact per-bucket metric accumulator using Vec (indexed by metric position)
/// instead of HashMap<String, _>.
#[derive(Clone)]
enum CompactMetricAccum {
    Count(u64),
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

struct OrdGroupedBucket {
    /// Ordinal keys for each group-by column.
    ord_keys: Vec<u64>,
    /// One accumulator per metric, indexed by position.
    accums: Vec<CompactMetricAccum>,
}

/// Multi-column GROUP BY uses Vec<u64> as the key (collision-free).
/// Single-column uses u64 directly via OrdHashMap (identity hasher).
enum GroupedBuckets {
    /// Single group-by column: u64 ordinal key, identity hasher, zero collisions.
    Single(OrdHashMap<OrdGroupedBucket>),
    /// Multi-column group-by: Vec<u64> composite key, standard hasher, zero collisions.
    Multi(std::collections::HashMap<Vec<u64>, OrdGroupedBucket>),
    /// No group-by columns (ungrouped aggregate): single global bucket.
    Global(Option<OrdGroupedBucket>),
}

struct GroupedAggSegmentEntry {
    key_readers: Vec<GroupKeyReader>,
    metric_entries: Vec<GroupedMetricEntry>,
    buckets: GroupedBuckets,
    /// Template of initial accumulators (one per metric).
    accum_template: Vec<CompactMetricAccum>,
    /// Buffered doc IDs for batch ordinal reads.
    doc_buffer: Vec<tantivy::DocId>,
    /// Reusable ordinal output buffer (avoids allocation per flush).
    ord_buffer: Vec<Option<u64>>,
    /// Reusable numeric value buffers — one per numeric metric (avoids per-doc reads).
    /// Indices correspond to positions in `metric_entries` that are `Numeric`.
    numeric_buffers: Vec<Vec<Option<f64>>>,
    /// Maps metric_entries index → numeric_buffers index (None if not numeric).
    numeric_buf_map: Vec<Option<usize>>,
}

const BATCH_SIZE: usize = 1024;

pub(crate) struct GroupedAggCollector {
    specs: Vec<ResolvedGroupedAggSpec>,
    schema: Schema,
    shard_top_k: Option<crate::search::ShardTopK>,
}

pub(crate) struct GroupedAggSegmentCollector {
    entries: Vec<(String, Option<GroupedAggSegmentEntry>)>,
}

impl GroupedAggCollector {
    fn has_grouped_metrics(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
    ) -> bool {
        aggs.values()
            .any(|agg| matches!(agg, crate::search::AggregationRequest::GroupedMetrics(_)))
    }

    fn from_request(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
        schema: Schema,
    ) -> Self {
        let specs = aggs
            .iter()
            .filter_map(|(name, req)| match req {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    Some(ResolvedGroupedAggSpec {
                        name: name.clone(),
                        group_by: params.group_by.clone(),
                        metrics: params
                            .metrics
                            .iter()
                            .map(|metric| ResolvedGroupedMetricSpec {
                                output_name: metric.output_name.clone(),
                                function: metric.function.clone(),
                                field_name: metric.field.clone(),
                            })
                            .collect(),
                    })
                }
                _ => None,
            })
            .collect();
        let shard_top_k = aggs.values().find_map(|agg| match agg {
            crate::search::AggregationRequest::GroupedMetrics(params) => params.shard_top_k.clone(),
            _ => None,
        });
        Self {
            specs,
            schema,
            shard_top_k,
        }
    }
}

impl tantivy::collector::Collector for GroupedAggCollector {
    type Fruit = std::collections::HashMap<String, crate::search::PartialAggResult>;
    type Child = GroupedAggSegmentCollector;

    fn for_segment(
        &self,
        _seg_id: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let ff = segment.fast_fields();
        let mut entries = Vec::with_capacity(self.specs.len());

        for spec in &self.specs {
            // Build compact key readers for GROUP BY columns
            let mut key_readers = Vec::with_capacity(spec.group_by.len());
            let mut unsupported_group = false;
            for field_name in &spec.group_by {
                match open_group_key_reader(&self.schema, ff, field_name) {
                    Some(reader) => key_readers.push(reader),
                    None => {
                        unsupported_group = true;
                        break;
                    }
                }
            }
            if unsupported_group {
                entries.push((spec.name.clone(), None));
                continue;
            }

            let mut metric_entries = Vec::with_capacity(spec.metrics.len());
            let mut accum_template = Vec::with_capacity(spec.metrics.len());
            let mut unsupported = false;
            for metric in &spec.metrics {
                let source = match metric.function {
                    crate::search::GroupedMetricFunction::Count => match &metric.field_name {
                        None => GroupedMetricSource::CountAll,
                        Some(field_name) => {
                            let reader = open_sql_field_reader(&self.schema, ff, field_name);
                            if matches!(reader, SqlFieldReader::SourceFallback) {
                                unsupported = true;
                                break;
                            }
                            GroupedMetricSource::CountField(reader)
                        }
                    },
                    crate::search::GroupedMetricFunction::Sum
                    | crate::search::GroupedMetricFunction::Avg
                    | crate::search::GroupedMetricFunction::Min
                    | crate::search::GroupedMetricFunction::Max => {
                        let Some(field_name) = &metric.field_name else {
                            unsupported = true;
                            break;
                        };
                        let Some(column) = open_num_col(&self.schema, ff, field_name) else {
                            unsupported = true;
                            break;
                        };
                        GroupedMetricSource::Numeric(column)
                    }
                };
                let template = match &source {
                    GroupedMetricSource::CountAll | GroupedMetricSource::CountField(_) => {
                        CompactMetricAccum::Count(0)
                    }
                    GroupedMetricSource::Numeric(_) => CompactMetricAccum::Stats {
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    },
                };
                accum_template.push(template);
                metric_entries.push(GroupedMetricEntry {
                    output_name: metric.output_name.clone(),
                    function: metric.function.clone(),
                    source,
                });
            }

            if unsupported {
                entries.push((spec.name.clone(), None));
                continue;
            }

            let multi_col = key_readers.len() > 1;

            // Pre-size the HashMap from dictionary cardinality to avoid rehashing.
            let approx_groups = key_readers.first().map(|r| r.num_terms()).unwrap_or(256);

            let buckets = if key_readers.is_empty() {
                GroupedBuckets::Global(None)
            } else if multi_col {
                GroupedBuckets::Multi(std::collections::HashMap::with_capacity(approx_groups))
            } else {
                GroupedBuckets::Single(OrdHashMap::with_capacity_and_hasher(
                    approx_groups,
                    OrdBuildHasher::default(),
                ))
            };

            // Build numeric buffer mapping: for each metric entry, assign a
            // numeric_buffers index if it's a Numeric source.
            let mut numeric_buf_map: Vec<Option<usize>> = Vec::with_capacity(metric_entries.len());
            let mut num_numeric = 0usize;
            for me in &metric_entries {
                if matches!(me.source, GroupedMetricSource::Numeric(_)) {
                    numeric_buf_map.push(Some(num_numeric));
                    num_numeric += 1;
                } else {
                    numeric_buf_map.push(None);
                }
            }
            let numeric_buffers: Vec<Vec<Option<f64>>> =
                (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();

            entries.push((
                spec.name.clone(),
                Some(GroupedAggSegmentEntry {
                    key_readers,
                    metric_entries,
                    buckets,
                    accum_template,
                    doc_buffer: Vec::with_capacity(BATCH_SIZE),
                    ord_buffer: vec![None; BATCH_SIZE],
                    numeric_buffers,
                    numeric_buf_map,
                }),
            ));
        }

        Ok(GroupedAggSegmentCollector { entries })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged: std::collections::HashMap<
            String,
            std::collections::HashMap<String, crate::search::GroupedMetricsBucket>,
        > = std::collections::HashMap::new();

        for fruit in segment_fruits {
            for (name, buckets) in fruit {
                let agg_buckets = merged.entry(name).or_default();
                for bucket in buckets {
                    // Use a compact merge key — the group_values are already resolved
                    // serde_json::Value strings at this point (from harvest), but only
                    // runs #unique_groups × #segments times, not per-doc.
                    let key = compact_group_key(&bucket.group_values);
                    let target = agg_buckets.entry(key).or_insert_with(|| {
                        crate::search::GroupedMetricsBucket {
                            group_values: bucket.group_values.clone(),
                            metrics: std::collections::HashMap::new(),
                        }
                    });
                    merge_grouped_bucket_metrics(target, &bucket);
                }
            }
        }

        let mut results = std::collections::HashMap::new();
        for spec in &self.specs {
            let mut buckets: Vec<_> = merged
                .remove(&spec.name)
                .unwrap_or_default()
                .into_values()
                .collect();

            // Shard-level top-K pruning after segment merge (collector path).
            if let Some(top_k) = &self.shard_top_k {
                apply_shard_top_k(&mut buckets, top_k);
            }

            results.insert(
                spec.name.clone(),
                crate::search::PartialAggResult::GroupedMetrics { buckets },
            );
        }
        Ok(results)
    }
}

impl tantivy::collector::SegmentCollector for GroupedAggSegmentCollector {
    type Fruit = Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        for (_, maybe_entry) in &mut self.entries {
            let Some(entry) = maybe_entry else {
                continue;
            };

            // For single-column GROUP BY, buffer docs for batch ordinal reads.
            // This works for both count-only and numeric metrics.
            if matches!(entry.buckets, GroupedBuckets::Single(_)) && entry.key_readers.len() == 1 {
                entry.doc_buffer.push(doc);
                if entry.doc_buffer.len() >= BATCH_SIZE {
                    flush_batch(entry);
                }
                continue;
            }

            // Multi-column or Global: buffer docs and batch-flush.
            // Avoids per-doc Vec allocation for ord_keys and per-doc fast-field reads.
            entry.doc_buffer.push(doc);
            if entry.doc_buffer.len() >= BATCH_SIZE {
                flush_batch_multi(entry);
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.entries
            .into_iter()
            .filter_map(|(name, entry)| {
                let mut entry = entry?;
                // Flush any remaining buffered docs (single-column or multi-column)
                if !entry.doc_buffer.is_empty() {
                    if matches!(entry.buckets, GroupedBuckets::Single(_))
                        && entry.key_readers.len() == 1
                    {
                        flush_batch(&mut entry);
                    } else {
                        flush_batch_multi(&mut entry);
                    }
                }

                // Collect all buckets from whichever variant
                let raw_buckets: Vec<OrdGroupedBucket> = match entry.buckets {
                    GroupedBuckets::Single(map) => map.into_values().collect(),
                    GroupedBuckets::Multi(map) => map.into_values().collect(),
                    GroupedBuckets::Global(opt) => opt.into_iter().collect(),
                };

                // Resolve ordinals to values and convert to GroupedMetricsBucket.
                let buckets: Vec<crate::search::GroupedMetricsBucket> = raw_buckets
                    .into_iter()
                    .map(|bucket| resolve_bucket(bucket, &entry.key_readers, &entry.metric_entries))
                    .collect();
                Some((name, buckets))
            })
            .collect()
    }
}

/// Contiguous string arena — avoids N individual heap allocations when batch-
/// resolving ordinals to strings.  All strings live in one `Vec<u8>` and are
/// referenced by `(offset, len)`.  Only the final `serde_json::Value::String`
/// conversion allocates a new owned String per group.
struct StringArena {
    buf: Vec<u8>,
    /// (start_offset, byte_length) for each interned string.
    entries: Vec<(u32, u32)>,
}

impl StringArena {
    fn with_capacity(num_strings: usize, avg_len: usize) -> Self {
        Self {
            buf: Vec::with_capacity(num_strings * avg_len),
            entries: Vec::with_capacity(num_strings),
        }
    }

    /// Append a string slice to the arena; returns its index.
    fn push(&mut self, s: &str) -> u32 {
        let idx = self.entries.len() as u32;
        let offset = self.buf.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        self.entries.push((offset, s.len() as u32));
        idx
    }

    /// Retrieve a reference to a previously interned string.
    fn get(&self, idx: u32) -> &str {
        let (offset, len) = self.entries[idx as usize];
        // SAFETY: all data pushed through `push` is valid UTF-8 (`&str`).
        unsafe {
            std::str::from_utf8_unchecked(&self.buf[offset as usize..(offset + len) as usize])
        }
    }

    fn to_json_value(&self, idx: u32) -> serde_json::Value {
        serde_json::Value::String(self.get(idx).to_string())
    }
}

/// Extract a sort value from flat metric arrays for a given ordinal.
/// Used by shard-level top-K pruning to rank groups without resolving strings.
fn flat_sort_value(
    flat_metrics: &[FlatMetric],
    metric_entries: &[GroupedMetricEntry],
    sort_by: &str,
    ord: usize,
) -> f64 {
    for (idx, me) in metric_entries.iter().enumerate() {
        if me.output_name != sort_by {
            continue;
        }
        return match (&me.function, &flat_metrics[idx]) {
            (_, FlatMetric::Count(counts)) => counts[ord] as f64,
            (crate::search::GroupedMetricFunction::Sum, FlatMetric::Stats { sum, .. }) => sum[ord],
            (crate::search::GroupedMetricFunction::Avg, FlatMetric::Stats { count, sum, .. }) => {
                if count[ord] > 0 {
                    sum[ord] / count[ord] as f64
                } else {
                    f64::NEG_INFINITY
                }
            }
            (crate::search::GroupedMetricFunction::Min, FlatMetric::Stats { min, .. }) => min[ord],
            (crate::search::GroupedMetricFunction::Max, FlatMetric::Stats { max, .. }) => max[ord],
            _ => 0.0,
        };
    }
    0.0
}

/// Extract a sort value from a fully-resolved `GroupedMetricsBucket`.
/// Uses the metric function from `ShardTopK` to compute the correct value
/// (avg = sum/count, min = min, max = max) instead of blindly using sum.
fn bucket_sort_value(
    bucket: &crate::search::GroupedMetricsBucket,
    sort_by: &str,
    sort_function: &crate::search::GroupedMetricFunction,
) -> f64 {
    let Some(partial) = bucket.metrics.get(sort_by) else {
        return f64::NEG_INFINITY;
    };
    match partial {
        crate::search::GroupedMetricPartial::Count { count } => *count as f64,
        crate::search::GroupedMetricPartial::Stats {
            count,
            sum,
            min,
            max,
        } => match sort_function {
            crate::search::GroupedMetricFunction::Count => *count as f64,
            crate::search::GroupedMetricFunction::Sum => {
                if *count > 0 {
                    *sum
                } else {
                    f64::NEG_INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Avg => {
                if *count > 0 {
                    *sum / *count as f64
                } else {
                    f64::NEG_INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Min => {
                if *count > 0 {
                    *min
                } else {
                    f64::INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Max => {
                if *count > 0 {
                    *max
                } else {
                    f64::NEG_INFINITY
                }
            }
        },
    }
}

/// Apply shard-level top-K pruning to a vec of grouped buckets.
/// Sorts by the named metric and truncates to `top_k.limit`.
fn apply_shard_top_k(
    buckets: &mut Vec<crate::search::GroupedMetricsBucket>,
    top_k: &crate::search::ShardTopK,
) {
    if buckets.len() <= top_k.limit {
        return;
    }
    let k = top_k.limit.min(buckets.len());
    // Partial sort: place the top-K elements in [0..k) in O(N) average.
    buckets.select_nth_unstable_by(k - 1, |a, b| {
        let va = bucket_sort_value(a, &top_k.sort_by, &top_k.sort_function);
        let vb = bucket_sort_value(b, &top_k.sort_by, &top_k.sort_function);
        if top_k.descending {
            vb.total_cmp(&va)
        } else {
            va.total_cmp(&vb)
        }
    });
    buckets.truncate(k);
}

/// Flat-array accumulation for single-column keyword GROUP BY on match_all scans.
/// Replaces HashMap with pre-allocated Vec<u64>/Vec<f64> arrays indexed directly
/// by ordinal — zero hash computation, zero collision handling, cache-friendly
/// sequential access. ~22MB for 300K groups × 5 metrics, fits in L3 cache.
fn flat_scan_segment(
    key_reader: &GroupKeyReader,
    metric_entries: &[GroupedMetricEntry],
    max_doc: u32,
    num_groups: usize,
    shard_top_k: Option<&crate::search::ShardTopK>,
) -> Vec<crate::search::GroupedMetricsBucket> {
    // Build flat metric arrays — one layout per metric
    let mut flat_metrics: Vec<FlatMetric> = metric_entries
        .iter()
        .map(|me| match &me.source {
            GroupedMetricSource::CountAll | GroupedMetricSource::CountField(_) => {
                FlatMetric::Count(vec![0u64; num_groups])
            }
            GroupedMetricSource::Numeric(_) => FlatMetric::Stats {
                count: vec![0u64; num_groups],
                sum: vec![0.0f64; num_groups],
                min: vec![f64::INFINITY; num_groups],
                max: vec![f64::NEG_INFINITY; num_groups],
            },
        })
        .collect();

    // Batch read ordinals + numerics, accumulate into flat arrays.
    // Use contiguous ranges instead of pushing individual doc IDs.
    let mut doc_buffer: Vec<u32> = Vec::with_capacity(BATCH_SIZE);
    let mut ord_buffer: Vec<Option<u64>> = vec![None; BATCH_SIZE];

    // Pre-allocate numeric batch buffers
    let numeric_cols: Vec<Option<&NumCol>> = metric_entries
        .iter()
        .map(|me| match &me.source {
            GroupedMetricSource::Numeric(col) => Some(col),
            _ => None,
        })
        .collect();
    let num_numeric = numeric_cols.iter().filter(|c| c.is_some()).count();
    let mut numeric_bufs: Vec<Vec<Option<f64>>> =
        (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();
    // Map: metric index → numeric_bufs index
    let numeric_buf_map: Vec<Option<usize>> = {
        let mut idx = 0usize;
        metric_entries
            .iter()
            .map(|me| {
                if matches!(me.source, GroupedMetricSource::Numeric(_)) {
                    let i = idx;
                    idx += 1;
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    };

    // Process contiguous batches of BATCH_SIZE docs at a time.
    let mut start = 0u32;
    while start < max_doc {
        let end = (start + BATCH_SIZE as u32).min(max_doc);
        doc_buffer.clear();
        doc_buffer.extend(start..end);
        flat_flush_batch(
            key_reader,
            &doc_buffer,
            &mut ord_buffer,
            &numeric_cols,
            &numeric_buf_map,
            &mut numeric_bufs,
            &mut flat_metrics,
            num_groups,
        );
        start = end;
    }

    // Convert flat arrays → GroupedMetricsBucket.
    //
    // Optimization 1 — shard-level top-K: when ORDER BY + LIMIT is present,
    // select only the top-K ordinals by the sort metric value BEFORE resolving
    // strings. This avoids 364K ord_to_str calls for LIMIT 10 queries.
    //
    // Optimization 2 — StringArena: batch-resolve all needed ordinals into one
    // contiguous buffer instead of N individual String heap allocations.

    // Step 1: collect populated ordinals.
    let mut populated_ords: Vec<usize> = (0..num_groups)
        .filter(|&ord| {
            flat_metrics.iter().any(|fm| match fm {
                FlatMetric::Count(counts) => counts[ord] > 0,
                FlatMetric::Stats { count, .. } => count[ord] > 0,
            })
        })
        .collect();

    // Step 2: top-K pruning on ordinals (before string resolution).
    if let Some(top_k) = shard_top_k
        && populated_ords.len() > top_k.limit
    {
        let k = top_k.limit.min(populated_ords.len());
        populated_ords.select_nth_unstable_by(k - 1, |&a, &b| {
            let va = flat_sort_value(&flat_metrics, metric_entries, &top_k.sort_by, a);
            let vb = flat_sort_value(&flat_metrics, metric_entries, &top_k.sort_by, b);
            if top_k.descending {
                vb.total_cmp(&va)
            } else {
                va.total_cmp(&vb)
            }
        });
        populated_ords.truncate(k);
    }

    // Step 3: batch-resolve ordinals into a StringArena.
    let mut arena = StringArena::with_capacity(populated_ords.len(), 16);
    let mut resolve_buf = String::with_capacity(64);
    // Map: position in populated_ords → arena index.
    // For non-string keys, arena is unused and we resolve inline.
    let is_str_key = matches!(key_reader, GroupKeyReader::Str(_));
    if is_str_key {
        for &ord in &populated_ords {
            resolve_buf.clear();
            if let GroupKeyReader::Str(reader) = key_reader {
                reader.ord_to_str(ord as u64, &mut resolve_buf);
            }
            arena.push(&resolve_buf);
        }
    }

    // Step 4: build buckets from the pruned + arena-resolved ordinals.
    let mut buckets = Vec::with_capacity(populated_ords.len());
    for (pos, &ord) in populated_ords.iter().enumerate() {
        let group_value = if is_str_key {
            arena.to_json_value(pos as u32)
        } else {
            key_reader.resolve(ord as u64)
        };

        let mut metrics = std::collections::HashMap::new();
        for (metric_idx, me) in metric_entries.iter().enumerate() {
            let partial = match &flat_metrics[metric_idx] {
                FlatMetric::Count(counts) => {
                    crate::search::GroupedMetricPartial::Count { count: counts[ord] }
                }
                FlatMetric::Stats {
                    count,
                    sum,
                    min,
                    max,
                } => crate::search::GroupedMetricPartial::Stats {
                    count: count[ord],
                    sum: sum[ord],
                    min: min[ord],
                    max: max[ord],
                },
            };
            metrics.insert(me.output_name.clone(), partial);
        }

        buckets.push(crate::search::GroupedMetricsBucket {
            group_values: vec![group_value],
            metrics,
        });
    }
    buckets
}

/// Flat metric storage: parallel arrays indexed by ordinal.
enum FlatMetric {
    Count(Vec<u64>),
    Stats {
        count: Vec<u64>,
        sum: Vec<f64>,
        min: Vec<f64>,
        max: Vec<f64>,
    },
}

/// Flush a batch of docs into flat arrays: batch-read ordinals + numerics,
/// then update flat metric arrays with direct indexed access (no HashMap).
#[allow(clippy::too_many_arguments)]
#[inline]
fn flat_flush_batch(
    key_reader: &GroupKeyReader,
    doc_buffer: &[u32],
    ord_buffer: &mut Vec<Option<u64>>,
    numeric_cols: &[Option<&NumCol>],
    numeric_buf_map: &[Option<usize>],
    numeric_bufs: &mut [Vec<Option<f64>>],
    flat_metrics: &mut [FlatMetric],
    num_groups: usize,
) {
    let batch_len = doc_buffer.len();

    // Batch-read ordinals
    if ord_buffer.len() < batch_len {
        ord_buffer.resize(batch_len, None);
    }
    for slot in &mut ord_buffer[..batch_len] {
        *slot = None;
    }
    key_reader.keys_batch(doc_buffer, &mut ord_buffer[..batch_len]);

    // Batch-read all numeric columns
    for (metric_idx, maybe_col) in numeric_cols.iter().enumerate() {
        if let Some(col) = maybe_col
            && let Some(buf_idx) = numeric_buf_map[metric_idx]
        {
            let buf = &mut numeric_bufs[buf_idx];
            if buf.len() < batch_len {
                buf.resize(batch_len, None);
            }
            for slot in &mut buf[..batch_len] {
                *slot = None;
            }
            col.first_vals_f64(doc_buffer, &mut buf[..batch_len]);
        }
    }

    // Accumulate into flat arrays — direct indexed, no HashMap
    for i in 0..batch_len {
        let Some(ord) = ord_buffer[i] else {
            continue; // NULL ordinal — skip
        };
        let ord = ord as usize;
        if ord >= num_groups {
            continue; // safety guard
        }

        for (metric_idx, fm) in flat_metrics.iter_mut().enumerate() {
            match fm {
                FlatMetric::Count(counts) => {
                    counts[ord] += 1;
                }
                FlatMetric::Stats {
                    count,
                    sum,
                    min,
                    max,
                } => {
                    if let Some(buf_idx) = numeric_buf_map[metric_idx]
                        && let Some(val) = numeric_bufs[buf_idx][i]
                    {
                        count[ord] += 1;
                        sum[ord] += val;
                        if val < min[ord] {
                            min[ord] = val;
                        }
                        if val > max[ord] {
                            max[ord] = val;
                        }
                    }
                }
            }
        }
    }
}

/// Flush a batch of buffered doc IDs: batch-read ordinals AND numeric values,
/// then update accumulators. Avoids per-doc fast-field reads for numeric metrics.
fn flush_batch(entry: &mut GroupedAggSegmentEntry) {
    let batch_len = entry.doc_buffer.len();
    if batch_len == 0 {
        return;
    }

    if entry.ord_buffer.len() < batch_len {
        entry.ord_buffer.resize(batch_len, None);
    }

    for slot in &mut entry.ord_buffer[..batch_len] {
        *slot = None;
    }

    if let Some(reader) = entry.key_readers.first() {
        reader.keys_batch(&entry.doc_buffer, &mut entry.ord_buffer[..batch_len]);
    }

    // Batch-read all numeric columns upfront (sequential memory access).
    for (metric_idx, metric) in entry.metric_entries.iter().enumerate() {
        if let Some(buf_idx) = entry.numeric_buf_map[metric_idx]
            && let GroupedMetricSource::Numeric(col) = &metric.source
        {
            let buf = &mut entry.numeric_buffers[buf_idx];
            if buf.len() < batch_len {
                buf.resize(batch_len, None);
            }
            for slot in &mut buf[..batch_len] {
                *slot = None;
            }
            col.first_vals_f64(&entry.doc_buffer, &mut buf[..batch_len]);
        }
    }

    let GroupedBuckets::Single(map) = &mut entry.buckets else {
        entry.doc_buffer.clear();
        return;
    };

    let has_numeric = !entry.numeric_buffers.is_empty();

    for i in 0..batch_len {
        let ord = entry.ord_buffer[i].unwrap_or(u64::MAX);

        let bucket = map.entry(ord).or_insert_with(|| OrdGroupedBucket {
            ord_keys: vec![ord],
            accums: entry.accum_template.clone(),
        });

        if has_numeric {
            // Use pre-fetched batch values instead of per-doc reads.
            for (metric_idx, metric) in entry.metric_entries.iter().enumerate() {
                match (&metric.function, &entry.numeric_buf_map[metric_idx]) {
                    (crate::search::GroupedMetricFunction::Count, _) => {
                        if let CompactMetricAccum::Count(c) = &mut bucket.accums[metric_idx] {
                            *c += 1;
                        }
                    }
                    (
                        crate::search::GroupedMetricFunction::Sum
                        | crate::search::GroupedMetricFunction::Avg
                        | crate::search::GroupedMetricFunction::Min
                        | crate::search::GroupedMetricFunction::Max,
                        Some(buf_idx),
                    ) => {
                        let Some(value) = entry.numeric_buffers[*buf_idx][i] else {
                            continue;
                        };
                        if let CompactMetricAccum::Stats {
                            count,
                            sum,
                            min,
                            max,
                        } = &mut bucket.accums[metric_idx]
                        {
                            *count += 1;
                            *sum += value;
                            if value < *min {
                                *min = value;
                            }
                            if value > *max {
                                *max = value;
                            }
                        }
                    }
                    _ => {}
                }
            }
        } else {
            // Count-only fast path
            for accum in &mut bucket.accums {
                if let CompactMetricAccum::Count(c) = accum {
                    *c += 1;
                }
            }
        }
    }

    entry.doc_buffer.clear();
}

/// Batched flush for multi-column GROUP BY or Global (ungrouped aggregates).
/// Batch-reads ordinals for ALL key readers and all numeric columns, then
/// accumulates — avoids per-doc fast-field reads and per-doc Vec allocations.
fn flush_batch_multi(entry: &mut GroupedAggSegmentEntry) {
    let batch_len = entry.doc_buffer.len();
    if batch_len == 0 {
        return;
    }

    let num_keys = entry.key_readers.len();

    // Batch-read ordinals for each key reader into a flat buffer.
    // Layout: ord_bufs[key_idx][doc_in_batch] = ordinal
    let mut ord_bufs: Vec<Vec<Option<u64>>> = Vec::with_capacity(num_keys);
    for reader in &entry.key_readers {
        let mut buf = vec![None; batch_len];
        reader.keys_batch(&entry.doc_buffer, &mut buf);
        ord_bufs.push(buf);
    }

    // Batch-read all numeric columns upfront (sequential memory access).
    for (metric_idx, metric) in entry.metric_entries.iter().enumerate() {
        if let Some(buf_idx) = entry.numeric_buf_map[metric_idx]
            && let GroupedMetricSource::Numeric(col) = &metric.source
        {
            let buf = &mut entry.numeric_buffers[buf_idx];
            if buf.len() < batch_len {
                buf.resize(batch_len, None);
            }
            for slot in &mut buf[..batch_len] {
                *slot = None;
            }
            col.first_vals_f64(&entry.doc_buffer, &mut buf[..batch_len]);
        }
    }

    let has_numeric = !entry.numeric_buffers.is_empty();

    // Reusable ord_keys buffer — avoids per-doc Vec allocation.
    let mut ord_keys = vec![0u64; num_keys];

    for i in 0..batch_len {
        // Build composite key from batch-read ordinals (no per-doc fast-field read).
        for (k, buf) in ord_bufs.iter().enumerate() {
            ord_keys[k] = buf[i].unwrap_or(u64::MAX);
        }

        let template = &entry.accum_template;
        let bucket = match &mut entry.buckets {
            GroupedBuckets::Single(map) => {
                let key = ord_keys[0];
                map.entry(key).or_insert_with(|| OrdGroupedBucket {
                    ord_keys: ord_keys.clone(),
                    accums: template.clone(),
                })
            }
            GroupedBuckets::Multi(map) => {
                map.entry(ord_keys.clone())
                    .or_insert_with(|| OrdGroupedBucket {
                        ord_keys: ord_keys.clone(),
                        accums: template.clone(),
                    })
            }
            GroupedBuckets::Global(slot) => slot.get_or_insert_with(|| OrdGroupedBucket {
                ord_keys: vec![],
                accums: template.clone(),
            }),
        };

        if has_numeric {
            // Use pre-fetched batch values instead of per-doc reads.
            for (metric_idx, metric) in entry.metric_entries.iter().enumerate() {
                match (&metric.function, &entry.numeric_buf_map[metric_idx]) {
                    (crate::search::GroupedMetricFunction::Count, _) => {
                        if let CompactMetricAccum::Count(c) = &mut bucket.accums[metric_idx] {
                            *c += 1;
                        }
                    }
                    (
                        crate::search::GroupedMetricFunction::Sum
                        | crate::search::GroupedMetricFunction::Avg
                        | crate::search::GroupedMetricFunction::Min
                        | crate::search::GroupedMetricFunction::Max,
                        Some(buf_idx),
                    ) => {
                        let Some(value) = entry.numeric_buffers[*buf_idx][i] else {
                            continue;
                        };
                        if let CompactMetricAccum::Stats {
                            count,
                            sum,
                            min,
                            max,
                        } = &mut bucket.accums[metric_idx]
                        {
                            *count += 1;
                            *sum += value;
                            if value < *min {
                                *min = value;
                            }
                            if value > *max {
                                *max = value;
                            }
                        }
                    }
                    _ => {}
                }
            }
        } else {
            // Count-only fast path
            for accum in &mut bucket.accums {
                if let CompactMetricAccum::Count(c) = accum {
                    *c += 1;
                }
            }
        }
    }

    entry.doc_buffer.clear();
}

/// Resolve an OrdGroupedBucket to a GroupedMetricsBucket (ordinals → values).
fn resolve_bucket(
    bucket: OrdGroupedBucket,
    key_readers: &[GroupKeyReader],
    metric_entries: &[GroupedMetricEntry],
) -> crate::search::GroupedMetricsBucket {
    let group_values: Vec<serde_json::Value> = bucket
        .ord_keys
        .iter()
        .zip(key_readers.iter())
        .map(|(&ord, reader)| reader.resolve(ord))
        .collect();

    let mut metrics = std::collections::HashMap::new();
    for (idx, metric) in metric_entries.iter().enumerate() {
        let partial = match &bucket.accums[idx] {
            CompactMetricAccum::Count(c) => {
                crate::search::GroupedMetricPartial::Count { count: *c }
            }
            CompactMetricAccum::Stats {
                count,
                sum,
                min,
                max,
            } => crate::search::GroupedMetricPartial::Stats {
                count: *count,
                sum: *sum,
                min: *min,
                max: *max,
            },
        };
        metrics.insert(metric.output_name.clone(), partial);
    }

    crate::search::GroupedMetricsBucket {
        group_values,
        metrics,
    }
}

/// Build a compact string key from group_values for segment merge.
/// Much cheaper than serde_json::to_string — concatenates string representations
/// with a separator. Only called #unique_groups × #segments times.
fn compact_group_key(values: &[serde_json::Value]) -> String {
    use std::fmt::Write;
    let mut key = String::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            key.push('\x1F'); // unit separator
        }
        match v {
            serde_json::Value::String(s) => key.push_str(s),
            serde_json::Value::Number(n) => write!(key, "{n}").unwrap(),
            serde_json::Value::Bool(b) => write!(key, "{b}").unwrap(),
            serde_json::Value::Null => key.push('\0'),
            _ => write!(key, "{v}").unwrap(),
        }
    }
    key
}

fn merge_grouped_bucket_metrics(
    target: &mut crate::search::GroupedMetricsBucket,
    source: &crate::search::GroupedMetricsBucket,
) {
    for (metric_name, incoming) in &source.metrics {
        match incoming {
            crate::search::GroupedMetricPartial::Count { count } => {
                let entry = target
                    .metrics
                    .entry(metric_name.clone())
                    .or_insert(crate::search::GroupedMetricPartial::Count { count: 0 });
                if let crate::search::GroupedMetricPartial::Count {
                    count: merged_count,
                } = entry
                {
                    *merged_count += count;
                }
            }
            crate::search::GroupedMetricPartial::Stats {
                count,
                sum,
                min,
                max,
            } => {
                let entry = target.metrics.entry(metric_name.clone()).or_insert(
                    crate::search::GroupedMetricPartial::Stats {
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    },
                );
                if let crate::search::GroupedMetricPartial::Stats {
                    count: merged_count,
                    sum: merged_sum,
                    min: merged_min,
                    max: merged_max,
                } = entry
                {
                    *merged_count += count;
                    *merged_sum += sum;
                    if *min < *merged_min {
                        *merged_min = *min;
                    }
                    if *max > *merged_max {
                        *merged_max = *max;
                    }
                }
            }
        }
    }
}

pub(crate) enum SegmentAggData {
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    Histogram {
        interval: f64,
        buckets: std::collections::HashMap<i64, u64>,
    },
    Terms {
        counts: std::collections::HashMap<String, u64>,
    },
}

fn merge_segment_data(target: &mut SegmentAggData, source: &SegmentAggData) {
    match (target, source) {
        (
            SegmentAggData::Stats {
                count: ca,
                sum: sa,
                min: mna,
                max: mxa,
            },
            SegmentAggData::Stats {
                count: cb,
                sum: sb,
                min: mnb,
                max: mxb,
            },
        ) => {
            *ca += cb;
            *sa += sb;
            if *mnb < *mna {
                *mna = *mnb;
            }
            if *mxb > *mxa {
                *mxa = *mxb;
            }
        }
        (
            SegmentAggData::Histogram { buckets: ba, .. },
            SegmentAggData::Histogram { buckets: bb, .. },
        ) => {
            for (k, v) in bb {
                *ba.entry(*k).or_insert(0) += v;
            }
        }
        (SegmentAggData::Terms { counts: ca }, SegmentAggData::Terms { counts: cb }) => {
            for (k, v) in cb {
                *ca.entry(k.clone()).or_insert(0) += v;
            }
        }
        _ => {}
    }
}

fn convert_to_partial(kind: &AggKind, data: SegmentAggData) -> crate::search::PartialAggResult {
    use crate::search::{HistogramBucket, PartialAggResult, TermsBucket};
    match (kind, data) {
        (
            AggKind::Stats,
            SegmentAggData::Stats {
                count,
                sum,
                min,
                max,
            },
        ) => PartialAggResult::Stats {
            count,
            sum,
            min,
            max,
        },
        (AggKind::Min, SegmentAggData::Stats { count, min, .. }) => PartialAggResult::Metric {
            value: if count > 0 { Some(min) } else { None },
        },
        (AggKind::Max, SegmentAggData::Stats { count, max, .. }) => PartialAggResult::Metric {
            value: if count > 0 { Some(max) } else { None },
        },
        (
            AggKind::Avg,
            SegmentAggData::Stats {
                count,
                sum,
                min,
                max,
            },
        ) => PartialAggResult::Stats {
            count,
            sum,
            min,
            max,
        },
        (AggKind::Sum, SegmentAggData::Stats { sum, .. }) => {
            PartialAggResult::Metric { value: Some(sum) }
        }
        (AggKind::ValueCount, SegmentAggData::Stats { count, .. }) => PartialAggResult::Metric {
            value: Some(count as f64),
        },
        (_, SegmentAggData::Histogram { interval, buckets }) => {
            let mut hb: Vec<HistogramBucket> = buckets
                .into_iter()
                .map(|(k, c)| HistogramBucket {
                    key: k as f64 * interval,
                    doc_count: c,
                })
                .collect();
            hb.sort_by(|a, b| {
                a.key
                    .partial_cmp(&b.key)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            PartialAggResult::Histogram { buckets: hb }
        }
        (AggKind::Terms, SegmentAggData::Terms { counts }) => {
            let mut tb: Vec<TermsBucket> = counts
                .into_iter()
                .map(|(k, c)| TermsBucket {
                    key: k,
                    doc_count: c,
                })
                .collect();
            tb.sort_by(|a, b| b.doc_count.cmp(&a.doc_count));
            // Preserve every bucket in the shard partial. Coordinator merge is
            // where the requested size is applied so distributed top terms stay correct.
            PartialAggResult::Terms { buckets: tb }
        }
        _ => PartialAggResult::Metric { value: None },
    }
}

#[derive(Clone)]
enum AggKind {
    Stats,
    Min,
    Max,
    Avg,
    Sum,
    ValueCount,
    Histogram { interval: f64 },
    Terms,
}

struct ResolvedAggSpec {
    name: String,
    field_name: String,
    kind: AggKind,
}

enum SegmentAggEntry {
    NumericStats {
        column: NumCol,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    Histogram {
        column: NumCol,
        interval: f64,
        buckets: std::collections::HashMap<i64, u64>,
    },
    TermsStr {
        column: tantivy::columnar::StrColumn,
        counts: std::collections::HashMap<u64, u64>,
    },
    TermsNum {
        column: NumCol,
        counts: std::collections::HashMap<String, u64>,
    },
    Skip,
}

/// Single-pass aggregation collector. Combine with TopDocs via tuple collector.
pub(crate) struct AggCollector {
    specs: Vec<ResolvedAggSpec>,
    schema: Schema,
}

impl AggCollector {
    pub(crate) fn from_request(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
        schema: Schema,
    ) -> Self {
        use crate::search::AggregationRequest;
        let specs = aggs
            .iter()
            .filter_map(|(name, req)| {
                let (field_name, kind) = match req {
                    AggregationRequest::Stats(p) => (p.field.clone(), AggKind::Stats),
                    AggregationRequest::Min(p) => (p.field.clone(), AggKind::Min),
                    AggregationRequest::Max(p) => (p.field.clone(), AggKind::Max),
                    AggregationRequest::Avg(p) => (p.field.clone(), AggKind::Avg),
                    AggregationRequest::Sum(p) => (p.field.clone(), AggKind::Sum),
                    AggregationRequest::ValueCount(p) => (p.field.clone(), AggKind::ValueCount),
                    AggregationRequest::Histogram(p) => (
                        p.field.clone(),
                        AggKind::Histogram {
                            interval: p.interval,
                        },
                    ),
                    AggregationRequest::Terms(p) => (p.field.clone(), AggKind::Terms),
                    AggregationRequest::GroupedMetrics(_) => return None,
                };
                Some(ResolvedAggSpec {
                    name: name.clone(),
                    field_name,
                    kind,
                })
            })
            .collect();
        Self { specs, schema }
    }
}

pub(crate) struct AggSegmentCollector {
    entries: Vec<(String, SegmentAggEntry)>,
}

impl tantivy::collector::Collector for AggCollector {
    type Fruit = std::collections::HashMap<String, crate::search::PartialAggResult>;
    type Child = AggSegmentCollector;

    fn for_segment(
        &self,
        _seg_id: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let ff = segment.fast_fields();
        let mut entries = Vec::with_capacity(self.specs.len());
        for spec in &self.specs {
            let entry = match &spec.kind {
                AggKind::Stats
                | AggKind::Min
                | AggKind::Max
                | AggKind::Avg
                | AggKind::Sum
                | AggKind::ValueCount => match open_num_col(&self.schema, ff, &spec.field_name) {
                    Some(col) => SegmentAggEntry::NumericStats {
                        column: col,
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    },
                    None => SegmentAggEntry::Skip,
                },
                AggKind::Histogram { interval } => {
                    match open_num_col(&self.schema, ff, &spec.field_name) {
                        Some(col) => SegmentAggEntry::Histogram {
                            column: col,
                            interval: *interval,
                            buckets: std::collections::HashMap::new(),
                        },
                        None => SegmentAggEntry::Skip,
                    }
                }
                AggKind::Terms => {
                    if let Ok(Some(str_col)) = ff.str(&spec.field_name) {
                        SegmentAggEntry::TermsStr {
                            column: str_col,
                            counts: std::collections::HashMap::new(),
                        }
                    } else if let Some(num_col) = open_num_col(&self.schema, ff, &spec.field_name) {
                        SegmentAggEntry::TermsNum {
                            column: num_col,
                            counts: std::collections::HashMap::new(),
                        }
                    } else {
                        SegmentAggEntry::Skip
                    }
                }
            };
            entries.push((spec.name.clone(), entry));
        }
        Ok(AggSegmentCollector { entries })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(String, SegmentAggData)>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged: std::collections::HashMap<String, SegmentAggData> =
            std::collections::HashMap::new();
        for fruit in segment_fruits {
            for (name, data) in fruit {
                merged
                    .entry(name)
                    .and_modify(|e| merge_segment_data(e, &data))
                    .or_insert(data);
            }
        }
        let mut results = std::collections::HashMap::new();
        for spec in &self.specs {
            if let Some(data) = merged.remove(&spec.name) {
                results.insert(spec.name.clone(), convert_to_partial(&spec.kind, data));
            }
        }
        Ok(results)
    }
}

impl tantivy::collector::SegmentCollector for AggSegmentCollector {
    type Fruit = Vec<(String, SegmentAggData)>;

    #[inline]
    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        for (_, entry) in &mut self.entries {
            match entry {
                SegmentAggEntry::NumericStats {
                    column,
                    count,
                    sum,
                    min,
                    max,
                } => {
                    if let Some(val) = column.first_f64(doc) {
                        *count += 1;
                        *sum += val;
                        if val < *min {
                            *min = val;
                        }
                        if val > *max {
                            *max = val;
                        }
                    }
                }
                SegmentAggEntry::Histogram {
                    column,
                    interval,
                    buckets,
                } => {
                    if let Some(val) = column.first_f64(doc) {
                        *buckets.entry((val / *interval).floor() as i64).or_insert(0) += 1;
                    }
                }
                SegmentAggEntry::TermsStr { column, counts } => {
                    for ord in column.term_ords(doc) {
                        *counts.entry(ord).or_insert(0) += 1;
                    }
                }
                SegmentAggEntry::TermsNum { column, counts } => {
                    if let Some(val) = column.first_f64(doc) {
                        let key = if val.fract() == 0.0 {
                            format!("{}", val as i64)
                        } else {
                            format!("{}", val)
                        };
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
                SegmentAggEntry::Skip => {}
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.entries
            .into_iter()
            .filter_map(|(name, entry)| {
                let data = match entry {
                    SegmentAggEntry::NumericStats {
                        count,
                        sum,
                        min,
                        max,
                        ..
                    } => SegmentAggData::Stats {
                        count,
                        sum,
                        min,
                        max,
                    },
                    SegmentAggEntry::Histogram {
                        interval, buckets, ..
                    } => SegmentAggData::Histogram { interval, buckets },
                    SegmentAggEntry::TermsStr { column, counts } => {
                        let mut resolved = std::collections::HashMap::with_capacity(counts.len());
                        let mut s = String::new();
                        for (ord, count) in counts {
                            s.clear();
                            if column.ord_to_str(ord, &mut s).unwrap_or(false) {
                                *resolved.entry(s.clone()).or_insert(0) += count;
                            }
                        }
                        SegmentAggData::Terms { counts: resolved }
                    }
                    SegmentAggEntry::TermsNum { counts, .. } => SegmentAggData::Terms { counts },
                    SegmentAggEntry::Skip => return None,
                };
                Some((name, data))
            })
            .collect()
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

    fn add_document_with_seq(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        seq_no: u64,
    ) -> Result<String> {
        {
            let tl = self.translog.lock().unwrap();
            let wal_entry = serde_json::json!({
                "_doc_id": doc_id,
                "_source": payload
            });
            tl.append_with_seq(seq_no, "index", wal_entry)?;
        }

        let id_field = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.delete_term(Term::from_field_text(id_field, doc_id));

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
            let doc =
                Self::build_tantivy_doc_inner(&registry, &self.index.schema(), doc_id, payload);
            writer.add_document(doc)?;
            doc_ids.push(doc_id.clone());
        }

        Ok(doc_ids)
    }

    fn bulk_add_documents_with_start_seq(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        start_seq_no: u64,
    ) -> Result<Vec<String>> {
        let ops: Vec<(&str, serde_json::Value)> = docs
            .iter()
            .map(|(id, p)| ("index", serde_json::json!({ "_doc_id": id, "_source": p })))
            .collect();
        {
            let tl = self.translog.lock().unwrap();
            tl.write_bulk_with_start_seq(start_seq_no, &ops)?;
        }

        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let mut doc_ids = Vec::with_capacity(docs.len());
        for (doc_id, payload) in &docs {
            writer.delete_term(Term::from_field_text(registry.id_field, doc_id));
            let doc =
                Self::build_tantivy_doc_inner(&registry, &self.index.schema(), doc_id, payload);
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

    fn delete_document_with_seq(&self, doc_id: &str, seq_no: u64) -> Result<u64> {
        {
            let tl = self.translog.lock().unwrap();
            tl.append_with_seq(seq_no, "delete", serde_json::json!({ "_doc_id": doc_id }))?;
        }

        let id_field = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .id_field;
        let writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        let opstamp = writer.delete_term(Term::from_field_text(id_field, doc_id));
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
                if let Some(text) = value.as_str()
                    && let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text)
                {
                    return Ok(Some(json_val));
                }
            }
        }
        Ok(None)
    }

    fn refresh(&self) -> Result<()> {
        let committed_next_seq = {
            let tl = self.translog.lock().unwrap();
            let next_seq = tl.next_seq_no();
            let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
            writer.commit()?;
            next_seq
        };
        self.persist_committed_next_seq_no(committed_next_seq)?;
        self.reader.reload()?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let tl = self.translog.lock().unwrap();
        let committed_next_seq = tl.next_seq_no();
        let mut writer = self.writer.write().unwrap_or_else(|e| e.into_inner());
        writer.commit()?;
        drop(writer); // release lock before reader reload
        self.reader.reload()?;
        self.persist_committed_next_seq_no(committed_next_seq)?;
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
    ) -> Result<(
        Vec<serde_json::Value>,
        usize,
        std::collections::HashMap<String, crate::search::PartialAggResult>,
    )> {
        let searcher = self.reader.searcher();
        // Use the exact requested limit when from+size is explicit.
        // The coordinator handles cross-shard merging at the API layer.
        let limit = req.from + req.size;
        let query = self.build_query(&req.query)?;
        let effective_limit = if limit == 0 { 1 } else { limit };

        if GroupedAggCollector::has_grouped_metrics(&req.aggs) {
            // Fast path: match_all + grouped partials → direct columnar scan
            // Bypasses Tantivy's scorer/collector entirely for unfiltered aggregations.
            if req.query.is_match_all() && req.size == 0 {
                let total = searcher
                    .segment_readers()
                    .iter()
                    .map(|r| r.max_doc() as usize)
                    .sum();
                let partial_aggs = self.grouped_partials_direct_scan(req)?;
                return Ok((Vec::new(), total, partial_aggs));
            }

            let grouped_collector =
                GroupedAggCollector::from_request(&req.aggs, self.index.schema());

            if req.size == 0 {
                let (partial_aggs, total) =
                    searcher.search(&*query, &(grouped_collector, Count))?;
                return Ok((Vec::new(), total, partial_aggs));
            }

            let (top_docs, partial_aggs, total) = searcher.search(
                &*query,
                &(
                    TopDocs::with_limit(effective_limit),
                    grouped_collector,
                    Count,
                ),
            )?;
            let hits = self.collect_hits(&searcher, top_docs)?;
            return Ok((hits, total, partial_aggs));
        }

        // Build optional aggregation collector (None = zero overhead when no aggs)
        let agg_collector = if !req.aggs.is_empty() {
            Some(AggCollector::from_request(&req.aggs, self.index.schema()))
        } else {
            None
        };

        // size=0 requests never need hits, so skip TopDocs entirely.
        if req.size == 0 {
            let (partial_aggs, total) = searcher.search(&*query, &(agg_collector, Count))?;
            return Ok((Vec::new(), total, partial_aggs.unwrap_or_default()));
        }

        // Fast-field sort: push sorting into Tantivy collector for numeric fields
        if let Some((sort_field, order)) = self.extract_fast_field_sort(req) {
            let schema = self.index.schema();
            let field = self.resolve_field(&sort_field);
            let field_type = schema.get_field_entry(field).field_type();
            match field_type {
                tantivy::schema::FieldType::F64(_) => {
                    let td = TopDocs::with_limit(effective_limit)
                        .order_by_fast_field::<f64>(&sort_field, order);
                    let (top_docs, partial_aggs, total) =
                        searcher.search(&*query, &(td, agg_collector, Count))?;
                    let hits = self.collect_hits_sorted(&searcher, top_docs)?;
                    return Ok((hits, total, partial_aggs.unwrap_or_default()));
                }
                tantivy::schema::FieldType::I64(_) => {
                    let td = TopDocs::with_limit(effective_limit)
                        .order_by_fast_field::<i64>(&sort_field, order);
                    let (top_docs, partial_aggs, total) =
                        searcher.search(&*query, &(td, agg_collector, Count))?;
                    let hits = self.collect_hits_sorted(&searcher, top_docs)?;
                    return Ok((hits, total, partial_aggs.unwrap_or_default()));
                }
                tantivy::schema::FieldType::U64(_) => {
                    let td = TopDocs::with_limit(effective_limit)
                        .order_by_fast_field::<u64>(&sort_field, order);
                    let (top_docs, partial_aggs, total) =
                        searcher.search(&*query, &(td, agg_collector, Count))?;
                    let hits = self.collect_hits_sorted(&searcher, top_docs)?;
                    return Ok((hits, total, partial_aggs.unwrap_or_default()));
                }
                _ => {} // fall through to default score-based collection
            }
        }

        let (top_docs, partial_aggs, total) = searcher.search(
            &*query,
            &(TopDocs::with_limit(effective_limit), agg_collector, Count),
        )?;
        let hits = self.collect_hits(&searcher, top_docs)?;
        Ok((hits, total, partial_aggs.unwrap_or_default()))
    }

    fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<Option<super::SqlBatchResult>> {
        Ok(Some(HotEngine::sql_record_batch(
            self,
            req,
            columns,
            needs_id,
            needs_score,
        )?))
    }

    fn sql_streaming_batches(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<Option<super::SqlStreamingResult>> {
        if !self.can_stream_sql_batches(columns, needs_score) {
            return Ok(None);
        }

        Ok(Some(HotEngine::sql_streaming_batches(
            self,
            req,
            columns,
            needs_id,
            needs_score,
            batch_size,
        )?))
    }

    fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

// ─── BitSet Collector ───────────────────────────────────────────────────────
// Collects ALL matching doc IDs as a bitset per segment.
// Memory: 1 bit per doc in the segment. 4M docs = 500KB. Trivial.
// Used for GROUP BY fallback queries that need to see all matches.

/// Per-segment bitset of matched doc IDs.
struct SegmentBitSet {
    segment_ord: u32,
    words: Vec<u64>,
    max_doc: u32,
    count: u32,
}

impl SegmentBitSet {
    fn new(segment_ord: u32, max_doc: u32) -> Self {
        let num_words = (max_doc as usize).div_ceil(64);
        Self {
            segment_ord,
            words: vec![0u64; num_words],
            max_doc,
            count: 0,
        }
    }

    #[inline]
    fn set(&mut self, doc_id: u32) {
        let word_idx = (doc_id / 64) as usize;
        let bit_idx = doc_id % 64;
        let mask = 1u64 << bit_idx;
        if self.words[word_idx] & mask == 0 {
            self.words[word_idx] |= mask;
            self.count += 1;
        }
    }

    /// Iterate all set bit positions.
    fn iter_set_bits(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(word_idx, &word)| {
            let base = (word_idx as u32) * 64;
            BitIter { word, base }
        })
    }
}

/// Iterator over set bits in a single u64 word using trailing-zeros scan.
struct BitIter {
    word: u64,
    base: u32,
}

impl Iterator for BitIter {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.word == 0 {
            return None;
        }
        let tz = self.word.trailing_zeros();
        self.word &= self.word - 1; // clear lowest set bit
        Some(self.base + tz)
    }
}

/// Collects matched doc IDs into per-segment bitsets.
struct BitSetCollector;

struct BitSetSegmentCollector {
    bitset: SegmentBitSet,
}

impl tantivy::collector::Collector for BitSetCollector {
    type Fruit = Vec<SegmentBitSet>;
    type Child = BitSetSegmentCollector;

    fn for_segment(
        &self,
        seg_ord: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(BitSetSegmentCollector {
            bitset: SegmentBitSet::new(seg_ord, segment.max_doc()),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<SegmentBitSet>,
    ) -> tantivy::Result<Vec<SegmentBitSet>> {
        Ok(segment_fruits)
    }
}

impl tantivy::collector::SegmentCollector for BitSetSegmentCollector {
    type Fruit = SegmentBitSet;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        self.bitset.set(doc);
    }

    fn harvest(self) -> SegmentBitSet {
        self.bitset
    }
}

// ─── Streaming batch reader ────────────────────────────────────────────────
// Reads fast-field columns for matched docs (from bitset) and produces
// Arrow RecordBatches of `batch_size` rows each.

/// Streaming batch size for bitset-based GROUP BY fallback queries.
const STREAMING_BATCH_SIZE: usize = 8192;

impl HotEngine {
    /// Streaming is only valid when every requested column is fast-field backed
    /// on every segment and the query does not require synthetic BM25 `_score`.
    fn can_stream_sql_batches(&self, columns: &[String], needs_score: bool) -> bool {
        if needs_score {
            return false;
        }

        let searcher = self.reader.searcher();
        let schema = self.index.schema();

        searcher.segment_readers().iter().all(|segment_reader| {
            let fast_fields = segment_reader.fast_fields();
            columns.iter().all(|column| {
                !matches!(
                    open_sql_field_reader(&schema, fast_fields, column),
                    SqlFieldReader::SourceFallback
                )
            })
        })
    }

    /// Collect ALL matching docs via bitset, read fast-field columns in streaming
    /// batches. Returns multiple small RecordBatches instead of one giant batch.
    /// No scan limit — bitset is O(segment_size/8) memory regardless of match count.
    pub fn sql_streaming_batches(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        _needs_score: bool,
        batch_size: usize,
    ) -> Result<super::SqlStreamingResult> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&req.query)?;

        // Collect all matching docs as bitsets + total count
        let (segment_bitsets, total_hits) = searcher.search(&*query, &(BitSetCollector, Count))?;

        let schema = self.index.schema();
        let segment_readers = searcher.segment_readers();

        // Build the Arrow schema: _id, _score, then user columns
        let mut arrow_fields = Vec::with_capacity(columns.len() + 2);
        arrow_fields.push(datafusion::arrow::datatypes::Field::new(
            "_id",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ));
        arrow_fields.push(datafusion::arrow::datatypes::Field::new(
            "_score",
            datafusion::arrow::datatypes::DataType::Float32,
            false,
        ));
        for col_name in columns {
            let dt = type_hint_from_schema(&schema, col_name).to_arrow_type();
            arrow_fields.push(datafusion::arrow::datatypes::Field::new(col_name, dt, true));
        }
        let arrow_schema =
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(arrow_fields));

        let mut batches = Vec::new();
        let batch_size = if batch_size == 0 {
            STREAMING_BATCH_SIZE
        } else {
            batch_size
        };

        // Process each segment: iterate set bits, read fast fields, produce batches
        for seg_bitset in &segment_bitsets {
            if seg_bitset.count == 0 {
                continue;
            }
            let seg_ord = seg_bitset.segment_ord as usize;
            if seg_ord >= segment_readers.len() {
                continue;
            }
            let seg_reader = &segment_readers[seg_ord];
            let ff = seg_reader.fast_fields();

            // Open fast-field readers for this segment
            let field_readers: Vec<SqlFieldReader> = columns
                .iter()
                .map(|col_name| open_sql_field_reader(&schema, ff, col_name))
                .collect();

            // Open _id reader (StrColumn + ordinal Column<u64>) if needed
            let id_reader = if needs_id {
                StringFastFieldReader::open(ff, "_id")
            } else {
                None
            };

            // Iterate matched docs in batches
            let mut id_builder =
                datafusion::arrow::array::StringBuilder::with_capacity(batch_size, batch_size * 16);
            let mut score_builder =
                datafusion::arrow::array::Float32Builder::with_capacity(batch_size);
            let mut col_builders: Vec<ColumnBuilder> = field_readers
                .iter()
                .map(|r| ColumnBuilder::new(r, batch_size))
                .collect();
            let mut rows_in_batch = 0usize;
            let mut id_text = String::new();

            for doc_id in seg_bitset.iter_set_bits() {
                // Bounds check: bitset may have bits beyond max_doc if segment shrank
                if doc_id >= seg_bitset.max_doc {
                    break;
                }

                // _id
                if needs_id {
                    if let Some(ref reader) = id_reader {
                        if reader.first_text(doc_id, &mut id_text) {
                            id_builder.append_value(&id_text);
                        } else {
                            id_builder.append_value("");
                        }
                    } else {
                        id_builder.append_value("");
                    }
                } else {
                    id_builder.append_value("");
                }

                // _score: always 0.0 for bitset path (no scoring; can_stream rejects needs_score)
                score_builder.append_value(0.0);

                // data columns
                for (i, reader) in field_readers.iter().enumerate() {
                    col_builders[i].append(reader, doc_id);
                }

                rows_in_batch += 1;
                if rows_in_batch >= batch_size {
                    let batch = self.finalize_streaming_batch(
                        &arrow_schema,
                        &mut id_builder,
                        &mut score_builder,
                        &mut col_builders,
                        columns,
                    )?;
                    batches.push(batch);
                    rows_in_batch = 0;
                }
            }

            // Flush remaining rows from this segment
            if rows_in_batch > 0 {
                let batch = self.finalize_streaming_batch(
                    &arrow_schema,
                    &mut id_builder,
                    &mut score_builder,
                    &mut col_builders,
                    columns,
                )?;
                batches.push(batch);
            }
        }

        // If no matches at all, produce one empty batch with correct schema
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(arrow_schema));
        }

        Ok(super::SqlStreamingResult {
            batches,
            total_hits,
        })
    }

    fn finalize_streaming_batch(
        &self,
        schema: &std::sync::Arc<datafusion::arrow::datatypes::Schema>,
        id_builder: &mut datafusion::arrow::array::StringBuilder,
        score_builder: &mut datafusion::arrow::array::Float32Builder,
        col_builders: &mut [ColumnBuilder],
        _columns: &[String],
    ) -> Result<RecordBatch> {
        let mut arrays: Vec<datafusion::arrow::array::ArrayRef> =
            Vec::with_capacity(col_builders.len() + 2);
        arrays.push(std::sync::Arc::new(id_builder.finish()));
        arrays.push(std::sync::Arc::new(score_builder.finish()));
        for builder in col_builders.iter_mut() {
            arrays.push(builder.finish());
        }
        Ok(RecordBatch::try_new(schema.clone(), arrays)?)
    }
}

// ─── Column builder helpers for streaming batches ──────────────────────────

enum ColumnBuilder {
    F64(datafusion::arrow::array::Float64Builder),
    I64(datafusion::arrow::array::Int64Builder),
    Str {
        builder: datafusion::arrow::array::StringBuilder,
        scratch: String,
    },
    Null(datafusion::arrow::array::StringBuilder),
}

impl ColumnBuilder {
    fn new(reader: &SqlFieldReader, capacity: usize) -> Self {
        match reader {
            SqlFieldReader::F64(_) => ColumnBuilder::F64(
                datafusion::arrow::array::Float64Builder::with_capacity(capacity),
            ),
            SqlFieldReader::I64(_) => ColumnBuilder::I64(
                datafusion::arrow::array::Int64Builder::with_capacity(capacity),
            ),
            SqlFieldReader::Str(_) => ColumnBuilder::Str {
                builder: datafusion::arrow::array::StringBuilder::with_capacity(
                    capacity,
                    capacity * 16,
                ),
                scratch: String::new(),
            },
            SqlFieldReader::SourceFallback => {
                ColumnBuilder::Null(datafusion::arrow::array::StringBuilder::new())
            }
        }
    }

    fn append(&mut self, reader: &SqlFieldReader, doc_id: u32) {
        match (self, reader) {
            (ColumnBuilder::F64(b), SqlFieldReader::F64(col)) => match col.first(doc_id) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            (ColumnBuilder::I64(b), SqlFieldReader::I64(col)) => match col.first(doc_id) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            (ColumnBuilder::Str { builder, scratch }, SqlFieldReader::Str(col)) => {
                if col.first_text(doc_id, scratch) {
                    builder.append_value(scratch.as_str());
                } else {
                    builder.append_null();
                }
            }
            (ColumnBuilder::Null(b), _) => {
                b.append_null();
            }
            // SourceFallback should never reach here — can_stream_sql_batches rejects it.
            // Type mismatches should also never happen. Panic to surface bugs immediately
            // instead of silently producing column-length mismatches that crash later.
            (_, SqlFieldReader::SourceFallback) => {
                unreachable!(
                    "SourceFallback column should have been rejected by can_stream_sql_batches"
                );
            }
            (builder, reader) => {
                unreachable!(
                    "ColumnBuilder/SqlFieldReader type mismatch: builder={}, reader={}",
                    builder.type_name(),
                    reader.type_name(),
                );
            }
        }
    }

    fn finish(&mut self) -> datafusion::arrow::array::ArrayRef {
        use std::sync::Arc;
        match self {
            ColumnBuilder::F64(b) => Arc::new(b.finish()),
            ColumnBuilder::I64(b) => Arc::new(b.finish()),
            ColumnBuilder::Str { builder, .. } => Arc::new(builder.finish()),
            ColumnBuilder::Null(b) => Arc::new(b.finish()),
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            ColumnBuilder::F64(_) => "F64",
            ColumnBuilder::I64(_) => "I64",
            ColumnBuilder::Str { .. } => "Str",
            ColumnBuilder::Null(_) => "Null",
        }
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

    #[test]
    fn terms_partial_keeps_all_buckets_for_coordinator_merge() {
        let partial = convert_to_partial(
            &AggKind::Terms,
            SegmentAggData::Terms {
                counts: std::collections::HashMap::from([
                    ("a".to_string(), 100),
                    ("global".to_string(), 99),
                    ("other".to_string(), 42),
                ]),
            },
        );

        let crate::search::PartialAggResult::Terms { buckets } = partial else {
            panic!("expected terms partial");
        };

        assert_eq!(buckets.len(), 3);
        assert!(buckets.iter().any(|b| b.key == "a" && b.doc_count == 100));
        assert!(
            buckets
                .iter()
                .any(|b| b.key == "global" && b.doc_count == 99)
        );
        assert!(
            buckets
                .iter()
                .any(|b| b.key == "other" && b.doc_count == 42)
        );
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
        let (results, total, _) = engine.search_query(&req).unwrap();
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

        // With size=0, hits are skipped entirely but total should still count all matches.
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 200, "total should count all matching docs");
        assert!(results.is_empty(), "size=0 should skip hit materialization");
    }

    #[test]
    fn size_zero_with_aggs_returns_no_hits_and_partial_aggs() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{AggregationRequest, MetricAggParams, TermsAggParams};

        let mut mappings = HashMap::new();
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"category": "books", "price": 10.0}))
            .unwrap();
        engine
            .add_document("d2", json!({"category": "books", "price": 20.0}))
            .unwrap();
        engine
            .add_document("d3", json!({"category": "toys", "price": 30.0}))
            .unwrap();
        engine.refresh().unwrap();

        let mut aggs = HashMap::new();
        aggs.insert(
            "top_categories".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "category".into(),
                size: 10,
            }),
        );
        aggs.insert(
            "price_stats".into(),
            AggregationRequest::Stats(MetricAggParams {
                field: "price".into(),
            }),
        );

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            aggs,
        };
        let (results, total, partial_aggs) = engine.search_query(&req).unwrap();

        assert!(results.is_empty());
        assert_eq!(total, 3);
        assert!(partial_aggs.contains_key("top_categories"));
        assert!(partial_aggs.contains_key("price_stats"));
    }

    #[test]
    fn size_zero_with_grouped_metrics_returns_grouped_partials() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{
            AggregationRequest, GroupedMetricAgg, GroupedMetricFunction, GroupedMetricsAggParams,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"brand": "Apple", "price": 999.0, "body": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"brand": "Apple", "price": 899.0, "body": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"brand": "Samsung", "price": 799.0, "body": "iphone"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match(HashMap::from([("body".to_string(), json!("iphone"))])),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into()],
                    metrics: vec![
                        GroupedMetricAgg {
                            output_name: "total".into(),
                            function: GroupedMetricFunction::Count,
                            field: None,
                        },
                        GroupedMetricAgg {
                            output_name: "avg_price".into(),
                            function: GroupedMetricFunction::Avg,
                            field: Some("price".into()),
                        },
                    ],
                    shard_top_k: None,
                }),
            )]),
        };

        let (hits, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert!(hits.is_empty());
        assert_eq!(total, 3);

        let crate::search::PartialAggResult::GroupedMetrics { buckets } =
            &partial_aggs["sql_grouped"]
        else {
            panic!("expected grouped metrics partial");
        };
        assert_eq!(buckets.len(), 2);
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
        let (results, total, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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

    #[test]
    fn refresh_then_reopen_does_not_duplicate_committed_docs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("safe", json!({"refreshed": true}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: HashMap::new(),
        };
        let (_hits, total, _) = engine2.search_query(&req).unwrap();
        assert_eq!(total, 1, "refresh-committed docs must not replay twice");
    }

    #[test]
    fn refresh_then_reopen_replays_only_uncommitted_entries_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("committed", json!({"kind": "committed"}))
                .unwrap();
            engine.refresh().unwrap();
            engine
                .add_document("pending", json!({"kind": "pending"}))
                .unwrap();
        }

        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: HashMap::new(),
        };
        let (_hits, total, _) = engine2.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "reopen should keep committed docs and replay only pending ones"
        );
        assert!(engine2.get_document("committed").unwrap().is_some());
        assert!(engine2.get_document("pending").unwrap().is_some());
    }

    #[test]
    fn reopen_with_same_mappings_in_different_hashmap_order_preserves_data() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        {
            let mut mappings = HashMap::new();
            mappings.insert(
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            );
            mappings.insert(
                "category".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            );

            let engine = HotEngine::new_with_mappings(
                dir.path(),
                Duration::from_secs(60),
                &mappings,
                TranslogDurability::Request,
                std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
            )
            .unwrap();
            engine
                .add_document("stable", json!({"title": "schema order", "category": "ok"}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let mut reopened_mappings = HashMap::new();
        reopened_mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        reopened_mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let reopened = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &reopened_mappings,
            TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        let doc = reopened.get_document("stable").unwrap();
        assert!(
            doc.is_some(),
            "document should survive reopen with reordered mappings"
        );
        assert_eq!(doc.unwrap()["title"], "schema order");
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

        // Engine returns exactly from+size hits; coordinator does further merging.
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 5, "engine returns exactly size hits");
        assert_eq!(total, 20, "total reflects all matching docs");
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

        // Engine always fetches from+size hits (10 here) — returns all 10
        let req_all = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (all_results, _, _) = engine.search_query(&req_all).unwrap();
        assert_eq!(all_results.len(), 10);

        // from=7, size=10 → engine fetches top 17, returns all 10 (< 17)
        let req_paged = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 7,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (paged_results, _, _) = engine.search_query(&req_paged).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            5,
            "engine returns all 5 hits; coordinator will slice to empty"
        );
    }

    #[test]
    fn pagination_total_is_accurate_after_coordinator_slice() {
        // Simulates what the API coordinator does: collect engine hits,
        // report total from Count collector, then return the hits.
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 15, "total should reflect all matching docs");
        assert_eq!(hits.len(), 5, "engine returns exactly size hits");
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"), "bob should match");
        assert!(ids.contains(&"d3"), "charlie should match");
        assert!(
            !ids.contains(&"d1"),
            "alice should be excluded (gt, not gte)"
        );
    }

    #[test]
    fn sql_record_batch_reads_filtered_fast_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Float,
                dimension: None,
            },
        );

        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            crate::wal::TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        engine
            .add_document(
                "d1",
                json!({"title": "iPhone Budget", "price": 499.0, "description": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "iPhone Pro", "price": 999.0, "description": "iphone"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Bool(crate::search::BoolQuery {
                must: vec![crate::search::QueryClause::Match(HashMap::from([(
                    "description".to_string(),
                    json!("iphone"),
                )]))],
                should: Vec::new(),
                must_not: Vec::new(),
                filter: vec![crate::search::QueryClause::Range(HashMap::from([(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gt: Some(json!(500)),
                        ..Default::default()
                    },
                )]))],
            }),
            size: 100,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };

        let batch = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(batch.total_hits, 1);
        assert_eq!(batch.batch.num_rows(), 1);

        let title = batch
            .batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let price = batch
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();

        assert_eq!(title.value(0), "iPhone Pro");
        assert_eq!(price.value(0), 999.0);
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
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
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Field mappings tests ────────────────────────────────────────────

    fn create_engine_with_mappings(
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
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
        let (hits, _, _) = engine.search_query(&req).unwrap();
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
        let (hits, _, _) = engine.search_query(&req).unwrap();
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
        let (hits, _, _) = engine.search_query(&req).unwrap();
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
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn range_query_integer_bounds_on_float_field() {
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
        engine.add_document("d1", json!({"price": 5.0})).unwrap();
        engine.add_document("d2", json!({"price": 50.0})).unwrap();
        engine.add_document("d3", json!({"price": 500.0})).unwrap();
        engine.refresh().unwrap();

        // Use integer JSON bounds (10, 100) on a float field — must still match
        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(10)),
                        lte: Some(json!(100)),
                        ..Default::default()
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 1, "integer bounds on float field should match d2");
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn term_query_on_integer_field() {
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
        engine.add_document("d1", json!({"year": 2024})).unwrap();
        engine.add_document("d2", json!({"year": 2025})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("year".to_string(), json!(2024));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 1, "term query on integer field should match");
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_all_clause_types_with_numeric_fields() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust book", "category": "books", "price": 29.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "rust course", "category": "education", "price": 49.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "python book", "category": "books", "price": 19.99}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // must: match "rust", must_not: category=education, filter: price 10-100 (integer bounds)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("education"));
                    m
                })],
                filter: vec![QueryClause::Range({
                    let mut m = HashMap::new();
                    m.insert(
                        "price".into(),
                        crate::search::RangeCondition {
                            gte: Some(json!(10)),
                            lte: Some(json!(100)),
                            ..Default::default()
                        },
                    );
                    m
                })],
                should: vec![],
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "complex bool with all clause types should match d1 only"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn float_field_indexed_with_integer_value_is_searchable() {
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
        // Index with integer JSON value on a float field
        engine.add_document("d1", json!({"price": 100})).unwrap();
        engine.add_document("d2", json!({"price": 200})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(50.0)),
                        lte: Some(json!(150.0)),
                        ..Default::default()
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "integer value indexed on float field should be searchable"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_should_only_matches_any_clause() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust book", "category": "books", "price": 29.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python course", "category": "education", "price": 49.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "go tutorial", "category": "education", "price": 9.99}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // should with no must: any matching should clause is sufficient
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                should: vec![
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("title".into(), json!("rust"));
                        m
                    }),
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("title".into(), json!("go"));
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "should-only bool should match d1 (rust) and d3 (go)"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn bool_must_not_with_filter_only() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"category": "books", "price": 10.0}))
            .unwrap();
        engine
            .add_document("d2", json!({"category": "education", "price": 50.0}))
            .unwrap();
        engine
            .add_document("d3", json!({"category": "books", "price": 100.0}))
            .unwrap();
        engine.refresh().unwrap();

        // must_not + filter, no must: exclude education, filter price >= 5
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("education"));
                    m
                })],
                filter: vec![QueryClause::Range({
                    let mut m = HashMap::new();
                    m.insert(
                        "price".into(),
                        crate::search::RangeCondition {
                            gte: Some(json!(5)),
                            ..Default::default()
                        },
                    );
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "must_not + filter should return d1 and d3 (books only)"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn bool_multiple_must_not_excludes_all() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "item one", "category": "books"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "item two", "category": "education"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "item three", "category": "sports"}))
            .unwrap();
        engine
            .add_document("d4", json!({"title": "item four", "category": "toys"}))
            .unwrap();
        engine.refresh().unwrap();

        // Exclude books AND education simultaneously
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("item"));
                    m
                })],
                must_not: vec![
                    QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("books"));
                        m
                    }),
                    QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("education"));
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "multiple must_not should exclude both books and education"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d3"));
        assert!(ids.contains(&"d4"));
    }

    #[test]
    fn nested_bool_inside_must() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust guide", "category": "books", "price": 25.0}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "rust video", "category": "education", "price": 75.0}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "python guide", "category": "books", "price": 15.0}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Outer: must match "rust". Inner (nested bool in filter): price 20-50 AND category != education
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                filter: vec![QueryClause::Bool(crate::search::BoolQuery {
                    filter: vec![QueryClause::Range({
                        let mut m = HashMap::new();
                        m.insert(
                            "price".into(),
                            crate::search::RangeCondition {
                                gte: Some(json!(20)),
                                lte: Some(json!(50)),
                                ..Default::default()
                            },
                        );
                        m
                    })],
                    must_not: vec![QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("education"));
                        m
                    })],
                    ..Default::default()
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "nested bool should match only d1 (rust, books, price 25)"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_must_not_excludes_everything_returns_zero() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "rust book", "category": "books"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust course", "category": "books"}))
            .unwrap();
        engine.refresh().unwrap();

        // must matches both, but must_not also excludes all (same category)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("books"));
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "must_not excluding all docs should return 0 hits");
        assert!(hits.is_empty());
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
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
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 50,
            "all 50 bulk docs should be visible after refresh"
        );
        assert_eq!(hits.len(), 10, "engine returns exactly size=10 hits");
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

    // ── Stored-fields optimization tests (fast-field _id path) ──────────

    fn create_typed_engine() -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Float,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            crate::wal::TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        (dir, engine)
    }

    #[test]
    fn sql_batch_fast_path_reads_id_from_fast_field() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // All requested columns (title, price) have fast fields → fast path
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-alpha", "doc-beta"]);
    }

    #[test]
    fn sql_batch_fast_path_reads_keyword_values_correctly() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-gamma",
                json!({"title": "Widget", "price": 9.99, "category": "gadgets"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "category".to_string()],
                true,
                false,
            )
            .unwrap();

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let title_col = result
            .batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let category_col = result
            .batch
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        let mut rows: Vec<(String, String, String)> = (0..result.batch.num_rows())
            .map(|i| {
                (
                    id_col.value(i).to_string(),
                    title_col.value(i).to_string(),
                    category_col.value(i).to_string(),
                )
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("doc-alpha".into(), "Widget".into(), "gadgets".into()),
                ("doc-beta".into(), "Sprocket".into(), "parts".into()),
                ("doc-gamma".into(), "Widget".into(), "gadgets".into()),
            ]
        );
    }

    #[test]
    fn sql_batch_fast_path_reorders_ids_with_multi_segment_sort() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("doc-low", json!({"title": "Low", "price": 10.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document("doc-high", json!({"title": "High", "price": 30.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document("doc-mid", json!({"title": "Mid", "price": 20.0}))
            .unwrap();
        engine.refresh().unwrap();

        let mut price_sort = HashMap::new();
        price_sort.insert(
            "price".to_string(),
            crate::search::SortOrder::Direction(crate::search::SortDirection::Desc),
        );

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![crate::search::SortClause::Field(price_sort)],
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, false)
            .unwrap();

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let price_col = result
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();

        let rows: Vec<(String, f64)> = (0..result.batch.num_rows())
            .map(|i| (id_col.value(i).to_string(), price_col.value(i)))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("doc-high".into(), 30.0),
                ("doc-mid".into(), 20.0),
                ("doc-low".into(), 10.0),
            ]
        );
    }

    #[test]
    fn sql_batch_fast_path_id_only_query_returns_ids() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("doc-alpha", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("doc-beta", json!({"title": "Sprocket", "price": 5.50}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine.sql_record_batch(&req, &[], true, false).unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();

        let mut ids: Vec<String> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-alpha", "doc-beta"]);
        assert_eq!(score_col.value(0), 0.0);
        assert_eq!(score_col.value(1), 0.0);
    }

    #[test]
    fn sql_batch_source_fallback_still_works() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "fb-1",
                json!({"title": "Laptop", "price": 1200.0, "description": "A fine laptop"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // "description" is not a mapped fast field → SourceFallback path
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "description".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "fb-1");

        let desc_col = result
            .batch
            .column_by_name("description")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(desc_col.value(0), "A fine laptop");
    }

    #[test]
    fn sql_batch_fast_path_preserves_correct_ids_after_delete() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("keep-me", json!({"title": "Keep", "price": 10.0}))
            .unwrap();
        engine
            .add_document("delete-me", json!({"title": "Delete", "price": 20.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine.delete_document("delete-me").unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "keep-me");
    }

    #[test]
    fn sql_batch_fast_path_empty_result() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 5.0}))
            .unwrap();
        engine.refresh().unwrap();

        // Query that matches nothing
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Term(HashMap::from([(
                "title".to_string(),
                json!("nonexistent"),
            )])),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
        assert_eq!(result.total_hits, 0);
    }

    #[test]
    fn sql_batch_fast_path_many_docs_ids_correct() {
        let (_dir, engine) = create_typed_engine();
        for i in 0..50 {
            engine
                .add_document(
                    &format!("doc-{:03}", i),
                    json!({"title": format!("item-{}", i), "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 100,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, true)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 50);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<String> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        ids.sort();
        let mut expected: Vec<String> = (0..50).map(|i| format!("doc-{:03}", i)).collect();
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn sql_batch_mixed_fast_and_fallback_columns() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "m1",
                json!({"title": "Gizmo", "price": 42.0, "notes": "special order"}),
            )
            .unwrap();
        engine
            .add_document(
                "m2",
                json!({"title": "Doodad", "price": 7.5, "notes": "in stock"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // "price" is fast, "notes" is SourceFallback
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["price".to_string(), "notes".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["m1", "m2"]);

        let notes_col = result
            .batch
            .column_by_name("notes")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut notes: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| notes_col.value(i))
            .collect();
        notes.sort();
        assert_eq!(notes, vec!["in stock", "special order"]);
    }

    // ── _id/_score skip optimization tests ──────────────────────────────

    #[test]
    fn sql_batch_skip_id_and_score_when_not_needed() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "Gadget", "price": 29.99}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=false, needs_score=false
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                false,
                false,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        // _id column should exist but contain empty strings
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "");
        assert_eq!(id_col.value(1), "");

        // _score column should exist but contain zeros
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 0.0);
        assert_eq!(score_col.value(1), 0.0);

        // Data columns should still have correct values
        let price_col = result
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        let mut prices: Vec<f64> = (0..result.batch.num_rows())
            .map(|i| price_col.value(i))
            .collect();
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(prices, vec![19.99, 29.99]);
    }

    #[test]
    fn sql_batch_skip_id_only() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("x1", json!({"title": "A", "price": 1.0}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=false, needs_score=true
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], false, true)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        // _id should be empty string
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "");

        // _score should have real value (> 0)
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert!(score_col.value(0) > 0.0);
    }

    #[test]
    fn sql_batch_skip_score_only() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("y1", json!({"title": "B", "price": 2.0}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=true, needs_score=false
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, false)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        // _id should have real value
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "y1");

        // _score should be zero
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 0.0);
    }

    #[test]
    fn sql_batch_skip_both_many_docs() {
        let (_dir, engine) = create_typed_engine();
        for i in 0..100 {
            engine
                .add_document(
                    &format!("d-{}", i),
                    json!({"title": format!("item-{}", i), "price": i as f64 * 1.5}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], false, false)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 100);

        // All _id values should be empty
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..100 {
            assert_eq!(id_col.value(i), "");
        }
    }

    #[test]
    fn sql_batch_zero_projection_preserves_row_count_without_needs_score() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "Gadget", "price": 29.99}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            aggs: HashMap::new(),
        };
        let result = engine.sql_record_batch(&req, &[], false, false).unwrap();
        assert_eq!(result.batch.num_rows(), 2);
        assert_eq!(result.batch.num_columns(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();

        for row in 0..result.batch.num_rows() {
            assert_eq!(id_col.value(row), "");
            assert_eq!(score_col.value(row), 0.0);
        }
    }

    // ── Batch numeric reads in GroupedAggCollector ───────────────────────

    /// Helper: extract count from a GroupedMetricPartial
    fn metric_count(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> u64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Count { count } => *count,
            crate::search::GroupedMetricPartial::Stats { count, .. } => *count,
        }
    }

    /// Helper: extract sum from a GroupedMetricPartial::Stats
    fn metric_sum(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { sum, .. } => *sum,
            _ => panic!("expected Stats for {}", name),
        }
    }

    /// Helper: extract min from a GroupedMetricPartial::Stats
    fn metric_min(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { min, .. } => *min,
            _ => panic!("expected Stats for {}", name),
        }
    }

    /// Helper: extract max from a GroupedMetricPartial::Stats
    fn metric_max(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { max, .. } => *max,
            _ => panic!("expected Stats for {}", name),
        }
    }

    /// Helper: create an engine with brand (keyword), price (float), quantity (integer)
    /// mappings and insert `n` documents spread across two brands.
    fn create_grouped_numeric_engine(n: usize) -> (tempfile::TempDir, HotEngine) {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );
        mappings.insert(
            "quantity".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let (_dir, engine) = create_engine_with_mappings(mappings);
        for i in 0..n {
            let brand = if i % 2 == 0 { "Apple" } else { "Samsung" };
            let price = 100.0 + i as f64;
            let quantity = (i + 1) as i64;
            engine
                .add_document(
                    &format!("d{}", i),
                    json!({"brand": brand, "price": price, "quantity": quantity, "body": "phone"}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();
        (_dir, engine)
    }

    fn grouped_numeric_request(
        _n_docs: usize,
        metrics: Vec<crate::search::GroupedMetricAgg>,
    ) -> SearchRequest {
        use crate::search::*;
        SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into()],
                    metrics,
                    shard_top_k: None,
                }),
            )]),
        }
    }

    #[test]
    fn grouped_batch_numeric_sum_correctness() {
        use crate::search::*;
        // Use > BATCH_SIZE (1024) docs to ensure multiple flushes exercise the batch path.
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "total".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };
        assert_eq!(buckets.len(), 2);

        // Apple = even indices: 0,2,4,...,2048 → 1025 docs, prices: 100,102,104,...,2148
        // Samsung = odd indices: 1,3,5,...,2049 → 1025 docs, prices: 101,103,105,...,2149
        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        assert_eq!(metric_count(apple, "total"), 1025);
        assert_eq!(metric_count(samsung, "total"), 1025);

        // sum(price) for Apple = sum of (100 + 2k) for k in 0..1025 = 1025*100 + 2*(0+1+...+1024) = 102500 + 2*524800 = 1152100
        let apple_sum = metric_sum(apple, "sum_price");
        let expected_apple_sum: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| 100.0 + i as f64)
            .sum();
        assert!(
            (apple_sum - expected_apple_sum).abs() < 0.01,
            "apple sum: {} != {}",
            apple_sum,
            expected_apple_sum
        );

        let samsung_sum = metric_sum(samsung, "sum_price");
        let expected_samsung_sum: f64 = (0..n)
            .filter(|i| i % 2 == 1)
            .map(|i| 100.0 + i as f64)
            .sum();
        assert!(
            (samsung_sum - expected_samsung_sum).abs() < 0.01,
            "samsung sum: {} != {}",
            samsung_sum,
            expected_samsung_sum
        );
    }

    #[test]
    fn grouped_batch_numeric_min_max_correctness() {
        use crate::search::*;
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "min_price".into(),
                    function: GroupedMetricFunction::Min,
                    field: Some("price".into()),
                },
                GroupedMetricAgg {
                    output_name: "max_price".into(),
                    function: GroupedMetricFunction::Max,
                    field: Some("price".into()),
                },
            ],
        );

        let (_, _, partial_aggs) = engine.search_query(&req).unwrap();
        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        // Apple: even indices 0..2048, prices 100.0..2148.0
        assert!((metric_min(apple, "min_price") - 100.0).abs() < 0.01);
        assert!((metric_max(apple, "max_price") - (100.0 + (n - 2) as f64)).abs() < 0.01);
        // Samsung: odd indices 1..2049, prices 101.0..2149.0
        assert!((metric_min(samsung, "min_price") - 101.0).abs() < 0.01);
        assert!((metric_max(samsung, "max_price") - (100.0 + (n - 1) as f64)).abs() < 0.01);
    }

    #[test]
    fn grouped_batch_numeric_avg_correctness() {
        use crate::search::*;
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "avg_price".into(),
                function: GroupedMetricFunction::Avg,
                field: Some("price".into()),
            }],
        );

        let (_, _, partial_aggs) = engine.search_query(&req).unwrap();
        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let expected_avg: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| 100.0 + i as f64)
            .sum::<f64>()
            / 1025.0;
        let actual_avg = metric_sum(apple, "avg_price") / metric_count(apple, "avg_price") as f64;
        assert!(
            (actual_avg - expected_avg).abs() < 0.01,
            "avg: {} != {}",
            actual_avg,
            expected_avg
        );
    }

    #[test]
    fn grouped_batch_numeric_multiple_columns() {
        use crate::search::*;
        // Verify batch reads work with multiple numeric columns (price + quantity).
        let n = 1500;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "total".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                },
                GroupedMetricAgg {
                    output_name: "sum_qty".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("quantity".into()),
                },
                GroupedMetricAgg {
                    output_name: "max_qty".into(),
                    function: GroupedMetricFunction::Max,
                    field: Some("quantity".into()),
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        assert_eq!(metric_count(apple, "total"), 750);

        // quantity for Apple (even i): i+1 for i in [0,2,4,...,1498] → [1,3,5,...,1499]
        let expected_qty_sum: f64 = (0..n).filter(|i| i % 2 == 0).map(|i| (i + 1) as f64).sum();
        let actual_qty_sum = metric_sum(apple, "sum_qty");
        assert!((actual_qty_sum - expected_qty_sum).abs() < 0.01);

        let expected_max_qty = (n - 1) as f64; // last even index (1498) → quantity = 1499
        assert!((metric_max(apple, "max_qty") - expected_max_qty).abs() < 0.01);
    }

    #[test]
    fn grouped_batch_count_only_still_works() {
        use crate::search::*;
        // Verify count-only (no numeric metrics) still uses the optimized batch path correctly.
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "cnt".into(),
                function: GroupedMetricFunction::Count,
                field: None,
            }],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };
        assert_eq!(buckets.len(), 2);

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();
        assert_eq!(metric_count(apple, "cnt"), 1025);
        assert_eq!(metric_count(samsung, "cnt"), 1025);
    }

    // ── BitSet collector + streaming batches ────────────────────────────

    #[test]
    fn bitset_collector_matches_topdocs_results() {
        let (_dir, engine) = create_engine();
        for i in 0..100 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("rust post {i}"), "brand": "tech", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };

        // TopDocs path
        let single = engine
            .sql_record_batch(&req, &["brand".into(), "price".into()], false, false)
            .unwrap();
        assert_eq!(single.batch.num_rows(), 100);

        // Streaming bitset path
        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into(), "price".into()], false, false, 0)
            .unwrap();
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
        assert_eq!(streaming.total_hits, 100);
    }

    #[test]
    fn bitset_streaming_produces_multiple_batches() {
        let (_dir, engine) = create_engine();
        // Insert more than one batch worth of docs (STREAMING_BATCH_SIZE = 8192)
        for i in 0..100 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("item {i}"), "brand": "test", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };

        // Use tiny batch size to force multiple batches
        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], false, false, 10)
            .unwrap();
        assert!(
            streaming.batches.len() >= 10,
            "100 docs with batch_size=10 should produce >=10 batches, got {}",
            streaming.batches.len()
        );
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn bitset_streaming_empty_result() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc-1", json!({"title": "hello", "brand": "test"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(
                [("title".into(), json!("nonexistent"))]
                    .into_iter()
                    .collect(),
            ),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], false, false, 0)
            .unwrap();
        assert_eq!(streaming.total_hits, 0);
        assert_eq!(streaming.batches.len(), 1); // one empty batch with correct schema
        assert_eq!(streaming.batches[0].num_rows(), 0);
    }

    #[test]
    fn bitset_streaming_schema_matches_topdocs_schema() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for i in 0..10 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("item {i}"), "brand": "test", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let cols = vec!["brand".to_string(), "price".to_string()];

        let single = engine.sql_record_batch(&req, &cols, true, false).unwrap();
        let streaming = engine
            .sql_streaming_batches(&req, &cols, true, false, 0)
            .unwrap();

        // Verify both schemas match: column names, types, and nullability
        let single_schema = single.batch.schema();
        let streaming_schema = streaming.batches[0].schema();

        let single_fields: Vec<(&str, &datafusion::arrow::datatypes::DataType, bool)> =
            single_schema
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
                .collect();
        let streaming_fields: Vec<(&str, &datafusion::arrow::datatypes::DataType, bool)> =
            streaming_schema
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
                .collect();
        assert_eq!(
            single_fields, streaming_fields,
            "streaming batch schema (names, types, nullability) must match single-batch schema"
        );
    }

    #[test]
    fn bitset_streaming_with_filtered_query() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "rust programming", "brand": "tech"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-2",
                json!({"title": "python scripting", "brand": "tech"}),
            )
            .unwrap();
        engine
            .add_document("doc-3", json!({"title": "rust systems", "brand": "infra"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match([("title".into(), json!("rust"))].into_iter().collect()),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], true, false, 0)
            .unwrap();
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "only 2 docs match 'rust'");
        assert_eq!(streaming.total_hits, 2);
    }

    #[test]
    fn bitset_streaming_preserves_ids_and_keyword_values() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-gamma",
                json!({"title": "Widget", "price": 9.99, "category": "gadgets"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["title".into(), "category".into()], true, false, 2)
            .unwrap();

        let mut rows = Vec::new();
        for batch in &streaming.batches {
            let id_col = batch
                .column_by_name("_id")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let title_col = batch
                .column_by_name("title")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let category_col = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                rows.push((
                    id_col.value(i).to_string(),
                    title_col.value(i).to_string(),
                    category_col.value(i).to_string(),
                ));
            }
        }

        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("doc-alpha".into(), "Widget".into(), "gadgets".into()),
                ("doc-beta".into(), "Sprocket".into(), "parts".into()),
                ("doc-gamma".into(), "Widget".into(), "gadgets".into()),
            ]
        );
    }

    #[test]
    fn bitset_streaming_rejected_for_source_fallback_columns() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "iphone", "description": "text-only field"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            !engine.can_stream_sql_batches(&["description".into()], false),
            "text/source-fallback columns must not use bitset streaming"
        );
    }

    #[test]
    fn bitset_streaming_rejected_when_score_needed() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "iphone", "brand": "Apple", "price": 999.0}),
            )
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            !engine.can_stream_sql_batches(&["brand".into()], true),
            "score-dependent queries must stay on the scored path"
        );
    }

    // ── StringArena tests ──────────────────────────────────────────

    #[test]
    fn string_arena_push_and_get() {
        let mut arena = StringArena::with_capacity(4, 8);
        let i0 = arena.push("hello");
        let i1 = arena.push("world");
        let i2 = arena.push("");
        let i3 = arena.push("ferris");
        assert_eq!(arena.get(i0), "hello");
        assert_eq!(arena.get(i1), "world");
        assert_eq!(arena.get(i2), "");
        assert_eq!(arena.get(i3), "ferris");
    }

    #[test]
    fn string_arena_to_json_value() {
        let mut arena = StringArena::with_capacity(2, 8);
        let idx = arena.push("test_value");
        let json = arena.to_json_value(idx);
        assert_eq!(json, serde_json::Value::String("test_value".into()));
    }

    // ── Shard top-K pruning tests ──────────────────────────────────

    #[test]
    fn apply_shard_top_k_prunes_to_limit() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..100)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::String(format!("author_{}", i))],
                metrics: std::collections::HashMap::from([(
                    "posts".to_string(),
                    crate::search::GroupedMetricPartial::Count { count: i as u64 },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 10,
                sort_by: "posts".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 10);
        // All remaining buckets should have count >= 90 (top 10 of 0..100)
        for bucket in &buckets {
            let count = match bucket.metrics.get("posts").unwrap() {
                crate::search::GroupedMetricPartial::Count { count } => *count,
                _ => panic!("expected Count"),
            };
            assert!(
                count >= 90,
                "top-K pruning kept count={} below cutoff",
                count
            );
        }
    }

    #[test]
    fn apply_shard_top_k_noop_when_under_limit() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..5)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "cnt".to_string(),
                    crate::search::GroupedMetricPartial::Count { count: i as u64 },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 20,
                sort_by: "cnt".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 5, "should not prune when under limit");
    }

    #[test]
    fn apply_shard_top_k_ascending_order() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..50)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "val".to_string(),
                    crate::search::GroupedMetricPartial::Stats {
                        count: 1,
                        sum: i as f64,
                        min: i as f64,
                        max: i as f64,
                    },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 5,
                sort_by: "val".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Sum,
                descending: false,
            },
        );

        assert_eq!(buckets.len(), 5);
        // All remaining should have sum <= 4 (bottom 5 of 0..50)
        for bucket in &buckets {
            let sum = match bucket.metrics.get("val").unwrap() {
                crate::search::GroupedMetricPartial::Stats { sum, .. } => *sum,
                _ => panic!("expected Stats"),
            };
            assert!(sum <= 4.0, "ascending top-K kept sum={} above cutoff", sum);
        }
    }

    #[test]
    fn flat_sort_value_extracts_correct_metric() {
        let flat_metrics = vec![
            FlatMetric::Count(vec![100, 200, 50]),
            FlatMetric::Stats {
                count: vec![10, 20, 5],
                sum: vec![1000.0, 2000.0, 500.0],
                min: vec![1.0, 2.0, 3.0],
                max: vec![99.0, 199.0, 49.0],
            },
        ];
        let metric_entries = vec![
            GroupedMetricEntry {
                output_name: "posts".to_string(),
                function: crate::search::GroupedMetricFunction::Count,
                source: GroupedMetricSource::CountAll,
            },
            GroupedMetricEntry {
                output_name: "total".to_string(),
                function: crate::search::GroupedMetricFunction::Sum,
                source: GroupedMetricSource::CountAll, // dummy, unused by sort
            },
        ];

        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "posts", 0),
            100.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "posts", 1),
            200.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "total", 0),
            1000.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "total", 2),
            500.0
        );
        // Non-existent metric falls back to 0.0
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "unknown", 0),
            0.0
        );
    }

    #[test]
    fn bucket_sort_value_count_metric() {
        let bucket = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("alice".into())],
            metrics: std::collections::HashMap::from([(
                "posts".to_string(),
                crate::search::GroupedMetricPartial::Count { count: 42 },
            )]),
        };
        assert_eq!(
            bucket_sort_value(
                &bucket,
                "posts",
                &crate::search::GroupedMetricFunction::Count
            ),
            42.0
        );
        assert_eq!(
            bucket_sort_value(
                &bucket,
                "missing",
                &crate::search::GroupedMetricFunction::Count
            ),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn shard_top_k_serialization_roundtrip() {
        let params = crate::search::GroupedMetricsAggParams {
            group_by: vec!["author".into()],
            metrics: vec![crate::search::GroupedMetricAgg {
                output_name: "posts".into(),
                function: crate::search::GroupedMetricFunction::Count,
                field: None,
            }],
            shard_top_k: Some(crate::search::ShardTopK {
                limit: 40,
                sort_by: "posts".into(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            }),
        };
        let json = serde_json::to_string(&params).unwrap();
        let restored: crate::search::GroupedMetricsAggParams = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.shard_top_k.as_ref().unwrap().limit, 40);
        assert_eq!(restored.shard_top_k.as_ref().unwrap().sort_by, "posts");
        assert!(restored.shard_top_k.as_ref().unwrap().descending);
    }

    #[test]
    fn shard_top_k_none_omitted_in_serialization() {
        let params = crate::search::GroupedMetricsAggParams {
            group_by: vec!["x".into()],
            metrics: vec![],
            shard_top_k: None,
        };
        let json = serde_json::to_string(&params).unwrap();
        assert!(!json.contains("shard_top_k"), "None should be omitted");
    }

    #[test]
    fn bucket_sort_value_avg_sorts_by_average_not_sum() {
        // GPT-identified regression: sorting by avg must use sum/count, not sum.
        // A group with sum=100, count=100 (avg=1.0) should rank LOWER than
        // sum=50, count=1 (avg=50.0) when sorting by avg DESC.
        let high_sum_low_avg = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("prolific".into())],
            metrics: std::collections::HashMap::from([(
                "avg_upvotes".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 100,
                    sum: 100.0,
                    min: 1.0,
                    max: 1.0,
                },
            )]),
        };
        let low_sum_high_avg = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("rare".into())],
            metrics: std::collections::HashMap::from([(
                "avg_upvotes".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 1,
                    sum: 50.0,
                    min: 50.0,
                    max: 50.0,
                },
            )]),
        };

        let avg_fn = crate::search::GroupedMetricFunction::Avg;
        let va = bucket_sort_value(&high_sum_low_avg, "avg_upvotes", &avg_fn);
        let vb = bucket_sort_value(&low_sum_high_avg, "avg_upvotes", &avg_fn);

        assert!(
            va < vb,
            "avg=1.0 should rank below avg=50.0, got va={} vb={}",
            va,
            vb
        );
    }

    #[test]
    fn bucket_sort_value_min_and_max() {
        let bucket = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("test".into())],
            metrics: std::collections::HashMap::from([(
                "price".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 10,
                    sum: 500.0,
                    min: 5.0,
                    max: 99.0,
                },
            )]),
        };

        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Min),
            5.0
        );
        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Max),
            99.0
        );
        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Sum),
            500.0
        );
        // avg = 500.0 / 10 = 50.0
        assert!(
            (bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Avg)
                - 50.0)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn apply_shard_top_k_avg_keeps_highest_average() {
        // 20 groups: group i has count=10, sum=i*10 → avg=i
        // Top 5 by avg DESC should keep groups 15..20 (avg 15..19)
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..20)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "avg_val".to_string(),
                    crate::search::GroupedMetricPartial::Stats {
                        count: 10,
                        sum: i as f64 * 10.0,
                        min: i as f64,
                        max: i as f64,
                    },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 5,
                sort_by: "avg_val".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Avg,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 5);
        for bucket in &buckets {
            let avg = match bucket.metrics.get("avg_val").unwrap() {
                crate::search::GroupedMetricPartial::Stats { count, sum, .. } => {
                    sum / *count as f64
                }
                _ => panic!("expected Stats"),
            };
            assert!(
                avg >= 15.0,
                "avg-sorted top-K should keep avg >= 15, got {}",
                avg
            );
        }
    }
}
