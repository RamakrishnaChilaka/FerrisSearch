use serde::{Deserialize, Serialize};
use tantivy::fieldnorm::FieldNormReader;
use tantivy::query::{
    AllQuery, ConstScorer, EmptyQuery, EmptyScorer, EnableScoring, ExistsQuery, Explanation, Query,
    QueryParser, QueryParserError, RegexQuery, Scorer, Weight,
};
use tantivy::schema::Field;
use tantivy::{DocId, DocSet, Index, Score, SegmentReader, TERMINATED};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStringParams {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_field: Option<String>,
}

#[derive(Clone, Debug)]
struct FieldNormPresenceQuery(Field);

impl Query for FieldNormPresenceQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(FieldNormPresenceWeight(self.0)))
    }
}

struct FieldNormPresenceWeight(Field);

impl Weight for FieldNormPresenceWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let Some(fieldnorms) = reader.fieldnorms_readers().get_field(self.0)? else {
            return Ok(Box::new(EmptyScorer));
        };
        let mut docs = FieldNormPresenceDocSet {
            fieldnorms,
            doc: 0,
            max_doc: reader.max_doc(),
        };
        docs.seek(0);
        Ok(Box::new(ConstScorer::new(docs, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if doc == TERMINATED || scorer.seek(doc) != doc {
            return Err(tantivy::TantivyError::InvalidArgument(format!(
                "document [{doc}] has no indexed tokens in the presence field"
            )));
        }
        Ok(Explanation::new("FieldNormPresenceQuery", 1.0))
    }
}

struct FieldNormPresenceDocSet {
    fieldnorms: FieldNormReader,
    doc: DocId,
    max_doc: DocId,
}

impl DocSet for FieldNormPresenceDocSet {
    fn advance(&mut self) -> DocId {
        self.seek(self.doc.saturating_add(1))
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = self.doc.max(target);
        while self.doc < self.max_doc {
            if self.fieldnorms.fieldnorm_id(self.doc) != 0 {
                return self.doc;
            }
            self.doc += 1;
        }
        self.doc = TERMINATED;
        TERMINATED
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed to parse query [{query}]: {source}")]
pub(crate) struct QueryParseError {
    query: String,
    #[source]
    source: QueryParserError,
}

pub(crate) fn parsing_error(query: &str, source: QueryParserError) -> anyhow::Error {
    QueryParseError {
        query: query.to_string(),
        source,
    }
    .into()
}

fn presence_query(index: &Index, field_name: &str) -> Result<Box<dyn Query>, QueryParserError> {
    let schema = index.schema();
    let Some((field, entry)) = schema
        .fields()
        .find(|(_, entry)| entry.name() == field_name)
    else {
        return Ok(Box::new(EmptyQuery));
    };
    if entry.is_fast() {
        return Ok(Box::new(ExistsQuery::new(field_name.to_string(), false)));
    }
    if entry.is_indexed() && entry.field_type().value_type() == tantivy::schema::Type::Str {
        if entry.has_fieldnorms() {
            return Ok(Box::new(FieldNormPresenceQuery(field)));
        }
        return RegexQuery::from_pattern(".*", field)
            .map(|query| Box::new(query) as Box<dyn Query>)
            .map_err(|error| QueryParserError::UnsupportedQuery(error.to_string()));
    }
    Err(QueryParserError::FieldNotIndexed(field_name.to_string()))
}

pub(crate) fn parse_query_string(
    index: &Index,
    query: &str,
    default_field: Option<&str>,
) -> anyhow::Result<Box<dyn Query>> {
    let parse = || {
        let expression = query.trim();
        if expression == "*:*" {
            return Ok(Box::new(AllQuery) as Box<dyn Query>);
        }
        if expression == "*" {
            return match default_field {
                Some(field) => presence_query(index, field),
                None => Ok(Box::new(AllQuery) as Box<dyn Query>),
            };
        }
        let default_field = default_field.unwrap_or("body");
        if let Some((field, value)) = expression.split_once(':') {
            let field = field.trim();
            if value.trim() == "*"
                && field
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
                && field
                    .chars()
                    .all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.'))
            {
                return presence_query(index, field);
            }
        }
        let schema = index.schema();
        let field = schema
            .fields()
            .find(|(_, entry)| entry.name() == default_field)
            .map(|(field, _)| field)
            .ok_or_else(|| QueryParserError::FieldDoesNotExist(default_field.to_string()))?;
        QueryParser::for_index(index, vec![field]).parse_query(query)
    };
    parse().map_err(|source| parsing_error(query, source))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryErrorReason {
    #[serde(rename = "type")]
    error_type: QueryErrorType,
    reason: String,
    caused_by: QueryErrorCause,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryErrorCause {
    #[serde(rename = "type")]
    error_type: QueryErrorCauseType,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryErrorType {
    QueryShardException,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryErrorCauseType {
    ParseException,
}

fn parser_error(error: &anyhow::Error) -> Option<&QueryParserError> {
    if let Some(error) = error.downcast_ref::<QueryParseError>() {
        Some(&error.source)
    } else {
        error.downcast_ref::<QueryParserError>()
    }
}

pub(crate) fn query_error_is_client(error: &anyhow::Error) -> bool {
    parser_error(error)
        .is_some_and(|error| !matches!(error, QueryParserError::UnknownTokenizer { .. }))
}

pub(crate) fn query_error_reason(error: &anyhow::Error) -> Option<QueryErrorReason> {
    let parser_error = parser_error(error)?;
    Some(QueryErrorReason {
        error_type: QueryErrorType::QueryShardException,
        reason: format!("{error:#}"),
        caused_by: QueryErrorCause {
            error_type: QueryErrorCauseType::ParseException,
            reason: parser_error.to_string(),
        },
    })
}

pub(crate) fn search_error_status(error: anyhow::Error) -> tonic::Status {
    let message = format!("{error:#}");
    match query_error_reason(&error) {
        Some(reason) => match serde_json::to_vec(&reason) {
            Ok(details) => {
                let code = if query_error_is_client(&error) {
                    tonic::Code::InvalidArgument
                } else {
                    tonic::Code::Internal
                };
                tonic::Status::with_details(code, message, details.into())
            }
            Err(cause) => tonic::Status::internal(format!(
                "{message}; failed to serialize query error details: {cause}"
            )),
        },
        None if matches!(
            error.downcast_ref::<tantivy::TantivyError>(),
            Some(tantivy::TantivyError::InvalidArgument(_))
        ) =>
        {
            tonic::Status::invalid_argument(message)
        }
        None => tonic::Status::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tantivy::schema::{FAST, INDEXED, STRING, Schema, TEXT};

    fn index() -> Index {
        let mut schema = Schema::builder();
        schema.add_text_field("body", TEXT);
        schema.add_text_field("tag", STRING | FAST);
        schema.add_i64_field("number", INDEXED | FAST);
        Index::create_in_ram(schema.build())
    }

    #[test]
    fn qsearch_helper_rejects_invalid_and_unsupported_syntax_with_typed_causes() {
        let index = index();
        for query in [
            "body:(",
            "number:not-a-number",
            "*:* AND tag:rust",
            "-tag:*",
        ] {
            let error = parse_query_string(&index, query, Some("body")).unwrap_err();
            assert!(error.to_string().contains(query), "{error:#}");
            assert!(error.is::<QueryParseError>(), "{error:#}");
            assert!(
                error.chain().any(|cause| cause.is::<QueryParserError>()),
                "{error:#}"
            );
        }
    }

    #[test]
    fn qsearch_helper_does_not_rewrite_quoted_stars_or_boolean_terms() {
        let index = index();
        for query in [
            "tag:\"*\"",
            "tag:rust OR tag:python",
            "(tag:rust AND body:search)",
        ] {
            parse_query_string(&index, query, Some("body")).unwrap();
        }
    }

    #[test]
    fn qsearch_helper_rejects_unknown_default_field_for_term_queries() {
        let error = parse_query_string(&index(), "rust", Some("missing")).unwrap_err();
        assert!(error.to_string().contains("missing"), "{error:#}");
        assert!(error.to_string().contains("rust"), "{error:#}");
    }

    #[test]
    fn qsearch_params_reject_unimplemented_options() {
        for option in ["fields", "lenient", "default_operator"] {
            let mut value = json!({"query": "rust"});
            value[option] = json!(true);
            let error = serde_json::from_value::<QueryStringParams>(value).unwrap_err();
            assert!(error.to_string().contains(option), "{error}");
        }
    }

    #[test]
    fn qsearch_review_implicit_star_uses_all_query_not_term_enumeration() {
        let index = index();
        for query in ["*", " * ", "*:*"] {
            let parsed = parse_query_string(&index, query, None).unwrap();
            assert!(
                parsed.as_ref().as_any().is::<AllQuery>(),
                "query [{query}]: {parsed:?}"
            );
        }
        let params: QueryStringParams = serde_json::from_value(json!({"query": "*"})).unwrap();
        assert!(params.default_field.is_none());
        assert_eq!(serde_json::to_value(params).unwrap(), json!({"query": "*"}));
    }

    #[test]
    fn qsearch_review_text_presence_uses_fieldnorms_and_respects_deletes() {
        use tantivy::collector::Count;
        let index = index();
        let body = index.schema().get_field("body").unwrap();
        let number = index.schema().get_field("number").unwrap();
        let mut writer = index
            .writer_with_num_threads::<tantivy::TantivyDocument>(1, 15_000_000)
            .unwrap();
        for document in [
            tantivy::doc!(body => "search", number => 1_i64),
            tantivy::doc!(body => "!!!", number => 2_i64),
            tantivy::doc!(body => "", number => 3_i64),
            tantivy::doc!(number => 4_i64),
            tantivy::doc!(body => "delete this", number => 5_i64),
        ] {
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();
        writer.delete_term(tantivy::Term::from_field_i64(number, 5));
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        for (query, field) in [("*", Some("body")), ("body:*", None)] {
            let parsed = parse_query_string(&index, query, field).unwrap();
            assert!(
                parsed.as_ref().as_any().is::<FieldNormPresenceQuery>(),
                "{parsed:?}"
            );
            assert_eq!(searcher.search(parsed.as_ref(), &Count).unwrap(), 1);
            let weight = parsed
                .weight(EnableScoring::disabled_from_schema(searcher.schema()))
                .unwrap();
            for segment in searcher.segment_readers() {
                let mut scorer = weight.scorer(segment, 2.0).unwrap();
                if scorer.doc() != TERMINATED {
                    let doc = scorer.doc();
                    assert_eq!(scorer.seek(doc), doc);
                    assert_eq!(scorer.score(), 2.0);
                }
                assert_eq!(scorer.seek(TERMINATED), TERMINATED);
                assert_eq!(scorer.advance(), TERMINATED);
            }
        }
        let implicit = parse_query_string(&index, "*", None).unwrap();
        assert_eq!(searcher.search(implicit.as_ref(), &Count).unwrap(), 4);
    }
}
