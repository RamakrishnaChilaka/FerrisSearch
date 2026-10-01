use serde::{Deserialize, Serialize};
use tantivy::Index;
use tantivy::query::{
    AllQuery, EmptyQuery, ExistsQuery, Query, QueryParser, QueryParserError, RegexQuery,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStringParams {
    pub query: String,
    #[serde(default = "default_field")]
    pub default_field: String,
}

pub(crate) fn default_field() -> String {
    "body".to_string()
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
        return RegexQuery::from_pattern(".*", field)
            .map(|query| Box::new(query) as Box<dyn Query>)
            .map_err(|error| QueryParserError::UnsupportedQuery(error.to_string()));
    }
    Err(QueryParserError::FieldNotIndexed(field_name.to_string()))
}

pub(crate) fn parse_query_string(
    index: &Index,
    query: &str,
    default_field: &str,
) -> anyhow::Result<Box<dyn Query>> {
    let parse = || {
        let expression = query.trim();
        if expression == "*:*" {
            return Ok(Box::new(AllQuery) as Box<dyn Query>);
        }
        if expression == "*" {
            return presence_query(index, default_field);
        }
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
            let error = parse_query_string(&index, query, "body").unwrap_err();
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
            parse_query_string(&index, query, "body").unwrap();
        }
    }

    #[test]
    fn qsearch_helper_rejects_unknown_default_field_for_term_queries() {
        let error = parse_query_string(&index(), "rust", "missing").unwrap_err();
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
}
