use axum::{Json, http::StatusCode};
use serde_json::{Value, json};

use crate::search::query_string::{QueryErrorReason, query_error_is_client, query_error_reason};

fn query_status(error: &anyhow::Error) -> StatusCode {
    if query_error_is_client(error) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShardFailure {
    status: StatusCode,
    details: Value,
}

impl ShardFailure {
    pub(crate) fn new(
        index: &str,
        shard: impl Into<Value>,
        node: &str,
        status: StatusCode,
        error_type: &str,
        reason: impl std::fmt::Display,
    ) -> Self {
        Self::with_reason(
            index,
            shard,
            node,
            status,
            json!({"type": error_type, "reason": reason.to_string()}),
        )
    }

    fn with_reason(
        index: &str,
        shard: impl Into<Value>,
        node: &str,
        status: StatusCode,
        reason: Value,
    ) -> Self {
        Self {
            status,
            details: json!({"shard": shard.into(), "index": index, "node": node, "reason": reason}),
        }
    }

    pub(crate) fn from_error(
        index: &str,
        shard: impl Into<Value>,
        node: &str,
        error: &anyhow::Error,
    ) -> Self {
        if let Some(reason) = query_error_reason(error) {
            return Self::with_reason(index, shard, node, query_status(error), json!(reason));
        }
        if let Some(status) = error.downcast_ref::<tonic::Status>() {
            if matches!(
                status.code(),
                tonic::Code::InvalidArgument | tonic::Code::Internal
            ) && !status.details().is_empty()
            {
                return match serde_json::from_slice::<QueryErrorReason>(status.details()) {
                    Ok(reason) => Self::with_reason(
                        index,
                        shard,
                        node,
                        if status.code() == tonic::Code::InvalidArgument {
                            StatusCode::BAD_REQUEST
                        } else {
                            StatusCode::INTERNAL_SERVER_ERROR
                        },
                        json!(reason),
                    ),
                    Err(cause) => Self::new(
                        index,
                        shard,
                        node,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "transport_exception",
                        format!("{error:#}; invalid query error details: {cause}"),
                    ),
                };
            }
            let (http_status, error_type) = match status.code() {
                tonic::Code::InvalidArgument => {
                    (StatusCode::BAD_REQUEST, "illegal_argument_exception")
                }
                tonic::Code::NotFound
                | tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Aborted
                | tonic::Code::FailedPrecondition
                | tonic::Code::Cancelled => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_not_available_exception",
                ),
                tonic::Code::ResourceExhausted => (
                    StatusCode::TOO_MANY_REQUESTS,
                    "rejected_execution_exception",
                ),
                tonic::Code::PermissionDenied => (StatusCode::FORBIDDEN, "security_exception"),
                tonic::Code::Unauthenticated => (StatusCode::UNAUTHORIZED, "security_exception"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "search_exception"),
            };
            return Self::new(
                index,
                shard,
                node,
                http_status,
                error_type,
                format!("{error:#}"),
            );
        }
        let (status, error_type) = if error.is::<tonic::transport::Error>() {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
            )
        } else if matches!(
            error.downcast_ref::<tantivy::TantivyError>(),
            Some(tantivy::TantivyError::InvalidArgument(_))
        ) {
            (StatusCode::BAD_REQUEST, "illegal_argument_exception")
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, "search_exception")
        };
        Self::new(index, shard, node, status, error_type, format!("{error:#}"))
    }

    pub(crate) fn to_json(&self) -> Value {
        self.details.clone()
    }
}

pub(crate) fn shard_stats(successful: u32, failed: u32, failures: &[ShardFailure]) -> Value {
    let mut stats =
        json!({"total": successful + failed, "successful": successful, "failed": failed});
    if !failures.is_empty() {
        stats["failures"] = json!(
            failures
                .iter()
                .map(ShardFailure::to_json)
                .collect::<Vec<_>>()
        );
    }
    stats
}

pub(crate) fn query_error_response(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    match query_error_reason(&error) {
        Some(reason) => {
            let status = query_status(&error);
            (
                status,
                Json(json!({"error": reason, "status": status.as_u16()})),
            )
        }
        None => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "search_exception",
            format!("{error:#}"),
        ),
    }
}

pub(crate) fn all_shards_failed_response(
    successful: u32,
    failures: &[ShardFailure],
) -> Option<(StatusCode, Json<Value>)> {
    if successful != 0 || failures.is_empty() {
        return None;
    }
    let status = [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::FORBIDDEN,
        StatusCode::UNAUTHORIZED,
        StatusCode::BAD_REQUEST,
    ]
    .into_iter()
    .find(|status| failures.iter().any(|failure| failure.status == *status))
    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    Some((
        status,
        Json(json!({
            "error": {
                "root_cause": failures.iter().map(|failure| &failure.details["reason"]).collect::<Vec<_>>(),
                "type": "search_phase_execution_exception",
                "reason": "all shards failed",
                "phase": "query",
                "grouped": false,
                "failed_shards": failures.iter().map(ShardFailure::to_json).collect::<Vec<_>>()
            },
            "status": status.as_u16()
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::query_string::{parsing_error, search_error_status};
    use tantivy::query::QueryParserError;

    fn parse_error() -> anyhow::Error {
        parsing_error(
            "body:(",
            QueryParserError::SyntaxError("original parser cause".to_string()),
        )
    }

    #[test]
    fn qsearch_all_parse_failures_preserve_local_and_transport_causes() {
        let local = ShardFailure::from_error("index", 0, "local", &parse_error());
        let remote_error: anyhow::Error = search_error_status(parse_error()).into();
        let remote = ShardFailure::from_error("index", 1, "remote", &remote_error);
        let (status, Json(body)) = all_shards_failed_response(0, &[local, remote]).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["failed_shards"].as_array().unwrap().len(), 2);
        for failure in body["error"]["failed_shards"].as_array().unwrap() {
            assert_eq!(failure["reason"]["type"], "query_shard_exception");
            assert!(
                failure["reason"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains("body:(")
            );
            assert_eq!(
                failure["reason"]["caused_by"]["reason"],
                "Syntax Error: original parser cause"
            );
        }
    }

    #[test]
    fn qsearch_server_failures_are_not_downgraded_to_parse_errors() {
        let parse = ShardFailure::from_error("index", 0, "local", &parse_error());
        for (error, expected) in [
            (
                anyhow::anyhow!("disk failure").context("search storage"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                tonic::Status::unavailable("peer unavailable").into(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                tonic::Status::resource_exhausted("queue full").into(),
                StatusCode::TOO_MANY_REQUESTS,
            ),
        ] {
            let failure = ShardFailure::from_error("index", 1, "remote", &error);
            let (status, Json(body)) =
                all_shards_failed_response(0, &[parse.clone(), failure]).unwrap();
            assert_eq!(status, expected, "{body}");
            assert!(
                body["error"]["failed_shards"][1]["reason"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains(&error.to_string()),
                "{body}"
            );
        }
    }

    #[test]
    fn qsearch_partial_failures_and_empty_target_sets_are_not_errors() {
        let failure = ShardFailure::from_error("index", 0, "local", &parse_error());
        assert!(all_shards_failed_response(1, &[failure]).is_none());
        assert!(all_shards_failed_response(0, &[]).is_none());
    }

    #[tokio::test]
    async fn contained_search_panic_preserves_shard_failure_and_transport_reason() {
        let pools = crate::worker::WorkerPools::new(1, 1);
        let error = pools
            .spawn_search(|| panic!("injected shard search panic"))
            .await
            .unwrap_err();
        let local = ShardFailure::from_error("index", 0, "local", &error);
        let remote_error: anyhow::Error = search_error_status(error).into();
        let remote = ShardFailure::from_error("index", 1, "remote", &remote_error);
        let failures = [local, remote];
        let (status, Json(body)) = all_shards_failed_response(0, &failures).unwrap();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["error"]["reason"], "all shards failed");
        for failure in body["error"]["failed_shards"].as_array().unwrap() {
            let reason = failure["reason"]["reason"].as_str().unwrap();
            assert!(
                reason.contains("search worker task panicked")
                    && reason.contains("injected shard search panic"),
                "{body}"
            );
        }
        assert!(all_shards_failed_response(1, &failures).is_none());
        assert_eq!(shard_stats(1, 2, &failures)["failed"], 2);
        assert_eq!(pools.spawn_search(|| 42).await.unwrap(), 42);
    }

    #[test]
    fn qsearch_all_validation_errors_return_400() {
        let local_error =
            tantivy::TantivyError::InvalidArgument("invalid collector argument".to_string()).into();
        let remote_error = tonic::Status::invalid_argument("invalid search request").into();
        let failures = [
            ShardFailure::from_error("index", 0, "local", &local_error),
            ShardFailure::from_error("index", 1, "remote", &remote_error),
        ];
        let (status, Json(body)) = all_shards_failed_response(0, &failures).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            body["error"]["failed_shards"][0]["reason"]["type"],
            "illegal_argument_exception"
        );
        assert!(
            body["error"]["failed_shards"][0]["reason"]["reason"]
                .as_str()
                .unwrap()
                .contains("invalid collector argument")
        );
        assert_eq!(
            shard_stats(1, 2, &failures)["failures"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn qsearch_tokenizer_configuration_failures_remain_server_errors_across_transport() {
        fn configuration_error() -> anyhow::Error {
            parsing_error(
                "body:rust",
                QueryParserError::UnknownTokenizer {
                    tokenizer: "missing-tokenizer".to_string(),
                    field: "body".to_string(),
                },
            )
        }
        for error in [
            configuration_error(),
            search_error_status(configuration_error()).into(),
        ] {
            let failures = [
                ShardFailure::from_error("index", 0, "local", &parse_error()),
                ShardFailure::from_error("index", 1, "remote", &error),
            ];
            let (status, Json(body)) = all_shards_failed_response(0, &failures).unwrap();
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
            assert!(
                body["error"]["failed_shards"][1]["reason"]["caused_by"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains("missing-tokenizer"),
                "{body}"
            );
        }
    }

    #[test]
    fn qsearch_malformed_transport_error_details_fail_loudly() {
        let error = tonic::Status::with_details(
            tonic::Code::InvalidArgument,
            "peer parser error",
            b"{\"type\":\"invented\"}".to_vec().into(),
        )
        .into();
        let failure = ShardFailure::from_error("index", 0, "remote", &error);
        let (status, Json(body)) = all_shards_failed_response(0, &[failure]).unwrap();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let reason = body["error"]["failed_shards"][0]["reason"]["reason"]
            .as_str()
            .unwrap();
        assert!(reason.contains("peer parser error"), "{body}");
        assert!(reason.contains("invalid query error details"), "{body}");
    }
}
