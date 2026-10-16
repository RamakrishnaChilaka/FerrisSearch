use crate::cluster::manager::ClusterManager;
use anyhow::Context;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) const STATE_VERSION_HEADER: &str = "x-ferris-cluster-state-version";
pub(crate) const INDEX_STATE_FLOOR_HEADER: &str = "x-ferris-index-state-floor";
pub(crate) const INDEX_NAME_HEADER: &str = "x-ferris-index-bin";
pub(crate) const INDEX_UUID_HEADER: &str = "x-ferris-index-uuid-bin";
pub(crate) const PRIMARY_TERM_HEADER: &str = "x-ferris-primary-term";
pub(crate) const ALLOCATION_ID_HEADER: &str = "x-ferris-allocation-id";
pub const REMAINING_BUDGET_MS_HEADER: &str = "x-ferris-remaining-budget-ms";
pub const DEADLINE_EPOCH_MS_HEADER: &str = "x-ferris-deadline-epoch-ms";
pub const EXECUTION_STAGE_HEADER: &str = "x-ferris-execution-stage";
pub(crate) const STATE_WAIT_STATUS_PREFIX: &str = "cluster state wait timed out: ";

pub(crate) fn is_state_wait_timeout(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status.message().starts_with(STATE_WAIT_STATUS_PREFIX)
}

/// Attach an explicit applied-state floor for the request's index.
pub fn request_with_cluster_state_version<T>(message: T, version: u64) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        STATE_VERSION_HEADER,
        version
            .to_string()
            .parse()
            .expect("u64 is valid ASCII metadata"),
    );
    request.metadata_mut().insert(
        INDEX_STATE_FLOOR_HEADER,
        version
            .to_string()
            .parse()
            .expect("u64 is valid ASCII metadata"),
    );
    request
}

pub(crate) fn decode_state_version(metadata: &tonic::metadata::MetadataMap) -> anyhow::Result<u64> {
    decode_optional_u64(metadata, STATE_VERSION_HEADER)?
        .ok_or_else(|| anyhow::anyhow!("missing required [{STATE_VERSION_HEADER}] metadata"))
}

pub(crate) fn decode_optional_u64(
    metadata: &tonic::metadata::MetadataMap,
    header: &'static str,
) -> anyhow::Result<Option<u64>> {
    let Some(value) = metadata.get(header) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .with_context(|| format!("[{header}] metadata is not ASCII"))?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        anyhow::bail!("[{header}] metadata must contain only unsigned decimal digits");
    }
    Ok(Some(value.parse::<u64>().with_context(|| {
        format!("[{header}] metadata must be an unsigned integer")
    })?))
}

pub(crate) fn decode_optional_text(
    metadata: &tonic::metadata::MetadataMap,
    header: &'static str,
) -> anyhow::Result<Option<String>> {
    metadata
        .get_bin(header)
        .map(|value| {
            let bytes = value
                .to_bytes()
                .with_context(|| format!("invalid [{header}] metadata"))?;
            String::from_utf8(bytes.to_vec())
                .with_context(|| format!("[{header}] metadata is not UTF-8"))
        })
        .transpose()
}

pub(crate) fn response_with_cluster_state_version<T>(message: T, version: u64) -> Response<T> {
    let mut response = Response::new(message);
    response.metadata_mut().insert(
        STATE_VERSION_HEADER,
        version
            .to_string()
            .parse()
            .expect("u64 is valid ASCII metadata"),
    );
    response
}

#[derive(Clone)]
pub struct AppliedStateInterceptor {
    pub(crate) cluster_manager: Option<Arc<ClusterManager>>,
}

impl tonic::service::Interceptor for AppliedStateInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let mut version = self
            .cluster_manager
            .as_ref()
            .map_or(0, |manager| manager.version());
        if request.metadata().contains_key(STATE_VERSION_HEADER) {
            version = version.max(
                decode_state_version(request.metadata())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            );
        }

        request.metadata_mut().insert(
            STATE_VERSION_HEADER,
            version
                .to_string()
                .parse()
                .expect("u64 is valid ASCII metadata"),
        );

        if let Some(ctx) = crate::worker::current_request_context() {
            attach_context_deadline_metadata(
                &mut request,
                &ctx,
                crate::worker::ExecutionStage::Search,
            )?;
        }

        Ok(request)
    }
}

pub fn attach_context_deadline_metadata<T>(
    request: &mut Request<T>,
    ctx: &crate::worker::RequestCancellationContext,
    stage: crate::worker::ExecutionStage,
) -> Result<(), Status> {
    if let Err(error) = ctx.check_active(stage) {
        return Err(status_from_worker_error(&error, stage));
    }
    if let Some(deadline) = ctx.deadline() {
        let remaining = deadline.remaining();
        let remaining_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
        if remaining.is_zero() || remaining_ms == 0 {
            let recorded_stage = deadline.record_exhausted_stage(stage);
            return Err(Status::deadline_exceeded(format!(
                "request deadline budget ({} ms) exhausted during stage [{}]",
                deadline.initial_budget_ms(),
                recorded_stage
            )));
        }
        if !request.metadata().contains_key(REMAINING_BUDGET_MS_HEADER) {
            request.metadata_mut().insert(
                REMAINING_BUDGET_MS_HEADER,
                remaining_ms
                    .to_string()
                    .parse()
                    .expect("u64 is valid ASCII metadata"),
            );
        }
        if !request.metadata().contains_key(DEADLINE_EPOCH_MS_HEADER) {
            request.metadata_mut().insert(
                DEADLINE_EPOCH_MS_HEADER,
                deadline
                    .deadline_epoch_ms()
                    .to_string()
                    .parse()
                    .expect("u64 is valid ASCII metadata"),
            );
        }
        if !request.metadata().contains_key("grpc-timeout") {
            let clamped_ms = remaining_ms.min(99_999_999);
            if let Ok(val) = format!("{clamped_ms}m").parse() {
                request.metadata_mut().insert("grpc-timeout", val);
            }
        }
    }
    Ok(())
}

pub fn extract_grpc_request_context<T>(
    request: &Request<T>,
    default_stage: crate::worker::ExecutionStage,
) -> Result<
    (
        crate::worker::RequestCancellationContext,
        crate::worker::CancellationDropGuard,
    ),
    Status,
> {
    let stage = request
        .metadata()
        .get(EXECUTION_STAGE_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(crate::worker::ExecutionStage::from_label)
        .unwrap_or(default_stage);

    let remaining_budget_ms = decode_optional_u64(request.metadata(), REMAINING_BUDGET_MS_HEADER)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let deadline_epoch_ms = decode_optional_u64(request.metadata(), DEADLINE_EPOCH_MS_HEADER)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

    let grpc_timeout = match request.metadata().get("grpc-timeout") {
        Some(value) => {
            let value = value.to_str().map_err(|error| {
                Status::invalid_argument(format!("invalid grpc-timeout: {error}"))
            })?;
            Some(super::refresh_deadline::parse_grpc_timeout(value)?)
        }
        None => None,
    };

    if remaining_budget_ms == Some(0) || grpc_timeout.is_some_and(|duration| duration.is_zero()) {
        crate::metrics::DEADLINE_EXHAUSTED_TOTAL
            .with_label_values(&[stage.as_str()])
            .inc();
        return Err(Status::deadline_exceeded(format!(
            "request deadline budget (0 ms) exhausted during stage [{stage}]"
        )));
    }

    let effective_budget = match (
        remaining_budget_ms.map(std::time::Duration::from_millis),
        grpc_timeout,
    ) {
        (Some(rem), Some(grpc)) => Some(rem.min(grpc)),
        (Some(rem), None) => Some(rem),
        (None, Some(grpc)) => Some(grpc),
        (None, None) => None,
    };

    let ctx = match effective_budget {
        Some(budget) => crate::worker::RequestCancellationContext::with_deadline(
            crate::worker::RequestDeadline::from_remaining_and_epoch(budget, deadline_epoch_ms),
        ),
        None => crate::worker::RequestCancellationContext::new(),
    };
    let guard = ctx.drop_guard(crate::worker::CancellationReason::ClientDisconnected);
    Ok((ctx, guard))
}

pub fn status_from_worker_error(
    error: &anyhow::Error,
    default_stage: crate::worker::ExecutionStage,
) -> Status {
    if let Some(deadline) = error.downcast_ref::<crate::worker::RequestDeadlineExceeded>() {
        return Status::deadline_exceeded(deadline.to_string());
    }
    if let Some(AdmissionError::DeadlineExceeded(deadline)) =
        error.downcast_ref::<crate::worker::AdmissionError>()
    {
        return Status::deadline_exceeded(deadline.to_string());
    }
    if let Some(AdmissionError::QueueFull { .. }) =
        error.downcast_ref::<crate::worker::AdmissionError>()
    {
        return Status::resource_exhausted(error.to_string());
    }
    if let Some(exhausted) = error.downcast_ref::<crate::worker::ResourceExhaustedError>() {
        return Status::resource_exhausted(exhausted.to_string());
    }
    if let Some(cancelled) = error.downcast_ref::<crate::worker::RequestCancelledError>() {
        if let crate::worker::CancellationReason::DeadlineExpired(stage) = cancelled.reason {
            return Status::deadline_exceeded(format!(
                "request deadline budget exhausted during stage [{stage}]"
            ));
        }
        return Status::cancelled(cancelled.to_string());
    }
    if let Some(AdmissionError::Cancelled(cancelled)) =
        error.downcast_ref::<crate::worker::AdmissionError>()
    {
        return Status::cancelled(cancelled.to_string());
    }
    let _ = default_stage;
    Status::internal(format!("{error:#}"))
}

use crate::worker::AdmissionError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarding_state_version_decoding_is_strict() {
        assert!(decode_state_version(&tonic::metadata::MetadataMap::new()).is_err());
        for value in [
            "",
            "-1",
            "+1",
            "1.0",
            "not-a-version",
            "18446744073709551616",
        ] {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert(STATE_VERSION_HEADER, value.parse().unwrap());
            assert!(decode_state_version(&metadata).is_err(), "{value}");
        }

        for version in [0, 1, u64::MAX] {
            let request = request_with_cluster_state_version((), version);
            assert_eq!(decode_state_version(request.metadata()).unwrap(), version);
        }
    }

    #[test]
    fn forwarding_scoped_context_decoding_is_strict_and_supports_utf8() {
        for header in [
            INDEX_STATE_FLOOR_HEADER,
            PRIMARY_TERM_HEADER,
            ALLOCATION_ID_HEADER,
        ] {
            let mut metadata = tonic::metadata::MetadataMap::new();
            assert_eq!(decode_optional_u64(&metadata, header).unwrap(), None);
            for value in ["", "+1", "-1", "1.0", "18446744073709551616"] {
                metadata.insert(header, value.parse().unwrap());
                assert!(decode_optional_u64(&metadata, header).is_err());
            }
            metadata.insert(header, "42".parse().unwrap());
            assert_eq!(decode_optional_u64(&metadata, header).unwrap(), Some(42));
        }
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert_eq!(
            decode_optional_text(&metadata, INDEX_NAME_HEADER).unwrap(),
            None
        );
        let name = "index-\u{e9}";
        metadata.insert_bin(
            INDEX_NAME_HEADER,
            tonic::metadata::MetadataValue::from_bytes(name.as_bytes()),
        );
        assert_eq!(
            decode_optional_text(&metadata, INDEX_NAME_HEADER)
                .unwrap()
                .as_deref(),
            Some(name)
        );
        metadata.insert_bin(
            INDEX_NAME_HEADER,
            tonic::metadata::MetadataValue::from_bytes(&[0xff]),
        );
        assert!(decode_optional_text(&metadata, INDEX_NAME_HEADER).is_err());
    }
}
