use crate::cluster::manager::ClusterManager;
use anyhow::Context;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) const STATE_VERSION_HEADER: &str = "x-ferris-cluster-state-version";
pub(crate) const STATE_WAIT_STATUS_PREFIX: &str = "cluster state wait timed out: ";

pub(crate) fn is_state_wait_timeout(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status.message().starts_with(STATE_WAIT_STATUS_PREFIX)
}

pub fn request_with_cluster_state_version<T>(message: T, version: u64) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        STATE_VERSION_HEADER,
        version
            .to_string()
            .parse()
            .expect("u64 is valid ASCII metadata"),
    );
    request
}

pub(crate) fn decode_state_version(metadata: &tonic::metadata::MetadataMap) -> anyhow::Result<u64> {
    let value = metadata
        .get(STATE_VERSION_HEADER)
        .ok_or_else(|| anyhow::anyhow!("missing required [{STATE_VERSION_HEADER}] metadata"))?;
    let value = value
        .to_str()
        .context("cluster state version is not ASCII")?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        anyhow::bail!("cluster state version must contain only unsigned decimal digits");
    }
    value
        .parse::<u64>()
        .context("cluster state version must be an unsigned integer")
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
    pub(crate) acknowledged_version: Arc<std::sync::atomic::AtomicU64>,
}

impl tonic::service::Interceptor for AppliedStateInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let mut version = self
            .cluster_manager
            .as_ref()
            .map_or(0, |manager| manager.version())
            .max(
                self.acknowledged_version
                    .load(std::sync::atomic::Ordering::Acquire),
            );
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
        Ok(request)
    }
}

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
}
