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
