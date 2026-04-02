//! Node-to-node transport via gRPC (tonic).
//! Uses protobuf for efficient binary serialization and HTTP/2 for multiplexed connections.

pub mod client;
pub mod server;

/// Generated protobuf types and gRPC service definitions
pub mod proto {
    tonic::include_proto!("transport");
}

pub use client::TransportClient;

// ─── TLS support (behind feature flag) ──────────────────────────────────────

/// Load TLS server configuration from PEM-encoded cert and key files.
/// Only available when the `transport-tls` feature is enabled.
#[cfg(feature = "transport-tls")]
pub fn load_server_tls_config(
    cert_path: &str,
    key_path: &str,
) -> anyhow::Result<tonic::transport::ServerTlsConfig> {
    let cert = std::fs::read_to_string(cert_path)
        .map_err(|e| anyhow::anyhow!("failed to read TLS cert {}: {}", cert_path, e))?;
    let key = std::fs::read_to_string(key_path)
        .map_err(|e| anyhow::anyhow!("failed to read TLS key {}: {}", key_path, e))?;

    let identity = tonic::transport::Identity::from_pem(cert, key);
    Ok(tonic::transport::ServerTlsConfig::new().identity(identity))
}

/// Concrete TLS connector that applies `ClientTlsConfig` to tonic endpoints.
/// Only available when the `transport-tls` feature is enabled.
#[cfg(feature = "transport-tls")]
pub struct TonicTlsConnector {
    config: tonic::transport::ClientTlsConfig,
}

#[cfg(feature = "transport-tls")]
impl TonicTlsConnector {
    pub fn from_ca_file(ca_path: &str) -> anyhow::Result<Self> {
        let ca_cert = std::fs::read_to_string(ca_path)
            .map_err(|e| anyhow::anyhow!("failed to read TLS CA cert {}: {}", ca_path, e))?;
        let ca = tonic::transport::Certificate::from_pem(ca_cert);
        Ok(Self {
            config: tonic::transport::ClientTlsConfig::new().ca_certificate(ca),
        })
    }
}

#[cfg(feature = "transport-tls")]
impl client::TlsConnector for TonicTlsConnector {
    fn configure_endpoint(
        &self,
        endpoint: tonic::transport::Endpoint,
    ) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
        endpoint.tls_config(self.config.clone())
    }
}
