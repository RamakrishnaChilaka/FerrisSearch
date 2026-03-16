//! Node-to-node transport via gRPC (tonic).
//! Uses protobuf for efficient binary serialization and HTTP/2 for multiplexed connections.

pub mod client;
pub mod server;

/// Generated protobuf types and gRPC service definitions
pub mod proto {
    tonic::include_proto!("transport");
}

pub use client::TransportClient;
