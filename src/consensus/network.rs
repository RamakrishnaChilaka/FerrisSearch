//! Raft network implementation using gRPC (tonic).
//!
//! Implements openraft's RaftNetworkFactory and RaftNetworkV2 traits,
//! forwarding Raft RPCs over the existing gRPC transport.

use std::future::Future;

use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetworkV2};

use crate::consensus::types::{self, TypeConfig};
use crate::transport::proto::RaftRequest;
use crate::transport::proto::internal_transport_client::InternalTransportClient;

// ─── Network Factory ────────────────────────────────────────────────────────

/// Creates network connections to individual Raft peers.
pub struct RaftNetworkFactoryImpl;

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftNetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        RaftNetworkConnection {
            addr: node.addr.clone(),
        }
    }
}

// ─── Network Connection ─────────────────────────────────────────────────────

/// A single network connection to a Raft peer, used for sending RPCs.
pub struct RaftNetworkConnection {
    addr: String,
}

impl RaftNetworkConnection {
    async fn connect(
        &self,
    ) -> Result<InternalTransportClient<tonic::transport::Channel>, RPCError<TypeConfig>> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", self.addr))
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .timeout(std::time::Duration::from_secs(5))
            .connect_timeout(std::time::Duration::from_secs(5));
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(InternalTransportClient::new(channel))
    }
}

impl RaftNetworkV2<TypeConfig> for RaftNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut client = self.connect().await?;
        let data =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let resp = client
            .raft_append_entries(tonic::Request::new(RaftRequest { data }))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .into_inner();

        if !resp.error.is_empty() {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(resp.error),
            )));
        }

        let result: AppendEntriesResponse<TypeConfig> = serde_json::from_slice(&resp.data)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut client = self.connect().await?;
        let data =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let resp = client
            .raft_vote(tonic::Request::new(RaftRequest { data }))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .into_inner();

        if !resp.error.is_empty() {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(resp.error),
            )));
        }

        let result: VoteResponse<TypeConfig> = serde_json::from_slice(&resp.data)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn full_snapshot(
        &mut self,
        vote: types::Vote,
        snapshot: types::Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut client = self.connect().await.map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(format!("{}", e))))
        })?;

        // Read snapshot data
        let snapshot_data = snapshot.snapshot.into_inner();

        let payload = serde_json::json!({
            "vote": vote,
            "meta": snapshot.meta,
            "data": snapshot_data,
        });
        let data = serde_json::to_vec(&payload).map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(format!("{}", e))))
        })?;

        let resp = client
            .raft_snapshot(tonic::Request::new(RaftRequest { data }))
            .await
            .map_err(|e| {
                StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                    "{}",
                    e
                ))))
            })?
            .into_inner();

        if !resp.error.is_empty() {
            return Err(StreamingError::Unreachable(Unreachable::new(
                &std::io::Error::other(resp.error),
            )));
        }

        let result: SnapshotResponse<TypeConfig> =
            serde_json::from_slice(&resp.data).map_err(|e| {
                StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                    "{}",
                    e
                ))))
            })?;
        Ok(result)
    }
}
