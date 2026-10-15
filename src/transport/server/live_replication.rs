use super::*;
use crate::cluster::state::{PrimaryAuthorityPhase, WriteAuthorityIdentity};
use crate::engine::{ClientOperationRecord, SearchEngine, SequencedOperation};

pub(super) fn decode_authority(
    envelope: Option<&ReplicaWriteAuthority>,
    index_uuid: &str,
    shard_id: u32,
) -> Result<WriteAuthorityIdentity, Status> {
    let envelope = envelope
        .ok_or_else(|| Status::invalid_argument("live replication requires current authority"))?;
    if envelope.captured_state_version == 0 {
        return Err(Status::invalid_argument(
            "live replication requires its captured applied-state version",
        ));
    }
    let authority = WriteAuthorityIdentity {
        index_uuid: index_uuid.to_string(),
        shard_id,
        primary_node_id: envelope.primary_node_id.clone(),
        primary_allocation_id: envelope.primary_allocation_id,
        primary_term: envelope.primary_term,
        activation_id: envelope.activation_id,
    };
    authority.validate().map_err(Status::invalid_argument)?;
    Ok(authority)
}

impl TransportService {
    pub(super) async fn publish_replica_global_checkpoint(
        &self,
        index: &str,
        allocation_id: u64,
        authority: &WriteAuthorityIdentity,
        checkpoint: Option<u64>,
    ) -> Result<(), Status> {
        let Some(checkpoint) = checkpoint else {
            return Ok(());
        };
        if self
            .shard_manager
            .copy_identity(index, authority.shard_id)
            .is_some_and(|identity| identity.safe_global_checkpoint >= Some(checkpoint))
        {
            return Ok(());
        }
        let manager = self.shard_manager.clone();
        let index = index.to_string();
        let authority = authority.clone();
        tokio::task::spawn_blocking(move || {
            manager.persist_safe_global_checkpoint(&index, allocation_id, &authority, checkpoint)
        })
        .await
        .map_err(|error| {
            Status::internal(format!("replica global checkpoint worker failed: {error}"))
        })?
        .map_err(|error| {
            Status::internal(format!(
                "replica global checkpoint publication failed: {error:#}"
            ))
        })
    }

    pub(super) async fn wait_for_replica_authority(
        &self,
        index: &str,
        shard_id: u32,
        allocation_id: u64,
        authority: &WriteAuthorityIdentity,
        captured_state_version: u64,
    ) -> Result<(), Status> {
        if self
            .replica_apply_routing(
                index,
                shard_id,
                &authority.index_uuid,
                allocation_id,
                authority,
            )
            .is_err()
            && self.cluster_manager.version() < captured_state_version
        {
            self.cluster_manager
                .wait_for_version(captured_state_version)
                .await
                .map_err(|error| {
                    Status::unavailable(format!(
                        "live replication authority has not applied: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    pub(super) fn validate_replica_write_proof(
        &self,
        index: &str,
        shard_id: u32,
        allocation_id: u64,
        authority: &WriteAuthorityIdentity,
        require_fence: bool,
    ) -> anyhow::Result<()> {
        self.replica_apply_routing(
            index,
            shard_id,
            &authority.index_uuid,
            allocation_id,
            authority,
        )
        .map_err(anyhow::Error::msg)?;
        let state = self.cluster_manager.get_state();
        state
            .validate_write_authority(index, authority, PrimaryAuthorityPhase::Active)
            .map_err(anyhow::Error::msg)?;
        if state.primary_unavailable(index, shard_id) {
            anyhow::bail!("live replica proof requires an available ACTIVE primary");
        }
        let history = state
            .shard_allocation_ids(index, shard_id)
            .and_then(|ids| ids.primary_authority.history.as_ref())
            .ok_or_else(|| anyhow::anyhow!("ACTIVE primary has no canonical history"))?;
        let identity = self
            .shard_manager
            .copy_identity(index, shard_id)
            .ok_or_else(|| anyhow::anyhow!("live replica proof has no durable copy identity"))?;
        if identity.index_uuid != authority.index_uuid
            || identity.allocation_id != allocation_id
            || identity.canonical_history.as_ref() != Some(history)
            || identity.replica_fence > authority.primary_term
            || (require_fence && identity.replica_fence != authority.primary_term)
        {
            anyhow::bail!("live replica proof does not match its durable certificate and fence");
        }
        let engine = self
            .shard_manager
            .get_shard(index, shard_id)
            .ok_or_else(|| anyhow::anyhow!("live replica proof has no open engine"))?;
        engine.ensure_write_health()?;
        if engine.copy_write_state().canonical_history()?.as_ref() != Some(history) {
            anyhow::bail!("live replica engine does not match the committed canonical certificate");
        }
        Ok(())
    }

    pub(super) async fn original_client_operation(
        &self,
        engine: Arc<dyn SearchEngine>,
        seq_no: u64,
        primary_term: u64,
        record: ClientOperationRecord,
    ) -> Result<SequencedOperation, Status> {
        tokio::task::spawn_blocking(move || {
            engine.retained_client_operation(seq_no, primary_term, &record)
        })
        .await
        .map_err(|error| {
            WriteFailure::indeterminate(
                format!("original client operation worker failed: {error}"),
                Some(seq_no),
                Some(primary_term),
                None,
            )
            .into_status()
        })?
        .map_err(|error| {
            WriteFailure::indeterminate(
                format!("cannot prove original client operation: {error:#}"),
                Some(seq_no),
                Some(primary_term),
                None,
            )
            .into_status()
        })
    }
}
