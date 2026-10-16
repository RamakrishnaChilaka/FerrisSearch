//! Explicit replica recovery and ISR state machine (`FS-025`).
//!
//! Drives replica copy transitions through:
//! `Unassigned` -> `Initializing` -> `Snapshotting` -> `CatchingUp` -> `InSync`,
//! plus `Stale` (lagging behind primary beyond policy threshold) and `Failed`
//! (definitive or escalated storage/collision/recovery failure).

use super::ShardKey;
use crate::cluster::state::{AllocationId, ClusterState};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaCopyState {
    Unassigned,
    Initializing,
    Snapshotting,
    CatchingUp,
    InSync,
    Stale,
    Failed,
}

impl ReplicaCopyState {
    pub const ALL: [Self; 7] = [
        Self::Unassigned,
        Self::Initializing,
        Self::Snapshotting,
        Self::CatchingUp,
        Self::InSync,
        Self::Stale,
        Self::Failed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unassigned => "unassigned",
            Self::Initializing => "initializing",
            Self::Snapshotting => "snapshotting",
            Self::CatchingUp => "catching_up",
            Self::InSync => "in_sync",
            Self::Stale => "stale",
            Self::Failed => "failed",
        }
    }

    pub fn cat_display_str(self) -> &'static str {
        match self {
            Self::Unassigned => "UNASSIGNED",
            Self::Initializing => "INITIALIZING",
            Self::Snapshotting => "SNAPSHOTTING",
            Self::CatchingUp => "CATCHING_UP",
            Self::InSync => "STARTED",
            Self::Stale => "STALE",
            Self::Failed => "FAILED",
        }
    }

    pub fn is_acknowledgement_eligible(self) -> bool {
        matches!(self, Self::InSync)
    }

    pub fn is_promotion_eligible(self) -> bool {
        matches!(self, Self::InSync)
    }

    pub fn can_schedule_recovery(self) -> bool {
        matches!(self, Self::Initializing)
    }

    pub fn accepts_live_replication(self, awaiting_membership: bool) -> bool {
        matches!(self, Self::InSync | Self::Stale)
            || (matches!(self, Self::CatchingUp) && awaiting_membership)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next || matches!(next, Self::Unassigned) {
            return true;
        }
        matches!(
            (self, next),
            (Self::Unassigned, Self::Initializing)
                | (
                    Self::Initializing,
                    Self::Snapshotting | Self::CatchingUp | Self::InSync | Self::Failed,
                )
                | (
                    Self::Snapshotting,
                    Self::CatchingUp | Self::Initializing | Self::Failed,
                )
                | (
                    Self::CatchingUp,
                    Self::InSync | Self::Initializing | Self::Failed,
                )
                | (
                    Self::InSync,
                    Self::Stale | Self::Initializing | Self::Failed,
                )
                | (
                    Self::Stale,
                    Self::InSync | Self::Initializing | Self::Failed,
                )
                | (
                    Self::Failed,
                    Self::Initializing | Self::Snapshotting | Self::CatchingUp | Self::InSync,
                )
        )
    }
}

impl std::fmt::Display for ReplicaCopyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaCopyLifecycleSnapshot {
    pub index_name: String,
    pub index_uuid: String,
    pub shard_id: u32,
    pub node_id: String,
    pub allocation_id: Option<AllocationId>,
    pub primary_term: u64,
    pub state: ReplicaCopyState,
    pub awaiting_membership: bool,
    pub attempts: u32,
    pub backoff_delay_ms: u64,
    pub retry_ready: bool,
    pub lag_ops: Option<u64>,
    pub last_error: Option<String>,
    pub transition_count: u64,
}

#[derive(Debug, Clone)]
struct TrackedReplicaState {
    index_uuid: String,
    node_id: String,
    allocation_id: Option<AllocationId>,
    primary_term: u64,
    state: ReplicaCopyState,
    awaiting_membership: bool,
    attempts: u32,
    backoff_delay: Duration,
    next_retry_at: Option<Instant>,
    lag_ops: Option<u64>,
    last_error: Option<String>,
    transition_count: u64,
}

#[derive(Default)]
pub struct ReplicaStateRegistry {
    entries: RwLock<HashMap<(ShardKey, String), TrackedReplicaState>>,
}

impl ReplicaStateRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transition(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        node_id: &str,
        allocation_id: Option<AllocationId>,
        primary_term: u64,
        next_state: ReplicaCopyState,
        awaiting_membership: bool,
        error: Option<String>,
    ) -> Result<ReplicaCopyState> {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let map_key = (key.clone(), node_id.to_string());
        let current = entries.get(&map_key).cloned();
        let effective_term = if primary_term == 0 {
            current
                .as_ref()
                .filter(|existing| {
                    existing.index_uuid == index_uuid && existing.allocation_id == allocation_id
                })
                .map(|existing| existing.primary_term)
                .unwrap_or(1)
        } else {
            primary_term
        };
        if let Some(ref existing) = current {
            let same_identity = existing.index_uuid == index_uuid
                && existing.allocation_id == allocation_id
                && existing.primary_term == effective_term;
            if same_identity && !existing.state.can_transition_to(next_state) {
                anyhow::bail!(
                    "invalid replica copy state transition for {}/{} on {} (allocation {:?}): {} -> {}",
                    key.index,
                    key.shard_id,
                    node_id,
                    allocation_id,
                    existing.state,
                    next_state
                );
            }
        }
        let previous_state = current
            .as_ref()
            .filter(|existing| {
                existing.index_uuid == index_uuid && existing.allocation_id == allocation_id
            })
            .map(|existing| existing.state)
            .unwrap_or(ReplicaCopyState::Unassigned);
        let transition_count = current
            .as_ref()
            .filter(|existing| {
                existing.index_uuid == index_uuid && existing.allocation_id == allocation_id
            })
            .map(|existing| {
                if existing.state == next_state {
                    existing.transition_count
                } else {
                    existing.transition_count.saturating_add(1)
                }
            })
            .unwrap_or(1);
        let (attempts, backoff_delay, next_retry_at) = current
            .as_ref()
            .filter(|existing| {
                existing.index_uuid == index_uuid && existing.allocation_id == allocation_id
            })
            .map(|existing| {
                if matches!(next_state, ReplicaCopyState::InSync) {
                    (0, Duration::ZERO, None)
                } else {
                    (
                        existing.attempts,
                        existing.backoff_delay,
                        existing.next_retry_at,
                    )
                }
            })
            .unwrap_or((0, Duration::ZERO, None));

        if previous_state != next_state {
            crate::metrics::record_replica_state_transition(
                previous_state.as_str(),
                next_state.as_str(),
            );
        }

        entries.insert(
            map_key,
            TrackedReplicaState {
                index_uuid: index_uuid.to_string(),
                node_id: node_id.to_string(),
                allocation_id,
                primary_term: effective_term,
                state: next_state,
                awaiting_membership,
                attempts,
                backoff_delay,
                next_retry_at,
                lag_ops: current.and_then(|c| c.lag_ops),
                last_error: error,
                transition_count,
            },
        );
        Ok(previous_state)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_retry_backoff(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        node_id: &str,
        allocation_id: AllocationId,
        primary_term: u64,
        initial_backoff: Duration,
        max_backoff: Duration,
        error: String,
    ) -> Duration {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let map_key = (key.clone(), node_id.to_string());
        let previous = entries.get(&map_key).filter(|existing| {
            existing.index_uuid == index_uuid && existing.allocation_id == Some(allocation_id)
        });
        let previous_state = previous
            .map(|existing| existing.state)
            .unwrap_or(ReplicaCopyState::Initializing);
        let attempts = previous
            .map(|existing| existing.attempts.saturating_add(1))
            .unwrap_or(1);
        let delay = previous
            .and_then(|existing| {
                (existing.backoff_delay > Duration::ZERO)
                    .then(|| (existing.backoff_delay * 2).min(max_backoff))
            })
            .unwrap_or_else(|| initial_backoff.min(max_backoff));
        let transition_count = previous
            .map(|existing| {
                if existing.state == ReplicaCopyState::Initializing {
                    existing.transition_count
                } else {
                    existing.transition_count.saturating_add(1)
                }
            })
            .unwrap_or(1);
        if previous_state != ReplicaCopyState::Initializing {
            crate::metrics::record_replica_state_transition(
                previous_state.as_str(),
                ReplicaCopyState::Initializing.as_str(),
            );
        }
        let now = Instant::now();
        entries.insert(
            map_key,
            TrackedReplicaState {
                index_uuid: index_uuid.to_string(),
                node_id: node_id.to_string(),
                allocation_id: Some(allocation_id),
                primary_term,
                state: ReplicaCopyState::Initializing,
                awaiting_membership: false,
                attempts,
                backoff_delay: delay,
                next_retry_at: Some(now + delay),
                lag_ops: None,
                last_error: Some(error),
                transition_count,
            },
        );
        delay
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_lag_observation(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        node_id: &str,
        allocation_id: AllocationId,
        primary_term: u64,
        lag_ops: Option<u64>,
        exceeds_lag_policy: bool,
    ) {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let map_key = (key.clone(), node_id.to_string());
        let target_state = if exceeds_lag_policy {
            ReplicaCopyState::Stale
        } else {
            ReplicaCopyState::InSync
        };
        if let Some(existing) = entries.get_mut(&map_key)
            && existing.index_uuid == index_uuid
            && existing.allocation_id == Some(allocation_id)
        {
            if matches!(
                existing.state,
                ReplicaCopyState::InSync | ReplicaCopyState::Stale
            ) && existing.state != target_state
            {
                crate::metrics::record_replica_state_transition(
                    existing.state.as_str(),
                    target_state.as_str(),
                );
                existing.state = target_state;
                existing.transition_count = existing.transition_count.saturating_add(1);
            }
            existing.lag_ops = lag_ops;
            existing.primary_term = existing.primary_term.max(primary_term);
            return;
        }
        crate::metrics::record_replica_state_transition(
            ReplicaCopyState::Unassigned.as_str(),
            target_state.as_str(),
        );
        entries.insert(
            map_key,
            TrackedReplicaState {
                index_uuid: index_uuid.to_string(),
                node_id: node_id.to_string(),
                allocation_id: Some(allocation_id),
                primary_term,
                state: target_state,
                awaiting_membership: false,
                attempts: 0,
                backoff_delay: Duration::ZERO,
                next_retry_at: None,
                lag_ops,
                last_error: None,
                transition_count: 1,
            },
        );
    }

    pub fn snapshot(
        &self,
        key: &ShardKey,
        node_id: &str,
        now: Instant,
    ) -> Option<ReplicaCopyLifecycleSnapshot> {
        let entries = self
            .entries
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = entries.get(&(key.clone(), node_id.to_string()))?;
        Some(ReplicaCopyLifecycleSnapshot {
            index_name: key.index.clone(),
            index_uuid: entry.index_uuid.clone(),
            shard_id: key.shard_id,
            node_id: entry.node_id.clone(),
            allocation_id: entry.allocation_id,
            primary_term: entry.primary_term,
            state: entry.state,
            awaiting_membership: entry.awaiting_membership,
            attempts: entry.attempts,
            backoff_delay_ms: entry.backoff_delay.as_millis() as u64,
            retry_ready: entry
                .next_retry_at
                .is_none_or(|next_retry| now >= next_retry),
            lag_ops: entry.lag_ops,
            last_error: entry.last_error.clone(),
            transition_count: entry.transition_count,
        })
    }

    pub fn remove_shard(&self, index: &str, shard_id: u32) {
        let key = ShardKey::new(index, shard_id);
        self.entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(entry_key, _), _| entry_key != &key);
    }

    pub fn remove_index(&self, index: &str) {
        self.entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(entry_key, _), _| entry_key.index != index);
    }

    pub fn evaluate_copy_state(
        &self,
        cluster_state: &ClusterState,
        index: &str,
        shard_id: u32,
        node_id: &str,
        local_copy_failed: bool,
    ) -> ReplicaCopyState {
        let Some(metadata) = cluster_state.indices.get(index) else {
            return ReplicaCopyState::Unassigned;
        };
        let Some(routing) = metadata.shard_routing.get(&shard_id) else {
            return ReplicaCopyState::Unassigned;
        };
        if !cluster_state.nodes.contains_key(node_id)
            || !routing.replicas.iter().any(|replica| replica == node_id)
        {
            return ReplicaCopyState::Unassigned;
        }
        let Some(allocation_id) = cluster_state.shard_allocation_id(index, shard_id, node_id)
        else {
            return ReplicaCopyState::Unassigned;
        };
        if local_copy_failed {
            return ReplicaCopyState::Failed;
        }
        let key = ShardKey::new(index, shard_id);
        let tracked = self
            .entries
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(key, node_id.to_string()))
            .filter(|entry| {
                entry.index_uuid == metadata.uuid.as_str()
                    && entry.allocation_id == Some(allocation_id)
            })
            .map(|entry| entry.state);

        if routing.is_replica_in_sync(node_id) {
            match tracked {
                Some(ReplicaCopyState::Stale) => ReplicaCopyState::Stale,
                Some(ReplicaCopyState::Failed) => ReplicaCopyState::Failed,
                _ => ReplicaCopyState::InSync,
            }
        } else {
            match tracked {
                Some(
                    state @ (ReplicaCopyState::Snapshotting
                    | ReplicaCopyState::CatchingUp
                    | ReplicaCopyState::Failed),
                ) => state,
                _ => ReplicaCopyState::Initializing,
            }
        }
    }
}
