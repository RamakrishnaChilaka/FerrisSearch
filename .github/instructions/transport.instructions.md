---
description: "Use for internal gRPC services and clients, protobuf evolution, forwarding, streaming SQL, limits, and transport TLS."
applyTo: "src/transport/**,proto/transport.proto"
---

# Transport Module — src/transport/

## gRPC Service Definition (proto/transport.proto)

### InternalTransport Service — All RPCs
```
// Cluster coordination
JoinCluster(JoinRequest) → JoinResponse
PublishState(PublishStateRequest) → Empty  // returns UNIMPLEMENTED; Raft manages cluster state
Ping(PingRequest) → Empty

// Document operations (routed to shard primary)
IndexDoc(ShardDocRequest) → ShardDocResponse
BulkIndex(ShardBulkRequest) → ShardBulkResponse
DeleteDoc(ShardDeleteRequest) → ShardDeleteResponse
GetDoc(ShardGetRequest) → ShardGetResponse

// Search (scatter to remote shards)
SearchShard(ShardSearchRequest) → ShardSearchResponse
SearchShardDsl(ShardSearchDslRequest) → ShardSearchResponse
GetRemoteStoreLeafStatus(RemoteStoreLeafStatusRequest) → RemoteStoreLeafStatusResponse
SearchRemoteStoreSplits(RemoteStoreSearchRequest) → RemoteStoreSearchResponse

// Distributed SQL (scatter Arrow IPC batches from remote shards)
SqlRecordBatch(SqlRecordBatchRequest) → SqlRecordBatchResponse
SqlRecordBatchStream(SqlRecordBatchRequest) → stream SqlRecordBatchResponse

// Replication (primary → replica)
ReplicateDoc(ReplicateDocRequest) → ReplicateDocResponse
ReplicateBulk(ReplicateBulkRequest) → ReplicateBulkResponse
StartPeerRecovery(StartPeerRecoveryRequest) → StartPeerRecoveryResponse
FetchRecoveryFileChunk(FetchRecoveryFileChunkRequest) → FetchRecoveryFileChunkResponse
FetchRecoveryOps(FetchRecoveryOpsRequest) → FetchRecoveryOpsResponse
PrepareFinalizeRecovery(PrepareFinalizeRecoveryRequest) → PrepareFinalizeRecoveryResponse
CompleteFinalizeRecovery(CompleteFinalizeRecoveryRequest) → CompleteFinalizeRecoveryResponse
FailShardCopy(FailShardCopyRequest) → FailShardCopyResponse
MarkPrimaryUnavailable(MarkPrimaryUnavailableRequest) → MarkPrimaryUnavailableResponse
MarkPrimaryAvailable(MarkPrimaryAvailableRequest) → MarkPrimaryAvailableResponse

// Forwarded to leader
UpdateSettings(UpdateSettingsRequest) → UpdateSettingsResponse
CreateIndex(CreateIndexRequest) → CreateIndexResponse
DeleteIndex(DeleteIndexRequest) → DeleteIndexResponse
TransferMaster(TransferMasterRequest) → TransferMasterResponse
AddMappings(AddMappingsRequest) → AddMappingsResponse

// Dynamic security control plane (forwarded to leader; mirror AddMappings)
PutApiKey(PutApiKeyRequest) → PutApiKeyResponse         // record_json → {acknowledged,error}
DeleteApiKey(DeleteApiKeyRequest) → DeleteApiKeyResponse // key_id → {acknowledged,error}
PutRole(PutRoleRequest) → PutRoleResponse               // role_json → {acknowledged,error}
DeleteRole(DeleteRoleRequest) → DeleteRoleResponse      // name → {acknowledged,error}

// Shard stats (for _cat endpoints)
GetShardStats(ShardStatsRequest) → ShardStatsResponse
GetSegmentStats(SegmentStatsRequest) → SegmentStatsResponse

// Index maintenance (fan-out from coordinator)
RefreshIndex(IndexMaintenanceRequest) → IndexMaintenanceResponse
FlushIndex(IndexMaintenanceRequest) → IndexMaintenanceResponse
ForceMergeIndex(ForceMergeRequest) → ForceMergeResponse
GetTaskStatus(GetTaskStatusRequest) → GetTaskStatusResponse

// Raft consensus (opaque JSON payloads)
RaftVote(RaftRequest) → RaftReply
RaftAppendEntries(RaftRequest) → RaftReply
RaftSnapshot(RaftRequest) → RaftReply
```

`ShardDocResponse.seq_no`, `ShardDeleteResponse.seq_no`, and
`ShardBulkResponse.start_seq_no` are optional on the wire so sequence zero is
distinct from missing metadata. A successful single/delete response must carry
`seq_no` and `primary_term`; a successful non-empty bulk response must carry
`start_seq_no` and `primary_term`, while
an empty bulk must omit it. New clients fail closed on missing or inconsistent
receipt metadata. FerrisSearch is pre-1.0: successful responses require these
receipts, and metadata-free success responses from older peers fail. Do not add
compatibility fallbacks or rollout machinery for this protocol change.

`ClusterState.format_version` is required and must equal the one current wire
version. Missing/unknown versions are rejected with recreate guidance.
`ShardAssignment.in_sync_replica_node_ids` carries the authoritative replica
acknowledgement/promotion set in JoinCluster snapshots. Conversion must preserve
it losslessly and reject duplicate IDs, the primary ID, or any ID absent from
`replica_node_ids` with `INVALID_ARGUMENT`.
`ShardAssignment` also carries primary/replica allocation IDs, the initial
CreateIndex allocation ID, `primary_initialized`, and the status-only
`primary_unavailable` flag. Missing allocation metadata is rejected on join
snapshots; pre-1.0 snapshots are not adopted.

### Runtime And Code Generation

- Keep the Tonic runtime, Prost codec, generated service code, and optional
  rich-error types on one coordinated release line. The current manifest uses
  `tonic`, `tonic-prost`, `tonic-types`, and `tonic-prost-build` 0.14.6 with
  Prost 0.14.4.
- `build.rs` compiles `proto/transport.proto` through
  `tonic_prost_build::compile_protos()`. Generated clients and servers refer to
  `tonic_prost::ProstCodec`; do not hand-edit generated files under `target/`.
- Preserve `tonic/tls-ring` when changing transport features, and validate both
  default plaintext transport and `transport-tls` integration before accepting
  another coordinated stack change.

## TransportService (src/transport/server/mod.rs)
```rust
pub struct TransportService {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    pub storage_manager: Arc<StorageManager>,
    pub remote_store_reader_cache: Arc<RemoteSplitReaderCache>,
    pub raft: Option<Arc<RaftInstance>>,
    pub local_node_id: NodeId,  // filters locally-assigned shards
    pub worker_pools: WorkerPools,
    pub task_manager: Arc<TaskManager>,
    join_lock: Arc<tokio::sync::Mutex<()>>,
}
```
Implements `InternalTransport` trait. All RPC handlers check Raft leadership or route to the correct shard.

### Shard Stats & Maintenance
- `get_shard_stats` only reports on **already-open** shards via `all_shards()` — it does NOT reopen shards from disk
- `get_segment_stats` only reports on **already-open** shards via `all_shards()` and returns every segment row from `segment_infos()` for each local shard copy
- `refresh_index` / `flush_index` reopen assigned shards with the same read-side UUID-dir guard as `get_or_open_search_shard()`, then run the engine refresh/flush on Tokio's blocking pool; missing authoritative UUID dirs are logged and skipped rather than creating fresh shard data
- `force_merge_index` rejects `max_num_segments = 0` with
  `INVALID_ARGUMENT`; valid requests still return immediately after enqueueing
  node-local background work.
- The maintenance helper only operates on shards where `primary == local_node_id` or the node is in `replicas` — orphaned shards are skipped
- The constructors require a local node ID and task manager. Production uses
  `create_transport_service_with_raft_and_storage_handle()` so the node
  lifecycle and request handlers share one `TransportService` activation
  cache; `create_transport_service_for_test()` supplies isolated defaults.

### Key Handler Patterns
- **join_cluster**: If leader → serialize concurrent joins, validate `node_id` / `raft_node_id`, register the transport address with `add_learner()` for non-voters, apply `AddNode`, then recompute the latest full voter set before `change_membership()`. If promotion fails, roll back the `AddNode`. If follower → **forwards to leader** via gRPC. NEVER mutate cluster state locally on a follower.
- **publish_state**: Returns `UNIMPLEMENTED`. Cluster state is exclusively managed via Raft consensus; the legacy gossip-based state broadcast path has been removed.
- **ping**: Returns `NOT_FOUND` when `source_node_id` is absent from cluster state. A successful ping means the target still recognizes the caller as a registered cluster node and has refreshed `last_seen`; a rejected ping is the follower's signal to re-run `JoinCluster`.
- **index_doc / bulk_index / delete_doc**: Look up shard in ShardManager, execute
  the receipt-returning engine operation, replicate the receipt's exact sequence
  or range to all authoritative in-sync replicas, and return it to the caller. **Returns
  `success: false` if replication fails** — write is only acknowledged after all
  in-sync replicas confirm (synchronous replication contract). Assigned
  out-of-sync replicas receive no live writes and cannot fail the request.
- **replicate_doc / replicate_bulk**: Require index UUID, sender primary term,
  and target allocation ID. Revalidate the current local assignment inside the
  write worker; then validate durable identity, recovery gate, and term fence
  before WAL/engine mutation. A higher term is fsynced before mutation. Bulk
  validates the shared envelope and every item before the first mutation and
  advances the fence once.
- Primary and replica index RPCs reject reserved document-source metadata
  through the shared validator before dynamic mapping, WAL append, or replica
  apply. These failures are `INVALID_ARGUMENT`, allowing REST coordinators to
  return `400 mapper_parsing_exception`.
- `ReplicateBulk` accepts either contiguous ordered index operations or a
  strictly increasing, potentially non-contiguous homogeneous NoOp batch.
  Promotion activation uses bounded NoOp batches rather than one RPC per gap.
- Successful replica responses carry optional processed and persisted
  checkpoints and must prove the exact single operation or every bulk item was
  processed. A behind contiguous checkpoint is a gap observation, not failure
  of the current operation. Under request durability the client also requires
  exact-operation persisted proof; async durability may acknowledge processing
  before the timer fsync.
- A sequence/version collision is `DATA_LOSS`, is quarantined immediately even
  when the replica's routing view lags the message term, and retains exact node
  and allocation identity through fan-out. The primary conditionally reports
  that exact copy at the write's captured term; do not depend only on the
  replica's local routing view to start recovery. Quarantine is durable in the
  copy identity; later replication to the marked allocation remains
  `DATA_LOSS` and cannot reopen it.
- Gap probes run concurrently under a two-second per-probe timeout. A copy that
  is assigned but still opening/replaying returns `UNAVAILABLE`. Only proven
  UUID/allocation/durable-identity mismatch (`FAILED_PRECONDITION`) or
  corruption (`DATA_LOSS`) is definitive. Probe completion must still match
  the exact observation start/target identity before removal.
- Peer-recovery catch-up uses the strict WAL document decoder. Missing
  `_doc_id`, or missing `_source` on an index
  operation, fails closed; delete operations carry no synthetic source and
  must never be converted back into indexed documents.
- A catch-up response stops at the first source-unprocessed WAL frame. An empty
  incomplete response at the unchanged cursor sends the target to
  `PrepareFinalizeRecovery`, whose exclusive source barrier rebuilds/replays the
  writer before the remaining suffix is fetched.
- **peer recovery RPCs**: source sessions are
  UUID/target/allocation/primary-term bound,
  file chunks are at most 1 MiB, operation batches are bounded by count and
  bytes, and stale authority aborts the session. WAL catch-up uses physical
  generation/byte cursors. Prepare captures the exclusive barrier WAL end and
  processed checkpoint; Complete requires the target to match both before
  conditional membership admission.
- `StartPeerRecovery` is an asynchronous start/status RPC. `preparing=true`
  means the client should poll the same request/session reservation; snapshot
  commit/link/hash work is not performed in the RPC future.
- Snapshot preparation failures are retained and returned once on the next
  poll, so the target enters normal recovery backoff instead of relaunching
  setup in a tight loop.
- Snapshot preparation on a repaired idle primary rebuilds a failed writer and
  replays its retained WAL suffix before committing the source snapshot; it
  does not require an unrelated client write to heal the source.
- `StartPeerRecovery` may poll a safe pre-finalize session, but it must reject
  reattachment to the same session once finalization, admission, or settlement
  has begun.
- **search_shard / search_shard_dsl**: Execute local shard search, return results
- **get_remote_store_leaf_status**: Report whether the local node is root/leaf-capable plus per-split artifact/reader warmth and current `StorageManager` load counters
- **search_remote_store_splits**: Validate the remote_store index/UUID, batch split execution through the shared leaf helper, and return per-split hits, totals, partial aggs, and per-split errors
- **sql_record_batch / sql_record_batch_stream**: Execute local shard SQL fast-field reads and return Arrow IPC batches. `SqlRecordBatchStream` may emit multiple batches for the same shard; `batch_size = 0` means use the engine default. Stream responses must carry `total_hits`, `collected_rows`, and actual `streaming_used` metadata on every batch so the coordinator can build accurate `meta` / truncation decisions before draining the rest of the stream. The coordinator-facing live path should prefer `open_sql_batch_stream_to_shard()` so only the first response is read eagerly.
- **raft_vote / raft_append_entries / raft_snapshot**: Deserialize JSON, forward to Raft instance
- **create_index / delete_index**: Must be leader; execute via `raft.client_write()`. `create_index` must preserve index settings from the forwarded JSON body, including `refresh_interval_ms` and `flush_threshold_bytes`.
- `AddMappings` rejects every shared reserved document metadata name before
  checking leadership or issuing a Raft write. There is no public put-mapping
  HTTP route yet; this RPC is the existing mapping-update trust boundary.
- **update_settings**: Must be leader; apply via `UpdateIndex` Raft command. Preserve `flush_threshold_bytes` exactly, including `null` resets and `0` as a valid disable value.

### Critical Invariants
- **join_cluster MUST forward on followers**: A follower receiving a JoinCluster RPC must forward it to the Raft leader. It must NEVER fall through to `cluster_manager.add_node()` when Raft is active, as this would add the node to local state without Raft membership.
- **ping MUST reject unknown nodes**: `Ping` is not just a transport liveness check. If `source_node_id` is absent from cluster state, return `NOT_FOUND` instead of silently succeeding, or removed/stale nodes will keep serving an old cluster view forever and never trigger `JoinCluster` recovery.
- **Join identity MUST be unique and stable**: `raft_node_id` cannot be reused by a different logical node, and an existing `node_id` cannot silently switch to a different `raft_node_id`. Reject the join instead of mutating membership.
- **Shard writes MUST fail on replication failure**: The `index_doc`, `bulk_index`, and `delete_doc` handlers must return `success: false` when `replicate_write()` / `replicate_bulk()` returns `Err`. Logging the error and returning `success: true` violates the synchronous replication contract.
- **Replica apply MUST preserve primary seq_nos**: `replicate_doc` and
  `replicate_bulk` must call the explicit-seq engine methods. Do not route
  replicated writes through local seq allocation APIs.
- **Replica apply is allocation- and term-fenced**: missing identity fields,
  UUID/allocation mismatches, installing targets, and terms below
  `max(applied_view_term, durable_fence)` reject before mutation and propagate
  through the synchronous replication failure path.
- **Installing targets reject live replica apply.** A finalized target awaiting
  membership accepts live apply and remains open; its durable pending marker is
  reconciled to admitted/promoted or definitively rejected state after restart.
  The in-progress marker still prevents a partial install from being opened.
- Local copy failure forwarding reports corruption-class identity, marker, WAL,
  and Tantivy decode/validation failures immediately. Other open/fence/marker
  and Apply I/O remains retryable under operation-specific per-copy state until
  the shared count/time policy is exhausted. Quarantine happens only after the
  report throttle and only for definitive or open-level failures; Apply
  escalation leaves the engine open for reads and never triggers runtime WAL
  replay. Sequence/version collisions bypass the ordinary quarantine throttle.
  Replica escalation removes the copy; primary escalation is
  promote-only and requires an in-sync candidate.
- Production transport and node lifecycle share one primary-activation state.
  Proactive lifecycle activation and request-triggered activation are
  idempotent for the same UUID/shard/allocation/term. The unavailable flag alone
  does not bypass that cache. A repaired quarantined open-level failure forces a
  fresh activation; a successful local write after an Apply-level failure
  spawns throttled best-effort `MarkPrimaryAvailable` reporting without a term
  bump. The already-successful write response must not wait for Raft leadership
  discovery, forwarding, or the transport timeout of that status-only report.
  Check `primary_unavailable` through the shared applied-state read lock before
  cloning the service or spawning the task, so ordinary writes allocate no
  status-report work.
- **Primary handlers hold the shared recovery barrier** from before engine
  mutation through replication and read the authoritative in-sync targets
  inside that guard.
- Revalidate `(index_uuid, primary, primary_term)` after acquiring the guard
  and after dynamic-mapping Raft work, then use the same routing snapshot for
  replica fan-out. Queued old-primary or same-term replaced-index writes fail
  before open or mutation.
- Before dynamic-mapping reopen, abort the shard's safe pre-finalize source
  session and wait for cleanup. Never remove the shard-map engine while a
  source-session `Arc` still owns its Tantivy writer/directory lock.
- Cancelled PrepareFinalize futures must clear the preparing state through a
  drop guard. Reapers never idle-expire `finalize_preparing`,
  barrier-owning, or settlement-running sessions.
- Reopen/replacement waits only until setup releases the old engine Arc.
  Hashing and cancellation cleanup continue on Tokio's blocking pool.
- Idle setup reaping also waits only for engine release; it must not let a
  long hash delay settlement or pin reaping for unrelated sessions. Blocking
  setup completion, including panic, is terminal for engine-release waiters.
- A dynamic-mapping reopen that loses its registered UUID or live engine is a
  retryable `ABORTED` write failure. It must not fall through to mutation or
  recreate the deleted shard.
- Primary and explicit-sequence replica handlers classify the shared 32 MiB
  encoded WAL-frame ceiling as `INVALID_ARGUMENT`; recovery operation batches
  use that same byte ceiling.
- **Successful write responses MUST carry valid receipts**: zero is a valid
  sequence, not a missing-value sentinel. Clients must reject successful
  single/delete responses without `seq_no`. A single index response must match
  a non-empty requested document ID exactly; an empty requested ID permits a
  server-generated ID, but a successful response ID must never be empty.
  Clients must also reject non-empty bulk responses without a contiguous
  `start_seq_no`, empty bulks with a start, and bulk responses whose document
  IDs differ in length or order from the request.
- **Write-side shard reopen MUST validate metadata**: `get_or_open_shard()` must return `NOT_FOUND` when the index or shard is absent from cluster state. It must NEVER create a shard with empty mappings/default settings on write or replication paths.
- **Shard reopen on gRPC paths MUST be async-safe**: `get_or_open_shard()` / `get_or_open_search_shard()` are async helpers and must use `open_shard_with_settings_blocking()` so shard recovery/open does not block tonic's async tasks. Likewise, leader-side delete cleanup must use `close_index_shards_blocking_with_reason()`.
- **Read-side shard reopen MUST fail closed on UUID mismatch**: `get_or_open_search_shard()` must reject empty UUIDs and missing expected UUID directories instead of creating a fresh shard on a read path.
- **Transport serialization must fail loudly**: gRPC handlers and clients must not use `unwrap_or_default()` for protocol payloads (`source_json`, `payload_json`, `partial_aggs_json`, Raft snapshot fields). Serialization or decode failures must surface as RPC errors, not empty payloads or silently dropped hits.
- **Raft snapshot RPCs must require all fields**: missing `vote`, `meta`, or `data` in `RaftSnapshot` is `INVALID_ARGUMENT`, not a defaulted empty snapshot.
- **ClusterState transport snapshots must be lossless**: startup consumes
  `JoinCluster` snapshots for shard reopen and orphan cleanup, so proto/domain
  conversion must preserve `raft_node_id`, `unassigned_replicas`,
  `in_sync_replicas`, index `mappings`, index `settings`, and `uuid` exactly.
  Never synthesize defaults or new UUIDs during roundtrip, and reject invalid
  in-sync membership, unknown field types, or unknown non-empty engine strings
  instead of coercing them.

## TransportClient (src/transport/client.rs)
```rust
pub struct TransportClient {
    connections: RwLock<HashMap<String, InternalTransportClient<Channel>>>,
}
```
- **Connection pooling**: reuses gRPC channels per node address
- `connect(host, port)` — lazy connection establishment (public, used by server for join forwarding)

### Forwarding Methods
| Method | Purpose |
|--------|--------|
| `forward_create_index()` | Forward index creation to leader |
| `forward_delete_index()` | Forward index deletion to leader |
| `forward_update_settings()` | Forward settings update to leader |
| `forward_transfer_master()` | Forward leadership transfer |
| `forward_mark_primary_available()` | Clear exact primary-unavailable status after a successful local write |
| `forward_put_api_key()` | Forward dynamic API-key upsert to leader (control plane) |
| `forward_delete_api_key()` | Forward dynamic API-key deletion to leader (control plane) |
| `forward_put_role()` | Forward custom-role upsert to leader (control plane) |
| `forward_delete_role()` | Forward custom-role deletion to leader (control plane) |
| `forward_index_to_shard()` | Route doc write to shard primary — returns `Err` on shard failure |
| `forward_delete_to_shard()` | Route doc delete to shard primary — returns `Err` on shard failure |
| `forward_get_to_shard()` | Route doc get to shard primary |
| `forward_bulk_to_shard()` | Route bulk write to shard primary — returns `Err` on shard failure |
| `forward_search_to_shard()` | Scatter search to remote shard |
| `forward_search_dsl_to_shard()` | Scatter DSL search to remote shard |
| `get_remote_store_leaf_status()` | Fetch remote_store cache/load warmth from a remote leaf |
| `forward_remote_store_search()` | Execute a batched remote_store split search on a remote leaf |
| `forward_sql_batch_to_shard()` | Scatter SQL RecordBatch to remote shard (Arrow IPC) |
| `open_sql_batch_stream_to_shard()` | Open a live remote SQL batch stream, eagerly decode only the first batch + metadata, then keep the remaining gRPC stream live for `StreamingTable` partitions |
| `forward_sql_batch_stream_to_shard()` | Stream multiple SQL RecordBatches from a remote shard (Arrow IPC) and report whether the shard actually used streaming |
| `get_shard_stats()` | Collect shard doc counts from remote node |
| `get_segment_stats()` | Collect per-segment rows from remote node |
| `forward_refresh()` | Fan out refresh to remote node |
| `forward_flush()` | Fan out flush to remote node |
| `forward_force_merge()` | Enqueue async force-merge work on a remote node |
| `get_task_status()` | Fetch the node-local async force-merge task snapshot |
| `replicate_to_shard()` | Primary → replica single write |
| `replicate_bulk_to_shard()` | Primary → replica batch write |
| `recover_replica()` | Request missed ops from primary's WAL |

### Critical Invariant: Shard Forwarding Must Propagate Errors
`forward_index_to_shard()` and `forward_bulk_to_shard()` MUST return `Err(...)` when the shard RPC returns `success: false`. Never wrap a shard failure in `Ok(json!({"error": ...}))` — this hides failures from API handlers, causing them to return HTTP 201 for failed writes.

Successful forwarding returns the primary-assigned write identity:
`forward_index_to_shard()` / `forward_delete_to_shard()` expose `_seq_no` and
`_primary_term`, and
`forward_bulk_to_shard()` returns a typed `BulkWriteReceipt`. Do not reconstruct
these values from a later checkpoint.

### Critical Invariant: Remote Search Decode Must Not Drop Data
- `forward_search_to_shard()` and `forward_search_dsl_to_shard()` must fail if a remote hit payload cannot be decoded.
- Do not use `filter_map(...ok())` on transport hits — returning partial results as success hides wire-format and compatibility bugs.
- Partial aggregation decode failures must also return `Err(...)`, not an empty aggregation map.

### Critical Invariant: Stream Metadata Must Stay Stable Across Batches
- `SqlBatchStream` must treat `total_hits`, `collected_rows`, and `streaming_used` as shard-level invariants captured from the first response.
- If any later `SqlRecordBatchStream` response disagrees with those first-batch values, decoding must fail instead of letting the coordinator emit a misleading NDJSON `meta` frame.
- The server-side `SqlRecordBatchStream` path should consume `SqlStreamingBatchHandle` lazily on the search pool so remote shards do not prebuild all Arrow batches in memory before tonic starts draining them.

### gRPC Limits
- **Max message size**: 64MB (`max_decoding_message_size` / `max_encoding_message_size`) on both client and server. Default tonic limit is 4MB, insufficient for high-cardinality GROUP BY partial results (~7MB for 200K groups).
- **Request timeout**: 30s (set on the tonic `Endpoint`). Full-table GROUP BY on large shards can take 10s+.
- **Connect timeout**: 5s (separate from request timeout).
- Both limits are set in `TransportClient::connect()` (client-side) and `create_transport_service*()` (server-side).

### Worker Pool Integration
All blocking engine calls in `TransportService` handlers are dispatched to dedicated rayon thread pools via `self.worker_pools.spawn_search()` / `self.worker_pools.spawn_write()`:
- **Search pool** (`search-N` threads): `get_doc`, `search_shard`, `search_shard_dsl`, `sql_record_batch`, `sql_record_batch_stream`, `recover_replica` (WAL I/O)
- **Write pool** (`write-N` threads): `index_doc`, `bulk_index`, `delete_doc`, `replicate_doc`, `replicate_bulk`
- The `TransportService` struct holds `worker_pools: WorkerPools` initialized in `create_transport_service*()` constructors.
- Shard open/close/reopen are separate from engine work: use Tokio blocking-pool wrappers for those filesystem/Tantivy recovery steps before dispatching steady-state search/write work onto rayon.
- Refresh, checkpoint-aware flush, and force merge use Tokio's blocking pool after any reopen. Their shard-local maintenance lock may be held through long compaction, so they must not occupy the fixed write pool or starve unrelated writes and replica applies.

### Transport TLS (optional, feature-gated)
Inter-node gRPC can be encrypted via the `transport-tls` Cargo feature flag. Disabled by default.
```bash
cargo build --features transport-tls
```

**Config** (`config/ferrissearch.yml` or `FERRISSEARCH_*` env vars):
```yaml
transport_tls_enabled: true
transport_tls_cert_file: /path/to/node.pem
transport_tls_key_file: /path/to/node-key.pem
transport_tls_ca_file: /path/to/ca.pem
```

**Architecture:**
- `TlsConnector` trait in `client.rs` — abstracts TLS endpoint configuration, no `#[cfg]` on the struct or `connect()` method.
- `TonicTlsConnector` in `transport/mod.rs` — concrete implementation behind `#[cfg(feature = "transport-tls")]`, applies `ClientTlsConfig` with CA verification.
- `transport/mod.rs` installs the rustls ring crypto provider once before building server/client TLS config so feature builds do not panic at runtime.
- `load_server_tls_config()` in `transport/mod.rs` — loads PEM cert+key into `tonic::transport::ServerTlsConfig`.
- `TransportClient::with_tls_connector(Arc<dyn TlsConnector>)` — factory that sets up https:// scheme and TLS for all connections.
- Server-side TLS (`node/mod.rs`): `Server::builder().tls_config(config)` when feature + config are both active.
- `node/mod.rs` validates TLS config up front: `transport_tls_ca_file`, `transport_tls_cert_file`, and `transport_tls_key_file` are required when TLS is enabled.
- Enabling `transport_tls_enabled: true` without compiling `--features transport-tls` must return a startup error. Never silently downgrade to plaintext transport.
- When `transport_tls_enabled: false` (default), all behavior is identical to pre-TLS code — zero overhead.
- Integration coverage for the encrypted path lives in `tests/replication_integration.rs` and should be run with `cargo test --test replication_integration --features transport-tls`.
