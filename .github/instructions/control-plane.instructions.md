---
description: "Use when adding or changing any Raft-replicated cluster-config mutation: new ClusterCommand variants, ClusterState fields, transport-forwarded write RPCs, or coordinator-pattern API write handlers. This is the canonical recipe for FerrisSearch control-plane changes."
applyTo: "src/consensus/**,src/cluster/state.rs,src/transport/server/**,src/transport/client.rs,src/api/mod.rs,src/api/security.rs,proto/transport.proto"
---

# Control-Plane Mutation Recipe — the FerrisSearch idiom

This file is the **single canonical recipe** for changing Raft-replicated cluster
configuration (nodes, indices, mappings, settings, master, API keys, roles, …).
Cluster *config* is small, strongly-consistent, and globally readable; it lives in
`ClusterState` and is mutated **only** through the Raft log. It is NOT document data
— never store control-plane state in shards, the WAL, ISR, or the `.ferris_security`
Tantivy index.

> **Golden rule:** to add a new control-plane mutation, copy an existing
> `ClusterCommand` variant end-to-end. `AddMappings` and the security commands
> (`PutApiKey` / `DeleteApiKey` / `PutRole` / `DeleteRole`) are the reference
> implementations. Do not invent a new storage or replication path.

Conditional shard-authority commands (`MarkReplicaInSync` and
`ActivatePrimary`) return `ClusterResponse::Error` when their UUID/primary/term
compare-and-set fails. Callers must inspect `client_write(...).data`; an
OpenRaft transport success is not proof that the application command applied.
Rejected conditional commands perform no partial mutation and do not bump the
cluster-state version.

## When to use this recipe (vs. the data path)

| Use the control-plane (this recipe) | Use the data path (docs/shards/WAL) |
|-------------------------------------|-------------------------------------|
| Cluster topology, index metadata, mappings, settings, master | Indexed documents, search hits |
| API-key hashes, role definitions, ACLs | Per-document `_source`, vectors |
| Anything needing instant cluster-wide consistency & cheap in-memory reads | High-volume, sharded, partially-replicated data |
| Low volume, snapshot-friendly | Large, needs ISR/translog/recovery |

If the thing you are storing is **small, must be identical on every node, and is read
on a hot path**, it belongs in `ClusterState`. Reads are then a short read-lock on the
shared `Arc<RwLock<ClusterState>>` — zero extra I/O.

## Why `ClusterState` snapshots your new field for free

The state machine snapshots the entire `ClusterState` with serde:
`serde_json::to_vec(&state)` in `src/consensus/state_machine.rs` (the
`ClusterSnapshotBuilder` path). Restore is the symmetric `serde_json::from_slice`.
Therefore **any new field you add to `ClusterState` is persisted and restored
automatically** — provided you make it backward-compatible with old snapshots:

```rust
// src/cluster/state.rs — on the ClusterState struct
#[serde(default)] pub api_keys: HashMap<String, SecurityApiKeyRecord>,
#[serde(default)] pub roles:    HashMap<String, SecurityRoleDefinition>,
```

`#[serde(default)]` is **mandatory** on every new `ClusterState` field: a node may load
a snapshot written before the field existed. Also initialize the field in
`ClusterState::new()` (and anywhere a `ClusterState` is constructed literally, e.g.
`Default`). Embedded structs derive `Serialize, Deserialize, Clone, Debug, PartialEq`
(add `Eq` when all fields are `Eq`), and use `#[serde(default)]` on their own optional
fields for the same forward-compat reason.

### ⚠️ The transport proto `ClusterState` is a SEPARATE representation — keep it in sync

The Raft *snapshot* serde is automatic, but `proto/transport.proto`'s `message ClusterState`
is a **second, hand-written** representation used by the `JoinCluster` response snapshot
(`cluster_state_to_proto` / `proto_to_cluster_state` in `src/transport/server/conversions.rs`).
Adding a `#[serde(default)]` field to the domain `ClusterState` does **not** update the proto —
the field is silently dropped on every join/startup snapshot roundtrip. The codebase invariant
is that **ClusterState transport snapshots must be lossless**, so when you add a control-plane
field you must also:

1. Add a field to `message ClusterState` in `proto/transport.proto`. For Raft-managed
   records, carrying them as `repeated string <name>_json` (serialized records) keeps proto
   churn minimal and matches how the forward RPCs already ship the records.
2. Populate it in `cluster_state_to_proto` (serialize each record to JSON).
3. Parse it back in `proto_to_cluster_state`, returning `Status::invalid_argument` on a
   malformed entry — **never** silently drop it (mirrors the strict field-type/engine decoding
   already there).
4. Add a roundtrip test in `src/transport/server/tests.rs` with the field populated, asserting
   equality after `cluster_state_to_proto` → `proto_to_cluster_state`, plus a malformed-entry
   rejection test.

## The 7 steps (every step has a copy-paste anchor)

Implement in this dependency order — each step compiles on top of the previous one.

### 1. Data structs → `src/cluster/state.rs`
Put the serde structs **in `cluster::state`**, not in `security` or `consensus`, so
`ClusterState` can embed them and `consensus::types` can import them without a circular
dependency (`consensus` already depends on `cluster::state`). Reference structs:
`SecurityApiKeyRecord`, `SecurityRoleDefinition`.

- Store **derived/safe** data only. For secrets, store the SHA-256 hash (64-char hex),
  never plaintext. Normalize on the way in.
- Add the field(s) to `ClusterState` with `#[serde(default)]`; init in `new()`.
- **Tests:** serde roundtrip of the struct; a `ClusterState` roundtrip with the field
  populated (snapshot safety); and an *old-snapshot* JSON literal missing the field that
  still deserializes via `#[serde(default)]`.

### 2. `ClusterCommand` variant + `Display` + apply arm
Three edits, all mirroring `AddMappings`:

- **`src/consensus/types.rs`** — add the enum variant (carry the whole record, or just
  an id/name for deletes) and a `Display` arm (used in logs; keep it short and
  **never** print secrets). Add a serde JSON roundtrip test per variant.
- **`src/consensus/state_machine.rs`** — add the `apply_command` match arm. Mutate the
  map, then **always `state.version += 1;`** — even for idempotent no-ops and deletes of
  absent keys (mirrors `DeleteIndex` / `RemoveNode`). Use `.entry().or_insert()` when
  first-write-wins idempotency matters (mappings); use `.insert()` for upserts (keys,
  roles); use `.remove()` for deletes.
  ```rust
  ClusterCommand::PutApiKey { record } => {
      state.api_keys.insert(record.id.clone(), record.clone());
      state.version += 1;
  }
  ClusterCommand::DeleteApiKey { key_id } => {
      state.api_keys.remove(key_id);   // bump version even if absent
      state.version += 1;
  }
  ```
- **Tests:** in `state_machine.rs`, apply each command and assert the map changed **and**
  `version` increased.

### 3. proto messages + rpc → `proto/transport.proto`
Add a `Request`/`Response` pair per command and one `rpc` line in the
`InternalTransport` service. Match the `AddMappings` shape exactly:

```proto
message PutApiKeyRequest  { string record_json = 1; }     // serialize the whole record as JSON
message PutApiKeyResponse { bool acknowledged = 1; string error = 2; }
message DeleteApiKeyRequest  { string key_id = 1; }
message DeleteApiKeyResponse { bool acknowledged = 1; string error = 2; }
// ... in service InternalTransport { ... }
rpc PutApiKey(PutApiKeyRequest) returns (PutApiKeyResponse);
```

- The `{ bool acknowledged; string error; }` response is the house convention: a
  non-empty `error` string means failure (client maps it to `Err`). Don't use a tonic
  `Status` for application-level failures here.
- Carrying the record as a single `record_json` string keeps proto churn minimal and
  reuses the serde structs as the wire contract. Typed sub-messages are also acceptable,
  but `*_json` matches the lowest-friction convention for control-plane records.
- `cargo build` regenerates the tonic stubs (build.rs runs `tonic-build`). After editing
  the proto you must rebuild before the server/client edits will compile.

### 4. Server handler → `src/transport/server/mod.rs`
Mirror `add_mappings`. **Every** Raft-write handler is leader-only:

```rust
let raft = self.raft.as_ref()
    .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
if !raft.is_leader() {
    return Err(Status::failed_precondition(
        "This node is not the Raft leader — caller should forward",
    ));
}
let record: SecurityApiKeyRecord = serde_json::from_str(&req.record_json)
    .map_err(|e| Status::invalid_argument(format!("invalid api key record: {e}")))?;
let cmd = ClusterCommand::PutApiKey { record };
raft.client_write(cmd).await
    .map_err(|e| Status::internal(format!("Raft PutApiKey failed: {e}")))?;
Ok(Response::new(PutApiKeyResponse { acknowledged: true, error: String::new() }))
```

- `failed_precondition` on a non-leader is **expected**: the *follower's API handler*
  is responsible for not calling this RPC on a follower; it forwards to the master
  instead (step 6). The handler itself never mutates local state on a follower.
- Deserialization failures are `invalid_argument`; Raft failures are `internal`.
- Do **not** `unwrap_or_default()` protocol payloads — surface decode errors.
- **Validate at this trust boundary.** The transport RPC is the node-to-node entry point;
  a record can arrive here without going through the HTTP handler. After deserializing,
  re-check the security-critical invariants before `client_write` (e.g. a `SecurityApiKeyRecord`
  must have a non-empty `id`/`name` and a 64-char hex `hash_sha256`), returning
  `invalid_argument` on a bad record. This mirrors `add_mappings` validating its proto
  field types, and keeps a malformed or hostile record off the replicated log.

### 5. Client forward → `src/transport/client.rs`
Mirror `forward_add_mappings`: connect to the master, call the rpc, map a non-empty
`error` to `Err`:

```rust
pub async fn forward_put_api_key(&self, master: &NodeInfo, record: &SecurityApiKeyRecord)
    -> Result<(), anyhow::Error> {
    let mut client = self.connect(&master.host, master.transport_port).await
        .map_err(|e| anyhow::anyhow!("connect to master for PutApiKey: {e}"))?;
    let record_json = serde_json::to_string(record)?;
    let resp = client.put_api_key(tonic::Request::new(PutApiKeyRequest { record_json })).await
        .map_err(|e| anyhow::anyhow!("PutApiKey RPC: {e}"))?;
    let inner = resp.into_inner();
    if !inner.error.is_empty() { return Err(anyhow::anyhow!("{}", inner.error)); }
    Ok(())
}
```

### 6. API handler → coordinator pattern (`src/api/*`)
Any node can accept the HTTP request. **Never** return "not the leader". Use the shared
helpers in `src/api/mod.rs`:

- `resolve_leader_or_master(&state, "op label")` → `Ok(None)` if **this** node is leader,
  `Ok(Some(master))` if a follower (forward there), `Err(response)` if no master exists.
- `raft_write(&state, cmd)` → `client_write` on the local leader, mapping errors to a
  `raft_write_exception` response.
- `error_response(status, "error_type", reason)` for all error shapes.

```rust
match resolve_leader_or_master(&state, "api key creation") {
    Ok(Some(master)) => {
        state.transport_client.forward_put_api_key(&master, &record).await
            .map_err(|e| /* forward_exception 500 */)?;
    }
    Ok(None) => { raft_write(&state, ClusterCommand::PutApiKey { record: record.clone() }).await?; }
    Err(resp) => return resp,   // no master discovered
}
```

- **Reads serve locally**: `state.cluster_manager.get_state()` (clone snapshot) or a
  read-lock; never forward reads. For security, never leak the stored hash in any GET.
- **Writes return only safe data.** A create endpoint may return the freshly generated
  secret **once** in its HTTP response, but the secret must never be logged or persisted
  anywhere except as its hash inside the `ClusterState` record.

### 7. Tests (BLOCKING — see `testing.instructions.md`)
Cover every new branch at three layers:
1. **Unit** — types serde roundtrip; state_machine apply (mutation + version bump);
   `ClusterState` snapshot roundtrip + old-snapshot default.
2. **Transport** — a direct gRPC test of each new RPC against a real
   `TransportService` (leader applies; non-leader returns `failed_precondition`).
3. **Coordinator / multi-node** — at least one test where a **follower** node's API
   handler forwards the write to the leader and the change is observable on the leader.
   Per `testing.instructions.md`, multi-node REST harnesses must preserve real
   `raft_node_id`s and route at least one request through a non-master node.

## Hot-path read access from in-memory state
Consumers that need the new config on a hot path (e.g. `SecurityManager` auth) should
hold a `clone` of the same shared `Arc<RwLock<ClusterState>>` and take a **short**
read-lock, cloning out only the matched record. Wire the shared handle in
`src/node/mod.rs`: the `state_handle` created during startup is moved into
`ClusterManager::with_shared_state(...)`, so capture a `.clone()` **before** the move and
pass it to the consumer (this is exactly how `SecurityManager::with_cluster_state` is
wired). Keep a static/`None` constructor so existing unit tests that don't have a cluster
state still compile.

```rust
let state_handle: Arc<RwLock<ClusterState>> = /* created ~here */;
let security_manager = SecurityManager::with_cluster_state(
    config.security.clone(), state_handle.clone(),   // clone BEFORE the move
)?;
let cluster_manager = ClusterManager::with_shared_state(state_handle); // moves the original
```

## Anti-patterns (reject these in review)
- ❌ Storing control-plane config as Tantivy documents / in a shard / in the WAL.
- ❌ Mutating `ClusterState` directly via `ClusterManager::update_state()` for a new
  feature — that is a full overwrite reserved for the state machine; go through a
  `ClusterCommand`.
- ❌ Returning "not the leader" / "send to master" from an API handler instead of
  forwarding.
- ❌ Forgetting `state.version += 1` in an apply arm.
- ❌ A new `ClusterState` field without `#[serde(default)]` (breaks old-snapshot restore).
- ❌ Adding a `ClusterState` field but not the transport proto `ClusterState` message +
  `cluster_state_to_proto` / `proto_to_cluster_state` (silently dropped on join snapshots).
- ❌ `unwrap_or_default()` on a transport JSON payload (silently drops data).
- ❌ Committing a record to the Raft log from a transport RPC without validating it at the
  leader (the RPC is a trust boundary independent of the HTTP handler).
- ❌ Logging or returning a plaintext secret anywhere except the one-time create response.
