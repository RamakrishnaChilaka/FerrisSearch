# Security Module — src/security/

Use when changing HTTP authentication, authorization, protected system-index behavior, body-routed global endpoints, or security configuration.

## Core Model
- Security is core enforcement, not a plugin boundary. Providers can become pluggable later, but Axum middleware and authorization checks stay in core code.
- `AppConfig.security` is disabled by default. When enabled, `SecurityManager` authenticates `Authorization: ApiKey <secret>` or `Authorization: Bearer <secret>` by SHA-256 hashing the presented secret and comparing against configured bootstrap hashes.
- `Principal` carries `name`, `key_id`, `roles`, and optional `indices` patterns. Empty `indices` means all indices; suffix `*` patterns are prefix matches.
- Role names currently recognized by core authorization: `admin`, `all_access`, `security_admin`, `manage`, `write`, `read`, and `metrics`.

## Dynamic Security Control Plane (runtime API keys + custom roles)
This is the canonical example of the control-plane idiom — read `control-plane.instructions.md` first; it has the full end-to-end recipe. Security-specific points:

- **Storage is Raft `ClusterState`, never the `.ferris_security` Tantivy index.** API-key hashes and role definitions live in `ClusterState.api_keys: HashMap<String, SecurityApiKeyRecord>` and `ClusterState.roles: HashMap<String, SecurityRoleDefinition>` (both `#[serde(default)]`, defined in `src/cluster/state.rs`). They are snapshotted/restored for free with the rest of `ClusterState`. The `.ferris_security` index lifecycle is unchanged and is **not** the storage mechanism for keys/roles.
- **Mutations go through `ClusterCommand`**: `PutApiKey { record }`, `DeleteApiKey { key_id }`, `PutRole { role }`, `DeleteRole { name }`. Each apply arm mutates the map and bumps `state.version`. Forwarded over typed transport RPCs (`PutApiKey` / `DeleteApiKey` / `PutRole` / `DeleteRole`, `*_json` request payload, `{acknowledged,error}` response) that mirror `AddMappings`.
- **Secrets**: the plaintext API key is generated server-side with the OS CSPRNG (`getrandom`, 32 bytes → `crate::security::hex_encode` → 64 hex chars), returned to the caller exactly **once** in the create response, and never persisted or logged. Only `sha256_hex(secret)` is stored. `id = uuid::Uuid::new_v4()`. Bootstrap keys (config file) and dynamic keys (cluster state) coexist; bootstrap keys are config-only and not deletable via the API.
- **`SecurityManager` reads dynamic state under a short read-lock.** Construct with `with_cluster_state(config, Arc<RwLock<ClusterState>>)` to enable dynamic keys/roles; keep `new(config)` (static-only, `dynamic_state: None`) so existing unit tests compile unchanged. `authenticate_api_key` checks static bootstrap keys first (`constant_time_eq` on the hash), then scans `state.api_keys`. `authorize` checks built-in role names first (no lock), then consults `state.roles` for custom roles.
- **Custom role mapping** (`dynamic_role_allows`): `index_privileges` strings map to index actions (`read`→`IndexRead`; `write`→`IndexRead`+`IndexWrite`; `admin`/`all`→ + `IndexAdmin`), gated by `role.indices` patterns (empty = all). `cluster` strings map to cluster actions (`monitor`→`ClusterMonitor`, `state`→`ClusterStateRead`, `admin`→`ClusterAdmin`, `metrics`→`MetricsRead`, `security`→`SecurityAdmin`, `all`→everything).

## `/_security/*` REST API (src/api/security.rs)
- `POST /_security/api_key` — create: CSPRNG secret, store hash, return `{id, name, api_key (ONCE), roles, indices, created:true}` (201).
- `GET /_security/api_key` / `GET /_security/api_key/{id}` — list/get **metadata only**; never return `hash_sha256` or any secret.
- `DELETE /_security/api_key/{id}` — 404 if the id is not in the dynamic store (bootstrap keys are not deletable here).
- `PUT /_security/role/{name}`, `GET /_security/role[/{name}]`, `DELETE /_security/role/{name}` — custom role CRUD.
- Writes use the coordinator pattern (`resolve_leader_or_master` → `forward_*` else `raft_write`); reads serve locally from `cluster_manager.get_state()`. Handlers must still function when security is disabled.
- Endpoint authz is automatic: `classify_request` maps `["_security", ..]` (and `["_plugins","_security",..]`) to `SecurityAction::SecurityAdmin`, enforced by `auth_middleware` when security is enabled. A non-admin principal therefore gets 403 on `/_security/*` without any handler-side check.

## Protected System Index
- Protected index name: `.ferris_security`.
- Normal user-facing index names still go through `IndexName`, which rejects dot-prefixed names. Internal security index creation must use `security_index_metadata()` and Raft `ClusterCommand::CreateIndex`.
- Security-enabled leaders auto-create `.ferris_security` only when `auto_create_security_index` is explicitly true and data nodes are known. The metadata is one shard, strict dynamic mapping, explicit `doc_type` / `payload` fields, and an adaptive replica count of `data_nodes - 1`.
- The leader lifecycle loop must continue reconciling `.ferris_security` after creation: add replicas when data nodes join, trim replicas when data nodes leave, and persist changes with Raft `UpdateIndex`.
- Ordinary index APIs, global bulk, SQL metadata commands, cat endpoints, and `SHOW TABLES` must not expose `.ferris_security`. Dedicated security APIs should be the only management path.

## Authorization Surfaces
- Path-routed endpoints are classified in `classify_request()` and enforced by `auth_middleware()`.
- Body-routed global endpoints need handler-level authorization after parsing resource names:
  - `POST /_bulk`: validate raw action `_index`, reject `.ferris_security`, and authorize `IndexWrite` per item before metadata lookup or auto-create.
  - `POST /_sql` and `POST /_sql/stream`: authorize extracted table names for `DESCRIBE`, `SHOW CREATE TABLE`, and `SELECT ... FROM` before metadata lookup or execution.
  - `SHOW TABLES` / `SHOW INDICES`: filter rows by the principal's index allow-list and hide `.ferris_security`.

## Testing
- Add middleware tests for disabled mode, missing auth, invalid auth, and role denial.
- Add body-routed tests for `_bulk` and global SQL because middleware cannot infer those target indices from the path.
- Add protected system-index tests whenever a new catalog, metadata, SQL, or admin surface can enumerate or target indices.
- For the dynamic control plane, follow the three-layer rule in `control-plane.instructions.md`: unit (types serde + state_machine apply/version bump + `ClusterState` snapshot/old-snapshot default), transport (direct gRPC `PutApiKey`/`DeleteApiKey`/`PutRole`/`DeleteRole`), and coordinator/multi-node (a follower forwards the write to the leader). Also assert: a created key authenticates, revoking it denies immediately, a custom role authorizes its mapped actions, static + dynamic keys coexist, `GET` never leaks the hash, and a non-admin principal gets 403 on `/_security/*`.