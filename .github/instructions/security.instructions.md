# Security Module — src/security/

Use when changing HTTP authentication, authorization, protected system-index behavior, body-routed global endpoints, or security configuration.

## Core Model
- Security is core enforcement, not a plugin boundary. Providers can become pluggable later, but Axum middleware and authorization checks stay in core code.
- `AppConfig.security` is disabled by default. When enabled, `SecurityManager` authenticates `Authorization: ApiKey <secret>` or `Authorization: Bearer <secret>` by SHA-256 hashing the presented secret and comparing against configured bootstrap hashes.
- `Principal` carries `name`, `key_id`, `roles`, and optional `indices` patterns. Empty `indices` means all indices; suffix `*` patterns are prefix matches.
- Role names currently recognized by core authorization: `admin`, `all_access`, `security_admin`, `manage`, `write`, `read`, and `metrics`.

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