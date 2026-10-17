//! Dynamic security control plane — runtime API-key and custom-role management.
//!
//! Secrets are stored the idiomatic Raft way: only the SHA-256 hash and role
//! metadata land in the Raft-replicated `ClusterState` (via the `PutApiKey` /
//! `DeleteApiKey` / `PutRole` / `DeleteRole` cluster commands, mirroring the
//! `AddMappings` idiom). The plaintext secret is generated server-side with a
//! CSPRNG, returned to the caller exactly once, and never persisted or logged.
//!
//! All writes follow the coordinator pattern: any node can accept the request;
//! if it is not the Raft leader it forwards the typed RPC to the master. Reads
//! are served locally from the in-memory cluster state under a short read-lock.

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{AppState, error_response, raft_write, resolve_leader_or_master};
use crate::cluster::state::{SecurityApiKeyRecord, SecurityRoleDefinition};
use crate::consensus::types::ClusterCommand;

/// Number of random bytes in a generated API-key secret (256 bits).
const SECRET_BYTES: usize = 32;

#[derive(Debug, Default, Deserialize)]
pub struct CreateApiKeyBody {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub indices: Vec<String>,
    #[serde(default)]
    pub max_concurrent_requests: Option<usize>,
    #[serde(default)]
    pub max_inflight_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AuditQueryParams {
    #[serde(default)]
    pub principal_id: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PutRoleBody {
    #[serde(default)]
    pub cluster: Vec<String>,
    #[serde(default)]
    pub indices: Vec<String>,
    #[serde(default)]
    pub index_privileges: Vec<String>,
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Generate a fresh high-entropy secret, hex-encoded. Uses the OS CSPRNG.
fn generate_secret() -> Result<String, (StatusCode, Json<Value>)> {
    let mut buf = [0u8; SECRET_BYTES];
    getrandom::getrandom(&mut buf).map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "security_exception",
            format!("failed to generate secret: {e}"),
        )
    })?;
    Ok(crate::security::hex_encode(&buf))
}

fn parse_body<T: Default + for<'de> Deserialize<'de>>(
    body: &Bytes,
) -> Result<T, (StatusCode, Json<Value>)> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|e| {
        error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!("invalid request body: {e}"),
        )
    })
}

// ─── API Keys ────────────────────────────────────────────────────────────────

/// `POST /_security/api_key` — create a new API key. Returns the plaintext
/// secret exactly once; only the hash is persisted.
pub async fn create_api_key(
    State(state): State<AppState>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let parsed: CreateApiKeyBody = match parse_body(&body) {
        Ok(p) => p,
        Err(e) => return e,
    };
    if parsed.name.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "api key 'name' is required",
        );
    }

    let secret = match generate_secret() {
        Ok(s) => s,
        Err(e) => return e,
    };
    let record = SecurityApiKeyRecord {
        id: uuid::Uuid::new_v4().to_string(),
        name: parsed.name,
        hash_sha256: crate::security::sha256_hex(&secret),
        roles: parsed.roles,
        indices: parsed.indices,
        created_at_millis: now_millis(),
    };

    // Coordinator: forward to leader or write locally via Raft.
    match resolve_leader_or_master(&state, "api key creation") {
        Ok(Some(master)) => {
            if let Err(e) = state
                .transport_client
                .forward_put_api_key(&master, &record)
                .await
            {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward api key creation to master: {e}"),
                );
            }
        }
        Ok(None) => {
            let cmd = ClusterCommand::PutApiKey {
                record: record.clone(),
            };
            if let Err(e) = raft_write(&state, cmd).await {
                return e;
            }
        }
        Err(e) => return e,
    }

    if parsed.max_concurrent_requests.is_some() || parsed.max_inflight_bytes.is_some() {
        state.security_manager.set_principal_quota(
            &record.id,
            crate::security::TenantQuotaConfig {
                max_concurrent_requests: parsed.max_concurrent_requests,
                max_inflight_bytes: parsed.max_inflight_bytes,
            },
        );
    }

    (
        StatusCode::CREATED,
        Json(json!({
            "id": record.id,
            "name": record.name,
            // Returned ONCE — never persisted or retrievable again.
            "api_key": secret,
            "roles": record.roles,
            "indices": record.indices,
            "created": true,
        })),
    )
}

fn api_key_metadata(record: &SecurityApiKeyRecord) -> Value {
    // Never leaks `hash_sha256`.
    json!({
        "id": record.id,
        "name": record.name,
        "roles": record.roles,
        "indices": record.indices,
        "created_at_millis": record.created_at_millis,
    })
}

/// `GET /_security/api_key` — list dynamic API-key metadata (never hashes).
pub async fn list_api_keys(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let cs = state.cluster_manager.get_state();
    let mut keys: Vec<&SecurityApiKeyRecord> = cs.api_keys.values().collect();
    keys.sort_by(|a, b| a.id.cmp(&b.id));
    let list: Vec<Value> = keys.into_iter().map(api_key_metadata).collect();
    (StatusCode::OK, Json(json!({ "api_keys": list })))
}

/// `GET /_security/api_key/{id}` — fetch one API-key's metadata (never the hash).
pub async fn get_api_key(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let cs = state.cluster_manager.get_state();
    match cs.api_keys.get(&id) {
        Some(record) => (StatusCode::OK, Json(api_key_metadata(record))),
        None => error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("api key [{id}] not found"),
        ),
    }
}

/// `DELETE /_security/api_key/{id}` — revoke a dynamic API key. Bootstrap keys
/// are config-only and cannot be deleted here.
pub async fn delete_api_key(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    // 404 when the id is not a dynamically-managed key.
    if !state.cluster_manager.get_state().api_keys.contains_key(&id) {
        return error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("api key [{id}] not found"),
        );
    }

    match resolve_leader_or_master(&state, "api key deletion") {
        Ok(Some(master)) => {
            if let Err(e) = state
                .transport_client
                .forward_delete_api_key(&master, &id)
                .await
            {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward api key deletion to master: {e}"),
                );
            }
        }
        Ok(None) => {
            let cmd = ClusterCommand::DeleteApiKey { key_id: id.clone() };
            if let Err(e) = raft_write(&state, cmd).await {
                return e;
            }
        }
        Err(e) => return e,
    }

    (StatusCode::OK, Json(json!({ "id": id, "deleted": true })))
}

// ─── Roles ───────────────────────────────────────────────────────────────────

fn role_to_json(role: &SecurityRoleDefinition) -> Value {
    json!({
        "name": role.name,
        "cluster": role.cluster,
        "indices": role.indices,
        "index_privileges": role.index_privileges,
    })
}

/// `PUT /_security/role/{name}` — create or replace a custom role.
pub async fn put_role(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    if name.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "role name is required",
        );
    }
    let parsed: PutRoleBody = match parse_body(&body) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let role = SecurityRoleDefinition {
        name: name.clone(),
        cluster: parsed.cluster,
        indices: parsed.indices,
        index_privileges: parsed.index_privileges,
    };

    match resolve_leader_or_master(&state, "role update") {
        Ok(Some(master)) => {
            if let Err(e) = state
                .transport_client
                .forward_put_role(&master, &role)
                .await
            {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward role update to master: {e}"),
                );
            }
        }
        Ok(None) => {
            let cmd = ClusterCommand::PutRole { role: role.clone() };
            if let Err(e) = raft_write(&state, cmd).await {
                return e;
            }
        }
        Err(e) => return e,
    }

    (
        StatusCode::OK,
        Json(json!({ "name": name, "acknowledged": true })),
    )
}

/// `GET /_security/role` — list all custom role definitions.
pub async fn list_roles(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let cs = state.cluster_manager.get_state();
    let mut roles: Vec<&SecurityRoleDefinition> = cs
        .roles
        .values()
        .filter(|r| {
            !r.name
                .starts_with(crate::security::INTERNAL_QUOTA_ROLE_PREFIX)
        })
        .collect();
    roles.sort_by(|a, b| a.name.cmp(&b.name));
    let list: Vec<Value> = roles.into_iter().map(role_to_json).collect();
    (StatusCode::OK, Json(json!({ "roles": list })))
}

/// `GET /_security/role/{name}` — fetch one custom role definition.
pub async fn get_role(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if name.starts_with(crate::security::INTERNAL_QUOTA_ROLE_PREFIX) {
        return error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("role [{name}] not found"),
        );
    }
    let cs = state.cluster_manager.get_state();
    match cs.roles.get(&name) {
        Some(role) => (StatusCode::OK, Json(role_to_json(role))),
        None => error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("role [{name}] not found"),
        ),
    }
}

/// `DELETE /_security/role/{name}` — remove a custom role definition.
pub async fn delete_role(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if name.starts_with(crate::security::INTERNAL_QUOTA_ROLE_PREFIX)
        || !state.cluster_manager.get_state().roles.contains_key(&name)
    {
        return error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("role [{name}] not found"),
        );
    }

    match resolve_leader_or_master(&state, "role deletion") {
        Ok(Some(master)) => {
            if let Err(e) = state
                .transport_client
                .forward_delete_role(&master, &name)
                .await
            {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward role deletion to master: {e}"),
                );
            }
        }
        Ok(None) => {
            let cmd = ClusterCommand::DeleteRole { name: name.clone() };
            if let Err(e) = raft_write(&state, cmd).await {
                return e;
            }
        }
        Err(e) => return e,
    }

    (
        StatusCode::OK,
        Json(json!({ "name": name, "deleted": true })),
    )
}

// ─── Tenant Quotas & Structured Redacted Audit Log (FS-049) ──────────────────

/// `GET /_security/audit` — inspect structured, redacted security and admission audit records.
pub async fn get_audit_log(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<AuditQueryParams>,
) -> (StatusCode, Json<Value>) {
    let outcome_filter = match params.outcome.as_deref() {
        Some(raw) => match crate::security::AuditOutcome::from_label(raw) {
            Some(o) => Some(o),
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    format!(
                        "invalid audit outcome filter [{raw}]; supported: [allowed, unauthenticated, forbidden, quota_exceeded]"
                    ),
                );
            }
        },
        None => None,
    };
    let limit = params.limit.unwrap_or(100);
    let (total_recorded, entries) = state.security_manager.audit_log_entries(
        params.principal_id.as_deref(),
        outcome_filter,
        limit,
    );
    (
        StatusCode::OK,
        Json(json!({
            "total_recorded": total_recorded,
            "retained": entries.len(),
            "entries": entries,
        })),
    )
}

/// `GET /_security/quota` — inspect active per-principal and per-index resource quotas.
pub async fn get_quotas(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK,
        Json(state.security_manager.quota_snapshot()),
    )
}

/// `PUT /_security/quota/{scope}/{id}` — configure a per-principal (`scope = "principal"`)
/// or per-index (`scope = "index"`) concurrency and inflight-byte quota (`FS-049`).
pub async fn put_quota(
    State(state): State<AppState>,
    Path((scope, id)): Path<(String, String)>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    if id.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "quota target id is required",
        );
    }
    let parsed: crate::security::TenantQuotaConfig = match parse_body(&body) {
        Ok(q) => q,
        Err(e) => return e,
    };
    if parsed.max_concurrent_requests == Some(0) || parsed.max_inflight_bytes == Some(0) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "quota limits must be greater than zero when specified",
        );
    }
    match scope.as_str() {
        "principal" => state.security_manager.set_principal_quota(&id, parsed),
        "index" => state.security_manager.set_index_quota(&id, parsed),
        other => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("unsupported quota scope [{other}]; expected 'principal' or 'index'"),
            );
        }
    }
    let role_record = crate::security::encode_quota_role_record(&scope, &id, parsed);
    match resolve_leader_or_master(&state, "quota update") {
        Ok(Some(master)) => {
            if let Err(e) = state
                .transport_client
                .forward_put_role(&master, &role_record)
                .await
            {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward quota update to master: {e}"),
                );
            }
        }
        Ok(None) => {
            let cmd = ClusterCommand::PutRole { role: role_record };
            if let Err(e) = raft_write(&state, cmd).await {
                return e;
            }
        }
        Err(e) => return e,
    }
    (
        StatusCode::OK,
        Json(json!({
            "scope": scope,
            "id": id,
            "max_concurrent_requests": parsed.max_concurrent_requests,
            "max_inflight_bytes": parsed.max_inflight_bytes,
            "acknowledged": true,
        })),
    )
}

/// `DELETE /_security/quota/{scope}/{id}` — remove a per-principal or per-index quota.
pub async fn delete_quota(
    State(state): State<AppState>,
    Path((scope, id)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    let role_key = format!(
        "{}{scope}:{id}",
        crate::security::INTERNAL_QUOTA_ROLE_PREFIX
    );
    let in_cluster = state
        .cluster_manager
        .get_state()
        .roles
        .contains_key(&role_key);
    let removed = match scope.as_str() {
        "principal" => state.security_manager.remove_principal_quota(&id) || in_cluster,
        "index" => state.security_manager.remove_index_quota(&id) || in_cluster,
        other => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("unsupported quota scope [{other}]; expected 'principal' or 'index'"),
            );
        }
    };
    if !removed {
        return error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("quota [{scope}/{id}] not found"),
        );
    }
    if in_cluster {
        match resolve_leader_or_master(&state, "quota deletion") {
            Ok(Some(master)) => {
                if let Err(e) = state
                    .transport_client
                    .forward_delete_role(&master, &role_key)
                    .await
                {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "forward_exception",
                        format!("Failed to forward quota deletion to master: {e}"),
                    );
                }
            }
            Ok(None) => {
                let cmd = ClusterCommand::DeleteRole { name: role_key };
                if let Err(e) = raft_write(&state, cmd).await {
                    return e;
                }
            }
            Err(e) => return e,
        }
    }
    (
        StatusCode::OK,
        Json(json!({ "scope": scope, "id": id, "deleted": true })),
    )
}
