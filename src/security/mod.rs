use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Method, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};

use crate::cluster::state::{ClusterState, SecurityRoleDefinition};

pub const SECURITY_INDEX_NAME: &str = ".ferris_security";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub auto_create_security_index: bool,
    #[serde(default)]
    pub bootstrap_api_keys: Vec<SecurityApiKeyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityApiKeyConfig {
    pub id: String,
    pub name: String,
    /// SHA-256 hash of the high-entropy API key, either raw hex or `sha256:<hex>`.
    pub hash_sha256: String,
    #[serde(default)]
    pub roles: Vec<String>,
    /// Optional index allow-list. Empty means all indices.
    #[serde(default)]
    pub indices: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub key_id: String,
    pub roles: Vec<String>,
    pub indices: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityAction {
    ClusterMonitor,
    ClusterStateRead,
    ClusterAdmin,
    IndexRead,
    IndexWrite,
    IndexAdmin,
    SecurityAdmin,
    MetricsRead,
}

#[derive(Debug, Clone)]
pub struct ClassifiedRequest {
    pub action: SecurityAction,
    pub index: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SecurityError {
    pub status: StatusCode,
    pub error_type: &'static str,
    pub reason: &'static str,
}

impl SecurityError {
    pub fn into_response_json(self) -> (StatusCode, Json<serde_json::Value>) {
        crate::api::error_response(self.status, self.error_type, self.reason)
    }
}

#[derive(Debug, Clone)]
struct ApiKeyRecord {
    id: String,
    name: String,
    hash_sha256: String,
    roles: Vec<String>,
    indices: Vec<String>,
}

#[derive(Debug)]
pub struct SecurityManager {
    enabled: bool,
    auto_create_security_index: bool,
    api_keys: Vec<ApiKeyRecord>,
    /// Shared, Raft-replicated cluster state for dynamic API keys/roles. `None`
    /// for the static-only constructors (`new`/`disabled`) used in unit tests.
    dynamic_state: Option<Arc<RwLock<ClusterState>>>,
}

impl SecurityManager {
    pub fn new(config: SecurityConfig) -> anyhow::Result<Self> {
        Self::build(config, None)
    }

    /// Like `new`, but also wires the shared cluster state so the manager can
    /// authenticate dynamically-managed API keys and consult custom roles.
    pub fn with_cluster_state(
        config: SecurityConfig,
        dynamic_state: Arc<RwLock<ClusterState>>,
    ) -> anyhow::Result<Self> {
        Self::build(config, Some(dynamic_state))
    }

    fn build(
        config: SecurityConfig,
        dynamic_state: Option<Arc<RwLock<ClusterState>>>,
    ) -> anyhow::Result<Self> {
        let mut api_keys = Vec::with_capacity(config.bootstrap_api_keys.len());
        for key in config.bootstrap_api_keys {
            let Some(hash_sha256) = normalize_sha256_hash(&key.hash_sha256) else {
                anyhow::bail!(
                    "security bootstrap api key [{}] must use a 64-character SHA-256 hex hash",
                    key.id
                );
            };
            api_keys.push(ApiKeyRecord {
                id: key.id,
                name: key.name,
                hash_sha256,
                roles: key.roles,
                indices: key.indices,
            });
        }

        if config.enabled && api_keys.is_empty() {
            tracing::warn!(
                "security is enabled but no bootstrap API keys are configured; \
                 only dynamically-created keys will be accepted"
            );
        }

        Ok(Self {
            enabled: config.enabled,
            auto_create_security_index: config.auto_create_security_index,
            api_keys,
            dynamic_state,
        })
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            auto_create_security_index: false,
            api_keys: Vec::new(),
            dynamic_state: None,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn should_auto_create_security_index(&self) -> bool {
        self.enabled && self.auto_create_security_index
    }

    fn authenticate_api_key(&self, api_key: &str) -> Option<Principal> {
        let presented = sha256_hex(api_key);
        // Static bootstrap keys first (preserves existing behavior).
        if let Some(principal) = self
            .api_keys
            .iter()
            .find(|record| constant_time_eq(&presented, &record.hash_sha256))
            .map(|record| Principal {
                name: record.name.clone(),
                key_id: record.id.clone(),
                roles: record.roles.clone(),
                indices: record.indices.clone(),
            })
        {
            return Some(principal);
        }

        // Dynamically-managed keys from the Raft-replicated cluster state.
        if let Some(state) = &self.dynamic_state {
            let guard = state.read().unwrap_or_else(|e| e.into_inner());
            for record in guard.api_keys.values() {
                if constant_time_eq(&presented, &record.hash_sha256) {
                    return Some(Principal {
                        name: record.name.clone(),
                        key_id: record.id.clone(),
                        roles: record.roles.clone(),
                        indices: record.indices.clone(),
                    });
                }
            }
        }

        None
    }

    pub fn authorize(&self, principal: &Principal, request: &ClassifiedRequest) -> bool {
        if let Some(index) = &request.index
            && !principal.indices.is_empty()
            && !principal
                .indices
                .iter()
                .any(|pattern| index_pattern_matches(pattern, index))
        {
            return false;
        }

        // Built-in role names take precedence (cheap, no lock).
        if principal
            .roles
            .iter()
            .any(|role| role_allows_action(role, request.action))
        {
            return true;
        }

        // Fall back to dynamically-defined custom roles.
        if let Some(state) = &self.dynamic_state {
            let guard = state.read().unwrap_or_else(|e| e.into_inner());
            for role_name in &principal.roles {
                if let Some(def) = guard.roles.get(role_name)
                    && dynamic_role_allows(def, request)
                {
                    return true;
                }
            }
        }

        false
    }

    pub fn authorize_or_error(
        &self,
        principal: Option<&Principal>,
        request: &ClassifiedRequest,
    ) -> Result<(), SecurityError> {
        if !self.enabled() {
            return Ok(());
        }

        let Some(principal) = principal else {
            return Err(SecurityError {
                status: StatusCode::UNAUTHORIZED,
                error_type: "security_exception",
                reason: "missing authenticated principal",
            });
        };

        if self.authorize(principal, request) {
            Ok(())
        } else {
            Err(SecurityError {
                status: StatusCode::FORBIDDEN,
                error_type: "security_exception",
                reason: "insufficient permissions",
            })
        }
    }
}

pub async fn auth_middleware(
    State(security): State<Arc<SecurityManager>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    if !security.enabled() {
        return next.run(req).await;
    }

    let classified = classify_request(req.method(), req.uri().path());
    let Some(api_key) = extract_api_key(&req) else {
        return security_error(
            StatusCode::UNAUTHORIZED,
            "security_exception",
            "missing Authorization header",
        );
    };

    let Some(principal) = security.authenticate_api_key(api_key) else {
        return security_error(
            StatusCode::UNAUTHORIZED,
            "security_exception",
            "invalid API key",
        );
    };

    if !security.authorize(&principal, &classified) {
        return security_error(
            StatusCode::FORBIDDEN,
            "security_exception",
            "insufficient permissions",
        );
    }

    req.extensions_mut().insert(principal);
    next.run(req).await
}

pub fn is_protected_system_index(index: &str) -> bool {
    index == SECURITY_INDEX_NAME
}

pub fn protected_system_index_error(index: &str) -> (StatusCode, Json<serde_json::Value>) {
    crate::api::error_response(
        StatusCode::FORBIDDEN,
        "security_exception",
        format!(
            "index [{index}] is a protected system index and cannot be accessed through ordinary index APIs"
        ),
    )
}

pub fn security_index_metadata(
    data_nodes: &[String],
) -> Result<crate::cluster::state::IndexMetadata, crate::cluster::state::CreateIndexMetadataError> {
    if data_nodes.is_empty() {
        return Err(crate::cluster::state::CreateIndexMetadataError::NoDataNodes);
    }

    let mut metadata = crate::cluster::state::IndexMetadata::build_shard_routing(
        SECURITY_INDEX_NAME,
        1,
        desired_security_index_replicas(data_nodes),
        data_nodes,
    );
    metadata.dynamic = crate::cluster::state::DynamicMapping::Strict;
    metadata.mappings.insert(
        "doc_type".to_string(),
        crate::cluster::state::FieldMapping {
            field_type: crate::cluster::state::FieldType::Keyword,
            dimension: None,
        },
    );
    metadata.mappings.insert(
        "payload".to_string(),
        crate::cluster::state::FieldMapping {
            field_type: crate::cluster::state::FieldType::Text,
            dimension: None,
        },
    );
    Ok(metadata)
}

pub fn desired_security_index_replicas(data_nodes: &[String]) -> u32 {
    data_nodes.len().saturating_sub(1).min(u32::MAX as usize) as u32
}

pub fn adapt_security_index_replicas(
    metadata: &crate::cluster::state::IndexMetadata,
    data_nodes: &[String],
) -> Option<crate::cluster::state::IndexMetadata> {
    if metadata.name != SECURITY_INDEX_NAME {
        return None;
    }

    let desired_replicas = desired_security_index_replicas(data_nodes);
    let mut updated = metadata.clone();
    let mut changed = false;

    if updated.number_of_replicas != desired_replicas {
        updated.update_number_of_replicas(desired_replicas);
        changed = true;
    }

    if updated.allocate_unassigned_replicas(data_nodes) {
        changed = true;
    }

    changed.then_some(updated)
}

pub fn classify_request(method: &Method, path: &str) -> ClassifiedRequest {
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    if segments
        .iter()
        .any(|segment| is_protected_system_index(segment))
    {
        return ClassifiedRequest {
            action: SecurityAction::SecurityAdmin,
            index: Some(SECURITY_INDEX_NAME.to_string()),
        };
    }

    match segments.as_slice() {
        [] => ClassifiedRequest {
            action: SecurityAction::ClusterMonitor,
            index: None,
        },
        ["_metrics"] => ClassifiedRequest {
            action: SecurityAction::MetricsRead,
            index: None,
        },
        ["_security", ..] | ["_plugins", "_security", ..] => ClassifiedRequest {
            action: SecurityAction::SecurityAdmin,
            index: None,
        },
        ["_cluster", "transfer_master"] => ClassifiedRequest {
            action: SecurityAction::ClusterAdmin,
            index: None,
        },
        ["_cluster", "state"] => ClassifiedRequest {
            action: SecurityAction::ClusterStateRead,
            index: None,
        },
        ["_cluster", ..] | ["_cat", ..] | ["_tasks", ..] => ClassifiedRequest {
            action: SecurityAction::ClusterMonitor,
            index: None,
        },
        ["_bulk"] => ClassifiedRequest {
            action: SecurityAction::IndexWrite,
            index: None,
        },
        ["_sql"] | ["_sql", "stream"] => ClassifiedRequest {
            action: SecurityAction::IndexRead,
            index: None,
        },
        [index] => ClassifiedRequest {
            action: match *method {
                Method::GET | Method::HEAD => SecurityAction::IndexRead,
                Method::PUT | Method::DELETE => SecurityAction::IndexAdmin,
                _ => SecurityAction::IndexAdmin,
            },
            index: Some((*index).to_string()),
        },
        [index, action, ..] => ClassifiedRequest {
            action: classify_index_action(method, action),
            index: Some((*index).to_string()),
        },
    }
}

fn classify_index_action(method: &Method, action: &str) -> SecurityAction {
    match action {
        "_doc" => match *method {
            Method::GET => SecurityAction::IndexRead,
            Method::POST | Method::PUT | Method::DELETE => SecurityAction::IndexWrite,
            _ => SecurityAction::IndexWrite,
        },
        "_update" | "_bulk" => SecurityAction::IndexWrite,
        "_search" | "_count" | "_sql" => SecurityAction::IndexRead,
        "_settings" => match *method {
            Method::GET => SecurityAction::IndexRead,
            _ => SecurityAction::IndexAdmin,
        },
        "_refresh" | "_flush" | "_forcemerge" => SecurityAction::IndexAdmin,
        "_remote_store" => SecurityAction::IndexAdmin,
        _ => SecurityAction::IndexAdmin,
    }
}

fn role_allows_action(role: &str, action: SecurityAction) -> bool {
    match role {
        "admin" | "all_access" => true,
        "security_admin" => matches!(action, SecurityAction::SecurityAdmin),
        "manage" => matches!(
            action,
            SecurityAction::ClusterMonitor
                | SecurityAction::ClusterStateRead
                | SecurityAction::ClusterAdmin
                | SecurityAction::IndexRead
                | SecurityAction::IndexWrite
                | SecurityAction::IndexAdmin
                | SecurityAction::MetricsRead
        ),
        "write" => matches!(
            action,
            SecurityAction::ClusterMonitor | SecurityAction::IndexRead | SecurityAction::IndexWrite
        ),
        "read" => matches!(
            action,
            SecurityAction::ClusterMonitor | SecurityAction::IndexRead
        ),
        "metrics" => matches!(action, SecurityAction::MetricsRead),
        _ => false,
    }
}

/// Whether a dynamically-defined custom role grants the requested action.
/// Index actions are additionally gated by the role's own index patterns.
fn dynamic_role_allows(role: &SecurityRoleDefinition, request: &ClassifiedRequest) -> bool {
    match request.action {
        SecurityAction::IndexRead | SecurityAction::IndexWrite | SecurityAction::IndexAdmin => {
            if let Some(index) = &request.index
                && !role.indices.is_empty()
                && !role
                    .indices
                    .iter()
                    .any(|pattern| index_pattern_matches(pattern, index))
            {
                return false;
            }
            role.index_privileges
                .iter()
                .any(|privilege| index_privilege_grants(privilege, request.action))
        }
        cluster_action => role
            .cluster
            .iter()
            .any(|privilege| cluster_privilege_grants(privilege, cluster_action)),
    }
}

/// Maps an index-privilege string to the index actions it grants. Higher
/// privileges imply the lower ones (write ⊇ read, admin ⊇ write ⊇ read).
fn index_privilege_grants(privilege: &str, action: SecurityAction) -> bool {
    match privilege {
        "read" => matches!(action, SecurityAction::IndexRead),
        "write" => matches!(
            action,
            SecurityAction::IndexRead | SecurityAction::IndexWrite
        ),
        "admin" | "all" => matches!(
            action,
            SecurityAction::IndexRead | SecurityAction::IndexWrite | SecurityAction::IndexAdmin
        ),
        _ => false,
    }
}

/// Maps a cluster-privilege string to the cluster/metrics/security actions it grants.
fn cluster_privilege_grants(privilege: &str, action: SecurityAction) -> bool {
    match privilege {
        "all" => matches!(
            action,
            SecurityAction::ClusterMonitor
                | SecurityAction::ClusterStateRead
                | SecurityAction::ClusterAdmin
                | SecurityAction::MetricsRead
                | SecurityAction::SecurityAdmin
        ),
        "monitor" => matches!(action, SecurityAction::ClusterMonitor),
        "state" => matches!(action, SecurityAction::ClusterStateRead),
        "admin" => matches!(action, SecurityAction::ClusterAdmin),
        "metrics" => matches!(action, SecurityAction::MetricsRead),
        "security" => matches!(action, SecurityAction::SecurityAdmin),
        _ => false,
    }
}

fn extract_api_key(req: &Request<Body>) -> Option<&str> {
    let header_value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    header_value
        .strip_prefix("ApiKey ")
        .or_else(|| header_value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn security_error(status: StatusCode, error_type: &str, reason: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": { "type": error_type, "reason": reason },
            "status": status.as_u16()
        })),
    )
        .into_response()
}

fn normalize_sha256_hash(hash: &str) -> Option<String> {
    let normalized = hash
        .strip_prefix("sha256:")
        .unwrap_or(hash)
        .to_ascii_lowercase();
    if normalized.len() == 64 && normalized.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(normalized)
    } else {
        None
    }
}

pub fn sha256_hex(value: &str) -> String {
    hex_encode(&Sha256::digest(value.as_bytes()))
}

/// Lowercase-hex encode arbitrary bytes (used for SHA-256 hashes and for
/// rendering CSPRNG-generated secrets).
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!(),
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }

    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn index_pattern_matches(pattern: &str, index: &str) -> bool {
    if pattern == "*" || pattern == index {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return index.starts_with(prefix);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_hash_accepts_prefixed_sha256() {
        let hash = sha256_hex("secret");
        assert_eq!(normalize_sha256_hash(&format!("sha256:{hash}")), Some(hash));
    }

    #[test]
    fn authenticate_api_key_uses_hash() {
        let manager = SecurityManager::new(SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![SecurityApiKeyConfig {
                id: "admin-key".into(),
                name: "admin".into(),
                hash_sha256: sha256_hex("secret"),
                roles: vec!["admin".into()],
                indices: vec![],
            }],
        })
        .unwrap();

        assert!(manager.authenticate_api_key("secret").is_some());
        assert!(manager.authenticate_api_key("wrong").is_none());
    }

    #[test]
    fn read_role_cannot_write() {
        let manager = SecurityManager::new(SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![SecurityApiKeyConfig {
                id: "reader-key".into(),
                name: "reader".into(),
                hash_sha256: sha256_hex("secret"),
                roles: vec!["read".into()],
                indices: vec!["logs-*".into()],
            }],
        })
        .unwrap();
        let principal = manager.authenticate_api_key("secret").unwrap();

        assert!(manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexRead,
                index: Some("logs-2026".into()),
            }
        ));
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexWrite,
                index: Some("logs-2026".into()),
            }
        ));
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexRead,
                index: Some("metrics".into()),
            }
        ));
    }

    #[test]
    fn protected_system_index_requires_security_admin() {
        let request = classify_request(&Method::GET, "/.ferris_security/_search");
        assert_eq!(request.action, SecurityAction::SecurityAdmin);
        assert_eq!(request.index.as_deref(), Some(SECURITY_INDEX_NAME));
    }

    #[test]
    fn cluster_state_requires_stronger_permission_than_monitor() {
        let manager = SecurityManager::new(SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![SecurityApiKeyConfig {
                id: "reader-key".into(),
                name: "reader".into(),
                hash_sha256: sha256_hex("secret"),
                roles: vec!["read".into()],
                indices: vec![],
            }],
        })
        .unwrap();
        let principal = manager.authenticate_api_key("secret").unwrap();

        let health = classify_request(&Method::GET, "/_cluster/health");
        let state = classify_request(&Method::GET, "/_cluster/state");

        assert_eq!(health.action, SecurityAction::ClusterMonitor);
        assert_eq!(state.action, SecurityAction::ClusterStateRead);
        assert!(manager.authorize(&principal, &health));
        assert!(!manager.authorize(&principal, &state));
    }

    #[test]
    fn security_index_metadata_is_strict_and_routed() {
        let metadata = security_index_metadata(&[
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ])
        .unwrap();

        assert_eq!(metadata.name, SECURITY_INDEX_NAME);
        assert_eq!(metadata.number_of_shards, 1);
        assert_eq!(metadata.number_of_replicas, 2);
        assert_eq!(
            metadata.dynamic,
            crate::cluster::state::DynamicMapping::Strict
        );
        assert_eq!(metadata.shard_routing[&0].primary, "node-1");
        assert_eq!(metadata.shard_routing[&0].replicas, ["node-2", "node-3"]);
        assert_eq!(
            metadata.shard_routing[&0].in_sync_replicas,
            ["node-2", "node-3"]
        );
        assert!(metadata.mappings.contains_key("doc_type"));
        assert!(metadata.mappings.contains_key("payload"));
    }

    #[test]
    fn security_index_metadata_uses_zero_replicas_on_single_node() {
        let metadata = security_index_metadata(&["node-1".to_string()]).unwrap();

        assert_eq!(metadata.number_of_replicas, 0);
        assert!(metadata.shard_routing[&0].replicas.is_empty());
        assert_eq!(metadata.shard_routing[&0].unassigned_replicas, 0);
    }

    #[test]
    fn adapt_security_index_replicas_adds_new_data_nodes() {
        let metadata = security_index_metadata(&["node-1".to_string()]).unwrap();
        let updated = adapt_security_index_replicas(
            &metadata,
            &[
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(updated.number_of_replicas, 2);
        assert_eq!(updated.shard_routing[&0].replicas, ["node-2", "node-3"]);
        assert!(
            updated.shard_routing[&0].in_sync_replicas.is_empty(),
            "security replicas added after creation require file recovery before admission"
        );
        assert_eq!(updated.shard_routing[&0].unassigned_replicas, 0);
    }

    #[test]
    fn adapt_security_index_replicas_trims_after_data_node_loss() {
        let metadata = security_index_metadata(&[
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ])
        .unwrap();
        let updated =
            adapt_security_index_replicas(&metadata, &["node-1".to_string(), "node-2".to_string()])
                .unwrap();

        assert_eq!(updated.number_of_replicas, 1);
        assert_eq!(updated.shard_routing[&0].replicas, ["node-2"]);
        assert_eq!(updated.shard_routing[&0].in_sync_replicas, ["node-2"]);
        assert_eq!(updated.shard_routing[&0].unassigned_replicas, 0);
    }

    // ─── Dynamic control plane (with_cluster_state) ──────────────────────────

    use crate::cluster::state::SecurityApiKeyRecord;

    fn shared_state() -> Arc<RwLock<ClusterState>> {
        Arc::new(RwLock::new(ClusterState::new("sec-test".into())))
    }

    fn enabled_config_with_bootstrap() -> SecurityConfig {
        SecurityConfig {
            enabled: true,
            auto_create_security_index: false,
            bootstrap_api_keys: vec![SecurityApiKeyConfig {
                id: "boot".into(),
                name: "bootstrap-admin".into(),
                hash_sha256: sha256_hex("boot-secret"),
                roles: vec!["admin".into()],
                indices: vec![],
            }],
        }
    }

    fn dyn_key(
        id: &str,
        secret: &str,
        roles: Vec<String>,
        indices: Vec<String>,
    ) -> SecurityApiKeyRecord {
        SecurityApiKeyRecord {
            id: id.into(),
            name: format!("{id}-name"),
            hash_sha256: sha256_hex(secret),
            roles,
            indices,
            created_at_millis: 1,
        }
    }

    #[test]
    fn dynamic_key_authenticates_and_static_key_still_works() {
        let state = shared_state();
        state.write().unwrap().api_keys.insert(
            "dyn-1".into(),
            dyn_key("dyn-1", "dyn-secret", vec!["read".into()], vec![]),
        );
        let manager =
            SecurityManager::with_cluster_state(enabled_config_with_bootstrap(), state).unwrap();

        // Static bootstrap key still authenticates.
        let boot = manager.authenticate_api_key("boot-secret").unwrap();
        assert_eq!(boot.key_id, "boot");
        // Dynamic key authenticates too.
        let dynamic = manager.authenticate_api_key("dyn-secret").unwrap();
        assert_eq!(dynamic.key_id, "dyn-1");
        assert_eq!(dynamic.roles, vec!["read".to_string()]);
        // Unknown secret is rejected.
        assert!(manager.authenticate_api_key("nope").is_none());
    }

    #[test]
    fn revoking_dynamic_key_removes_authentication() {
        let state = shared_state();
        state.write().unwrap().api_keys.insert(
            "dyn-1".into(),
            dyn_key("dyn-1", "dyn-secret", vec!["read".into()], vec![]),
        );
        let manager =
            SecurityManager::with_cluster_state(enabled_config_with_bootstrap(), state.clone())
                .unwrap();
        assert!(manager.authenticate_api_key("dyn-secret").is_some());

        // Simulate a DeleteApiKey applied via Raft.
        state.write().unwrap().api_keys.remove("dyn-1");
        assert!(manager.authenticate_api_key("dyn-secret").is_none());
    }

    #[test]
    fn custom_role_grants_index_read_within_pattern() {
        let state = shared_state();
        state.write().unwrap().api_keys.insert(
            "dyn-1".into(),
            dyn_key("dyn-1", "dyn-secret", vec!["log-reader".into()], vec![]),
        );
        state.write().unwrap().roles.insert(
            "log-reader".into(),
            SecurityRoleDefinition {
                name: "log-reader".into(),
                cluster: vec!["monitor".into()],
                indices: vec!["logs-*".into()],
                index_privileges: vec!["read".into()],
            },
        );
        let manager =
            SecurityManager::with_cluster_state(enabled_config_with_bootstrap(), state).unwrap();
        let principal = manager.authenticate_api_key("dyn-secret").unwrap();

        // Reading an allowed index works.
        assert!(manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexRead,
                index: Some("logs-2024".into()),
            }
        ));
        // Writing is not granted by a read role.
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexWrite,
                index: Some("logs-2024".into()),
            }
        ));
        // Reading an index outside the role's pattern is denied.
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexRead,
                index: Some("metrics-2024".into()),
            }
        ));
        // Cluster monitor privilege is granted.
        assert!(manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::ClusterMonitor,
                index: None,
            }
        ));
    }

    #[test]
    fn custom_role_admin_privilege_implies_lower_index_actions() {
        let state = shared_state();
        state.write().unwrap().api_keys.insert(
            "dyn-1".into(),
            dyn_key("dyn-1", "dyn-secret", vec!["idx-admin".into()], vec![]),
        );
        state.write().unwrap().roles.insert(
            "idx-admin".into(),
            SecurityRoleDefinition {
                name: "idx-admin".into(),
                cluster: vec![],
                indices: vec![], // empty = all indices
                index_privileges: vec!["admin".into()],
            },
        );
        let manager =
            SecurityManager::with_cluster_state(enabled_config_with_bootstrap(), state).unwrap();
        let principal = manager.authenticate_api_key("dyn-secret").unwrap();

        for action in [
            SecurityAction::IndexRead,
            SecurityAction::IndexWrite,
            SecurityAction::IndexAdmin,
        ] {
            assert!(manager.authorize(
                &principal,
                &ClassifiedRequest {
                    action,
                    index: Some("anything".into()),
                }
            ));
        }
        // No cluster privilege was granted.
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::SecurityAdmin,
                index: None,
            }
        ));
    }

    #[test]
    fn unknown_custom_role_denies() {
        let state = shared_state();
        state.write().unwrap().api_keys.insert(
            "dyn-1".into(),
            dyn_key("dyn-1", "dyn-secret", vec!["ghost-role".into()], vec![]),
        );
        let manager =
            SecurityManager::with_cluster_state(enabled_config_with_bootstrap(), state).unwrap();
        let principal = manager.authenticate_api_key("dyn-secret").unwrap();
        assert!(!manager.authorize(
            &principal,
            &ClassifiedRequest {
                action: SecurityAction::IndexRead,
                index: Some("logs".into()),
            }
        ));
    }

    #[test]
    fn hex_encode_roundtrips_known_bytes() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
        assert_eq!(hex_encode(&[]), "");
    }
}
