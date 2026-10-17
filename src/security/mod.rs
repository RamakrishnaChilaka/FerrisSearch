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
pub const INTERNAL_QUOTA_ROLE_PREFIX: &str = "__quota:";

pub fn encode_quota_role_record(
    scope: &str,
    id: &str,
    quota: TenantQuotaConfig,
) -> SecurityRoleDefinition {
    let mut cluster = Vec::new();
    if let Some(c) = quota.max_concurrent_requests {
        cluster.push(format!("max_concurrent_requests={c}"));
    }
    if let Some(b) = quota.max_inflight_bytes {
        cluster.push(format!("max_inflight_bytes={b}"));
    }
    SecurityRoleDefinition {
        name: format!("{INTERNAL_QUOTA_ROLE_PREFIX}{scope}:{id}"),
        cluster,
        indices: Vec::new(),
        index_privileges: Vec::new(),
    }
}

pub fn decode_quota_role_record(
    role: &SecurityRoleDefinition,
) -> Option<(&str, &str, TenantQuotaConfig)> {
    let rest = role.name.strip_prefix(INTERNAL_QUOTA_ROLE_PREFIX)?;
    let (scope, id) = rest.split_once(':')?;
    let mut cfg = TenantQuotaConfig::default();
    for entry in &role.cluster {
        if let Some(v) = entry.strip_prefix("max_concurrent_requests=") {
            cfg.max_concurrent_requests = v.parse::<usize>().ok();
        } else if let Some(v) = entry.strip_prefix("max_inflight_bytes=") {
            cfg.max_inflight_bytes = v.parse::<u64>().ok();
        }
    }
    Some((scope, id, cfg))
}

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

impl SecurityAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClusterMonitor => "cluster:monitor",
            Self::ClusterStateRead => "cluster:state",
            Self::ClusterAdmin => "cluster:admin",
            Self::IndexRead => "indices:data/read",
            Self::IndexWrite => "indices:data/write",
            Self::IndexAdmin => "indices:admin",
            Self::SecurityAdmin => "cluster:admin/security",
            Self::MetricsRead => "cluster:monitor/metrics",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_requests: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_inflight_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Allowed,
    Unauthenticated,
    Forbidden,
    QuotaExceeded,
}

impl AuditOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::QuotaExceeded => "quota_exceeded",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label.trim() {
            "allowed" => Some(Self::Allowed),
            "unauthenticated" => Some(Self::Unauthenticated),
            "forbidden" => Some(Self::Forbidden),
            "quota_exceeded" => Some(Self::QuotaExceeded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub seq: u64,
    pub timestamp_epoch_ms: u64,
    pub request_id: String,
    pub principal_id: String,
    pub principal_name: String,
    pub action: String,
    pub method: String,
    pub path: String,
    pub target_indices: Vec<String>,
    pub outcome: AuditOutcome,
    pub status: u16,
    pub reason: String,
    pub redacted: bool,
}

#[derive(Debug, Default)]
struct TenantQuotaEntry {
    config: TenantQuotaConfig,
    active_requests: usize,
    inflight_bytes: u64,
    rejected_concurrency_total: u64,
    rejected_bytes_total: u64,
    admitted_total: u64,
}

#[derive(Debug, Default)]
struct TenantQuotaState {
    principal_quotas: std::collections::HashMap<String, TenantQuotaEntry>,
    index_quotas: std::collections::HashMap<String, TenantQuotaEntry>,
}

const DEFAULT_MAX_AUDIT_LOG_ENTRIES: usize = 4_096;

#[derive(Debug)]
struct AuditLogState {
    next_seq: u64,
    max_entries: usize,
    entries: std::collections::VecDeque<AuditLogEntry>,
}

impl Default for AuditLogState {
    fn default() -> Self {
        Self {
            next_seq: 1,
            max_entries: DEFAULT_MAX_AUDIT_LOG_ENTRIES,
            entries: std::collections::VecDeque::new(),
        }
    }
}

pub struct TenantQuotaBytesGuard {
    manager: Arc<SecurityManager>,
    principal_id: Option<String>,
    index_name: Option<String>,
    reserved_bytes: u64,
}

impl Drop for TenantQuotaBytesGuard {
    fn drop(&mut self) {
        let mut guard = self
            .manager
            .quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(ref pid) = self.principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
        {
            entry.inflight_bytes = entry.inflight_bytes.saturating_sub(self.reserved_bytes);
        }
        if let Some(ref idx) = self.index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
        {
            entry.inflight_bytes = entry.inflight_bytes.saturating_sub(self.reserved_bytes);
        }
    }
}

#[derive(Debug)]
pub struct TenantQuotaLease {
    manager: Arc<SecurityManager>,
    principal_id: Option<String>,
    index_name: Option<String>,
    reserved_bytes: u64,
}

impl Drop for TenantQuotaLease {
    fn drop(&mut self) {
        let mut guard = self
            .manager
            .quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(ref pid) = self.principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
        {
            entry.active_requests = entry.active_requests.saturating_sub(1);
            entry.inflight_bytes = entry.inflight_bytes.saturating_sub(self.reserved_bytes);
        }
        if let Some(ref idx) = self.index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
        {
            entry.active_requests = entry.active_requests.saturating_sub(1);
            entry.inflight_bytes = entry.inflight_bytes.saturating_sub(self.reserved_bytes);
        }
    }
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
    quotas: std::sync::Mutex<TenantQuotaState>,
    audit_log: std::sync::Mutex<AuditLogState>,
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
            quotas: std::sync::Mutex::new(TenantQuotaState::default()),
            audit_log: std::sync::Mutex::new(AuditLogState::default()),
        })
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            auto_create_security_index: false,
            api_keys: Vec::new(),
            dynamic_state: None,
            quotas: std::sync::Mutex::new(TenantQuotaState::default()),
            audit_log: std::sync::Mutex::new(AuditLogState::default()),
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

    pub fn find_principal_by_id(&self, key_id: &str) -> Option<Principal> {
        if let Some(record) = self.api_keys.iter().find(|r| r.id == key_id) {
            return Some(Principal {
                name: record.name.clone(),
                key_id: record.id.clone(),
                roles: record.roles.clone(),
                indices: record.indices.clone(),
            });
        }
        if let Some(state) = &self.dynamic_state {
            let guard = state.read().unwrap_or_else(|e| e.into_inner());
            if let Some(record) = guard.api_keys.get(key_id) {
                return Some(Principal {
                    name: record.name.clone(),
                    key_id: record.id.clone(),
                    roles: record.roles.clone(),
                    indices: record.indices.clone(),
                });
            }
        }
        None
    }

    pub fn authorize_current_request_index(
        &self,
        action: SecurityAction,
        index_name: &str,
    ) -> Result<(), SecurityError> {
        if !self.enabled() {
            return Ok(());
        }
        let Some((key_id, _)) = crate::worker::current_request_principal() else {
            return Ok(());
        };
        let principal = self.find_principal_by_id(&key_id);
        self.authorize_or_error(
            principal.as_ref(),
            &ClassifiedRequest {
                action,
                index: Some(index_name.to_string()),
            },
        )
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

    pub fn set_principal_quota(&self, principal_id: &str, quota: TenantQuotaConfig) {
        let mut guard = self.quotas.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard
            .principal_quotas
            .entry(principal_id.to_string())
            .or_default();
        entry.config = quota;
    }

    pub fn remove_principal_quota(&self, principal_id: &str) -> bool {
        self.quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .principal_quotas
            .remove(principal_id)
            .is_some()
    }

    pub fn set_index_quota(&self, index_name: &str, quota: TenantQuotaConfig) {
        let mut guard = self.quotas.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard
            .index_quotas
            .entry(index_name.to_string())
            .or_default();
        entry.config = quota;
    }

    pub fn remove_index_quota(&self, index_name: &str) -> bool {
        self.quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .index_quotas
            .remove(index_name)
            .is_some()
    }

    fn sync_dynamic_quotas_locked(&self, guard: &mut TenantQuotaState) {
        let Some(ref state) = self.dynamic_state else {
            return;
        };
        let cs = state.read().unwrap_or_else(|e| e.into_inner());
        for role in cs.roles.values() {
            if let Some((scope, id, cfg)) = decode_quota_role_record(role) {
                match scope {
                    "principal" => {
                        guard
                            .principal_quotas
                            .entry(id.to_string())
                            .or_default()
                            .config = cfg;
                    }
                    "index" => {
                        guard.index_quotas.entry(id.to_string()).or_default().config = cfg;
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn quota_snapshot(&self) -> serde_json::Value {
        let mut guard = self.quotas.lock().unwrap_or_else(|e| e.into_inner());
        self.sync_dynamic_quotas_locked(&mut guard);
        let mut principals = serde_json::Map::new();
        for (id, entry) in &guard.principal_quotas {
            principals.insert(
                id.clone(),
                serde_json::json!({
                    "max_concurrent_requests": entry.config.max_concurrent_requests,
                    "max_inflight_bytes": entry.config.max_inflight_bytes,
                    "active_requests": entry.active_requests,
                    "inflight_bytes": entry.inflight_bytes,
                    "admitted_total": entry.admitted_total,
                    "rejected_concurrency_total": entry.rejected_concurrency_total,
                    "rejected_bytes_total": entry.rejected_bytes_total,
                }),
            );
        }
        let mut indices = serde_json::Map::new();
        for (idx, entry) in &guard.index_quotas {
            indices.insert(
                idx.clone(),
                serde_json::json!({
                    "max_concurrent_requests": entry.config.max_concurrent_requests,
                    "max_inflight_bytes": entry.config.max_inflight_bytes,
                    "active_requests": entry.active_requests,
                    "inflight_bytes": entry.inflight_bytes,
                    "admitted_total": entry.admitted_total,
                    "rejected_concurrency_total": entry.rejected_concurrency_total,
                    "rejected_bytes_total": entry.rejected_bytes_total,
                }),
            );
        }
        serde_json::json!({
            "principals": principals,
            "indices": indices,
        })
    }

    pub fn acquire_quota_lease(
        self: &Arc<Self>,
        principal_id: Option<&str>,
        index_name: Option<&str>,
        bytes: u64,
    ) -> Result<TenantQuotaLease, SecurityError> {
        let mut guard = self.quotas.lock().unwrap_or_else(|e| e.into_inner());
        self.sync_dynamic_quotas_locked(&mut guard);
        if let Some(pid) = principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
        {
            if let Some(max_conc) = entry.config.max_concurrent_requests
                && entry.active_requests >= max_conc
            {
                entry.rejected_concurrency_total += 1;
                return Err(SecurityError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    error_type: "tenant_quota_exceeded_exception",
                    reason: "principal concurrent request quota exceeded",
                });
            }
            if let Some(max_bytes) = entry.config.max_inflight_bytes
                && entry.inflight_bytes.saturating_add(bytes) > max_bytes
            {
                entry.rejected_bytes_total += 1;
                return Err(SecurityError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    error_type: "tenant_quota_exceeded_exception",
                    reason: "principal inflight byte quota exceeded",
                });
            }
        }
        if let Some(idx) = index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
        {
            if let Some(max_conc) = entry.config.max_concurrent_requests
                && entry.active_requests >= max_conc
            {
                entry.rejected_concurrency_total += 1;
                return Err(SecurityError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    error_type: "tenant_quota_exceeded_exception",
                    reason: "index concurrent request quota exceeded",
                });
            }
            if let Some(max_bytes) = entry.config.max_inflight_bytes
                && entry.inflight_bytes.saturating_add(bytes) > max_bytes
            {
                entry.rejected_bytes_total += 1;
                return Err(SecurityError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    error_type: "tenant_quota_exceeded_exception",
                    reason: "index inflight byte quota exceeded",
                });
            }
        }

        let mut leased_principal = None;
        if let Some(pid) = principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
        {
            entry.active_requests += 1;
            entry.inflight_bytes = entry.inflight_bytes.saturating_add(bytes);
            entry.admitted_total += 1;
            leased_principal = Some(pid.to_string());
        }
        let mut leased_index = None;
        if let Some(idx) = index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
        {
            entry.active_requests += 1;
            entry.inflight_bytes = entry.inflight_bytes.saturating_add(bytes);
            entry.admitted_total += 1;
            leased_index = Some(idx.to_string());
        }
        drop(guard);

        Ok(TenantQuotaLease {
            manager: Arc::clone(self),
            principal_id: leased_principal,
            index_name: leased_index,
            reserved_bytes: bytes,
        })
    }

    pub fn charge_additional_quota_bytes(
        self: &Arc<Self>,
        principal_id: Option<&str>,
        index_name: Option<&str>,
        bytes: u64,
    ) -> Result<TenantQuotaBytesGuard, SecurityError> {
        if bytes == 0 {
            return Ok(TenantQuotaBytesGuard {
                manager: Arc::clone(self),
                principal_id: None,
                index_name: None,
                reserved_bytes: 0,
            });
        }
        let mut guard = self.quotas.lock().unwrap_or_else(|e| e.into_inner());
        self.sync_dynamic_quotas_locked(&mut guard);
        if let Some(pid) = principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
            && let Some(max_bytes) = entry.config.max_inflight_bytes
            && entry.inflight_bytes.saturating_add(bytes) > max_bytes
        {
            entry.rejected_bytes_total += 1;
            return Err(SecurityError {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "tenant_quota_exceeded_exception",
                reason: "principal inflight byte quota exceeded",
            });
        }
        if let Some(idx) = index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
            && let Some(max_bytes) = entry.config.max_inflight_bytes
            && entry.inflight_bytes.saturating_add(bytes) > max_bytes
        {
            entry.rejected_bytes_total += 1;
            return Err(SecurityError {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "tenant_quota_exceeded_exception",
                reason: "index inflight byte quota exceeded",
            });
        }
        let mut leased_principal = None;
        if let Some(pid) = principal_id
            && let Some(entry) = guard.principal_quotas.get_mut(pid)
        {
            entry.inflight_bytes = entry.inflight_bytes.saturating_add(bytes);
            leased_principal = Some(pid.to_string());
        }
        let mut leased_index = None;
        if let Some(idx) = index_name
            && let Some(entry) = guard.index_quotas.get_mut(idx)
        {
            entry.inflight_bytes = entry.inflight_bytes.saturating_add(bytes);
            leased_index = Some(idx.to_string());
        }
        drop(guard);
        Ok(TenantQuotaBytesGuard {
            manager: Arc::clone(self),
            principal_id: leased_principal,
            index_name: leased_index,
            reserved_bytes: bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_audit_event(
        &self,
        principal: Option<&Principal>,
        action: SecurityAction,
        method: &str,
        path: &str,
        target_indices: &[String],
        outcome: AuditOutcome,
        status: u16,
        reason: &str,
        had_auth_header: bool,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let req_id = crate::worker::current_request_context()
            .map(|ctx| ctx.trace_ids().request_id)
            .unwrap_or_else(|| "req-0".to_string());
        let (clean_path, path_redacted) = redact_sensitive_audit_text(path);
        let (clean_reason, reason_redacted) = redact_sensitive_audit_text(reason);
        let (principal_id, principal_name) = match principal {
            Some(p) => (p.key_id.clone(), p.name.clone()),
            None => crate::worker::current_request_principal()
                .unwrap_or_else(|| ("anonymous".to_string(), "anonymous".to_string())),
        };
        let mut audit = self.audit_log.lock().unwrap_or_else(|e| e.into_inner());
        let seq = audit.next_seq;
        audit.next_seq = audit.next_seq.saturating_add(1);
        let entry = AuditLogEntry {
            seq,
            timestamp_epoch_ms: now_ms,
            request_id: req_id,
            principal_id,
            principal_name,
            action: action.as_str().to_string(),
            method: method.to_string(),
            path: clean_path,
            target_indices: target_indices.to_vec(),
            outcome,
            status,
            reason: clean_reason,
            redacted: had_auth_header || path_redacted || reason_redacted,
        };
        audit.entries.push_back(entry);
        let cap = audit.max_entries.max(1);
        while audit.entries.len() > cap {
            audit.entries.pop_front();
        }
    }

    pub fn audit_log_entries(
        &self,
        principal_filter: Option<&str>,
        outcome_filter: Option<AuditOutcome>,
        limit: usize,
    ) -> (u64, Vec<AuditLogEntry>) {
        let audit = self.audit_log.lock().unwrap_or_else(|e| e.into_inner());
        let total = audit.next_seq.saturating_sub(1);
        let cap = limit.clamp(1, 1_000);
        let filtered: Vec<AuditLogEntry> = audit
            .entries
            .iter()
            .rev()
            .filter(|e| {
                principal_filter.is_none_or(|pf| e.principal_id == pf || e.principal_name == pf)
                    && outcome_filter.is_none_or(|of| e.outcome == of)
            })
            .take(cap)
            .cloned()
            .collect();
        (total, filtered)
    }
}

pub fn redact_sensitive_audit_text(input: &str) -> (String, bool) {
    let lower = input.to_ascii_lowercase();
    for marker in [
        "apikey ",
        "bearer ",
        "authorization:",
        "aws_secret_access_key",
        "aws_session_token",
        "private_key",
        "secret=",
        "api_key=",
        "password=",
        "sha256:",
    ] {
        if lower.contains(marker) {
            return ("[REDACTED]".to_string(), true);
        }
    }
    // Redact any raw 64-character hex token (API key secret or SHA-256 hash).
    for token in input.split(|c: char| !c.is_ascii_hexdigit()) {
        if token.len() == 64 {
            return (input.replace(token, "[REDACTED]"), true);
        }
    }
    (input.to_string(), false)
}

pub async fn auth_middleware(
    State(security): State<Arc<SecurityManager>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let classified = classify_request(&method, &path);
    if let Some(ref idx) = classified.index {
        crate::worker::add_current_request_index(idx);
    }
    let content_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    if !security.enabled() {
        let _quota_lease = match security.acquire_quota_lease(
            Some("anonymous"),
            classified.index.as_deref(),
            content_len,
        ) {
            Ok(lease) => lease,
            Err(err) => {
                let indices = crate::worker::current_request_indices();
                security.record_audit_event(
                    None,
                    classified.action,
                    method.as_str(),
                    &path,
                    &indices,
                    AuditOutcome::QuotaExceeded,
                    err.status.as_u16(),
                    err.reason,
                    false,
                );
                return security_error(err.status, err.error_type, err.reason);
            }
        };
        return next.run(req).await;
    }

    let had_auth_header = req.headers().contains_key(header::AUTHORIZATION);
    let Some(api_key) = extract_api_key(&req) else {
        let indices = crate::worker::current_request_indices();
        security.record_audit_event(
            None,
            classified.action,
            method.as_str(),
            &path,
            &indices,
            AuditOutcome::Unauthenticated,
            StatusCode::UNAUTHORIZED.as_u16(),
            "missing Authorization header",
            had_auth_header,
        );
        return security_error(
            StatusCode::UNAUTHORIZED,
            "security_exception",
            "missing Authorization header",
        );
    };

    let Some(principal) = security.authenticate_api_key(api_key) else {
        let indices = crate::worker::current_request_indices();
        security.record_audit_event(
            None,
            classified.action,
            method.as_str(),
            &path,
            &indices,
            AuditOutcome::Unauthenticated,
            StatusCode::UNAUTHORIZED.as_u16(),
            "invalid API key",
            true,
        );
        return security_error(
            StatusCode::UNAUTHORIZED,
            "security_exception",
            "invalid API key",
        );
    };

    crate::worker::set_current_request_principal(&principal.key_id, &principal.name);

    if !security.authorize(&principal, &classified) {
        let indices = crate::worker::current_request_indices();
        security.record_audit_event(
            Some(&principal),
            classified.action,
            method.as_str(),
            &path,
            &indices,
            AuditOutcome::Forbidden,
            StatusCode::FORBIDDEN.as_u16(),
            "insufficient permissions",
            true,
        );
        return security_error(
            StatusCode::FORBIDDEN,
            "security_exception",
            "insufficient permissions",
        );
    }

    let _quota_lease = match security.acquire_quota_lease(
        Some(&principal.key_id),
        classified.index.as_deref(),
        content_len,
    ) {
        Ok(lease) => lease,
        Err(err) => {
            let indices = crate::worker::current_request_indices();
            security.record_audit_event(
                Some(&principal),
                classified.action,
                method.as_str(),
                &path,
                &indices,
                AuditOutcome::QuotaExceeded,
                err.status.as_u16(),
                err.reason,
                true,
            );
            return security_error(err.status, err.error_type, err.reason);
        }
    };

    req.extensions_mut().insert(principal.clone());
    let response = next.run(req).await;
    let indices = crate::worker::current_request_indices();
    let effective_status = crate::worker::current_request_security_denial()
        .and_then(|s| StatusCode::from_u16(s).ok())
        .unwrap_or_else(|| response.status());
    let status = effective_status.as_u16();
    let (outcome, reason) = match effective_status {
        StatusCode::FORBIDDEN => (AuditOutcome::Forbidden, "insufficient permissions"),
        StatusCode::TOO_MANY_REQUESTS => (
            AuditOutcome::QuotaExceeded,
            "quota or admission limit exceeded",
        ),
        StatusCode::UNAUTHORIZED => (AuditOutcome::Unauthenticated, "unauthenticated"),
        _ => (AuditOutcome::Allowed, "authorized"),
    };
    security.record_audit_event(
        Some(&principal),
        classified.action,
        method.as_str(),
        &path,
        &indices,
        outcome,
        status,
        reason,
        true,
    );
    response
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
        ["_cluster", ..] | ["_cat", ..] | ["_tasks", ..] | ["_nodes", ..] => ClassifiedRequest {
            action: SecurityAction::ClusterMonitor,
            index: None,
        },
        ["_snapshot", ..] => ClassifiedRequest {
            action: SecurityAction::ClusterAdmin,
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
        "_update" | "_create" | "_bulk" => SecurityAction::IndexWrite,
        "_search" | "_count" | "_sql" => SecurityAction::IndexRead,
        "_settings" | "_mapping" | "_mappings" => match *method {
            Method::GET => SecurityAction::IndexRead,
            _ => SecurityAction::IndexAdmin,
        },
        "_refresh" | "_flush" | "_forcemerge" => SecurityAction::IndexAdmin,
        "_remote_store" => SecurityAction::IndexAdmin,
        "_lifecycle" => match *method {
            Method::GET => SecurityAction::IndexRead,
            _ => SecurityAction::IndexAdmin,
        },
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
    crate::api::error_response(status, error_type, reason).into_response()
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
    fn create_document_routes_require_index_write_not_admin() {
        for method in [Method::PUT, Method::POST] {
            let request = classify_request(&method, "/logs-2026/_create/doc");
            assert_eq!(request.action, SecurityAction::IndexWrite);
            assert_eq!(request.index.as_deref(), Some("logs-2026"));
        }
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
        assert!(metadata.shard_routing[&0].in_sync_replicas.is_empty());
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
        assert!(updated.shard_routing[&0].in_sync_replicas.is_empty());
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

    #[test]
    fn fs049_tenant_quotas_and_redacted_audit_log_enforce_isolation() {
        let manager = Arc::new(
            SecurityManager::new(SecurityConfig {
                enabled: true,
                auto_create_security_index: false,
                bootstrap_api_keys: vec![SecurityApiKeyConfig {
                    id: "tenant-a".into(),
                    name: "tenant-a-user".into(),
                    hash_sha256: sha256_hex("secret-a"),
                    roles: vec!["write".into()],
                    indices: vec!["tenant-a-*".into()],
                }],
            })
            .unwrap(),
        );
        manager.set_principal_quota(
            "tenant-a",
            TenantQuotaConfig {
                max_concurrent_requests: Some(1),
                max_inflight_bytes: Some(1024),
            },
        );
        manager.set_index_quota(
            "tenant-a-logs",
            TenantQuotaConfig {
                max_concurrent_requests: Some(2),
                max_inflight_bytes: Some(512),
            },
        );

        let lease1 = manager
            .acquire_quota_lease(Some("tenant-a"), Some("tenant-a-logs"), 256)
            .expect("first request within quota must succeed");

        // Second concurrent request for tenant-a must fail with 429 Too Many Requests.
        let err_conc = manager
            .acquire_quota_lease(Some("tenant-a"), Some("tenant-a-logs"), 64)
            .expect_err("second concurrent request must exceed principal concurrency quota");
        assert_eq!(err_conc.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(err_conc.error_type, "tenant_quota_exceeded_exception");

        drop(lease1);

        // Request exceeding index byte quota (600 > 512) must fail with 429.
        let err_bytes = manager
            .acquire_quota_lease(Some("tenant-a"), Some("tenant-a-logs"), 600)
            .expect_err("request exceeding index byte quota must be rejected");
        assert_eq!(err_bytes.status, StatusCode::TOO_MANY_REQUESTS);

        // Verify redaction of bearer/api_key and 64-char hex hashes in audit events.
        let raw_hash = "a".repeat(64);
        manager.record_audit_event(
            None,
            SecurityAction::IndexRead,
            "GET",
            "/tenant-a-logs/_search?api_key=supersecret",
            &["tenant-a-logs".to_string()],
            AuditOutcome::Unauthenticated,
            401,
            &format!("presented invalid hash {raw_hash}"),
            true,
        );
        let (total, entries) =
            manager.audit_log_entries(None, Some(AuditOutcome::Unauthenticated), 10);
        assert_eq!(total, 1);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].redacted);
        assert_eq!(entries[0].path, "[REDACTED]");
        assert!(!entries[0].reason.contains(&raw_hash));
        assert!(entries[0].reason.contains("[REDACTED]"));
    }
}
