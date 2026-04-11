// Adds required field to AppConfig
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

/// Deserializes `seed_hosts` from either a YAML array `["a", "b"]` or a
/// comma-separated env var string `"a,b,c"`. This avoids the `config` crate's
/// `try_parsing(true)` which breaks all other string fields.
fn deserialize_seed_hosts<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct SeedHostsVisitor;

    impl<'de> de::Visitor<'de> for SeedHostsVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a list of strings or a comma-separated string")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Vec<String>, E> {
            Ok(value.split(',').map(|s| s.trim().to_string()).collect())
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<Vec<String>, E> {
            Ok(value.split(',').map(|s| s.trim().to_string()).collect())
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<String>, A::Error> {
            let mut hosts = Vec::new();
            while let Some(val) = seq.next_element::<String>()? {
                hosts.push(val);
            }
            Ok(hosts)
        }
    }

    deserializer.deserialize_any(SeedHostsVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub node_name: String,
    pub cluster_name: String,
    pub http_port: u16,
    pub transport_port: u16,
    pub data_dir: String,
    #[serde(deserialize_with = "deserialize_seed_hosts")]
    pub seed_hosts: Vec<String>,
    /// Unique numeric node ID for Raft consensus (must be unique across cluster).
    #[serde(default = "default_raft_node_id")]
    pub raft_node_id: u64,
    /// Translog durability: "request" (fsync per write, default) or "async" (timer-based fsync).
    #[serde(default = "default_translog_durability")]
    pub translog_durability: String,
    /// Interval in ms for background translog fsync. Only used when durability is "async".
    /// Defaults to 5000ms if unset.
    #[serde(default)]
    pub translog_sync_interval_ms: Option<u64>,
    /// Percentage of system memory to use for the column cache (0-100).
    /// Caches both Arrow arrays for SQL fast-field reads and grouped-partials
    /// decoded full-segment columns.
    /// Default: 10. Set to 0 to disable.
    #[serde(default = "default_column_cache_size_percent")]
    pub column_cache_size_percent: u8,
    /// Selectivity threshold (0-100) for eager column cache population.
    /// On a cache miss, the full-segment array is only built when matched docs
    /// exceed this percentage of the segment's total docs. Below this threshold,
    /// only matching docs are read directly (no cache population).
    /// Default: 5. Set to 0 to always populate. Set to 100 to never eagerly populate.
    #[serde(default = "default_column_cache_populate_threshold")]
    pub column_cache_populate_threshold: u8,
    /// Maximum docs to scan for GROUP BY queries that fall back to the
    /// tantivy_fast_fields path (expression GROUP BY, STDDEV_POP, etc.).
    /// Default: 1,000,000. Set to 0 for unlimited.
    #[serde(default = "default_sql_group_by_scan_limit")]
    pub sql_group_by_scan_limit: usize,
    /// Enable approximate shard-level top-K pruning for grouped-partials
    /// GROUP BY queries with ORDER BY + LIMIT.  When true, each shard keeps
    /// only `(offset + limit) * 3 + 10` buckets sorted by the ORDER BY metric
    /// before shipping to the coordinator.  This is NOT standard SQL semantics
    /// (a bucket below every shard's cutoff can still be in the exact global
    /// top-K), but reduces latency dramatically on high-cardinality GROUP BY.
    /// Default: true. Set to false to force exact coordinator-side merge.
    #[serde(default = "default_sql_approximate_top_k")]
    pub sql_approximate_top_k: bool,
    /// Enable TLS for gRPC inter-node transport. Requires `transport-tls` feature.
    /// Default: false.
    #[serde(default)]
    pub transport_tls_enabled: bool,
    /// Path to PEM-encoded TLS certificate for the gRPC server.
    /// Required when `transport_tls_enabled` is true.
    #[serde(default)]
    pub transport_tls_cert_file: Option<String>,
    /// Path to PEM-encoded TLS private key for the gRPC server.
    /// Required when `transport_tls_enabled` is true.
    #[serde(default)]
    pub transport_tls_key_file: Option<String>,
    /// Path to PEM-encoded CA certificate for verifying peer nodes.
    /// When set, the client verifies the server's certificate chain.
    /// Required when `transport_tls_enabled` is true.
    #[serde(default)]
    pub transport_tls_ca_file: Option<String>,
}

fn default_raft_node_id() -> u64 {
    1
}

fn default_translog_durability() -> String {
    "request".to_string()
}

fn default_column_cache_size_percent() -> u8 {
    10
}

fn default_column_cache_populate_threshold() -> u8 {
    5
}

fn default_sql_group_by_scan_limit() -> usize {
    1_000_000
}

fn default_sql_approximate_top_k() -> bool {
    true
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            node_name: generate_node_name(),
            cluster_name: "ferrissearch".into(),
            http_port: 9200,
            transport_port: 9300,
            data_dir: "./data".into(),
            seed_hosts: vec!["127.0.0.1:9300".into()],
            raft_node_id: 1,
            translog_durability: "request".to_string(),
            translog_sync_interval_ms: None,
            column_cache_size_percent: 10,
            column_cache_populate_threshold: 5,
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: true,
            transport_tls_enabled: false,
            transport_tls_cert_file: None,
            transport_tls_key_file: None,
            transport_tls_ca_file: None,
        }
    }
}

impl AppConfig {
    /// Load configuration layered from defaults, file, and environment
    pub fn load() -> Result<Self, ConfigError> {
        let default = AppConfig::default();

        let builder = Config::builder()
            .set_default("node_name", default.node_name)?
            .set_default("cluster_name", default.cluster_name)?
            .set_default("http_port", default.http_port)?
            .set_default("transport_port", default.transport_port)?
            .set_default("data_dir", default.data_dir)?
            .set_default("seed_hosts", default.seed_hosts)?
            .set_default("raft_node_id", default.raft_node_id)?
            .set_default("translog_durability", default.translog_durability)?
            .add_source(File::with_name("config/ferrissearch").required(false))
            .add_source(Environment::with_prefix("FERRISSEARCH"));

        let config = builder.build()?;
        let app_config: AppConfig = config.try_deserialize()?;

        Ok(app_config)
    }
}

/// Generate a random node name in the format "adjective-noun" (e.g. "rusty-falcon").
/// Similar to Elasticsearch's random node naming convention.
fn generate_node_name() -> String {
    use std::time::SystemTime;

    const ADJECTIVES: &[&str] = &[
        "amber", "bold", "bright", "calm", "clever", "crimson", "daring", "eager", "fast",
        "fierce", "gentle", "golden", "happy", "iron", "keen", "lively", "mighty", "noble",
        "proud", "quick", "rapid", "rusty", "sharp", "silent", "sleek", "smooth", "solid",
        "steady", "strong", "swift", "tidal", "valiant", "vivid", "warm", "wild", "wise", "young",
        "zealous",
    ];
    const NOUNS: &[&str] = &[
        "badger", "bear", "condor", "crane", "eagle", "falcon", "ferret", "fox", "hawk", "heron",
        "horse", "jaguar", "kestrel", "lemur", "lion", "lynx", "moose", "orca", "osprey", "otter",
        "owl", "panda", "panther", "parrot", "puma", "raven", "salmon", "shark", "sparrow",
        "stork", "tiger", "viper", "whale", "wolf", "wren",
    ];

    // Simple seed from system time nanos — no external RNG dependency needed.
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as usize;
    let adj = ADJECTIVES[seed % ADJECTIVES.len()];
    let noun = NOUNS[(seed / ADJECTIVES.len()) % NOUNS.len()];
    format!("{adj}-{noun}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_translog_durability_is_request() {
        let config = AppConfig::default();
        assert_eq!(config.translog_durability, "request");
    }

    #[test]
    fn default_translog_sync_interval_is_none() {
        let config = AppConfig::default();
        assert!(config.translog_sync_interval_ms.is_none());
    }

    #[test]
    fn deserialize_async_durability_with_interval() {
        let json = r#"{
            "node_name": "n1",
            "cluster_name": "test",
            "http_port": 9200,
            "transport_port": 9300,
            "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "translog_durability": "async",
            "translog_sync_interval_ms": 3000
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.translog_durability, "async");
        assert_eq!(config.translog_sync_interval_ms, Some(3000));
    }

    #[test]
    fn deserialize_request_durability_without_interval() {
        let json = r#"{
            "node_name": "n1",
            "cluster_name": "test",
            "http_port": 9200,
            "transport_port": 9300,
            "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "translog_durability": "request"
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.translog_durability, "request");
        assert!(config.translog_sync_interval_ms.is_none());
    }

    #[test]
    fn deserialize_omitted_durability_defaults_to_request() {
        let json = r#"{
            "node_name": "n1",
            "cluster_name": "test",
            "http_port": 9200,
            "transport_port": 9300,
            "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"]
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.translog_durability, "request");
        assert!(config.translog_sync_interval_ms.is_none());
    }

    #[test]
    fn seed_hosts_deserializes_from_array() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302"]
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.seed_hosts.len(), 3);
        assert_eq!(config.seed_hosts[0], "127.0.0.1:9300");
        assert_eq!(config.seed_hosts[2], "127.0.0.1:9302");
    }

    #[test]
    fn seed_hosts_deserializes_from_comma_separated_string() {
        // This is what the `config` crate produces when reading from env vars
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": "127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302"
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.seed_hosts.len(), 3);
        assert_eq!(config.seed_hosts[0], "127.0.0.1:9300");
        assert_eq!(config.seed_hosts[2], "127.0.0.1:9302");
    }

    #[test]
    fn seed_hosts_deserializes_single_value_string() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": "127.0.0.1:9300"
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.seed_hosts.len(), 1);
        assert_eq!(config.seed_hosts[0], "127.0.0.1:9300");
    }

    #[test]
    fn seed_hosts_trims_whitespace() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": "127.0.0.1:9300 , 127.0.0.1:9301 , 127.0.0.1:9302"
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.seed_hosts.len(), 3);
        assert_eq!(config.seed_hosts[1], "127.0.0.1:9301");
    }

    #[test]
    fn default_column_cache_settings() {
        let config = AppConfig::default();
        assert_eq!(config.column_cache_size_percent, 10);
        assert_eq!(config.column_cache_populate_threshold, 5);
    }

    #[test]
    fn column_cache_settings_deserialize_from_json() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "column_cache_size_percent": 25,
            "column_cache_populate_threshold": 10
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.column_cache_size_percent, 25);
        assert_eq!(config.column_cache_populate_threshold, 10);
    }

    #[test]
    fn column_cache_settings_default_when_omitted() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"]
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.column_cache_size_percent, 10);
        assert_eq!(config.column_cache_populate_threshold, 5);
    }

    #[test]
    fn transport_tls_disabled_by_default() {
        let config = AppConfig::default();
        assert!(!config.transport_tls_enabled);
        assert!(config.transport_tls_cert_file.is_none());
        assert!(config.transport_tls_key_file.is_none());
        assert!(config.transport_tls_ca_file.is_none());
    }

    #[test]
    fn transport_tls_deserializes_when_set() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "transport_tls_enabled": true,
            "transport_tls_cert_file": "/path/to/cert.pem",
            "transport_tls_key_file": "/path/to/key.pem",
            "transport_tls_ca_file": "/path/to/ca.pem"
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert!(config.transport_tls_enabled);
        assert_eq!(
            config.transport_tls_cert_file.as_deref(),
            Some("/path/to/cert.pem")
        );
        assert_eq!(
            config.transport_tls_key_file.as_deref(),
            Some("/path/to/key.pem")
        );
        assert_eq!(
            config.transport_tls_ca_file.as_deref(),
            Some("/path/to/ca.pem")
        );
    }

    #[test]
    fn transport_tls_omitted_defaults_to_disabled() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"]
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert!(!config.transport_tls_enabled);
        assert!(config.transport_tls_cert_file.is_none());
    }

    #[test]
    fn sql_group_by_scan_limit_defaults_to_1m() {
        let config = AppConfig::default();
        assert_eq!(config.sql_group_by_scan_limit, 1_000_000);
    }

    #[test]
    fn sql_group_by_scan_limit_deserializes() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "sql_group_by_scan_limit": 5000000
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sql_group_by_scan_limit, 5_000_000);
    }

    #[test]
    fn sql_group_by_scan_limit_zero_means_unlimited() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "sql_group_by_scan_limit": 0
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sql_group_by_scan_limit, 0);
    }

    #[test]
    fn sql_approximate_top_k_defaults_to_true() {
        let config = AppConfig::default();
        assert!(config.sql_approximate_top_k);
    }

    #[test]
    fn sql_approximate_top_k_can_be_disabled() {
        let json = r#"{
            "node_name": "n1", "cluster_name": "test", "http_port": 9200,
            "transport_port": 9300, "data_dir": "./data",
            "seed_hosts": ["127.0.0.1:9300"],
            "sql_approximate_top_k": false
        }"#;
        let config: AppConfig = serde_json::from_str(json).unwrap();
        assert!(!config.sql_approximate_top_k);
    }
}
