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
}

fn default_raft_node_id() -> u64 {
    1
}

fn default_translog_durability() -> String {
    "request".to_string()
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            node_name: "node-1".into(),
            cluster_name: "ferrissearch".into(),
            http_port: 9200,
            transport_port: 9300,
            data_dir: "./data".into(),
            seed_hosts: vec!["127.0.0.1:9300".into()],
            raft_node_id: 1,
            translog_durability: "request".to_string(),
            translog_sync_interval_ms: None,
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
}
