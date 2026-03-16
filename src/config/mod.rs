// Adds required field to AppConfig
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub node_name: String,
    pub cluster_name: String,
    pub http_port: u16,
    pub transport_port: u16,
    pub data_dir: String,
    pub seed_hosts: Vec<String>,
    /// Unique numeric node ID for Raft consensus (must be unique across cluster).
    #[serde(default = "default_raft_node_id")]
    pub raft_node_id: u64,
}

fn default_raft_node_id() -> u64 {
    1
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            node_name: "node-1".into(),
            cluster_name: "ropensearch".into(),
            http_port: 9200,
            transport_port: 9300,
            data_dir: "./data".into(),
            seed_hosts: vec!["127.0.0.1:9300".into()],
            raft_node_id: 1,
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
            .add_source(File::with_name("config/ropensearch").required(false))
            .add_source(Environment::with_prefix("ROPENSEARCH").list_separator(","));

        let config = builder.build()?;
        let app_config: AppConfig = config.try_deserialize()?;

        Ok(app_config)
    }
}
