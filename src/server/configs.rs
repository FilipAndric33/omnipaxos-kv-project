use std::env;

use config::{Config, ConfigError, Environment, File};
use omnipaxos::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
}

/// Clock quality: uncertainty = ±bound (microseconds), sync_interval_ms = resync period.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClockConfig {
    /// Clock uncertainty bound in microseconds (±). e.g. 10 = ±10μs (high), 100 = ±100μs (medium), 1000 = ±1ms (low).
    #[serde(default = "default_clock_uncertainty_us")]
    pub clock_uncertainty_us: u64,
    /// Sync interval in milliseconds. e.g. 1 (high), 10 (medium), 100 (low).
    #[serde(default = "default_clock_sync_interval_ms")]
    pub clock_sync_interval_ms: u32,
}

fn default_clock_uncertainty_us() -> u64 {
    10
}
fn default_clock_sync_interval_ms() -> u32 {
    1
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub num_clients: usize,
    pub output_filepath: String,
    #[serde(default)]
    pub clock: Option<ClockConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosKVConfig {
    #[serde(flatten)]
    pub local: LocalConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

impl Into<OmniPaxosConfig> for OmniPaxosKVConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.local.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl ClockConfig {
    /// From env OMNIPAXOS_CLOCK_UNCERTAINTY_US and OMNIPAXOS_CLOCK_SYNC_INTERVAL_MS (for benchmarks).
    pub fn from_env() -> Option<Self> {
        let uncertainty = env::var("OMNIPAXOS_CLOCK_UNCERTAINTY_US").ok()?.parse().ok()?;
        let sync_ms = env::var("OMNIPAXOS_CLOCK_SYNC_INTERVAL_MS").ok()?.parse().ok()?;
        Some(ClockConfig { clock_uncertainty_us: uncertainty, clock_sync_interval_ms: sync_ms })
    }
}

impl OmniPaxosKVConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}
