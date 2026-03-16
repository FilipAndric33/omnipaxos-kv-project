use omnipaxos::util::NodeId;
use omnipaxos_kv::common::kv::ClientId;
use serde::{Deserialize, Serialize};
use std::env;
use config::{Config, ConfigError, File};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    location: Option<String>,
    proxy_id: u16,
    pub listen_address: String,
    pub listen_port: u16,
    pub fault_tolerance: usize,
    pub clients: Vec<ClientId>,
    pub nodes: Vec<Nodes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Nodes {
    pub id: NodeId,
    pub address: String,
    pub listening_port: u16,
}

impl ProxyConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("PROXY_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Proxy requires the PROXY_CONFIG_FILE to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .build()?;

        config.try_deserialize()
    }
}