use proxy::Proxy;
use configs::ProxyConfig;
use core::panic;
use env_logger;

mod proxy;
mod configs;
mod database;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let proxy_config = match ProxyConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut proxy = Proxy::new(proxy_config).await;
    proxy.run().await;
}