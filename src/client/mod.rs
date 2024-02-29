mod database;
mod mqtt;
pub mod setup_config;

pub use database::*;
pub use mqtt::*;

#[derive(Debug, serde::Deserialize)]
pub struct SetupConfig {
    pub client_id: String,
    pub broker_uri: String,
    pub subscriptions: Vec<setup_config::mqtt::Topic>,
}
