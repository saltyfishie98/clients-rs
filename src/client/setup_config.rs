use std::{fs::File, io::BufReader, path::PathBuf};

#[derive(Debug, serde::Deserialize)]
pub struct MqttSetupConfig {
    pub client_id: String,
    pub broker_uri: String,
    pub subscriptions: Vec<mqtt::Topic>,
}

impl TryFrom<PathBuf> for MqttSetupConfig {
    type Error = std::io::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let file = File::open(path)?;
        let config_reader = BufReader::new(file);
        Ok(serde_json::from_reader(config_reader).expect("Failed to deserialize JSON"))
    }
}

pub mod mqtt {
    #[derive(Debug, serde::Deserialize, Clone, Copy)]
    pub struct TopicOptions {
        pub no_local: bool,
        pub retain_as_publish: bool,
        pub retain_handling: i32,
    }

    impl From<TopicOptions> for paho_mqtt::SubscribeOptions {
        fn from(value: TopicOptions) -> Self {
            paho_mqtt::SubscribeOptionsBuilder::new()
                .no_local(value.no_local)
                .retain_as_published(value.retain_as_publish)
                .retain_handling(
                    value
                        .retain_handling
                        .try_into()
                        .expect("valid input is 0, 1, and 2"),
                )
                .finalize()
        }
    }

    #[derive(Debug, serde::Deserialize)]
    pub struct Topic {
        pub topic: String,
        pub qos: i32,
        pub options: Option<TopicOptions>,
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct SqlServerSetupConfig {
    pub host: String,
    pub username: String,
    pub password: Option<String>,
    pub database: String,
    pub port: Option<u16>,
    pub topic_table_map: sql_server::TopicTableMapping,
}

impl TryFrom<PathBuf> for SqlServerSetupConfig {
    type Error = std::io::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let file = File::open(path)?;
        let config_reader = BufReader::new(file);
        Ok(serde_json::from_reader(config_reader).expect("Failed to deserialize JSON"))
    }
}

pub mod sql_server {
    use std::collections::HashMap;

    pub type TopicTableMapping = HashMap<String, String>;
}
