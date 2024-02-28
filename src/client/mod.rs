mod database;
mod mqtt;

pub use database::*;
pub use mqtt::*;

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    pub mqtt_topics: Vec<mqtt_topic::Topic>,
}

pub mod mqtt_topic {
    #[derive(Debug, serde::Deserialize)]
    pub struct TopicOptions {
        pub no_local: bool,
        pub retain_as_publish: bool,
        pub retain_handling: i32,
    }

    impl Into<paho_mqtt::SubscribeOptions> for TopicOptions {
        fn into(self) -> paho_mqtt::SubscribeOptions {
            use paho_mqtt::{RetainHandling, SubscribeOptions};

            SubscribeOptions::new(
                self.no_local,
                self.retain_as_publish,
                RetainHandling::try_from(self.retain_handling).unwrap(),
            )
        }
    }

    #[derive(Debug, serde::Deserialize)]
    pub struct Topic {
        pub topic: String,
        pub qos: i32,
        pub options: Option<TopicOptions>,
    }
}
