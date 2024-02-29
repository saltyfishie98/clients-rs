pub mod mqtt {
    #[derive(Debug, serde::Deserialize)]
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

pub mod sql_server {}
