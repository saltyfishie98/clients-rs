use futures_util::StreamExt;
use paho_mqtt as mqtt;
use std::time::Duration;

pub mod mqtt_config {
    use super::*;

    #[derive(Default)]
    pub struct MqttSubscriptions {
        pub(super) topics: Vec<String>,
        pub(super) qos: Vec<i32>,
        pub(super) opts: Vec<mqtt::SubscribeOptions>,
        pub(super) props: Option<mqtt::Properties>,
    }

    impl MqttSubscriptions {
        pub fn new(props: Option<mqtt::Properties>) -> Self {
            Self {
                topics: Vec::new(),
                qos: Vec::new(),
                opts: Vec::new(),
                props,
            }
        }

        pub fn add(
            &mut self,
            topic: impl Into<String>,
            qos: i32,
            opt: mqtt::SubscribeOptions,
        ) -> &Self {
            self.topics.push(topic.into());
            self.qos.push(qos);
            self.opts.push(opt);
            self
        }

        pub fn finalize(self) -> Self {
            self
        }
    }

    pub struct MqttClient {
        pub mqtt_create_options: mqtt::CreateOptions,
        pub mqtt_connect_options: mqtt::ConnectOptions,
        pub msg_buffer_limit: usize,
        pub subscriptions: MqttSubscriptions,
    }

    impl Default for MqttClient {
        fn default() -> Self {
            Self {
                mqtt_create_options: Default::default(),
                mqtt_connect_options: Default::default(),
                subscriptions: Default::default(),
                msg_buffer_limit: 1,
            }
        }
    }
}

pub struct MqttClient {
    mqtt_client: mqtt::AsyncClient,
    mqtt_subscription_stream: mqtt::AsyncReceiver<Option<mqtt::Message>>,
    mqtt_connect_opt: mqtt::ConnectOptions,
    mqtt_subscriptions: mqtt_config::MqttSubscriptions,
}

impl MqttClient {
    pub fn new(config: mqtt_config::MqttClient) -> Result<Self, mqtt::Error> {
        let mut mqtt_client = mqtt::AsyncClient::new(config.mqtt_create_options)?;
        let mqtt_subscription_stream = mqtt_client.get_stream(config.msg_buffer_limit);

        Ok(MqttClient {
            mqtt_client,
            mqtt_subscription_stream,
            mqtt_connect_opt: config.mqtt_connect_options,
            mqtt_subscriptions: config.subscriptions,
        })
    }

    pub async fn connect(&self) {
        let host = self.mqtt_client.server_uri();

        while let Err(_) = self
            .mqtt_client
            .connect(self.mqtt_connect_opt.clone())
            .await
        {
            log::warn!("Error establishing connection to '{}', retrying...", host);
        }
        log::info!("Connected to broker '{}'", host);

        let mut subscription = self.mqtt_client.subscribe_many_with_options(
            &self.mqtt_subscriptions.topics,
            &self.mqtt_subscriptions.qos,
            &self.mqtt_subscriptions.opts,
            self.mqtt_subscriptions.props.clone(),
        );

        // while self.mqtt_client.is_connected() {}

        loop {
            // Does not work on paho.mqtt.rust v0.12.3
            if subscription.try_wait().is_none() {
                if !self.mqtt_client.is_connected() {
                    self.reconnect().await;
                } else {
                    break;
                }
            }
        }

        log::info!("Subscribed to topics: {:?}", self.mqtt_subscriptions.topics);
    }

    pub async fn poll(&mut self) -> Option<mqtt::Message> {
        if !self.mqtt_client.is_connected() {
            self.reconnect().await;
        }

        self.mqtt_subscription_stream.next().await?
    }

    pub fn publish(&self, msg: mqtt::Message) -> mqtt::DeliveryToken {
        self.mqtt_client.publish(msg)
    }

    pub async fn reconnect(&self) {
        use tokio::time::sleep;

        let host = self.mqtt_client.server_uri();

        let log_lost_connection = |hostname: String| {
            tokio::spawn(async move {
                log::warn!("Lost connection to '{}', reconnecting...", hostname);
                sleep(Duration::from_secs(2)).await;
            })
        };

        while !self.mqtt_client.is_connected() {
            let mut logger = log_lost_connection(host.clone());

            match self.mqtt_client.reconnect().await {
                Ok(res) => {
                    log::debug!("Reconnection response: {:?}", res);
                    log::warn!("Reconnected to '{}'", host);
                    logger.abort();
                }
                Err(e) => {
                    log::debug!("Reconnection error: {}", e);
                    if logger.is_finished() {
                        logger = log_lost_connection(host.clone());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use mqtt::MQTT_VERSION_5;
    use serde_json::json;

    use super::*;

    #[test]
    fn building_client() {
        let configs = mqtt_config::MqttClient {
            mqtt_create_options: {
                mqtt::CreateOptionsBuilder::new()
                    .server_uri("mqtt://test.mosquitto.org:1883")
                    .client_id("saltyfishie_1")
                    .finalize()
            },

            mqtt_connect_options: {
                let lwt = mqtt::Message::new(
                    "saltyfishie/echo/lwt",
                    "[LWT] Async subscriber v5 lost connection",
                    mqtt::QOS_1,
                );

                mqtt::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
                    .keep_alive_interval(Duration::from_millis(5000))
                    .clean_start(false)
                    .properties(mqtt::properties![mqtt::PropertyCode::SessionExpiryInterval => 60])
                    .will_message(lwt)
                    .finalize()
            },

            subscriptions: {
                mqtt_config::MqttSubscriptions::new(None).add("saltyfishe", 1, Default::default())
            },

            msg_buffer_limit: 10,
        };

        let mut client = MqttClient::new(configs).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            client.connect().await;
            loop {
                if let Some(msg) = client.poll().await {
                    if msg.retained() {
                        print!("(R) ");
                    }
                    log::info!("\n{}", msg);

                    let json = serde_json::from_slice::<serde_json::Value>(msg.payload()).unwrap();

                    if let serde_json::Value::Object(data) = json {
                        let mut keys = Vec::<&String>::new();
                        let mut values = Vec::<&serde_json::Value>::new();

                        data.iter().for_each(|(k, v)| {
                            keys.push(k);
                            values.push(v)
                        });

                        let out = json!({
                            "keys": keys,
                            "values": values,
                            "timestamp": chrono::Utc::now().to_string()
                        });

                        client.publish(mqtt::Message::new(
                            "saltyfishie/echo",
                            serde_json::to_string_pretty(&out).unwrap(),
                            1,
                        ));
                    }
                }
            }
        });
    }

    #[test]
    fn building_subscriptions() {
        let subs = mqtt_config::MqttSubscriptions::new(None).add(
            "topic",
            1,
            mqtt::SubscribeOptions::default(),
        );

        assert!(subs.topics[0] == "topic".to_string());
        assert!(subs.qos[0] == 1);
    }
}
