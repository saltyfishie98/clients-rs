use async_std::stream::StreamExt;
use paho_mqtt as mqtt;
use std::time::Duration;

pub struct MqttClient {
    mqtt_client: mqtt::AsyncClient,
    mqtt_subscription_stream: mqtt::AsyncReceiver<Option<mqtt::Message>>,
    mqtt_connect_opt: mqtt::ConnectOptions,
    mqtt_subscriptions: MqttSubscriptions,
}

impl MqttClient {
    pub fn new(config: MqttSetupConfigs) -> Result<Self, mqtt::Error> {
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
        while let Err(e) = self
            .mqtt_client
            .connect(self.mqtt_connect_opt.clone())
            .await
        {
            log::error!("{}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        log::info!("Connected to '{}'!", self.mqtt_client.server_uri());

        while self.mqtt_client.is_connected() {}

        self.mqtt_client.subscribe_many_with_options(
            &self.mqtt_subscriptions.topics,
            &self.mqtt_subscriptions.qos,
            &self.mqtt_subscriptions.opts,
            self.mqtt_subscriptions.props.clone(),
        );

        log::info!("Subscribed to topics: {:?}", self.mqtt_subscriptions.topics);
    }

    pub async fn poll(&mut self) -> Option<mqtt::Message> {
        if !self.mqtt_client.is_connected() {
            log::info!("Lost connection. Attempting reconnect.");
            self.reconnect().await;
        }

        self.mqtt_subscription_stream.next().await?
    }

    pub fn publish(&self, msg: mqtt::Message) -> mqtt::DeliveryToken {
        self.mqtt_client.publish(msg)
    }

    pub async fn reconnect(&self) {
        while !self.mqtt_client.is_connected() {
            match self.mqtt_client.reconnect().await {
                Ok(res) => {
                    log::info!("Reconnected!");
                    log::info!("Reconnect response: {:?}", res)
                }
                Err(e) => log::error!("Reconnection error: {}", e),
            }
        }
    }
}

#[derive(Default)]
pub struct MqttSubscriptions {
    topics: Vec<String>,
    qos: Vec<i32>,
    opts: Vec<mqtt::SubscribeOptions>,
    props: Option<mqtt::Properties>,
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

    pub fn add(mut self, topic: impl Into<String>, qos: i32, opt: mqtt::SubscribeOptions) -> Self {
        self.topics.push(topic.into());
        self.qos.push(qos);
        self.opts.push(opt);
        self
    }
}

pub struct MqttSetupConfigs {
    pub mqtt_create_options: mqtt::CreateOptions,
    pub mqtt_connect_options: mqtt::ConnectOptions,
    pub msg_buffer_limit: usize,
    pub subscriptions: MqttSubscriptions,
}

impl Default for MqttSetupConfigs {
    fn default() -> Self {
        Self {
            mqtt_create_options: Default::default(),
            mqtt_connect_options: Default::default(),
            subscriptions: Default::default(),
            msg_buffer_limit: 1,
        }
    }
}

// pub struct MqttClientBuilder {
//     mqtt_configs: MqttSetupConfigs,
// }

// impl MqttClientBuilder {
//     pub fn new() -> Self {
//         Self {
//             mqtt_configs: Default::default(),
//         }
//     }

//     pub fn with_mqtt_configs(mut self, create_opt: MqttSetupConfigs) -> Self {
//         self.mqtt_configs = create_opt;
//         self
//     }

//     pub fn build(self) -> Result<MqttClient, mqtt::Error> {
//         let mut mqtt_client = mqtt::AsyncClient::new(self.mqtt_configs.create_opt)?;
//         let mqtt_subscription_stream = mqtt_client.get_stream(self.mqtt_configs.msg_buffer_limit);

//         Ok(MqttClient {
//             mqtt_client,
//             mqtt_subscription_stream,
//             mqtt_connect_opt: self.mqtt_configs.connect_opt,
//             mqtt_subscriptions: self.mqtt_configs.subscriptions,
//         })
//     }
// }

#[cfg(test)]
mod test {
    use mqtt::MQTT_VERSION_5;
    use serde_json::json;

    use super::*;

    #[test]
    fn building_client() {
        let configs = MqttSetupConfigs {
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
                MqttSubscriptions::new(None).add("saltyfishe", 1, Default::default())
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
                } else {
                    log::info!("Lost connection. Attempting reconnect.");
                    while let Err(err) = client.reconnect().await {
                        log::info!("Error reconnecting: {}", err);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    log::info!("Reconnected!");
                }
            }
        });
    }

    #[test]
    fn building_subscriptions() {
        let subs = MqttSubscriptions::new(None).add("topic", 1, mqtt::SubscribeOptions::default());

        assert!(subs.topics[0] == "topic".to_string());
        assert!(subs.qos[0] == 1);
    }
}
