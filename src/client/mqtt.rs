use futures_util::StreamExt;
use paho::MQTT_VERSION_5;
use paho_mqtt as paho;
use serde::Deserialize;
use std::{collections::HashMap, time::Duration};

pub mod topic {
    use super::*;

    #[derive(Default, Debug)]
    pub struct Subscriptions {
        pub(super) topics: Vec<String>,
        pub(super) qos: Vec<i32>,
        pub(super) opts: Vec<paho::SubscribeOptions>,
        pub(super) props: Option<paho::Properties>,
    }

    impl Subscriptions {
        pub fn new(props: Option<paho::Properties>) -> Self {
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
            opt: paho::SubscribeOptions,
        ) -> &Self {
            self.topics.push(topic.into());
            self.qos.push(qos);
            self.opts.push(opt);
            self
        }

        pub fn finalize(&mut self) -> Self {
            std::mem::take(self)
        }
    }

    pub mod options {
        use super::*;
        use serde::de::Visitor;

        #[derive(Debug)]
        pub struct RetainHandling {
            pub(super) inner: paho::RetainHandling,
        }
        impl<'de> Deserialize<'de> for RetainHandling {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct PahoRetainHandlingVisitor;
                impl<'de> Visitor<'de> for PahoRetainHandlingVisitor {
                    type Value = paho::RetainHandling;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("integer 0, 1, or 2")
                    }

                    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        match v {
                            0 => Ok(paho::RetainHandling::SendRetainedOnSubscribe),
                            1 => Ok(paho::RetainHandling::SendRetainedOnNew),
                            2 => Ok(paho::RetainHandling::DontSendRetained),
                            _ => Err(serde::de::Error::unknown_variant(
                                &format!("{v}"),
                                &["0", "1", "2"],
                            )),
                        }
                    }
                }

                let inner = deserializer.deserialize_u64(PahoRetainHandlingVisitor)?;
                Ok(RetainHandling { inner })
            }
        }
    }

    #[derive(Debug, Deserialize)]
    pub struct Options {
        pub no_local: bool,
        pub retain_as_publish: bool,
        pub retain_handling: options::RetainHandling,
    }
    impl From<Options> for paho_mqtt::SubscribeOptions {
        fn from(value: Options) -> Self {
            paho_mqtt::SubscribeOptionsBuilder::new()
                .no_local(value.no_local)
                .retain_as_published(value.retain_as_publish)
                .retain_handling(value.retain_handling.inner)
                .finalize()
        }
    }

    #[derive(Debug, serde::Deserialize)]
    pub struct Settings {
        pub qos: i32,
        pub options: Option<Options>,
    }

    pub type Topic = HashMap<String, Settings>;
}

pub mod deserialized {
    use super::*;

    #[derive(Debug, serde::Deserialize, Default)]
    pub struct MqttClientConfig {
        pub client_id: String,
        pub broker_uri: String,
        pub subscriptions: Vec<topic::Topic>,
    }
}

pub struct MqttClientConfig {
    pub mqtt_create_options: paho::CreateOptions,
    pub mqtt_connect_options: paho::ConnectOptions,
    pub msg_buffer_limit: usize,
    pub subscriptions: topic::Subscriptions,
}
impl From<deserialized::MqttClientConfig> for MqttClientConfig {
    fn from(value: deserialized::MqttClientConfig) -> Self {
        let mqtt_create_options = paho::CreateOptionsBuilder::new()
            .server_uri(value.broker_uri)
            .client_id(value.client_id)
            .finalize();

        let mqtt_connect_options = {
            let lwt = paho_mqtt::Message::new(
                "saltyfishie/echo/lwt",
                "[LWT] Async subscriber v5 lost connection",
                paho_mqtt::QOS_1,
            );

            paho_mqtt::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
                .keep_alive_interval(Duration::from_millis(5000))
                .clean_start(false)
                .properties(
                    paho_mqtt::properties![paho_mqtt::PropertyCode::SessionExpiryInterval => 60],
                )
                .will_message(lwt)
                .finalize()
        };

        let subscriptions = {
            let mut s = topic::Subscriptions::new(None);

            value.subscriptions.into_iter().for_each(|topic| {
                topic.into_iter().for_each(|(k, v)| {
                    s.add(
                        &k,
                        v.qos,
                        match v.options {
                            Some(o) => o.into(),
                            None => paho_mqtt::SubscribeOptions::default(),
                        },
                    );
                })
            });

            s.finalize()
        };

        Self {
            mqtt_create_options,
            mqtt_connect_options,
            msg_buffer_limit: 100,
            subscriptions,
        }
    }
}
impl Default for MqttClientConfig {
    fn default() -> Self {
        Self {
            mqtt_create_options: Default::default(),
            mqtt_connect_options: Default::default(),
            subscriptions: Default::default(),
            msg_buffer_limit: 1,
        }
    }
}

pub struct MqttClient {
    mqtt_client: paho::AsyncClient,
    mqtt_subscription_stream: paho::AsyncReceiver<Option<paho::Message>>,
    mqtt_connect_opt: paho::ConnectOptions,
    mqtt_subscriptions: topic::Subscriptions,
}

impl MqttClient {
    pub async fn start(config: MqttClientConfig) -> Self {
        let mut mqtt_client = match paho::AsyncClient::new(config.mqtt_create_options) {
            Ok(c) => c,
            Err(e) => {
                log::error!("MqttClient start error: {}", e);
                std::process::exit(1)
            }
        };

        let mqtt_subscription_stream = mqtt_client.get_stream(config.msg_buffer_limit);
        let out = MqttClient {
            mqtt_client,
            mqtt_subscription_stream,
            mqtt_connect_opt: config.mqtt_connect_options,
            mqtt_subscriptions: config.subscriptions,
        };

        Self::connect(&out).await;
        out
    }

    pub async fn poll(&mut self) -> Option<paho::Message> {
        if !self.mqtt_client.is_connected() {
            self.reconnect().await;
        }

        self.mqtt_subscription_stream.next().await?
    }

    pub fn publish(&self, msg: paho::Message) -> paho::DeliveryToken {
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

        let mut logger = log_lost_connection(host.clone());

        while !self.mqtt_client.is_connected() {
            match self.mqtt_client.reconnect().await {
                Ok(res) => {
                    log::debug!("Reconnection response: {:?}", res);
                    log::warn!("Reconnected to '{}'", host);
                    logger.abort();
                    break;
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

    async fn connect(&self) {
        let host = self.mqtt_client.server_uri();

        while (self
            .mqtt_client
            .connect(self.mqtt_connect_opt.clone())
            .await)
            .is_err()
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
            // Does not work on paho.paho.rust v0.12.3
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
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn building_client() {
        let _subscriptions = {
            let mut s = topic::Subscriptions::new(None);
            s.add("saltyfishe", 1, Default::default());
            s.finalize()
        };

        let configs = MqttClientConfig {
            mqtt_create_options: {
                paho::CreateOptionsBuilder::new()
                    .server_uri("paho://test.mosquitto.org:1883")
                    .client_id("saltyfishie_1")
                    .finalize()
            },

            mqtt_connect_options: {
                let lwt = paho::Message::new(
                    "saltyfishie/echo/lwt",
                    "[LWT] Async subscriber v5 lost connection",
                    paho::QOS_1,
                );

                paho::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
                    .keep_alive_interval(Duration::from_millis(5000))
                    .clean_start(false)
                    .properties(paho::properties![paho::PropertyCode::SessionExpiryInterval => 0xFFFFFFFF as u32])
                    .will_message(lwt)
                    .finalize()
            },

            subscriptions: {
                let mut s = topic::Subscriptions::new(None);
                s.add("saltyfishe", paho::QOS_1, Default::default());
                s.finalize()
            },

            msg_buffer_limit: 10,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut client = MqttClient::start(configs).await;

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

                        client.publish(paho::Message::new(
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
        let subs = {
            let mut s = topic::Subscriptions::new(None);
            s.add("topic", 1, paho::SubscribeOptions::default());
            s.finalize()
        };

        assert!(subs.topics[0] == "topic".to_string());
        assert!(subs.qos[0] == 1);
    }
}
