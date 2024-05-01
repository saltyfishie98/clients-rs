use futures_util::StreamExt;
use paho::MQTT_VERSION_5;
use paho_mqtt as paho;
use serde::Deserialize;
use std::time::Duration;

pub mod deserialized {
    use super::*;
    use serde::de::Visitor;

    pub mod subscribe_options {
        use super::*;

        #[derive(Debug)]
        pub struct RetainHandling {
            pub(crate) inner: paho::RetainHandling,
        }
        impl<'de> Deserialize<'de> for RetainHandling {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                const VALID_RETAIN_HANDLING: &[&str] = &["0", "1", "2"];

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
                        match TryInto::<i64>::try_into(v) {
                            Ok(v) => self.visit_i64(v),
                            _ => Err(serde::de::Error::unknown_variant(
                                &format!("{v}"),
                                VALID_RETAIN_HANDLING,
                            )),
                        }
                    }

                    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        match v {
                            0 => Ok(paho::RetainHandling::SendRetainedOnSubscribe),
                            1 => Ok(paho::RetainHandling::SendRetainedOnNew),
                            2 => Ok(paho::RetainHandling::DontSendRetained),
                            _ => Err(serde::de::Error::unknown_variant(
                                &format!("{v}"),
                                VALID_RETAIN_HANDLING,
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
    pub struct SubscribeOptions {
        pub(crate) no_local: bool,
        pub(crate) retain_as_publish: bool,
        pub(crate) retain_handling: subscribe_options::RetainHandling,
    }
    impl From<SubscribeOptions> for paho_mqtt::SubscribeOptions {
        fn from(value: SubscribeOptions) -> Self {
            paho_mqtt::SubscribeOptionsBuilder::new()
                .no_local(value.no_local)
                .retain_as_published(value.retain_as_publish)
                .retain_handling(value.retain_handling.inner)
                .finalize()
        }
    }

    #[derive(Debug, Deserialize)]
    pub struct Properties {}
    impl Into<paho::Properties> for Properties {
        fn into(self) -> paho::Properties {
            // TODO: actually read
            paho::Properties::default()
        }
    }

    #[derive(Default, Debug)]
    pub struct Subscriptions {
        pub(crate) topics: Vec<String>,
        pub(crate) qos: Vec<i32>,
        pub(crate) opts: Vec<SubscribeOptions>,
    }
    impl<'de> Deserialize<'de> for Subscriptions {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct SubscriptionVisitor;
            impl<'de> Visitor<'de> for SubscriptionVisitor {
                type Value = Subscriptions;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("tables")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    #[derive(Deserialize)]
                    struct SettingFields {
                        qos: i32,
                        options: SubscribeOptions,
                    }

                    let mut out = Self::Value::default();
                    while let Some((k, v)) = map.next_entry::<String, SettingFields>()? {
                        out.topics.push(k.to_string());
                        out.qos.push(v.qos);
                        out.opts.push(v.options);
                    }
                    Ok(out)
                }
            }
            deserializer.deserialize_map(SubscriptionVisitor)
        }
    }

    #[derive(Debug, Default, Deserialize)]
    pub struct MqttClientConfig {
        pub(crate) client_id: String,
        pub(crate) broker_uri: String,
        pub(crate) subscription_props: Option<Properties>,
        pub(crate) subscriptions: Subscriptions,
    }
}

#[derive(Default, Debug)]
pub(crate) struct SubscriptionData {
    pub(crate) topics: Vec<String>,
    pub(crate) qos: Vec<i32>,
    pub(crate) opts: Vec<paho::SubscribeOptions>,
}
impl From<deserialized::Subscriptions> for SubscriptionData {
    fn from(value: deserialized::Subscriptions) -> Self {
        SubscriptionData {
            topics: value.topics,
            qos: value.qos,
            opts: value.opts.into_iter().map(|o| o.into()).collect(),
        }
    }
}

pub struct MqttClientConfig {
    pub(crate) mqtt_create_options: paho::CreateOptions,
    pub(crate) mqtt_connect_options: paho::ConnectOptions,
    pub(crate) msg_buffer_limit: usize,
    pub(crate) subscription_props: Option<paho::Properties>,
    pub(crate) subscriptions: SubscriptionData,
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

        let subscriptions = value.subscriptions.into();
        let subscription_props = match value.subscription_props {
            Some(p) => Some(p.into()),
            None => None,
        };

        Self {
            mqtt_create_options,
            mqtt_connect_options,
            msg_buffer_limit: 100,
            subscription_props,
            subscriptions,
        }
    }
}
impl Default for MqttClientConfig {
    fn default() -> Self {
        Self {
            mqtt_create_options: Default::default(),
            mqtt_connect_options: Default::default(),
            msg_buffer_limit: 1,
            subscription_props: None,
            subscriptions: Default::default(),
        }
    }
}

pub struct MqttClient {
    pub(crate) mqtt_client: paho::AsyncClient,
    pub(crate) mqtt_subscription_stream: paho::AsyncReceiver<Option<paho::Message>>,
    pub(crate) mqtt_connect_opt: paho::ConnectOptions,
    pub(crate) mqtt_subscription_props: Option<paho::Properties>,
    pub(crate) mqtt_subscriptions: SubscriptionData,
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
            mqtt_subscription_props: config.subscription_props,
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
            self.mqtt_subscription_props.clone(),
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
mod test {}
