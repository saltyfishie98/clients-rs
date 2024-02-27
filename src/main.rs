mod forwarder;

use forwarder::mqtt_client::*;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use serde_json::json;
use std::time::Duration;

const HOST: &str = "mqtt://test.mosquitto.org:1883";

#[tokio::main]
async fn main() -> Result<(), mqtt::Error> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Trace)
        .init();

    let mut client = MqttClient::new(MqttSetupConfigs {
        mqtt_create_options: {
            mqtt::CreateOptionsBuilder::new()
                .server_uri(HOST)
                .client_id("saltyfishie_3")
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

        subscriptions: MqttSubscriptions::new(None).add("saltyfishie", 1, Default::default()),
        msg_buffer_limit: 10,
    })?;

    client.connect().await;

    loop {
        if let Some(msg) = client.poll().await {
            if msg.retained() {
                print!("(R) ");
            }
            log::info!("Received message:\n\n{}\n", msg);

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
}
