#[allow(dead_code)]
mod client;

use client::mqtt_config;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use serde_json::json;
use std::fs::File;
use std::io::{BufReader, Write};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), mqtt::Error> {
    env_logger::Builder::from_default_env()
        .filter_module("mqtt_sql_forwarder", log::LevelFilter::Info)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] ({}:{}) - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();

    let (_db_client_haiwell, mut mqtt_client) = tokio::join!(make_db_client(), make_mqtt_client());

    loop {
        if let Some(msg) = mqtt_client.poll().await {
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

                mqtt_client.publish(mqtt::Message::new(
                    "saltyfishie/echo",
                    serde_json::to_string_pretty(&out).unwrap(),
                    1,
                ));
            }
        }
    }
}

async fn make_db_client() -> client::DatabaseClient {
    client::DatabaseClient::connect().await
}

async fn make_mqtt_client() -> client::MqttClient {
    let mqtt_create_options = mqtt::CreateOptionsBuilder::new()
        .server_uri("mqtt://test.mosquitto.org:1883")
        .client_id("saltyfishie_3")
        .finalize();

    let mqtt_connect_options = {
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
    };

    let subscriptions = {
        let source_dir = std::env::current_dir().unwrap();
        let config_file_path = source_dir.join("config").join("client.json");

        log::info!("Configuration: \"{}\"", config_file_path.to_str().unwrap_or("{unknown}"));
        let file = match File::open(config_file_path) {
            Ok(f) => f,
            Err(e) => {
                println!("Error opening config file: {}", e);
                std::process::exit(1);
            },
        };

        let config_reader = BufReader::new(file);
        let config: client::Config =
            serde_json::from_reader(config_reader).expect("Failed to deserialize JSON");

        let mut s = mqtt_config::MqttSubscriptions::new(None);

        config.mqtt_topics.into_iter().for_each(|topic| {
            let opts = match topic.options {
                Some(o) => o.into(),
                None => mqtt::SubscribeOptions::default(),
            };
            s.add(topic.topic.as_str(), topic.qos, opts);
        });

        s.finalize()
    };

    let out_client = client::MqttClient::new(mqtt_config::MqttClient {
        mqtt_create_options,
        mqtt_connect_options,
        subscriptions,
        msg_buffer_limit: 10,
    })
    .unwrap();

    out_client.connect().await;
    out_client
}
