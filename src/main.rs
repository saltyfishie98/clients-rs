#[allow(dead_code)]
mod client;

use client::topic;
use paho_mqtt::MQTT_VERSION_5;
use std::fs::File;
use std::io::{BufReader, Write};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), paho_mqtt::Error> {
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

            let msg_json: serde_json::Value = serde_json::from_slice(msg.payload()).unwrap();

            log::info!(
                "Received message\ntopic: {}, \npayload: {:#?}\n",
                msg.topic(),
                msg_json
            );

            // use serde_json::json;
            // let json = serde_json::from_slice::<serde_json::Value>(msg.payload()).unwrap();

            // if let serde_json::Value::Object(data) = json {
            //     let mut keys = Vec::<&String>::new();
            //     let mut values = Vec::<&serde_json::Value>::new();

            //     data.iter().for_each(|(k, v)| {
            //         keys.push(k);
            //         values.push(v)
            //     });

            //     let out = json!({
            //         "keys": keys,
            //         "values": values,
            //         "timestamp": chrono::Utc::now().to_string()
            //     });

            //     mqtt_client.publish(paho_mqtt::Message::new(
            //         "saltyfishie/echo",
            //         serde_json::to_string_pretty(&out).unwrap(),
            //         1,
            //     ));
            // }
        }
    }
}

async fn make_db_client() -> client::DatabaseClient {
    client::DatabaseClient::connect().await
}

async fn make_mqtt_client() -> client::MqttClient {
    let config: client::SetupConfig = {
        let source_dir = std::env::current_dir().unwrap();
        let config_file_path = source_dir.join("config").join("client.json");

        log::info!(
            "Configuration: \"{}\"",
            config_file_path.to_str().unwrap_or("{unknown}")
        );

        let file = match File::open(config_file_path) {
            Ok(f) => f,
            Err(e) => {
                println!("Error opening config file: {}", e);
                std::process::exit(1);
            }
        };

        let config_reader = BufReader::new(file);
        serde_json::from_reader(config_reader).expect("Failed to deserialize JSON")
    };

    let mqtt_create_options = paho_mqtt::CreateOptionsBuilder::new()
        .server_uri(&config.broker_uri)
        .client_id(&config.client_id)
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

        config.subscriptions.into_iter().for_each(|topic| {
            let opts = match topic.options {
                Some(o) => o.into(),
                None => paho_mqtt::SubscribeOptions::default(),
            };
            s.add(topic.topic.as_str(), topic.qos, opts);
        });

        s.finalize()
    };

    let out_client = client::MqttClient::new(client::MqttClientConfig {
        mqtt_create_options,
        mqtt_connect_options,
        subscriptions,
        msg_buffer_limit: 100,
    })
    .unwrap();

    out_client.connect().await;
    out_client
}
