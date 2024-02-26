use futures::stream::StreamExt;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use serde_json::json;
use std::{process, time::Duration};

async fn poll_subscription(
    mut strm: mqtt::AsyncReceiver<Option<mqtt::Message>>,
) -> Option<mqtt::Message> {
    match strm.next().await {
        Some(msg_opt) => msg_opt,
        None => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), mqtt::Error> {
    env_logger::init();

    let host = "mqtt://broker.emqx.io:1883";
    println!("Connecting to the MQTT server at '{}'...", host);

    let mut client = {
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .client_id("saltyfishie_6")
            .finalize();

        mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
            println!("Error creating the client: {:?}", e);
            process::exit(1);
        })
    };

    let conn_opts = {
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

    let strm = client.get_stream(25);
    client.connect(conn_opts).await?;

    let topic = "saltyfishie";
    client.subscribe(topic, 1).await?;
    println!("Subscribed to topic: {}", topic);
    println!("Waiting for messages...");

    loop {
        if let Some(msg) = poll_subscription(strm.clone()).await {
            if msg.retained() {
                print!("(R) ");
            }
            println!("{}", msg);

            let json = serde_json::from_slice::<serde_json::Value>(msg.payload()).unwrap();

            if let serde_json::Value::Object(data) = json {
                let keys: Vec<String> = data.iter().map(|(k, _)| k.clone()).collect();
                let values: Vec<serde_json::Value> = data.iter().map(|(_, v)| v.clone()).collect();

                let mut out = json!({
                    "keys": keys,
                    "values": values,
                });

                out.as_object_mut().unwrap().insert(
                    "timestamp".into(),
                    serde_json::Value::String(chrono::Utc::now().to_string()),
                );

                client.publish(mqtt::Message::new(
                    "saltyfishie/echo",
                    serde_json::to_string_pretty(&out).unwrap(),
                    1,
                ));
            }
        } else {
            println!("Lost connection. Attempting reconnect.");
            while let Err(err) = client.reconnect().await {
                println!("Error reconnecting: {}", err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            println!("Reconnected!");
        }
    }
}
