use futures::stream::StreamExt;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use serde_json::json;
use std::{process, time::Duration};

async fn poll_subscription(
    strm: &mut mqtt::AsyncReceiver<Option<mqtt::Message>>,
) -> Option<mqtt::Message> {
    strm.next().await?
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

    let mut msg_stream = client.get_stream(25);

    while let Err(e) = client.connect(conn_opts.clone()).await {
        eprintln!("{}", e);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let topic = "saltyfishie";
    println!("Subscribing to topic: {}", topic);

    while (tokio::time::timeout(Duration::from_secs(1), client.subscribe(topic, 1)).await).is_err()
    {
        eprintln!("Topic subsciption timeout!");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    println!("Waiting for messages...");

    loop {
        if let Some(msg) = poll_subscription(&mut msg_stream).await {
            if msg.retained() {
                print!("(R) ");
            }
            println!("\n{}", msg);

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
            println!("Lost connection. Attempting reconnect.");
            while let Err(err) = client.reconnect().await {
                println!("Error reconnecting: {}", err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            println!("Reconnected!");
        }
    }
}
