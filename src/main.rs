use futures::stream::StreamExt;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use std::{env, process, time::Duration};

#[tokio::main]
async fn main() -> Result<(), mqtt::Error> {
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    println!("Connecting to the MQTT server at '{}'...", host);

    let mut client = {
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .client_id("rust_async_sub_v5_3")
            .finalize();

        mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
            println!("Error creating the client: {:?}", e);
            process::exit(1);
        })
    };

    let conn_opts = {
        let lwt = mqtt::Message::new(
            "test/lwt",
            "[LWT] Async subscriber v5 lost connection",
            mqtt::QOS_1,
        );
        mqtt::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
            .clean_start(false)
            .properties(mqtt::properties![mqtt::PropertyCode::SessionExpiryInterval => 3600])
            .will_message(lwt)
            .finalize()
    };

    let mut strm = client.get_stream(25);
    client.connect(conn_opts).await?;

    let topic = "test/#";
    client.subscribe(topic, 1).await?;
    println!("Subscribed to topic: {}", topic);
    println!("Waiting for messages...");

    while let Some(msg_opt) = strm.next().await {
        if let Some(msg) = msg_opt {
            if msg.retained() {
                print!("(R) ");
            }
            println!("{}", msg);
        } else {
            println!("Lost connection. Attempting reconnect.");
            while let Err(err) = client.reconnect().await {
                println!("Error reconnecting: {}", err);

                async_std::task::sleep(Duration::from_millis(1000)).await;
            }
        }
    }

    Ok(())
}
