use client::deserialized;
use saltyfishie_clients as client;
use std::fs::File;
use std::io::{BufReader, Write};

#[tokio::main]
async fn main() -> Result<(), paho_mqtt::Error> {
    setup_logger();
    dotenv::dotenv().ok();

    run().await
}

async fn run() -> Result<(), paho_mqtt::Error> {
    // let (db_client, mut mqtt_client) = tokio::join!(make_db_client(), make_mqtt_client());
    let (mut mqtt_client,) = tokio::join!(make_mqtt_client());

    // loop {
    if let Some(msg) = mqtt_client.poll().await {
        if msg.retained() {
            print!("(R) ");
        }

        log::info!(
            "Received message\ntopic: {}, \npayload: {:#?}\n",
            msg.topic(),
            serde_json::from_slice::<serde_json::Value>(msg.payload()).unwrap()
        );

        // if let Err(err) = db_client
        //     .push(msg.topic(), std::str::from_utf8(msg.payload()).unwrap())
        //     .await
        // {
        //     log::error!("db push error: {}", err)
        // };
    }
    Ok(())
    // }
}

fn setup_logger() {
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
}

async fn make_mqtt_client() -> client::MqttClient {
    let source_dir = std::env::current_dir().unwrap();
    let config_file_path = source_dir.join("configs").join("mqtt_connection.json");

    let file = File::open(config_file_path).unwrap();
    let config_reader = BufReader::new(file);
    let config: deserialized::MqttClientConfig =
        serde_json::from_reader(config_reader).expect("Failed to deserialize JSON");

    dbg!(&config);

    client::MqttClient::start(config.into()).await
}

// async fn make_db_client() -> client::MysqlClient {
//     use client::{
//         setup_config::{self, SqlServerSetupConfig},
//         MysqlClientConfig,
//     };

//     let config: setup_config::SqlServerSetupConfig = {
//         let source_dir = std::env::current_dir().unwrap();
//         let config_file_path = source_dir.join("configs").join("db_connection.json");

//         log::info!(
//             "Database client configuration: \"{}\"",
//             config_file_path.to_str().unwrap_or("{unknown}")
//         );

//         match SqlServerSetupConfig::try_from(config_file_path) {
//             Ok(f) => f,
//             Err(e) => {
//                 println!("Error opening config file: {}", e);
//                 std::process::exit(1);
//             }
//         }
//     };

//     let mut db_opts = sqlx::mysql::MySqlConnectOptions::new()
//         .host(&config.host)
//         .username(&config.username)
//         .database(&config.database);

//     match &config.password {
//         Some(password) => {
//             db_opts = db_opts.password(password);
//         }
//         None => {
//             let password = match std::env::var("DB_PASSWORD") {
//                 Ok(o) => o,
//                 Err(_) => "".to_string(),
//             };
//             db_opts = db_opts.password(&password);
//         }
//     }

//     if let Some(port) = &config.port {
//         db_opts = db_opts.port(*port);
//     }

//     client::MysqlClient::start(MysqlClientConfig {
//         connect_options: db_opts,
//         topic_table_map: config.topic_table_map,
//     })
//     .await
// }
