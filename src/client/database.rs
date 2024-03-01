use super::setup_config;

#[derive(Debug)]
pub enum DbClientError {
    MqttPayload(serde_json::Error),
    Unsupported,
}

impl std::fmt::Display for DbClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbClientError::MqttPayload(e) => write!(f, "{}", e),
            DbClientError::Unsupported => write!(f, "unsupported"),
        }
    }
}

pub struct MysqlClientConfig {
    pub connect_options: sqlx::mysql::MySqlConnectOptions,
    pub topic_table_map: setup_config::sql_server::TopicTableMapping,
}

pub struct MysqlClient {
    connection_pool: sqlx::MySqlPool,
    topic_table_map: setup_config::sql_server::TopicTableMapping,
}

impl MysqlClient {
    pub async fn start(config: MysqlClientConfig) -> Self {
        let connection_pool = match sqlx::MySqlPool::connect_with(config.connect_options).await {
            Ok(p) => {
                log::info!(
                    "Connected to database '{}'!",
                    p.connect_options().get_database().unwrap()
                );
                p
            }
            Err(e) => {
                log::error!("MysqlClient start error: {}", e);
                std::process::exit(1);
            }
        };

        Self {
            connection_pool,
            topic_table_map: config.topic_table_map,
        }
    }

    pub async fn query_table(&self) {
        let a = "select * from user";
        match sqlx::query(a).fetch_all(&self.connection_pool).await {
            Ok(data) => {
                log::info!("Response: [");
                data.iter().for_each(|r| {
                    println!("    {:?},", r);
                });
                println!("]");
            }
            Err(e) => handle_query_error(e),
        };
    }

    pub async fn push(&self, topic: &str, payload: &str) -> Result<(), DbClientError> {
        let payload_any: serde_json::Value = match serde_json::from_str(payload) {
            Ok(o) => o,
            Err(e) => return Err(DbClientError::MqttPayload(e)),
        };

        if let serde_json::Value::Object(data) = payload_any {
            let mut builder: sqlx::QueryBuilder<sqlx::MySql> =
                sqlx::QueryBuilder::new(format_args!("INSERT INTO `{}`", topic).to_string());

            let mut col_name = data.keys().fold(String::new(), |acc, key| {
                if !acc.is_empty() {
                    acc + ", " + key
                } else {
                    acc + key
                }
            });
            col_name = format!("({}) VALUES (", col_name);
            builder.push(col_name);

            let mut insert = builder.separated(", ");
            for (_, v) in &data {
                match v {
                    serde_json::Value::Bool(o) => {
                        insert.push_bind(o);
                    }
                    serde_json::Value::Number(o) => {
                        insert.push_bind(o.as_i64());
                    }
                    serde_json::Value::String(o) => {
                        insert.push_bind(o);
                    }
                    _ => return Err(DbClientError::Unsupported),
                }
            }
            insert.push_unseparated(")");

            let query = builder.build();
            query.execute(&self.connection_pool).await.unwrap();

            Ok(())
        } else {
            Err(DbClientError::Unsupported)
        }
    }
}

fn handle_query_error(e: sqlx::Error) {
    if let sqlx::Error::Database(err) = e {
        if let Some(code) = err.code() {
            if let "3d000" = code.to_ascii_lowercase().as_str() {
                log::error!("Database Error: {}", err.message());
            }
        } else {
            log::error!("{}", err);
        }
        std::process::exit(1);
    };
}
