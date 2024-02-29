pub struct MysqlClient {
    connection_pool: sqlx::MySqlPool,
}

impl MysqlClient {
    pub async fn connect() -> Self {
        let connection_pool =
            match sqlx::MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap()).await {
                Ok(p) => {
                    log::info!(
                        "Connected to database '{}'",
                        p.connect_options().get_database().unwrap()
                    );
                    p
                }
                Err(e) => {
                    log::error!("Database Error: {}", e);
                    std::process::exit(1);
                }
            };

        Self { connection_pool }
    }

    pub async fn connect_with(options: sqlx::mysql::MySqlConnectOptions) -> Self {
        let connection_pool = match sqlx::MySqlPool::connect_with(options).await {
            Ok(p) => {
                log::info!(
                    "Connected to database '{}'!",
                    p.connect_options().get_database().unwrap()
                );
                p
            }
            Err(e) => {
                log::error!("Database Error: {}", e);
                std::process::exit(1);
            }
        };

        Self { connection_pool }
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
