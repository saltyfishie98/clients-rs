pub struct DatabaseClient {
    connection_pool: sqlx::MySqlPool,
}

impl DatabaseClient {
    pub async fn connect() -> Self {
        let connection_pool =
            match sqlx::MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap()).await {
                Ok(p) => {
                    log::info!("Connected to database");
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
    match e {
        sqlx::Error::Database(err) => {
            if let Some(code) = err.code() {
                match code.to_ascii_lowercase().as_str() {
                    "3d000" => {
                        log::error!("Database Error: {}", err.message());
                    }
                    _ => {}
                }
            } else {
                log::error!("{}", err);
            }
            std::process::exit(1);
        }
        _ => {}
    };
}
