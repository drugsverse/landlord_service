use std::env;
use std::time::Duration;
use sqlx::{PgPool, postgres::PgPoolOptions};

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub http_host: String,
    pub http_port: u16,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")?,
            redpanda_brokers: env::var("REDPANDA_BROKERS")?,
            http_host: env::var("LANDLORD_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            http_port: env::var("LANDLORD_PORT")
                .unwarp_or_else(|_| "8081".into())
                .parse()?,
        })
    }
}

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .min_connections(5)
        .acqure_timeout(Duration::from_secs(3))
        .idle_timeout(Duration::from_mins(10))
        .max_lifetime(Duration::from_mins(30))
        .test_before_acquire(true) // Reliability and performance tuning
        .connect(database_url)
        .await?;
    Ok(pool)
}
