use std::env;
use anyhow::Result;
use std::time::Duration;
use sqlx::{PgPool, postgres::PgPoolOptions};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};

use crate::messaging::topics::auth_topics;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub db_encryption_key: String,
    pub redpanda_brokers: String,
    pub http_host: String,
    pub http_port: u16,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")?,
            db_encryption_key: env::var("DB_ENCRYPTION_KEY")?,
            redpanda_brokers: env::var("REDPANDA_BROKERS")?,
            http_host: env::var("LANDLORD_HOST")
                .unwrap_or_else(|_| "0.0.0.0".into()),
            http_port: env::var("LANDLORD_PORT")
                .unwrap_or_else(|_| "8081".into())
                .parse()?,
        })
    }
}

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .idle_timeout(Duration::from_secs(10 * 60))
        .max_lifetime(Duration::from_secs(30 * 60))
        .test_before_acquire(true) // Reliability and performance tuning
        .connect(database_url)
        .await?;
    Ok(pool)
}

pub fn create_rp_consumer(rp_brokers: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", rp_brokers)
        .set("group.id", "landlord-service")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.offset.store", "false")
        .create()?;

    // Topics to listen to.
    consumer.subscribe(&[
        auth_topics::USER_REGISTRATION,
        auth_topics::USER_LOGIN,
    ])?;

    Ok(consumer)
}

pub fn create_rp_producer(rp_brokers: &str) -> Result<FutureProducer> {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", rp_brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .create()?;

    Ok(producer)
}
