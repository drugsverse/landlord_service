//! Program entrypoint.
#![allow(warnings)]
mod helpers;
mod repository;
mod messaging;
mod service;
#[path="./config.rs"]
mod config;

use config::{
    Config, create_pool,
    create_rp_consumer,
    create_rp_producer,
};
use messaging::{consumer, worker};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // Infrastructure.
    let config = Config::from_env()?;
    let db = create_pool(&config.database_url).await?;
    let consumer = create_rp_consumer(&config.redpanda_brokers)?;
    let producer = create_rp_producer(&config.redpanda_brokers)?;

    // Communication channel
    let (tx, rx) = tokio::sync::mpsc::channel(1024);

    // Lifecycle
    let shutdown = CancellationToken::new();

    // Spawn long lived components.
    let consumer_task = tokio::spawn(
        consumer::run(tx, shutdown.clone(), consumer)
    );

    let worker_task = tokio::spawn(
        worker::run(rx, db.clone(), producer.clone(), shutdown.clone())
    );

    // Monitoring/observability.
    //let monitoring_task = tokio::spawn(
    //    //monitoring::run()
    //);

    // Supervisor.
    tokio::select! {
        result = consumer_task => {
            tracing::error!(?result, "Consumer exited");
        }

        result = worker_task => {
            tracing::error!(?result, "Worker exited");
        }

        //result = monitoring_task => {
        //    tracing::error!(?result, "Monitoring exited");
        //}

        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown requested");
        }
    }

    // Graceful shutdown.
    shutdown.cancel();
    Ok(())
}
