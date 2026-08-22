//! Program entrypoint.
#![allow(warnings)]
mod helpers;
mod repository;
mod messaging;
mod service;
#[path="./config.rs"]
mod config;
#[path="./monitoring.rs"]
mod monitoring;

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
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the environment variables from .env
    let _ = dotenvy::dotenv();

    // Initialize the logger subscriber to print to standard output
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    tracing::info!("Application booting up...");

    // Infrastructure setup and initialization
    let config = Config::from_env().map_err(|e| {
        tracing::error!("Configuration Loading Failed: {:?}", e);
        e
    })?;
    let db = create_pool(&config.database_url).await?;
    let consumer = create_rp_consumer(&config.redpanda_brokers)?;
    let producer = create_rp_producer(&config.redpanda_brokers)?;

    // Synchronization and lifecycle components
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let shutdown = CancellationToken::new();

    // Spawn long lived tasks. Clone before moving.
    let consumer_shutdown = shutdown.clone();
    let dlq_producer = producer.clone();
    // Redpanda consumer. No .await since we have removed async move entirely. Tokio will handle .await
    let mut consumer_task = tokio::spawn(consumer::run(tx, consumer_shutdown, consumer, dlq_producer));

    // Start workers. No .await since we have removed async move block. Tokio will handle .await
    let worker_shutdown = shutdown.clone();
    let db_clone = db.clone();
    let producer_clone = producer.clone();
    let mut worker_task = tokio::spawn(worker::run(rx, db_clone, producer_clone, worker_shutdown));

    // Monitoring/observability with actix web.
    let http_host = config.http_host.clone();
    let http_port = config.http_port;
    let mut monitoring_task = tokio::spawn(async move {
        monitoring::run(http_host, http_port).await
    });

    // Supervisor or Error selector.
    tokio::select! {
        result = &mut consumer_task => {
            tracing::error!(?result, "Consumer exited");
        }

        result = &mut worker_task => {
            tracing::error!(?result, "Worker exited");
        }

        result = &mut monitoring_task => {
            tracing::error!(?result, "Monitoring exited");
        }

        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown requested");
        }
    }

    // Explicit graceful shutdown sequence.
    tracing::info!("Broadcasting cancel signal to all tasks...");
    shutdown.cancel();

    // Join Guard: Wait for tasks to drain and finish clean up
    // Set a timeout so a stuck worker won't hand the container forever.
    let shutdown_timeout = std::time::Duration::from_secs(15);
    let _ = tokio::time::timeout(shutdown_timeout, async {
        // If the selector already consumed a task, it will be skipped.
        // Otherwise, wait for it.
        if !consumer_task.is_finished() {
            let _ = consumer_task.await;
        }
        
        if !worker_task.is_finished() {
            let _ = worker_task.await;
        }

        if !monitoring_task.is_finished() {
            let _ = monitoring_task.await;
        }

    }).await;

    tracing::info!("All systems terminated cleanly");
    Ok(())
}
