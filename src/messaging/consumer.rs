use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message, OwnedMessage,
    producer::{FutureProducer, FutureRecord},
};
use rdkafka::message::BorrowedMessage;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::Result;

/// Consumer loop
/// A dedicated pool of workers handles the processing, asynchronous offset commiting and Dead
/// Letter Queue (DLQ) routing.
pub async fn run_consumer(brokers: &str) -> Result<()> {
    // Initialize Consumer and DLQ Producer.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootrap.servers", brokers)
        .set("group.id", "landlord-service")
        .set("enable.auto.commit", "false") // Manual commit for reliability
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000") // 45s to prevent accidental rebalances
        .set("max.poll.interval.ms", "300000") // 5m max window to process a batch
        .set("fetch.min.bytes", "1048576") // 1MB batching for high throughput
        .set("fetch.wait.max.ms", "50") // Wait up to 50ms to hit 1MB target
        .create()?;

    // Thread safe and optimized for async DLQ forwading.
    let dlq_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;

    let consumer = Arc::new(consumer);
    let dlq_producer = Arc::new(dlq_producer);

    consumer.subscribe(&[
        "orgs.topic.registration",
        "orgs_topic.org_login",
        "user_topic.registration",
        "user_topic.user_login",
        "orgs_topic.update",
        "orgs_topic.remove",
        "user_topic.update",
        "user_topic.remove",
    ])?;

    // Create a bounded channel to decouple fetching from processing.
    // Backpressure: If workers fall behind, the channel fills up and stalls the fetch loop.
    let (tx, rx) = mpsc::channel::<OwnedMessage>(1000);

    // Spawn a pool of concurrent worker tasks
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut worker_rx = rx;
    tokio::spawn(start_worker_pool(
            num_workers, worker_rx,
            consumer.clone(), dlq_producer.clone(),
    ));

    tracing::info!("Landlord Redpanda consumer started");

    // High performance, non blocking event fetch loop
    loop {
        match consumer.recv().await {
            Ok(message) => {
                // Convert BorrowedMessage to OwnedMessage to pass across threads safely.
                let owned_msg = message.detach();

                // Send to workers. If channel is full, this blocks (creating backpressure)
                if let Err(_) = tx.send(&message).await {
                    tracing::error!("Worker channel closed. Exiting fetch loop");
                    break;
                }

            }
            Err(error) => {
                tracing::error!(?error,"Redpanda fetch error");
                // Prevent CPU spinning on broker failing
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Ok(())
}

async fn start_worker_pool(
    mut num_workers: usize,
    mut rx: mpsc::Receiver<OwnedMessage>,
    consumer: Arc<StreamConsumer>,
    dlq_producer: Arc<FutureProducer>,
) {
    // Read from shared channel queue.
    while let Some(message) = rx.recv().await {
        let consumer_clone = consumer.clone();
        let dlq_clone = dlq_producer.clone();

        // Process each message concurrently
        tokio::spawn(async move {
            match process_message(&message).await {
                Ok(_) => {
                    // Idempotent DB write succeeded . Commit offset asynchronously
                    if let Err(e) = consumer_clone.commit_message(&message, CommitMode::Async) {
                        tracing::warn!(?e, "Failed to commit offset");
                    }
                },
                Err(err) => {
                    tracing::error!(?err, "Processing failed. Routing to DLQ.");
                    // Route to DLQ topic based on original topic name.
                    let dlq_topic = format!("{}.failed", message.topic());
                    let payload = message.payload().unwrap_or(&[]);

                    let record = FutureRecord::to(&dlq_topic)
                        .payload(payload)
                        .key(message.key().unwrap_or(&[]));

                    match dlq_clone.send(record, Duration::from_secs(3)).await {
                        Ok(_) => {
                            // Routed to DLQ safely. We can now commit broker offset.
                            let _ = consumer_clone.commit_message(&message, CommitMode::Async);
                        },
                        Err((send_err, _)) => {
                            tracing::critical!(?send_err, "DLQ shipment failed! Message is stuck.");
                        }
                    }
                }
            }
        });
    }
}

// The business logic. E.g the action to perform. if CRUD Operation etc.
async fn process_message(message: &OwnedMessage) -> anyhow::Result<()> {
    // Example of Idempotent SQL
    // sqlx::query!("INSERT INTO user (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING", id,
    // name);
    Ok(())
}
