//! Receives event and sends them to message channel
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use rdkafka::{
    consumer::{StreamConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    message::{OwnedHeaders, BorrowedMessage},
    Message,
};
use tracing;

use crate::helpers::user_types::{
    UserJob, UserRegistrationEvent, LoginIdentityEvent,
    UserEvent,
};
use crate::messaging::topics::{
    auth_topics,
};

pub async fn run(
    tx: mpsc::Sender<UserJob>,
    shutdown: CancellationToken,
    consumer: StreamConsumer,
    producer: FutureProducer,
) -> Result<()> {
    tracing::info!("Redpanda consumer for drugsverse landlord started");

    loop {
        tokio::select! {
            message = consumer.recv() => {
                let message = message?;
                let topic = message.topic();

                // Extracting raw payload bytes safely.
                let payload = match message.payload() {
                    Some(p) => p,
                    None => {
                        tracing::warn!("Message in topic {} has not payload. Skipping.", topic);
                        let _ = consumer.store_offset(message.topic(), message.partition(), message.offset()); // Acknowledge empty message
                        continue;
                    }
                };

                let event = match topic {
                    auth_topics::USER_REGISTRATION => {
                        match serde_json::from_slice::<UserRegistrationEvent>(payload) {
                            Ok(parsed_payload) => UserEvent::Registration(parsed_payload),
                            Err(e) => {
                                tracing::warn!("Malformed payload on {}. Routing to DLQ. Error: {}", topic, e);
                                // Allocate owned copies of data to the heap.
                                // To provide 'static data for the background process
                                let owned_payload = payload.to_vec();
                                let owned_key = message.key().unwrap_or(&[]).to_vec();
                                let error_string = e.to_string();
                                let target_topic = "user.registration.dlq".to_string();
                                let producer_clone = producer.clone();

                                // Fire and forget async publish do it doesn't lag the core
                                // consumer loop thread via spawning it in a different worker as we
                                // continue processing messages.
                                tokio::spawn(async move {
                                    // Async DLQ ROUTING ROUTINE.
                                    let dlq_record = FutureRecord::to(&target_topic)
                                        .key(&owned_key)
                                        .payload(&owned_payload)
                                        .headers(
                                            OwnedHeaders::new().insert(
                                                rdkafka::message::Header {
                                                    key: "x-dead-letter-reason",
                                                    value: Some(&error_string),
                                                }
                                            )
                                        );
                                    if let Err((err, _)) = producer_clone.send(dlq_record, tokio::time::Duration::from_secs(3)).await {
                                        tracing::error!("CRITICAL: Failed to write DLQ topic! Error: {:?}", err);
                                    }
                                });
                                // Tell redpanda to commit or store offset for this toxic message.
                                // If we skip this, redpanda will re-send this bad payload forever
                                // on boot.
                                let _ = consumer.store_offset(message.topic(), message.partition(), message.offset());
                                continue;
                            }
                        }
                    }

                    auth_topics::USER_LOGIN => {
                        let event: LoginIdentityEvent = serde_json::from_slice(payload)?;
                        UserEvent::Login(event)
                    }

                    _ => {
                        tracing::warn!(topic, "Unknown topic");
                        continue;
                    }
                };

                let job = UserJob {
                    topic: message.topic().to_string(),
                    partition: message.partition(),
                    offset: message.offset(),
                    event,
                };

                // Forward successfully parsed job to the worker pipeline
                tx.send(job).await?;
                // Coordinate manual offset storage after pushing to the channel queue
                let _ = consumer.store_offset(message.topic(), message.partition(), message.offset());
            }
            
            _ = shutdown.cancelled() => {
                tracing::info!("Redpanda consumer shutting down");
                break;
            }
        }
    }

    Ok(())
}
