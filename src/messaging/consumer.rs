//! Receives event and sends them to message channel
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use rdkafka::consumer::StreamConsumer;
use tracing;

use crate::helpers::user_types::{
    UserJob
};

pub async fn run(
    tx: mpsc::Sender<UserJob>,
    shutdown: CancellationToken,
    consumer: StreamConsumer,
) -> Result<()> {
    consumer.subscribe(&["user.registration", "user.login"])?;

    tracing::info!("Redpanda consumer started");
    loop {
        tokio::select! {
            message = consumer.recv() => {
                let message = message?;
                let topic = message.topic();
                let payload = message.payload().ok_or_else(|| anyhow::anyhow!("Message has no payload"))?;

                let event = match topic {
                    "user.registration" => {
                        let event: RegistrationEvent = serde_json::from_slice(payload)?;
                        UserJob::Registration(event)
                    }
                    "user.login" => {
                        let event: LoginEvent = serde_json::from_slice(payload)?;
                        UserJob::Login(event)
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

                // Consumer has received the message, but we haven't acknowledged or committed it
                // yet
                tx.send(job).await?;
                // In real implementation, you need to coordinate the Kafka offset with successful
                // processing.
            }
            
            _ = shutdown.cancelled() => {
                tracing::info!("Redpanda consumer shutting down");
                break;
            }
        }
    }

    Ok(())
}
