//! Receives event and sends them to message channel
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use rdkafka::{
    consumer::StreamConsumer,
    Message,
};
use tracing;

use crate::helpers::user_types::{
    UserJob, UserCreatedEvent, LoginIdentityEvent,
    UserEvent,
};
use crate::messaging::topics::{
    auth_topics,
};

pub async fn run(
    tx: mpsc::Sender<UserJob>,
    shutdown: CancellationToken,
    consumer: StreamConsumer,
) -> Result<()> {
    //consumer.subscribe(&["user.registration", "user.login"])?;

    tracing::info!("Redpanda consumer started");
    loop {
        tokio::select! {
            message = consumer.recv() => {
                let message = message?;
                let topic = message.topic();
                let payload = message.payload().ok_or_else(|| anyhow::anyhow!("Message has no payload"))?;

                let event = match topic {
                    auth_topics::USER_REGISTRATION => {
                        let event: UserCreatedEvent = serde_json::from_slice(payload)?;
                        UserEvent::Registration(event)
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
