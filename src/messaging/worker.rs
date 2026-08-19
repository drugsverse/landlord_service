//! Publishing events to redpanda topics
use anyhow::Result;
use sqlx::PgPool;
use tracing;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use rdkafka::{
    producer::FutureProducer,
};

use crate::service;
use crate::helpers::user_types::{
    UserJob, UserEvent, UserLoginCredentials,
};

pub async fn run(
    mut rx: mpsc::Receiver<UserJob>,
    db: PgPool,
    producer: FutureProducer,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            Some(job) = rx.recv() => {
                let result = match job.event {
                    UserEvent::Registration(event) => {
                        service::user::register_user(
                            &db, &producer, event
                        ).await
                    }

                    UserEvent::Login(event) => {
                        todo!();
                        //service::user::login_user(
                        //    &db, &producer, event
                        //).await
                    }
                };

                match result {
                    Ok(()) => {
                        // Tell the consumer that this job
                        // was successfully processed.
                    }
                    Err(error) => {
                        tracing::error!(
                            ?error, "Job processing failed"
                        );
                        // Do NOT acknowledge the message here.
                        // Retry/DLQ strategy goes here.
                    }
                }
            }
            _ = shutdown.cancelled() => {
                break;
            }
        }
    }
}
