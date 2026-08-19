//! Contains business logic or processes
use anyhow::Result;
use sqlx::PgPool;
use rdkafka::producer::FutureProducer;

use crate::{
    helpers::user_types::{UserCreatedEvent},
    repository::users::{save_new_user_tx},
};

pub async fn register_user(
    db: &PgPool, producer: &FutureProducer, event: UserCreatedEvent
) ->Result<()> {
    // We will call the function to persist user to db here.
    println!("User creation event is {:#?}", event);
    Ok(())
}
