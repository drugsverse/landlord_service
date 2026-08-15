//! USERS DATA PERSISTANCE.
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
use crate::helpers::user_types::{
    UserAuditAction, AuthProvider, UserCreatedEvent,
};
// UserAuditAction, AuthProvider, UserCreatedEvent
fn encrypt_field(cleartext: &str) -> Vec<u8> {
    cleartext.as_bytes().to_vec()
}

pub async fn save_new_user_tx(
    pool: &PgPool,
    event: UserCreatedEvent,
    password_hash: String
) -> Result<(), sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    sqlx::query!(
        r#"
            INSERT INTO users
                (id, username, role) 
            VALUES
                ($1, $2, 'User'::user_role)
        "#,
        event.user_id, event.username,
    ).execute(&mut *tx).await?;
    // Encrypting cleartext strings from Redpanda to BYTEA.
    let encrypted_first = encrypt_field(&event.first_name);
    let encrypted_last = encrypt_field(&event.last_name);
    let encrypted_phone = encrypt_field(&event.telephone);

    // Inserting User profile.
    sqlx::query!(
        r#"
            INSERT INTO user_profiles
                (user_id, email, first_name, last_name, telephone, country)
            VALUES
                ($1, $2, $3, $4, $5, $6)
        "#,
        event.user_id, event.email, encrypted_first,
        encrypted_last, encrypted_phone, event.country
    ).execute(&mut *tx).await?;

    // Inserting user identity.
    // created_at, updated_at,
    let identity_id = Uuid::now_v7();
    sqlx::query!(
        r#"
            INSERT INTO user_identities
                (id, user_id, provider, provider_id, password_hash)
            VALUES
                ($1, $2, $3::auth_provider, $4, $5)
        "#,
        identity_id,
        event.user_id,
        event.provider as AuthProvider,
        event.email, // Email is the provider_id for Local auth
        password_hash
    ).execute(&mut *tx).await?;

    // Initializing the user logs.
    let audit_id = Uuid::now_v7();
    sqlx::query!(
        r#"
            INSERT INTO user_audit_logs
                (id, user_id, actor_id, action)
            VALUES
                ($1, $2, $3, $4)
        "#,
        audit_id, event.user_id, event.user_id,
        event.action as UserAuditAction
    ).execute(&mut *tx).await?;

    // Commit the transaction atomically across all 4 tables.
    tx.commit().await?;
    Ok(())
}
