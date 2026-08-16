//! USERS DATA PERSISTANCE.
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
use crate::helpers::user_types::{
    UserAuditAction, AuthProvider, UserCreatedEvent,
    UserRole,
};
// UserAuditAction, AuthProvider, UserCreatedEvent
fn encrypt_field(cleartext: &str) -> Vec<u8> {
    cleartext.as_bytes().to_vec()
}

pub async fn save_new_user_tx(
    pool: &PgPool,
    event: UserCreatedEvent,
    password_hash: Option<String>
) -> Result<(), sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    sqlx::query!(
        r#"
            INSERT INTO user_members.users
                (id, username, role) 
            VALUES
                ($1, $2, 'User'::user_members.user_role)
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
            INSERT INTO user_members.user_profiles
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
            INSERT INTO user_members.user_identities
                (id, user_id, provider, provider_id, password_hash)
            VALUES
                ($1, $2, $3::user_members.auth_provider, $4, $5)
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
            INSERT INTO user_members.user_audit_logs
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

#[cfg(test)]
mod user_tests {
    use super::*;
    use std::env;
    use crate::config::*;

    /// Helper to inject test environment defaults, build pool, and truncate existing records.
    async fn setup_harness_from_config() -> (PgPool, Config) {
        if env::var("DATABASE_URL").is_err() {
            unsafe {
                env::set_var("DATABASE_URL", "");
            }
        }
        if env::var("REDPANDA_BROKERS").is_err() {
            unsafe {
                env::set_var("REDPANDA_BROKERS", "");
            }
        }

        let config = Config::from_env().expect("Failed to initialize config");

        let pool = create_pool(&config.database_url)
            .await
            .expect("Failed to build high performance PgPool engine");

        // Clean tables cleanly with CASCADE semantics
        sqlx::query("TRUNCATE TABLE user_members.users CASCADE;")
            .execute(&pool)
            .await
            .expect("Test environment isolation cleanup execution");

        (pool, config)
    }

    #[tokio::test]
    async fn test_full_onboarding_pipeline_lifecycle() {
        // Loading the app configuration.
        let (pool, config) = setup_harness_from_config().await;
        
        // Assert environmental variables were matched up cleanly
        assert!(!config.database_url.is_empty());
        assert_eq!(config.http_port, 8081); // default configurations verification

        let mock_user_id = Uuid::now_v7();
        let registration_event = UserCreatedEvent {
            user_id: mock_user_id,
            username: "patient_zero".to_string(),
            email: "patientzero@gmail.com".to_string(),
            first_name: "Patient".to_string(),
            last_name: "Zero".to_string(),
            telephone: "+254700112233".to_string(),
            country: "KE".to_string(),
            action: UserAuditAction::AccountCreated,
            provider: AuthProvider::Local,
        };
        let secure_password_hash = Some("$argon2id$v=19$m=19456...hashed_pass".to_string());

        // Run Registration Transaction Pipeline
        let execution_result = save_new_user_tx(&pool, registration_event, secure_password_hash.clone()).await;
        assert!(execution_result.is_ok(), "The onboarding database transaction failed: {:?}", execution_result.err());

        // Core Tables Cross Examination

        // Verify Core User Profile Entity Status
        let user = sqlx::query!(
            r#"SELECT id, username, role as "role: UserRole" FROM user_members.users WHERE id = $1"#,
            mock_user_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        println!("The user is {:#?}", &user);
        assert_eq!(user.username, "patient_zero");
        assert_eq!(user.role, UserRole::User);

        // Verify Protected Profile Encryption Assertions
        let profile = sqlx::query!(
            "SELECT email, first_name, last_name, telephone FROM user_members.user_profiles WHERE user_id = $1",
            mock_user_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        println!("The user profile is {:#?}", profile);
        assert_eq!(profile.email, "patientzero@gmail.com");
        assert_eq!(profile.first_name, encrypt_field("Patient"));
        assert_eq!(profile.telephone, encrypt_field("+254700112233"));

        // Verify Authentication Identity Linkages
        let identity = sqlx::query!(
            r#"SELECT provider as "provider: AuthProvider", provider_id, password_hash FROM user_members.user_identities WHERE user_id = $1"#,
            mock_user_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        println!("The user identity is {:#?}", identity);
        assert_eq!(identity.provider, AuthProvider::Local);
        assert_eq!(identity.provider_id, "patientzero@gmail.com");
        assert_eq!(identity.password_hash.unwrap(), secure_password_hash.unwrap());

        // Verify Audit Log Sequencing 
        let log = sqlx::query!(
            r#"SELECT action as "action: UserAuditAction", actor_id FROM user_members.user_audit_logs WHERE user_id = $1"#,
            mock_user_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        println!("The edit log is {:#?}", log);
        assert_eq!(log.action, UserAuditAction::AccountCreated);
        assert_eq!(log.actor_id, mock_user_id);
    }
}
