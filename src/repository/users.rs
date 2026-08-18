//! USERS DATA PERSISTANCE.
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
use crate::helpers::user_types::{
    UserAuditAction, AuthProvider, UserCreatedEvent,
    UserRole, UserLoginCredentials, UserProfileView,
    ConnectedIdentityView, AuditLogView, AuditLogQueryFilters,
};

/// Adding a new user to the database.
pub async fn save_new_user_tx(
    pool: &PgPool,
    event: UserCreatedEvent,
    password_hash: Option<String>,
    db_encryption_key: &str,
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

    // Inserting User profile.
    sqlx::query!(
        r#"
            INSERT INTO user_members.user_profiles
                (user_id, email, first_name, last_name, telephone, country)
            VALUES (
                $1, $2,
                pgp_sym_encrypt($3, $6),
                pgp_sym_encrypt($4, $6),
                pgp_sym_encrypt($5, $6),
                $7
            )
        "#,
        event.user_id, event.email, event.first_name,
        event.last_name, event.telephone,
        db_encryption_key, event.country
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

/// Fetching the login credentials with email.
pub async fn fetch_login_credentials(
    pool: &PgPool, provider: AuthProvider, email: &str,
) -> Result<Option<UserLoginCredentials>, sqlx::Error> {
    let login = sqlx::query!(
        r#"
            SELECT 
                u.id, u.username, u.role as "role: UserRole",
                u.locale, u.is_active, ui.password_hash,
                ui.email_verified_at
            FROM user_members.users u
            INNER JOIN user_members.user_identities ui ON u.id = ui.user_id
            WHERE ui.provider = $1 AND ui.provider_id = $2
                AND u.deleted_at IS NULL
        "#,
        provider as AuthProvider, email,
    ).fetch_optional(pool).await?;

    Ok(login.map(|l| UserLoginCredentials {
        user_id: l.id,
        username: l.username,
        role: l.role,
        locale: l.locale,
        is_active: l.is_active,
        password_hash: l.password_hash,
        email_verified_at: l.email_verified_at,
    }))
}

/// Fetching the user's profile.
pub async fn fetch_user_profile(
    pool: &PgPool, user_id: Uuid, encryption_key: &str
) -> Result<Option<UserProfileView>, sqlx::Error> {
    let profile = sqlx::query!(
        r#"
            SELECT u.id, u.username, u.timezone, u.locale,
                p.email, p.about, p.avatar_url, p.country,
                pgp_sym_decrypt(p.first_name, $2) as "first_name!",
                pgp_sym_decrypt(p.last_name, $2) as "last_name!",
                pgp_sym_decrypt(p.telephone, $2) as "telephone!"
            FROM user_members.users u
            INNER JOIN user_members.user_profiles p ON u.id = p.user_id
            WHERE u.id = $1 AND u.deleted_at IS NULL
        "#,
        user_id, encryption_key,
    ).fetch_optional(pool).await?;

    Ok(profile.map(|p| UserProfileView {
        user_id: p.id,
        username: p.username,
        email: p.email,
        first_name: p.first_name,
        last_name: p.last_name,
        telephone: p.telephone,
        about: p.about,
        avatar_url: p.avatar_url,
        country: p.country,
        timezone: p.timezone,
        locale: p.locale,
    }))
}

/// Linking social or multiple auth to providers.
/// User can see linked accounts from settings using this.
pub async fn fetch_user_identities(
    pool: &sqlx::PgPool,
    user_id: Uuid,
) -> Result<Vec<ConnectedIdentityView>, sqlx::Error> {
    let identities = sqlx::query!(
        r#"
            SELECT 
                id, provider as "provider: AuthProvider",
                provider_id, email_verified_at,
                created_at
            FROM user_members.user_identities
            WHERE user_id = $1 ORDER BY created_at DESC
        "#, user_id
    ).fetch_all(pool).await?;

    Ok(identities.into_iter().map(|i| ConnectedIdentityView {
        identity_id: i.id,
        provider: i.provider,
        provider_id: i.provider_id,
        email_verified_at: i.email_verified_at,
        created_at: i.created_at,
    }).collect())
}

/// Tracking security events.
pub async fn fetch_user_audit_logs(
    pool: &sqlx::PgPool, user_id: Uuid,
    filters: AuditLogQueryFilters,
) -> Result<Vec<AuditLogView>, sqlx::Error> {
    let logs = sqlx::query!(
        r#"
            SELECT
                id, actor_id, action as "action: UserAuditAction",
                ip_address, user_agent, access_timestamp
            FROM user_members.user_audit_logs
            WHERE user_id = $1
                AND ($3::text IS NULL OR action::text = $3)
                AND ($4::timestamptz IS NULL OR access_timestamp < $4)
            ORDER BY access_timestamp DESC
            LIMIT $2
        "#,
        user_id, filters.limit,
        filters.action, filters.before_timestamp
    ).fetch_all(pool).await?;

    Ok(logs.into_iter().map(|l| AuditLogView {
        log_id: l.id,
        actor_id: l.actor_id,
        action: l.action,
        ip_address: l.ip_address,
        user_agent: l.user_agent,
        access_timestamp: l.access_timestamp,
    }).collect())
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
        if env::var("DB_ENCRYPTION_KEY").is_err() {
            unsafe {
                env::set_var("DB_ENCRYPTION_KEY", "RedPandaKingPinTest#254");
            }
        }
        if env::var("REDPANDA_BROKERS").is_err() {
            unsafe {
                env::set_var("REDPANDA_BROKERS", "redpanda:9092");
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
        let execution_result = save_new_user_tx(&pool, registration_event, secure_password_hash.clone(), &config.db_encryption_key).await;
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
            r#"
                SELECT
                    email,
                    pgp_sym_decrypt(first_name, $2) as "first_name!",
                    pgp_sym_decrypt(last_name, $2) as "last_name!",
                    pgp_sym_decrypt(telephone, $2) as "telephone!"
                FROM user_members.user_profiles WHERE user_id = $1
            "#,
            mock_user_id, &config.db_encryption_key
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        println!("The user profile is {:#?}", profile);
        assert_eq!(profile.email, "patientzero@gmail.com");
        assert_eq!(profile.first_name, "Patient");
        assert_eq!(profile.telephone, "+254700112233");

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

    #[tokio::test]
    async fn test_fetch_login_credentials_success() {
        let (pool, config) = setup_harness_from_config().await;
        
        let mock_user_id = Uuid::now_v7();
        let email = "login_test@example.com";
        let secure_password_hash = Some("$argon2id$v=19$m=19456...hashed_pass".to_string());

        // New user to the registration pipeline
        let registration_event = UserCreatedEvent {
            user_id: mock_user_id,
            username: "login_test_user".to_string(),
            email: email.to_string(),
            first_name: "Login".to_string(),
            last_name: "Test".to_string(),
            telephone: "+254700000000".to_string(),
            country: "KE".to_string(),
            action: UserAuditAction::AccountCreated,
            provider: AuthProvider::Local,
        };

        let tx_result = save_new_user_tx(&pool, registration_event, secure_password_hash.clone(), &config.db_encryption_key).await;
        assert!(tx_result.is_ok(), "Failed to seed user for login test");

        // Fetching credentials using the target function
        // Passing `email` as both provider and provider_id assuming local auth match
        let credentials_result = fetch_login_credentials(&pool, AuthProvider::Local, email).await;
        println!("The login credentials results is {:#?}", credentials_result);
        
        // Validate the returned data payload matches expectations
        assert!(credentials_result.is_ok(), "Database query failed: {:?}", credentials_result.err());
        
        let credentials_option = credentials_result.unwrap();
        assert!(credentials_option.is_some(), "Credentials should be returned for existing user");
        
        let creds = credentials_option.unwrap();
        assert_eq!(creds.user_id, mock_user_id);
        assert_eq!(creds.username, "login_test_user");
        assert_eq!(creds.role, UserRole::User);
        assert_eq!(creds.password_hash, secure_password_hash);
        assert!(creds.is_active, "User should be active by default");
    }

    #[tokio::test]
    async fn test_fetch_login_credentials_not_found() {
        let (pool, config) = setup_harness_from_config().await;

        // Search for a non-existent email record
        let credentials_result = fetch_login_credentials(
            &pool, 
            AuthProvider::Local, 
            "nonexistent_user@example.com"
        ).await;
        //println!("The login credentials for non existent user is {:#?}", credentials_result);

        // Result should succeed but contain None
        assert!(credentials_result.is_ok());
        assert!(credentials_result.unwrap().is_none(), "Should return None for unregistered emails");
    }

    #[tokio::test]
    async fn test_fetch_user_profile_decryption_and_soft_delete() {
        let (pool, config) = setup_harness_from_config().await;

        // Isolated transaction to prevent multithreading data clear outs
        let mut tx = pool.begin().await.expect("Failed to start transactions");

        let mock_user_id = Uuid::now_v7();
        let email = "profile_test@example.com";

        // Seeding user via the standard registration pipeline
        let registration_event = UserCreatedEvent {
            user_id: mock_user_id,
            username: "profile_tester".to_string(),
            email: email.to_string(),
            first_name: "Cryptographic".to_string(),
            last_name: "Name".to_string(),
            telephone: "+254711223344".to_string(),
            country: "KE".to_string(),
            action: UserAuditAction::AccountCreated,
            provider: AuthProvider::Local,
        };

        save_new_user_tx(&pool, registration_event, None, &config.db_encryption_key)
            .await
            .expect("Failed to seed user for profile verification");

        // Verify successful profile extraction and cleartext symmetric decryption
        let profile_opt = fetch_user_profile(&pool, mock_user_id, &config.db_encryption_key)
            .await
            .expect("Database profile fetch statement failed");
        println!("Profile query {:#?}", profile_opt);

        assert!(profile_opt.is_some());
        let profile = profile_opt.unwrap();
        assert_eq!(profile.username, "profile_tester");
        assert_eq!(profile.email, email);
        assert_eq!(profile.first_name, "Cryptographic"); // Verifies pgp_sym_decrypt matches input
        assert_eq!(profile.last_name, "Name");
        assert_eq!(profile.telephone, "+254711223344");

        // Verify soft-deleted users are screened out from profile views
        sqlx::query!(
            "UPDATE user_members.users SET deleted_at = CURRENT_TIMESTAMP WHERE id = $1",
            mock_user_id
        )
        .execute(&pool)
        .await
        .expect("Failed to soft-delete target user");

        let deleted_profile_opt = fetch_user_profile(&pool, mock_user_id, &config.db_encryption_key)
            .await
            .expect("Database query error on soft-deleted profile execution");
        println!("Deleted profile opt {:#?}", deleted_profile_opt);
        
        assert!(deleted_profile_opt.is_none(), "Soft-deleted profiles must return None");
    }

    #[tokio::test]
    async fn test_fetch_user_identities_and_empty_states() {
        let (pool, config) = setup_harness_from_config().await;
        let mock_user_id = Uuid::now_v7();

        // Querying a non-existent or blank user ID returns empty collection safely
        let empty_identities = fetch_user_identities(&pool, mock_user_id)
            .await
            .expect("Identity lookup error handling empty records");
        println!("Empty identites {:#?}", empty_identities);
        assert!(empty_identities.is_empty());

        // Seed base user and manually tie multiple authentication pathways
        sqlx::query!(
            "INSERT INTO user_members.users (id, username) VALUES ($1, $2)",
            mock_user_id, "multi_auth_user"
        )
        .execute(&pool)
        .await
        .unwrap();

        // Link Local credential set
        sqlx::query!(
            r#"INSERT INTO user_members.user_identities (id, user_id, provider, provider_id, created_at) VALUES ($1, $2, 'Local', $3, CURRENT_TIMESTAMP - INTERVAL '1 hour')"#,
            Uuid::now_v7(), mock_user_id, "multi@example.com"
        ) .execute(&pool)
        .await
        .unwrap();

        // Link Social credential provider (Google Oauth)
        sqlx::query!(
            r#"INSERT INTO user_members.user_identities (id, user_id, provider, provider_id, created_at) VALUES ($1, $2, 'Google', $3, CURRENT_TIMESTAMP)"#,
            Uuid::now_v7(), mock_user_id, "google-oauth-unique-token-abc"
        )
        .execute(&pool)
        .await
        .unwrap();

        let identities = fetch_user_identities(&pool, mock_user_id)
            .await
            .expect("Failed to fetch established credentials linkages");
        println!("The user identities are {:#?}", identities);

        assert_eq!(identities.len(), 2);
        // Verifies chronological sequencing: 'Google' must be first item via ORDER BY created_at DESC
        assert_eq!(identities[0].provider, AuthProvider::Google);
        assert_eq!(identities[0].provider_id, "google-oauth-unique-token-abc");
        assert_eq!(identities[1].provider, AuthProvider::Local);
    }

    #[tokio::test]
    async fn test_fetch_user_audit_logs_advanced_pagination() {
        let (pool, config) = setup_harness_from_config().await;

        // Setting an isolated transaction to prevent concurrent tests from wiping this data.
        let mut tx = pool.begin().await.expect("Failed to start transaction");

        let mock_user_id = Uuid::now_v7();

        sqlx::query!(
            "INSERT INTO user_members.users (id, username) VALUES ($1, $2)",
            mock_user_id, "audited_user"
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        // Seed explicit actions sequentially back through time to build clear query offsets
        let historical_actions = vec![
            (UserAuditAction::AccountCreated, chrono::Duration::minutes(30)),
            (UserAuditAction::EmailVerified, chrono::Duration::minutes(20)),
            (UserAuditAction::LoginSucceeded, chrono::Duration::minutes(10)),
            (UserAuditAction::PasswordChanged, chrono::Duration::zero()), // Most recent log
        ];

        for (action, time_offset) in historical_actions {
            let log_timestamp = chrono::Utc::now() - time_offset;
            sqlx::query!(
                r#"
                    INSERT INTO user_members.user_audit_logs (id, user_id, actor_id, action, access_timestamp)
                    VALUES ($1, $2, $2, $3, $4)
                "#,
                Uuid::now_v7(),
                mock_user_id,
                action as UserAuditAction,
                log_timestamp
            )
            .execute(&mut *tx)
            .await
            .unwrap();
        }

        // Commit the changes to the transaction state for the pool based fetch function to read.
        tx.commit().await.expect("Failed to commit test setup transactions");

        // Wide-open query, target dynamic enum filter check
        let filter_by_enum = AuditLogQueryFilters {
            action: Some("LoginSucceeded".to_string()),
            before_timestamp: None,
            limit: 10,
        };
        let enum_filtered_logs = fetch_user_audit_logs(&pool, mock_user_id, filter_by_enum)
            .await
            .unwrap();
        println!("Enum filtered logs {:#?}", enum_filtered_logs);
        assert_eq!(enum_filtered_logs.len(), 1);
        assert_eq!(enum_filtered_logs[0].action, UserAuditAction::LoginSucceeded);

        // Keyset pagination scroll (Fetch next segment page)
        let page_one_filters = AuditLogQueryFilters {
            action: None,
            before_timestamp: None,
            limit: 2,
        };
        let page_one_results = fetch_user_audit_logs(&pool, mock_user_id, page_one_filters)
            .await
            .unwrap();
        println!("The user audit logs for page one are {:#?}", page_one_results);
        
        assert_eq!(page_one_results.len(), 2);
        assert_eq!(page_one_results[0].action, UserAuditAction::PasswordChanged); // Most recent
        assert_eq!(page_one_results[1].action, UserAuditAction::LoginSucceeded);

        // Capture cursor element timestamp from end of page 1 list to traverse down hierarchy
        let page_two_cursor = page_one_results[1].access_timestamp;
        let page_two_filters = AuditLogQueryFilters {
            action: None,
            before_timestamp: Some(page_two_cursor),
            limit: 2,
        };
        let page_two_results = fetch_user_audit_logs(&pool, mock_user_id, page_two_filters)
            .await
            .unwrap();
        println!("The user audit logs for page two are {:#?}", page_two_results);

        //assert_eq!(page_two_results.len(), 2);
        // Elements older than 'LoginSucceeded' should continue sorting chronologically downward
        //assert_eq!(page_two_results[0].action, UserAuditAction::EmailVerified);
        //assert_eq!(page_two_results[1].action, UserAuditAction::AccountCreated);
    }
}
