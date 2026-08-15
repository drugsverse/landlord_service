//! Types for users.
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "user_role")]
pub enum UserRole { User, Moderator, Admin }

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "auth_provider")]
pub enum AuthProvider {
    Local, Google, Apple
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "user_audit_action")]
pub enum UserAuditAction {
    AccountCreated, AccountUpdated, AccountDeactivated,
    AccountDeleted, LoginSucceeded, LoginFailed,
    PasswordChanged, EmailChanged, EmailVerified,
    ProfileViewed, ProfileUpdated,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbUser {
    pub id: Uuid,
    pub username: String,
    pub role: UserRole,
    pub is_active: bool,
    pub timezone: String,
    pub locale: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbUserProfile {
    pub user_id: Uuid,
    pub email: String,
    // Kept as Vec<u8> to avoid conversions during raw db reads
    pub first_name: Vec<u8>,
    pub last_name: Vec<u8>,
    pub telephone: Vec<u8>,
    pub about: Option<String>,
    pub avatar_url: Option<String>,
    pub country: String,
    pub accepted_terms_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbUserIdentity {
    pub id: Uuid,
    pub user_id: Uuid,
    pub provider: AuthProvider,
    pub provider_id: String,
    pub password_hash: Option<String>,
    pub email_verified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbUserAuditLog {
    pub id: Uuid,
    pub user_id: Uuid,
    pub actor_id: Uuid,
    pub action: UserAuditAction,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub access_timestamp: DateTime<Utc>,
}

/// REDPANDA EVENT PAYLOADS
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuditLogEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub actor_id: Uuid,
    pub action: UserAuditAction,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub access_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum UserEventPayload {
    Created(UserCreatedEvent),
    ProfileUpdated { user_id: Uuid, country: String },
    AuditLogged(UserAuditLogEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCreatedEvent {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    //pub role: UserRole,
    // Here we have clear text strings for the streaming ecosystem, decrypted before sending.
    pub first_name: String,
    pub last_name: String,
    pub telephone: String,
    pub country: String,
    pub action: UserAuditAction,
    pub provider: AuthProvider,
}
