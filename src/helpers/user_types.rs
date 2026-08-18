//! Types for users.
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "user_members.user_role")]
pub enum UserRole { User, Moderator, Admin }

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "user_members.auth_provider")]
pub enum AuthProvider {
    Local, Google, Apple
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "user_members.user_audit_action")]
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
    pub provider_id: String, // Google/Apple ID, email for Local provider.
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

/// DB Query response objects.
///
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserLoginCredentials {
    pub user_id: Uuid,
    pub username: String,
    pub role: UserRole,
    pub locale: String,
    pub is_active: bool,
    pub password_hash: Option<String>,
    pub email_verified_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct UserProfileView {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub telephone: String,
    pub about: Option<String>,
    pub avatar_url: Option<String>,
    pub country: String,
    pub timezone: String,
    pub locale: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectedIdentityView {
    pub identity_id: Uuid,
    pub provider: AuthProvider,
    pub provider_id: String,
    pub email_verified_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct AuditLogView {
    pub log_id: Uuid,
    pub actor_id: Uuid,
    pub action: UserAuditAction,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub access_timestamp: chrono::DateTime<chrono::Utc>,
}

// Handles optional filtering and pagination rules.
pub struct AuditLogQueryFilters {
    pub action: Option<String>, // Filter by action, e.g PasswordChange
    pub before_timestamp: Option<DateTime<Utc>>, // Cursor for next page
    pub limit: i64, // Page size
}

#[derive(Debug, Deserialize)]
pub struct MasterUserFilters {
    pub search_term: Option<String>, // Search agains username or email
    pub role: Option<UserRole>,
    pub is_active: Option<bool>,
    pub show_deleted: Option<bool>, // Toggle to see soft deleted users
    pub page: i64, // Standard page offset. e.g 1, 2, 3
    pub page_size: i64, // Items per page e.g 20
}

#[derive(Debug, Serialize)]
pub struct PaginatedUserResponse {
    pub users: Vec<UserProfileView>,
    pub total_count: i64,
    pub total_pages: i64,
    pub current_page: i64,
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
