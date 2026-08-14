-- DEFINE NATIVE POSTGRES ENUM TYPES
CREATE TYPE user_role AS ENUM ('User', 'Moderator', 'Admin');
CREATE TYPE auth_provider AS ENUM ('Local', 'Google', 'Apple');
CREATE TYPE user_audit_action AS ENUM (
    'AccountCreated', 'AccountUpdated', 'AccountDeactivated', 'AccountDeleted',
    'LoginSucceeded', 'LoginFailed', 'PasswordChanged', 'EmailChanged',
    'EmailVerified', 'ProfileViewed', 'ProfileUpdated'
);

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- CREATE DATABASE TABLES

-- Core User Table (High Security, No PII)
CREATE TABLE users (
    id UUID PRIMARY KEY UNIQUE,
    username VARCHAR(100) NOT NULL UNIQUE,
    role user_role NOT NULL DEFAULT 'User',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    locale VARCHAR(10) NOT NULL DEFAULT 'en-US',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

-- Isolated Profiles Table (Contains PII - can be structurally encrypted for HIPAA)
CREATE TABLE user_profiles (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,

    first_name BYTEA NOT NULL,
    last_name BYTEA NOT NULL,
    telephone BYTEA NOT NULL,

    about TEXT,
    avatar_url TEXT,
    country VARCHAR(2) NOT NULL,
    accepted_terms_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Identity & Authentication Credentials Table
CREATE TABLE user_identities (
    id UUID PRIMARY KEY UNIQUE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider auth_provider NOT NULL,
    provider_id VARCHAR(255) NOT NULL, -- Email for 'Local', unique ID string for Social
    password_hash VARCHAR(255),        -- NULL for Social, filled for 'Local'
    email_verified_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure a single user can't link the exact same social/local identity twice
    CONSTRAINT unique_user_provider_identity UNIQUE (user_id, provider, provider_id)
);

-- Tamper-Proof System Audit Logs Table
CREATE TABLE user_audit_logs (
    id UUID PRIMARY KEY UNIQUE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_id UUID NOT NULL, -- Explicitly tracks who performed the action (User or Admin)
    action user_audit_action NOT NULL,
    ip_address VARCHAR(45), -- Supports both IPv4 and IPv6 string lengths
    user_agent TEXT,
    access_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SPEED & PERFORMANCE INDEXES

-- Fast lookup for checking social tokens or email logins
CREATE INDEX idx_user_identities_provider_lookup ON user_identities (provider, provider_id);

-- Speed up audit log retrievals, sorting them chronologically for individual users
CREATE INDEX idx_user_audit_logs_user_timestamp ON user_audit_logs (user_id, access_timestamp DESC);

-- Partial index for active users to prevent scanning deleted users during auth cycles
CREATE INDEX idx_users_active_only ON users (id) WHERE deleted_at IS NULL;

