-- Add down migration script here

DROP TABLE IF EXISTS user_audit_logs;
DROP TABLE IF EXISTS user_identities;
DROP TABLE IF EXISTS user_profiles;
DROP TABLE IF EXISTS users;

DROP TYPE IF EXISTS user_audit_action;
DROP TYPE IF EXISTS auth_provider;
DROP TYPE IF EXISTS user_role;
