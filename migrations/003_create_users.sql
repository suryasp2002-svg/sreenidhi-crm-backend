-- Migration 003: Create users table for auth Phase 1
-- Requires pgcrypto or uuid-ossp for gen_random_uuid(). Enable extension if needed.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    full_name TEXT,
    role TEXT NOT NULL CHECK (role IN ('OWNER','ADMIN','EMPLOYEE')),
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP WITHOUT TIME ZONE,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);

-- Partial unique index ensures only one active OWNER at a time
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uniq_active_owner'
    ) THEN
        BEGIN
            CREATE UNIQUE INDEX uniq_active_owner ON users ((role)) WHERE role = 'OWNER' AND active = TRUE;
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;
