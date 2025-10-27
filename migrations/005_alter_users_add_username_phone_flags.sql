-- Migration 005: Extend users table with username, phone, and password change flags
-- Adds:
--  - username TEXT UNIQUE (with case-insensitive unique index on LOWER(username))
--  - phone TEXT
--  - must_change_password BOOLEAN DEFAULT FALSE
--  - last_password_change_at TIMESTAMP
-- Idempotent: checks column existence before altering.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='username'
  ) THEN
    EXECUTE 'ALTER TABLE users ADD COLUMN username TEXT UNIQUE';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='phone'
  ) THEN
    EXECUTE 'ALTER TABLE users ADD COLUMN phone TEXT';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='must_change_password'
  ) THEN
    EXECUTE 'ALTER TABLE users ADD COLUMN must_change_password BOOLEAN NOT NULL DEFAULT FALSE';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='last_password_change_at'
  ) THEN
    EXECUTE 'ALTER TABLE users ADD COLUMN last_password_change_at TIMESTAMP WITHOUT TIME ZONE';
  END IF;
END $$;

-- Case-insensitive unique index on LOWER(username) if not exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uniq_users_username_lower'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX uniq_users_username_lower ON users ((LOWER(username))) WHERE username IS NOT NULL';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
END $$;
