-- Migration 004: user_permissions table for per-employee tab & action toggles
CREATE TABLE IF NOT EXISTS user_permissions (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  tabs JSONB NOT NULL DEFAULT '{}'::jsonb,  -- e.g. {"Opportunities": true, "Customers": false}
  actions JSONB NOT NULL DEFAULT '{}'::jsonb, -- e.g. {"Opportunities.create": true, "Opportunities.edit": false}
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION touch_user_permissions() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_touch_user_permissions'
  ) THEN
    CREATE TRIGGER trg_touch_user_permissions BEFORE UPDATE ON user_permissions
      FOR EACH ROW EXECUTE FUNCTION touch_user_permissions();
  END IF;
END $$;