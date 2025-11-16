-- 008_create_reminders_audit_v2.sql
BEGIN;

CREATE TABLE IF NOT EXISTS reminders_audit_v2 (
  id BIGSERIAL PRIMARY KEY,
  reminder_id VARCHAR(20) NOT NULL,
  version INTEGER NOT NULL,
  action TEXT NOT NULL,
  performed_by_user_id UUID NULL REFERENCES public.users(id) ON DELETE SET NULL,
  performed_by TEXT NULL,
  performed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  diff JSONB NULL,
  snapshot JSONB NULL,
  note TEXT NULL,
  context JSONB NULL,
  CONSTRAINT reminders_audit_v2_unique_per_reminder_version UNIQUE(reminder_id, version)
);

CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_reminder_version ON reminders_audit_v2(reminder_id, version);
CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_performed_at ON reminders_audit_v2(performed_at DESC);
-- Optional JSONB indexes for ad-hoc queries
-- CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_diff_gin ON reminders_audit_v2 USING GIN (diff);
-- CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_snapshot_gin ON reminders_audit_v2 USING GIN (snapshot);

COMMIT;
