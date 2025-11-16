-- 009_create_reminder_email_selected_audit.sql
BEGIN;

CREATE TABLE IF NOT EXISTS reminder_email_selected_audit (
  id BIGSERIAL PRIMARY KEY,
  operation_id UUID NOT NULL,
  reminder_id VARCHAR(20) NOT NULL,
  performed_by_user_id UUID NULL REFERENCES public.users(id) ON DELETE SET NULL,
  performed_by TEXT NULL,
  performed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  subject TEXT NULL,
  to_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
  cc_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
  bcc_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
  recipients_dedup JSONB NOT NULL DEFAULT '[]'::jsonb,
  sent_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  message_id TEXT NULL,
  error TEXT NULL,
  meta JSONB NULL
);

CREATE INDEX IF NOT EXISTS idx_resa_reminder_performed_at ON reminder_email_selected_audit(reminder_id, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_resa_operation_id ON reminder_email_selected_audit(operation_id);
CREATE INDEX IF NOT EXISTS idx_resa_status_sent_partial ON reminder_email_selected_audit(reminder_id) WHERE status = 'SENT';

COMMIT;
