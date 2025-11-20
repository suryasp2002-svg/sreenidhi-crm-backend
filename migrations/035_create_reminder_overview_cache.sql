-- Phase 3: Persistent summary cache table for reminders overview extended
CREATE TABLE IF NOT EXISTS reminder_overview_cache (
  user_id UUID PRIMARY KEY,
  data JSONB NOT NULL,
  generated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_reminder_overview_cache_generated_at ON reminder_overview_cache(generated_at);
