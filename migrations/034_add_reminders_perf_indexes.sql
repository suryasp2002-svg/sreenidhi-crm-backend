-- Phase 2: Performance indexes for reminders summary queries
-- Run outside transaction (migrate.js detects CONCURRENTLY)

-- Using regular CREATE INDEX (non-concurrent) for compatibility with transaction wrapper
CREATE INDEX IF NOT EXISTS idx_reminders_status ON reminders(status);
CREATE INDEX IF NOT EXISTS idx_reminders_created_due_status ON reminders(created_by_user_id, due_ts, status);
CREATE INDEX IF NOT EXISTS idx_reminders_assigned_due_status ON reminders(assigned_to_user_id, due_ts, status);
-- Partial index focusing on pending reminders (most frequently counted windows)
CREATE INDEX IF NOT EXISTS idx_reminders_pending_due ON reminders(due_ts) WHERE status='PENDING';
