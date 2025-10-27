-- Migration: create targets table for upcoming campaign targets
CREATE TABLE IF NOT EXISTS targets (
  id               VARCHAR(24) PRIMARY KEY,
  client_name      TEXT NOT NULL,
  notes            TEXT,
  status           TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DONE')),
  created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Helpful indexes for list queries
CREATE INDEX IF NOT EXISTS idx_targets_status ON targets(status);
CREATE INDEX IF NOT EXISTS idx_targets_updated_at ON targets(updated_at DESC);
