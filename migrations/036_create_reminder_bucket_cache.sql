-- Phase 4: Bucket preclassification cache (dynamic refresh via server job)
CREATE TABLE IF NOT EXISTS reminder_bucket_cache (
  user_id UUID NOT NULL,
  bucket TEXT NOT NULL, -- DELAYED, TODAY, TOMORROW
  status TEXT NOT NULL, -- PENDING, DONE, SENT, FAILED
  count INT NOT NULL DEFAULT 0,
  generated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY(user_id, bucket, status)
);
CREATE INDEX IF NOT EXISTS idx_reminder_bucket_cache_generated_at ON reminder_bucket_cache(generated_at);
