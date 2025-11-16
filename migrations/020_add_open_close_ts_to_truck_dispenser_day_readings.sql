-- 020_add_open_close_ts_to_truck_dispenser_day_readings.sql
-- Adds opening_at and closing_at timestamps to truck_dispenser_day_readings for precise sale windows

BEGIN;

ALTER TABLE public.truck_dispenser_day_readings
  ADD COLUMN IF NOT EXISTS opening_at TIMESTAMP WITHOUT TIME ZONE NULL,
  ADD COLUMN IF NOT EXISTS closing_at TIMESTAMP WITHOUT TIME ZONE NULL;

-- Optional indexes to help time-bounded lookups later
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_td_day_opening_at'
  ) THEN
    CREATE INDEX idx_td_day_opening_at ON public.truck_dispenser_day_readings(opening_at);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_td_day_closing_at'
  ) THEN
    CREATE INDEX idx_td_day_closing_at ON public.truck_dispenser_day_readings(closing_at);
  END IF;
END $$;

COMMIT;
