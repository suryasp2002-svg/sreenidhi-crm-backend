-- 023_add_open_close_ts_to_truck_odometer_day_readings.sql
-- Adds opening_at and closing_at timestamps to truck_odometer_day_readings

BEGIN;

ALTER TABLE public.truck_odometer_day_readings
  ADD COLUMN IF NOT EXISTS opening_at TIMESTAMP WITHOUT TIME ZONE NULL,
  ADD COLUMN IF NOT EXISTS closing_at TIMESTAMP WITHOUT TIME ZONE NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_to_day_opening_at'
  ) THEN
    CREATE INDEX idx_to_day_opening_at ON public.truck_odometer_day_readings(opening_at);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_to_day_closing_at'
  ) THEN
    CREATE INDEX idx_to_day_closing_at ON public.truck_odometer_day_readings(closing_at);
  END IF;
END $$;

COMMIT;
