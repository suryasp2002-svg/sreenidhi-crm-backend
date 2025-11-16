-- 027_add_load_time_to_fuel_lots.sql
ALTER TABLE public.fuel_lots
  ADD COLUMN IF NOT EXISTS load_time TIMESTAMP WITHOUT TIME ZONE NULL;

COMMENT ON COLUMN public.fuel_lots.load_time IS 'Original purchase time (date + time), not the row creation time.';
