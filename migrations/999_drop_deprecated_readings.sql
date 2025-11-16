-- 999_drop_deprecated_readings.sql
-- Drop deprecated tables: truck_dispenser_day_readings and dispenser_readings

BEGIN;

-- Drop the legacy per-day authoritative table (deprecated in favor of dispenser_day_reading_logs)
DROP TABLE IF EXISTS public.truck_dispenser_day_readings CASCADE;

-- Drop legacy dispenser readings table (use truck_dispenser_meter_snapshots instead)
DROP TABLE IF EXISTS public.dispenser_readings CASCADE;

COMMIT;
