-- 997_drop_truck_odometer_readings.sql
-- Drop deprecated truck_odometer_readings and truck_daily_readings (if present)

BEGIN;

DROP TABLE IF EXISTS public.truck_odometer_readings CASCADE;
DROP TABLE IF EXISTS public.truck_daily_readings CASCADE;

COMMIT;
