-- 998_drop_fuel_lot_activities.sql
-- Drop legacy fuel_lot_activities table and dependent objects

BEGIN;

DROP TABLE IF EXISTS public.fuel_lot_activities CASCADE;

COMMIT;
