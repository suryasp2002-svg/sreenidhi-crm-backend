-- 026_add_trip_to_fuel_sale_transfers.sql
-- Adds a nullable integer column `trip` to tag each sale/transfer with the trip number

BEGIN;

ALTER TABLE IF EXISTS fuel_sale_transfers
ADD COLUMN IF NOT EXISTS trip integer;

COMMENT ON COLUMN fuel_sale_transfers.trip IS 'Trip number for which this sale/transfer was recorded (e.g., 1, 2, 3 …).';

COMMIT;
