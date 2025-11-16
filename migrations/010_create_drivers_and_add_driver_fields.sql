-- 010_create_drivers_and_add_driver_fields.sql
-- Create drivers table and add optional driver fields to daily reading tables

BEGIN;

-- Drivers master table
CREATE TABLE IF NOT EXISTS public.drivers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    phone TEXT NULL,
    driver_id TEXT NOT NULL UNIQUE, -- business identifier shown in UI
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Driver info attached to daily readings (optional text fields)
ALTER TABLE public.truck_dispenser_day_readings
    ADD COLUMN IF NOT EXISTS driver_name TEXT NULL,
    ADD COLUMN IF NOT EXISTS driver_code TEXT NULL;

ALTER TABLE public.truck_odometer_day_readings
    ADD COLUMN IF NOT EXISTS driver_name TEXT NULL,
    ADD COLUMN IF NOT EXISTS driver_code TEXT NULL;

COMMIT;
