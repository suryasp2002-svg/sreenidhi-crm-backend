-- 016_add_activity_to_transfer_tables.sql
-- Add 'activity' column to transfer tables and backfill values based on unit types

BEGIN;

-- Add column for internal transfers
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='activity'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN activity TEXT NULL';
  END IF;
END $$;

-- Add column for sale transfers
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='activity'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_sale_transfers ADD COLUMN activity TEXT NULL';
  END IF;
END $$;

-- Backfill internal activities: DATUM destination -> TANKER_TO_DATUM; else TANKER_TO_TANKER
UPDATE public.fuel_internal_transfers fit
SET activity = COALESCE(activity,
  CASE WHEN su_to.unit_type = 'DATUM' THEN 'TANKER_TO_DATUM' ELSE 'TANKER_TO_TANKER' END
)
FROM public.storage_units su_to
WHERE fit.to_unit_id = su_to.id;

-- Backfill sale activities: DATUM source -> DATUM_TO_VEHICLE; else TANKER_TO_VEHICLE
UPDATE public.fuel_sale_transfers fst
SET activity = COALESCE(activity,
  CASE WHEN su_from.unit_type = 'DATUM' THEN 'DATUM_TO_VEHICLE' ELSE 'TANKER_TO_VEHICLE' END
)
FROM public.storage_units su_from
WHERE fst.from_unit_id = su_from.id;

-- Ensure NOT NULL after backfill
ALTER TABLE public.fuel_internal_transfers ALTER COLUMN activity SET NOT NULL;
ALTER TABLE public.fuel_sale_transfers ALTER COLUMN activity SET NOT NULL;

COMMIT;
