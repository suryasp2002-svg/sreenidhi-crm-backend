-- Migration: Remove name and driver_name columns from storage_units
-- Safe to run multiple times
DO $$
BEGIN
  -- Drop driver_name if present
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='storage_units' AND column_name='driver_name'
  ) THEN
    EXECUTE 'ALTER TABLE public.storage_units DROP COLUMN IF EXISTS driver_name';
  END IF;
  -- Drop name if present
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='storage_units' AND column_name='name'
  ) THEN
    EXECUTE 'ALTER TABLE public.storage_units DROP COLUMN IF EXISTS name';
  END IF;
END
$$;
