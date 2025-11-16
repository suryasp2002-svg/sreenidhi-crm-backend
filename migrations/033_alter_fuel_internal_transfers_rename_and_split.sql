-- 033_alter_fuel_internal_transfers_rename_and_split.sql
-- Alter table in-place using RENAME/ADD/DROP, similar to fuel_lots approach.
-- Renames:
--   transfer_volume_liters -> transfer_volume
--   from_lot_code_after    -> from_lot_code_change
--   to_lot_code_after      -> to_lot_code_change
-- Splits performed_at into transfer_date (DATE) + transfer_time (TIME)
-- Drops legacy columns: driver_id, performed_by_user_id, performed_at
-- Rebuilds helpful indexes to use (transfer_date, transfer_time)

BEGIN;

-- 1) Renames (idempotent)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_volume_liters'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_volume'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers RENAME COLUMN transfer_volume_liters TO transfer_volume';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='from_lot_code_after'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='from_lot_code_change'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers RENAME COLUMN from_lot_code_after TO from_lot_code_change';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='to_lot_code_after'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='to_lot_code_change'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers RENAME COLUMN to_lot_code_after TO to_lot_code_change';
  END IF;
END $$;

-- 2) Add split columns if missing and backfill from performed_at
ALTER TABLE public.fuel_internal_transfers
  ADD COLUMN IF NOT EXISTS transfer_date DATE;
ALTER TABLE public.fuel_internal_transfers
  ADD COLUMN IF NOT EXISTS transfer_time TIME WITHOUT TIME ZONE;

UPDATE public.fuel_internal_transfers
   SET transfer_date = COALESCE(transfer_date, performed_at::date),
       transfer_time = COALESCE(transfer_time, performed_at::time)
 WHERE TRUE;

-- 3) Defaults and indexes
ALTER TABLE public.fuel_internal_transfers
  ALTER COLUMN transfer_date SET DEFAULT CURRENT_DATE;
ALTER TABLE public.fuel_internal_transfers
  ALTER COLUMN transfer_time SET DEFAULT TIME '00:00';

-- Drop old performed_at-based ordering index if present
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE c.relname='idx_fit_time' AND n.nspname='public'
  ) THEN
    EXECUTE 'DROP INDEX public.idx_fit_time';
  END IF;
END $$;

-- Create new indexes for ordering and lookups
CREATE INDEX IF NOT EXISTS idx_fit_transfer_date ON public.fuel_internal_transfers(transfer_date DESC);
CREATE INDEX IF NOT EXISTS idx_fit_when ON public.fuel_internal_transfers(transfer_date DESC, transfer_time DESC, id DESC);

-- 4) Drop legacy columns no longer used (safe if already dropped)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='driver_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN driver_id';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='performed_by_user_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN performed_by_user_id';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='performed_at'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN performed_at';
  END IF;
END $$;

COMMIT;
