-- 032_fix_fuel_internal_transfers_restructure.sql
-- Bring public.fuel_internal_transfers to the desired schema even if prior 031 migration didn't apply.
-- Idempotent: add/rename/backfill new columns, drop legacy ones, and recreate helpful indexes.

BEGIN;

-- 1) Add new columns if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_volume'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN transfer_volume INTEGER';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='from_lot_code_change'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN from_lot_code_change TEXT';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='to_lot_code_change'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN to_lot_code_change TEXT';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_time'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN transfer_time TIME WITHOUT TIME ZONE';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='created_at'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE';
  END IF;
END $$;

-- 2) Backfill new columns from legacy ones where available
UPDATE public.fuel_internal_transfers
   SET transfer_volume = COALESCE(transfer_volume, transfer_volume_liters),
       from_lot_code_change = COALESCE(from_lot_code_change, from_lot_code_after),
       to_lot_code_change = COALESCE(to_lot_code_change, to_lot_code_after),
       transfer_date = COALESCE(transfer_date, performed_at::date),
       transfer_time = COALESCE(transfer_time, performed_at::time),
       created_at = COALESCE(created_at, performed_at)
 WHERE TRUE;

-- 3) Set NOT NULL and defaults on new columns now that data exists
-- Use two-step: set defaults, then enforce NOT NULL where safe
ALTER TABLE public.fuel_internal_transfers
  ALTER COLUMN transfer_volume SET DEFAULT 0;
-- Enforce positive volume where present
DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers
               ADD CONSTRAINT chk_fit_transfer_volume_positive CHECK (transfer_volume IS NULL OR transfer_volume > 0)';
  EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Ensure date/time defaults
ALTER TABLE public.fuel_internal_transfers
  ALTER COLUMN transfer_date SET DEFAULT CURRENT_DATE;
ALTER TABLE public.fuel_internal_transfers
  ALTER COLUMN transfer_time SET DEFAULT TIME '00:00';

-- 4) Drop legacy columns if they still exist
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_volume_liters'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN transfer_volume_liters';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='from_lot_code_after'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN from_lot_code_after';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='to_lot_code_after'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN to_lot_code_after';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='performed_at'
  ) THEN
    -- Drop dependent index if present
    BEGIN EXECUTE 'DROP INDEX IF EXISTS public.idx_fit_time'; EXCEPTION WHEN others THEN NULL; END;
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN performed_at';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='performed_by_user_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN performed_by_user_id';
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='driver_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers DROP COLUMN driver_id';
  END IF;
END $$;

-- 5) Recreate useful indexes for ordering and lookups
DO $$
BEGIN
  BEGIN EXECUTE 'DROP INDEX IF EXISTS public.idx_fit_transfer_date'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'DROP INDEX IF EXISTS public.idx_fit_when'; EXCEPTION WHEN others THEN NULL; END;
END $$;

CREATE INDEX IF NOT EXISTS idx_fit_transfer_date ON public.fuel_internal_transfers(transfer_date DESC);
CREATE INDEX IF NOT EXISTS idx_fit_when ON public.fuel_internal_transfers(transfer_date DESC, transfer_time DESC, id DESC);

COMMIT;
