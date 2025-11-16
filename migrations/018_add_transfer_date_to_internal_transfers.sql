-- 018_add_transfer_date_to_internal_transfers.sql
-- Adds a DATE column for easy filtering/grouping and backfills existing rows.

BEGIN;

-- Add transfer_date if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_date'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN transfer_date DATE NULL';
  END IF;
END$$;

-- Backfill from performed_at if null
UPDATE public.fuel_internal_transfers
   SET transfer_date = DATE(performed_at)
 WHERE transfer_date IS NULL;

-- Helpful index for date queries
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE c.relname='idx_fit_transfer_date' AND n.nspname='public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_fit_transfer_date ON public.fuel_internal_transfers(transfer_date DESC)';
  END IF;
END$$;

COMMIT;
