-- 017_add_load_type_and_dispenser_adjust.sql
-- Adds:
-- 1) fuel_lots.load_type (PURCHASE | EMPTY_TRANSFER) with default PURCHASE
-- 2) Immutable created_at enforcement on fuel_lots (prevent updates)
-- 3) fuel_internal_transfers.dispenser_reading_transfer_adjust (cumulative per from_unit)
--    and backfill of cumulative values ordered by performed_at

BEGIN;

-- 1) Add load_type column to fuel_lots
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='load_type'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_lots ADD COLUMN load_type TEXT NOT NULL DEFAULT ''PURCHASE''';
  END IF;
END $$;

-- Add CHECK constraint for allowed values (idempotent-safe)
DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT chk_fuel_lots_load_type CHECK (load_type IN (''PURCHASE'',''EMPTY_TRANSFER''))';
  EXCEPTION WHEN others THEN
    NULL;
  END;
END $$;

-- 1b) Backfill EMPTY_TRANSFER for lots created via transfer-to-empty seeding
UPDATE public.fuel_lots fl
SET load_type = 'EMPTY_TRANSFER'
WHERE load_type <> 'EMPTY_TRANSFER'
  AND EXISTS (
    SELECT 1
      FROM public.fuel_internal_transfers fit
      WHERE fit.to_lot_id = fl.id
        AND (
          fit.transfer_to_empty = TRUE
          OR (fit.to_lot_code_after = fl.lot_code_initial AND fit.transfer_volume_liters = fl.loaded_liters)
        )
  );

-- 2) Enforce immutable created_at on fuel_lots by resetting any attempted change during UPDATE
-- Create or replace immutable trigger function (simpler, not nested $$ blocks)
CREATE OR REPLACE FUNCTION public.prevent_created_at_update_fuel_lots()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    NEW.created_at := OLD.created_at;
  END IF;
  RETURN NEW;
END;
$$;

-- Safely create trigger if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_fuel_lots_created_at_immutable'
  ) THEN
    EXECUTE 'CREATE TRIGGER trg_fuel_lots_created_at_immutable BEFORE UPDATE ON public.fuel_lots FOR EACH ROW EXECUTE FUNCTION public.prevent_created_at_update_fuel_lots()';
  END IF;
END $$;

-- 3) Add dispenser_reading_transfer_adjust to fuel_internal_transfers
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='dispenser_reading_transfer_adjust'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_internal_transfers ADD COLUMN dispenser_reading_transfer_adjust INTEGER NOT NULL DEFAULT 0';
  END IF;
END $$;

-- Optional supporting index by from_unit for queries
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE INDEX idx_fit_from_unit_adjust ON public.fuel_internal_transfers(from_unit_id, dispenser_reading_transfer_adjust)';
  EXCEPTION WHEN others THEN
    NULL;
  END;
END $$;

-- Backfill cumulative adjustment per from_unit ordered by time
WITH sums AS (
  SELECT id,
         SUM(transfer_volume_liters) OVER (
           PARTITION BY from_unit_id
           ORDER BY performed_at, id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         )::int AS cum
    FROM public.fuel_internal_transfers
)
UPDATE public.fuel_internal_transfers fit
   SET dispenser_reading_transfer_adjust = s.cum
  FROM sums s
 WHERE s.id = fit.id;

COMMIT;
