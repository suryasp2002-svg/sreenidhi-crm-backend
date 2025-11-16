-- 031_restructure_fuel_internal_transfers.sql
-- Recreate public.fuel_internal_transfers with requested columns and order.
-- Implements:
-- - Rename transfer_volume_liters -> transfer_volume
-- - Rename from_lot_code_after -> from_lot_code_change
-- - Rename to_lot_code_after   -> to_lot_code_change
-- - Split performed_at into transfer_date (DATE) and transfer_time (TIME)
-- - Add created_at (backfilled from performed_at) and keep updated_at
-- - Drop driver_id, performed_by_user_id
-- - Ensure column order in the physical table as requested

BEGIN;

-- Create new table with desired columns and order
CREATE TABLE IF NOT EXISTS public.fuel_internal_transfers_new (
  id BIGSERIAL PRIMARY KEY,
  from_lot_id BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  to_lot_id   BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  activity    TEXT NOT NULL,
  from_unit_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  from_unit_code TEXT NOT NULL,
  to_unit_id   INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  to_unit_code   TEXT NOT NULL,
  transfer_volume INTEGER NOT NULL CHECK (transfer_volume > 0),
  from_tanker_change INTEGER NOT NULL,
  from_lot_code_change TEXT NOT NULL,
  to_tanker_change   INTEGER NOT NULL,
  to_lot_code_change   TEXT NOT NULL,
  transfer_to_empty BOOLEAN NOT NULL DEFAULT FALSE,
  transfer_date DATE NOT NULL DEFAULT CURRENT_DATE,
  transfer_time TIME WITHOUT TIME ZONE NOT NULL DEFAULT TIME '00:00',
  driver_name TEXT NULL,
  performed_by TEXT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  dispenser_reading_transfer_adjust INTEGER NOT NULL DEFAULT 0
);

-- Copy data from existing table if present
INSERT INTO public.fuel_internal_transfers_new (
  id, from_lot_id, to_lot_id, activity,
  from_unit_id, from_unit_code, to_unit_id, to_unit_code,
  transfer_volume, from_tanker_change, from_lot_code_change, to_tanker_change, to_lot_code_change,
  transfer_to_empty, transfer_date, transfer_time,
  driver_name, performed_by, created_at, updated_at, dispenser_reading_transfer_adjust
)
SELECT 
  id, from_lot_id, to_lot_id, activity,
  from_unit_id, from_unit_code, to_unit_id, to_unit_code,
  transfer_volume_liters, from_tanker_change, from_lot_code_after, to_tanker_change, to_lot_code_after,
  transfer_to_empty,
  COALESCE(transfer_date, performed_at::date) AS transfer_date,
  COALESCE(performed_at::time, TIME '00:00') AS transfer_time,
  driver_name, performed_by,
  COALESCE(performed_at, NOW()) AS created_at,
  updated_at,
  dispenser_reading_transfer_adjust
FROM public.fuel_internal_transfers
ON CONFLICT DO NOTHING;

-- Drop old indexes referencing performed_at
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE c.relname='idx_fit_time' AND n.nspname='public'
  ) THEN
    EXECUTE 'DROP INDEX public.idx_fit_time';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE c.relname='idx_fit_transfer_date' AND n.nspname='public'
  ) THEN
    EXECUTE 'DROP INDEX public.idx_fit_transfer_date';
  END IF;
END$$;

-- Drop the old table and rename new
DROP TABLE IF EXISTS public.fuel_internal_transfers CASCADE;
ALTER TABLE public.fuel_internal_transfers_new RENAME TO fuel_internal_transfers;

-- Recreate helpful indexes
CREATE INDEX IF NOT EXISTS idx_fit_from_lot ON public.fuel_internal_transfers(from_lot_id);
CREATE INDEX IF NOT EXISTS idx_fit_to_lot   ON public.fuel_internal_transfers(to_lot_id);
CREATE INDEX IF NOT EXISTS idx_fit_transfer_date ON public.fuel_internal_transfers(transfer_date DESC);
CREATE INDEX IF NOT EXISTS idx_fit_from_unit_adjust ON public.fuel_internal_transfers(from_unit_id, dispenser_reading_transfer_adjust);
CREATE INDEX IF NOT EXISTS idx_fit_when ON public.fuel_internal_transfers(transfer_date DESC, transfer_time DESC, id DESC);

COMMIT;
