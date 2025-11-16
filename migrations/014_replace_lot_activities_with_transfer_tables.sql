-- 014_replace_lot_activities_with_transfer_tables.sql
-- Drop legacy fuel_lot_activities and introduce two tables:
--   1) fuel_internal_transfers (tanker↔tanker | tanker↔datum)
--   2) fuel_sale_transfers (tanker|datum → vehicle)

BEGIN;

-- Drop old activities table
DROP TABLE IF EXISTS public.fuel_lot_activities CASCADE;

-- Internal transfers: movement between storage units (no vehicle)
CREATE TABLE IF NOT EXISTS public.fuel_internal_transfers (
  id BIGSERIAL PRIMARY KEY,
  from_lot_id BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  to_lot_id   BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  from_unit_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  to_unit_id   INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  from_unit_code TEXT NOT NULL,
  to_unit_code   TEXT NOT NULL,
  transfer_volume_liters INTEGER NOT NULL CHECK (transfer_volume_liters > 0),
  from_tanker_change INTEGER NOT NULL, -- negative of transfer_volume
  to_tanker_change   INTEGER NOT NULL, -- positive of transfer_volume
  from_lot_code_after TEXT NOT NULL,   -- e.g., LOT..-<used> or LOT..-<used>+(added)
  to_lot_code_after   TEXT NOT NULL,   -- e.g., LOT..-<used>+(<added cumulative>)
  transfer_to_empty BOOLEAN NOT NULL DEFAULT FALSE,
  driver_id INTEGER NULL REFERENCES public.drivers(id) ON DELETE SET NULL,
  driver_name TEXT NULL,
  performed_by TEXT NULL,
  performed_by_user_id UUID NULL,
  performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fit_from_lot ON public.fuel_internal_transfers(from_lot_id);
CREATE INDEX IF NOT EXISTS idx_fit_to_lot   ON public.fuel_internal_transfers(to_lot_id);
CREATE INDEX IF NOT EXISTS idx_fit_time     ON public.fuel_internal_transfers(performed_at DESC);

-- Sale transfers: movement from a storage unit to a vehicle/consumer
CREATE TABLE IF NOT EXISTS public.fuel_sale_transfers (
  id BIGSERIAL PRIMARY KEY,
  lot_id BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  from_unit_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  from_unit_code TEXT NOT NULL,
  to_vehicle TEXT NOT NULL,
  sale_volume_liters INTEGER NOT NULL CHECK (sale_volume_liters > 0),
  lot_code_after TEXT NOT NULL, -- after deducting sale from the lot
  driver_id INTEGER NULL REFERENCES public.drivers(id) ON DELETE SET NULL,
  driver_name TEXT NULL,
  performed_by TEXT NULL,
  performed_by_user_id UUID NULL,
  performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fst_lot  ON public.fuel_sale_transfers(lot_id);
CREATE INDEX IF NOT EXISTS idx_fst_time ON public.fuel_sale_transfers(performed_at DESC);

COMMIT;
