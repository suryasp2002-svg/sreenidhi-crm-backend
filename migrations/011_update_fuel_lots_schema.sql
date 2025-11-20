-- 011_update_fuel_lots_schema.sql
-- Update fuel_lots schema to enhanced model and support lot activities

BEGIN;

-- Add new columns if missing
ALTER TABLE public.fuel_lots
    ADD COLUMN IF NOT EXISTS tanker_code TEXT,
    ADD COLUMN IF NOT EXISTS tanker_capacity INTEGER,
    ADD COLUMN IF NOT EXISTS lot_code_initial TEXT,
    ADD COLUMN IF NOT EXISTS activity TEXT NOT NULL DEFAULT 'NEW_LOAD',
    ADD COLUMN IF NOT EXISTS from_unit_code TEXT,
    ADD COLUMN IF NOT EXISTS to_unit_code TEXT,
    ADD COLUMN IF NOT EXISTS to_vehicle TEXT,
    ADD COLUMN IF NOT EXISTS driver_id INTEGER REFERENCES public.drivers(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS driver_name TEXT,
    ADD COLUMN IF NOT EXISTS transfer_volume_liters INTEGER,
    ADD COLUMN IF NOT EXISTS sale_volume_liters INTEGER,
    ADD COLUMN IF NOT EXISTS cumulative_transfer_liters INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS used_liters INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lot_code_by_transfer TEXT,
    ADD COLUMN IF NOT EXISTS stock_status TEXT NOT NULL DEFAULT 'INSTOCK',
    ADD COLUMN IF NOT EXISTS lot_sold_status TEXT NOT NULL DEFAULT 'INSTOCK',
    ADD COLUMN IF NOT EXISTS created_by TEXT,
    ADD COLUMN IF NOT EXISTS created_by_user_id UUID;

-- Backfill tanker_code/tanker_capacity/lot_code_initial from legacy columns if present
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='unit_code') THEN
    EXECUTE 'UPDATE public.fuel_lots SET tanker_code = COALESCE(tanker_code, unit_code)';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='capacity_liters') THEN
    EXECUTE 'UPDATE public.fuel_lots SET tanker_capacity = COALESCE(tanker_capacity, capacity_liters)';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='lot_code') THEN
    EXECUTE 'UPDATE public.fuel_lots SET lot_code_initial = COALESCE(lot_code_initial, lot_code)';
  END IF;
END $$;

-- Constrain columns (best-effort)
-- Safely constrain columns (guard lot_code_initial which may be renamed in later migrations)
DO $$
BEGIN
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ALTER COLUMN tanker_code SET NOT NULL'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ALTER COLUMN tanker_capacity SET NOT NULL'; EXCEPTION WHEN others THEN NULL; END;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='lot_code_initial'
  ) THEN
    BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ALTER COLUMN lot_code_initial SET NOT NULL'; EXCEPTION WHEN others THEN NULL; END;
  END IF;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ALTER COLUMN activity SET DEFAULT ''NEW_LOAD'''; EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Add checks and indexes (best-effort)
DO $$ BEGIN
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT chk_fuel_lots_activity CHECK (activity IN (''NEW_LOAD'',''TANKER_TO_TANKER'',''TANKER_TO_DATUM'',''TANKER_TO_VEHICLE'',''DATUM_TO_VEHICLE''))'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT chk_fuel_lots_stock CHECK (stock_status IN (''SOLD'',''INSTOCK''))'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT chk_fuel_lots_lot_sold CHECK (lot_sold_status IN (''SOLD'',''INSTOCK''))'; EXCEPTION WHEN others THEN NULL; END;
  -- Unique index only if original column still exists (may be renamed later)
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='lot_code_initial') THEN
    BEGIN EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_fuel_lots_initial_code ON public.fuel_lots(lot_code_initial)'; EXCEPTION WHEN others THEN NULL; END;
  END IF;
  BEGIN EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_stock ON public.fuel_lots(stock_status)'; EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Drop old columns if they exist
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='unit_code') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN unit_code';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='capacity_liters') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN capacity_liters';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fuel_lots' AND column_name='lot_code') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN lot_code';
  END IF;
END $$;

-- Create activities table (if not exists)
CREATE TABLE IF NOT EXISTS public.fuel_lot_activities (
  id BIGSERIAL PRIMARY KEY,
  lot_id BIGINT NOT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
  activity TEXT NOT NULL CHECK (activity IN ('TANKER_TO_TANKER','TANKER_TO_DATUM','TANKER_TO_VEHICLE','DATUM_TO_VEHICLE')),
  from_unit_id INTEGER NULL REFERENCES public.storage_units(id) ON DELETE SET NULL,
  to_unit_id INTEGER NULL REFERENCES public.storage_units(id) ON DELETE SET NULL,
  to_vehicle TEXT NULL,
  driver_id INTEGER NULL REFERENCES public.drivers(id) ON DELETE SET NULL,
  driver_name TEXT NULL,
  transfer_volume_liters INTEGER NULL,
  sale_volume_liters INTEGER NULL,
  performed_by TEXT NULL,
  performed_by_user_id UUID NULL,
  performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fla_lot ON public.fuel_lot_activities(lot_id);
CREATE INDEX IF NOT EXISTS idx_fla_performed_at ON public.fuel_lot_activities(performed_at DESC);

COMMIT;
