-- 013_simplify_fuel_lots_and_seq.sql
-- Simplify fuel_lots to store only lot creation data + cumulative usage + completion status + updated_at.
-- Also drop latest-activity fields from fuel_lots and remove related constraints.

BEGIN;

-- Drop constraints that reference activity/lot_sold_status if they exist
DO $$ BEGIN
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP CONSTRAINT IF EXISTS chk_fuel_lots_activity'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP CONSTRAINT IF EXISTS chk_fuel_lots_lot_sold'; EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Ensure required columns exist with defaults
ALTER TABLE public.fuel_lots
  ADD COLUMN IF NOT EXISTS used_liters INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS stock_status TEXT NOT NULL DEFAULT 'INSTOCK',
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW();

-- Drop "latest activity" columns from fuel_lots (they are tracked in fuel_lot_activities)
DO $$ BEGIN
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS activity'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS from_unit_code'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS to_unit_code'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS to_vehicle'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS driver_id'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS driver_name'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS transfer_volume_liters'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS sale_volume_liters'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS cumulative_transfer_liters'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS lot_code_by_transfer'; EXCEPTION WHEN others THEN NULL; END;
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN IF EXISTS lot_sold_status'; EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Retain stock_status constraint to INSTOCK/SOLD (best-effort re-add)
DO $$ BEGIN
  BEGIN EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT chk_fuel_lots_stock CHECK (stock_status IN (''SOLD'',''INSTOCK''))'; EXCEPTION WHEN others THEN NULL; END;
END $$;

COMMIT;
