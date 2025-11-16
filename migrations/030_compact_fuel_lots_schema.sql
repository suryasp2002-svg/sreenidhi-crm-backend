-- 030_compact_fuel_lots_schema.sql
-- Reshape fuel_lots per Option A (preserve internal seq + functions),
-- rename lot_code_initial -> lot_code_created, add load_time_hhmm, drop unused legacy columns,
-- and provide a view with requested column order for PG Admin.

BEGIN;

-- 1) Rename lot_code_initial -> lot_code_created (idempotent-safe)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='lot_code_initial'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='lot_code_created'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_lots RENAME COLUMN lot_code_initial TO lot_code_created';
  END IF;
END $$;

-- 2) Add load_time_hhmm (stores HH:MM string for display)
ALTER TABLE public.fuel_lots
  ADD COLUMN IF NOT EXISTS load_time_hhmm TEXT NULL;

-- Backfill load_time_hhmm from load_time if present
UPDATE public.fuel_lots
   SET load_time_hhmm = COALESCE(load_time_hhmm, TO_CHAR(load_time, 'HH24:MI'))
 WHERE load_time IS NOT NULL;

-- 3) Drop unused legacy columns if present (keep minimal surface)
DO $$
BEGIN
  -- Activity & legacy inline fields (moved to transfer tables)
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='activity') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN activity';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='from_unit_code') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN from_unit_code';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='to_unit_code') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN to_unit_code';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='to_vehicle') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN to_vehicle';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='driver_id') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN driver_id';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='driver_name') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN driver_name';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='transfer_volume_liters') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN transfer_volume_liters';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='sale_volume_liters') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN sale_volume_liters';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='cumulative_transfer_liters') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN cumulative_transfer_liters';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='lot_code_by_transfer') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN lot_code_by_transfer';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='lot_sold_status') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN lot_sold_status';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='created_by_user_id') THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP COLUMN created_by_user_id';
  END IF;
END $$;

-- 4) Ensure unique index aligns to new column name (drop old if exists)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_fuel_lots_initial_code'
  ) THEN
    BEGIN
      EXECUTE 'DROP INDEX IF EXISTS public.uq_fuel_lots_initial_code';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
  -- Create new if missing
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_fuel_lots_created_code'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX uq_fuel_lots_created_code ON public.fuel_lots(lot_code_created)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
END $$;

-- 5) Update create_fuel_lot() to insert into lot_code_created
CREATE OR REPLACE FUNCTION public.create_fuel_lot(
    p_unit_id INTEGER,
    p_load_date DATE,
    p_loaded_liters INTEGER
) RETURNS public.fuel_lots LANGUAGE plpgsql AS $$
DECLARE
    v_unit public.storage_units%ROWTYPE;
    v_seq INTEGER;
    v_letters TEXT;
    v_initial_code TEXT;
    v_row public.fuel_lots%ROWTYPE;
BEGIN
    SELECT * INTO v_unit FROM public.storage_units WHERE id = p_unit_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown storage unit id %', p_unit_id USING ERRCODE = '22P02';
    END IF;
    IF p_loaded_liters <= 0 OR p_loaded_liters > v_unit.capacity_liters THEN
        RAISE EXCEPTION 'Loaded liters % must be >0 and <= capacity %', p_loaded_liters, v_unit.capacity_liters USING ERRCODE = '22000';
    END IF;
    -- advisory lock per (unit_id, date)
    PERFORM pg_advisory_xact_lock(p_unit_id, CAST(to_char(p_load_date, 'YYYYMMDD') AS INTEGER));
    v_seq := COALESCE((SELECT MAX(seq_index) FROM public.fuel_lots WHERE load_date = p_load_date AND unit_id = p_unit_id), 0) + 1;
    v_letters := public.seq_index_to_letters(v_seq);
    v_initial_code := public.gen_lot_code(v_unit.unit_code, p_load_date, v_seq, p_loaded_liters);
    INSERT INTO public.fuel_lots (
        unit_id, tanker_code, tanker_capacity, load_date, seq_index, seq_letters,
        loaded_liters, lot_code_created, stock_status, used_liters, updated_at
    )
    VALUES (
        v_unit.id, v_unit.unit_code, v_unit.capacity_liters, p_load_date, v_seq, v_letters,
        p_loaded_liters, v_initial_code, 'INSTOCK', 0, NOW()
    )
    RETURNING * INTO v_row;
    RETURN v_row;
END $$;

-- 6) View for PG Admin with requested order and friendly names
CREATE OR REPLACE VIEW public.fuel_lots_admin AS
SELECT
  fl.id,
  fl.tanker_code,
  fl.unit_id,
  fl.tanker_capacity,
  fl.load_date        AS loaded_date,
  fl.load_time_hhmm   AS loaded_time_hhmm,
  fl.loaded_liters,
  fl.lot_code_created,
  fl.load_type,
  fl.used_liters,
  fl.cumulative_testing_liters AS cummulative_testing_liters,
  fl.stock_status,
  fl.created_by,
  fl.created_at,
  fl.updated_at
FROM public.fuel_lots fl;

COMMIT;
