-- 012_update_fuel_lot_functions.sql
-- Update fuel lot helper functions to match the enhanced schema

BEGIN;

-- Convert 1 -> 'A', 2 -> 'B', 26 -> 'Z', 27 -> 'AA', etc. (idempotent)
CREATE OR REPLACE FUNCTION public.seq_index_to_letters(idx INTEGER)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    n INTEGER := idx;
    result TEXT := '';
    rem INTEGER;
BEGIN
    IF n IS NULL OR n < 1 THEN
        RETURN '';
    END IF;
    WHILE n > 0 LOOP
        rem := (n - 1) % 26;
        result := chr(65 + rem) || result; -- 65 = 'A'
        n := (n - 1) / 26;
    END LOOP;
    RETURN result;
END $$;

-- Build lot code: 'LOT' || DDMONYY || unit_code || letters || loaded (no dash)
CREATE OR REPLACE FUNCTION public.gen_lot_code(
    p_unit_code TEXT,
    p_load_date DATE,
    p_seq_index INTEGER,
    p_loaded_liters INTEGER
) RETURNS TEXT LANGUAGE sql AS $$
    SELECT 'LOT' || to_char(p_load_date, 'DDMONYY') || p_unit_code || public.seq_index_to_letters(p_seq_index)
           || CAST(p_loaded_liters AS TEXT);
$$;

-- Next sequence index for a given unit and date (per-unit-per-day)
CREATE OR REPLACE FUNCTION public.next_seq_index_for_date_unit(p_date DATE, p_unit_id INTEGER)
RETURNS INTEGER LANGUAGE sql AS $$
    SELECT COALESCE(MAX(seq_index), 0) + 1 FROM public.fuel_lots WHERE load_date = p_date AND unit_id = p_unit_id;
$$;

-- Helper: preview next lot code for unit/date/liters without inserting
CREATE OR REPLACE FUNCTION public.preview_next_lot_code(
    p_unit_id INTEGER,
    p_load_date DATE,
    p_loaded_liters INTEGER
) RETURNS TABLE(lot_code TEXT, seq_index INTEGER) LANGUAGE plpgsql AS $$
DECLARE
    v_unit_code TEXT;
    v_cap INTEGER;
    v_seq INTEGER;
BEGIN
    SELECT unit_code, capacity_liters INTO v_unit_code, v_cap FROM public.storage_units WHERE id = p_unit_id;
    IF v_unit_code IS NULL THEN
        RAISE EXCEPTION 'Unknown storage unit id %', p_unit_id USING ERRCODE = '22P02';
    END IF;
    v_seq := public.next_seq_index_for_date_unit(p_load_date, p_unit_id);
    RETURN QUERY SELECT public.gen_lot_code(v_unit_code, p_load_date, v_seq, p_loaded_liters), v_seq;
END $$;

-- Safe insert: take advisory lock on date to prevent race on seq_index
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
    v_key BIGINT;
BEGIN
    SELECT * INTO v_unit FROM public.storage_units WHERE id = p_unit_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown storage unit id %', p_unit_id USING ERRCODE = '22P02';
    END IF;
    IF p_loaded_liters <= 0 OR p_loaded_liters > v_unit.capacity_liters THEN
        RAISE EXCEPTION 'Loaded liters % must be >0 and <= capacity %', p_loaded_liters, v_unit.capacity_liters USING ERRCODE = '22000';
    END IF;
    -- advisory lock key based on (unit_id, yyyymmdd) to avoid per-day race per unit
    PERFORM pg_advisory_xact_lock(p_unit_id, CAST(to_char(p_load_date, 'YYYYMMDD') AS INTEGER));
    v_seq := COALESCE((SELECT MAX(seq_index) FROM public.fuel_lots WHERE load_date = p_load_date AND unit_id = p_unit_id), 0) + 1;
    v_letters := public.seq_index_to_letters(v_seq);
    v_initial_code := public.gen_lot_code(v_unit.unit_code, p_load_date, v_seq, p_loaded_liters);
    INSERT INTO public.fuel_lots (
        unit_id, tanker_code, tanker_capacity, load_date, seq_index, seq_letters,
        loaded_liters, lot_code_initial, stock_status, used_liters, updated_at
    )
    VALUES (
        v_unit.id, v_unit.unit_code, v_unit.capacity_liters, p_load_date, v_seq, v_letters,
        p_loaded_liters, v_initial_code, 'INSTOCK', 0, NOW()
    )
    RETURNING * INTO v_row;
    RETURN v_row;
END $$;

COMMIT;
