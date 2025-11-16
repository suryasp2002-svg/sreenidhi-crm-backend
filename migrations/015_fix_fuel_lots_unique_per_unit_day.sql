-- 015_fix_fuel_lots_unique_per_unit_day.sql
-- Goal: allow creating multiple lots on the same day for different tankers (per-unit-per-day uniqueness)
-- Safe and idempotent: drops the old per-day constraint if present; adds the new per-unit-per-day constraint if missing.

BEGIN;

-- Drop old unique constraint on (load_date, seq_index) if it exists
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema = 'public'
      AND table_name = 'fuel_lots'
      AND constraint_name = 'uniq_fuel_lots_per_day_seq'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_lots DROP CONSTRAINT uniq_fuel_lots_per_day_seq';
  END IF;
END $$;

-- Create new unique constraint on (unit_id, load_date, seq_index) if not already present
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema = 'public'
      AND table_name = 'fuel_lots'
      AND constraint_name = 'uniq_fuel_lots_per_unit_day_seq'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_lots ADD CONSTRAINT uniq_fuel_lots_per_unit_day_seq UNIQUE (unit_id, load_date, seq_index)';
  END IF;
END $$;

COMMIT;
