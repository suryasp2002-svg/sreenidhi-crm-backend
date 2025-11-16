-- 031_ensure_testing_columns.sql
-- Ensure `activity`, `sale_date`, and `trip` columns exist on fuel_sale_transfers for TESTING entries

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='activity'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_sale_transfers ADD COLUMN activity TEXT NULL';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='sale_date'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_sale_transfers ADD COLUMN sale_date DATE NULL';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='trip'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_sale_transfers ADD COLUMN trip integer NULL';
  END IF;
END $$;

COMMIT;
