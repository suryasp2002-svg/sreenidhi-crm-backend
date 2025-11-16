-- 019_add_sale_date_to_fuel_sale_transfers.sql
-- Adds sale_date column to fuel_sale_transfers and backfills from performed_at::date

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='sale_date'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_sale_transfers ADD COLUMN sale_date DATE NULL';
  END IF;
END $$;

-- Backfill sale_date from performed_at
UPDATE public.fuel_sale_transfers
   SET sale_date = performed_at::date
 WHERE sale_date IS NULL;

CREATE INDEX IF NOT EXISTS idx_fst_sale_date ON public.fuel_sale_transfers(sale_date DESC);

COMMIT;
