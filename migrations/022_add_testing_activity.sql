-- 022_add_testing_activity.sql
-- Add TESTING activity support while preserving original NEW_LOAD metadata on fuel_lots.
-- Introduces cumulative_testing_liters on fuel_lots and testing_volume_liters on fuel_lot_activities.

BEGIN;

-- Add new cumulative testing liters column if missing
ALTER TABLE public.fuel_lots
  ADD COLUMN IF NOT EXISTS cumulative_testing_liters INTEGER NOT NULL DEFAULT 0;

-- If legacy fuel_lot_activities still exists (pre-014), extend it; otherwise skip gracefully
DO $$
DECLARE
  cons RECORD;
BEGIN
  IF to_regclass('public.fuel_lot_activities') IS NOT NULL THEN
    BEGIN
      EXECUTE 'ALTER TABLE public.fuel_lot_activities ADD COLUMN IF NOT EXISTS testing_volume_liters INTEGER';
    EXCEPTION WHEN others THEN NULL; END;
    -- Drop existing activity constraints
    FOR cons IN SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema='public' AND table_name='fuel_lot_activities' AND constraint_type='CHECK' AND constraint_name ILIKE '%activity%'
    LOOP
      BEGIN
        EXECUTE format('ALTER TABLE public.fuel_lot_activities DROP CONSTRAINT %I', cons.constraint_name);
      EXCEPTION WHEN others THEN NULL; END;
    END LOOP;
    -- Recreate with TESTING included
    BEGIN
      EXECUTE 'ALTER TABLE public.fuel_lot_activities ADD CONSTRAINT chk_fla_activity CHECK (activity IN (''TANKER_TO_TANKER'',''TANKER_TO_DATUM'',''TANKER_TO_VEHICLE'',''DATUM_TO_VEHICLE'',''TESTING''))';
    EXCEPTION WHEN others THEN NULL; END;
    -- Index for testing rows
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fla_testing ON public.fuel_lot_activities(lot_id) WHERE testing_volume_liters IS NOT NULL';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
END $$;

COMMIT;
