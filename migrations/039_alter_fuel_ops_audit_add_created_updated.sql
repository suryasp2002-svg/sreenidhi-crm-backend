-- 039_alter_fuel_ops_audit_add_created_updated.sql
-- Add created_at and updated_at (both TIMESTAMP WITHOUT TIME ZONE),
-- backfill created_at from legacy event_ts preserving wall-clock time,
-- drop event_ts and re-create related indexes on created_at.

DO $$
BEGIN
  -- Add created_at if missing
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_ops_audit' AND column_name='created_at'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_ops_audit ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE NULL';
  END IF;

  -- Add updated_at if missing
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_ops_audit' AND column_name='updated_at'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_ops_audit ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE NULL';
  END IF;

  -- Backfill created_at from event_ts (timestamptz) preserving wall-clock
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_ops_audit' AND column_name='event_ts'
  ) THEN
    BEGIN
      EXECUTE 'UPDATE public.fuel_ops_audit
                  SET created_at = CASE
                    WHEN created_at IS NULL THEN (event_ts AT TIME ZONE current_setting(''TIMEZONE''))
                    ELSE created_at
                  END,
                      updated_at = COALESCE(updated_at, (event_ts AT TIME ZONE current_setting(''TIMEZONE'')))';
    EXCEPTION WHEN undefined_column THEN
      -- Ignore if event_ts doesn't exist despite check (race)
      NULL;
    END;
  END IF;

  -- Ensure NOT NULL + default now() for created_at/updated_at
  EXECUTE 'ALTER TABLE public.fuel_ops_audit ALTER COLUMN created_at SET DEFAULT NOW()';
  EXECUTE 'UPDATE public.fuel_ops_audit SET created_at = NOW() WHERE created_at IS NULL';
  EXECUTE 'ALTER TABLE public.fuel_ops_audit ALTER COLUMN created_at SET NOT NULL';

  EXECUTE 'ALTER TABLE public.fuel_ops_audit ALTER COLUMN updated_at SET DEFAULT NOW()';
  EXECUTE 'UPDATE public.fuel_ops_audit SET updated_at = NOW() WHERE updated_at IS NULL';
  EXECUTE 'ALTER TABLE public.fuel_ops_audit ALTER COLUMN updated_at SET NOT NULL';

  -- Drop old indexes on event_ts if exist
  BEGIN
    EXECUTE 'DROP INDEX IF EXISTS idx_fuel_ops_audit_event_ts';
  EXCEPTION WHEN undefined_table THEN NULL; END;
  BEGIN
    EXECUTE 'DROP INDEX IF EXISTS idx_fuel_ops_audit_tab_section';
  EXCEPTION WHEN undefined_table THEN NULL; END;
  BEGIN
    EXECUTE 'DROP INDEX IF EXISTS idx_fuel_ops_audit_unit_date';
  EXCEPTION WHEN undefined_table THEN NULL; END;

  -- Drop legacy column event_ts if present
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='fuel_ops_audit' AND column_name='event_ts'
  ) THEN
    EXECUTE 'ALTER TABLE public.fuel_ops_audit DROP COLUMN event_ts';
  END IF;

  -- Recreate indexes referencing created_at
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_created_at ON public.fuel_ops_audit (created_at DESC)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_tab_section ON public.fuel_ops_audit (tab, section, created_at DESC)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_unit_date ON public.fuel_ops_audit (unit_id, op_date, created_at DESC)';
END $$;
