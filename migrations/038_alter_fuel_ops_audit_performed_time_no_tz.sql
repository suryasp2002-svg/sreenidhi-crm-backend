-- 038_alter_fuel_ops_audit_performed_time_no_tz.sql
-- Convert performed_time to TIMESTAMP WITHOUT TIME ZONE and preserve wall-clock times

DO $$
BEGIN
  -- Only run if the column exists and is of timestamptz type
  IF EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema='public'
       AND table_name='fuel_ops_audit'
       AND column_name='performed_time'
       AND data_type IN ('timestamp with time zone')
  ) THEN
    -- Convert timestamptz -> timestamp without time zone in the current DB timezone
    -- This preserves the displayed wall-clock time users expect.
    EXECUTE 'ALTER TABLE public.fuel_ops_audit
              ALTER COLUMN performed_time TYPE TIMESTAMP WITHOUT TIME ZONE
              USING (performed_time AT TIME ZONE current_setting(''TIMEZONE''))';
  END IF;
END $$;