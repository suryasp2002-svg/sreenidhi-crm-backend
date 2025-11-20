-- Add composite indexes to optimize frequent ops/day and ops/trip queries
DO $$
BEGIN
  -- Fuel Internal Transfers: by from_unit and to_unit with date+time for range scans
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='from_unit_id'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_date'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_internal_transfers' AND column_name='transfer_time'
  ) THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fit_from_unit_date_time ON public.fuel_internal_transfers(from_unit_id, transfer_date DESC, transfer_time DESC, id DESC)';
    EXCEPTION WHEN others THEN NULL; END;
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fit_to_unit_date_time ON public.fuel_internal_transfers(to_unit_id, transfer_date DESC, transfer_time DESC, id DESC)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;

  -- Fuel Sale Transfers: by from_unit and performed_at/sale_date
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='from_unit_id'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='performed_at'
    ) THEN
      BEGIN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fst_from_unit_performed_at ON public.fuel_sale_transfers(from_unit_id, performed_at DESC, id DESC)';
      EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_sale_transfers' AND column_name='sale_date'
    ) THEN
      BEGIN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fst_from_unit_sale_date ON public.fuel_sale_transfers(from_unit_id, sale_date DESC, id DESC)';
      EXCEPTION WHEN others THEN NULL; END;
    END IF;
  END IF;

  -- Testing Self Transfers: from_unit with performed_at
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='testing_self_transfers' AND column_name='from_unit_id'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='testing_self_transfers' AND column_name='performed_at'
  ) THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_testing_self_from_unit_time ON public.testing_self_transfers(from_unit_id, performed_at DESC, id DESC)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;

  -- Fuel lots: support latest in-stock per unit
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='unit_id'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='stock_status'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='created_at'
  ) THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_unit_stock_created ON public.fuel_lots(unit_id, stock_status, created_at DESC, id DESC)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
  -- Fuel lots: per unit per date scans for day views
  IF EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='unit_id'
  ) AND (
      EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='load_date'
      ) OR EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='loaded_date'
      )
  ) THEN
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='load_date'
      ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_unit_date ON public.fuel_lots(unit_id, load_date DESC, id DESC)';
      ELSE
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_unit_date ON public.fuel_lots(unit_id, loaded_date DESC, id DESC)';
      END IF;
    EXCEPTION WHEN others THEN NULL; END;
  END IF;

  -- Status history pagination support
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='status_history' AND column_name='opportunity_id'
  ) THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_status_history_opp ON public.status_history(opportunity_id)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='status_history' AND column_name='at'
  ) THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS idx_status_history_at ON public.status_history(at DESC, id DESC)';
    EXCEPTION WHEN others THEN NULL; END;
  END IF;
END $$;