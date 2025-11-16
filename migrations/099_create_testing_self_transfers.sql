-- Migration: create testing_self_transfers
-- Purpose: store same-tanker TESTING records separately so they do not affect lot stock
CREATE TABLE IF NOT EXISTS public.testing_self_transfers (
  id BIGSERIAL PRIMARY KEY,
  lot_id INTEGER REFERENCES public.fuel_lots(id) ON DELETE SET NULL,
  activity TEXT NOT NULL DEFAULT 'TESTING',
  from_unit_id INTEGER REFERENCES public.storage_units(id) ON DELETE SET NULL,
  from_unit_code TEXT,
  to_vehicle TEXT,
  transfer_volume_liters INTEGER NOT NULL,
  lot_code TEXT,
  driver_id INTEGER,
  driver_name TEXT,
  performed_by TEXT,
  performed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  updated_by TEXT,
  sale_date DATE,
  trip INTEGER,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_testing_self_from_unit ON public.testing_self_transfers(from_unit_id);
CREATE INDEX IF NOT EXISTS idx_testing_self_performed_at ON public.testing_self_transfers(performed_at);

-- Keep migration idempotent
