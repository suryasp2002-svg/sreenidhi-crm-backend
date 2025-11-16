-- 021_create_truck_dispenser_meter_snapshots.sql
-- Creates per-truck dispenser meter snapshots for off-hours monitoring and reconciliation support

BEGIN;

CREATE TABLE IF NOT EXISTS public.truck_dispenser_meter_snapshots (
  id BIGSERIAL PRIMARY KEY,
  truck_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
  reading_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  reading_liters NUMERIC(14,3) NOT NULL CHECK (reading_liters >= 0),
  source TEXT NOT NULL DEFAULT 'SNAPSHOT' CHECK (source IN ('SNAPSHOT','OPENING','CLOSING')),
  note TEXT NULL,
  created_by TEXT NULL,
  created_by_user_id UUID NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tdm_snapshots_truck_ts ON public.truck_dispenser_meter_snapshots(truck_id, reading_at DESC);

COMMIT;
