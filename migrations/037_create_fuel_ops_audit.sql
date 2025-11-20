-- Create audit table for Fuel Ops (At Depot, Day Logs)
-- Captures create/update/delete across sections with useful denormalized columns

-- Ensure UUID support
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS fuel_ops_audit (
  id               BIGSERIAL PRIMARY KEY,
  event_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  user_id          INTEGER,
  username         TEXT,
  tab              TEXT NOT NULL CHECK (tab IN ('At Depot','Day Logs')),
  section          TEXT NOT NULL,                                  -- At Depot: info|opening|ops|closing; Day Logs: logs
  action           TEXT NOT NULL CHECK (action IN ('CREATE','UPDATE','DELETE')),
  entity_type      TEXT NOT NULL,                                  -- trip|opening_reading|sale|transfer_out|transfer_in|testing|closing_reading|day_log
  entity_id        BIGINT,                                         -- id from the source table when applicable
  unit_id          INTEGER,                                        -- storage_units.id if relevant
  unit_type        TEXT,                                           -- TRUCK|DATUM|STORAGE (snapshot)
  driver_id        INTEGER,                                        -- drivers.id when applicable
  op_date          DATE,                                           -- operational day context
  performed_time   TIMESTAMPTZ,                                    -- timestamp of the operation, if any
  amount_liters    NUMERIC(12,2),                                  -- for sales/transfers/testing
  meter_reading    NUMERIC(12,2),                                  -- for opening/closing readings
  payload_old      JSONB,                                          -- previous full row/object snapshot
  payload_new      JSONB,                                          -- new full row/object snapshot
  reason           TEXT,                                           -- optional note / message
  request_id       UUID,                                           -- correlates multi-step actions
  ip_addr          TEXT
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_event_ts          ON fuel_ops_audit (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_tab_section       ON fuel_ops_audit (tab, section, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_unit_date         ON fuel_ops_audit (unit_id, op_date, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_entity            ON fuel_ops_audit (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_user              ON fuel_ops_audit (user_id, event_ts DESC);

-- Optional: GIN index for JSON if heavy querying on payloads is expected
-- CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_payload_old_gin ON fuel_ops_audit USING GIN (payload_old);
-- CREATE INDEX IF NOT EXISTS idx_fuel_ops_audit_payload_new_gin ON fuel_ops_audit USING GIN (payload_new);
