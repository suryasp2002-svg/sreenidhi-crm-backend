-- PostgreSQL schema for Sreenidhi CRM

-- Try to enable pgcrypto for gen_random_uuid(); ignore if not permitted by provider
END $$;
-- (fuel_lot_activities removed; audit now uses transfer tables and testing_self_transfers)
                                 ADD CONSTRAINT opportunities_sector_check
                                 CHECK (
                                     sector IS NULL OR sector IN (
                                         ''CONSTRUCTION'',''MINING'',''HOSPITAL & HEALTHCARE'',''COMMERCIAL'',''INSTITUTIONAL'',''LOGISTICS'',''INDUSTRIAL'',''RESIDENTIAL'',''AGRICULTURE'',''OTHER''
                                     )
                                 )';
        EXCEPTION WHEN others THEN NULL; END;
END $$;

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(6) PRIMARY KEY,
    opportunity_id VARCHAR(20) NOT NULL,
    client_name VARCHAR(128) NOT NULL,
    gstin VARCHAR(15),
    primary_contact VARCHAR(64),
    phone VARCHAR(20),
    alt_phone VARCHAR(20),
    email VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS contracts (
    contract_id VARCHAR(6) PRIMARY KEY,           -- Unique 6-char alphanumeric contract ID
    opportunity_id VARCHAR(20) NOT NULL,          -- References opportunities(opportunity_id)
    client_name VARCHAR(128) NOT NULL,            -- Always copied from opportunities.client_name
    quoted_price_per_litre NUMERIC(10,2),
    start_date DATE,
    end_date DATE,                                -- New: end date column
    credit_period VARCHAR(32),                    -- Renamed from payment_terms
    primary_contact VARCHAR(64),
    phone_number VARCHAR(20),                     -- Renamed from phone
    alt_phone VARCHAR(20),
    email VARCHAR(128),
    gstin VARCHAR(15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_contract_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE
    -- client_name is not a foreign key, but should always be set to the value from opportunities.client_name
);

CREATE TABLE IF NOT EXISTS status_history (
    id SERIAL PRIMARY KEY,
    opportunity_id VARCHAR(20) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE,
    stage VARCHAR(32),
    reason TEXT,
    at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS expenses (
    id SERIAL PRIMARY KEY,
    opportunity_id VARCHAR(20) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE,
    amount NUMERIC(12,2),
    at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    note TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Ensure new columns exist if table predated this migration
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='expenses' AND column_name='status') THEN
        EXECUTE 'ALTER TABLE expenses ADD COLUMN status TEXT NOT NULL DEFAULT ''ACTIVE''';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='expenses' AND column_name='created_at') THEN
        EXECUTE 'ALTER TABLE expenses ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='expenses' AND column_name='updated_at') THEN
        EXECUTE 'ALTER TABLE expenses ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()';
    END IF;
END $$;

-- Audit table for expenses changes
CREATE TABLE IF NOT EXISTS expenses_audit (
    id SERIAL PRIMARY KEY,
    expense_id INTEGER,
    opportunity_id VARCHAR(20),
    action VARCHAR(16), -- CREATE, UPDATE, DELETE
    old_amount NUMERIC(12,2),
    new_amount NUMERIC(12,2),
    old_at TIMESTAMP WITHOUT TIME ZONE,
    new_at TIMESTAMP WITHOUT TIME ZONE,
    old_note TEXT,
    new_note TEXT,
    performed_by VARCHAR(64),
    performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_expenses_opp ON expenses(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_expenses_opp_at ON expenses(opportunity_id, at DESC);
CREATE INDEX IF NOT EXISTS idx_expenses_audit_exp ON expenses_audit(expense_id);
CREATE INDEX IF NOT EXISTS idx_expenses_audit_opp ON expenses_audit(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_expenses_audit_performed_at ON expenses_audit(performed_at DESC);
-- Optional helper for client name lookups via join
CREATE INDEX IF NOT EXISTS idx_opportunities_client_name_lower ON opportunities((LOWER(client_name)));
-- Helpful index for sector filtering
CREATE INDEX IF NOT EXISTS idx_opportunities_sector ON opportunities(sector);

-- Ensure the sequence for expenses.id is in sync with current max(id)
DO $$
DECLARE
    seq_name text;
    cur_max integer;
BEGIN
    seq_name := pg_get_serial_sequence('expenses','id');
    IF seq_name IS NOT NULL THEN
        SELECT MAX(id) INTO cur_max FROM expenses;
        IF cur_max IS NULL OR cur_max < 1 THEN
            -- No rows yet: set to 1 and mark is_called=false so nextval() returns 1
            PERFORM setval(seq_name, 1, false);
        ELSE
            -- Existing rows: set to current max and mark as called so nextval() returns max+1
            PERFORM setval(seq_name, cur_max, true);
        END IF;
    END IF;
END $$;

-- Meetings table (enhanced). We retain when_ts for backward-compat but introduce starts_at and richer metadata.
CREATE TABLE IF NOT EXISTS meetings (
    id VARCHAR(16) PRIMARY KEY,
    customer_id VARCHAR(6) REFERENCES customers(customer_id) ON DELETE CASCADE,
    opportunity_id VARCHAR(20),
    contract_id VARCHAR(6),
    subject VARCHAR(128),
    starts_at TIMESTAMP,
    when_ts TIMESTAMP, -- legacy; to be deprecated in favor of starts_at
    location VARCHAR(128),
    -- New optional metadata
    person_name TEXT,
    contact_phone TEXT,
    notes TEXT,
    status VARCHAR(32),
    assigned_to VARCHAR(64),
    created_by VARCHAR(64),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Meetings audit (append-only)
CREATE TABLE IF NOT EXISTS meetings_audit (
    id BIGSERIAL PRIMARY KEY,
    meeting_id VARCHAR(16) NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('CREATE','UPDATE','CANCEL','COMPLETE','DELETE','RESCHEDULE')),
    performed_by TEXT,
    performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    before_subject TEXT, after_subject TEXT,
    before_starts_at TIMESTAMP, after_starts_at TIMESTAMP,
    before_status TEXT, after_status TEXT,
    outcome_notes TEXT
);

-- Indexes for meetings audit
CREATE INDEX IF NOT EXISTS idx_meetings_audit_meeting ON meetings_audit(meeting_id);
CREATE INDEX IF NOT EXISTS idx_meetings_audit_performed_at ON meetings_audit(performed_at DESC);

-- If meetings_audit.meeting_id exists with a different type (e.g., INT), alter to VARCHAR(16) idempotently
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'meetings_audit' AND column_name = 'meeting_id' AND data_type <> 'character varying'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings_audit ALTER COLUMN meeting_id TYPE VARCHAR(16) USING meeting_id::text';
        EXCEPTION WHEN others THEN
            -- Swallow errors to keep migration idempotent in environments where casting is not needed/possible
            NULL;
        END;
    END IF;
END $$;

-- V2 Meetings audit with JSONB diff + snapshot (append-only)
-- Keeps legacy meetings_audit untouched; new ingestion writes can target this table
CREATE TABLE IF NOT EXISTS meetings_audit_v2 (
    id BIGSERIAL PRIMARY KEY,
    meeting_id VARCHAR(16) NOT NULL,
    version INTEGER NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('CREATE','UPDATE','CANCEL','COMPLETE','DELETE','RESCHEDULE')),
    performed_by_user_id UUID NULL,
    performed_by TEXT NULL,
    performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    diff JSONB NULL,
    snapshot JSONB NULL,
    note TEXT NULL,
    context JSONB NULL,
    CONSTRAINT uniq_meetings_audit_v2_version UNIQUE (meeting_id, version)
);
CREATE INDEX IF NOT EXISTS idx_meetings_audit_v2_meeting ON meetings_audit_v2(meeting_id);
CREATE INDEX IF NOT EXISTS idx_meetings_audit_v2_performed_at ON meetings_audit_v2(performed_at DESC);

-- Meeting email invite audit: one row per send attempt
CREATE TABLE IF NOT EXISTS meeting_email_audit (
    id BIGSERIAL PRIMARY KEY,
    meeting_id VARCHAR(16) NOT NULL,
    performed_by_user_id UUID NULL,
    performed_by TEXT NULL,
    performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    subject TEXT,
    to_recipients JSONB,
    cc_recipients JSONB,
    bcc_recipients JSONB,
    status TEXT NOT NULL CHECK (status IN ('SENT','FAILED')),
    message_id TEXT,
    error TEXT,
    sent_count INTEGER,
    recipients_detail JSONB
);
CREATE INDEX IF NOT EXISTS idx_meeting_email_audit_meeting ON meeting_email_audit(meeting_id);
CREATE INDEX IF NOT EXISTS idx_meeting_email_audit_performed_at ON meeting_email_audit(performed_at DESC);

-- Idempotent schema change: drop old notes columns and add outcome_notes
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings_audit' AND column_name='before_notes'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings_audit DROP COLUMN IF EXISTS before_notes';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings_audit' AND column_name='after_notes'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings_audit DROP COLUMN IF EXISTS after_notes';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings_audit' AND column_name='outcome_notes'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings_audit ADD COLUMN outcome_notes TEXT';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- Idempotent upgrades for meetings table
DO $$
BEGIN
    -- Add opportunity_id
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='opportunity_id'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN opportunity_id VARCHAR(20)';
    END IF;
    -- Add contract_id
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='contract_id'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN contract_id VARCHAR(6)';
    END IF;
    -- Add starts_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='starts_at'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN starts_at TIMESTAMP';
        -- Backfill from legacy when_ts if present
        IF EXISTS (
            SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='when_ts'
        ) THEN
            EXECUTE 'UPDATE meetings SET starts_at = COALESCE(starts_at, when_ts)';
        END IF;
    END IF;
    -- Add metadata columns if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='assigned_to'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN assigned_to VARCHAR(64)';
    END IF;
    -- Person name and contact phone for meeting context
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='person_name'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN person_name TEXT';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='contact_phone'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN contact_phone TEXT';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='created_by'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN created_by VARCHAR(64)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='created_at'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='updated_at'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='completed_at'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN completed_at TIMESTAMP';
    END IF;
    -- Subject should be NOT NULL (backfill empties)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='subject'
    ) THEN
        EXECUTE 'UPDATE meetings SET subject = COALESCE(NULLIF(subject, ''''), ''Meeting'') WHERE subject IS NULL OR subject = ''''';
        BEGIN
            EXECUTE 'ALTER TABLE meetings ALTER COLUMN subject SET NOT NULL';
        EXCEPTION WHEN others THEN
            -- If any conflicting rows remain, skip enforcing NOT NULL to avoid migration failure
            NULL;
        END;
    END IF;
    -- Status: constrain to expected values via a CHECK constraint if not present
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'meetings_status_check'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT meetings_status_check CHECK (status IN (''SCHEDULED'',''COMPLETED'',''CANCELLED'',''NO_SHOW'',''RESCHEDULED''))';
    END IF;
END $$;

-- =========================
-- Fuel Ops schema (lots, storage units) and helpers
-- Idempotent and safe to re-apply
-- =========================

-- Storage units: trucks (tankers), datum tanks, dispensers
CREATE TABLE IF NOT EXISTS public.storage_units (
    id SERIAL PRIMARY KEY,
    unit_type TEXT NOT NULL CHECK (unit_type IN ('TRUCK','DATUM','DISPENSER')),
    unit_code TEXT NOT NULL UNIQUE,          -- short code like '4T1' for 4-ton truck #1
    capacity_liters INTEGER NOT NULL CHECK (capacity_liters > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_storage_units_type ON public.storage_units(unit_type);
CREATE INDEX IF NOT EXISTS idx_storage_units_active ON public.storage_units(active) WHERE active = TRUE;

-- Optional vehicle metadata on storage_units (TRUCK/DATUM)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='storage_units' AND column_name='vehicle_number'
    ) THEN
        ALTER TABLE public.storage_units ADD COLUMN vehicle_number TEXT NULL;
    END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS uq_storage_units_vehicle_number ON public.storage_units(vehicle_number) WHERE vehicle_number IS NOT NULL;

-- Fuel lots (enhanced schema)
CREATE TABLE IF NOT EXISTS public.fuel_lots (
    id BIGSERIAL PRIMARY KEY,
    unit_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
    -- Snapshot at creation
    tanker_code TEXT NOT NULL,               -- from storage_units.unit_code
    tanker_capacity INTEGER NOT NULL CHECK (tanker_capacity > 0),
    load_date DATE NOT NULL,
    seq_index INTEGER NOT NULL CHECK (seq_index > 0),
    seq_letters TEXT NOT NULL,
    loaded_liters INTEGER NOT NULL CHECK (loaded_liters > 0),
    lot_code_initial TEXT NOT NULL UNIQUE,   -- LOTDDMONYY[UnitCode][SeqLetters][Loaded]
    -- Lifecycle and activity
    activity TEXT NOT NULL DEFAULT 'NEW_LOAD' CHECK (activity IN ('NEW_LOAD','TANKER_TO_TANKER','TANKER_TO_DATUM','TANKER_TO_VEHICLE','DATUM_TO_VEHICLE')),
    from_unit_code TEXT NULL,                -- applies to transfers
    to_unit_code TEXT NULL,                  -- applies to transfers (tanker/datum)
    to_vehicle TEXT NULL,                    -- applies to *to vehicle* activities
    driver_id INTEGER NULL REFERENCES public.drivers(id) ON DELETE SET NULL,
    driver_name TEXT NULL,
    transfer_volume_liters INTEGER NULL CHECK (transfer_volume_liters IS NULL OR transfer_volume_liters >= 0),
    sale_volume_liters INTEGER NULL CHECK (sale_volume_liters IS NULL OR sale_volume_liters >= 0),
    cumulative_transfer_liters INTEGER NOT NULL DEFAULT 0 CHECK (cumulative_transfer_liters >= 0),
    used_liters INTEGER NOT NULL DEFAULT 0 CHECK (used_liters >= 0),
    lot_code_by_transfer TEXT NULL,          -- e.g., LOT...-2000
    stock_status TEXT NOT NULL DEFAULT 'INSTOCK' CHECK (stock_status IN ('SOLD','INSTOCK')),
    lot_sold_status TEXT NOT NULL DEFAULT 'INSTOCK' CHECK (lot_sold_status IN ('SOLD','INSTOCK')),
    created_by TEXT NULL,
    created_by_user_id UUID NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    -- Per-unit-per-day uniqueness of sequence index
    CONSTRAINT uniq_fuel_lots_per_unit_day_seq UNIQUE (unit_id, load_date, seq_index)
);
-- Create date index on the actual date column name (load_date vs loaded_date)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='load_date'
    ) THEN
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_date ON public.fuel_lots(load_date DESC)';
        EXCEPTION WHEN others THEN NULL; END;
    ELSIF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='fuel_lots' AND column_name='loaded_date'
    ) THEN
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fuel_lots_date ON public.fuel_lots(loaded_date DESC)';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_fuel_lots_unit ON public.fuel_lots(unit_id);
CREATE INDEX IF NOT EXISTS idx_fuel_lots_stock ON public.fuel_lots(stock_status);

-- Convert 1 -> 'A', 2 -> 'B', 26 -> 'Z', 27 -> 'AA', etc.
CREATE OR REPLACE FUNCTION public.seq_index_to_letters(idx INTEGER)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    n INTEGER := idx;
    result TEXT := '';
    rem INTEGER;
BEGIN
    IF n IS NULL OR n < 1 THEN
        RETURN '';
    END IF;
    WHILE n > 0 LOOP
        rem := (n - 1) % 26;
        result := chr(65 + rem) || result; -- 65 = 'A'
        n := (n - 1) / 26;
    END LOOP;
    RETURN result;
END $$;

-- Build lot code: 'LOT' || DDMON || unit_code || letters || '-' || loaded
-- Example: LOT25NOV4T1A-3400
CREATE OR REPLACE FUNCTION public.gen_lot_code(
    p_unit_code TEXT,
    p_load_date DATE,
    p_seq_index INTEGER,
    p_loaded_liters INTEGER
) RETURNS TEXT LANGUAGE sql AS $$
    SELECT 'LOT' || to_char(p_load_date, 'DDMONYY') || p_unit_code || public.seq_index_to_letters(p_seq_index)
           || CAST(p_loaded_liters AS TEXT);
$$;

-- Next sequence index for a given date (global per-day, not per unit)
CREATE OR REPLACE FUNCTION public.next_seq_index_for_date(p_date DATE)
RETURNS INTEGER LANGUAGE sql AS $$
    SELECT COALESCE(MAX(seq_index), 0) + 1 FROM public.fuel_lots WHERE load_date = p_date;
$$;

-- Helper: preview next lot code for unit/date/liters without inserting
CREATE OR REPLACE FUNCTION public.preview_next_lot_code(
    p_unit_id INTEGER,
    p_load_date DATE,
    p_loaded_liters INTEGER
) RETURNS TABLE(lot_code TEXT, seq_index INTEGER) LANGUAGE plpgsql AS $$
DECLARE
    v_unit_code TEXT;
    v_cap INTEGER;
    v_seq INTEGER;
BEGIN
    SELECT unit_code, capacity_liters INTO v_unit_code, v_cap FROM public.storage_units WHERE id = p_unit_id;
    IF v_unit_code IS NULL THEN
        RAISE EXCEPTION 'Unknown storage unit id %', p_unit_id USING ERRCODE = '22P02';
    END IF;
    v_seq := public.next_seq_index_for_date(p_load_date);
    RETURN QUERY SELECT public.gen_lot_code(v_unit_code, p_load_date, v_seq, p_loaded_liters), v_seq;
END $$;

-- Safe insert: take advisory lock on date to prevent race on seq_index
CREATE OR REPLACE FUNCTION public.create_fuel_lot(
    p_unit_id INTEGER,
    p_load_date DATE,
    p_loaded_liters INTEGER
) RETURNS public.fuel_lots LANGUAGE plpgsql AS $$
DECLARE
    v_unit public.storage_units%ROWTYPE;
    v_seq INTEGER;
    v_letters TEXT;
    v_initial_code TEXT;
    v_row public.fuel_lots%ROWTYPE;
    v_key BIGINT;
BEGIN
    SELECT * INTO v_unit FROM public.storage_units WHERE id = p_unit_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown storage unit id %', p_unit_id USING ERRCODE = '22P02';
    END IF;
    IF p_loaded_liters <= 0 OR p_loaded_liters > v_unit.capacity_liters THEN
        RAISE EXCEPTION 'Loaded liters % must be >0 and <= capacity %', p_loaded_liters, v_unit.capacity_liters USING ERRCODE = '22000';
    END IF;
    -- advisory lock key based on yyyymmdd of date
    v_key := CAST(to_char(p_load_date, 'YYYYMMDD') AS BIGINT);
    PERFORM pg_advisory_xact_lock(v_key);
    v_seq := COALESCE((SELECT MAX(seq_index) FROM public.fuel_lots WHERE load_date = p_load_date), 0) + 1;
    v_letters := public.seq_index_to_letters(v_seq);
    v_initial_code := public.gen_lot_code(v_unit.unit_code, p_load_date, v_seq, p_loaded_liters);
    INSERT INTO public.fuel_lots (
        unit_id, tanker_code, tanker_capacity, load_date, seq_index, seq_letters,
        loaded_liters, lot_code_initial, activity, stock_status, lot_sold_status,
        cumulative_transfer_liters, used_liters
    )
    VALUES (
        v_unit.id, v_unit.unit_code, v_unit.capacity_liters, p_load_date, v_seq, v_letters,
        p_loaded_liters, v_initial_code, 'NEW_LOAD', 'INSTOCK', 'INSTOCK',
        0, 0
    )
    RETURNING * INTO v_row;
    RETURN v_row;
END $$;

-- (fuel_lot_activities removed; audit now uses transfer tables and testing_self_transfers)

-- =========================
-- Fuel Ops Readings
-- =========================

-- (truck_odometer_readings removed; use truck_odometer_day_readings and other odometer endpoints)

-- Daily rollups with opening/closing semantics (authoritative for reconciliation)
CREATE TABLE IF NOT EXISTS public.truck_odometer_day_readings (
    id BIGSERIAL PRIMARY KEY,
    truck_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
    reading_date DATE NOT NULL,
    opening_km NUMERIC(14,3) NOT NULL CHECK (opening_km >= 0),
    closing_km NUMERIC(14,3) NOT NULL CHECK (closing_km >= opening_km),
    opening_at TIMESTAMP WITHOUT TIME ZONE NULL,
    closing_at TIMESTAMP WITHOUT TIME ZONE NULL,
    note TEXT,
    created_by TEXT,
    created_by_user_id UUID,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uniq_truck_odometer_day UNIQUE (truck_id, reading_date)
);
CREATE INDEX IF NOT EXISTS idx_truck_odometer_day_truck_date ON public.truck_odometer_day_readings(truck_id, reading_date DESC);


-- Add foreign keys for opportunity and contract if not present
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'fk_meetings_opp' AND table_name = 'meetings'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT fk_meetings_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE SET NULL';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'fk_meetings_contract' AND table_name = 'meetings'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT fk_meetings_contract FOREIGN KEY (contract_id) REFERENCES contracts(contract_id) ON DELETE SET NULL';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'fk_meetings_customer' AND table_name = 'meetings'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT fk_meetings_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE';
        EXCEPTION WHEN others THEN
            -- If existing column length differs (e.g., VARCHAR(16)), FK add may still succeed; otherwise skip
            NULL;
        END;
    END IF;
END $$;

-- Helpful indexes for meetings
CREATE INDEX IF NOT EXISTS idx_meetings_starts_at ON meetings(starts_at);
CREATE INDEX IF NOT EXISTS idx_meetings_status ON meetings(status);
CREATE INDEX IF NOT EXISTS idx_meetings_customer ON meetings(customer_id);
CREATE INDEX IF NOT EXISTS idx_meetings_opportunity ON meetings(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_meetings_contract ON meetings(contract_id);
CREATE INDEX IF NOT EXISTS idx_meetings_assigned_to ON meetings(assigned_to);
CREATE INDEX IF NOT EXISTS idx_meetings_subject_lower ON meetings((LOWER(subject)));

-- Add user_id references for assignee/creator (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='assigned_to_user_id'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN assigned_to_user_id UUID NULL';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='meetings' AND column_name='created_by_user_id'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN created_by_user_id UUID NULL';
    END IF;
END $$;

-- FKs to users for new columns (best-effort)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='meetings' AND constraint_name='fk_meetings_assigned_user'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT fk_meetings_assigned_user FOREIGN KEY (assigned_to_user_id) REFERENCES users(id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='meetings' AND constraint_name='fk_meetings_created_user'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE meetings ADD CONSTRAINT fk_meetings_created_user FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- Helpful indexes for new user_id columns
CREATE INDEX IF NOT EXISTS idx_meetings_assigned_to_user ON meetings(assigned_to_user_id);
CREATE INDEX IF NOT EXISTS idx_meetings_created_by_user ON meetings(created_by_user_id);

-- Backfill user_id columns from legacy text fields (best-effort, idempotent)
DO $$
BEGIN
        -- meetings.created_by_user_id from created_by
        BEGIN
                EXECUTE 'UPDATE meetings m
                             SET created_by_user_id = u.id
                            FROM users u
                         WHERE m.created_by_user_id IS NULL
                             AND u.active = TRUE
                             AND (
                                 LOWER(COALESCE(m.created_by,'''')) = LOWER(COALESCE(u.email,'''')) OR
                                 LOWER(COALESCE(m.created_by,'''')) = LOWER(COALESCE(u.username,'''')) OR
                                 LOWER(COALESCE(m.created_by,'''')) = LOWER(COALESCE(u.full_name,''''))
                             )';
        EXCEPTION WHEN others THEN NULL; END;

        -- meetings.assigned_to_user_id from assigned_to
        BEGIN
                EXECUTE 'UPDATE meetings m
                             SET assigned_to_user_id = u.id
                            FROM users u
                         WHERE m.assigned_to_user_id IS NULL
                             AND u.active = TRUE
                             AND (
                                 LOWER(COALESCE(m.assigned_to,'''')) = LOWER(COALESCE(u.email,'''')) OR
                                 LOWER(COALESCE(m.assigned_to,'''')) = LOWER(COALESCE(u.username,'''')) OR
                                 LOWER(COALESCE(m.assigned_to,'''')) = LOWER(COALESCE(u.full_name,''''))
                             )';
        EXCEPTION WHEN others THEN NULL; END;
END $$;

CREATE TABLE IF NOT EXISTS reminders (
    id VARCHAR(16) PRIMARY KEY,
    title VARCHAR(128),
    due_ts TIMESTAMP,
    notes TEXT,
    status VARCHAR(32)
);

-- Idempotent upgrades for reminders table
DO $$
BEGIN
    -- Add type column (CALL, EMAIL, MEETING) if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='type'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN type TEXT';
        -- Default existing rows to CALL where possible
        EXECUTE 'UPDATE reminders SET type = COALESCE(type, ''CALL'')';
    END IF;
    -- Add notify_at timestamp for scheduling
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='notify_at'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN notify_at TIMESTAMP';
    END IF;
    -- Add recipient_email for email reminders
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='recipient_email'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN recipient_email TEXT';
    END IF;
    -- Status default to PENDING if not set
    BEGIN
        EXECUTE 'ALTER TABLE reminders ALTER COLUMN status SET DEFAULT ''PENDING''';
    EXCEPTION WHEN others THEN NULL; END;
    -- Add a simple CHECK for type values (best-effort)
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'reminders_type_check'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders ADD CONSTRAINT reminders_type_check CHECK (type IN (''CALL'',''EMAIL'',''MEETING''))';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- Helpful indexes for reminders
CREATE INDEX IF NOT EXISTS idx_reminders_due_ts ON reminders(due_ts);
CREATE INDEX IF NOT EXISTS idx_reminders_type ON reminders(type);

-- Additional fields for contact details (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='person_name'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN person_name TEXT';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='phone'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN phone TEXT';
    END IF;
END $$;

-- Remove deprecated column customer_id if exists (no longer needed)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='customer_id'
    ) THEN
        EXECUTE 'ALTER TABLE reminders DROP COLUMN IF EXISTS customer_id';
    END IF;
END $$;

-- Add linkage columns and receiver email (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='opportunity_id'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN opportunity_id VARCHAR(20)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='meeting_id'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN meeting_id VARCHAR(16)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='receiver_email'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN receiver_email TEXT';
    END IF;
    -- Track reminder creator for scoping
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='created_by'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN created_by TEXT';
    END IF;
    -- Add client_name for quick display without join
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='client_name'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN client_name TEXT';
    END IF;
    -- Add assignee fields for reminders (separate from creator)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='assigned_to'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN assigned_to TEXT';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='assigned_to_user_id'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN assigned_to_user_id UUID';
    END IF;
END $$;

-- Add foreign keys if not present
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='reminders' AND constraint_name='fk_reminders_opp'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders ADD CONSTRAINT fk_reminders_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='reminders' AND constraint_name='fk_reminders_meeting'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders ADD CONSTRAINT fk_reminders_meeting FOREIGN KEY (meeting_id) REFERENCES meetings(id) ON DELETE CASCADE';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- Indexes for linkage columns
CREATE INDEX IF NOT EXISTS idx_reminders_meeting ON reminders(meeting_id);
CREATE INDEX IF NOT EXISTS idx_reminders_opportunity ON reminders(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_reminders_created_by ON reminders(created_by);
-- Index to speed case-insensitive search on client_name
CREATE INDEX IF NOT EXISTS idx_reminders_client_name_lower ON reminders((LOWER(client_name)));

-- Add created_by_user_id to reminders (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='reminders' AND column_name='created_by_user_id'
    ) THEN
        EXECUTE 'ALTER TABLE reminders ADD COLUMN created_by_user_id UUID';
    END IF;
END $$;

-- FK and index for reminders.created_by_user_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='reminders' AND constraint_name='fk_reminders_created_user'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders ADD CONSTRAINT fk_reminders_created_user FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_reminders_created_by_user ON reminders(created_by_user_id);

-- FK and index for reminders.assigned_to_user_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='reminders' AND constraint_name='fk_reminders_assigned_user'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders ADD CONSTRAINT fk_reminders_assigned_user FOREIGN KEY (assigned_to_user_id) REFERENCES users(id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_reminders_assigned_to_user ON reminders(assigned_to_user_id);

-- Backfill reminders.created_by_user_id from created_by
DO $$
BEGIN
        BEGIN
                EXECUTE 'UPDATE reminders r
                             SET created_by_user_id = u.id
                            FROM users u
                         WHERE r.created_by_user_id IS NULL
                             AND u.active = TRUE
                             AND (
                                 LOWER(COALESCE(r.created_by,'''')) = LOWER(COALESCE(u.email,'''')) OR
                                 LOWER(COALESCE(r.created_by,'''')) = LOWER(COALESCE(u.username,'''')) OR
                                 LOWER(COALESCE(r.created_by,'''')) = LOWER(COALESCE(u.full_name,''''))
                             )';
        EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Backfill reminders.client_name from opportunities or via meetings linkage
DO $$
BEGIN
    -- From direct opportunity link
    BEGIN
        EXECUTE 'UPDATE reminders r
                    SET client_name = o.client_name
                   FROM opportunities o
                  WHERE r.client_name IS NULL
                    AND r.opportunity_id = o.opportunity_id';
    EXCEPTION WHEN others THEN NULL; END;
    -- From meeting link to opportunity
    BEGIN
        EXECUTE 'UPDATE reminders r
                    SET client_name = o.client_name,
                        opportunity_id = COALESCE(r.opportunity_id, m.opportunity_id)
                   FROM meetings m
                   JOIN opportunities o ON o.opportunity_id = m.opportunity_id
                  WHERE r.client_name IS NULL
                    AND r.meeting_id = m.id';
    EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Reminders audit v2 (append-only)
-- Mirrors migration 008_create_reminders_audit_v2.sql; kept idempotent
CREATE TABLE IF NOT EXISTS reminders_audit_v2 (
    id BIGSERIAL PRIMARY KEY,
    reminder_id VARCHAR(20) NOT NULL,
    version INTEGER NOT NULL,
    action TEXT NOT NULL,
    performed_by_user_id UUID NULL REFERENCES public.users(id) ON DELETE SET NULL,
    performed_by TEXT NULL,
    performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    diff JSONB NULL,
    snapshot JSONB NULL,
    note TEXT NULL,
    context JSONB NULL,
    CONSTRAINT reminders_audit_v2_unique_per_reminder_version UNIQUE(reminder_id, version)
);

CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_reminder_version ON reminders_audit_v2(reminder_id, version);
CREATE INDEX IF NOT EXISTS idx_reminders_audit_v2_performed_at ON reminders_audit_v2(performed_at DESC);

-- Add reminder_type column to reminders_audit_v2 and backfill idempotently
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='reminders_audit_v2' AND column_name='reminder_type'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminders_audit_v2 ADD COLUMN reminder_type TEXT NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- Backfill from snapshot first
    BEGIN
        EXECUTE 'UPDATE reminders_audit_v2 SET reminder_type = COALESCE(reminder_type, snapshot->>''type'') WHERE reminder_type IS NULL AND snapshot IS NOT NULL';
    EXCEPTION WHEN others THEN NULL; END;
    -- Backfill remaining from reminders table
    BEGIN
        EXECUTE 'UPDATE reminders_audit_v2 a SET reminder_type = r.type FROM reminders r WHERE a.reminder_type IS NULL AND r.id = a.reminder_id';
    EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Reminder Email Selected audit (one row per send attempt)
-- Mirrors migration 009_create_reminder_email_selected_audit.sql; kept idempotent
CREATE TABLE IF NOT EXISTS reminder_email_selected_audit (
    id BIGSERIAL PRIMARY KEY,
    operation_id UUID NOT NULL,
    reminder_id VARCHAR(20) NOT NULL,
    performed_by_user_id UUID NULL REFERENCES public.users(id) ON DELETE SET NULL,
    performed_by TEXT NULL,
    performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    subject TEXT NULL,
    to_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
    cc_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
    bcc_recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
    recipients_dedup JSONB NOT NULL DEFAULT '[]'::jsonb,
    sent_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    message_id TEXT NULL,
    error TEXT NULL,
    meta JSONB NULL
);

CREATE INDEX IF NOT EXISTS idx_resa_reminder_performed_at ON reminder_email_selected_audit(reminder_id, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_resa_operation_id ON reminder_email_selected_audit(operation_id);
CREATE INDEX IF NOT EXISTS idx_resa_status_sent_partial ON reminder_email_selected_audit(reminder_id) WHERE status = 'SENT';

-- Add reminder_type column to reminder_email_selected_audit and backfill idempotently
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='reminder_email_selected_audit' AND column_name='reminder_type'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE reminder_email_selected_audit ADD COLUMN reminder_type TEXT NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- Backfill from reminders table
    BEGIN
        EXECUTE 'UPDATE reminder_email_selected_audit a SET reminder_type = r.type FROM reminders r WHERE a.reminder_type IS NULL AND r.id = a.reminder_id';
    EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Reminder Call Attempt audit (one row per call attempt)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='reminder_call_attempt_audit'
    ) THEN
        CREATE TABLE public.reminder_call_attempt_audit (
            id BIGSERIAL PRIMARY KEY,
            reminder_id VARCHAR(20) NOT NULL,
            performed_by_user_id UUID NULL REFERENCES public.users(id) ON DELETE SET NULL,
            performed_by TEXT NULL,
            performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            phone TEXT NULL,
            status TEXT NOT NULL CHECK (status IN ('INITIATED','COMPLETED','FAILED')),
            error TEXT NULL,
            meta JSONB NULL
        );
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_rcaa_reminder_performed_at ON public.reminder_call_attempt_audit(reminder_id, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_rcaa_status_completed_partial ON public.reminder_call_attempt_audit(reminder_id) WHERE status = 'COMPLETED';

-- ==================================
-- Targets (Upcoming Campaign Targets)
-- ==================================
CREATE TABLE IF NOT EXISTS targets (
    id VARCHAR(24) PRIMARY KEY,
    client_name TEXT NOT NULL,
    notes TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    assigned_to TEXT,
    assigned_to_user_id UUID,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Ensure status constraint exists (best-effort idempotent)
DO $$
BEGIN
    -- Replace legacy status constraint with expanded set
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'targets_status_check'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE targets DROP CONSTRAINT targets_status_check';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    BEGIN
        EXECUTE 'ALTER TABLE targets ADD CONSTRAINT targets_status_check CHECK (status IN (''PENDING'',''DONE'',''COMPETITOR'',''ON_HOLD'',''CANCELLED'',''DUPLICATE'',''FOLLOW_UP''))';
    EXCEPTION WHEN others THEN NULL; END;
END $$;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_targets_status ON targets(status);
CREATE INDEX IF NOT EXISTS idx_targets_updated_at ON targets(updated_at DESC);
-- Ensure new assignment columns exist if table predated this update
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='targets' AND column_name='assigned_to'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE targets ADD COLUMN assigned_to TEXT';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='targets' AND column_name='assigned_to_user_id'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE targets ADD COLUMN assigned_to_user_id UUID';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_targets_assigned_user ON targets(assigned_to_user_id);

-- Optional FK to users
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints WHERE table_name='targets' AND constraint_name='fk_targets_assigned_user'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE targets ADD CONSTRAINT fk_targets_assigned_user FOREIGN KEY (assigned_to_user_id) REFERENCES users(id) ON DELETE SET NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- ==========================
-- Stage & Audit Enhancements
-- ==========================

-- Canonical stages lookup (flexible vs ENUM)
CREATE TABLE IF NOT EXISTS stages (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order INT NOT NULL DEFAULT 0
);

-- Opportunity stage audit (append-only)
CREATE TABLE IF NOT EXISTS opportunity_stage_audit (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id VARCHAR(20) NOT NULL,
    from_stage TEXT NOT NULL,
    to_stage TEXT NOT NULL,
    reason_code TEXT NULL,
    reason_text TEXT NULL,
    changed_by TEXT NULL,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'user',
    correlation_id UUID NULL,
    CONSTRAINT fk_opp_audit_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_opp_audit_opp ON opportunity_stage_audit(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_opp_audit_changed_at ON opportunity_stage_audit(changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_opp_audit_opp_changed_at ON opportunity_stage_audit(opportunity_id, changed_at DESC);

-- Contract status columns and audit
ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS contract_status TEXT NOT NULL DEFAULT 'ACTIVE';

-- Cleanup deprecated columns if present
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contracts' AND column_name = 'cancellation_reason_code'
    ) THEN
        EXECUTE 'ALTER TABLE contracts DROP COLUMN cancellation_reason_code';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contracts' AND column_name = 'cancellation_reason_text'
    ) THEN
        EXECUTE 'ALTER TABLE contracts DROP COLUMN cancellation_reason_text';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contracts' AND column_name = 'cancelled_at'
    ) THEN
        EXECUTE 'ALTER TABLE contracts DROP COLUMN cancelled_at';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS contract_status_audit (
    id BIGSERIAL PRIMARY KEY,
    contract_id VARCHAR(6) NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    reason_code TEXT NULL,
    reason_text TEXT NULL,
    changed_by TEXT NULL,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'user',
    correlation_id UUID NULL,
    CONSTRAINT fk_contract_audit_contract FOREIGN KEY (contract_id) REFERENCES contracts(contract_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_contract_audit_contract ON contract_status_audit(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_audit_changed_at ON contract_status_audit(changed_at DESC);

-- Customer status column and audit (optional but recommended)
ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS customer_status TEXT NOT NULL DEFAULT 'ACTIVE';

CREATE TABLE IF NOT EXISTS customer_status_audit (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(6) NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    reason_code TEXT NULL,
    reason_text TEXT NULL,
    changed_by TEXT NULL,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'user',
    correlation_id UUID NULL,
    CONSTRAINT fk_customer_audit_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_customer_audit_customer ON customer_status_audit(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_audit_changed_at ON customer_status_audit(changed_at DESC);

-- Seed core stages (idempotent)
INSERT INTO stages (id, label, is_terminal, sort_order) VALUES
    ('LEAD','LEAD', FALSE, 10),
    ('QUALIFIED','QUALIFIED', FALSE, 20),
    ('NEGOTIATION','NEGOTIATION', FALSE, 30),
    ('AGREED','AGREED', FALSE, 40),
    ('DISAGREED','DISAGREED', TRUE, 90),
    ('CANCELLED','CANCELLED', TRUE, 95)
ON CONFLICT (id) DO NOTHING;

-- Backfill baseline audit entries for existing opportunities (optional)
-- This inserts a seed row where from_stage = to_stage = current stage
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'opportunities') THEN
        INSERT INTO opportunity_stage_audit (opportunity_id, from_stage, to_stage, changed_at, source)
        SELECT o.opportunity_id, COALESCE(o.stage,'LEAD'), COALESCE(o.stage,'LEAD'), NOW(), 'migration'
        FROM opportunities o
        WHERE NOT EXISTS (
            SELECT 1 FROM opportunity_stage_audit a WHERE a.opportunity_id = o.opportunity_id
        );
    END IF;
END $$;

-- =====================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    username TEXT UNIQUE,
    full_name TEXT,
    role TEXT NOT NULL CHECK (role IN ('OWNER','ADMIN','EMPLOYEE')),
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP WITHOUT TIME ZONE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    phone TEXT,
    must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
    last_password_change_at TIMESTAMP WITHOUT TIME ZONE
);

-- Password Audit table: logs who changed whose password and when
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
         WHERE table_schema = 'public' AND table_name = 'users_password_audit'
    ) THEN
        CREATE TABLE public.users_password_audit (
            id SERIAL PRIMARY KEY,
            target_user_id UUID NOT NULL,
            target_email TEXT,
            target_username TEXT,
            target_full_name TEXT,
            target_role TEXT,
            changed_by_user_id UUID,
            changed_by TEXT,
            changed_by_role TEXT,
            performed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
    END IF;
END$$;

-- Helpful indexes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relname = 'idx_upa_performed_at' AND n.nspname = 'public'
    ) THEN
        CREATE INDEX idx_upa_performed_at ON public.users_password_audit (performed_at DESC);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relname = 'idx_upa_target_user_id' AND n.nspname = 'public'
    ) THEN
        CREATE INDEX idx_upa_target_user_id ON public.users_password_audit (target_user_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relname = 'idx_upa_changed_by_user_id' AND n.nspname = 'public'
    ) THEN
        CREATE INDEX idx_upa_changed_by_user_id ON public.users_password_audit (changed_by_user_id);
    END IF;
END$$;


-- Ensure new auth columns exist if the users table predated this schema update
DO $$
BEGIN
    -- username
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='username'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN username TEXT UNIQUE';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- phone
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='phone'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN phone TEXT';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- must_change_password
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='must_change_password'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN must_change_password BOOLEAN NOT NULL DEFAULT FALSE';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- last_password_change_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='last_password_change_at'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN last_password_change_at TIMESTAMP WITHOUT TIME ZONE';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- =============================
-- Opportunity Images (BYTEA)
-- =============================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_images'
    ) THEN
        CREATE TABLE public.opportunity_images (
            id BIGSERIAL PRIMARY KEY,
            opportunity_id VARCHAR(20) NOT NULL,
            mime_type TEXT NOT NULL,
            file_name TEXT,
            file_size_bytes INTEGER NOT NULL,
            data BYTEA NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            created_by TEXT NULL,
            created_by_user_id UUID NULL,
            CONSTRAINT fk_opp_images_opp FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id) ON DELETE CASCADE,
            CONSTRAINT chk_opp_images_size CHECK (file_size_bytes >= 0 AND file_size_bytes <= 5*1024*1024)
        );
    END IF;
END $$;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_opp_images_opp ON public.opportunity_images(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_opp_images_created_at ON public.opportunity_images(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
-- Case-insensitive unique index for username if present
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uniq_users_username_lower'
    ) THEN
        BEGIN
            EXECUTE 'CREATE UNIQUE INDEX uniq_users_username_lower ON users ((LOWER(username))) WHERE username IS NOT NULL';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- Additional auth fields for users: joining_date and status
DO $$
BEGIN
    -- joining_date DATE with default CURRENT_DATE
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='joining_date'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN joining_date DATE DEFAULT CURRENT_DATE';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- status TEXT with allowlist and default 'ACTIVE'
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='status'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD COLUMN status TEXT';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- Add CHECK constraint if not present
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'users_status_check'
    ) THEN
        BEGIN
            EXECUTE 'ALTER TABLE users ADD CONSTRAINT users_status_check CHECK (status IS NULL OR status IN (''ACTIVE'',''INACTIVE'',''ON_LEAVE'',''SUSPENDED''))';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
    -- Ensure default for status
    BEGIN
        EXECUTE 'ALTER TABLE users ALTER COLUMN status SET DEFAULT ''ACTIVE''';
    EXCEPTION WHEN others THEN NULL; END;
END $$;

-- =============================
-- User Profile details (personal/contact identifiers)
-- =============================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='user_profiles'
    ) THEN
        CREATE TABLE public.user_profiles (
            user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            date_of_birth DATE,
            gender TEXT CHECK (gender IN ('MALE','FEMALE','OTHER','PREFER_NOT_TO_SAY')),
            emergency_contact_name TEXT,
            emergency_contact_phone TEXT,
            address TEXT,
            pan TEXT,
            pan_normalized TEXT,
            aadhaar TEXT,
            aadhaar_last4 TEXT,
            updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
        );
    END IF;
END $$;

-- Helpful index for PAN lookups (not unique)
CREATE INDEX IF NOT EXISTS idx_user_profiles_pan_norm ON public.user_profiles ((COALESCE(pan_normalized, '')));

-- Keep updated_at fresh on update
CREATE OR REPLACE FUNCTION touch_user_profiles() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_touch_user_profiles') THEN
    CREATE TRIGGER trg_touch_user_profiles BEFORE UPDATE ON public.user_profiles
      FOR EACH ROW EXECUTE FUNCTION touch_user_profiles();
  END IF;
END $$;

-- =============================
-- User Photos (single current photo per user)
-- =============================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='user_photos'
    ) THEN
        CREATE TABLE public.user_photos (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            mime_type TEXT NOT NULL,
            file_name TEXT,
            file_size_bytes INTEGER NOT NULL,
            data BYTEA NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_user_photo_size CHECK (file_size_bytes >= 0 AND file_size_bytes <= 5*1024*1024)
        );
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_user_photos_user ON public.user_photos(user_id);
CREATE INDEX IF NOT EXISTS idx_user_photos_created_at ON public.user_photos(created_at DESC);

-- Allow multiple OWNER accounts: drop legacy unique constraint if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uniq_active_owner'
    ) THEN
        BEGIN
            EXECUTE 'DROP INDEX IF EXISTS uniq_active_owner';
        EXCEPTION WHEN others THEN NULL; END;
    END IF;
END $$;

-- =====================
-- User Permissions (Phase 2 partial)
-- =====================
CREATE TABLE IF NOT EXISTS user_permissions (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    tabs JSONB NOT NULL DEFAULT '{}'::jsonb,
    actions JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE OR REPLACE FUNCTION touch_user_permissions() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_touch_user_permissions') THEN
        CREATE TRIGGER trg_touch_user_permissions BEFORE UPDATE ON user_permissions
            FOR EACH ROW EXECUTE FUNCTION touch_user_permissions();
    END IF;
END $$;

